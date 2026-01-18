package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mevdschee/tqcache/internal/config"
	"github.com/mevdschee/tqcache/pkg/server"
	"github.com/mevdschee/tqcache/pkg/tqcache"
)

func main() {
	defaults := tqcache.DefaultConfig()

	// Memcached-compatible short flags
	port := flag.Int("p", 11211, "TCP port to listen on")
	listenAddr := flag.String("l", "", "Interface to listen on (default: INADDR_ANY)")
	socketPath := flag.String("s", "", "Unix socket path (overrides -p and -l)")
	connections := flag.Int("c", 1024, "Max simultaneous connections")
	threads := flag.Int("t", tqcache.DefaultShardCount, "Number of shards/threads to use")

	// Long name alternatives (same variables)
	flag.IntVar(port, "port", 11211, "TCP port to listen on")
	flag.StringVar(listenAddr, "listen", "", "Interface to listen on")
	flag.StringVar(socketPath, "socket", "", "Unix socket path")
	flag.IntVar(connections, "connections", 1024, "Max simultaneous connections")
	flag.IntVar(threads, "threads", tqcache.DefaultShardCount, "Number of shards/threads")

	// TQCache-specific options (not in memcached)
	configFile := flag.String("config", "", "Path to config file (INI format)")
	dataDir := flag.String("data-dir", defaults.DataDir, "Directory for data files")
	defaultTTL := flag.Duration("default-ttl", defaults.DefaultTTL, "Default TTL for keys without explicit expiry (0 = no expiry)")
	maxTTL := flag.Duration("max-ttl", defaults.MaxTTL, "Maximum TTL cap for any key (0 = unlimited)")
	syncMode := flag.String("sync-mode", "periodic", "Sync mode: none, periodic, always")
	syncInterval := flag.Duration("sync-interval", defaults.SyncInterval, "Sync interval for periodic fsync")
	pprofEnabled := flag.Bool("pprof", false, "Enable pprof profiling server on :6062")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nTQCache - High-performance persistent cache\n\n")
		fmt.Fprintf(os.Stderr, "Memcached-compatible options:\n")
		fmt.Fprintf(os.Stderr, "  -p, -port <num>          TCP port to listen on (default: 11211)\n")
		fmt.Fprintf(os.Stderr, "  -l, -listen <addr>       Interface to listen on (default: INADDR_ANY)\n")
		fmt.Fprintf(os.Stderr, "  -s, -socket <path>       Unix socket path (overrides -p and -l)\n")
		fmt.Fprintf(os.Stderr, "  -c, -connections <num>   Max simultaneous connections (default: 1024)\n")
		fmt.Fprintf(os.Stderr, "  -t, -threads <num>       Number of shards/threads (default: %d)\n", tqcache.DefaultShardCount)
		fmt.Fprintf(os.Stderr, "\nTQCache options:\n")
		fmt.Fprintf(os.Stderr, "  -config <file>           Path to config file\n")
		fmt.Fprintf(os.Stderr, "  -data-dir <path>         Directory for data files (default: %s)\n", defaults.DataDir)
		fmt.Fprintf(os.Stderr, "  -default-ttl <duration>  Default TTL for keys (default: %v)\n", defaults.DefaultTTL)
		fmt.Fprintf(os.Stderr, "  -max-ttl <duration>      Maximum TTL cap (default: %v)\n", defaults.MaxTTL)
		fmt.Fprintf(os.Stderr, "  -sync-mode <mode>        Sync mode: none, periodic, always (default: periodic)\n")
		fmt.Fprintf(os.Stderr, "  -sync-interval <dur>     Sync interval for periodic mode (default: %v)\n", defaults.SyncInterval)
		fmt.Fprintf(os.Stderr, "  -pprof                   Enable pprof profiling server on :6062\n")
	}
	flag.Parse()

	var cfg tqcache.Config
	var listenString string
	var shardCount int
	var maxConnections int

	// Load config file if specified
	if *configFile != "" {
		fileCfg, err := config.Load(*configFile)
		if err != nil {
			log.Fatalf("Failed to load config file: %v", err)
		}
		cfg, err = fileCfg.ToTQCacheConfig()
		if err != nil {
			log.Fatalf("Invalid config: %v", err)
		}
		// Build listen string from config
		serverPort := fileCfg.Server.Listen
		if serverPort == "" {
			serverPort = ":11211"
		}
		listenString = serverPort
		shardCount = fileCfg.Shards()
		maxConnections = *connections // Use command-line default
		log.Printf("Loaded config from %s", *configFile)
	} else {
		// Use command-line flags, starting from defaults
		cfg = defaults
		cfg.DataDir = *dataDir
		cfg.DefaultTTL = *defaultTTL
		cfg.MaxTTL = *maxTTL
		cfg.SyncInterval = *syncInterval

		switch *syncMode {
		case "none":
			cfg.SyncStrategy = tqcache.SyncNone
		case "periodic":
			cfg.SyncStrategy = tqcache.SyncPeriodic
		case "always":
			cfg.SyncStrategy = tqcache.SyncAlways
		default:
			log.Fatalf("Invalid sync-mode: %s (valid: none, periodic, always)", *syncMode)
		}

		// Build listen string
		if *socketPath != "" {
			listenString = *socketPath
		} else if *listenAddr != "" {
			listenString = fmt.Sprintf("%s:%d", *listenAddr, *port)
		} else {
			listenString = fmt.Sprintf(":%d", *port)
		}
		shardCount = *threads
		maxConnections = *connections
	}

	cache, err := tqcache.NewSharded(cfg, shardCount)
	if err != nil {
		log.Fatalf("Failed to initialize TQCache: %v", err)
	}
	defer cache.Close()

	srv := server.NewWithOptions(cache, listenString, maxConnections)
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Start pprof server if enabled
	if *pprofEnabled {
		go func() {
			log.Println("Starting pprof server on :6062")
			if err := http.ListenAndServe("localhost:6062", nil); err != nil {
				log.Println("Pprof failed:", err)
			}
		}()
	}

	// Set up signal handling
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	log.Printf("TQCache started on %s (shards: %d, connections: %d, data-dir: %s)",
		listenString, shardCount, maxConnections, cfg.DataDir)
	<-quit
	log.Println("Shutting down TQCache...")
}

// parseDuration parses a duration string allowing for time unit suffixes
func parseDuration(s string) (time.Duration, error) {
	return time.ParseDuration(s)
}
