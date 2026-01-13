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

	"github.com/mevdschee/tqsession/internal/config"
	"github.com/mevdschee/tqsession/pkg/server"
	"github.com/mevdschee/tqsession/pkg/tqsession"
)

func main() {
	defaults := tqsession.DefaultConfig()

	configFile := flag.String("config", "", "Path to config file (INI format)")
	listen := flag.String("listen", ":11211", "Address to listen on ([host]:port)")
	dataDir := flag.String("data-dir", defaults.DataDir, "Directory for data files")
	shards := flag.Int("shards", tqsession.DefaultShardCount, "Number of shards for parallel access")
	defaultTTL := flag.Duration("default-ttl", defaults.DefaultTTL, "Default TTL for keys without explicit expiry (0 = no expiry)")
	maxTTL := flag.Duration("max-ttl", defaults.MaxTTL, "Maximum TTL cap for any key (0 = unlimited)")
	maxDataSize := flag.Int64("max-data-size", defaults.MaxDataSize, "Maximum live data size in bytes for LRU eviction (0 = unlimited)")
	syncMode := flag.String("sync-mode", "periodic", "Sync mode: none, periodic, always")
	syncInterval := flag.Duration("sync-interval", defaults.SyncInterval, "Sync interval for periodic fsync")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	var cfg tqsession.Config
	var serverPort string
	var shardCount int

	// Load config file if specified
	if *configFile != "" {
		fileCfg, err := config.Load(*configFile)
		if err != nil {
			log.Fatalf("Failed to load config file: %v", err)
		}
		cfg, err = fileCfg.ToTQSessionConfig()
		if err != nil {
			log.Fatalf("Invalid config: %v", err)
		}
		serverPort = fileCfg.Server.Listen
		if serverPort == "" {
			serverPort = ":11211"
		}
		shardCount = fileCfg.Shards()
		log.Printf("Loaded config from %s", *configFile)
	} else {
		// Use command-line flags, starting from defaults
		cfg = defaults
		cfg.DataDir = *dataDir
		cfg.DefaultTTL = *defaultTTL
		cfg.MaxTTL = *maxTTL
		cfg.MaxDataSize = *maxDataSize
		cfg.SyncInterval = *syncInterval

		switch *syncMode {
		case "none":
			cfg.SyncStrategy = tqsession.SyncNone
		case "periodic":
			cfg.SyncStrategy = tqsession.SyncPeriodic
		case "always":
			cfg.SyncStrategy = tqsession.SyncAlways
		default:
			log.Fatalf("Invalid sync-mode: %s (valid: none, periodic, always)", *syncMode)
		}

		serverPort = *listen
		shardCount = *shards
	}

	cache, err := tqsession.NewSharded(cfg, shardCount)
	if err != nil {
		log.Fatalf("Failed to initialize TQSession: %v", err)
	}
	defer cache.Close()

	srv := server.New(cache, serverPort)
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Start pprof server
	go func() {
		log.Println("Starting pprof server on :6062")
		if err := http.ListenAndServe("localhost:6062", nil); err != nil {
			log.Println("Pprof failed:", err)
		}
	}()

	// Set up signal handling
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	log.Printf("TQSession started on %s", serverPort)
	<-quit
	log.Println("Shutting down TQSession...")
}
