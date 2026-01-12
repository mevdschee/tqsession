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

	"github.com/mevdschee/tqsession/pkg/server"
	"github.com/mevdschee/tqsession/pkg/tqsession"
)

func main() {
	dataDir := flag.String("data-dir", "data", "Directory for data files")
	port := flag.String("port", ":11211", "Port to listen on")
	syncMode := flag.String("sync-mode", "periodic", "Sync mode: none, periodic")
	syncInterval := flag.Duration("sync-interval", time.Second, "Sync interval for periodic fsync")
	defaultTTL := flag.Duration("default-ttl", 0, "Default TTL for keys without explicit expiry (0 = no expiry)")
	maxDataSize := flag.Int64("max-data-size", 64*1024*1024, "Maximum live data size in bytes for LRU eviction (0 = unlimited)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	// Parse sync strategy
	var syncStrategy tqsession.SyncStrategy
	switch *syncMode {
	case "none":
		syncStrategy = tqsession.SyncNone
	case "periodic":
		syncStrategy = tqsession.SyncPeriodic
	default:
		log.Fatalf("Invalid sync-mode: %s (valid: none, periodic)", *syncMode)
	}

	cfg := tqsession.Config{
		DataDir:       *dataDir,
		DefaultExpiry: *defaultTTL,
		MaxKeySize:    250,
		MaxValueSize:  1024 * 1024, // 1MB
		MaxDataSize:   *maxDataSize,
		SyncStrategy:  syncStrategy,
		SyncInterval:  *syncInterval,
	}

	cache, err := tqsession.New(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize TQSession: %v", err)
	}
	defer cache.Close()

	srv := server.New(cache, *port)
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

	log.Println("TQSession started")
	<-quit
	log.Println("Shutting down TQSession...")
}
