package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/andy/mikago/internal/api"
	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/server"
)

func main() {
	// Parse flags
	host := flag.String("host", "localhost", "Broker host address")
	port := flag.Int("port", 9092, "Broker port")
	brokerID := flag.Int("broker-id", 0, "Broker ID")
	dataDir := flag.String("data-dir", "./data", "Data directory for persistent storage")
	maxMsgBytes := flag.Int("max-message-bytes", 1048576, "Max message size in bytes (default: 1MB)")
	maxReqBytes := flag.Int("max-request-bytes", 104857600, "Max request size in bytes (default: 100MB)")
	logSegBytes := flag.Int64("log-segment-bytes", 1073741824, "Max log segment size in bytes (default: 1GB)")
	retentionHours := flag.Int64("retention-hours", 168, "Data retention in hours (default: 168 = 7 days, -1 = forever)")
	flag.Parse()

	// Banner
	fmt.Println(`
    _  __  _  _  __                
   (_)|  \/ |(_)/ /   __ _  __ _  ___  
   | || |\/| || / /__ / _' |/ _' |/ _ \ 
   | || |  | || \___/| (_| || (_| || (_) |
   |_||_|  |_||_|    \__,_| \__, |\___/ 
                              |___/      
   mini Kafka with Go — Phase 2 Persistent
	`)

	// Configure broker
	cfg := broker.Config{
		BrokerID:        int32(*brokerID),
		Host:            *host,
		Port:            int32(*port),
		DataDir:         *dataDir,
		MaxMessageBytes: int32(*maxMsgBytes),
		MaxRequestBytes: int32(*maxReqBytes),
		LogSegmentBytes: *logSegBytes,
		RetentionMs:     *retentionHours * 60 * 60 * 1000, // hours → ms
	}

	// Initialize components
	b := broker.NewBroker(cfg)
	defer b.Close()
	handler := api.NewHandler(b)
	addr := fmt.Sprintf("%s:%d", *host, *port)
	srv := server.NewServer(addr, handler, cfg.MaxRequestBytes)
	log.Printf("[miKago] Data directory: %s", *dataDir)
	log.Printf("[miKago] Limits: max_message=%dKB, max_request=%dMB, segment=%dMB",
		cfg.MaxMessageBytes/1024, cfg.MaxRequestBytes/1024/1024, cfg.LogSegmentBytes/1024/1024)
	if cfg.RetentionMs > 0 {
		log.Printf("[miKago] Retention: %d hours", cfg.RetentionMs/1000/3600)
	} else {
		log.Printf("[miKago] Retention: forever")
	}

	// Context with signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("[miKago] Received signal %v, shutting down...", sig)
		cancel()
	}()

	// Start server
	log.Printf("[miKago] Starting broker (id=%d) on %s", cfg.BrokerID, addr)
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("[miKago] Server error: %v", err)
	}

	log.Println("[miKago] Broker stopped.")
}
