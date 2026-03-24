package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/andy/mikago/internal/api"
	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/raft"
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
	raftPort := flag.Int("raft-port", 0, "Raft RPC port (0 to disable cluster mode)")
	peersStr := flag.String("peers", "", "Comma-separated list of raft peers (e.g., 1@localhost:8001,2@localhost:8002)")
	// --cluster-brokers lists ALL brokers' Kafka endpoints for metadata and partition assignment.
	// Format: id@host:kafkaPort (e.g., 0@localhost:9092,1@localhost:9093,2@localhost:9094)
	clusterBrokersStr := flag.String("cluster-brokers", "", "Comma-separated list of all cluster brokers (e.g., 0@localhost:9092,1@localhost:9093)")
	tlsEnabled := flag.Bool("tls-enabled", false, "Enable TLS for Kafka client connections")
	tlsCert := flag.String("tls-cert", "", "Path to TLS certificate file (PEM)")
	tlsKey := flag.String("tls-key", "", "Path to TLS private key file (PEM)")
	flag.Parse()

	// Banner
	fmt.Println(`
            _ _  __                 
  _ __ ___ (_) |/ /__ _  __ _  ___  
 | '_ ' _ \| | ' // _' |/ _' |/ _ \ 
 | | | | | | | . \ (_| | (_| | (_) |
 |_| |_| |_|_|_|\_\__,_|\__, |\___/ 
                        |___/       
   mini Kafka with Go
	`)

	// Configure broker
	var peers []raft.Peer
	if *peersStr != "" {
		peerList := strings.Split(*peersStr, ",")
		for _, pStr := range peerList {
			parts := strings.SplitN(pStr, "@", 2)
			if len(parts) == 2 {
				peers = append(peers, raft.Peer{ID: parts[0], Address: parts[1]})
			}
		}
	}

	// Parse cluster broker endpoints for metadata and partition leader assignment.
	var knownBrokers []broker.ClusterBroker
	if *clusterBrokersStr != "" {
		for _, entry := range strings.Split(*clusterBrokersStr, ",") {
			parts := strings.SplitN(entry, "@", 2)
			if len(parts) != 2 {
				continue
			}
			var id, port32 int
			hostPort := strings.SplitN(parts[1], ":", 2)
			if len(hostPort) != 2 {
				continue
			}
			fmt.Sscanf(parts[0], "%d", &id)
			fmt.Sscanf(hostPort[1], "%d", &port32)
			knownBrokers = append(knownBrokers, broker.ClusterBroker{
				ID:   int32(id),
				Host: hostPort[0],
				Port: int32(port32),
			})
		}
	}

	cfg := broker.Config{
		BrokerID:        int32(*brokerID),
		Host:            *host,
		Port:            int32(*port),
		DataDir:         *dataDir,
		MaxMessageBytes: int32(*maxMsgBytes),
		MaxRequestBytes: int32(*maxReqBytes),
		LogSegmentBytes: *logSegBytes,
		RetentionMs:     *retentionHours * 60 * 60 * 1000, // hours → ms
		RaftPort:        int32(*raftPort),
		RaftPeers:       peers,
		KnownBrokers:    knownBrokers,
	}

	// Build optional TLS config
	var tlsCfg *tls.Config
	if *tlsEnabled {
		if *tlsCert == "" || *tlsKey == "" {
			log.Fatal("[miKago] --tls-cert and --tls-key are required when --tls-enabled is set")
		}
		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			log.Fatalf("[miKago] Failed to load TLS certificate: %v", err)
		}
		tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}}
	}

	// Initialize components
	b := broker.NewBroker(cfg)
	defer b.Close()
	handler := api.NewHandler(b)
	addr := fmt.Sprintf("%s:%d", *host, *port)
	srv := server.NewServer(addr, handler, cfg.MaxRequestBytes, tlsCfg)
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
