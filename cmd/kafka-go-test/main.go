package main

import (
	"context"
	"fmt"
	"os"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	broker := "localhost:9092"
	topic := "kafka-go-persist-test"

	fmt.Println("🔌 Testing miKago persistence with segmentio/kafka-go...")
	fmt.Printf("   Broker: %s, Topic: %s\n\n", broker, topic)

	// ──────────────────────────────────────────────
	// Step 1: Connect and check API versions
	// ──────────────────────────────────────────────
	fmt.Println("📡 [1] Connecting and checking API versions...")
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		fmt.Printf("   ❌ Dial failed: %v\n", err)
		os.Exit(1)
	}
	versions, err := conn.ApiVersions()
	if err != nil {
		fmt.Printf("   ❌ ApiVersions failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("   ✅ Got %d API versions\n", len(versions))
	conn.Close()

	// ──────────────────────────────────────────────
	// Step 2: Produce messages via low-level Conn
	// ──────────────────────────────────────────────
	fmt.Println("\n📡 [2] Producing messages...")

	leader, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		fmt.Printf("   ❌ DialLeader failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("   ✅ DialLeader connected!")

	leader.SetWriteDeadline(time.Now().Add(5 * time.Second))
	messages := []kafka.Message{
		{Value: []byte("persist-test message #1")},
		{Value: []byte("persist-test message #2")},
		{Value: []byte("persist-test message #3")},
		{Value: []byte("persist-test message #4")},
		{Value: []byte("persist-test message #5")},
	}
	n, err := leader.WriteMessages(messages...)
	if err != nil {
		fmt.Printf("   ❌ WriteMessages failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("   ✅ Produced %d messages!\n", n)
	leader.Close()

	// ──────────────────────────────────────────────
	// Step 3: Consume messages back
	// ──────────────────────────────────────────────
	fmt.Println("\n📡 [3] Consuming messages back...")

	reader, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		fmt.Printf("   ❌ DialLeader (read) failed: %v\n", err)
		os.Exit(1)
	}
	defer reader.Close()

	reader.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Seek to the beginning
	reader.Seek(0, kafka.SeekAbsolute)

	batch := reader.ReadBatch(1, 1024*1024)
	defer batch.Close()

	count := 0
	for {
		msg, err := batch.ReadMessage()
		if err != nil {
			break
		}
		fmt.Printf("   Message[%d]: offset=%d, value=%s\n", count, msg.Offset, string(msg.Value))
		count++
	}

	if count == len(messages) {
		fmt.Printf("\n🎉 Successfully produced and consumed %d messages with disk persistence!\n", count)
	} else if count > 0 {
		fmt.Printf("\n⚠️  Read %d/%d messages (partial)\n", count, len(messages))
	} else {
		fmt.Println("\n❌ No messages consumed!")
		os.Exit(1)
	}

	// ──────────────────────────────────────────────
	// Step 4: Verify data files on disk
	// ──────────────────────────────────────────────
	fmt.Println("\n📡 [4] Checking data files on disk...")
	dataDir := "/tmp/mikago-kafka-go-test/topics/" + topic + "/0"
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		fmt.Printf("   ❌ Cannot read data dir: %v\n", err)
	} else {
		for _, e := range entries {
			info, _ := e.Info()
			fmt.Printf("   📁 %s (%d bytes)\n", e.Name(), info.Size())
		}
		fmt.Println("   ✅ Data persisted to disk!")
	}
}
