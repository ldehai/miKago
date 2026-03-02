package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	broker := "localhost:9092"
	topic := "test-3rd-party"

	fmt.Println("🔌 Testing miKago with segmentio/kafka-go client...")
	fmt.Printf("   Broker: %s, Topic: %s\n\n", broker, topic)

	// --- Test 1: Dial and check connection ---
	fmt.Println("📡 [1] Connecting to broker...")
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		log.Fatalf("❌ Failed to connect: %v", err)
	}
	fmt.Println("   ✅ Connected!")

	// --- Test 2: API Versions ---
	fmt.Println("\n📡 [2] Fetching API versions...")
	versions, err := conn.ApiVersions()
	if err != nil {
		log.Fatalf("❌ ApiVersions failed: %v", err)
	}
	fmt.Printf("   ✅ Got %d API versions\n", len(versions))
	for _, v := range versions {
		fmt.Printf("     API key=%d: v%d ~ v%d\n", v.ApiKey, v.MinVersion, v.MaxVersion)
	}
	conn.Close()

	// --- Test 3: Produce messages using Writer ---
	fmt.Println("\n📡 [3] Producing messages...")
	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

	messages := []kafka.Message{
		{Key: []byte("key-1"), Value: []byte("Hello from kafka-go!")},
		{Key: []byte("key-2"), Value: []byte("Second message via kafka-go!")},
		{Key: []byte("key-3"), Value: []byte("Third message — miKago works! 🎉")},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = writer.WriteMessages(ctx, messages...)
	if err != nil {
		fmt.Printf("   ⚠️  Writer.WriteMessages error: %v\n", err)
		fmt.Println("   (This is expected if kafka-go uses a newer protocol version)")
		fmt.Println("   Trying low-level Conn.Write instead...")

		// Fallback: use low-level connection to produce
		conn2, err := kafka.DialLeader(ctx, "tcp", broker, topic, 0)
		if err != nil {
			log.Fatalf("❌ DialLeader failed: %v", err)
		}
		conn2.SetWriteDeadline(time.Now().Add(10 * time.Second))
		_, err = conn2.WriteMessages(
			kafka.Message{Value: []byte("Hello from kafka-go (low-level)!")},
			kafka.Message{Value: []byte("Second message (low-level)!")},
		)
		if err != nil {
			log.Fatalf("❌ Low-level write failed: %v", err)
		}
		fmt.Println("   ✅ Messages produced via low-level Conn!")
		conn2.Close()
	} else {
		fmt.Printf("   ✅ Produced %d messages!\n", len(messages))
		writer.Close()
	}

	// --- Test 4: Consume messages using Reader ---
	fmt.Println("\n📡 [4] Consuming messages...")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  1e6,
		MaxWait:   1 * time.Second,
	})

	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()

	for i := 0; i < 5; i++ {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			if err == context.DeadlineExceeded {
				fmt.Printf("   (No more messages after %d reads)\n", i)
				break
			}
			fmt.Printf("   ⚠️  Read error: %v\n", err)
			break
		}
		keyStr := "<null>"
		if msg.Key != nil {
			keyStr = string(msg.Key)
		}
		fmt.Printf("   Message[%d]: offset=%d, key=%s, value=%s\n",
			i, msg.Offset, keyStr, string(msg.Value))
	}
	reader.Close()

	fmt.Println("\n🎉 Third-party client integration test complete!")
}
