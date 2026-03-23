package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andy/mikago/internal/protocol"
)

func main() {
	address := flag.String("address", "localhost:9092", "Broker address")
	topic := flag.String("topic", "raw-bench", "Topic name")
	workers := flag.Int("workers", 10, "Number of concurrent TCP connections")
	totalMsgs := flag.Int("num", 100000, "Total messages to produce")
	msgSize := flag.Int("msg-size", 1024, "Message value size in bytes")
	batchSize := flag.Int("batch", 500, "Messages per Produce request (MessageSet size)")
	partitions := flag.Int("partitions", 1, "Number of partitions")
	flag.Parse()

	msgsPerWorker := *totalMsgs / *workers

	// Create topic with specified partitions if > 1
	if *partitions > 1 {
		fmt.Printf("📦 Creating topic %q with %d partitions...\n", *topic, *partitions)
		if err := createTopicRaw(*address, *topic, *partitions); err != nil {
			fmt.Printf("⚠️  %v (may already exist)\n", err)
		} else {
			fmt.Printf("✅ Created\n")
		}
	}

	fmt.Println("----------------------------------------------------------------")
	fmt.Println("🚀 miKago RAW TCP BENCHMARK (No kafka-go overhead)")
	fmt.Printf("📍 Broker:      %s\n", *address)
	fmt.Printf("📂 Topic:       %s\n", *topic)
	fmt.Printf("📦 Message:     %d bytes\n", *msgSize)
	fmt.Printf("🧵 Workers:     %d TCP connections\n", *workers)
	fmt.Printf("🔢 Total:       %d messages\n", *totalMsgs)
	fmt.Printf("📦 Batch:       %d msgs/request\n", *batchSize)
	fmt.Printf("🗂️  Partitions:  %d\n", *partitions)
	fmt.Println("----------------------------------------------------------------")

	// Pre-build the message value
	value := make([]byte, *msgSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	var totalSent atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", *address)
			if err != nil {
				log.Printf("Worker %d: connect failed: %v", workerID, err)
				return
			}
			defer conn.Close()

			sent := 0
			corrID := int32(workerID * 1000000)

			for sent < msgsPerWorker {
				// Figure out how many messages in this batch
				batch := *batchSize
				if sent+batch > msgsPerWorker {
					batch = msgsPerWorker - sent
				}

				corrID++
				// Round-robin partition across workers
				partID := int32(workerID % *partitions)

				// Build and send produce request with large MessageSet
				payload := buildProduceRequest(corrID, *topic, partID, value, batch)
				sendRaw(conn, payload)

				// Read response
				if err := readAndDiscard(conn); err != nil {
					log.Printf("Worker %d: read response failed: %v", workerID, err)
					return
				}

				sent += batch
				totalSent.Add(int64(batch))
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	actual := totalSent.Load()
	qps := float64(actual) / elapsed.Seconds()
	throughput := float64(actual*int64(*msgSize)) / 1024 / 1024 / elapsed.Seconds()

	fmt.Println("✅ All messages produced successfully.")
	fmt.Println("\n📊 PERFORMANCE SUMMARY")
	fmt.Printf("⏰ Time elapsed:  %v\n", elapsed)
	fmt.Printf("📈 QPS:           %.2f msgs/sec\n", qps)
	fmt.Printf("🚀 Throughput:    %.2f MB/sec\n", throughput)
	fmt.Println("----------------------------------------------------------------")
}

// buildProduceRequest creates a Produce v0 request with `count` messages in a single MessageSet.
func buildProduceRequest(corrID int32, topic string, partition int32, value []byte, count int) []byte {
	// Build MessageSet: N messages packed together
	msgSet := protocol.NewEncoder()
	for i := 0; i < count; i++ {
		// Each message in MessageSet v0:
		// offset(8) + message_size(4) + crc(4) + magic(1) + attributes(1) + keyLen(4) + valueLen(4) + value
		inner := protocol.NewEncoder()
		inner.PutInt8(0) // magic
		inner.PutInt8(0) // attributes
		inner.PutInt32(-1) // null key
		inner.PutInt32(int32(len(value)))
		inner.PutRawBytes(value)
		innerBytes := inner.Bytes()

		msgSet.PutInt64(0)                          // offset (ignored by broker)
		msgSet.PutInt32(int32(4 + len(innerBytes))) // message size
		msgSet.PutInt32(0)                          // crc
		msgSet.PutRawBytes(innerBytes)
	}
	msgSetBytes := msgSet.Bytes()

	// Produce request envelope
	enc := protocol.NewEncoder()
	enc.PutInt16(0)  // api_key = Produce
	enc.PutInt16(0)  // api_version = v0
	enc.PutInt32(corrID)
	enc.PutInt16(-1)      // client_id (null)
	enc.PutInt16(1)       // acks = 1
	enc.PutInt32(30000)   // timeout_ms
	enc.PutArrayLength(1) // 1 topic
	enc.PutString(topic)
	enc.PutArrayLength(1) // 1 partition
	enc.PutInt32(partition)
	enc.PutBytes(msgSetBytes)

	return enc.Bytes()
}

func sendRaw(conn net.Conn, payload []byte) {
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	conn.Write(buf)
}

func readAndDiscard(conn net.Conn) error {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(sizeBuf[:])
	_, err := io.ReadFull(conn, make([]byte, size))
	return err
}

// createTopicRaw sends a CreateTopics request (api_key=19, v0) via raw TCP.
func createTopicRaw(address, topic string, numPartitions int) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()

	enc := protocol.NewEncoder()
	enc.PutInt16(19) // api_key = CreateTopics
	enc.PutInt16(0)  // api_version = v0
	enc.PutInt32(99) // correlation_id
	enc.PutInt16(-1) // client_id (null)

	enc.PutArrayLength(1) // 1 topic
	enc.PutString(topic)
	enc.PutInt32(int32(numPartitions))
	enc.PutInt16(1)       // replication_factor
	enc.PutArrayLength(0) // no assignments
	enc.PutArrayLength(0) // no configs
	enc.PutInt32(5000)    // timeout_ms

	sendRaw(conn, enc.Bytes())
	return readAndDiscard(conn)
}
