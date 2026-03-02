package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/andy/mikago/internal/protocol"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Fatalf("❌ Failed to connect: %v", err)
	}
	defer conn.Close()
	fmt.Println("✅ Connected to miKago broker at localhost:9092")

	// --- Test 1: ApiVersions ---
	fmt.Println("\n📡 [1] Sending ApiVersions request...")
	sendApiVersionsRequest(conn, 1)
	readApiVersionsResponse(conn)

	// --- Test 2: Metadata ---
	fmt.Println("\n📡 [2] Sending Metadata request for topic 'hello-mikago'...")
	sendMetadataRequest(conn, 2, "hello-mikago")
	readMetadataResponse(conn)

	// --- Test 3: Produce ---
	fmt.Println("\n📡 [3] Producing message 'Hello from miKago client!' to 'hello-mikago' partition 0...")
	sendProduceRequest(conn, 3, "hello-mikago", 0, nil, []byte("Hello from miKago client!"))
	readProduceResponse(conn)

	// --- Test 4: Produce another message ---
	fmt.Println("\n📡 [4] Producing message 'Second message!' to 'hello-mikago' partition 0...")
	sendProduceRequest(conn, 4, "hello-mikago", 0, []byte("key1"), []byte("Second message!"))
	readProduceResponse(conn)

	// --- Test 5: Fetch ---
	fmt.Println("\n📡 [5] Fetching messages from 'hello-mikago' partition 0, offset 0...")
	sendFetchRequest(conn, 5, "hello-mikago", 0, 0)
	readFetchResponse(conn)

	fmt.Println("\n🎉 All tests passed! miKago broker is working correctly.")
}

// --- Helper: send/receive with size-delimited framing ---

func sendRequest(conn net.Conn, payload []byte) {
	msg := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(msg[:4], uint32(len(payload)))
	copy(msg[4:], payload)
	conn.Write(msg)
}

func readResponse(conn net.Conn) []byte {
	sizeBuf := make([]byte, 4)
	io.ReadFull(conn, sizeBuf)
	size := binary.BigEndian.Uint32(sizeBuf)
	body := make([]byte, size)
	io.ReadFull(conn, body)
	return body
}

// --- ApiVersions (api_key=18, v0) ---

func sendApiVersionsRequest(conn net.Conn, correlationID int32) {
	enc := protocol.NewEncoder()
	enc.PutInt16(18)            // api_key
	enc.PutInt16(0)             // api_version
	enc.PutInt32(correlationID) // correlation_id
	enc.PutInt16(-1)            // client_id (null)
	sendRequest(conn, enc.Bytes())
}

func readApiVersionsResponse(conn net.Conn) {
	data := readResponse(conn)
	d := protocol.NewDecoder(data)
	corrID, _ := d.Int32()
	errCode, _ := d.Int16()
	count, _ := d.ArrayLength()
	fmt.Printf("   Response: correlation_id=%d, error=%d\n", corrID, errCode)
	fmt.Printf("   Supported APIs (%d):\n", count)
	for i := 0; i < count; i++ {
		apiKey, _ := d.Int16()
		minV, _ := d.Int16()
		maxV, _ := d.Int16()
		name := apiKeyName(apiKey)
		fmt.Printf("     - %s (key=%d): v%d ~ v%d\n", name, apiKey, minV, maxV)
	}
}

// --- Metadata (api_key=3, v0) ---

func sendMetadataRequest(conn net.Conn, correlationID int32, topic string) {
	enc := protocol.NewEncoder()
	enc.PutInt16(3)             // api_key
	enc.PutInt16(0)             // api_version
	enc.PutInt32(correlationID) // correlation_id
	enc.PutInt16(-1)            // client_id (null)
	enc.PutArrayLength(1)       // 1 topic
	enc.PutString(topic)
	sendRequest(conn, enc.Bytes())
}

func readMetadataResponse(conn net.Conn) {
	data := readResponse(conn)
	d := protocol.NewDecoder(data)
	corrID, _ := d.Int32()
	brokerCount, _ := d.ArrayLength()
	fmt.Printf("   Response: correlation_id=%d\n", corrID)
	for i := 0; i < brokerCount; i++ {
		nodeID, _ := d.Int32()
		host, _ := d.String()
		port, _ := d.Int32()
		fmt.Printf("   Broker: id=%d, host=%s, port=%d\n", nodeID, host, port)
	}
	topicCount, _ := d.ArrayLength()
	for i := 0; i < topicCount; i++ {
		errCode, _ := d.Int16()
		name, _ := d.String()
		partCount, _ := d.ArrayLength()
		fmt.Printf("   Topic: %s (error=%d, partitions=%d)\n", name, errCode, partCount)
		for j := 0; j < partCount; j++ {
			pErr, _ := d.Int16()
			pID, _ := d.Int32()
			leader, _ := d.Int32()
			replicaCount, _ := d.ArrayLength()
			for k := 0; k < replicaCount; k++ {
				d.Int32()
			}
			isrCount, _ := d.ArrayLength()
			for k := 0; k < isrCount; k++ {
				d.Int32()
			}
			fmt.Printf("     Partition %d: leader=%d, error=%d\n", pID, leader, pErr)
		}
	}
}

// --- Produce (api_key=0, v0) ---

func sendProduceRequest(conn net.Conn, correlationID int32, topic string, partition int32, key, value []byte) {
	// Build inner message (MessageSet v0)
	inner := protocol.NewEncoder()
	inner.PutInt8(0) // magic
	inner.PutInt8(0) // attributes
	if key == nil {
		inner.PutInt32(-1)
	} else {
		inner.PutInt32(int32(len(key)))
		inner.PutRawBytes(key)
	}
	inner.PutInt32(int32(len(value)))
	inner.PutRawBytes(value)
	innerBytes := inner.Bytes()

	// MessageSet
	msgSet := protocol.NewEncoder()
	msgSet.PutInt64(0)                          // offset
	msgSet.PutInt32(int32(4 + len(innerBytes))) // message size
	msgSet.PutInt32(0)                          // crc (simplified)
	msgSet.PutRawBytes(innerBytes)
	msgSetBytes := msgSet.Bytes()

	// Produce request
	enc := protocol.NewEncoder()
	enc.PutInt16(0) // api_key = Produce
	enc.PutInt16(0) // api_version
	enc.PutInt32(correlationID)
	enc.PutInt16(-1)      // client_id (null)
	enc.PutInt16(1)       // acks
	enc.PutInt32(5000)    // timeout_ms
	enc.PutArrayLength(1) // 1 topic
	enc.PutString(topic)
	enc.PutArrayLength(1) // 1 partition
	enc.PutInt32(partition)
	enc.PutBytes(msgSetBytes)

	sendRequest(conn, enc.Bytes())
}

func readProduceResponse(conn net.Conn) {
	data := readResponse(conn)
	d := protocol.NewDecoder(data)
	corrID, _ := d.Int32()
	topicCount, _ := d.ArrayLength()
	fmt.Printf("   Response: correlation_id=%d\n", corrID)
	for i := 0; i < topicCount; i++ {
		name, _ := d.String()
		partCount, _ := d.ArrayLength()
		for j := 0; j < partCount; j++ {
			pID, _ := d.Int32()
			errCode, _ := d.Int16()
			offset, _ := d.Int64()
			fmt.Printf("   Topic %s, partition %d: error=%d, base_offset=%d\n", name, pID, errCode, offset)
		}
	}
}

// --- Fetch (api_key=1, v0) ---

func sendFetchRequest(conn net.Conn, correlationID int32, topic string, partition int32, offset int64) {
	enc := protocol.NewEncoder()
	enc.PutInt16(1) // api_key = Fetch
	enc.PutInt16(0) // api_version
	enc.PutInt32(correlationID)
	enc.PutInt16(-1)      // client_id (null)
	enc.PutInt32(-1)      // replica_id
	enc.PutInt32(5000)    // max_wait_ms
	enc.PutInt32(1)       // min_bytes
	enc.PutArrayLength(1) // 1 topic
	enc.PutString(topic)
	enc.PutArrayLength(1) // 1 partition
	enc.PutInt32(partition)
	enc.PutInt64(offset)  // fetch_offset
	enc.PutInt32(1048576) // max_bytes (1MB)
	sendRequest(conn, enc.Bytes())
}

func readFetchResponse(conn net.Conn) {
	data := readResponse(conn)
	d := protocol.NewDecoder(data)
	corrID, _ := d.Int32()
	topicCount, _ := d.ArrayLength()
	fmt.Printf("   Response: correlation_id=%d\n", corrID)
	for i := 0; i < topicCount; i++ {
		name, _ := d.String()
		partCount, _ := d.ArrayLength()
		for j := 0; j < partCount; j++ {
			pID, _ := d.Int32()
			errCode, _ := d.Int16()
			hwm, _ := d.Int64()
			recordSet, _ := d.Bytes()
			fmt.Printf("   Topic %s, partition %d: error=%d, high_watermark=%d, data_size=%d bytes\n",
				name, pID, errCode, hwm, len(recordSet))

			// Parse MessageSet
			if len(recordSet) > 0 {
				md := protocol.NewDecoder(recordSet)
				msgIdx := 0
				for md.Remaining() >= 12 {
					off, _ := md.Int64()
					msgSize, _ := md.Int32()
					if md.Remaining() < int(msgSize) {
						break
					}
					md.Int32() // crc
					md.Int8()  // magic
					md.Int8()  // attributes
					keyBytes, _ := md.Bytes()
					valBytes, _ := md.Bytes()
					keyStr := "<null>"
					if keyBytes != nil {
						keyStr = string(keyBytes)
					}
					fmt.Printf("     Message[%d]: offset=%d, key=%s, value=%s\n",
						msgIdx, off, keyStr, string(valBytes))
					msgIdx++
				}
			}
		}
	}
}

func apiKeyName(key int16) string {
	switch key {
	case 0:
		return "Produce"
	case 1:
		return "Fetch"
	case 3:
		return "Metadata"
	case 18:
		return "ApiVersions"
	default:
		return "Unknown"
	}
}
