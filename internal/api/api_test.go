package api

import (
	"testing"

	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

func newTestBroker(t *testing.T) *broker.Broker {
	t.Helper()
	b := broker.NewBroker(broker.Config{
		BrokerID: 0,
		Host:     "localhost",
		Port:     9092,
		DataDir:  t.TempDir(),
	})
	t.Cleanup(func() { b.Close() })
	return b
}

func TestApiVersions(t *testing.T) {
	b := newTestBroker(t)

	// Build a ApiVersions v0 request body (empty)
	header := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyApiVersions,
		APIVersion:    0,
		CorrelationID: 42,
	}

	body := protocol.NewDecoder(nil)
	resp, err := HandleApiVersions(header, body, b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Parse response
	d := protocol.NewDecoder(resp)

	// correlation_id
	corrID, _ := d.Int32()
	if corrID != 42 {
		t.Fatalf("expected correlation_id=42, got %d", corrID)
	}

	// error_code
	errCode, _ := d.Int16()
	if errCode != 0 {
		t.Fatalf("expected error_code=0, got %d", errCode)
	}

	// api_versions array
	count, _ := d.ArrayLength()
	if count != len(protocol.SupportedAPIVersions) {
		t.Fatalf("expected %d api versions, got %d", len(protocol.SupportedAPIVersions), count)
	}

	for i := 0; i < count; i++ {
		apiKey, _ := d.Int16()
		minVer, _ := d.Int16()
		maxVer, _ := d.Int16()
		t.Logf("API key=%d, min=%d, max=%d", apiKey, minVer, maxVer)
	}
}

func TestMetadata(t *testing.T) {
	b := newTestBroker(t)

	// Build Metadata v0 request: 1 topic named "test-topic"
	reqEnc := protocol.NewEncoder()
	reqEnc.PutArrayLength(1)
	reqEnc.PutString("test-topic")

	header := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyMetadata,
		APIVersion:    0,
		CorrelationID: 7,
	}

	body := protocol.NewDecoder(reqEnc.Bytes())
	resp, err := HandleMetadata(header, body, b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Parse response
	d := protocol.NewDecoder(resp)

	corrID, _ := d.Int32()
	if corrID != 7 {
		t.Fatalf("expected correlation_id=7, got %d", corrID)
	}

	// Brokers
	brokerCount, _ := d.ArrayLength()
	if brokerCount != 1 {
		t.Fatalf("expected 1 broker, got %d", brokerCount)
	}
	nodeID, _ := d.Int32()
	host, _ := d.String()
	port, _ := d.Int32()
	t.Logf("Broker: id=%d, host=%s, port=%d", nodeID, host, port)

	// Topics
	topicCount, _ := d.ArrayLength()
	if topicCount != 1 {
		t.Fatalf("expected 1 topic, got %d", topicCount)
	}

	topicErr, _ := d.Int16()
	if topicErr != 0 {
		t.Fatalf("expected no topic error, got %d", topicErr)
	}
	topicName, _ := d.String()
	if topicName != "test-topic" {
		t.Fatalf("expected topic 'test-topic', got '%s'", topicName)
	}

	// Partitions
	partCount, _ := d.ArrayLength()
	if partCount != 1 {
		t.Fatalf("expected 1 partition, got %d", partCount)
	}

	partErr, _ := d.Int16()
	partID, _ := d.Int32()
	leader, _ := d.Int32()
	t.Logf("Partition: err=%d, id=%d, leader=%d", partErr, partID, leader)
}

func TestProduceThenFetch(t *testing.T) {
	b := newTestBroker(t)

	// First, produce a message using MessageSet v0 format
	// Build the inner message
	innerEnc := protocol.NewEncoder()
	innerEnc.PutInt8(0)   // magic
	innerEnc.PutInt8(0)   // attributes
	innerEnc.PutInt32(-1) // null key
	value := []byte("hello miKago")
	innerEnc.PutInt32(int32(len(value)))
	innerEnc.PutRawBytes(value)
	innerBytes := innerEnc.Bytes()

	// Build MessageSet
	msgSetEnc := protocol.NewEncoder()
	msgSetEnc.PutInt64(0)                          // offset (ignored by broker)
	msgSetEnc.PutInt32(int32(4 + len(innerBytes))) // message size (crc + inner)
	msgSetEnc.PutInt32(0)                          // crc (we skip validation)
	msgSetEnc.PutRawBytes(innerBytes)
	msgSetBytes := msgSetEnc.Bytes()

	// Build Produce v0 request body
	produceReq := protocol.NewEncoder()
	produceReq.PutInt16(1)    // acks
	produceReq.PutInt32(1000) // timeout_ms
	produceReq.PutArrayLength(1)
	produceReq.PutString("test-topic")
	produceReq.PutArrayLength(1)
	produceReq.PutInt32(0) // partition 0
	produceReq.PutBytes(msgSetBytes)

	header := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyProduce,
		APIVersion:    0,
		CorrelationID: 10,
	}

	body := protocol.NewDecoder(produceReq.Bytes())
	resp, err := HandleProduce(header, body, b)
	if err != nil {
		t.Fatalf("produce error: %v", err)
	}

	// Parse produce response
	d := protocol.NewDecoder(resp)
	corrID, _ := d.Int32()
	if corrID != 10 {
		t.Fatalf("expected correlation_id=10, got %d", corrID)
	}
	topicCount, _ := d.ArrayLength()
	if topicCount != 1 {
		t.Fatalf("expected 1 topic response, got %d", topicCount)
	}
	topicName, _ := d.String()
	if topicName != "test-topic" {
		t.Fatalf("expected 'test-topic', got '%s'", topicName)
	}
	partCount, _ := d.ArrayLength()
	if partCount != 1 {
		t.Fatalf("expected 1 partition response, got %d", partCount)
	}
	partID, _ := d.Int32()
	errCode, _ := d.Int16()
	baseOffset, _ := d.Int64()
	t.Logf("Produce response: partition=%d, error=%d, baseOffset=%d", partID, errCode, baseOffset)

	if errCode != 0 {
		t.Fatalf("expected no error, got %d", errCode)
	}
	if baseOffset != 0 {
		t.Fatalf("expected baseOffset=0, got %d", baseOffset)
	}

	// Now fetch the message
	fetchReq := protocol.NewEncoder()
	fetchReq.PutInt32(-1)   // replica_id (-1 for consumer)
	fetchReq.PutInt32(5000) // max_wait_ms
	fetchReq.PutInt32(1)    // min_bytes
	fetchReq.PutArrayLength(1)
	fetchReq.PutString("test-topic")
	fetchReq.PutArrayLength(1)
	fetchReq.PutInt32(0)       // partition 0
	fetchReq.PutInt64(0)       // fetch_offset
	fetchReq.PutInt32(1048576) // max_bytes (1MB)

	fetchHeader := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyFetch,
		APIVersion:    0,
		CorrelationID: 11,
	}

	fetchBody := protocol.NewDecoder(fetchReq.Bytes())
	fetchResp, err := HandleFetch(fetchHeader, fetchBody, b)
	if err != nil {
		t.Fatalf("fetch error: %v", err)
	}

	// Parse fetch response
	fd := protocol.NewDecoder(fetchResp)
	corrID, _ = fd.Int32()
	if corrID != 11 {
		t.Fatalf("expected correlation_id=11, got %d", corrID)
	}

	topicCount, _ = fd.ArrayLength()
	if topicCount != 1 {
		t.Fatalf("expected 1 topic, got %d", topicCount)
	}
	topicName, _ = fd.String()
	partCount, _ = fd.ArrayLength()
	partID, _ = fd.Int32()
	errCode, _ = fd.Int16()
	hwm, _ := fd.Int64()
	t.Logf("Fetch response: topic=%s, partition=%d, error=%d, hwm=%d", topicName, partID, errCode, hwm)

	// Record set (MessageSet)
	recordSet, _ := fd.Bytes()
	if len(recordSet) == 0 {
		t.Fatal("expected non-empty record set")
	}

	// Parse the MessageSet to verify our message
	msgD := protocol.NewDecoder(recordSet)
	offset, _ := msgD.Int64()
	msgSize, _ := msgD.Int32()
	t.Logf("Message: offset=%d, size=%d", offset, msgSize)

	if offset != 0 {
		t.Fatalf("expected offset=0, got %d", offset)
	}

	// Skip CRC
	_, _ = msgD.Int32()
	// Magic
	magic, _ := msgD.Int8()
	if magic != 1 {
		t.Fatalf("expected magic=1, got %d", magic)
	}
	// Attributes
	_, _ = msgD.Int8()
	// Timestamp (v1)
	_, _ = msgD.Int64()
	// Key (should be null)
	keyBytes, _ := msgD.Bytes()
	if keyBytes != nil {
		t.Fatalf("expected nil key, got %v", keyBytes)
	}
	// Value
	valueBytes, _ := msgD.Bytes()
	if string(valueBytes) != "hello miKago" {
		t.Fatalf("expected 'hello miKago', got '%s'", string(valueBytes))
	}

	t.Log("✅ Produce → Fetch round-trip successful!")
}
