package api

import (
	"bytes"
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

func payloadToBytes(t *testing.T, p protocol.Payload) []byte {
	t.Helper()
	var buf bytes.Buffer
	_, err := p.WriteTo(&buf)
	if err != nil {
		t.Fatalf("failed to write payload to buffer: %v", err)
	}
	p.Release()
	return buf.Bytes()
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
	d := protocol.NewDecoder(payloadToBytes(t, resp))

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
	d := protocol.NewDecoder(payloadToBytes(t, resp))

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
	d := protocol.NewDecoder(payloadToBytes(t, resp))
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
	fetchRespPayload, err := HandleFetchZeroCopy(fetchHeader, fetchBody, b)
	if err != nil {
		t.Fatalf("fetch error: %v", err)
	}

	var fetchBuf bytes.Buffer
	fetchRespPayload.WriteTo(&fetchBuf)
	fetchRespPayload.Release()
	fetchResp := fetchBuf.Bytes()

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

func TestConsumerGroupAPIs(t *testing.T) {
	b := newTestBroker(t)

	// 1. Create a Topic manually for test
	_, err := b.TopicManager.CreateTopic("group-topic", 1)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	groupID := "test-cg"
	topicName := "group-topic"
	var partition int32 = 0
	var commitOffset int64 = 42

	// --- Test OffsetCommit ---
	commitEnc := protocol.NewEncoder()
	commitEnc.PutString(groupID)
	commitEnc.PutArrayLength(1) // 1 topic
	commitEnc.PutString(topicName)
	commitEnc.PutArrayLength(1) // 1 partition
	commitEnc.PutInt32(partition)
	commitEnc.PutInt64(commitOffset)
	commitEnc.PutString("some-metadata")

	commitHeader := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyOffsetCommit,
		APIVersion:    0,
		CorrelationID: 101,
	}

	commitResp, err := HandleOffsetCommit(commitHeader, protocol.NewDecoder(commitEnc.Bytes()), b)
	if err != nil {
		t.Fatalf("HandleOffsetCommit error: %v", err)
	}

	d := protocol.NewDecoder(payloadToBytes(t, commitResp))
	corrID, _ := d.Int32()
	if corrID != 101 {
		t.Fatalf("expected correlation_id=101, got %d", corrID)
	}
	topicCount, _ := d.ArrayLength()
	if topicCount != 1 {
		t.Fatalf("expected 1 topic response, got %d", topicCount)
	}
	tName, _ := d.String()
	if tName != topicName {
		t.Fatalf("expected topic %q, got %q", topicName, tName)
	}
	partCount, _ := d.ArrayLength()
	if partCount != 1 {
		t.Fatalf("expected 1 partition response, got %d", partCount)
	}
	pID, _ := d.Int32()
	errCode, _ := d.Int16()
	if pID != partition || errCode != protocol.ErrNone {
		t.Fatalf("expected partition=%d errCode=0, got %d, %d", partition, pID, errCode)
	}

	// --- Test OffsetFetch ---
	fetchEnc := protocol.NewEncoder()
	fetchEnc.PutString(groupID)
	fetchEnc.PutArrayLength(1) // 1 topic
	fetchEnc.PutString(topicName)
	fetchEnc.PutArrayLength(1) // 1 partition
	fetchEnc.PutInt32(partition)

	fetchHeader := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyOffsetFetch,
		APIVersion:    1,
		CorrelationID: 102,
	}

	fetchResp, err := HandleOffsetFetch(fetchHeader, protocol.NewDecoder(fetchEnc.Bytes()), b)
	if err != nil {
		t.Fatalf("HandleOffsetFetch error: %v", err)
	}

	d = protocol.NewDecoder(payloadToBytes(t, fetchResp))
	corrID, _ = d.Int32()
	if corrID != 102 {
		t.Fatalf("expected correlation_id=102, got %d", corrID)
	}
	topicCount, _ = d.ArrayLength()
	if topicCount != 1 {
		t.Fatalf("expected 1 topic response, got %d", topicCount)
	}
	tName, _ = d.String()
	if tName != topicName {
		t.Fatalf("expected topic %q, got %q", topicName, tName)
	}
	partCount, _ = d.ArrayLength()
	if partCount != 1 {
		t.Fatalf("expected 1 partition response, got %d", partCount)
	}
	pID, _ = d.Int32()
	fetchedOffset, _ := d.Int64()
	_, _ = d.String() // metadata
	errCode, _ = d.Int16()

	if pID != partition || errCode != protocol.ErrNone || fetchedOffset != commitOffset {
		t.Fatalf("expected partition=%d errCode=0 offset=%d, got p=%d err=%d off=%d",
			partition, commitOffset, pID, errCode, fetchedOffset)
	}

	t.Log("✅ OffsetCommit -> OffsetFetch round-trip successful!")

	// --- Test FindCoordinator ---
	fcEnc := protocol.NewEncoder()
	fcEnc.PutString(groupID)

	fcHeader := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyFindCoordinator,
		APIVersion:    0,
		CorrelationID: 103,
	}

	fcResp, err := HandleFindCoordinator(fcHeader, protocol.NewDecoder(fcEnc.Bytes()), b)
	if err != nil {
		t.Fatalf("HandleFindCoordinator error: %v", err)
	}

	d = protocol.NewDecoder(payloadToBytes(t, fcResp))
	corrID, _ = d.Int32()
	if corrID != 103 {
		t.Fatalf("expected correlation_id=103, got %d", corrID)
	}
	errCode, _ = d.Int16()
	if errCode != protocol.ErrNone {
		t.Fatalf("expected ErrNone, got %d", errCode)
	}
	nodeID, _ := d.Int32()
	host, _ := d.String()
	port, _ := d.Int32()

	if nodeID != b.Config.BrokerID || host != b.Config.Host || port != b.Config.Port {
		t.Fatalf("expected broker %d %s:%d, got %d %s:%d",
			b.Config.BrokerID, b.Config.Host, b.Config.Port, nodeID, host, port)
	}

	t.Log("✅ FindCoordinator successful!")
}

func TestCreateTopicsAPI(t *testing.T) {
	b := newTestBroker(t)

	// Create request
	enc := protocol.NewEncoder()
	enc.PutArrayLength(2)

	// Topic 1: 3 partitions
	enc.PutString("multi-part-topic")
	enc.PutInt32(3)
	enc.PutInt16(1)       // replication factor
	enc.PutArrayLength(0) // assignments
	enc.PutArrayLength(0) // configs

	// Topic 2: invalid partitions
	enc.PutString("invalid-topic")
	enc.PutInt32(0)
	enc.PutInt16(1)
	enc.PutArrayLength(0) // assignments
	enc.PutArrayLength(0) // configs

	enc.PutInt32(5000) // timeout ms

	header := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyCreateTopics,
		APIVersion:    0,
		CorrelationID: 200,
	}

	respBytes, err := HandleCreateTopics(header, protocol.NewDecoder(enc.Bytes()), b)
	if err != nil {
		t.Fatalf("HandleCreateTopics error: %v", err)
	}

	d := protocol.NewDecoder(payloadToBytes(t, respBytes))
	corrID, _ := d.Int32()
	if corrID != 200 {
		t.Fatalf("expected correlation_id=200, got %d", corrID)
	}

	topicCount, _ := d.ArrayLength()
	if topicCount != 2 {
		t.Fatalf("expected 2 topic responses, got %d", topicCount)
	}

	// Topic 1 response
	name1, _ := d.String()
	err1, _ := d.Int16()
	if name1 != "multi-part-topic" || err1 != protocol.ErrNone {
		t.Fatalf("expected multi-part-topic success, got name=%s err=%d", name1, err1)
	}

	// Topic 2 response
	name2, _ := d.String()
	err2, _ := d.Int16()
	if name2 != "invalid-topic" || err2 != protocol.ErrInvalidPartitions {
		t.Fatalf("expected invalid-topic ErrInvalidPartitions, got name=%s err=%d", name2, err2)
	}

	// Verify topic actually created
	topic := b.TopicManager.GetTopic("multi-part-topic")
	if topic == nil {
		t.Fatalf("topic not found in manager")
	}
	if len(topic.Partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(topic.Partitions))
	}

	t.Log("✅ CreateTopics API successful!")
}
