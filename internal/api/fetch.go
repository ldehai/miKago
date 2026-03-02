package api

import (
	"hash/crc32"
	"time"

	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// HandleFetch handles Fetch requests (api_key=1, v0-v2).
//
// v0 Request: replica_id, max_wait_ms, min_bytes, [topics: topic, [partitions: partition, offset, max_bytes]]
// v2 Request: same as v0 (no extra fields)
//
// v0 Response: [responses: topic, [partition_responses: partition, error, hwm, record_set]]
// v2 Response: throttle_time_ms + same as v0
func HandleFetch(header *protocol.RequestHeader, body *protocol.Decoder, b *broker.Broker) ([]byte, error) {
	_, err := body.Int32() // replica_id
	if err != nil {
		return nil, err
	}
	_, err = body.Int32() // max_wait_ms
	if err != nil {
		return nil, err
	}
	_, err = body.Int32() // min_bytes
	if err != nil {
		return nil, err
	}

	topicCount, err := body.ArrayLength()
	if err != nil {
		return nil, err
	}

	type partitionFetch struct {
		partition   int32
		fetchOffset int64
		maxBytes    int32
	}
	type topicFetch struct {
		name       string
		partitions []partitionFetch
	}

	requests := make([]topicFetch, 0, topicCount)

	for i := 0; i < topicCount; i++ {
		topicName, err := body.String()
		if err != nil {
			return nil, err
		}

		partCount, err := body.ArrayLength()
		if err != nil {
			return nil, err
		}

		partitions := make([]partitionFetch, 0, partCount)
		for j := 0; j < partCount; j++ {
			partition, err := body.Int32()
			if err != nil {
				return nil, err
			}
			fetchOffset, err := body.Int64()
			if err != nil {
				return nil, err
			}
			maxBytes, err := body.Int32()
			if err != nil {
				return nil, err
			}
			partitions = append(partitions, partitionFetch{
				partition:   partition,
				fetchOffset: fetchOffset,
				maxBytes:    maxBytes,
			})
		}

		requests = append(requests, topicFetch{
			name:       topicName,
			partitions: partitions,
		})
	}

	// Build response
	enc := protocol.NewEncoder()
	protocol.EncodeResponseHeader(enc, header.CorrelationID)

	// v2+: throttle_time_ms at the beginning
	if header.APIVersion >= 2 {
		enc.PutInt32(0) // throttle_time_ms
	}

	enc.PutArrayLength(len(requests))
	for _, tf := range requests {
		enc.PutString(tf.name)

		topic := b.TopicManager.GetTopic(tf.name)

		enc.PutArrayLength(len(tf.partitions))
		for _, pf := range tf.partitions {
			enc.PutInt32(pf.partition)

			if topic == nil || int(pf.partition) >= len(topic.Partitions) || pf.partition < 0 {
				enc.PutInt16(protocol.ErrUnknownTopicOrPartition)
				enc.PutInt64(0)
				enc.PutBytes(nil)
				continue
			}

			partition := topic.Partitions[pf.partition]
			messages, hwm := partition.Fetch(pf.fetchOffset, pf.maxBytes)

			enc.PutInt16(protocol.ErrNone)
			enc.PutInt64(hwm)

			// Encode messages as MessageSet v1 (magic=1, with timestamp)
			messageSetBytes := encodeMessageSetV1(messages)
			enc.PutBytes(messageSetBytes)
		}
	}

	return enc.Bytes(), nil
}

// encodeMessageSetV1 encodes messages in Kafka MessageSet v1 format (magic=1).
//
// MessageSet v1:
//
//	offset (INT64)
//	message_size (INT32)
//	Message:
//	  crc (INT32)
//	  magic (INT8) = 1
//	  attributes (INT8) = 0
//	  timestamp (INT64)
//	  key (BYTES)
//	  value (BYTES)
func encodeMessageSetV1(messages []broker.Message) []byte {
	if len(messages) == 0 {
		return nil
	}

	enc := protocol.NewEncoder()

	for _, msg := range messages {
		inner := protocol.NewEncoder()
		inner.PutInt8(1) // magic = 1 (v1, has timestamp)
		inner.PutInt8(0) // attributes

		// Timestamp (milliseconds since epoch)
		inner.PutInt64(msg.Timestamp.UnixMilli())

		// Key
		if msg.Key == nil {
			inner.PutInt32(-1)
		} else {
			inner.PutInt32(int32(len(msg.Key)))
			inner.PutRawBytes(msg.Key)
		}

		// Value
		if msg.Value == nil {
			inner.PutInt32(-1)
		} else {
			inner.PutInt32(int32(len(msg.Value)))
			inner.PutRawBytes(msg.Value)
		}

		innerBytes := inner.Bytes()
		crc := crc32.ChecksumIEEE(innerBytes)

		enc.PutInt64(msg.Offset)
		enc.PutInt32(int32(4 + len(innerBytes)))
		enc.PutInt32(int32(crc))
		enc.PutRawBytes(innerBytes)
	}

	return enc.Bytes()
}

// timestamp returns milliseconds since epoch, used by Kafka protocol
func timestamp(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}
