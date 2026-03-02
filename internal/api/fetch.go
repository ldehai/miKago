package api

import (
	"hash/crc32"

	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// HandleFetch handles Fetch requests (api_key=1, v0).
//
// Fetch v0 Request:
//
//	replica_id (INT32)     -- -1 for consumers
//	max_wait_ms (INT32)
//	min_bytes (INT32)
//	[topics:
//	  topic_name (STRING)
//	  [partitions:
//	    partition (INT32)
//	    fetch_offset (INT64)
//	    max_bytes (INT32)
//	  ]
//	]
//
// Fetch v0 Response:
//
//	[responses:
//	  topic_name (STRING)
//	  [partition_responses:
//	    partition (INT32)
//	    error_code (INT16)
//	    high_watermark (INT64)
//	    record_set (BYTES)  -- MessageSet v0 format
//	  ]
//	]
func HandleFetch(header *protocol.RequestHeader, body *protocol.Decoder, b *broker.Broker) ([]byte, error) {
	// Parse request
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

	enc.PutArrayLength(len(requests))
	for _, tf := range requests {
		enc.PutString(tf.name)

		topic := b.TopicManager.GetTopic(tf.name)

		enc.PutArrayLength(len(tf.partitions))
		for _, pf := range tf.partitions {
			enc.PutInt32(pf.partition)

			if topic == nil || int(pf.partition) >= len(topic.Partitions) || pf.partition < 0 {
				enc.PutInt16(protocol.ErrUnknownTopicOrPartition)
				enc.PutInt64(0)   // high watermark
				enc.PutBytes(nil) // empty record set
				continue
			}

			partition := topic.Partitions[pf.partition]
			messages, hwm := partition.Fetch(pf.fetchOffset, pf.maxBytes)

			enc.PutInt16(protocol.ErrNone)
			enc.PutInt64(hwm)

			// Encode messages as MessageSet v0
			messageSetBytes := encodeMessageSetV0(messages)
			enc.PutBytes(messageSetBytes)
		}
	}

	return enc.Bytes(), nil
}

// encodeMessageSetV0 encodes messages in Kafka MessageSet v0 format.
//
// MessageSet v0:
//
//	offset (INT64)
//	message_size (INT32)
//	Message:
//	  crc (INT32)
//	  magic (INT8) = 0
//	  attributes (INT8) = 0
//	  key (BYTES)
//	  value (BYTES)
func encodeMessageSetV0(messages []broker.Message) []byte {
	if len(messages) == 0 {
		return nil
	}

	enc := protocol.NewEncoder()

	for _, msg := range messages {
		// Build the inner message first to compute CRC
		inner := protocol.NewEncoder()
		inner.PutInt8(0) // magic
		inner.PutInt8(0) // attributes

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

		// Offset
		enc.PutInt64(msg.Offset)

		// Message size (4 bytes CRC + inner message)
		enc.PutInt32(int32(4 + len(innerBytes)))

		// CRC
		enc.PutInt32(int32(crc))

		// Inner message bytes
		enc.PutRawBytes(innerBytes)
	}

	return enc.Bytes()
}
