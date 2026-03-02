package api

import (
	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// HandleProduce handles Produce requests (api_key=0, v0).
//
// Produce v0 Request:
//
//	acks (INT16)
//	timeout_ms (INT32)
//	[topic_data:
//	  topic_name (STRING)
//	  [partition_data:
//	    partition (INT32)
//	    record_set (BYTES)  -- MessageSet v0 format
//	  ]
//	]
//
// Produce v0 Response:
//
//	[responses:
//	  topic_name (STRING)
//	  [partition_responses:
//	    partition (INT32)
//	    error_code (INT16)
//	    base_offset (INT64)
//	  ]
//	]
func HandleProduce(header *protocol.RequestHeader, body *protocol.Decoder, b *broker.Broker) ([]byte, error) {
	// Parse request
	acks, err := body.Int16()
	if err != nil {
		return nil, err
	}
	_ = acks // We always acknowledge for now

	_, err = body.Int32() // timeout_ms
	if err != nil {
		return nil, err
	}

	topicCount, err := body.ArrayLength()
	if err != nil {
		return nil, err
	}

	type partitionResult struct {
		partition  int32
		errCode    int16
		baseOffset int64
	}
	type topicResult struct {
		name       string
		partitions []partitionResult
	}

	results := make([]topicResult, 0, topicCount)

	for i := 0; i < topicCount; i++ {
		topicName, err := body.String()
		if err != nil {
			return nil, err
		}

		partCount, err := body.ArrayLength()
		if err != nil {
			return nil, err
		}

		topic := b.TopicManager.GetOrCreateTopic(topicName)
		pResults := make([]partitionResult, 0, partCount)

		for j := 0; j < partCount; j++ {
			partitionID, err := body.Int32()
			if err != nil {
				return nil, err
			}

			// Read the record set (MessageSet as raw bytes)
			recordSet, err := body.Bytes()
			if err != nil {
				return nil, err
			}

			// Check partition exists
			if int(partitionID) >= len(topic.Partitions) || partitionID < 0 {
				pResults = append(pResults, partitionResult{
					partition:  partitionID,
					errCode:    protocol.ErrUnknownTopicOrPartition,
					baseOffset: -1,
				})
				continue
			}

			partition := topic.Partitions[partitionID]

			// Parse the MessageSet v0 format and extract messages
			messages := parseMessageSetV0(recordSet)
			var baseOffset int64 = -1
			for idx, msg := range messages {
				off := partition.Append(msg.key, msg.value)
				if idx == 0 {
					baseOffset = off
				}
			}

			if len(messages) == 0 {
				// No valid messages, just store the raw bytes as a single message
				baseOffset = partition.Append(nil, recordSet)
			}

			pResults = append(pResults, partitionResult{
				partition:  partitionID,
				errCode:    protocol.ErrNone,
				baseOffset: baseOffset,
			})
		}

		results = append(results, topicResult{
			name:       topicName,
			partitions: pResults,
		})
	}

	// Encode response
	enc := protocol.NewEncoder()
	protocol.EncodeResponseHeader(enc, header.CorrelationID)

	enc.PutArrayLength(len(results))
	for _, tr := range results {
		enc.PutString(tr.name)
		enc.PutArrayLength(len(tr.partitions))
		for _, pr := range tr.partitions {
			enc.PutInt32(pr.partition)
			enc.PutInt16(pr.errCode)
			enc.PutInt64(pr.baseOffset)
		}
	}

	return enc.Bytes(), nil
}

// rawMessage is a parsed message from a MessageSet.
type rawMessage struct {
	key   []byte
	value []byte
}

// parseMessageSetV0 parses the Kafka MessageSet v0 binary format.
//
// MessageSet v0:
//
//	offset (INT64)
//	message_size (INT32)
//	Message:
//	  crc (INT32)
//	  magic (INT8) = 0
//	  attributes (INT8)
//	  key (BYTES)
//	  value (BYTES)
func parseMessageSetV0(data []byte) []rawMessage {
	if len(data) == 0 {
		return nil
	}

	d := protocol.NewDecoder(data)
	var messages []rawMessage

	for d.Remaining() >= 12 { // minimum: 8 (offset) + 4 (message_size)
		_, err := d.Int64() // offset (client-set, we ignore)
		if err != nil {
			break
		}

		msgSize, err := d.Int32() // message_size
		if err != nil {
			break
		}

		if d.Remaining() < int(msgSize) || msgSize < 6 {
			break
		}

		// CRC (4 bytes) - we skip validation for MVP
		_, err = d.Int32()
		if err != nil {
			break
		}

		// Magic byte
		_, err = d.Int8()
		if err != nil {
			break
		}

		// Attributes
		_, err = d.Int8()
		if err != nil {
			break
		}

		// Key
		key, err := d.Bytes()
		if err != nil {
			break
		}

		// Value
		value, err := d.Bytes()
		if err != nil {
			break
		}

		messages = append(messages, rawMessage{key: key, value: value})
	}

	return messages
}
