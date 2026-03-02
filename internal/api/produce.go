package api

import (
	"time"

	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// HandleProduce handles Produce requests (api_key=0, v0-v2).
//
// v0/v1/v2 Request format is the same:
//
//	acks (INT16), timeout_ms (INT32),
//	[topic_data: topic_name, [partition_data: partition, record_set (BYTES)]]
//
// v0 Response: [responses: topic, [partitions: partition, error_code, base_offset]]
// v2 Response: [responses: topic, [partitions: partition, error_code, base_offset, log_append_time]] + throttle_time_ms
//
// Messages inside record_set use MessageSet format:
//
//	v0 (magic=0): offset, size, crc, magic, attributes, key, value
//	v1 (magic=1): offset, size, crc, magic, attributes, timestamp, key, value
func HandleProduce(header *protocol.RequestHeader, body *protocol.Decoder, b *broker.Broker) ([]byte, error) {
	// Parse request
	acks, err := body.Int16()
	if err != nil {
		return nil, err
	}
	_ = acks

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
		appendTime int64
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

			recordSet, err := body.Bytes()
			if err != nil {
				return nil, err
			}

			// Check message size limit
			if int32(len(recordSet)) > b.Config.MaxMessageBytes {
				pResults = append(pResults, partitionResult{
					partition:  partitionID,
					errCode:    protocol.ErrMessageTooLarge,
					baseOffset: -1,
					appendTime: -1,
				})
				continue
			}

			if int(partitionID) >= len(topic.Partitions) || partitionID < 0 {
				pResults = append(pResults, partitionResult{
					partition:  partitionID,
					errCode:    protocol.ErrUnknownTopicOrPartition,
					baseOffset: -1,
					appendTime: -1,
				})
				continue
			}

			partition := topic.Partitions[partitionID]
			messages := parseMessageSet(recordSet)
			var baseOffset int64 = -1
			for idx, msg := range messages {
				off := partition.Append(msg.key, msg.value)
				if idx == 0 {
					baseOffset = off
				}
			}

			if len(messages) == 0 {
				baseOffset = partition.Append(nil, recordSet)
			}

			pResults = append(pResults, partitionResult{
				partition:  partitionID,
				errCode:    protocol.ErrNone,
				baseOffset: baseOffset,
				appendTime: time.Now().UnixMilli(),
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
			// v2+: log_append_time (INT64)
			if header.APIVersion >= 2 {
				enc.PutInt64(pr.appendTime)
			}
		}
	}

	// v2+: throttle_time_ms
	if header.APIVersion >= 2 {
		enc.PutInt32(0)
	}

	return enc.Bytes(), nil
}

// rawMessage is a parsed message from a MessageSet.
type rawMessage struct {
	key   []byte
	value []byte
}

// parseMessageSet parses Kafka MessageSet binary format (magic 0 and 1).
//
// magic=0: offset(8), message_size(4), crc(4), magic(1), attributes(1), key(BYTES), value(BYTES)
// magic=1: offset(8), message_size(4), crc(4), magic(1), attributes(1), timestamp(8), key(BYTES), value(BYTES)
func parseMessageSet(data []byte) []rawMessage {
	if len(data) == 0 {
		return nil
	}

	d := protocol.NewDecoder(data)
	var messages []rawMessage

	for d.Remaining() >= 12 {
		_, err := d.Int64() // offset
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

		_, err = d.Int32() // CRC
		if err != nil {
			break
		}

		magic, err := d.Int8() // magic byte
		if err != nil {
			break
		}

		_, err = d.Int8() // attributes
		if err != nil {
			break
		}

		// magic=1 has a timestamp field
		if magic >= 1 {
			_, err = d.Int64() // timestamp
			if err != nil {
				break
			}
		}

		key, err := d.Bytes()
		if err != nil {
			break
		}

		value, err := d.Bytes()
		if err != nil {
			break
		}

		messages = append(messages, rawMessage{key: key, value: value})
	}

	return messages
}
