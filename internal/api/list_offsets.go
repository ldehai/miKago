package api

import (
	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// HandleListOffsets handles ListOffsets requests (api_key=2, v0-v1).
//
// v1 Request:
//
//	replica_id (INT32)
//	[topics:
//	  topic_name (STRING)
//	  [partitions:
//	    partition (INT32)
//	    timestamp (INT64)   -- -1 = latest, -2 = earliest
//	  ]
//	]
//
// v1 Response:
//
//	[responses:
//	  topic_name (STRING)
//	  [partition_responses:
//	    partition (INT32)
//	    error_code (INT16)
//	    timestamp (INT64)
//	    offset (INT64)
//	  ]
//	]
func HandleListOffsets(header *protocol.RequestHeader, body *protocol.Decoder, b *broker.Broker) (protocol.Payload, error) {
	_, err := body.Int32() // replica_id
	if err != nil {
		return nil, err
	}

	topicCount, err := body.ArrayLength()
	if err != nil {
		return nil, err
	}

	type partReq struct {
		partition int32
		timestamp int64
	}
	type topicReq struct {
		name       string
		partitions []partReq
	}

	requests := make([]topicReq, 0, topicCount)

	for i := 0; i < topicCount; i++ {
		topicName, err := body.String()
		if err != nil {
			return nil, err
		}

		partCount, err := body.ArrayLength()
		if err != nil {
			return nil, err
		}

		partitions := make([]partReq, 0, partCount)
		for j := 0; j < partCount; j++ {
			partition, err := body.Int32()
			if err != nil {
				return nil, err
			}

			ts, err := body.Int64()
			if err != nil {
				return nil, err
			}

			// v0 has an extra max_num_offsets field
			if header.APIVersion == 0 {
				_, err = body.Int32() // max_num_offsets
				if err != nil {
					return nil, err
				}
			}

			partitions = append(partitions, partReq{
				partition: partition,
				timestamp: ts,
			})
		}

		requests = append(requests, topicReq{
			name:       topicName,
			partitions: partitions,
		})
	}

	// Build response
	enc := protocol.GetEncoder()
	protocol.EncodeResponseHeader(enc, header.CorrelationID)

	enc.PutArrayLength(len(requests))
	for _, tr := range requests {
		enc.PutString(tr.name)

		topic := b.TopicManager.GetTopic(tr.name)

		enc.PutArrayLength(len(tr.partitions))
		for _, pr := range tr.partitions {
			enc.PutInt32(pr.partition)

			if topic == nil || int(pr.partition) >= len(topic.Partitions) || pr.partition < 0 {
				enc.PutInt16(protocol.ErrUnknownTopicOrPartition)
				if header.APIVersion >= 1 {
					enc.PutInt64(-1) // timestamp
					enc.PutInt64(-1) // offset
				} else {
					// v0: array of offsets
					enc.PutArrayLength(0)
				}
				continue
			}

			partition := topic.Partitions[pr.partition]
			hwm := partition.HighWaterMark()

			enc.PutInt16(protocol.ErrNone)

			if header.APIVersion >= 1 {
				// v1 response: timestamp + offset
				var offset int64
				switch pr.timestamp {
				case -1: // Latest
					offset = hwm
				case -2: // Earliest
					offset = 0
				default:
					// For a specific timestamp, return the earliest offset
					// In our MVP, just return 0
					offset = 0
				}
				enc.PutInt64(-1)     // timestamp (we don't track per-message timestamps for lookup)
				enc.PutInt64(offset) // offset
			} else {
				// v0 response: array of offsets
				switch pr.timestamp {
				case -1: // Latest
					enc.PutArrayLength(1)
					enc.PutInt64(hwm)
				case -2: // Earliest
					enc.PutArrayLength(1)
					enc.PutInt64(0)
				default:
					enc.PutArrayLength(0)
				}
			}
		}
	}

	return protocol.EncoderPayload{Encoder: enc}, nil
}
