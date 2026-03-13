package api

import (
	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// HandleFetchZeroCopy handles Fetch requests with zero-copy 'sendfile' mechanism.
func HandleFetchZeroCopy(header *protocol.RequestHeader, body *protocol.Decoder, b *broker.Broker) (protocol.Payload, error) {
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

	mp := protocol.MultiPayload{}

	// Initial header encoder
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
				enc.PutInt64(0)   // hwm
				enc.PutInt32(0) // message set size (bytes length)
				continue
			}

			partition := topic.Partitions[pf.partition]
			file, fileOff, bytesToRead, hwm := partition.FetchZeroCopy(pf.fetchOffset, pf.maxBytes)

			enc.PutInt16(protocol.ErrNone)
			enc.PutInt64(hwm)
			enc.PutInt32(bytesToRead) // size of the upcoming byte array

			if bytesToRead > 0 && file != nil {
				// Commit the current encoder to the multi payload
				mp.Payloads = append(mp.Payloads, protocol.EncoderPayload{Encoder: enc})
				
				// Append the File Payload for Zero Copy
				mp.Payloads = append(mp.Payloads, protocol.FilePayload{
					File:   file,
					Offset: fileOff,
					Length: int64(bytesToRead),
				})
				
				// Create a new encoder for any following partitions/topics
				enc = protocol.NewEncoder()
			}
		}
	}

	// Always append the final encoder (might be empty, but EncoderPayload handles it)
	if enc.Len() > 0 {
		mp.Payloads = append(mp.Payloads, protocol.EncoderPayload{Encoder: enc})
	}

	return mp, nil
}
