package api

import (
	"fmt"
	"log"

	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// HandleOffsetCommit processes an OffsetCommit v0 request (API key 8).
func HandleOffsetCommit(header *protocol.RequestHeader, d *protocol.Decoder, b *broker.Broker) (protocol.Payload, error) {
	// Request structure v0:
	// group_id (string)
	// [topics]
	//   name (string)
	//   [partitions]
	//     partition_index (int32)
	//     committed_offset (int64)
	//     metadata (string)

	groupID, _ := d.String()

	topicCount, err := d.ArrayLength()
	if err != nil {
		return nil, fmt.Errorf("read topics array length: %w", err)
	}

	type partResp struct {
		partition int32
		errCode   int16
	}
	type topicResp struct {
		name       string
		partitions []partResp
	}

	responses := make([]topicResp, 0, topicCount)

	for i := 0; i < topicCount; i++ {
		topicName, _ := d.String()

		partCount, err := d.ArrayLength()
		if err != nil {
			return nil, fmt.Errorf("read partitions array length: %w", err)
		}

		pResps := make([]partResp, 0, partCount)
		for j := 0; j < partCount; j++ {
			partition, _ := d.Int32()
			offset, _ := d.Int64()
			_, _ = d.String() // metadata (ignored)

			// Store offset in GroupManager
			b.GroupManager.CommitOffset(groupID, topicName, partition, offset)
			log.Printf("[miKago] Committed offset %d for group %q, topic %q, partition %d", offset, groupID, topicName, partition)

			pResps = append(pResps, partResp{
				partition: partition,
				errCode:   protocol.ErrNone,
			})
		}

		responses = append(responses, topicResp{
			name:       topicName,
			partitions: pResps,
		})
	}

	// Response structure v0:
	// [responses]
	//   name (string)
	//   [partitions]
	//     partition_index (int32)
	//     error_code (int16)

	enc := protocol.GetEncoder()
	protocol.EncodeResponseHeader(enc, header.CorrelationID)
	enc.PutArrayLength(len(responses))

	for _, tRes := range responses {
		enc.PutString(tRes.name)
		enc.PutArrayLength(len(tRes.partitions))
		for _, pRes := range tRes.partitions {
			enc.PutInt32(pRes.partition)
			enc.PutInt16(pRes.errCode)
		}
	}

	return protocol.EncoderPayload{Encoder: enc}, nil
}
