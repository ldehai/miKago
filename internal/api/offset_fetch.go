package api

import (
	"fmt"

	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// HandleOffsetFetch processes an OffsetFetch v1 request (API key 9).
func HandleOffsetFetch(header *protocol.RequestHeader, d *protocol.Decoder, b *broker.Broker) ([]byte, error) {
	// Request structure v1:
	// group_id (string)
	// [topics]
	//   name (string)
	//   [partitions]
	//     partition_index (int32)

	groupID, _ := d.String()

	topicCount, err := d.ArrayLength()
	if err != nil {
		return nil, fmt.Errorf("read topics array length: %w", err)
	}

	type partResp struct {
		partition int32
		offset    int64
		metadata  string
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

			offset, found := b.GroupManager.FetchOffset(groupID, topicName, partition)
			errCode := protocol.ErrNone
			if !found {
				offset = -1
				// Error code 0 is still returned if no offset is found (it just returns -1)
			}

			pResps = append(pResps, partResp{
				partition: partition,
				offset:    offset,
				metadata:  "",
				errCode:   errCode,
			})
		}

		responses = append(responses, topicResp{
			name:       topicName,
			partitions: pResps,
		})
	}

	// Response structure v1:
	// [responses]
	//   name (string)
	//   [partitions]
	//     partition_index (int32)
	//     committed_offset (int64)
	//     metadata (string)
	//     error_code (int16)

	enc := protocol.NewEncoder()
	protocol.EncodeResponseHeader(enc, header.CorrelationID)
	enc.PutArrayLength(len(responses))

	for _, tRes := range responses {
		enc.PutString(tRes.name)
		enc.PutArrayLength(len(tRes.partitions))
		for _, pRes := range tRes.partitions {
			enc.PutInt32(pRes.partition)
			enc.PutInt64(pRes.offset)
			enc.PutString(pRes.metadata)
			enc.PutInt16(pRes.errCode)
		}
	}

	return enc.Bytes(), nil
}
