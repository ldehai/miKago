package api

import (
	"fmt"
	"log"

	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// HandleCreateTopics processes a CreateTopics v0/v1 request (API key 19).
func HandleCreateTopics(header *protocol.RequestHeader, d *protocol.Decoder, b *broker.Broker) (protocol.Payload, error) {
	// Request structure (v0/v1):
	// [topics]
	//   name (string)
	//   num_partitions (int32)
	//   replication_factor (int16)
	//   [assignments] (we ignore custom replica assignments)
	//   [configs]
	// timeout_ms (int32)
	// validate_only (bool) [v1+]

	topicCount, err := d.ArrayLength()
	if err != nil {
		return nil, fmt.Errorf("read topics array length: %w", err)
	}

	type topicReq struct {
		name              string
		numPartitions     int32
		replicationFactor int16
	}

	reqs := make([]topicReq, 0, topicCount)
	for i := 0; i < topicCount; i++ {
		name, _ := d.String()
		numPartitions, _ := d.Int32()
		replicationFactor, _ := d.Int16()

		// Skip assignments array
		assignmentCount, _ := d.ArrayLength()
		for j := 0; j < assignmentCount; j++ {
			d.Int32() // partition index
			replicaCount, _ := d.ArrayLength()
			for k := 0; k < replicaCount; k++ {
				d.Int32() // broker id
			}
		}

		// Skip configs array
		configCount, _ := d.ArrayLength()
		for j := 0; j < configCount; j++ {
			d.String() // config name
			d.String() // config value
		}

		reqs = append(reqs, topicReq{
			name:              name,
			numPartitions:     numPartitions,
			replicationFactor: replicationFactor,
		})
	}

	timeoutMs, _ := d.Int32()
	_ = timeoutMs

	validateOnly := false
	if header.APIVersion >= 1 {
		validateOnly, _ = d.Bool()
	}

	enc := protocol.GetEncoder()
	protocol.EncodeResponseHeader(enc, header.CorrelationID)
	if header.APIVersion >= 1 {
		enc.PutInt32(0) // throttle_time_ms
	}
	enc.PutArrayLength(len(reqs))

	for _, req := range reqs {
		errCode := protocol.ErrNone

		if req.numPartitions < 1 {
			errCode = protocol.ErrInvalidPartitions
		} else if req.replicationFactor != 1 && req.replicationFactor != -1 {
			// We only support 1 replica for now
			errCode = protocol.ErrInvalidReplicationFactor
		} else if !validateOnly {
			// Create the topic
			_, err := b.TopicManager.CreateTopic(req.name, int(req.numPartitions))
			if err != nil {
				log.Printf("[miKago] Failed to create topic %q: %v", req.name, err)
				errCode = protocol.ErrTopicAlreadyExists
			} else {
				log.Printf("[miKago] Created topic %q with %d partitions", req.name, req.numPartitions)
			}
		}

		enc.PutString(req.name)
		enc.PutInt16(errCode)
	}

	return protocol.EncoderPayload{Encoder: enc}, nil
}
