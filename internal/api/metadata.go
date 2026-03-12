package api

import (
	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// HandleMetadata handles Metadata requests (api_key=3, v0-v1).
//
// Metadata v0 Request: [topics: topic_name]
// Metadata v0 Response: [brokers: node_id, host, port], [topics: error_code, topic_name, [partitions: ...]]
//
// Metadata v1 adds:
//   - Request: same as v0 (null array = all topics)
//   - Response: brokers get "rack" field, topics get "is_internal" field, adds "controller_id"
func HandleMetadata(header *protocol.RequestHeader, body *protocol.Decoder, b *broker.Broker) ([]byte, error) {
	// Parse request: array of topic names
	topicCount, err := body.ArrayLength()
	if err != nil {
		return nil, err
	}

	var requestedTopics []string
	allTopics := topicCount < 0

	if !allTopics {
		requestedTopics = make([]string, 0, topicCount)
		for i := 0; i < topicCount; i++ {
			name, err := body.String()
			if err != nil {
				return nil, err
			}
			requestedTopics = append(requestedTopics, name)
		}
	}

	enc := protocol.NewEncoder()
	protocol.EncodeResponseHeader(enc, header.CorrelationID)

	// Brokers array
	enc.PutArrayLength(1)
	enc.PutInt32(b.Config.BrokerID)
	enc.PutString(b.Config.Host)
	enc.PutInt32(b.Config.Port)
	if header.APIVersion >= 1 {
		enc.PutString("") // rack (empty string, not null)
	}

	// v1: controller_id
	if header.APIVersion >= 1 {
		enc.PutInt32(b.Config.BrokerID) // we are the controller
	}

	// Determine which topics to return
	var topics []*broker.Topic
	if allTopics {
		topics = b.TopicManager.AllTopics()
	} else {
		// If we are asked about a topic that doesn't exist, create it auto-magically
		topics = make([]*broker.Topic, 0, len(requestedTopics))
		for _, topicName := range requestedTopics {
			topic := b.TopicManager.GetTopic(topicName)
			if topic == nil {
				topic = b.TopicManager.GetOrCreateTopic(topicName)
			}
			topics = append(topics, topic)
		}
	}

	// Topics array
	enc.PutArrayLength(len(topics))
	for _, t := range topics {
		enc.PutInt16(protocol.ErrNone) // topic error code
		enc.PutString(t.Name)

		// v1: is_internal
		if header.APIVersion >= 1 {
			enc.PutBool(false) // not internal
		}

		// Partitions array
		enc.PutArrayLength(len(t.Partitions))
		for _, p := range t.Partitions {
			enc.PutInt16(protocol.ErrNone)
			enc.PutInt32(p.ID())
			enc.PutInt32(b.Config.BrokerID) // leader
			enc.PutArrayLength(1)           // replicas
			enc.PutInt32(b.Config.BrokerID)
			enc.PutArrayLength(1) // ISR
			enc.PutInt32(b.Config.BrokerID)
		}
	}

	return enc.Bytes(), nil
}
