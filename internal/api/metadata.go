package api

import (
	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// HandleMetadata handles Metadata requests (api_key=3, v0).
//
// Metadata v0 Request: [topics: topic_name]  (null array = all topics)
// Metadata v0 Response: [brokers: node_id, host, port], [topics: error_code, topic_name, [partitions: error_code, partition_id, leader, replicas, isr]]
func HandleMetadata(header *protocol.RequestHeader, body *protocol.Decoder, b *broker.Broker) ([]byte, error) {
	// Parse request: array of topic names
	topicCount, err := body.ArrayLength()
	if err != nil {
		return nil, err
	}

	// Collect requested topic names (nil = all topics)
	var requestedTopics []string
	allTopics := topicCount < 0 // null array means all topics

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

	// Response header
	protocol.EncodeResponseHeader(enc, header.CorrelationID)

	// Brokers array (single broker)
	enc.PutArrayLength(1)
	enc.PutInt32(b.Config.BrokerID) // node_id
	enc.PutString(b.Config.Host)    // host
	enc.PutInt32(b.Config.Port)     // port

	// Determine which topics to return
	var topics []*broker.Topic
	if allTopics {
		topics = b.TopicManager.AllTopics()
	} else {
		topics = make([]*broker.Topic, 0, len(requestedTopics))
		for _, name := range requestedTopics {
			t := b.TopicManager.GetOrCreateTopic(name)
			topics = append(topics, t)
		}
	}

	// Topics array
	enc.PutArrayLength(len(topics))
	for _, t := range topics {
		enc.PutInt16(protocol.ErrNone) // topic error code
		enc.PutString(t.Name)          // topic name

		// Partitions array
		enc.PutArrayLength(len(t.Partitions))
		for _, p := range t.Partitions {
			enc.PutInt16(protocol.ErrNone)  // partition error code
			enc.PutInt32(p.ID())            // partition index
			enc.PutInt32(b.Config.BrokerID) // leader (us)
			enc.PutArrayLength(1)           // replicas (just us)
			enc.PutInt32(b.Config.BrokerID)
			enc.PutArrayLength(1) // ISR (just us)
			enc.PutInt32(b.Config.BrokerID)
		}
	}

	return enc.Bytes(), nil
}
