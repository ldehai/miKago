package api

import (
	"strconv"

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
func HandleMetadata(header *protocol.RequestHeader, body *protocol.Decoder, b *broker.Broker) (protocol.Payload, error) {
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

	enc := protocol.GetEncoder()
	protocol.EncodeResponseHeader(enc, header.CorrelationID)

	// Brokers array — include all known cluster brokers so clients can connect to any partition leader.
	brokers := b.AllBrokers()
	enc.PutArrayLength(len(brokers))
	for _, br := range brokers {
		enc.PutInt32(br.ID)
		enc.PutString(br.Host)
		enc.PutInt32(br.Port)
		if header.APIVersion >= 1 {
			enc.PutString("") // rack (empty string)
		}
	}

	// v1: controller_id (the current Raft leader / Controller)
	if header.APIVersion >= 1 {
		controllerID := b.Config.BrokerID // default: self
		if b.Raft != nil {
			leaderID := b.Raft.GetLeaderID()
			// Convert Raft peer string ID (e.g. "1") back to broker ID if available.
			for _, br := range brokers {
				if leaderID == intToStr(br.ID) {
					controllerID = br.ID
					break
				}
			}
		}
		enc.PutInt32(controllerID)
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
			enc.PutBool(false)
		}

		// Partitions array
		enc.PutArrayLength(len(t.Partitions))
		for _, p := range t.Partitions {
			// Ask the controller which broker is leader for this partition.
			leaderID := b.Controller.GetLeader(t.Name, p.ID())

			enc.PutInt16(protocol.ErrNone)
			enc.PutInt32(p.ID())
			enc.PutInt32(leaderID) // leader broker ID
			enc.PutArrayLength(1)  // replicas: only the leader for now (no ISR replication)
			enc.PutInt32(leaderID)
			enc.PutArrayLength(1) // ISR
			enc.PutInt32(leaderID)
		}
	}

	return protocol.EncoderPayload{Encoder: enc}, nil
}

// intToStr converts an int32 broker ID to the string form used as a Raft peer ID.
func intToStr(id int32) string {
	return strconv.Itoa(int(id))
}
