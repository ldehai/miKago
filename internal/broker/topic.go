package broker

import (
	"fmt"
	"sync"
	"time"
)

// Message represents a single message stored in a partition.
type Message struct {
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// Partition is an append-only in-memory message log.
type Partition struct {
	mu       sync.RWMutex
	id       int32
	messages []Message
	nextOff  int64
}

// NewPartition creates a new empty partition.
func NewPartition(id int32) *Partition {
	return &Partition{
		id:       id,
		messages: make([]Message, 0, 256),
		nextOff:  0,
	}
}

// ID returns the partition ID.
func (p *Partition) ID() int32 {
	return p.id
}

// Append adds a message to the partition and returns the assigned offset.
func (p *Partition) Append(key, value []byte) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	offset := p.nextOff
	p.messages = append(p.messages, Message{
		Offset:    offset,
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
	})
	p.nextOff++
	return offset
}

// Fetch retrieves messages starting from the given offset, up to maxBytes of data.
// Returns the messages and the high-water mark (next offset to be written).
func (p *Partition) Fetch(startOffset int64, maxBytes int32) ([]Message, int64) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	hwm := p.nextOff

	if startOffset >= hwm || len(p.messages) == 0 {
		return nil, hwm
	}

	// Find the starting index (offset == index in our simple model)
	startIdx := int(startOffset)
	if startIdx < 0 {
		startIdx = 0
	}
	if startIdx >= len(p.messages) {
		return nil, hwm
	}

	var result []Message
	var totalSize int32
	for i := startIdx; i < len(p.messages); i++ {
		msgSize := int32(len(p.messages[i].Key) + len(p.messages[i].Value) + 26) // overhead estimate
		if totalSize+msgSize > maxBytes && len(result) > 0 {
			break
		}
		result = append(result, p.messages[i])
		totalSize += msgSize
	}

	return result, hwm
}

// HighWaterMark returns the next offset to be written.
func (p *Partition) HighWaterMark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextOff
}

// Topic holds one or more partitions.
type Topic struct {
	Name       string
	Partitions []*Partition
}

// TopicManager manages all topics and their partitions.
type TopicManager struct {
	mu     sync.RWMutex
	topics map[string]*Topic
}

// NewTopicManager creates a new TopicManager.
func NewTopicManager() *TopicManager {
	return &TopicManager{
		topics: make(map[string]*Topic),
	}
}

// CreateTopic creates a topic with the given number of partitions.
// Returns an error if the topic already exists.
func (tm *TopicManager) CreateTopic(name string, numPartitions int) (*Topic, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[name]; exists {
		return nil, fmt.Errorf("topic %q already exists", name)
	}

	partitions := make([]*Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		partitions[i] = NewPartition(int32(i))
	}

	topic := &Topic{
		Name:       name,
		Partitions: partitions,
	}
	tm.topics[name] = topic
	return topic, nil
}

// GetOrCreateTopic gets an existing topic or creates one with 1 partition.
func (tm *TopicManager) GetOrCreateTopic(name string) *Topic {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if t, exists := tm.topics[name]; exists {
		return t
	}

	partitions := []*Partition{NewPartition(0)}
	topic := &Topic{
		Name:       name,
		Partitions: partitions,
	}
	tm.topics[name] = topic
	return topic
}

// GetTopic returns a topic by name, or nil if not found.
func (tm *TopicManager) GetTopic(name string) *Topic {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.topics[name]
}

// ListTopics returns all topic names.
func (tm *TopicManager) ListTopics() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	names := make([]string, 0, len(tm.topics))
	for name := range tm.topics {
		names = append(names, name)
	}
	return names
}

// AllTopics returns all topics.
func (tm *TopicManager) AllTopics() []*Topic {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	topics := make([]*Topic, 0, len(tm.topics))
	for _, t := range tm.topics {
		topics = append(topics, t)
	}
	return topics
}
