package broker

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/andy/mikago/internal/storage"
)

// Message represents a single message returned from a partition fetch.
type Message struct {
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp interface{ UnixMilli() int64 }
}

// Partition is an append-only message log backed by disk storage.
type Partition struct {
	mu  sync.RWMutex
	id  int32
	log *storage.Log
}

// NewPartition creates a new partition, opening or recovering the log from disk.
func NewPartition(id int32, dataDir string, logSegmentBytes int64) (*Partition, error) {
	dir := filepath.Join(dataDir, fmt.Sprintf("%d", id))
	var l *storage.Log
	var err error
	if logSegmentBytes > 0 {
		l, err = storage.NewLogWithConfig(dir, logSegmentBytes)
	} else {
		l, err = storage.NewLog(dir)
	}
	if err != nil {
		return nil, fmt.Errorf("open partition %d log: %w", id, err)
	}

	return &Partition{
		id:  id,
		log: l,
	}, nil
}

// ID returns the partition ID.
func (p *Partition) ID() int32 {
	return p.id
}

// Append adds a message to the partition and returns the assigned offset.
func (p *Partition) Append(key, value []byte) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	offset, err := p.log.Append(key, value)
	if err != nil {
		// In production, we'd handle this more gracefully
		panic(fmt.Sprintf("failed to append to partition %d: %v", p.id, err))
	}
	return offset
}

// Fetch retrieves messages starting from the given offset, up to maxBytes of data.
// Returns the messages and the high-water mark (next offset to be written).
func (p *Partition) Fetch(startOffset int64, maxBytes int32) ([]Message, int64) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	records, hwm := p.log.Fetch(startOffset, maxBytes)
	if len(records) == 0 {
		return nil, hwm
	}

	messages := make([]Message, len(records))
	for i, rec := range records {
		messages[i] = Message{
			Offset:    rec.Offset,
			Key:       rec.Key,
			Value:     rec.Value,
			Timestamp: rec.Timestamp,
		}
	}

	return messages, hwm
}

// HighWaterMark returns the next offset to be written.
func (p *Partition) HighWaterMark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.log.HighWaterMark()
}

// Close flushes and closes the partition's log.
func (p *Partition) Close() error {
	return p.log.Close()
}

// Topic holds one or more partitions.
type Topic struct {
	Name       string
	Partitions []*Partition
}

// Close closes all partitions in the topic.
func (t *Topic) Close() error {
	for _, p := range t.Partitions {
		if err := p.Close(); err != nil {
			return err
		}
	}
	return nil
}

// TopicManager manages all topics and their partitions.
type TopicManager struct {
	mu              sync.RWMutex
	topics          map[string]*Topic
	dataDir         string
	logSegmentBytes int64
}

// NewTopicManager creates a new TopicManager and recovers existing topics from disk.
func NewTopicManager(dataDir string, logSegmentBytes int64) *TopicManager {
	tm := &TopicManager{
		topics:          make(map[string]*Topic),
		dataDir:         dataDir,
		logSegmentBytes: logSegmentBytes,
	}
	tm.recoverFromDisk()
	return tm
}

// recoverFromDisk scans the data directory for existing topics and partitions.
func (tm *TopicManager) recoverFromDisk() {
	topicsDir := filepath.Join(tm.dataDir, "topics")
	entries, err := os.ReadDir(topicsDir)
	if err != nil {
		// No topics directory yet — fresh start
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		topicName := entry.Name()
		topicDir := filepath.Join(topicsDir, topicName)

		// Discover partitions
		partEntries, err := os.ReadDir(topicDir)
		if err != nil {
			log.Printf("[miKago] Warning: cannot read topic dir %s: %v", topicDir, err)
			continue
		}

		var partIDs []int
		for _, pe := range partEntries {
			if !pe.IsDir() {
				continue
			}
			id, err := strconv.Atoi(pe.Name())
			if err != nil {
				continue
			}
			partIDs = append(partIDs, id)
		}
		sort.Ints(partIDs)

		if len(partIDs) == 0 {
			continue
		}

		partitions := make([]*Partition, len(partIDs))
		for i, pid := range partIDs {
			p, err := NewPartition(int32(pid), topicDir, tm.logSegmentBytes)
			if err != nil {
				log.Printf("[miKago] Warning: cannot recover partition %d of topic %s: %v", pid, topicName, err)
				continue
			}
			partitions[i] = p
		}

		topic := &Topic{
			Name:       topicName,
			Partitions: partitions,
		}
		tm.topics[topicName] = topic
		log.Printf("[miKago] Recovered topic %q with %d partition(s), hwm=%d",
			topicName, len(partitions), partitions[len(partitions)-1].HighWaterMark())
	}
}

// topicDir returns the directory for a topic's data.
func (tm *TopicManager) topicDir(name string) string {
	return filepath.Join(tm.dataDir, "topics", name)
}

// CreateTopic creates a topic with the given number of partitions.
// Returns an error if the topic already exists.
func (tm *TopicManager) CreateTopic(name string, numPartitions int) (*Topic, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[name]; exists {
		return nil, fmt.Errorf("topic %q already exists", name)
	}

	topicDir := tm.topicDir(name)
	partitions := make([]*Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		p, err := NewPartition(int32(i), topicDir, tm.logSegmentBytes)
		if err != nil {
			// Cleanup already created partitions
			for j := 0; j < i; j++ {
				partitions[j].Close()
			}
			return nil, fmt.Errorf("create partition %d: %w", i, err)
		}
		partitions[i] = p
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

	topicDir := tm.topicDir(name)
	p, err := NewPartition(0, topicDir, tm.logSegmentBytes)
	if err != nil {
		panic(fmt.Sprintf("failed to create partition for topic %q: %v", name, err))
	}

	topic := &Topic{
		Name:       name,
		Partitions: []*Partition{p},
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

// Close closes all topics.
func (tm *TopicManager) Close() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, t := range tm.topics {
		if err := t.Close(); err != nil {
			return err
		}
	}
	return nil
}
