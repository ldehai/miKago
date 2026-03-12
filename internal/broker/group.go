package broker

import (
	"sync"
)

// GroupManager handles consumer group offset storage.
// For the MVP, this stores offsets in-memory. In a real system,
// these would be durably stored in an internal topic (__consumer_offsets).
type GroupManager struct {
	mu sync.RWMutex
	// Group -> Topic -> Partition -> Offset
	offsets map[string]map[string]map[int32]int64
}

// NewGroupManager creates a new in-memory GroupManager.
func NewGroupManager() *GroupManager {
	return &GroupManager{
		offsets: make(map[string]map[string]map[int32]int64),
	}
}

// CommitOffset saves a committed offset for a given group, topic, and partition.
func (gm *GroupManager) CommitOffset(groupID, topic string, partition int32, offset int64) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	groupMap, ok := gm.offsets[groupID]
	if !ok {
		groupMap = make(map[string]map[int32]int64)
		gm.offsets[groupID] = groupMap
	}

	topicMap, ok := groupMap[topic]
	if !ok {
		topicMap = make(map[int32]int64)
		groupMap[topic] = topicMap
	}

	topicMap[partition] = offset
}

// FetchOffset retrieves the committed offset for a given group, topic, and partition.
// Returns the offset and true if found, or -1 and false if no offset is saved.
func (gm *GroupManager) FetchOffset(groupID, topic string, partition int32) (int64, bool) {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	groupMap, ok := gm.offsets[groupID]
	if !ok {
		return -1, false
	}

	topicMap, ok := groupMap[topic]
	if !ok {
		return -1, false
	}

	offset, ok := topicMap[partition]
	if !ok {
		return -1, false
	}

	return offset, true
}
