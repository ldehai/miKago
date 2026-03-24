package broker

import (
	"fmt"
	"sync"

	"github.com/andy/mikago/internal/raft"
)

// PartitionController tracks partition leader assignments for all topics in the cluster.
// It is updated on all nodes by applying LeaderAssignmentCmd entries from the Raft log.
//
// Design: The Raft leader (Controller) decides the assignment; all brokers apply it
// and route produce requests based on whether they are the assigned leader.
type PartitionController struct {
	mu     sync.RWMutex
	leaders map[string]int32 // partitionKey(topic, id) → brokerID
	selfID  int32
}

// NewPartitionController creates a new controller for the given broker.
func NewPartitionController(selfID int32) *PartitionController {
	return &PartitionController{
		leaders: make(map[string]int32),
		selfID:  selfID,
	}
}

func partitionKey(topic string, partitionID int32) string {
	return fmt.Sprintf("%s\x00%d", topic, partitionID)
}

// ApplyAssignments updates the leader map from a replicated command.
// Called by all brokers when they apply a LeaderAssignmentCmd from the Raft log.
func (pc *PartitionController) ApplyAssignments(assignments []raft.PartitionLeaderAssignment) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, a := range assignments {
		pc.leaders[partitionKey(a.Topic, a.PartitionID)] = a.BrokerID
	}
}

// IsLeaderFor returns true if this broker is the current leader for the given partition.
// Returns true by default when no assignment is known (single-broker / standalone mode).
func (pc *PartitionController) IsLeaderFor(topic string, partitionID int32) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	leader, ok := pc.leaders[partitionKey(topic, partitionID)]
	if !ok {
		return true // no assignment yet → assume self is leader
	}
	return leader == pc.selfID
}

// AllLeaders returns a snapshot of all partition-leader assignments.
// Used by the admin server for the dashboard.
func (pc *PartitionController) AllLeaders() map[string]int32 {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	result := make(map[string]int32, len(pc.leaders))
	for k, v := range pc.leaders {
		result[k] = v
	}
	return result
}

// GetLeader returns the broker ID of the current leader for the given partition.
// Returns selfID when no assignment is known.
func (pc *PartitionController) GetLeader(topic string, partitionID int32) int32 {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	leader, ok := pc.leaders[partitionKey(topic, partitionID)]
	if !ok {
		return pc.selfID
	}
	return leader
}
