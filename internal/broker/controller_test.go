package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/andy/mikago/internal/raft"
)

// TestPartitionControllerBasic verifies that ApplyAssignments correctly routes
// IsLeaderFor and GetLeader queries.
func TestPartitionControllerBasic(t *testing.T) {
	pc := NewPartitionController(1)

	// Before any assignment: default to self.
	if !pc.IsLeaderFor("test", 0) {
		t.Fatal("expected self to be default leader before any assignment")
	}
	if got := pc.GetLeader("test", 0); got != 1 {
		t.Fatalf("expected GetLeader=1 (self), got %d", got)
	}

	// Apply an assignment that puts partition 0 on broker 2 and partition 1 on broker 1 (self).
	pc.ApplyAssignments([]raft.PartitionLeaderAssignment{
		{Topic: "test", PartitionID: 0, BrokerID: 2},
		{Topic: "test", PartitionID: 1, BrokerID: 1},
	})

	if pc.IsLeaderFor("test", 0) {
		t.Fatal("broker 1 should NOT be leader for partition 0")
	}
	if !pc.IsLeaderFor("test", 1) {
		t.Fatal("broker 1 SHOULD be leader for partition 1")
	}
	if got := pc.GetLeader("test", 0); got != 2 {
		t.Fatalf("expected leader=2 for partition 0, got %d", got)
	}
}

// TestPartitionControllerReassignment verifies that assignments can be updated
// (simulating failover reassignment by a new Controller).
func TestPartitionControllerReassignment(t *testing.T) {
	pc := NewPartitionController(3)

	// Initial assignment: broker 1 leads everything.
	pc.ApplyAssignments([]raft.PartitionLeaderAssignment{
		{Topic: "events", PartitionID: 0, BrokerID: 1},
		{Topic: "events", PartitionID: 1, BrokerID: 2},
		{Topic: "events", PartitionID: 2, BrokerID: 3},
	})

	if !pc.IsLeaderFor("events", 2) {
		t.Fatal("broker 3 should be leader for partition 2")
	}

	// Simulate failover: reassign broker 2's partition to broker 3.
	pc.ApplyAssignments([]raft.PartitionLeaderAssignment{
		{Topic: "events", PartitionID: 1, BrokerID: 3},
	})

	if !pc.IsLeaderFor("events", 1) {
		t.Fatal("after reassignment, broker 3 should be leader for partition 1")
	}
	if !pc.IsLeaderFor("events", 2) {
		t.Fatal("broker 3 should still be leader for partition 2")
	}
}

// TestPartitionLeaderDistribution tests the full controller assignment pipeline:
// a 3-node Raft cluster elects a leader, which then assigns partitions evenly across brokers.
func TestPartitionLeaderDistribution(t *testing.T) {
	// Build a 3-node Raft cluster.
	peers1 := []raft.Peer{{ID: "1", Address: "localhost:9101"}, {ID: "2", Address: "localhost:9102"}}
	peers2 := []raft.Peer{{ID: "0", Address: "localhost:9100"}, {ID: "2", Address: "localhost:9102"}}
	peers3 := []raft.Peer{{ID: "0", Address: "localhost:9100"}, {ID: "1", Address: "localhost:9101"}}

	knownBrokers := []ClusterBroker{
		{ID: 0, Host: "localhost", Port: 9092},
		{ID: 1, Host: "localhost", Port: 9093},
		{ID: 2, Host: "localhost", Port: 9094},
	}

	// Use temp dirs for data.
	dirs := [3]string{t.TempDir(), t.TempDir(), t.TempDir()}

	configs := []Config{
		{BrokerID: 0, Host: "localhost", Port: 9092, DataDir: dirs[0], RaftPort: 9100, RaftPeers: peers1, KnownBrokers: knownBrokers, DefaultNumPartitions: 1},
		{BrokerID: 1, Host: "localhost", Port: 9093, DataDir: dirs[1], RaftPort: 9101, RaftPeers: peers2, KnownBrokers: knownBrokers, DefaultNumPartitions: 1},
		{BrokerID: 2, Host: "localhost", Port: 9094, DataDir: dirs[2], RaftPort: 9102, RaftPeers: peers3, KnownBrokers: knownBrokers, DefaultNumPartitions: 1},
	}

	brokers := make([]*Broker, 3)
	for i, cfg := range configs {
		brokers[i] = NewBroker(cfg)
	}
	defer func() {
		for _, b := range brokers {
			b.Close()
		}
	}()

	// Create topics with multiple partitions across all nodes before assignment.
	// We pre-create on all brokers so the controller has something to assign.
	for _, b := range brokers {
		if _, err := b.TopicManager.CreateTopic("orders", 6); err != nil {
			// Topic may already exist if CreateTopic races — that's OK.
			_ = err
		}
	}

	// Wait for Raft leader election and partition assignment.
	// Election: up to ~300ms. Assignment delay: 200ms. Plus some buffer.
	time.Sleep(1200 * time.Millisecond)

	// Find the Raft leader (Controller).
	var controllerIdx int = -1
	for i, b := range brokers {
		leaderID := b.Raft.GetLeaderID()
		selfID := fmt.Sprintf("%d", b.Config.BrokerID)
		if leaderID == selfID {
			controllerIdx = i
			break
		}
	}
	if controllerIdx < 0 {
		t.Fatal("no Raft leader elected within timeout")
	}
	t.Logf("Controller (Raft leader): broker %d", brokers[controllerIdx].Config.BrokerID)

	// After the controller assigns partitions, ALL brokers should have received the assignment
	// via the Raft log. Give the state machine a moment to apply.
	time.Sleep(300 * time.Millisecond)

	// Count how many partitions each broker leads on the controller node's view.
	// In a balanced assignment, each of the 3 brokers should lead exactly 2 of 6 partitions.
	leaderCount := make(map[int32]int)
	ctrl := brokers[controllerIdx].Controller
	for pid := int32(0); pid < 6; pid++ {
		lid := ctrl.GetLeader("orders", pid)
		leaderCount[lid]++
	}

	t.Logf("Partition leader distribution: %v", leaderCount)

	if len(leaderCount) < 2 {
		t.Errorf("expected leaders distributed across >= 2 brokers, got %d: %v", len(leaderCount), leaderCount)
	}

	// Verify that all brokers agree on the same leaders (assignment was replicated).
	for i, b := range brokers {
		for pid := int32(0); pid < 6; pid++ {
			expected := ctrl.GetLeader("orders", pid)
			got := b.Controller.GetLeader("orders", pid)
			if expected != got {
				t.Errorf("broker %d disagrees on leader for orders/%d: want %d, got %d",
					i, pid, expected, got)
			}
		}
	}
}
