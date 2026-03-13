package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestRaftElection(t *testing.T) {
	// 1. Setup a cluster of 3 nodes
	peers1 := []Peer{{ID: "node2", Address: "localhost:8002"}, {ID: "node3", Address: "localhost:8003"}}
	peers2 := []Peer{{ID: "node1", Address: "localhost:8001"}, {ID: "node3", Address: "localhost:8003"}}
	peers3 := []Peer{{ID: "node1", Address: "localhost:8001"}, {ID: "node2", Address: "localhost:8002"}}

	rf1 := NewRaft("node1", peers1)
	rf2 := NewRaft("node2", peers2)
	rf3 := NewRaft("node3", peers3)

	srv1, _ := StartRaftServer(rf1, 8001)
	srv2, _ := StartRaftServer(rf2, 8002)
	srv3, _ := StartRaftServer(rf3, 8003)

	defer srv1.Close()
	defer srv2.Close()
	defer srv3.Close()

	// Give them time to elect a leader
	time.Sleep(1 * time.Second)

	leaders := 0
	leaderID := ""

	nodes := []*Raft{rf1, rf2, rf3}
	for _, n := range nodes {
		n.mu.Lock()
		if n.state == Leader {
			leaders++
			leaderID = n.ID
		}
		n.mu.Unlock()
	}

	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaders)
	}
	fmt.Printf("✅ Leader elected successfully: %s\n", leaderID)
}
