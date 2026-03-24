package raft

import (
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"
)

// TestLeaderFailover simulates a random node going down and verifies
// that the remaining two nodes elect a new leader automatically.
func TestLeaderFailover(t *testing.T) {
	ports := freePorts(t, 3)
	ids := []string{"node1", "node2", "node3"}

	peers := func(i int) []Peer {
		var ps []Peer
		for j, id := range ids {
			if j != i {
				ps = append(ps, Peer{ID: id, Address: fmt.Sprintf("localhost:%d", ports[j])})
			}
		}
		return ps
	}

	nodes := make([]*Raft, 3)
	servers := make([]*NetServer, 3)
	for i := range nodes {
		nodes[i] = NewRaft(ids[i], peers(i))
		srv, err := StartRaftServer(nodes[i], ports[i])
		if err != nil {
			t.Fatalf("start %s: %v", ids[i], err)
		}
		servers[i] = srv
	}
	defer func() {
		for _, srv := range servers {
			if srv != nil {
				srv.Close()
			}
		}
	}()

	// --- Phase 1: initial election ---
	fmt.Println("\n[Phase 1] Waiting for initial leader election...")
	firstIdx := waitForLeader(t, nodes, nil, 2*time.Second)
	fmt.Printf("         Leader: %s\n", nodes[firstIdx].ID)

	// --- Phase 2: kill a random node ---
	victim := rand.Intn(3)
	fmt.Printf("\n[Phase 2] Killing %s...\n", nodes[victim].ID)
	nodes[victim].Stop()   // stop goroutines (no more heartbeats sent)
	servers[victim].Close() // stop accepting RPCs
	servers[victim] = nil

	// --- Phase 3: remaining nodes detect failure and re-elect ---
	fmt.Println("\n[Phase 3] Waiting for failover election...")
	skip := map[int]bool{victim: true}
	newIdx := waitForLeader(t, nodes, skip, 2*time.Second)
	fmt.Printf("         New leader: %s\n", nodes[newIdx].ID)

	if newIdx == firstIdx {
		fmt.Println("         (victim was a follower — same leader survived)")
	} else {
		fmt.Printf("         (leadership transferred: %s → %s)\n",
			nodes[firstIdx].ID, nodes[newIdx].ID)
	}
}

// waitForLeader polls until exactly one non-skipped node is leader, or times out.
func waitForLeader(t *testing.T, nodes []*Raft, skip map[int]bool, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		idx := -1
		for i, rf := range nodes {
			if skip[i] {
				continue
			}
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if isLeader {
				if idx >= 0 {
					idx = -1 // two leaders at once — wait for it to settle
					break
				}
				idx = i
			}
		}
		if idx >= 0 {
			return idx
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no stable leader elected within timeout")
	return -1
}

// freePorts returns n available TCP ports.
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, n)
	for i := range ports {
		l, err := net.Listen("tcp", ":0")
		if err != nil {
			t.Fatalf("find free port: %v", err)
		}
		ports[i] = l.Addr().(*net.TCPAddr).Port
		l.Close()
	}
	return ports
}
