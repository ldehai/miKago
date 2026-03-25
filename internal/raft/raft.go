package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/andy/mikago/internal/metrics"
)

// SetAdminAddr stores this node's admin HTTP URL so it can be gossiped to peers
// via Raft heartbeats. Call this before the node starts sending RPCs.
func (r *Raft) SetAdminAddr(addr string) {
	r.mu.Lock()
	r.localAdminAddr = addr
	r.mu.Unlock()
}

// PeerAdminAddrs returns a snapshot of all discovered peer admin URLs
// (nodeID → "http://host:port"). Updated within ~2 heartbeat cycles of startup.
func (r *Raft) PeerAdminAddrs() map[string]string {
	r.peerAdminMu.RLock()
	defer r.peerAdminMu.RUnlock()
	out := make(map[string]string, len(r.peerAdminAddrs))
	for k, v := range r.peerAdminAddrs {
		out[k] = v
	}
	return out
}

// recordPeerAdmin stores a discovered peer admin URL. Safe to call concurrently.
func (r *Raft) recordPeerAdmin(nodeID, addr string) {
	if addr == "" || nodeID == r.ID {
		return
	}
	r.peerAdminMu.Lock()
	r.peerAdminAddrs[nodeID] = addr
	r.peerAdminMu.Unlock()
}

// OnLeaderChangeFn is called when this node's leadership status changes.
// isLeader=true means this node just became the Raft leader (Controller).
// Called in a separate goroutine, safe to block.
type OnLeaderChangeFn func(isLeader bool)

// State representing the node's current role in the cluster.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Peer represents another node in the cluster.
type Peer struct {
	ID      string
	Address string
}

// Raft implements a single node in the Raft consensus cluster.
type Raft struct {
	mu sync.Mutex

	ID    string   // This node's unique ID
	Peers []Peer   // Other nodes in the cluster
	state State    // Current state (Follower, Candidate, Leader)

	// Persistent state on all servers (should be persisted to disk ideally)
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders (Reinitialized after election)
	nextIndex  map[string]int
	matchIndex map[string]int

	// Timers and channels
	heartbeatTimer *time.Timer
	heartbeatCh    chan struct{} // receives a token from AppendEntries on each valid heartbeat
	ApplyCh        chan ApplyMsg

	// Partition-level leader election support
	currentLeaderID string                 // ID of the current known leader (self if leader)
	peerLastSeen    map[string]time.Time   // last successful heartbeat response time per peer
	OnLeaderChange  OnLeaderChangeFn       // callback when leadership changes (may be nil)
	leaderReadyCh   chan struct{}           // closed by runLeader() once the heartbeat loop starts

	// Admin auto-discovery via heartbeat gossip
	localAdminAddr string            // this node's admin HTTP URL (set by SetAdminAddr)
	peerAdminAddrs map[string]string // nodeID → admin URL, populated from RPC gossip
	peerAdminMu    sync.RWMutex      // protects peerAdminAddrs only

	done chan struct{} // closed by Stop() to terminate the run loop
}

// ApplyMsg represents a committed message to be applied to the state machine.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// ReplicateCmd encapsulates the data we want to replicate via Raft.
type ReplicateCmd struct {
	Topic       string
	PartitionID int32
	RecordSet   []byte
}

// NewRaft creates and initializes a new Raft node.
func NewRaft(id string, peers []Peer) *Raft {
	r := &Raft{
		ID:           id,
		Peers:        peers,
		state:        Follower,
		currentTerm:  0,
		votedFor:     "",
		log:          make([]LogEntry, 1), // Dummy entry at index 0 for 1-based indexing simplicity
		commitIndex:  0,
		lastApplied:  0,
		nextIndex:    make(map[string]int),
		matchIndex:   make(map[string]int),
		ApplyCh:        make(chan ApplyMsg, 100),
		heartbeatCh:    make(chan struct{}, 1), // buffered 1: non-blocking send from AppendEntries
		peerLastSeen:   make(map[string]time.Time),
		peerAdminAddrs: make(map[string]string),
		done:           make(chan struct{}),
	}

	// Start the background loop to handle state transitions
	go r.run()

	return r
}

func randomElectionDuration() time.Duration {
	// Random election timeout between 150ms to 300ms
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// Stop shuts down the Raft node, terminating its run loop.
func (r *Raft) Stop() {
	select {
	case <-r.done:
	default:
		close(r.done)
	}
}

// run is the main lifecycle loop for the Raft node.
func (r *Raft) run() {
	for {
		select {
		case <-r.done:
			return
		default:
		}

		r.mu.Lock()
		state := r.state
		r.mu.Unlock()

		switch state {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

func (r *Raft) runFollower() {
	// Use a fresh local timer each call; reset it whenever a valid heartbeat
	// arrives via heartbeatCh. This avoids the concurrent-Reset race that
	// plagued the old shared electionTimer approach.
	timer := time.NewTimer(randomElectionDuration())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			// Timeout elapsed with no heartbeat → start election.
			r.mu.Lock()
			r.becomeCandidate()
			r.mu.Unlock()
			return

		case <-r.heartbeatCh:
			// Valid heartbeat received; reset the timeout window.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(randomElectionDuration())

		case <-r.done:
			return
		}
	}
}

func (r *Raft) runCandidate() {
	r.mu.Lock()
	term := r.currentTerm
	lastLogIndex := r.getLastLogIndex()
	lastLogTerm := r.getLastLogTerm()
	r.mu.Unlock()

	r.mu.Lock()
	localAdmin := r.localAdminAddr
	r.mu.Unlock()

	// Send RequestVote to all peers
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  r.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		AdminAddr:    localAdmin,
	}

	votes := 1 // We vote for ourselves
	var votesMu sync.Mutex

	for _, peer := range r.Peers {
		go func(p Peer) {
			var reply RequestVoteReply
			log.Printf("[Raft %s] Sending RequestVote to %s for term %d", r.ID, p.ID, term)
			if SendRequestVote(&p, &args, &reply) {
				r.mu.Lock()
				defer r.mu.Unlock()

				// If we stepped down during election
				if r.state != Candidate {
					return
				}

				if reply.Term > r.currentTerm {
					r.becomeFollower(reply.Term)
					return
				}

				if reply.VoteGranted {
					votesMu.Lock()
					votes++
					if votes > (len(r.Peers)+1)/2 { // Majority threshold
						r.becomeLeader()
					}
					votesMu.Unlock()
				}
			}
		}(peer)
	}

	// Wait for election timeout; if still candidate, start a new election round.
	timer := time.NewTimer(randomElectionDuration())
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-r.done:
		return
	}

	r.mu.Lock()
	if r.state == Candidate {
		r.becomeCandidate() // Start new election for new term
	}
	r.mu.Unlock()
}

func (r *Raft) runLeader() {
	// Signal that the heartbeat loop is now running. Callers waiting on
	// WaitLeaderReady() (e.g. the partition assignment goroutine) unblock here.
	r.mu.Lock()
	ch := r.leaderReadyCh
	r.mu.Unlock()
	if ch != nil {
		close(ch)
	}

	for r.state == Leader {
		select {
		case <-r.heartbeatTimer.C:
		case <-r.done:
			return
		}

		r.mu.Lock()
		if r.state != Leader {
			r.mu.Unlock()
			return
		}

		term := r.currentTerm
		leaderCommit := r.commitIndex
		r.mu.Unlock()

		for _, peer := range r.Peers {
			go func(p Peer) {
				r.mu.Lock()
				// Construct AppendEntries args for each peer based on nextIndex
				prevLogIndex := r.nextIndex[p.ID] - 1
				prevLogTerm := 0
				if prevLogIndex > 0 && prevLogIndex <= len(r.log)-1 {
					prevLogTerm = r.log[prevLogIndex].Term
				}

				// The entries we want to send
				var entries []LogEntry
				if r.getLastLogIndex() >= r.nextIndex[p.ID] {
					entries = r.log[r.nextIndex[p.ID]:]
				}

				// Snapshot known peer admin addrs to broadcast (read under peerAdminMu).
				r.peerAdminMu.RLock()
				knownAdminAddrs := make(map[string]string, len(r.peerAdminAddrs))
				for k, v := range r.peerAdminAddrs {
					knownAdminAddrs[k] = v
				}
				r.peerAdminMu.RUnlock()

				args := AppendEntriesArgs{
					Term:           term,
					LeaderID:       r.ID,
					PrevLogIndex:   prevLogIndex,
					PrevLogTerm:    prevLogTerm,
					Entries:        entries,
					LeaderCommit:   leaderCommit,
					AdminAddr:      r.localAdminAddr,
					PeerAdminAddrs: knownAdminAddrs,
				}
				r.mu.Unlock()

				var reply AppendEntriesReply
				if SendAppendEntries(&p, &args, &reply) {
					// Record the follower's admin addr from the reply (outside main lock).
					r.recordPeerAdmin(p.ID, reply.AdminAddr)

					r.mu.Lock()
					defer r.mu.Unlock()

					// Track peer health: update last seen time on any successful RPC response
					r.peerLastSeen[p.ID] = time.Now()

					// Stepped down
					if r.state != Leader || term != r.currentTerm {
						return
					}

					if reply.Term > r.currentTerm {
						r.becomeFollower(reply.Term)
						return
					}

					// Fast update indices
					if reply.Success {
						if len(entries) > 0 {
							r.nextIndex[p.ID] = args.PrevLogIndex + len(entries) + 1
							r.matchIndex[p.ID] = r.nextIndex[p.ID] - 1
							log.Printf("[Raft %s] Replicated %d entries to %s. new matchIndex: %d",
								r.ID, len(entries), p.ID, r.matchIndex[p.ID])
							
							// Update commit index if majority reached
							for N := len(r.log) - 1; N > r.commitIndex; N-- {
								if r.log[N].Term != r.currentTerm {
									continue
								}

								matchCount := 1 // We already have it
								for _, match := range r.matchIndex {
									if match >= N {
										matchCount++
									}
								}

								if matchCount > (len(r.Peers)+1)/2 {
									r.commitIndex = N
									log.Printf("[Raft %s] Leader updated commitIndex to %d (applied to state machine)", r.ID, r.commitIndex)
									r.applyCommitted()
									break
								}
							}
						}
					} else {
						// Simple decrement for MVP backtrack
						r.nextIndex[p.ID]--
					}
				}
			}(peer)
		}

		r.mu.Lock()
		r.heartbeatTimer.Reset(50 * time.Millisecond)
		r.mu.Unlock()
	}
}

// becomeFollower transit node to Follower state.
// Assumes caller holds r.mu
func (r *Raft) becomeFollower(term int) {
	wasLeader := r.state == Leader
	log.Printf("[Raft %s] Term %d: Becoming Follower", r.ID, term)
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	metrics.Default.RaftTerm.Store(int64(term))

	if wasLeader && r.OnLeaderChange != nil {
		cb := r.OnLeaderChange
		go cb(false)
	}
}

// becomeCandidate transit node to Candidate state and initiates an election.
// Assumes caller holds r.mu
func (r *Raft) becomeCandidate() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.ID
	log.Printf("[Raft %s] Term %d: Election timeout -> Becoming Candidate", r.ID, r.currentTerm)

	// Instrument: track election count and current term (atomic, hot-path safe).
	metrics.Default.RaftElections.Add(1)
	metrics.Default.RaftTerm.Store(int64(r.currentTerm))

}

// becomeLeader transit node to Leader state.
// Assumes caller holds r.mu
func (r *Raft) becomeLeader() {
	log.Printf("[Raft %s] Term %d: Election won! Becoming LEADER", r.ID, r.currentTerm)
	r.state = Leader
	r.currentLeaderID = r.ID

	// Initialize volatile leader state
	lastIndex := r.getLastLogIndex()
	for _, peer := range r.Peers {
		r.nextIndex[peer.ID] = lastIndex + 1
		r.matchIndex[peer.ID] = 0
	}

	// Initialize heartbeatTimer here so Propose() can safely use it
	// before runLeader() has had a chance to start.
	r.heartbeatTimer = time.NewTimer(0)

	// leaderReadyCh is closed by runLeader() once the heartbeat loop is running.
	// Callers that need to wait for the loop to be live use WaitLeaderReady().
	r.leaderReadyCh = make(chan struct{})

	if r.OnLeaderChange != nil {
		cb := r.OnLeaderChange
		go cb(true)
	}
}

// WaitLeaderReady blocks until runLeader() has started its heartbeat loop,
// or the timeout elapses. Returns false on timeout.
func (r *Raft) WaitLeaderReady(timeout time.Duration) bool {
	r.mu.Lock()
	ch := r.leaderReadyCh
	r.mu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

// GetLeaderID returns the current known leader ID (empty if unknown).
func (r *Raft) GetLeaderID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentLeaderID
}

// IsLeader returns true if this node is currently the Raft leader.
func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == Leader
}

// AddPeer adds a new peer to the cluster. Idempotent: silently skips if the
// peer is already present. On the leader it also initialises per-peer
// replication state so the heartbeat loop begins replicating immediately.
func (r *Raft) AddPeer(p Peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.Peers {
		if existing.ID == p.ID {
			return
		}
	}
	r.Peers = append(r.Peers, p)
	if r.state == Leader {
		r.nextIndex[p.ID] = r.getLastLogIndex() + 1
		r.matchIndex[p.ID] = 0
	}
	log.Printf("[Raft %s] Added peer %s (%s) to cluster", r.ID, p.ID, p.Address)
}

// RecordPeerAdmin stores a discovered peer admin URL (exported for use outside the package).
func (r *Raft) RecordPeerAdmin(nodeID, addr string) {
	r.recordPeerAdmin(nodeID, addr)
}

// ActivePeerIDs returns the IDs of peers that responded to heartbeats within the given threshold.
func (r *Raft) ActivePeerIDs(threshold time.Duration) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	var active []string
	for id, lastSeen := range r.peerLastSeen {
		if now.Sub(lastSeen) <= threshold {
			active = append(active, id)
		}
	}
	return active
}

// Helper methods (assuming caller holds r.mu)
func (r *Raft) getLastLogIndex() int {
	return r.log[len(r.log)-1].Index
}

func (r *Raft) getLastLogTerm() int {
	return r.log[len(r.log)-1].Term
}

// applyCommitted pushes committed entries to the apply channel.
// Must be called with lock held.
func (r *Raft) applyCommitted() {
	if r.commitIndex > r.lastApplied {
		entriesToApply := append([]LogEntry{}, r.log[r.lastApplied+1:r.commitIndex+1]...)
		
		go func(entries []LogEntry) {
			for _, entry := range entries {
				r.ApplyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				
				r.mu.Lock()
				if r.lastApplied < entry.Index {
					r.lastApplied = entry.Index
				}
				r.mu.Unlock()
			}
		}(entriesToApply)
	}
}
