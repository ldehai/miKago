package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

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
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	ApplyCh        chan ApplyMsg
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
		ID:          id,
		Peers:       peers,
		state:       Follower,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]LogEntry, 1), // Dummy entry at index 0 for 1-based indexing simplicity
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]int),
		matchIndex:  make(map[string]int),
		ApplyCh:     make(chan ApplyMsg, 100),
	}

	// Start the election timer (random duration between 150ms and 300ms)
	r.electionTimer = time.NewTimer(randomElectionDuration())
	
	// Start the background loop to handle state transitions
	go r.run()

	return r
}

func randomElectionDuration() time.Duration {
	// Random election timeout between 150ms to 300ms
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// run is the main lifecycle loop for the Raft node.
func (r *Raft) run() {
	for {
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
	<-r.electionTimer.C

	// Election timeout elapsed without receiving heartbeat. Start an election.
	r.mu.Lock()
	r.becomeCandidate()
	r.mu.Unlock()
}

func (r *Raft) runCandidate() {
	r.mu.Lock()
	term := r.currentTerm
	lastLogIndex := r.getLastLogIndex()
	lastLogTerm := r.getLastLogTerm()
	r.mu.Unlock()

	// Send RequestVote to all peers
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  r.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
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

	// For now, if timeout hits again while candidate, we restart the election loop.
	<-r.electionTimer.C

	r.mu.Lock()
	if r.state == Candidate {
		r.becomeCandidate() // Start new election for new term
	}
	r.mu.Unlock()
}

func (r *Raft) runLeader() {
	// Start sending heartbeats immediately
	r.heartbeatTimer = time.NewTimer(0) // Fire instantly for the first time

	for r.state == Leader {
		<-r.heartbeatTimer.C

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

				args := AppendEntriesArgs{
					Term:         term,
					LeaderID:     r.ID,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}
				r.mu.Unlock()

				var reply AppendEntriesReply
				if SendAppendEntries(&p, &args, &reply) {
					r.mu.Lock()
					defer r.mu.Unlock()

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
	log.Printf("[Raft %s] Term %d: Becoming Follower", r.ID, term)
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	r.electionTimer.Reset(randomElectionDuration())
}

// becomeCandidate transit node to Candidate state and initiates an election.
// Assumes caller holds r.mu
func (r *Raft) becomeCandidate() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.ID
	log.Printf("[Raft %s] Term %d: Election timeout -> Becoming Candidate", r.ID, r.currentTerm)

	// Reset election timer so we don't block forever if election splits.
	r.electionTimer.Reset(randomElectionDuration())
}

// becomeLeader transit node to Leader state.
// Assumes caller holds r.mu
func (r *Raft) becomeLeader() {
	log.Printf("[Raft %s] Term %d: Election won! Becoming LEADER 👑", r.ID, r.currentTerm)
	r.state = Leader

	// Initialize volatile leader state
	lastIndex := r.getLastLogIndex()
	for _, peer := range r.Peers {
		r.nextIndex[peer.ID] = lastIndex + 1
		r.matchIndex[peer.ID] = 0
	}
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
