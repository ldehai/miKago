package raft

import (
	"fmt"
)

// LogEntry represents a single operation/message in the Raft log.
type LogEntry struct {
	Term    int         // The term when this entry was received by the leader
	Index   int         // The logical index in the log
	Command interface{} // The actual data to be replicated (e.g., Produce records, Metadata)
}

// ==============================
//  RPC Definitions
// ==============================

// RequestVoteArgs contains the arguments for a RequestVote RPC.
type RequestVoteArgs struct {
	Term         int    // Candidate's term
	CandidateID  string // Candidate requesting vote
	LastLogIndex int    // Index of candidate's last log entry
	LastLogTerm  int    // Term of candidate's last log entry
}

// RequestVoteReply contains the reply for a RequestVote RPC.
type RequestVoteReply struct {
	Term        int  // Current term, for candidate to update itself
	VoteGranted bool // True means candidate received vote
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("RequestVote(Term:%d, Candidate:%s, LastLogIdx:%d, LastLogTerm:%d)",
		args.Term, args.CandidateID, args.LastLogIndex, args.LastLogTerm)
}

// AppendEntriesArgs contains the arguments for an AppendEntries RPC.
type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderID     string     // So follower can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commitIndex
}

// AppendEntriesReply contains the reply for an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int  // Current term, for leader to update itself
	Success bool // True if follower contained entry matching prevLogIndex and prevLogTerm
	
	// Fast backup (optimizations to avoid backtracking 1 by 1)
	ConflictTerm  int // The conflicting term
	ConflictIndex int // The first index it stores for the conflicting term
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("AppendEntries(Term:%d, Leader:%s, PrevLogIdx:%d, EntriesCnt:%d, CommitIdx:%d)",
		args.Term, args.LeaderID, args.PrevLogIndex, len(args.Entries), args.LeaderCommit)
}

// PartitionLeaderAssignment assigns a leader broker to a specific topic-partition.
type PartitionLeaderAssignment struct {
	Topic       string
	PartitionID int32
	BrokerID    int32
}

// LeaderAssignmentCmd is a Raft command that assigns partition leaders to brokers.
// Proposed by the Controller (Raft leader) and applied by all nodes.
type LeaderAssignmentCmd struct {
	Assignments []PartitionLeaderAssignment
}
