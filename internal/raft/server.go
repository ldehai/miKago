package raft

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

// RaftRPCWrapper adapts the Raft instance to net/rpc exported methods.
type RaftRPCWrapper struct {
	rf *Raft
}

func (rw *RaftRPCWrapper) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	// Record the candidate's admin addr before acquiring the main lock.
	rw.rf.recordPeerAdmin(args.CandidateID, args.AdminAddr)

	rw.rf.mu.Lock()
	defer rw.rf.mu.Unlock()

	log.Printf("[Raft %s] Received %v", rw.rf.ID, args)

	// Rule 1: Reply false if term < currentTerm
	if args.Term < rw.rf.currentTerm {
		reply.Term = rw.rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	// Rule 2: If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rw.rf.currentTerm {
		rw.rf.becomeFollower(args.Term)
	}

	// Logic for granting vote:
	// - We haven't voted for anyone yet in this term (or voted for the same candidate)
	// - Candidate's log is at least as up-to-date as receiver's log
	lastIdx := rw.rf.getLastLogIndex()
	lastTerm := rw.rf.getLastLogTerm()

	upToDate := false
	if args.LastLogTerm > lastTerm {
		upToDate = true
	} else if args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx {
		upToDate = true
	}

	if (rw.rf.votedFor == "" || rw.rf.votedFor == args.CandidateID) && upToDate {
		// Grant vote
		rw.rf.votedFor = args.CandidateID
		rw.rf.electionTimer.Reset(randomElectionDuration()) // Reset timer since we granted a vote

		reply.Term = rw.rf.currentTerm
		reply.VoteGranted = true
		log.Printf("[Raft %s] Granted vote to %s for term %d", rw.rf.ID, args.CandidateID, args.Term)
	} else {
		reply.Term = rw.rf.currentTerm
		reply.VoteGranted = false
	}

	return nil
}

func (rw *RaftRPCWrapper) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	// Gossip: record leader's admin addr and all known peer addrs (uses peerAdminMu, not main lock).
	rw.rf.recordPeerAdmin(args.LeaderID, args.AdminAddr)
	for nodeID, addr := range args.PeerAdminAddrs {
		rw.rf.recordPeerAdmin(nodeID, addr)
	}

	rw.rf.mu.Lock()
	defer rw.rf.mu.Unlock()

	// Echo back our own admin addr so the leader can discover us.
	reply.AdminAddr = rw.rf.localAdminAddr

	// Handle term checking and stepping down if needed
	if args.Term < rw.rf.currentTerm {
		reply.Term = rw.rf.currentTerm
		reply.Success = false
		return nil
	}

	// Recognize the leader and reset election timer
	rw.rf.electionTimer.Reset(randomElectionDuration())
	rw.rf.currentLeaderID = args.LeaderID // track who the current leader is

	if args.Term > rw.rf.currentTerm {
		rw.rf.becomeFollower(args.Term)
	} else if rw.rf.state == Candidate {
		// If we are candidate but received heartbeat from valid leader, step down
		rw.rf.becomeFollower(args.Term)
	}

	reply.Term = rw.rf.currentTerm

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > rw.rf.getLastLogIndex() {
		reply.Success = false
		return nil
	}

	if args.PrevLogIndex > 0 {
		termAtPrev := rw.rf.log[args.PrevLogIndex].Term
		if termAtPrev != args.PrevLogTerm {
			reply.Success = false
			return nil
		}
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	insertIndex := args.PrevLogIndex + 1
	newEntriesIndex := 0

	for {
		if insertIndex >= len(rw.rf.log) || newEntriesIndex >= len(args.Entries) {
			break
		}
		if rw.rf.log[insertIndex].Term != args.Entries[newEntriesIndex].Term {
			// Conflict: truncate log
			rw.rf.log = rw.rf.log[:insertIndex]
			break
		}
		insertIndex++
		newEntriesIndex++
	}

	// 4. Append any new entries not already in the log
	if newEntriesIndex < len(args.Entries) {
		rw.rf.log = append(rw.rf.log, args.Entries[newEntriesIndex:]...)
		log.Printf("[Raft %s] Appended %d entries from leader %s. Last Index: %d",
			rw.rf.ID, len(args.Entries)-newEntriesIndex, args.LeaderID, rw.rf.getLastLogIndex())
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rw.rf.commitIndex {
		lastNewIndex := rw.rf.getLastLogIndex()
		if args.LeaderCommit < lastNewIndex {
			rw.rf.commitIndex = args.LeaderCommit
		} else {
			rw.rf.commitIndex = lastNewIndex
		}
		log.Printf("[Raft %s] Updating commitIndex to %d", rw.rf.ID, rw.rf.commitIndex)
		rw.rf.applyCommitted()
	}

	reply.Success = true

	return nil
}

// NetServer wraps the net/http server and listener to gracefully shutdown.
type NetServer struct {
	listener net.Listener
	server   *http.Server
	wg       sync.WaitGroup
}

// StartRaftServer starts the RPC server for this Raft node.
func StartRaftServer(rf *Raft, port int) (*NetServer, error) {
	// Register all command types with gob so they can be sent over RPC within interface{}
	gob.Register(ReplicateCmd{})
	gob.Register(LeaderAssignmentCmd{})
	gob.Register(PartitionLeaderAssignment{})
	gob.Register(MembershipChangeCmd{})

	rpcServer := rpc.NewServer()
	
	wrapper := &RaftRPCWrapper{rf: rf}
	err := rpcServer.RegisterName("Raft", wrapper)
	if err != nil {
		return nil, fmt.Errorf("register raft rpc: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)

	addr := fmt.Sprintf(":%d", port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}

	ns := &NetServer{
		listener: l,
		server:   &http.Server{Handler: mux},
	}

	ns.wg.Add(1)
	go func() {
		defer ns.wg.Done()
		log.Printf("[Raft %s] Internal RPC listening on %s", rf.ID, addr)
		if err := ns.server.Serve(l); err != nil && err != http.ErrServerClosed {
			log.Printf("[Raft %s] Server error: %v", rf.ID, err)
		}
	}()

	return ns, nil
}

func (ns *NetServer) Close() error {
	return ns.server.Close()
}

// SendRequestVote calls the RequestVote RPC on a peer.
func SendRequestVote(peer *Peer, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	client, err := rpc.DialHTTP("tcp", peer.Address)
	if err != nil {
		return false
	}
	defer client.Close()

	err = client.Call("Raft.RequestVote", args, reply)
	return err == nil
}

// SendAppendEntries calls the AppendEntries RPC on a peer.
func SendAppendEntries(peer *Peer, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	client, err := rpc.DialHTTP("tcp", peer.Address)
	if err != nil {
		return false
	}
	defer client.Close()

	err = client.Call("Raft.AppendEntries", args, reply)
	return err == nil
}
