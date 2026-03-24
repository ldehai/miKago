package broker

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/andy/mikago/internal/raft"
)

// Default configuration values matching Kafka defaults.
const (
	DefaultMaxMessageBytes int32 = 1 * 1024 * 1024         // 1MB - message.max.bytes
	DefaultMaxRequestBytes int32 = 100 * 1024 * 1024       // 100MB - max request size on the wire
	DefaultLogSegmentBytes int64 = 1 * 1024 * 1024 * 1024  // 1GB - log.segment.bytes
	DefaultRetentionMs     int64 = 7 * 24 * 60 * 60 * 1000 // 7 days
	DefaultNumPartitions   int32 = 1                       // num.partitions
)

// ClusterBroker describes a broker's Kafka endpoint for metadata responses.
type ClusterBroker struct {
	ID   int32
	Host string
	Port int32
}

// Config holds the broker configuration.
type Config struct {
	BrokerID             int32
	Host                 string
	Port                 int32
	DataDir              string
	MaxMessageBytes      int32
	MaxRequestBytes      int32
	LogSegmentBytes      int64
	RetentionMs          int64 // log.retention.ms, -1 = keep forever
	DefaultNumPartitions int32 // default number of partitions for auto-created topics
	RaftPort             int32
	RaftPeers            []raft.Peer
	// KnownBrokers lists all brokers in the cluster (including self) for metadata and
	// partition leader assignment. If empty, only self is known.
	KnownBrokers []ClusterBroker
}

// Broker is the main broker instance.
type Broker struct {
	Config       Config
	TopicManager *TopicManager
	GroupManager *GroupManager
	Raft         *raft.Raft
	RaftServer   *raft.NetServer
	Controller   *PartitionController
}

// NewBroker creates a new Broker with the given configuration.
func NewBroker(cfg Config) *Broker {
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = DefaultMaxMessageBytes
	}
	if cfg.MaxRequestBytes <= 0 {
		cfg.MaxRequestBytes = DefaultMaxRequestBytes
	}
	if cfg.LogSegmentBytes <= 0 {
		cfg.LogSegmentBytes = DefaultLogSegmentBytes
	}
	if cfg.RetentionMs == 0 {
		cfg.RetentionMs = DefaultRetentionMs
	}
	if cfg.DefaultNumPartitions <= 0 {
		cfg.DefaultNumPartitions = DefaultNumPartitions
	}

	var rf *raft.Raft
	var rs *raft.NetServer
	if cfg.RaftPort > 0 {
		peerID := fmt.Sprintf("%d", cfg.BrokerID)
		rf = raft.NewRaft(peerID, cfg.RaftPeers)
		var err error
		rs, err = raft.StartRaftServer(rf, int(cfg.RaftPort))
		if err != nil {
			log.Fatalf("[miKago] Failed to start Raft server: %v", err)
		}
	}

	b := &Broker{
		Config:       cfg,
		TopicManager: NewTopicManager(cfg.DataDir, cfg.LogSegmentBytes, cfg.RetentionMs, int(cfg.DefaultNumPartitions)),
		GroupManager: NewGroupManager(),
		Raft:         rf,
		RaftServer:   rs,
		Controller:   NewPartitionController(cfg.BrokerID),
	}

	if b.Raft != nil {
		b.Raft.OnLeaderChange = b.onRaftLeaderChange
		go b.runStateMachine()
	}

	return b
}

// onRaftLeaderChange is the callback invoked when this node's Raft leadership changes.
// When isLeader=true this node is the new Controller and must assign partition leaders.
func (b *Broker) onRaftLeaderChange(isLeader bool) {
	if !isLeader {
		return
	}
	log.Printf("[Broker %d] Became Raft leader (Controller). Waiting for heartbeat loop...", b.Config.BrokerID)

	// Block until runLeader() has started its heartbeat loop (heartbeatTimer is live).
	// This replaces the old time.Sleep(200ms) with an explicit synchronization point.
	if !b.Raft.WaitLeaderReady(500 * time.Millisecond) {
		log.Printf("[Broker %d] Timed out waiting for leader ready; skipping partition assignment", b.Config.BrokerID)
		return
	}

	b.assignPartitionLeaders()
}

// assignPartitionLeaders distributes all known partition leaders evenly across active brokers.
// Called only by the current Raft leader (Controller).
func (b *Broker) assignPartitionLeaders() {
	activeBrokerIDs := b.getActiveBrokerIDs()
	if len(activeBrokerIDs) == 0 {
		activeBrokerIDs = []int32{b.Config.BrokerID}
	}

	topics := b.TopicManager.AllTopics()
	var assignments []raft.PartitionLeaderAssignment
	idx := 0
	for _, topic := range topics {
		for _, partition := range topic.Partitions {
			brokerID := activeBrokerIDs[idx%len(activeBrokerIDs)]
			assignments = append(assignments, raft.PartitionLeaderAssignment{
				Topic:       topic.Name,
				PartitionID: partition.ID(),
				BrokerID:    brokerID,
			})
			idx++
		}
	}

	if len(assignments) == 0 {
		log.Printf("[Broker %d] No partitions to assign yet", b.Config.BrokerID)
		return
	}

	_, _, ok := b.Raft.Propose(raft.LeaderAssignmentCmd{Assignments: assignments})
	if !ok {
		log.Printf("[Broker %d] Failed to propose partition assignments (no longer leader?)", b.Config.BrokerID)
		return
	}

	log.Printf("[Broker %d] Controller: assigned %d partition(s) across %d broker(s): %v",
		b.Config.BrokerID, len(assignments), len(activeBrokerIDs), activeBrokerIDs)
}

// getActiveBrokerIDs returns the sorted list of broker IDs to use for partition assignment.
// Includes self always. Includes peers that have responded to heartbeats recently, or falls
// back to all configured brokers if no health data is available yet.
func (b *Broker) getActiveBrokerIDs() []int32 {
	result := map[int32]bool{b.Config.BrokerID: true}

	// Peers that responded within the last second.
	activePeerIDs := b.Raft.ActivePeerIDs(time.Second)
	activePeerSet := make(map[string]bool, len(activePeerIDs))
	for _, id := range activePeerIDs {
		activePeerSet[id] = true
	}

	for _, kb := range b.Config.KnownBrokers {
		if kb.ID == b.Config.BrokerID {
			continue
		}
		peerID := fmt.Sprintf("%d", kb.ID)
		// If we have no health data at all (fresh leader), include all configured brokers.
		if len(activePeerIDs) == 0 || activePeerSet[peerID] {
			result[kb.ID] = true
		}
	}

	ids := make([]int32, 0, len(result))
	for id := range result {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// AllBrokers returns all known cluster brokers (always includes self).
func (b *Broker) AllBrokers() []ClusterBroker {
	selfIncluded := false
	result := make([]ClusterBroker, 0, len(b.Config.KnownBrokers)+1)
	for _, kb := range b.Config.KnownBrokers {
		result = append(result, kb)
		if kb.ID == b.Config.BrokerID {
			selfIncluded = true
		}
	}
	if !selfIncluded {
		result = append(result, ClusterBroker{
			ID:   b.Config.BrokerID,
			Host: b.Config.Host,
			Port: b.Config.Port,
		})
	}
	return result
}

// runStateMachine listens for committed log entries from the Raft consensus engine
// and applies them to the local node's state machine (disk storage, partition leader map, etc).
func (b *Broker) runStateMachine() {
	for msg := range b.Raft.ApplyCh {
		if !msg.CommandValid {
			continue
		}

		switch cmd := msg.Command.(type) {

		case raft.ReplicateCmd:
			// Legacy data-replication path (kept for backward compatibility).
			log.Printf("[Broker %d] Applying ReplicateCmd(Index=%d, Topic=%s, Partition=%d, Size=%dB)",
				b.Config.BrokerID, msg.CommandIndex, cmd.Topic, cmd.PartitionID, len(cmd.RecordSet))
			topic := b.TopicManager.GetOrCreateTopic(cmd.Topic)
			if int(cmd.PartitionID) < len(topic.Partitions) {
				topic.Partitions[cmd.PartitionID].Append(nil, cmd.RecordSet)
			}

		case raft.LeaderAssignmentCmd:
			// Control-plane path: update partition leader assignments on all brokers.
			b.Controller.ApplyAssignments(cmd.Assignments)
			log.Printf("[Broker %d] Applied %d partition leader assignment(s) from Raft log",
				b.Config.BrokerID, len(cmd.Assignments))

		default:
			log.Printf("[Broker %d] Unknown command type in Raft log (index=%d)", b.Config.BrokerID, msg.CommandIndex)
		}
	}
}

// Close shuts down the broker, flushing all data to disk.
func (b *Broker) Close() error {
	if b.RaftServer != nil {
		b.RaftServer.Close()
	}
	return b.TopicManager.Close()
}
