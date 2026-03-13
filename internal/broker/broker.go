package broker

import (
	"fmt"
	"log"

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
}

// Broker is the main broker instance.
type Broker struct {
	Config       Config
	TopicManager *TopicManager
	GroupManager *GroupManager
	Raft         *raft.Raft
	RaftServer   *raft.NetServer
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
	}

	if b.Raft != nil {
		go b.runStateMachine()
	}

	return b
}

// runStateMachine listens for committed log entries from the Raft consensus engine
// and applies them to the local node's state machine (disk storage, offset maps, etc).
func (b *Broker) runStateMachine() {
	for msg := range b.Raft.ApplyCh {
		if !msg.CommandValid {
			continue
		}

		// Decode the command and apply to local storage.
		// For MVP, we pass around a custom struct: ReplicateCmd
		cmd, ok := msg.Command.(raft.ReplicateCmd)
		if !ok {
			log.Printf("[Broker %d] Unknown command type received from Raft state machine", b.Config.BrokerID)
			continue
		}

		log.Printf("[Broker %d] Applying Raft Cmd(Index=%d, Topic=%s, Partition=%d, Size=%dbytes) to local Disk",
			b.Config.BrokerID, msg.CommandIndex, cmd.Topic, cmd.PartitionID, len(cmd.RecordSet))

		topic := b.TopicManager.GetOrCreateTopic(cmd.Topic)
		if int(cmd.PartitionID) < len(topic.Partitions) {
			topic.Partitions[cmd.PartitionID].Append(nil, cmd.RecordSet)
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
