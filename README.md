# 🚀 miKago (mini Kafka with Go)

**miKago** is a lightweight, zero-dependency, distributed message broker written entirely in Go. It speaks the native Apache Kafka binary protocol, meaning you can connect to it using standard Kafka clients, but it compiles down to a single, lightning-fast Go binary.

## ✨ Key Features

*   **Self-Contained Distributed Architecture (Raft-Based)**
    Unlike classic Kafka which relies heavily on Apache ZooKeeper for cluster management, miKago uses its own **built-in Raft Consensus Engine**. It handles leader election, cluster discovery, and distributed log replication natively. No extra services to deploy, no JVM—just start the nodes and they form a cluster automatically.
*   **Disk-Backed Persistence Engine**
    Data isn't just kept in memory. miKago implements a true Kafka-style storage engine using append-only `.log` segment files and sparse `.index` files for `O(log N)` binary search lookups. It supports automatic segment rolling and time-based retention cleanup strategies.
*   **Multi-Partition & Consumer Groups**
    Full support for topics with multiple partitions for parallel throughput. Built-in `GroupManager` handles Consumer Group paradigms, answering `OffsetCommit` and `OffsetFetch` requests so clients can pause and resume workloads without data loss or duplication.
*   **Native Kafka Protocol Compatibility**
    Speaks the exact size-delimited binary TCP framing format used by Kafka. It decodes intricate Kafka Request Headers, `MessageSet` wrappers, and correlation IDs to guarantee compatibility with official Kafka client libraries.
*   **Zero-Dependency Core**
    Written purely using Go's standard library (`net`, `io`, `sync`, `net/rpc`). No heavy third-party vendor bloat.

## 🆚 miKago vs. Apache Kafka

| Feature | Apache Kafka | miKago |
| :--- | :--- | :--- |
| **Language** | Java / Scala | Go |
| **Runtime** | JVM (Heavy memory footprint) | Native Binary (Ultra-lightweight) |
| **Orchestration** | ZooKeeper (Legacy) / KRaft | Built-in Raft Consensus Engine |
| **API Compatibility** | 100% (The original) | Core Subset (Produce, Fetch, Metadata, etc.) |
| **Deployment** | Complex multi-tier setup | Drop-in single binary execution |

---

## 🚀 Quick Start

### 1. Build from Source
```bash
make build
```

### 2. Run a Single Node
```bash
# Starts on default localhost:9092
make run

# With custom limits and paths
./mikago -host 0.0.0.0 -port 9092 -broker-id 1 -data-dir ./data -max-message-bytes 1048576
```

### 3. Spin Up a 3-Node Distributed Cluster
Start three separate broker processes pointing to each other via internal Raft RPC ports:
```bash
# Node 1
./mikago -broker-id 1 -port 9091 -data-dir ./data1 -raft-port 8001 -peers "2@localhost:8002,3@localhost:8003"
# Node 2 
./mikago -broker-id 2 -port 9092 -data-dir ./data2 -raft-port 8002 -peers "1@localhost:8001,3@localhost:8003"
# Node 3
./mikago -broker-id 3 -port 9093 -data-dir ./data3 -raft-port 8003 -peers "1@localhost:8001,2@localhost:8002"
```

---

## 🏗️ Architecture Stack

```text
miKago/
├── cmd/mikago/         # CLI Entry point & flag parsing
├── internal/
│   ├── protocol/       # Binary encoder/decoder, message framing, Kafka constants
│   ├── api/            # API Handlers (Produce, Fetch, OffsetCommit, etc.)
│   ├── raft/           # Custom Raft Engine (Heartbeats, Leader Election, Log Appending)
│   ├── broker/         # Global Broker state, Topic mapping, Group Manager
│   ├── storage/        # File-system Segment/Index interactions & crash recovery
│   └── server/         # TCP Socket Listener & Connection handling pool
└── tests/              # E2E Integration tests simulating Kafka clients
```

## 🔌 Supported Kafka APIs

| API | Key | Versions | Description |
|-----|-----|----------|-------------|
| Produce | 0 | v0 | Send `MessageSet` payloads into topics/partitions |
| Fetch | 1 | v0 | Consume messages via offset tracking |
| Metadata | 3 | v0 | Cluster and Topic/Leader discovery |
| OffsetCommit | 8 | v0 | Save Consumer Group progress checkpoints |
| OffsetFetch | 9 | v1 | Retrieve Consumer Group checkpoints |
| FindCoordinator | 10 | v0 | Broker lookup for consumer group tasks |
| ApiVersions | 18 | v0-v1 | Client capability handshake |
| CreateTopics | 19 | v0-v1 | Explicit topic creation with custom partition counts |

## 🗺️ Project Roadmap

- [x] **Phase 1**: In-memory TCP protocol scaffold (`ApiVersions`, `Metadata`, `Produce`, `Fetch`)
- [x] **Phase 2**: Disk persistence (segment `.log` & `.index` files, crash recovery)
- [x] **Phase 3**: Multiple partitions & consumer groups
- [ ] **Phase 4**: Multi-broker replication (Raft Data Log Replication)

## License

MIT
