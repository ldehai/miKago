# 🚀 miKago

**mini Kafka with Go** — A Kafka-compatible message broker MVP written in Go.

## Features

- ✅ Kafka binary protocol over TCP
- ✅ `ApiVersions` API (handshake & version negotiation)
- ✅ `Metadata` API (broker & topic discovery)
- ✅ `Produce` API (send messages with MessageSet v0)
- ✅ `Fetch` API (consume messages with offset tracking)
- ✅ In-memory topic/partition storage
- ✅ Auto topic creation on first access
- ✅ Graceful shutdown with signal handling
- ✅ **[Phase 4]** Raft Consensus Engine for Multi-Broker Clustering

## Quick Start

```bash
# Build
make build

# Run (default: localhost:9092)
make run

# Run a Raft Distributed Cluster (3 nodes)
./mikago -broker-id 1 -port 9091 -data-dir ./data1 -raft-port 8001 -peers "2@localhost:8002,3@localhost:8003"
./mikago -broker-id 2 -port 9092 -data-dir ./data2 -raft-port 8002 -peers "1@localhost:8001,3@localhost:8003"
./mikago -broker-id 3 -port 9093 -data-dir ./data3 -raft-port 8003 -peers "1@localhost:8001,2@localhost:8002"
```

## Architecture

```
miKago/
├── cmd/mikago/         # Entry point
├── internal/
│   ├── protocol/       # Binary encoder/decoder, headers, API keys
│   ├── api/            # API handlers (ApiVersions, Metadata, Produce, Fetch)
│   ├── raft/           # Raft consensus engine (Leader election, Log replication)
│   ├── broker/         # Broker config, topic/partition management
│   ├── storage/        # Disk persistence (segments, indexes)
│   └── server/         # TCP server with Kafka framing
└── tests/              # Integration tests
```

## Testing

```bash
# Run all tests
make test

# Run with verbose output
go test ./... -v
```

## Supported Kafka APIs

| API | Key | Versions | Description |
|-----|-----|----------|-------------|
| Produce | 0 | v0 | Send messages to topics |
| Fetch | 1 | v0 | Consume messages from topics |
| Metadata | 3 | v0 | Discover broker/topic info |
| ApiVersions | 18 | v0-v1 | Version negotiation |

## Roadmap

- [x] **Phase 2**: Disk persistence (segment files + index)
- [x] **Phase 3**: Multiple partitions & consumer groups
- [ ] **Phase 4**: Multi-broker replication

## License

MIT
