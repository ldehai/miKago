# 🚀 miKago

**mini Kafka with Go** — A Kafka-compatible message broker MVP written in Go.

## Features (Phase 1)

- ✅ Kafka binary protocol over TCP
- ✅ `ApiVersions` API (handshake & version negotiation)
- ✅ `Metadata` API (broker & topic discovery)
- ✅ `Produce` API (send messages with MessageSet v0)
- ✅ `Fetch` API (consume messages with offset tracking)
- ✅ In-memory topic/partition storage
- ✅ Auto topic creation on first access
- ✅ Graceful shutdown with signal handling

## Quick Start

```bash
# Build
make build

# Run (default: localhost:9092)
make run

# Or with custom options
./mikago -host 0.0.0.0 -port 9092 -broker-id 0
```

## Architecture

```
miKago/
├── cmd/mikago/         # Entry point
├── internal/
│   ├── protocol/       # Binary encoder/decoder, headers, API keys
│   ├── api/            # API handlers (ApiVersions, Metadata, Produce, Fetch)
│   ├── broker/         # Broker config, topic/partition management
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

- [ ] **Phase 2**: Disk persistence (segment files + index)
- [ ] **Phase 3**: Multiple partitions & consumer groups
- [ ] **Phase 4**: Multi-broker replication

## License

MIT
