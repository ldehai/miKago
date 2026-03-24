```text
            _ _  __                 
  _ __ ___ (_) |/ /__ _  __ _  ___  
 | '_ ' _ \| | ' // _' |/ _' |/ _ \ 
 | | | | | | | . \ (_| | (_| | (_) |
 |_| |_| |_|_|_|\_\__,_|\__, |\___/ 
                        |___/       
   mini Kafka with Go
```

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
*   **Zero-Copy Network I/O (`sendfile`)**
    To maximize consumer throughput, the `Fetch` API utilizes OS-level `sendfile` zero-copy system calls. Data streams directly from the disk's Page Cache into the network socket buffer, completely bypassing user-space memory copying and reducing CPU overhead to almost zero.
*   **Built-in Observability**
    Every broker ships with a zero-overhead metrics engine and a built-in Web Dashboard. Open `http://localhost:8080` in your browser for real-time charts, partition leader distribution, consumer group lag, and Raft state — no external monitoring stack required.
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
| **Observability** | External JMX + Prometheus exporters | Built-in Dashboard + `/metrics` endpoint |

---

## 🧠 Distributed Consensus (Raft Engine)

At the heart of miKago's distributed capabilities lies a **from-scratch implementation of the Raft Consensus Algorithm**.

Instead of delegating cluster state and partition leadership to an external system like ZooKeeper, miKago embeds a fully asynchronous, goroutine-safe Raft engine.
* **Leader Election**: Brokers use randomized timers and `RequestVote` RPCs to dynamically elect a cluster leader, capable of auto-recovering from network partitions or broker crashes.
* **Log Replication**: Every Kafka `Produce` payload is converted into a Raft `LogEntry`. The Leader broadcasts these blocks across the cluster using `AppendEntries` heartbeats.
* **Strong Consistency**: A message is only acknowledged back to the Kafka producer once it has safely replicated beyond the cluster's majority quorum. It is then securely dispatched down to the `Partition.Append()` storage layer via an abstracted core `ApplyCh` channel.
* **Zero-Dep RPC**: Inter-broker Raft communication is engineered entirely over the native `net/rpc` standard library utilizing `encoding/gob` for blazing-fast interface serialization.

---

## 📊 Built-in Observability

Every miKago broker ships with a zero-overhead metrics engine and a self-contained Web Dashboard. No Prometheus server, no Grafana, no JMX — just open a browser.

### Web Dashboard

Start the broker and navigate to **`http://localhost:8080`** (or whatever `--admin-port` is set to):

```bash
./mikago -port 9092 -admin-port 8080
```

The dashboard auto-refreshes every 2 seconds and shows:

| Panel | What you see |
|-------|-------------|
| **Overview cards** | Active connections, topic count, partition count, Raft term, election counter, produce req/s, fetch req/s, bytes-in rate |
| **Request Rate chart** | Real-time line chart of Produce and Fetch requests per second (last 60 samples) |
| **Throughput chart** | Bytes In/s and Bytes Out/s over time |
| **Produce Latency chart** | P50 / P95 / P99 latency of Produce operations in milliseconds |
| **Partitions table** | Per-partition: topic, partition ID, leader broker, high-water mark, total messages produced, total bytes produced |
| **Consumer Group Lag table** | Per group/topic/partition: committed offset, HWM, and lag (colour-coded green/orange/red) |

![Dashboard dark theme with charts, partition table and consumer group lag](.github/dashboard-preview.png)

> The dashboard is a single self-contained HTML page with no external JavaScript or CSS dependencies. It is safe to serve behind an internal firewall.

### Prometheus Endpoint

Scrape the `/metrics` endpoint with Prometheus (text format, no third-party library required):

```
http://localhost:8080/metrics
```

Example output:

```
# HELP mikago_produce_requests_total Total produce requests received
# TYPE mikago_produce_requests_total counter
mikago_produce_requests_total 48291

# HELP mikago_bytes_in_total Total bytes received from producers
# TYPE mikago_bytes_in_total counter
mikago_bytes_in_total 49450496

# HELP mikago_active_connections Current active client connections
# TYPE mikago_active_connections gauge
mikago_active_connections 3

# HELP mikago_produce_latency_p99_ms Produce latency P99 ms
# TYPE mikago_produce_latency_p99_ms gauge
mikago_produce_latency_p99_ms 1.823

mikago_partition_messages_in_total{topic="events",partition="0"} 48291
mikago_partition_bytes_in_total{topic="events",partition="0"} 49450496
```

Sample `prometheus.yml` scrape config:

```yaml
scrape_configs:
  - job_name: mikago
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: /metrics
```

### JSON Metrics API

Fetch a machine-readable snapshot as JSON:

```bash
curl -s http://localhost:8080/api/metrics | jq .
```

<details>
<summary>Example JSON response</summary>

```json
{
  "broker_id": 0,
  "timestamp_ms": 1711234567890,
  "produce_requests": 48291,
  "fetch_requests": 12043,
  "messages_in": 48291,
  "messages_out": 12043,
  "bytes_in": 49450496,
  "bytes_out": 12332032,
  "active_connections": 3,
  "raft_term": 2,
  "raft_elections": 1,
  "produce_p50_ms": 0.412,
  "produce_p95_ms": 1.105,
  "produce_p99_ms": 1.823,
  "fetch_p50_ms": 0.198,
  "fetch_p95_ms": 0.603,
  "fetch_p99_ms": 1.241,
  "topics": [
    {
      "name": "events",
      "partitions": [
        {
          "id": 0,
          "hwm": 48291,
          "leader_broker_id": 0,
          "messages_in": 48291,
          "bytes_in": 49450496,
          "bytes_out": 12332032
        }
      ]
    }
  ],
  "consumer_groups": [
    {
      "group_id": "my-consumer",
      "topic": "events",
      "partition": 0,
      "committed_offset": 47950,
      "hwm": 48291,
      "lag": 341
    }
  ]
}
```

</details>

### Cluster-wide Dashboard

When running a multi-broker cluster, the dashboard **automatically discovers all peer nodes** — no extra flags needed. Each broker advertises its admin URL through the existing Raft heartbeat RPCs. Within ~100 ms of startup, every node knows every other node's admin address.

```bash
# Node 1 — admin on :9181, auto-discovers peers via Raft
./mikago -broker-id 1 -port 9091 -admin-port 9181 -data-dir ./data1 \
  -raft-port 8001 -peers "2@localhost:8002,3@localhost:8003"

# Node 2 — admin on :9182
./mikago -broker-id 2 -port 9092 -admin-port 9182 -data-dir ./data2 \
  -raft-port 8002 -peers "1@localhost:8001,3@localhost:8003"

# Node 3 — admin on :9183
./mikago -broker-id 3 -port 9093 -admin-port 9183 -data-dir ./data3 \
  -raft-port 8003 -peers "1@localhost:8001,2@localhost:8002"
```

Open **any one** of `http://localhost:9181`, `:9182`, or `:9183` — the dashboard shows all 3 nodes together automatically.

**How auto-discovery works:** Each broker sets its admin URL (`http://host:admin-port`) into the Raft `AppendEntries` heartbeat args. Followers echo back their own URL in the reply. The leader then gossips all known URLs to every follower in the next heartbeat. After two heartbeat cycles (~100 ms), every node has a complete peer map — no coordination service or manual config required.

**What the cluster dashboard shows:**
- **Node tabs** — click to switch the detail view to any broker; offline nodes are greyed out
- **Cluster Nodes strip** — status card per node (connections, topics, Raft term)
- **Detail section** — request rate / throughput / latency charts, partition table, and consumer group lag for the selected node

The `/api/cluster` endpoint is also queryable directly:

```bash
curl -s http://localhost:9181/api/cluster | jq '.[].broker_id, .[].online'
```

> **Tip:** `--cluster-admin` is still accepted as a manual override for edge cases where Raft peers are on a different network from the admin HTTP interface.

### Admin Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--admin-port` | `8080` | HTTP port for Dashboard, `/metrics`, and `/api/metrics`. Set to `0` to disable. |
| `--cluster-admin` | _(empty)_ | Optional: manually override peer admin URLs (e.g. `http://host1:8081,http://host2:8082`). Usually not needed — peers are auto-discovered via Raft. |

The admin port is completely independent of the Kafka protocol port and adds no overhead to the message hot path — all metric writes are single atomic operations.

---

## 🚀 Quick Start

### 1. Build from Source
```bash
make build
```

### 2. Run a Single Node
```bash
# Starts on default localhost:9092, admin dashboard on :8080
make run

# With custom limits and paths
./mikago -host 0.0.0.0 -port 9092 -broker-id 1 -data-dir ./data -max-message-bytes 1048576

# Disable the admin server
./mikago -port 9092 -admin-port 0
```

Open **http://localhost:8080** in your browser to see the live dashboard.

### 3. Spin Up a 3-Node Distributed Cluster
Start three separate broker processes pointing to each other via internal Raft RPC ports:
```bash
# Node 1 — dashboard on :9181, peers auto-discovered via Raft heartbeat
./mikago -broker-id 1 -port 9091 -admin-port 9181 -data-dir ./data1 \
  -raft-port 8001 -peers "2@localhost:8002,3@localhost:8003"

# Node 2 — dashboard on :9182
./mikago -broker-id 2 -port 9092 -admin-port 9182 -data-dir ./data2 \
  -raft-port 8002 -peers "1@localhost:8001,3@localhost:8003"

# Node 3 — dashboard on :9183
./mikago -broker-id 3 -port 9093 -admin-port 9183 -data-dir ./data3 \
  -raft-port 8003 -peers "1@localhost:8001,2@localhost:8002"
```

Open **any one** of http://localhost:9181, :9182, or :9183 — within ~100 ms the dashboard auto-discovers all nodes and shows them together with node selector tabs and live status indicators.

---

## 🏗️ Architecture Stack

```text
miKago/
├── cmd/mikago/         # CLI entry point & flag parsing
├── internal/
│   ├── protocol/       # Binary encoder/decoder, message framing, Kafka constants
│   ├── api/            # API Handlers (Produce, Fetch, OffsetCommit, etc.)
│   ├── raft/           # Custom Raft Engine (Heartbeats, Leader Election, Log Appending)
│   ├── broker/         # Global Broker state, Topic mapping, Group Manager
│   ├── storage/        # File-system Segment/Index interactions & crash recovery
│   ├── server/         # TCP Socket Listener & Connection handling pool
│   ├── metrics/        # Zero-overhead atomic counters + latency ring buffers
│   └── admin/          # HTTP admin server: Dashboard, /metrics, /api/metrics
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

---

## ⚡ Performance Benchmark

miKago ships with two benchmark tools to measure write throughput.

### Benchmark Tool (`cmd/benchmark`)

Uses the standard `segmentio/kafka-go` client library. Ideal for testing real-world Kafka client compatibility.

```bash
# Build
go build -o benchmark ./cmd/benchmark/

# Basic run (10 workers, 100K messages, 1KB each, async mode)
./benchmark -concurrency 10 -num 100000 -msg-size 1024 -async=true -batch 100

# Multi-partition test (auto-creates topic with 10 partitions)
./benchmark -concurrency 10 -num 100000 -partitions 10
```

| Flag | Default | Description |
|------|---------|-------------|
| `-address` | `localhost:9092` | Broker address |
| `-topic` | `benchmark-topic` | Topic name |
| `-concurrency` | `10` | Number of concurrent workers |
| `-num` | `100000` | Total messages to produce |
| `-msg-size` | `1024` | Message size in bytes |
| `-async` | `true` | Async produce mode |
| `-batch` | `100` | Producer batch size |
| `-partitions` | `0` | Partitions (0 = auto-create default) |

### Raw TCP Benchmark (`cmd/rawbench`)

Uses the native Kafka binary protocol directly over raw TCP sockets. Bypasses all client-library overhead to measure the server's true throughput ceiling.

```bash
# Build
go build -o rawbench ./cmd/rawbench/

# Maximum throughput test (10 connections, 1M messages, 500 msgs/request)
./rawbench -workers 10 -num 1000000 -msg-size 1024 -batch 500

# Multi-partition test
./rawbench -workers 10 -num 1000000 -batch 500 -partitions 10
```

| Flag | Default | Description |
|------|---------|-------------|
| `-address` | `localhost:9092` | Broker address |
| `-topic` | `raw-bench` | Topic name |
| `-workers` | `10` | Concurrent TCP connections |
| `-num` | `100000` | Total messages to produce |
| `-msg-size` | `1024` | Message size in bytes |
| `-batch` | `500` | Messages per Produce request |
| `-partitions` | `1` | Number of partitions |

### Results (MacBook Pro, Apple Silicon, 1KB messages)

| Scenario | QPS | Throughput |
|:---------|:----|:-----------|
| kafka-go, 1 partition, async | 884,006 msgs/sec | 863 MB/sec |
| kafka-go, 10 partitions, async | 840,305 msgs/sec | 821 MB/sec |
| **Raw TCP, 1 partition** | **1,381,599 msgs/sec** | **1,349 MB/sec** |
| **Raw TCP, 10 partitions** | **1,537,908 msgs/sec** | **1,502 MB/sec** |

### Write Path Optimizations

miKago achieves high throughput through several key optimizations in the storage engine:

*   **Ring Buffer Batch Writer** — Concurrent producers push to a lock-free ring buffer; a single writer goroutine pops batches and writes them under one lock with one `Flush()` syscall.
*   **Batch Produce API** — `HandleProduce` sends an entire `MessageSet` as a single batch through the ring buffer (1 channel round-trip for N messages).
*   **Pooled Single-Write Encoding** — `EncodeRecord` uses `sync.Pool` buffers to combine header + key + value into a single `Write()` call.
*   **Buffered I/O** — Both log files (256KB buffer) and index files (4KB buffer) use `bufio.Writer` to minimize syscalls.
*   **Zero-Copy Fetch** — The `Fetch` API uses OS-level `sendfile` to stream data directly from disk to network socket.

## 🗺️ Project Roadmap

- [x] **Phase 1**: In-memory TCP protocol scaffold (`ApiVersions`, `Metadata`, `Produce`, `Fetch`)
- [x] **Phase 2**: Disk persistence (segment `.log` & `.index` files, crash recovery)
- [x] **Phase 3**: Multiple partitions & consumer groups
- [x] **Phase 4**: Multi-broker replication (Raft Data Log Replication)
- [x] **Phase 5**: Zero-Copy Network I/O (using `sendfile` for `Fetch` API to bypass user-space memory)
- [x] **Phase 6**: Partition-Level Leadership (Controller-based per-partition leader election; leaders distributed evenly across brokers; automatic failover via `Raft.Stop()` + re-election)
- [x] **Phase 7**: Built-in Observability (atomic metrics engine, Web Dashboard, Prometheus `/metrics`, JSON `/api/metrics`)
- [ ] **Phase 8**: Exactly-Once Semantics (EOS) / Idempotent Producers
- [ ] **Phase 9**: Security (TLS/SSL Network Encryption & SASL Authentication/ACLs)

## License

MIT
