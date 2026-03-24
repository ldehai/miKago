// Package metrics provides zero-overhead instrumentation for the miKago broker.
// Hot-path writes use atomic operations only; all aggregation happens at read time.
package metrics

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// LatencySampler records latency samples in a pre-allocated ring buffer.
// Recording is lock-free (one atomic.Add + one atomic.Store, no heap allocation).
type LatencySampler struct {
	buf  []int64 // nanoseconds
	size int64
	pos  atomic.Int64
}

// NewLatencySampler creates a sampler with the given ring-buffer capacity.
func NewLatencySampler(size int) *LatencySampler {
	return &LatencySampler{buf: make([]int64, size), size: int64(size)}
}

// Record stores a sample on the hot path.
func (ls *LatencySampler) Record(d time.Duration) {
	i := ls.pos.Add(1) % ls.size
	atomic.StoreInt64(&ls.buf[i], int64(d))
}

// Percentiles returns P50/P95/P99 in milliseconds. Called only on the read path.
func (ls *LatencySampler) Percentiles() (p50, p95, p99 float64) {
	tmp := make([]int64, ls.size)
	for i := int64(0); i < ls.size; i++ {
		tmp[i] = atomic.LoadInt64(&ls.buf[i])
	}
	valid := tmp[:0]
	for _, v := range tmp {
		if v > 0 {
			valid = append(valid, v)
		}
	}
	n := len(valid)
	if n == 0 {
		return 0, 0, 0
	}
	sort.Slice(valid, func(i, j int) bool { return valid[i] < valid[j] })
	at := func(pct float64) float64 {
		idx := int(math.Floor(float64(n-1) * pct))
		if idx >= n {
			idx = n - 1
		}
		return float64(valid[idx]) / 1e6 // ns → ms
	}
	return at(0.50), at(0.95), at(0.99)
}

// partKey is the map key for per-partition stats.
type partKey struct {
	topic     string
	partition int32
}

// PartitionStats holds per-partition throughput counters (all atomic).
type PartitionStats struct {
	MessagesIn atomic.Int64
	BytesIn    atomic.Int64
	BytesOut   atomic.Int64
}

// Store is the central metrics registry. All hot-path writes use atomic ops;
// aggregation and formatting happen only at read time.
type Store struct {
	// Global throughput counters
	ProduceRequests atomic.Int64
	FetchRequests   atomic.Int64
	MessagesIn      atomic.Int64
	MessagesOut     atomic.Int64
	BytesIn         atomic.Int64
	BytesOut        atomic.Int64

	// Connection tracking
	ActiveConnections atomic.Int64

	// Raft state
	RaftTerm      atomic.Int64
	RaftElections atomic.Int64

	// Latency ring buffers (pre-allocated, no heap alloc on record)
	ProduceDurations *LatencySampler
	FetchDurations   *LatencySampler

	// Per-partition stats (lazily created)
	partMu     sync.RWMutex
	partitions map[partKey]*PartitionStats
}

// New creates a Store with pre-allocated ring buffers.
func New() *Store {
	return &Store{
		ProduceDurations: NewLatencySampler(1024),
		FetchDurations:   NewLatencySampler(1024),
		partitions:       make(map[partKey]*PartitionStats),
	}
}

// Default is the process-wide singleton store.
var Default = New()

// Partition returns (or lazily creates) per-partition stats.
// The fast path (RLock, map read, RUnlock) is lock-free under no contention.
func (s *Store) Partition(topic string, partition int32) *PartitionStats {
	k := partKey{topic: topic, partition: partition}
	s.partMu.RLock()
	ps, ok := s.partitions[k]
	s.partMu.RUnlock()
	if ok {
		return ps
	}
	s.partMu.Lock()
	defer s.partMu.Unlock()
	if ps, ok = s.partitions[k]; ok {
		return ps
	}
	ps = &PartitionStats{}
	s.partitions[k] = ps
	return ps
}

// PartitionSnapshot is a read-only view of one partition's counters.
type PartitionSnapshot struct {
	Topic      string `json:"topic"`
	Partition  int32  `json:"partition"`
	MessagesIn int64  `json:"messages_in"`
	BytesIn    int64  `json:"bytes_in"`
	BytesOut   int64  `json:"bytes_out"`
}

// Snapshot is a point-in-time view of all metrics.
type Snapshot struct {
	ProduceRequests   int64              `json:"produce_requests"`
	FetchRequests     int64              `json:"fetch_requests"`
	MessagesIn        int64              `json:"messages_in"`
	MessagesOut       int64              `json:"messages_out"`
	BytesIn           int64              `json:"bytes_in"`
	BytesOut          int64              `json:"bytes_out"`
	ActiveConnections int64              `json:"active_connections"`
	RaftTerm          int64              `json:"raft_term"`
	RaftElections     int64              `json:"raft_elections"`
	ProduceP50Ms      float64            `json:"produce_p50_ms"`
	ProduceP95Ms      float64            `json:"produce_p95_ms"`
	ProduceP99Ms      float64            `json:"produce_p99_ms"`
	FetchP50Ms        float64            `json:"fetch_p50_ms"`
	FetchP95Ms        float64            `json:"fetch_p95_ms"`
	FetchP99Ms        float64            `json:"fetch_p99_ms"`
	Partitions        []PartitionSnapshot `json:"partitions,omitempty"`
}

// Snapshot reads a point-in-time view of all metrics. Called only on the read path.
func (s *Store) Snapshot() Snapshot {
	snap := Snapshot{
		ProduceRequests:   s.ProduceRequests.Load(),
		FetchRequests:     s.FetchRequests.Load(),
		MessagesIn:        s.MessagesIn.Load(),
		MessagesOut:       s.MessagesOut.Load(),
		BytesIn:           s.BytesIn.Load(),
		BytesOut:          s.BytesOut.Load(),
		ActiveConnections: s.ActiveConnections.Load(),
		RaftTerm:          s.RaftTerm.Load(),
		RaftElections:     s.RaftElections.Load(),
	}
	snap.ProduceP50Ms, snap.ProduceP95Ms, snap.ProduceP99Ms = s.ProduceDurations.Percentiles()
	snap.FetchP50Ms, snap.FetchP95Ms, snap.FetchP99Ms = s.FetchDurations.Percentiles()

	s.partMu.RLock()
	defer s.partMu.RUnlock()
	for k, ps := range s.partitions {
		snap.Partitions = append(snap.Partitions, PartitionSnapshot{
			Topic:      k.topic,
			Partition:  k.partition,
			MessagesIn: ps.MessagesIn.Load(),
			BytesIn:    ps.BytesIn.Load(),
			BytesOut:   ps.BytesOut.Load(),
		})
	}
	return snap
}

// FormatPrometheus emits Prometheus text-format output (no third-party library).
func (s *Store) FormatPrometheus() string {
	snap := s.Snapshot()
	buf := make([]byte, 0, 2048)

	gauge := func(name, help string, val interface{}) {
		buf = append(buf, fmt.Sprintf(
			"# HELP %s %s\n# TYPE %s gauge\n%s %v\n",
			name, help, name, name, val)...)
	}
	counter := func(name, help string, val interface{}) {
		buf = append(buf, fmt.Sprintf(
			"# HELP %s %s\n# TYPE %s counter\n%s %v\n",
			name, help, name, name, val)...)
	}

	counter("mikago_produce_requests_total", "Total produce requests received", snap.ProduceRequests)
	counter("mikago_fetch_requests_total", "Total fetch requests received", snap.FetchRequests)
	counter("mikago_messages_in_total", "Total messages produced", snap.MessagesIn)
	counter("mikago_messages_out_total", "Total messages consumed", snap.MessagesOut)
	counter("mikago_bytes_in_total", "Total bytes received from producers", snap.BytesIn)
	counter("mikago_bytes_out_total", "Total bytes sent to consumers", snap.BytesOut)
	gauge("mikago_active_connections", "Current active client connections", snap.ActiveConnections)
	gauge("mikago_raft_term", "Current Raft term", snap.RaftTerm)
	counter("mikago_raft_elections_total", "Total Raft elections started", snap.RaftElections)
	gauge("mikago_produce_latency_p50_ms", "Produce latency P50 ms", fmt.Sprintf("%.3f", snap.ProduceP50Ms))
	gauge("mikago_produce_latency_p95_ms", "Produce latency P95 ms", fmt.Sprintf("%.3f", snap.ProduceP95Ms))
	gauge("mikago_produce_latency_p99_ms", "Produce latency P99 ms", fmt.Sprintf("%.3f", snap.ProduceP99Ms))
	gauge("mikago_fetch_latency_p50_ms", "Fetch latency P50 ms", fmt.Sprintf("%.3f", snap.FetchP50Ms))
	gauge("mikago_fetch_latency_p95_ms", "Fetch latency P95 ms", fmt.Sprintf("%.3f", snap.FetchP95Ms))
	gauge("mikago_fetch_latency_p99_ms", "Fetch latency P99 ms", fmt.Sprintf("%.3f", snap.FetchP99Ms))

	for _, ps := range snap.Partitions {
		lbl := fmt.Sprintf(`{topic=%q,partition="%d"}`, ps.Topic, ps.Partition)
		buf = append(buf, fmt.Sprintf("mikago_partition_messages_in_total%s %d\n", lbl, ps.MessagesIn)...)
		buf = append(buf, fmt.Sprintf("mikago_partition_bytes_in_total%s %d\n", lbl, ps.BytesIn)...)
		buf = append(buf, fmt.Sprintf("mikago_partition_bytes_out_total%s %d\n", lbl, ps.BytesOut)...)
	}

	return string(buf)
}
