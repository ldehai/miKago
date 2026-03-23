package storage

import (
	"sync"
)

type ProduceRequest struct {
	Key        []byte
	Value      []byte
	ResultChan chan ProduceResult
}

type ProduceResult struct {
	Offset int64
	Err    error
}

// BatchProduceRequest allows pushing multiple records as a single ring buffer item.
// All records share a single result channel to avoid per-message channel overhead.
type BatchProduceRequest struct {
	Keys       [][]byte
	Values     [][]byte
	ResultChan chan BatchProduceResult
}

type BatchProduceResult struct {
	BaseOffset int64
	Err        error
}

// RingItem is a union type for individual and batch produce requests.
type RingItem struct {
	Single *ProduceRequest
	Batch  *BatchProduceRequest
}

type RingBuffer struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []RingItem
	head   int
	tail   int
	count  int
	size   int
	closed bool
}

func NewRingBuffer(size int) *RingBuffer {
	rb := &RingBuffer{
		items: make([]RingItem, size),
		size:  size,
	}
	rb.cond = sync.NewCond(&rb.mu)
	return rb
}

func (rb *RingBuffer) Push(req *ProduceRequest) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for rb.count == rb.size && !rb.closed {
		rb.cond.Wait()
	}
	if rb.closed {
		return false
	}

	rb.items[rb.tail] = RingItem{Single: req}
	rb.tail = (rb.tail + 1) % rb.size
	rb.count++
	rb.cond.Signal()
	return true
}

// PushBatch pushes an entire batch as a single ring buffer item.
func (rb *RingBuffer) PushBatch(req *BatchProduceRequest) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for rb.count == rb.size && !rb.closed {
		rb.cond.Wait()
	}
	if rb.closed {
		return false
	}

	rb.items[rb.tail] = RingItem{Batch: req}
	rb.tail = (rb.tail + 1) % rb.size
	rb.count++
	rb.cond.Signal()
	return true
}

func (rb *RingBuffer) PopBatch(maxBatch int) []RingItem {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for rb.count == 0 && !rb.closed {
		rb.cond.Wait()
	}
	if rb.closed && rb.count == 0 {
		return nil
	}

	batchSize := rb.count
	if batchSize > maxBatch {
		batchSize = maxBatch
	}

	batch := make([]RingItem, batchSize)
	for i := 0; i < batchSize; i++ {
		batch[i] = rb.items[rb.head]
		rb.items[rb.head] = RingItem{}
		rb.head = (rb.head + 1) % rb.size
	}
	rb.count -= batchSize
	rb.cond.Signal()
	return batch
}

func (rb *RingBuffer) Close() {
	rb.mu.Lock()
	rb.closed = true
	rb.cond.Broadcast()
	rb.mu.Unlock()
}
