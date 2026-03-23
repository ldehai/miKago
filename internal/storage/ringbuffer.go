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

type RingBuffer struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []*ProduceRequest
	head   int
	tail   int
	count  int
	size   int
	closed bool
}

func NewRingBuffer(size int) *RingBuffer {
	rb := &RingBuffer{
		items: make([]*ProduceRequest, size),
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

	rb.items[rb.tail] = req
	rb.tail = (rb.tail + 1) % rb.size
	rb.count++
	rb.cond.Signal()
	return true
}

func (rb *RingBuffer) PopBatch(maxBatch int) []*ProduceRequest {
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

	batch := make([]*ProduceRequest, batchSize)
	for i := 0; i < batchSize; i++ {
		batch[i] = rb.items[rb.head]
		rb.items[rb.head] = nil
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
