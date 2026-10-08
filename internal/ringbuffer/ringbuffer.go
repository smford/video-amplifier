package ringbuffer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrQueueClosed  = errors.New("queue is closed")
	ErrClientLagged = errors.New("client lag exceeded maximum threshold")
)

// RingBuffer is a high-performance, bounded, non-blocking FIFO ring buffer.
// When the buffer is full, Push drops the oldest item without blocking the producer,
// preventing slow consumers from causing head-of-line blocking.
type RingBuffer[T any] struct {
	mu           sync.Mutex
	notEmpty     sync.Cond
	items        []T
	head         int
	tail         int
	count        int
	capacity     int
	closed       bool
	droppedCount uint64

	lastActivity atomic.Int64 // UnixNano timestamp of last consumer pop
	timeout      time.Duration
}

// NewRingBuffer initializes a bounded ring buffer with the given capacity.
func NewRingBuffer[T any](capacity int, clientTimeout time.Duration) *RingBuffer[T] {
	if capacity <= 0 {
		capacity = 60
	}
	rb := &RingBuffer[T]{
		items:    make([]T, capacity),
		capacity: capacity,
		timeout:  clientTimeout,
	}
	rb.notEmpty.L = &rb.mu
	rb.lastActivity.Store(time.Now().UnixNano())
	return rb
}

// Push adds an item to the buffer. If the buffer is full, it drops the oldest item
// and appends the new item, returning dropped=true. Push NEVER blocks.
func (rb *RingBuffer[T]) Push(item T) (dropped bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.closed {
		return false
	}

	if rb.count == rb.capacity {
		// Buffer is full: drop oldest element at head
		var zero T
		rb.items[rb.head] = zero // avoid memory leak
		rb.head = (rb.head + 1) % rb.capacity
		rb.count--
		rb.droppedCount++
		dropped = true
	}

	rb.items[rb.tail] = item
	rb.tail = (rb.tail + 1) % rb.capacity
	rb.count++

	rb.notEmpty.Signal()
	return dropped
}

// Pop retrieves the next available item. It blocks until an item is available,
// the context is cancelled, the buffer is closed, or the client lag timeout is exceeded.
func (rb *RingBuffer[T]) Pop(ctx context.Context) (item T, err error) {
	// Fast check context
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, err
	}

	// Channel to wake up sync.Cond if context is done
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			rb.mu.Lock()
			rb.notEmpty.Broadcast()
			rb.mu.Unlock()
		case <-done:
		}
	}()

	rb.mu.Lock()
	defer rb.mu.Unlock()

	for rb.count == 0 && !rb.closed && ctx.Err() == nil {
		rb.notEmpty.Wait()
	}

	if ctx.Err() != nil {
		var zero T
		return zero, ctx.Err()
	}

	if rb.count == 0 && rb.closed {
		var zero T
		return zero, ErrQueueClosed
	}

	// Check if client has lagged beyond timeout
	now := time.Now()
	lastAct := time.Unix(0, rb.lastActivity.Load())
	if rb.timeout > 0 && now.Sub(lastAct) > rb.timeout {
		var zero T
		return zero, ErrClientLagged
	}

	result := rb.items[rb.head]
	var zero T
	rb.items[rb.head] = zero // avoid retaining pointer
	rb.head = (rb.head + 1) % rb.capacity
	rb.count--

	rb.lastActivity.Store(now.UnixNano())
	return result, nil
}

// TryPop retrieves the next item without blocking.
func (rb *RingBuffer[T]) TryPop() (item T, ok bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.count == 0 || rb.closed {
		var zero T
		return zero, false
	}

	result := rb.items[rb.head]
	var zero T
	rb.items[rb.head] = zero
	rb.head = (rb.head + 1) % rb.capacity
	rb.count--

	rb.lastActivity.Store(time.Now().UnixNano())
	return result, true
}

// DroppedCount returns the total number of items dropped due to buffer overflow.
func (rb *RingBuffer[T]) DroppedCount() uint64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.droppedCount
}

// Snapshot returns a shallow copy of all currently buffered items in FIFO order.
func (rb *RingBuffer[T]) Snapshot() []T {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.count == 0 {
		return nil
	}

	result := make([]T, rb.count)
	for i := 0; i < rb.count; i++ {
		idx := (rb.head + i) % rb.capacity
		result[i] = rb.items[idx]
	}
	return result
}

// Len returns the current number of queued items.
func (rb *RingBuffer[T]) Len() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.count
}

// Close closes the ring buffer and wakes up any waiting consumers.
func (rb *RingBuffer[T]) Close() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if !rb.closed {
		rb.closed = true
		rb.notEmpty.Broadcast()
	}
}

// IsClosed returns true if the buffer has been closed.
func (rb *RingBuffer[T]) IsClosed() bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.closed
}
