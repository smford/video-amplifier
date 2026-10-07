package ringbuffer

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
)

var (
	ErrCursorClosed = errors.New("shared ring buffer closed")
	ErrCursorLagged = errors.New("reader cursor lagged past oldest packet")
)

// PacketSlot represents a slot in the circular packet ring buffer.
type PacketSlot struct {
	Index  uint64      // Monotonically increasing absolute index
	Packet *rtp.Packet // Reference to the packet
	Raw    []byte      // Marshaled/raw wire bytes
}

// SharedRingBuffer is a single circular packet buffer in memory for incoming upstream RTP packets.
// Downstream consumers attach via lightweight reader cursors pointing directly to slots/slices,
// achieving zero-copy fan-out across all active clients.
type SharedRingBuffer struct {
	mu       sync.RWMutex
	cond     *sync.Cond
	slots    []PacketSlot
	capacity int
	head     uint64 // Next absolute index to write
	closed   bool

	// Active readers tracking cursors
	readersMu sync.RWMutex
	readers   map[string]*ReaderCursor
}

// ReaderCursor represents a lightweight cursor for a single downstream client.
type ReaderCursor struct {
	ID        string
	rb        *SharedRingBuffer
	cursor    uint64 // Current absolute packet index the reader is waiting to consume
	timeout   time.Duration
	lastSeen  atomic.Int64 // UnixNano
	lagMs     atomic.Int64 // Current lag in ms
	dropCount atomic.Uint64
}

// NewSharedRingBuffer allocates a circular packet buffer with the specified slot capacity.
func NewSharedRingBuffer(capacity int) *SharedRingBuffer {
	if capacity <= 0 {
		capacity = 512
	}
	srb := &SharedRingBuffer{
		slots:    make([]PacketSlot, capacity),
		capacity: capacity,
		readers:  make(map[string]*ReaderCursor),
	}
	srb.cond = sync.NewCond(&srb.mu)
	return srb
}

// Capacity returns the total slots in the circular buffer.
func (srb *SharedRingBuffer) Capacity() int {
	return srb.capacity
}

// WritePacket pushes an incoming upstream RTP packet into the shared circular buffer.
// It is non-blocking and overwrites the oldest slot when full, updating the head index.
// Packet memory is shared and only reclaimed when the circular buffer overwrites the slot.
func (srb *SharedRingBuffer) WritePacket(pkt *rtp.Packet) {
	if pkt == nil {
		return
	}

	raw, err := pkt.Marshal()
	if err != nil {
		return
	}

	srb.mu.Lock()
	if srb.closed {
		srb.mu.Unlock()
		return
	}

	idx := srb.head
	slotIdx := idx % uint64(srb.capacity)

	srb.slots[slotIdx] = PacketSlot{
		Index:  idx,
		Packet: pkt,
		Raw:    raw,
	}
	srb.head++

	srb.cond.Broadcast()
	srb.mu.Unlock()
}

// NewReaderCursor registers a downstream client and attaches a cursor.
func (srb *SharedRingBuffer) NewReaderCursor(id string, timeout time.Duration) *ReaderCursor {
	srb.mu.RLock()
	startIdx := srb.head
	srb.mu.RUnlock()

	cursor := &ReaderCursor{
		ID:      id,
		rb:      srb,
		cursor:  startIdx,
		timeout: timeout,
	}
	cursor.lastSeen.Store(time.Now().UnixNano())

	srb.readersMu.Lock()
	srb.readers[id] = cursor
	srb.readersMu.Unlock()

	return cursor
}

// RemoveReaderCursor unregisters a client cursor from the shared buffer.
func (srb *SharedRingBuffer) RemoveReaderCursor(id string) {
	srb.readersMu.Lock()
	delete(srb.readers, id)
	srb.readersMu.Unlock()
}

// Close closes the shared ring buffer and wakes up all waiting reader cursors.
func (srb *SharedRingBuffer) Close() {
	srb.mu.Lock()
	if !srb.closed {
		srb.closed = true
		srb.cond.Broadcast()
	}
	srb.mu.Unlock()
}

// ReadNext pops the next PacketSlot for this client cursor.
// If the reader lags behind the circular buffer's window (overwritten), it advances
// to the oldest surviving packet and increments the client's drop counter.
func (rc *ReaderCursor) ReadNext(ctx context.Context) (PacketSlot, error) {
	if err := ctx.Err(); err != nil {
		return PacketSlot{}, err
	}

	rc.rb.mu.Lock()
	defer rc.rb.mu.Unlock()

	for rc.cursor >= rc.rb.head && !rc.rb.closed && ctx.Err() == nil {
		// Wakeup helper
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				rc.rb.mu.Lock()
				rc.rb.cond.Broadcast()
				rc.rb.mu.Unlock()
			case <-done:
			}
		}()

		rc.rb.cond.Wait()
		close(done)
	}

	if ctx.Err() != nil {
		return PacketSlot{}, ctx.Err()
	}
	if rc.rb.closed {
		return PacketSlot{}, ErrCursorClosed
	}

	// Calculate oldest valid index in buffer
	var oldestIdx uint64
	if rc.rb.head > uint64(rc.rb.capacity) {
		oldestIdx = rc.rb.head - uint64(rc.rb.capacity)
	}

	now := time.Now()
	// Check if this reader lagged behind the circular buffer window
	if rc.cursor < oldestIdx {
		dropped := oldestIdx - rc.cursor
		rc.dropCount.Add(dropped)
		rc.cursor = oldestIdx
	}

	// Calculate lag in packets/time
	lagPackets := rc.rb.head - rc.cursor
	// Estimate lag ms assuming ~30fps (~33ms per packet)
	estimatedLagMs := int64(lagPackets * 33)
	rc.lagMs.Store(estimatedLagMs)

	slotIdx := rc.cursor % uint64(rc.rb.capacity)
	slot := rc.rb.slots[slotIdx]
	rc.cursor++
	rc.lastSeen.Store(now.UnixNano())

	return slot, nil
}

// WriteScatterGather writes a sequence of packet slots directly to a network connection
// using non-blocking scatter-gather I/O (net.Buffers / writev) with zero frame copying.
func (rc *ReaderCursor) WriteScatterGather(w io.Writer, slots []PacketSlot) (int64, error) {
	if len(slots) == 0 {
		return 0, nil
	}

	buffers := make(net.Buffers, len(slots))
	for i, slot := range slots {
		buffers[i] = slot.Raw
	}

	return buffers.WriteTo(w)
}

// LagMs returns the current calculated lag for this reader cursor in milliseconds.
func (rc *ReaderCursor) LagMs() int64 {
	return rc.lagMs.Load()
}

// DroppedCount returns the count of packets dropped for this reader cursor.
func (rc *ReaderCursor) DroppedCount() uint64 {
	return rc.dropCount.Load()
}
