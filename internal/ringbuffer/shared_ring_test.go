package ringbuffer

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSharedRingBuffer_MultiReaderZeroCopy(t *testing.T) {
	srb := NewSharedRingBuffer(8)
	defer srb.Close()

	c1 := srb.NewReaderCursor("client-1", time.Second)
	c2 := srb.NewReaderCursor("client-2", time.Second)
	defer srb.RemoveReaderCursor("client-1")
	defer srb.RemoveReaderCursor("client-2")

	// Push 3 RTP packets
	for i := uint16(1); i <= 3; i++ {
		pkt := &rtp.Packet{
			Header:  rtp.Header{SequenceNumber: i, Timestamp: uint32(i * 1000)},
			Payload: []byte{byte(i), 0xAA, 0xBB},
		}
		srb.WritePacket(pkt)
	}

	ctx := context.Background()

	// Both cursors independently read the same packet instances without copying
	s1, err := c1.ReadNext(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint16(1), s1.Packet.SequenceNumber)

	s2, err := c2.ReadNext(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint16(1), s2.Packet.SequenceNumber)
	assert.Same(t, s1.Packet, s2.Packet, "packet instances should point to the exact same shared memory reference")

	// Read remaining
	for i := uint16(2); i <= 3; i++ {
		slot1, err := c1.ReadNext(ctx)
		require.NoError(t, err)
		assert.Equal(t, i, slot1.Packet.SequenceNumber)

		slot2, err := c2.ReadNext(ctx)
		require.NoError(t, err)
		assert.Equal(t, i, slot2.Packet.SequenceNumber)
	}
}

func TestSharedRingBuffer_ReaderLagAndScatterGather(t *testing.T) {
	// Small ring capacity: 4 slots
	srb := NewSharedRingBuffer(4)
	defer srb.Close()

	laggingReader := srb.NewReaderCursor("lag-client", time.Second)
	defer srb.RemoveReaderCursor("lag-client")

	// Push 10 packets -> causes wrap-around and drops oldest for lagging reader
	for i := uint16(1); i <= 10; i++ {
		pkt := &rtp.Packet{
			Header:  rtp.Header{SequenceNumber: i, Timestamp: uint32(i * 1000)},
			Payload: []byte{byte(i)},
		}
		srb.WritePacket(pkt)
	}

	ctx := context.Background()
	slot, err := laggingReader.ReadNext(ctx)
	require.NoError(t, err)

	// Since capacity is 4 and 10 packets written (head=10), oldest surviving index is 6 (seq=7)
	assert.True(t, laggingReader.DroppedCount() > 0, "lagging reader should have detected dropped packets")
	assert.Equal(t, uint16(7), slot.Packet.SequenceNumber)

	// Test scatter-gather non-blocking writev
	var buf bytes.Buffer
	slots := []PacketSlot{slot}
	n, err := laggingReader.WriteScatterGather(&buf, slots)
	require.NoError(t, err)
	assert.True(t, n > 0)
	assert.Equal(t, len(slot.Raw), int(n))
}
