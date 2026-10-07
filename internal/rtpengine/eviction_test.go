package rtpengine

import (
	"testing"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/pion/rtp"
)

func TestInspectPacket_H264(t *testing.T) {
	// 1. Single NALU IDR (type 5)
	idrPayload := []byte{0x25, 0x88, 0x84, 0x00} // nal_ref_idc=1, type=5
	info := InspectPacket(idrPayload, CodecH264)
	if info.Codec != CodecH264 {
		t.Fatalf("expected H264, got %v", info.Codec)
	}
	if !info.IsKeyframe {
		t.Fatalf("expected IDR to be keyframe")
	}
	if info.IsNonRef {
		t.Fatalf("expected IDR to not be non-ref")
	}

	// 2. SPS (type 7)
	spsPayload := []byte{0x67, 0x42, 0xc0, 0x1e} // nal_ref_idc=3, type=7
	infoSPS := InspectPacket(spsPayload, CodecH264)
	if !infoSPS.IsKeyframe || !infoSPS.ContainsSPS || !infoSPS.IsSPSorPPS {
		t.Fatalf("expected SPS detection: %+v", infoSPS)
	}

	// 3. PPS (type 8)
	ppsPayload := []byte{0x68, 0xce, 0x3c, 0x80}
	infoPPS := InspectPacket(ppsPayload, CodecH264)
	if !infoPPS.IsKeyframe || !infoPPS.ContainsPPS || !infoPPS.IsSPSorPPS {
		t.Fatalf("expected PPS detection: %+v", infoPPS)
	}

	// 4. Non-IDR reference P-frame (nal_ref_idc=2, type=1)
	refP := []byte{0x41, 0x9a, 0x00}
	infoRefP := InspectPacket(refP, CodecH264)
	if infoRefP.IsKeyframe || infoRefP.IsNonRef {
		t.Fatalf("expected reference P-frame: %+v", infoRefP)
	}

	// 5. Disposable B-frame (nal_ref_idc=0, type=1)
	nonRefB := []byte{0x01, 0x9a, 0x00}
	infoB := InspectPacket(nonRefB, CodecH264)
	if infoB.IsKeyframe || !infoB.IsNonRef {
		t.Fatalf("expected non-reference B-frame: %+v", infoB)
	}

	// 6. FU-A starting with IDR (indicator=28, header=start+5 = 0x85)
	fuaIDR := []byte{0x1C, 0x85, 0x00, 0x01}
	infoFUA := InspectPacket(fuaIDR, CodecH264)
	if !infoFUA.IsKeyframe {
		t.Fatalf("expected FU-A IDR to be keyframe: %+v", infoFUA)
	}
}

func TestInspectPacket_H265(t *testing.T) {
	// H.265 IDR (type 19 = IDR_W_RADL: (19 << 1) = 38 = 0x26)
	idrPayload := []byte{0x26, 0x01, 0xaf, 0x00}
	info := InspectPacket(idrPayload, CodecH265)
	if info.Codec != CodecH265 {
		t.Fatalf("expected H265, got %v", info.Codec)
	}
	if !info.IsKeyframe {
		t.Fatalf("expected H265 IDR to be keyframe")
	}

	// H.265 VPS (type 32: (32 << 1) = 64 = 0x40)
	vpsPayload := []byte{0x40, 0x01, 0x0c, 0x01}
	infoVPS := InspectPacket(vpsPayload, CodecH265)
	if !infoVPS.IsKeyframe || !infoVPS.ContainsVPS {
		t.Fatalf("expected VPS detection: %+v", infoVPS)
	}

	// H.265 Non-reference TRAIL_N (type 0: (0 << 1) = 0)
	trailN := []byte{0x00, 0x01, 0x02}
	infoTrailN := InspectPacket(trailN, CodecH265)
	if !infoTrailN.IsNonRef {
		t.Fatalf("expected TRAIL_N to be non-ref: %+v", infoTrailN)
	}
}

func TestClientEvictionTracker_Lifecycle(t *testing.T) {
	watermark := 1500 * time.Millisecond
	tracker := NewClientEvictionTracker("client-1", watermark, true)

	pkt := &rtp.Packet{Header: rtp.Header{SequenceNumber: 10}}
	pFrameInfo := FrameInfo{Codec: CodecH264, IsKeyframe: false, IsNonRef: false}
	bFrameInfo := FrameInfo{Codec: CodecH264, IsKeyframe: false, IsNonRef: true}
	keyInfo := FrameInfo{Codec: CodecH264, IsKeyframe: true, IsNonRef: false}

	// 1. Normal latency (500ms <= 1500ms): should forward all frames
	if !tracker.ShouldForward(pkt, 500*time.Millisecond, pFrameInfo) {
		t.Fatalf("expected normal forwarding")
	}
	if !tracker.ShouldForward(pkt, 500*time.Millisecond, bFrameInfo) {
		t.Fatalf("expected normal forwarding of B-frame")
	}

	// 2. Moderate congestion (1100ms > 1000ms degradation threshold): StateDegraded
	// Ref P-frame is kept
	if !tracker.ShouldForward(pkt, 1100*time.Millisecond, pFrameInfo) {
		t.Fatalf("expected P-frame to pass in degraded state")
	}
	// Non-ref B-frame should be DROPPED
	if tracker.ShouldForward(pkt, 1100*time.Millisecond, bFrameInfo) {
		t.Fatalf("expected non-ref B-frame to be dropped in degraded state")
	}
	if tracker.State != StateDegraded {
		t.Fatalf("expected state degraded, got %v", tracker.State)
	}

	// 3. Severe congestion (1600ms > 1500ms watermark): StateEvicted
	// P-frame should be DROPPED
	if tracker.ShouldForward(pkt, 1600*time.Millisecond, pFrameInfo) {
		t.Fatalf("expected P-frame to be dropped in evicted state")
	}
	if tracker.State != StateEvicted {
		t.Fatalf("expected state evicted, got %v", tracker.State)
	}

	// Trailing P and B frames must continue to be DROPPED
	if tracker.ShouldForward(pkt, 800*time.Millisecond, pFrameInfo) {
		t.Fatalf("expected trailing P-frame to be dropped while awaiting keyframe")
	}
	if tracker.ShouldForward(pkt, 800*time.Millisecond, bFrameInfo) {
		t.Fatalf("expected trailing B-frame to be dropped while awaiting keyframe")
	}

	// 4. Arrival of next keyframe (SPS/PPS + IDR): transitions back to StateForwarding!
	if !tracker.ShouldForward(pkt, 800*time.Millisecond, keyInfo) {
		t.Fatalf("expected keyframe to be accepted and resume forwarding")
	}
	if tracker.State != StateForwarding {
		t.Fatalf("expected state restored to forwarding, got %v", tracker.State)
	}

	// Next P-frame passes cleanly
	if !tracker.ShouldForward(pkt, 600*time.Millisecond, pFrameInfo) {
		t.Fatalf("expected subsequent P-frame to pass")
	}
}

func TestStreamRebaser_MonotonicSequencingAndRecovery(t *testing.T) {
	rebaser := NewStreamRebaser(90000)

	now := time.Now()
	pkt1 := &rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 100, Timestamp: 1000},
		Payload: []byte{1, 2, 3},
	}

	out1, drop := rebaser.RebaseProcess(pkt1, true, now)
	if drop || out1 == nil {
		t.Fatalf("expected out1 to be accepted")
	}
	if out1.SequenceNumber != 100 {
		t.Fatalf("expected initial seq 100, got %d", out1.SequenceNumber)
	}

	// Next packet 40ms later with camera jump
	now2 := now.Add(40 * time.Millisecond)
	pkt2 := &rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 500, Timestamp: 50000}, // Camera reboot jump
		Payload: []byte{4, 5, 6},
	}
	out2, drop := rebaser.RebaseProcess(pkt2, false, now2)
	if drop || out2 == nil {
		t.Fatalf("expected out2 to be accepted")
	}
	if out2.SequenceNumber != 101 {
		t.Fatalf("expected monotonic seq 101, got %d", out2.SequenceNumber)
	}
	if out2.Timestamp <= out1.Timestamp {
		t.Fatalf("expected timestamp to increase monotonically")
	}

	// Simulate upstream disconnect and reconnect
	rebaser.SignalUpstreamReconnected()
	if !rebaser.WaitingForKeyframe() {
		t.Fatalf("expected rebaser to wait for keyframe")
	}

	// Non-keyframe arrives during recovery -> should be dropped/gated
	now3 := now2.Add(40 * time.Millisecond)
	pkt3 := &rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 600, Timestamp: 60000},
		Payload: []byte{7, 8, 9},
	}
	_, drop = rebaser.RebaseProcess(pkt3, false, now3)
	if !drop {
		t.Fatalf("expected non-keyframe to be gated during recovery")
	}

	// Keyframe arrives -> accepted and waiting cleared
	now4 := now3.Add(40 * time.Millisecond)
	pktKey := &rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 601, Timestamp: 63600},
		Payload: []byte{0x25, 1, 2},
	}
	outKey, drop := rebaser.RebaseProcess(pktKey, true, now4)
	if drop || outKey == nil {
		t.Fatalf("expected keyframe to be accepted")
	}
	if rebaser.WaitingForKeyframe() {
		t.Fatalf("expected waitingForKeyframe to be false")
	}
	if outKey.SequenceNumber != 102 {
		t.Fatalf("expected monotonic sequence continuation 102, got %d", outKey.SequenceNumber)
	}
}

func TestSyntheticStreamGenerator(t *testing.T) {
	forma := &format.H264{
		PayloadTyp:        96,
		PacketizationMode: 1,
	}
	media := &description.Media{
		Type:    description.MediaTypeVideo,
		Formats: []format.Format{forma},
	}

	gen, err := NewSyntheticStreamGenerator(media, forma)
	if err != nil {
		t.Fatalf("failed to create synthetic generator: %v", err)
	}

	packets := gen.GenerateKeepAliveFrame(nil)
	if len(packets) == 0 {
		t.Fatalf("expected at least 1 synthetic packet")
	}
	if packets[0].PayloadType != 96 {
		t.Fatalf("expected payload type 96, got %d", packets[0].PayloadType)
	}
}
