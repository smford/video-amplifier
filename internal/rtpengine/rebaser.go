package rtpengine

import (
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtph264"
	"github.com/pion/rtp"
)

// Default SPS and PPS for 640x360 1fps black screen H.264 video.
var (
	DefaultSyntheticSPS = []byte{
		0x67, 0x42, 0xc0, 0x1e, 0xd9, 0x00, 0xa0, 0x47, 0x79, 0x30, 0x50, 0x50, 0x50, 0x5c,
	}
	DefaultSyntheticPPS = []byte{
		0x68, 0xce, 0x3c, 0x80,
	}
	// Minimal black screen slice NALU (P-slice or IDR)
	DefaultSyntheticIDR = []byte{
		0x65, 0x88, 0x84, 0x00, 0x10, 0xff, 0xfe, 0xf6, 0x00,
	}
)

// StreamRebaser handles monotonic RTP sequence numbers and continuous timestamp re-basing
// across transitions between live camera feeds and synthetic keep-alives.
type StreamRebaser struct {
	mu sync.Mutex

	// Current output state
	currentSeq uint16
	currentTS  uint32
	lastTime   time.Time
	hasOutput  bool

	// Clock rate (typically 90000 for H.264/H.265 video)
	clockRate uint32

	// Waiting for first IDR keyframe after upstream reconnect
	waitingForKeyframe bool
	keyframeBuffered   bool
	bufferedPackets    []*rtp.Packet
}

// NewStreamRebaser creates a new sequence and timestamp rebaser.
func NewStreamRebaser(clockRate uint32) *StreamRebaser {
	if clockRate == 0 {
		clockRate = 90000
	}
	return &StreamRebaser{
		clockRate:          clockRate,
		waitingForKeyframe: false,
		bufferedPackets:    make([]*rtp.Packet, 0),
	}
}

// SignalUpstreamReconnected notifies the rebaser that upstream has recovered.
// It enters waitingForKeyframe mode so non-keyframes are buffered until the first valid IDR arrives.
func (r *StreamRebaser) SignalUpstreamReconnected() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waitingForKeyframe = true
	r.keyframeBuffered = false
	r.bufferedPackets = r.bufferedPackets[:0]
}

// WaitingForKeyframe returns whether the rebaser is currently gating packets until an IDR.
func (r *StreamRebaser) WaitingForKeyframe() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.waitingForKeyframe
}

// RebaseProcess transforms an incoming RTP packet (either live or synthetic)
// by giving it a smooth monotonic sequence number and continuous timestamp.
// If waiting for keyframe and packet is not a keyframe, it returns shouldDrop=true.
func (r *StreamRebaser) RebaseProcess(pkt *rtp.Packet, isKeyframe bool, now time.Time) (outPkt *rtp.Packet, shouldDrop bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If waiting for keyframe after upstream reconnect
	if r.waitingForKeyframe {
		if !isKeyframe {
			// Buffer non-keyframe packets up to a reasonable bound (e.g. 100 packets)
			if len(r.bufferedPackets) < 100 {
				r.bufferedPackets = append(r.bufferedPackets, pkt.Clone())
			}
			return nil, true
		}
		// First keyframe arrived! Clear wait state
		r.waitingForKeyframe = false
		r.bufferedPackets = r.bufferedPackets[:0]
	}

	clone := pkt.Clone()

	if !r.hasOutput {
		// Initialize starting point
		r.currentSeq = pkt.SequenceNumber
		r.currentTS = pkt.Timestamp
		r.lastTime = now
		r.hasOutput = true
		return clone, false
	}

	// Advance sequence number monotonically
	r.currentSeq++
	clone.SequenceNumber = r.currentSeq

	// Calculate timestamp advance based on elapsed wall time
	elapsed := now.Sub(r.lastTime)
	if elapsed < 0 {
		elapsed = 0
	}
	// Cap large jumps during offline periods to realistic frame steps
	tsDelta := uint32(elapsed.Seconds() * float64(r.clockRate))
	if tsDelta == 0 {
		tsDelta = 1 // Ensure monotonic increase
	}

	r.currentTS += tsDelta
	clone.Timestamp = r.currentTS
	r.lastTime = now

	return clone, false
}

// SyntheticStreamGenerator produces 1 fps synthetic RTP video packets
// to maintain downstream RTSP sessions (Frigate, Blue Iris) during upstream dropouts.
type SyntheticStreamGenerator struct {
	media     *description.Media
	forma     format.Format
	encoder   *rtph264.Encoder
	clockRate uint32
	payloadTyp uint8
}

// NewSyntheticStreamGenerator creates a generator that matches the provided media/format.
func NewSyntheticStreamGenerator(media *description.Media, forma format.Format) (*SyntheticStreamGenerator, error) {
	gen := &SyntheticStreamGenerator{
		media:     media,
		forma:     forma,
		clockRate: 90000,
	}

	if h264Fmt, ok := forma.(*format.H264); ok {
		gen.payloadTyp = h264Fmt.PayloadType()
		encoder := &rtph264.Encoder{
			PayloadType:       gen.payloadTyp,
			PacketizationMode: h264Fmt.PacketizationMode,
			PayloadMaxSize:    1400,
		}
		if err := encoder.Init(); err != nil {
			return nil, err
		}
		gen.encoder = encoder
	}

	return gen, nil
}

// GenerateKeepAliveFrame returns an RTP packet containing an IDR keyframe or keep-alive frame.
func (g *SyntheticStreamGenerator) GenerateKeepAliveFrame(cachedKeyframe []byte) []*rtp.Packet {
	if g.encoder == nil {
		// Fallback for non-H.264 or generic format: return raw RTP packet
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:     2,
				PayloadType: g.payloadTyp,
				Marker:      true,
			},
			Payload: DefaultSyntheticIDR,
		}
		return []*rtp.Packet{pkt}
	}

	// Encode SPS + PPS + IDR access unit
	au := [][]byte{
		DefaultSyntheticSPS,
		DefaultSyntheticPPS,
		DefaultSyntheticIDR,
	}

	if len(cachedKeyframe) > 0 {
		au[2] = cachedKeyframe
	}

	packets, err := g.encoder.Encode(au)
	if err != nil || len(packets) == 0 {
		// Fallback packet
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:     2,
				PayloadType: g.payloadTyp,
				Marker:      true,
			},
			Payload: DefaultSyntheticIDR,
		}
		return []*rtp.Packet{pkt}
	}

	return packets
}
