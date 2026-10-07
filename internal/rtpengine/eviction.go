package rtpengine

import (
	"time"

	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h265"
	"github.com/pion/rtp"
)

// CodecType identifies the underlying video compression format.
type CodecType int

const (
	CodecUnknown CodecType = iota
	CodecH264
	CodecH265
)

// FrameInfo holds parsed NALU characteristics for an RTP packet.
type FrameInfo struct {
	Codec         CodecType
	IsKeyframe    bool // IDR or IRAP (or SPS/PPS/VPS)
	IsNonRef      bool // Non-reference frame (B-frame or non-ref P-frame)
	ContainsSPS   bool
	ContainsPPS   bool
	ContainsVPS   bool
	IsSPSorPPS    bool
	NALUType      uint8
}

// InspectPacket analyzes the payload of an RTP packet to identify codec, keyframes,
// parameter sets, and reference status for H.264 and H.265 streams.
func InspectPacket(payload []byte, hint CodecType) FrameInfo {
	if len(payload) == 0 {
		return FrameInfo{Codec: CodecUnknown}
	}

	// Try H.264 inspection first unless explicitly told it's H.265
	if hint == CodecH265 {
		return inspectH265(payload)
	}

	info := inspectH264(payload)
	if info.Codec == CodecH264 {
		return info
	}

	if hint == CodecUnknown {
		// Try H.265 if H.264 did not clearly match
		h265Info := inspectH265(payload)
		if h265Info.Codec == CodecH265 {
			return h265Info
		}
	}

	return info
}

// inspectH264 parses RFC 6184 H.264 RTP payloads (Single NALU, STAP-A, FU-A).
func inspectH264(payload []byte) FrameInfo {
	if len(payload) == 0 {
		return FrameInfo{}
	}

	naluHeader := payload[0]
	naluType := h264.NALUType(naluHeader & 0x1F)
	nalRefIdc := (naluHeader >> 5) & 0x03

	switch naluType {
	case h264.NALUTypeIDR:
		return FrameInfo{
			Codec:       CodecH264,
			IsKeyframe:  true,
			IsNonRef:    false,
			NALUType:    uint8(naluType),
		}

	case h264.NALUTypeSPS:
		return FrameInfo{
			Codec:       CodecH264,
			IsKeyframe:  true,
			ContainsSPS: true,
			IsSPSorPPS:  true,
			NALUType:    uint8(naluType),
		}

	case h264.NALUTypePPS:
		return FrameInfo{
			Codec:       CodecH264,
			IsKeyframe:  true,
			ContainsPPS: true,
			IsSPSorPPS:  true,
			NALUType:    uint8(naluType),
		}

	case h264.NALUTypeNonIDR:
		// nalRefIdc == 0 indicates non-reference picture (e.g. disposable B-frame)
		return FrameInfo{
			Codec:       CodecH264,
			IsKeyframe:  false,
			IsNonRef:    nalRefIdc == 0,
			NALUType:    uint8(naluType),
		}

	case h264.NALUTypeSTAPA:
		// Aggregation packet: contains multiple NALUs with 2-byte length prefixes
		p := payload[1:]
		var hasKey, hasSPS, hasPPS bool
		for len(p) >= 2 {
			size := int(p[0])<<8 | int(p[1])
			p = p[2:]
			if size <= 0 || size > len(p) {
				break
			}
			innerNALU := p[:size]
			p = p[size:]
			if len(innerNALU) > 0 {
				innerType := h264.NALUType(innerNALU[0] & 0x1F)
				switch innerType {
				case h264.NALUTypeIDR:
					hasKey = true
				case h264.NALUTypeSPS:
					hasKey = true
					hasSPS = true
				case h264.NALUTypePPS:
					hasKey = true
					hasPPS = true
				}
			}
		}
		return FrameInfo{
			Codec:       CodecH264,
			IsKeyframe:  hasKey,
			ContainsSPS: hasSPS,
			ContainsPPS: hasPPS,
			IsSPSorPPS:  hasSPS || hasPPS,
			NALUType:    uint8(naluType),
		}

	case h264.NALUTypeFUA:
		// Fragmentation unit: indicator (byte 0) + header (byte 1)
		if len(payload) >= 2 {
			fuHeader := payload[1]
			fuType := h264.NALUType(fuHeader & 0x1F)
			isStart := (fuHeader & 0x80) != 0
			isKey := isStart && (fuType == h264.NALUTypeIDR || fuType == h264.NALUTypeSPS || fuType == h264.NALUTypePPS)
			return FrameInfo{
				Codec:      CodecH264,
				IsKeyframe: isKey,
				IsNonRef:   nalRefIdc == 0,
				NALUType:   uint8(fuType),
			}
		}
	}

	return FrameInfo{
		Codec:    CodecH264,
		IsNonRef: nalRefIdc == 0,
		NALUType: uint8(naluType),
	}
}

// inspectH265 parses RFC 7798 H.265 RTP payloads.
func inspectH265(payload []byte) FrameInfo {
	if len(payload) < 2 {
		return FrameInfo{}
	}

	// H.265 NAL header is 2 bytes: Type is in bits 1..6 of first byte
	naluType := h265.NALUType((payload[0] >> 1) & 0x3F)

	switch naluType {
	case h265.NALUType_IDR_W_RADL, h265.NALUType_IDR_N_LP, h265.NALUType_CRA_NUT,
		h265.NALUType_BLA_W_LP, h265.NALUType_BLA_W_RADL, h265.NALUType_BLA_N_LP:
		return FrameInfo{
			Codec:      CodecH265,
			IsKeyframe: true,
			NALUType:   uint8(naluType),
		}

	case h265.NALUType_VPS_NUT:
		return FrameInfo{
			Codec:       CodecH265,
			IsKeyframe:  true,
			ContainsVPS: true,
			IsSPSorPPS:  true,
			NALUType:    uint8(naluType),
		}

	case h265.NALUType_SPS_NUT:
		return FrameInfo{
			Codec:       CodecH265,
			IsKeyframe:  true,
			ContainsSPS: true,
			IsSPSorPPS:  true,
			NALUType:    uint8(naluType),
		}

	case h265.NALUType_PPS_NUT:
		return FrameInfo{
			Codec:       CodecH265,
			IsKeyframe:  true,
			ContainsPPS: true,
			IsSPSorPPS:  true,
			NALUType:    uint8(naluType),
		}

	case h265.NALUType_TRAIL_N, h265.NALUType_TSA_N, h265.NALUType_STSA_N, h265.NALUType_RADL_N, h265.NALUType_RASL_N:
		// Sub-types ending in _N are non-reference pictures in HEVC
		return FrameInfo{
			Codec:    CodecH265,
			IsNonRef: true,
			NALUType: uint8(naluType),
		}

	case h265.NALUType_AggregationUnit:
		// Aggregation Unit (RFC 7798 section 4.4.2)
		p := payload[2:]
		var hasKey, hasSPS, hasPPS, hasVPS bool
		for len(p) >= 2 {
			size := int(p[0])<<8 | int(p[1])
			p = p[2:]
			if size <= 0 || size > len(p) {
				break
			}
			inner := p[:size]
			p = p[size:]
			if len(inner) >= 2 {
				innerType := h265.NALUType((inner[0] >> 1) & 0x3F)
				switch innerType {
				case h265.NALUType_IDR_W_RADL, h265.NALUType_IDR_N_LP, h265.NALUType_CRA_NUT:
					hasKey = true
				case h265.NALUType_VPS_NUT:
					hasKey = true
					hasVPS = true
				case h265.NALUType_SPS_NUT:
					hasKey = true
					hasSPS = true
				case h265.NALUType_PPS_NUT:
					hasKey = true
					hasPPS = true
				}
			}
		}
		return FrameInfo{
			Codec:       CodecH265,
			IsKeyframe:  hasKey,
			ContainsVPS: hasVPS,
			ContainsSPS: hasSPS,
			ContainsPPS: hasPPS,
			IsSPSorPPS:  hasVPS || hasSPS || hasPPS,
			NALUType:    uint8(naluType),
		}

	case h265.NALUType_FragmentationUnit:
		if len(payload) >= 3 {
			fuHeader := payload[2]
			fuType := h265.NALUType(fuHeader & 0x3F)
			isStart := (fuHeader & 0x80) != 0
			isKey := isStart && (fuType == h265.NALUType_IDR_W_RADL || fuType == h265.NALUType_IDR_N_LP || fuType == h265.NALUType_CRA_NUT)
			return FrameInfo{
				Codec:      CodecH265,
				IsKeyframe: isKey,
				NALUType:   uint8(fuType),
			}
		}
	}

	return FrameInfo{
		Codec:    CodecH265,
		NALUType: uint8(naluType),
	}
}

// EvictionState tracks client state for GOP eviction.
type EvictionState int

const (
	// StateForwarding: Client receives all frames normally.
	StateForwarding EvictionState = iota
	// StateDegraded: Client latency exceeded initial watermark; non-reference frames are dropped (30fps -> 15fps).
	StateDegraded
	// StateEvicted: Client latency exceeded max watermark; all frames dropped until next SPS/PPS + IDR keyframe arrives.
	StateEvicted
)

// ClientEvictionTracker tracks frame delivery, queue latency, and eviction state for an individual downstream client.
type ClientEvictionTracker struct {
	ClientID          string
	Watermark         time.Duration
	DegradationStep   bool
	State             EvictionState
	LastQueueLatency  time.Duration
	LastKeyframeTime  time.Time
	HasSeenKeyframe   bool
	DroppedFrames     uint64
	EvictionCount     uint64
}

// NewClientEvictionTracker creates a new tracker for a client.
func NewClientEvictionTracker(clientID string, watermark time.Duration, degradationStep bool) *ClientEvictionTracker {
	if watermark <= 0 {
		watermark = 1500 * time.Millisecond
	}
	return &ClientEvictionTracker{
		ClientID:        clientID,
		Watermark:       watermark,
		DegradationStep: degradationStep,
		State:           StateForwarding,
	}
}

// ShouldForward evaluates an incoming frame against client latency and GOP state.
// Returns shouldDeliver=true if packet should be queued/written to client, or false if evicted.
func (t *ClientEvictionTracker) ShouldForward(pkt *rtp.Packet, queueLatency time.Duration, info FrameInfo) bool {
	t.LastQueueLatency = queueLatency

	// Check if watermark exceeded
	if queueLatency > t.Watermark {
		if t.State != StateEvicted {
			// Trigger full eviction
			t.State = StateEvicted
			t.EvictionCount++
		}
	} else if t.DegradationStep && queueLatency > (t.Watermark*2)/3 {
		// Degradation step (approx 66% of watermark): enter degraded state if not already evicted
		if t.State == StateForwarding {
			t.State = StateDegraded
		}
	} else if queueLatency < (t.Watermark / 2) {
		// Latency has recovered below half of watermark
		if t.State == StateDegraded {
			t.State = StateForwarding
		}
	}

	// Decision based on current state
	switch t.State {
	case StateEvicted:
		// In evicted state: drop ALL trailing P and B frames.
		// Resume transmission ONLY upon arrival of next keyframe (SPS/PPS + IDR).
		if info.IsKeyframe {
			// Keyframe arrived! Transition back to forwarding
			t.State = StateForwarding
			t.HasSeenKeyframe = true
			t.LastKeyframeTime = time.Now()
			return true
		}
		t.DroppedFrames++
		return false

	case StateDegraded:
		// In degraded state: drop non-reference frames (B-frames and non-ref P-frames) to cut rate
		if info.IsNonRef {
			t.DroppedFrames++
			return false
		}
		return true

	default: // StateForwarding
		return true
	}
}
