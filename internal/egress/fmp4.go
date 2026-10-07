package egress

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/format/rtph264"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/bluenviron/mediacommon/v2/pkg/formats/fmp4"
	"github.com/bluenviron/mediacommon/v2/pkg/formats/fmp4/seekablebuffer"
	"github.com/bluenviron/mediacommon/v2/pkg/formats/mp4/codecs"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/smford/video-amplifier/internal/upstream"
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for egress
	},
}

// FMP4Manager handles fragmented MP4 egress over HTTP chunked transfer and WebSockets.
type FMP4Manager struct {
	logger *slog.Logger
}

// NewFMP4Manager creates a new fragmented MP4 egress manager.
func NewFMP4Manager(logger *slog.Logger) *FMP4Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &FMP4Manager{
		logger: logger.With(slog.String("component", "fmp4_manager")),
	}
}

// HandleFMP4Stream serves an fMP4 stream over HTTP chunked transfer (video/mp4).
func (m *FMP4Manager) HandleFMP4Stream(w http.ResponseWriter, r *http.Request, cam *upstream.CameraStream) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "HTTP streaming not supported", http.StatusInternalServerError)
		return
	}

	clientID := uuid.New().String()[:8]
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	m.logger.Info("Downstream fMP4 HTTP client connected",
		slog.String("camera", cam.Config.Name),
		slog.String("client_id", clientID),
		slog.String("remote_addr", r.RemoteAddr),
	)

	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	queue := cam.SubscribeRTP("fmp4-"+clientID, ctx)
	defer cam.UnsubscribeRTP("fmp4-" + clientID)

	decoder := &rtph264.Decoder{}
	_ = decoder.Init()

	var sps, pps []byte
	var initSent bool
	var seqNum uint32 = 1
	var lastTime uint64

	for {
		pkt, err := queue.Pop(ctx)
		if err != nil {
			return
		}
		if pkt == nil {
			continue
		}

		nalus, err := decoder.Decode(pkt)
		if err != nil || len(nalus) == 0 {
			continue
		}

		// Extract SPS / PPS if present
		var hasIDR bool
		var filteredNALUs [][]byte
		for _, nalu := range nalus {
			if len(nalu) == 0 {
				continue
			}
			nType := h264.NALUType(nalu[0] & 0x1F)
			switch nType {
			case h264.NALUTypeSPS:
				sps = append([]byte(nil), nalu...)
			case h264.NALUTypePPS:
				pps = append([]byte(nil), nalu...)
			case h264.NALUTypeIDR:
				hasIDR = true
				filteredNALUs = append(filteredNALUs, nalu)
			case h264.NALUTypeNonIDR:
				filteredNALUs = append(filteredNALUs, nalu)
			default:
				filteredNALUs = append(filteredNALUs, nalu)
			}
		}

		// Send Init segment if we have SPS/PPS and haven't sent it yet
		if !initSent {
			if len(sps) == 0 || len(pps) == 0 || !hasIDR {
				continue
			}

			initSeg := fmp4.Init{
				Tracks: []*fmp4.InitTrack{
					{
						ID:        1,
						TimeScale: 90000,
						Codec: &codecs.H264{
							SPS: sps,
							PPS: pps,
						},
					},
				},
			}

			var buf seekablebuffer.Buffer
			if err := initSeg.Marshal(&buf); err != nil {
				m.logger.Error("Failed to marshal fMP4 init segment", slog.Any("error", err))
				return
			}

			if _, err := w.Write(buf.Bytes()); err != nil {
				return
			}
			flusher.Flush()
			initSent = true
			lastTime = uint64(pkt.Timestamp)
		}

		if len(filteredNALUs) == 0 {
			continue
		}

		sample, err := fmp4.NewSampleH264(0, filteredNALUs)
		if err != nil {
			continue
		}
		sample.IsNonSyncSample = !hasIDR

		// Calculate duration relative to 90kHz RTP clock
		curTime := uint64(pkt.Timestamp)
		duration := uint32(3000) // Default fallback ~30fps
		if curTime > lastTime {
			diff := curTime - lastTime
			if diff < 90000 {
				duration = uint32(diff)
			}
		}
		sample.Duration = duration
		lastTime = curTime

		part := fmp4.Part{
			SequenceNumber: seqNum,
			Tracks: []*fmp4.PartTrack{
				{
					ID:       1,
					BaseTime: curTime,
					Samples:  []*fmp4.Sample{sample},
				},
			},
		}
		seqNum++

		var buf seekablebuffer.Buffer
		if err := part.Marshal(&buf); err != nil {
			m.logger.Debug("Failed to marshal fMP4 part", slog.Any("error", err))
			continue
		}

		if _, err := w.Write(buf.Bytes()); err != nil {
			return
		}
		flusher.Flush()
	}
}

// HandleWebSocketStream serves an fMP4 binary stream over WebSockets.
func (m *FMP4Manager) HandleWebSocketStream(w http.ResponseWriter, r *http.Request, cam *upstream.CameraStream) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		m.logger.Warn("Failed to upgrade WebSocket connection", slog.Any("error", err))
		return
	}
	defer conn.Close()

	clientID := uuid.New().String()[:8]
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	m.logger.Info("Downstream fMP4 WebSocket client connected",
		slog.String("camera", cam.Config.Name),
		slog.String("client_id", clientID),
		slog.String("remote_addr", r.RemoteAddr),
	)

	// Pump pings / read discard loop
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				cancel()
				return
			}
		}
	}()

	queue := cam.SubscribeRTP("fmp4ws-"+clientID, ctx)
	defer cam.UnsubscribeRTP("fmp4ws-" + clientID)

	decoder := &rtph264.Decoder{}
	_ = decoder.Init()

	var sps, pps []byte
	var initSent bool
	var seqNum uint32 = 1
	var lastTime uint64

	for {
		pkt, err := queue.Pop(ctx)
		if err != nil {
			return
		}
		if pkt == nil {
			continue
		}

		nalus, err := decoder.Decode(pkt)
		if err != nil || len(nalus) == 0 {
			continue
		}

		var hasIDR bool
		var filteredNALUs [][]byte
		for _, nalu := range nalus {
			if len(nalu) == 0 {
				continue
			}
			nType := h264.NALUType(nalu[0] & 0x1F)
			switch nType {
			case h264.NALUTypeSPS:
				sps = append([]byte(nil), nalu...)
			case h264.NALUTypePPS:
				pps = append([]byte(nil), nalu...)
			case h264.NALUTypeIDR:
				hasIDR = true
				filteredNALUs = append(filteredNALUs, nalu)
			case h264.NALUTypeNonIDR:
				filteredNALUs = append(filteredNALUs, nalu)
			default:
				filteredNALUs = append(filteredNALUs, nalu)
			}
		}

		if !initSent {
			if len(sps) == 0 || len(pps) == 0 || !hasIDR {
				continue
			}

			initSeg := fmp4.Init{
				Tracks: []*fmp4.InitTrack{
					{
						ID:        1,
						TimeScale: 90000,
						Codec: &codecs.H264{
							SPS: sps,
							PPS: pps,
						},
					},
				},
			}

			var buf seekablebuffer.Buffer
			if err := initSeg.Marshal(&buf); err != nil {
				return
			}

			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteMessage(websocket.BinaryMessage, buf.Bytes()); err != nil {
				return
			}
			initSent = true
			lastTime = uint64(pkt.Timestamp)
		}

		if len(filteredNALUs) == 0 {
			continue
		}

		sample, err := fmp4.NewSampleH264(0, filteredNALUs)
		if err != nil {
			continue
		}
		sample.IsNonSyncSample = !hasIDR

		curTime := uint64(pkt.Timestamp)
		duration := uint32(3000)
		if curTime > lastTime {
			diff := curTime - lastTime
			if diff < 90000 {
				duration = uint32(diff)
			}
		}
		sample.Duration = duration
		lastTime = curTime

		part := fmp4.Part{
			SequenceNumber: seqNum,
			Tracks: []*fmp4.PartTrack{
				{
					ID:       1,
					BaseTime: curTime,
					Samples:  []*fmp4.Sample{sample},
				},
			},
		}
		seqNum++

		var buf seekablebuffer.Buffer
		if err := part.Marshal(&buf); err != nil {
			continue
		}

		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := conn.WriteMessage(websocket.BinaryMessage, buf.Bytes()); err != nil {
			return
		}
	}
}
