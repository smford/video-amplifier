package upstream

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtpmjpeg"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/pion/rtp"
	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/rtpengine"
)

// RTSPStreamPublisher is the interface implemented by the downstream RTSP server
// to register and deregister active broadcast streams.
type RTSPStreamPublisher interface {
	SetStreamReady(cameraID string, desc *description.Session) (*gortsplib.ServerStream, error)
	SetStreamUnready(cameraID string)
}

// RTSPDriver ingests an RTSP stream (over TCP or UDP) and relays it to downstream readers.
type RTSPDriver struct {
	stream    *CameraStream
	publisher RTSPStreamPublisher

	mu             sync.Mutex
	client         *gortsplib.Client
	rebaser        *rtpengine.StreamRebaser
	synthGen       *rtpengine.SyntheticStreamGenerator
	lastPacketTime atomic.Int64 // UnixNano

	// Cached DESCRIBE/SDP negotiation session to avoid redundant queries (ITEM 3)
	cachedDescMu sync.RWMutex
	cachedDesc   *description.Session
}

// NewRTSPDriver creates a new RTSP driver.
func NewRTSPDriver(stream *CameraStream, publisher RTSPStreamPublisher) *RTSPDriver {
	return &RTSPDriver{
		stream:    stream,
		publisher: publisher,
		rebaser:   rtpengine.NewStreamRebaser(90000),
	}
}

// Start connects to the upstream RTSP server, sets up tracks, and streams RTP packets.
func (d *RTSPDriver) Start(ctx context.Context) error {
	u, err := base.ParseURL(d.stream.Config.UpstreamURL)
	if err != nil {
		return fmt.Errorf("failed to parse upstream rtsp url: %w", err)
	}

	// Configure OS-level TCP keep-alive socket hygiene flags on the camera ingest connection (ITEM 3):
	// TCP_KEEPIDLE=5s, TCP_KEEPINTVL=2s, TCP_KEEPCNT=3 to immediately detect dead peers.
	dialer := &net.Dialer{
		Timeout: 10 * time.Second,
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     5 * time.Second,
			Interval: 2 * time.Second,
			Count:    3,
		},
	}

	client := &gortsplib.Client{
		Scheme:       u.Scheme,
		Host:         u.Host,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		DialContext:  dialer.DialContext,
	}

	// Configure transport
	switch d.stream.Config.RTSPTransport {
	case config.TransportTCP:
		proto := gortsplib.ProtocolTCP
		client.Protocol = &proto
	case config.TransportUDP:
		proto := gortsplib.ProtocolUDP
		client.Protocol = &proto
	}

	d.mu.Lock()
	d.client = client
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		if d.client != nil {
			d.client.Close()
			d.client = nil
		}
		d.mu.Unlock()
	}()

	// Start RTSP client connection
	if err := client.Start(); err != nil {
		return fmt.Errorf("rtsp client start failed: %w", err)
	}

	// Describe stream (ITEM 3: Cache SDP/Describe body in memory)
	desc, _, err := client.Describe(u)
	if err != nil {
		return fmt.Errorf("rtsp describe failed: %w", err)
	}

	d.cachedDescMu.Lock()
	d.cachedDesc = desc
	d.cachedDescMu.Unlock()

	// Setup all medias
	if err := client.SetupAll(desc.BaseURL, desc.Medias); err != nil {
		return fmt.Errorf("rtsp setup failed: %w", err)
	}

	// Register stream with downstream server
	var serverStream *gortsplib.ServerStream
	if d.publisher != nil {
		sStream, err := d.publisher.SetStreamReady(d.stream.Config.ID, desc)
		if err != nil {
			return fmt.Errorf("failed to register stream with downstream rtsp server: %w", err)
		}
		serverStream = sStream
		defer d.publisher.SetStreamUnready(d.stream.Config.ID)
	}

	// Check if stream contains MJPEG format for snapshots
	var mjpegDecoder *rtpmjpeg.Decoder
	for _, media := range desc.Medias {
		for _, forma := range media.Formats {
			if _, ok := forma.(*format.MJPEG); ok {
				dec := &rtpmjpeg.Decoder{}
				if err := dec.Init(); err == nil {
					mjpegDecoder = dec
				}
				break
			}
		}
	}

	// Find video media and format to initialize synthetic keep-alive generator
	var videoMedia *description.Media
	var videoFormat format.Format
	for _, media := range desc.Medias {
		if media.Type == description.MediaTypeVideo {
			videoMedia = media
			if len(media.Formats) > 0 {
				videoFormat = media.Formats[0]
			}
			break
		}
	}
	if videoMedia != nil && videoFormat != nil {
		if gen, err := rtpengine.NewSyntheticStreamGenerator(videoMedia, videoFormat); err == nil {
			d.synthGen = gen
		}
	}

	// Signal rebaser that upstream has reconnected (buffers until first keyframe)
	d.rebaser.SignalUpstreamReconnected()

	d.lastPacketTime.Store(time.Now().UnixNano())

	// Handle RTP packet arrival
	client.OnPacketRTPAny(func(medi *description.Media, forma format.Format, pkt *rtp.Packet) {
		now := time.Now()
		d.lastPacketTime.Store(now.UnixNano())

		// Increment upstream bytes
		d.stream.AddBytesReceived(int64(len(pkt.Payload) + 12))

		// If MJPEG format present, decode to cache snapshot
		if mjpegDecoder != nil {
			if _, ok := forma.(*format.MJPEG); ok {
				if jpegBytes, err := mjpegDecoder.Decode(pkt); err == nil && len(jpegBytes) > 0 {
					d.stream.BroadcastMJPEGFrame(jpegBytes)
				}
			}
		}

		if serverStream != nil {
			// Inspect packet for keyframe/GOP info
			var codecHint rtpengine.CodecType
			if _, ok := forma.(*format.H264); ok {
				codecHint = rtpengine.CodecH264
			} else if _, ok := forma.(*format.H265); ok {
				codecHint = rtpengine.CodecH265
			}
			frameInfo := rtpengine.InspectPacket(pkt.Payload, codecHint)

			// Rebase sequence numbers and timestamps to prevent downstream player crashes
			outPkt, drop := d.rebaser.RebaseProcess(pkt, frameInfo.IsKeyframe, now)
			if drop || outPkt == nil {
				return
			}

			if err := serverStream.WritePacketRTP(medi, outPkt); err != nil {
				d.stream.logger.Debug("ServerStream write packet error", slog.Any("error", err))
			}

			// Broadcast rebased RTP packet to WebRTC (WHEP) and fMP4 subscribers (ITEM 4)
			d.stream.BroadcastRTPPacket(outPkt)
		}
	})

	// Start playback
	if _, err := client.Play(nil); err != nil {
		return fmt.Errorf("rtsp play failed: %w", err)
	}

	d.stream.SetState(StateStreaming)
	d.stream.ReportSuccess()
	d.stream.logger.Info("Connected to upstream RTSP camera feed",
		slog.Int("medias", len(desc.Medias)),
	)

	// Launch background synthetic keep-alive monitor (ITEM 2)
	synthCtx, synthCancel := context.WithCancel(ctx)
	defer synthCancel()

	disconnectTimeout := d.stream.Config.DisconnectTimeout.Duration()
	if disconnectTimeout <= 0 {
		disconnectTimeout = 3 * time.Second
	}
	enableSynth := true
	if d.stream.Config.SyntheticKeepAlives != nil {
		enableSynth = *d.stream.Config.SyntheticKeepAlives
	}

	if enableSynth && serverStream != nil && videoMedia != nil {
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-synthCtx.Done():
					return
				case tNow := <-ticker.C:
					lastPkt := time.Unix(0, d.lastPacketTime.Load())
					// If camera has stalled longer than disconnectTimeout, inject 1 fps keep-alive
					if tNow.Sub(lastPkt) > disconnectTimeout && d.synthGen != nil {
						cachedSnap, _, _ := d.stream.GetLatestSnapshot()
						pkts := d.synthGen.GenerateKeepAliveFrame(cachedSnap)
						for _, p := range pkts {
							rebased, drop := d.rebaser.RebaseProcess(p, true, tNow)
							if !drop && rebased != nil {
								_ = serverStream.WritePacketRTP(videoMedia, rebased)
								d.stream.BroadcastRTPPacket(rebased)
								d.stream.metrics.IncSyntheticFrames(d.stream.Config.Name)
							}
						}
					}
				}
			}
		}()
	}

	// Wait loop watching context cancellation or client error
	waitErrChan := make(chan error, 1)
	go func() {
		waitErrChan <- client.Wait()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-waitErrChan:
		return err
	}
}

// Stop terminates the RTSP client connection.
func (d *RTSPDriver) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.client != nil {
		d.client.Close()
		d.client = nil
	}
}

// CachedDesc returns the in-memory cached session description (ITEM 3).
func (d *RTSPDriver) CachedDesc() *description.Session {
	d.cachedDescMu.RLock()
	defer d.cachedDescMu.RUnlock()
	return d.cachedDesc
}


// IsH264Keyframe inspects an RTP packet's payload to determine if it contains an H.264 IDR/SPS/PPS frame.
func IsH264Keyframe(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}

	naluType := h264.NALUType(payload[0] & 0x1F)
	switch naluType {
	case h264.NALUTypeIDR, h264.NALUTypeSPS, h264.NALUTypePPS:
		return true
	case h264.NALUTypeFUA:
		if len(payload) > 1 {
			fuType := h264.NALUType(payload[1] & 0x1F)
			isStart := (payload[1] & 0x80) != 0
			if isStart && (fuType == h264.NALUTypeIDR || fuType == h264.NALUTypeSPS || fuType == h264.NALUTypePPS) {
				return true
			}
		}
	}
	return false
}
