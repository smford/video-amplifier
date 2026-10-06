package upstream

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtpmjpeg"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/pion/rtp"
	"github.com/smford/video-amplifier/internal/config"
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

	mu     sync.Mutex
	client *gortsplib.Client
}

// NewRTSPDriver creates a new RTSP driver.
func NewRTSPDriver(stream *CameraStream, publisher RTSPStreamPublisher) *RTSPDriver {
	return &RTSPDriver{
		stream:    stream,
		publisher: publisher,
	}
}

// Start connects to the upstream RTSP server, sets up tracks, and streams RTP packets.
func (d *RTSPDriver) Start(ctx context.Context) error {
	u, err := base.ParseURL(d.stream.Config.UpstreamURL)
	if err != nil {
		return fmt.Errorf("failed to parse upstream rtsp url: %w", err)
	}

	client := &gortsplib.Client{
		Scheme:       u.Scheme,
		Host:         u.Host,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
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

	// Describe stream
	desc, _, err := client.Describe(u)
	if err != nil {
		return fmt.Errorf("rtsp describe failed: %w", err)
	}

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

	// Handle RTP packet arrival
	client.OnPacketRTPAny(func(medi *description.Media, forma format.Format, pkt *rtp.Packet) {
		// Increment upstream bytes
		d.stream.AddBytesReceived(int64(len(pkt.Payload) + 12))

		// If MJPEG format present, decode to cache snapshot
		if mjpegDecoder != nil {
			if _, ok := forma.(*format.MJPEG); ok {
				if jpegBytes, err := mjpegDecoder.Decode(pkt); err == nil && len(jpegBytes) > 0 {
					d.stream.UpdateSnapshot(jpegBytes)
				}
			}
		}

		// Relay to downstream readers
		if serverStream != nil {
			if err := serverStream.WritePacketRTP(medi, pkt); err != nil {
				d.stream.logger.Debug("ServerStream write packet error", slog.Any("error", err))
			}
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
