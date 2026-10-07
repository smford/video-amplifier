package egress

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/upstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWHEPManager_OfferAndCloseSession(t *testing.T) {
	manager := NewWHEPManager(nil)

	camCfg := config.CameraConfig{
		ID:          "test-cam",
		Name:        "Test Camera",
		UpstreamURL: "rtsp://localhost/test",
		Mode:        config.ModeAlwaysOn,
	}
	camStream := upstream.NewCameraStream(camCfg, nil, nil)

	// Create client peer connection to generate a real SDP offer
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc.Close()

	_, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	})
	require.NoError(t, err)

	offer, err := pc.CreateOffer(nil)
	require.NoError(t, err)
	err = pc.SetLocalDescription(offer)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/cameras/test-cam/whep", strings.NewReader(offer.SDP))
	req.Header.Set("Content-Type", "application/sdp")
	w := httptest.NewRecorder()

	manager.HandleWHEPOffer(w, req, camStream)
	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Equal(t, "application/sdp", resp.Header.Get("Content-Type"))
	location := resp.Header.Get("Location")
	assert.NotEmpty(t, location)
	assert.True(t, strings.HasPrefix(location, "/cameras/test-cam/whep/"))

	// Verify session exists
	sessionID := strings.TrimPrefix(location, "/cameras/test-cam/whep/")
	manager.mu.RLock()
	sess, exists := manager.sessions[sessionID]
	manager.mu.RUnlock()
	assert.True(t, exists)
	assert.NotNil(t, sess)

	// Test DELETE endpoint
	delReq := httptest.NewRequest(http.MethodDelete, location, nil)
	delW := httptest.NewRecorder()
	manager.HandleWHEPResource(delW, delReq, "test-cam", sessionID)
	assert.Equal(t, http.StatusOK, delW.Code)

	// Verify session removed
	manager.mu.RLock()
	_, existsAfter := manager.sessions[sessionID]
	manager.mu.RUnlock()
	assert.False(t, existsAfter)
}

func TestFMP4Manager_StreamInitAndParts(t *testing.T) {
	mgr := NewFMP4Manager(nil)

	camCfg := config.CameraConfig{
		ID:          "fmp4-cam",
		Name:        "FMP4 Camera",
		UpstreamURL: "rtsp://localhost/test",
		Mode:        config.ModeAlwaysOn,
	}
	camStream := upstream.NewCameraStream(camCfg, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/cameras/fmp4-cam/fmp4", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.HandleFMP4Stream(w, req, camStream)
	}()

	// Simulate RTP packets: SPS, PPS, IDR frame
	sps := []byte{0x67, 0x42, 0xc0, 0x28, 0xd9, 0x00, 0x78, 0x02, 0x27, 0xe5, 0x84, 0x00, 0x00, 0x03, 0x00, 0x04, 0x00, 0x00, 0x03, 0x00, 0xf0, 0x3c, 0x60, 0xc9, 0x20}
	pps := []byte{0x68, 0xce, 0x3c, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00, 0x10, 0x20, 0x30}

	time.Sleep(50 * time.Millisecond)

	// Broadcast SPS
	camStream.BroadcastRTPPacket(&rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 1, Timestamp: 90000},
		Payload: sps,
	})
	// Broadcast PPS
	camStream.BroadcastRTPPacket(&rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 2, Timestamp: 90000},
		Payload: pps,
	})
	// Broadcast IDR
	camStream.BroadcastRTPPacket(&rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 3, Timestamp: 90000, Marker: true},
		Payload: idr,
	})

	time.Sleep(100 * time.Millisecond)
	cancel() // Stop stream
	<-done

	body := w.Body.Bytes()
	// Should contain ftyp and moov box headers from init segment
	assert.True(t, len(body) > 0, "expected fmp4 data written")
	assert.Contains(t, string(body), "ftyp")
	assert.Contains(t, string(body), "moov")
	assert.Contains(t, string(body), "moof")
}
