package egress

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
	"github.com/smford/video-amplifier/internal/upstream"
)

// WHEPManager handles WebRTC HTTP Egress Protocol sessions for low-latency browser streaming.
type WHEPManager struct {
	logger *slog.Logger

	mu       sync.RWMutex
	sessions map[string]*WHEPSession
}

// WHEPSession represents an active WHEP streaming session for a browser peer.
type WHEPSession struct {
	ID         string
	CameraID   string
	PC         *webrtc.PeerConnection
	Track      *webrtc.TrackLocalStaticRTP
	Cancel     context.CancelFunc
	CreatedAt  time.Time
}

// NewWHEPManager creates a new WHEP egress manager.
func NewWHEPManager(logger *slog.Logger) *WHEPManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &WHEPManager{
		logger:   logger.With(slog.String("component", "whep_manager")),
		sessions: make(map[string]*WHEPSession),
	}
}

// HandleWHEPOffer handles POST /cameras/{id}/whep requests containing browser SDP offers.
func (m *WHEPManager) HandleWHEPOffer(w http.ResponseWriter, r *http.Request, cam *upstream.CameraStream) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := ioReadAll(r.Body)
	if err != nil || len(body) == 0 {
		http.Error(w, "invalid or empty SDP offer body", http.StatusBadRequest)
		return
	}
	offerSDP := string(body)

	// WebRTC API configuration with basic STUN configuration
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
		},
	}

	// Create new WebRTC PeerConnection
	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		m.logger.Error("Failed to create WebRTC PeerConnection", slog.Any("error", err))
		http.Error(w, fmt.Sprintf("WebRTC init error: %v", err), http.StatusInternalServerError)
		return
	}

	sessionID := uuid.New().String()[:8]

	// Add video track (H.264 video, clock rate 90000)
	videoTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeH264,
			ClockRate: 90000,
		},
		"video",
		cam.Config.ID,
	)
	if err != nil {
		pc.Close()
		http.Error(w, fmt.Sprintf("failed to create video track: %v", err), http.StatusInternalServerError)
		return
	}

	rtpSender, err := pc.AddTrack(videoTrack)
	if err != nil {
		pc.Close()
		http.Error(w, fmt.Sprintf("failed to add track to PeerConnection: %v", err), http.StatusInternalServerError)
		return
	}

	// Read RTCP packets from browser (e.g. PLI / picture loss indication / receiver reports)
	go func() {
		rtcpBuf := make([]byte, 1500)
		for {
			if _, _, rtcpErr := rtpSender.Read(rtcpBuf); rtcpErr != nil {
				return
			}
		}
	}()

	// Set remote SDP Offer
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP,
	}); err != nil {
		pc.Close()
		http.Error(w, fmt.Sprintf("failed to set remote description: %v", err), http.StatusBadRequest)
		return
	}

	// Create SDP Answer
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		pc.Close()
		http.Error(w, fmt.Sprintf("failed to create SDP answer: %v", err), http.StatusInternalServerError)
		return
	}

	// Complete ICE gathering before returning (or trickle)
	gatherComplete := webrtc.GatheringCompletePromise(pc)

	if err := pc.SetLocalDescription(answer); err != nil {
		pc.Close()
		http.Error(w, fmt.Sprintf("failed to set local description: %v", err), http.StatusInternalServerError)
		return
	}

	// Wait for ICE candidates gathering (up to 2s timeout)
	select {
	case <-gatherComplete:
	case <-time.After(2 * time.Second):
	}

	ctx, cancel := context.WithCancel(context.Background())
	sess := &WHEPSession{
		ID:        sessionID,
		CameraID:  cam.Config.ID,
		PC:        pc,
		Track:     videoTrack,
		Cancel:    cancel,
		CreatedAt: time.Now(),
	}

	m.mu.Lock()
	m.sessions[sessionID] = sess
	m.mu.Unlock()

	// Clean up session when peer connection closes or fails
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		m.logger.Debug("WHEP PeerConnection state change",
			slog.String("session_id", sessionID),
			slog.String("camera_id", cam.Config.ID),
			slog.String("state", state.String()),
		)
		if state == webrtc.PeerConnectionStateFailed ||
			state == webrtc.PeerConnectionStateClosed ||
			state == webrtc.PeerConnectionStateDisconnected {
			cancel()
			m.CloseSession(sessionID)
		}
	})

	// Start packet pump from camera stream into WebRTC track
	go m.streamPump(ctx, cam, sess)

	// Respond with SDP answer per WHEP specification (RFC draft-murillo-whep)
	w.Header().Set("Content-Type", "application/sdp")
	w.Header().Set("Location", fmt.Sprintf("/cameras/%s/whep/%s", cam.Config.ID, sessionID))
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Location")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(pc.LocalDescription().SDP))
}

// HandleWHEPResource handles DELETE /cameras/{id}/whep/{sessionID} or PATCH for trickle ICE.
func (m *WHEPManager) HandleWHEPResource(w http.ResponseWriter, r *http.Request, camID, sessionID string) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	switch r.Method {
	case http.MethodDelete:
		m.CloseSession(sessionID)
		w.WriteHeader(http.StatusOK)

	case http.MethodOptions:
		w.Header().Set("Access-Control-Allow-Methods", "POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// streamPump pops rebased RTP packets from camera's shared ring buffer and writes to WebRTC track.
func (m *WHEPManager) streamPump(ctx context.Context, cam *upstream.CameraStream, sess *WHEPSession) {
	queue := cam.SubscribeRTP(sess.ID, ctx)
	defer cam.UnsubscribeRTP(sess.ID)
	defer sess.PC.Close()

	for {
		pkt, err := queue.Pop(ctx)
		if err != nil {
			return
		}
		if pkt == nil {
			continue
		}

		if err := sess.Track.WriteRTP(pkt); err != nil {
			m.logger.Debug("WHEP track write error", slog.Any("error", err))
			return
		}
	}
}

// CloseSession terminates and removes an active WHEP session.
func (m *WHEPManager) CloseSession(sessionID string) {
	m.mu.Lock()
	sess, exists := m.sessions[sessionID]
	if exists {
		delete(m.sessions, sessionID)
	}
	m.mu.Unlock()

	if exists && sess != nil {
		sess.Cancel()
		_ = sess.PC.Close()
		m.logger.Info("Closed WHEP session", slog.String("session_id", sessionID), slog.String("camera_id", sess.CameraID))
	}
}

func ioReadAll(r ioReader) ([]byte, error) {
	var buf []byte
	b := make([]byte, 1024)
	for {
		n, err := r.Read(b)
		if n > 0 {
			buf = append(buf, b[:n]...)
		}
		if err != nil {
			if err.Error() == "EOF" {
				return buf, nil
			}
			return buf, err
		}
	}
}

type ioReader interface {
	Read(p []byte) (n int, err error)
}
