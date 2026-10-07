package server

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/liberrors"
	"github.com/google/uuid"
	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/metrics"
	"github.com/smford/video-amplifier/internal/ringbuffer"
	"github.com/smford/video-amplifier/internal/rtpengine"
	"github.com/smford/video-amplifier/internal/upstream"
)

type rtspSessionData struct {
	cameraID        string
	clientID        string
	evictionTracker *rtpengine.ClientEvictionTracker
	cursor          *ringbuffer.ReaderCursor
	lastWriteErr    time.Time
}

// RTSPServer manages the downstream RTSP relay server.
type RTSPServer struct {
	cfg     *config.Config
	metrics *metrics.Metrics
	logger  *slog.Logger
	manager *upstream.Manager

	server *gortsplib.Server

	mu      sync.RWMutex
	started bool
	streams map[string]*gortsplib.ServerStream

	sessionsMu sync.RWMutex
	sessions   map[*gortsplib.ServerSession]*rtspSessionData
}

// NewRTSPServer initializes the RTSP relay server.
func NewRTSPServer(cfg *config.Config, m *metrics.Metrics, logger *slog.Logger) *RTSPServer {
	if logger == nil {
		logger = slog.Default()
	}

	s := &RTSPServer{
		cfg:      cfg,
		metrics:  m,
		logger:   logger.With(slog.String("component", "rtsp_server")),
		streams:  make(map[string]*gortsplib.ServerStream),
		sessions: make(map[*gortsplib.ServerSession]*rtspSessionData),
	}

	s.server = &gortsplib.Server{
		Handler:        s,
		RTSPAddress:    fmt.Sprintf(":%d", cfg.Server.RTSPPort),
		ReadTimeout:    cfg.Server.ReadTimeout.Duration(),
		WriteTimeout:   cfg.Server.WriteTimeout.Duration(),
		WriteQueueSize: 256,
	}

	return s
}

// SetManager links the camera stream manager.
func (s *RTSPServer) SetManager(mgr *upstream.Manager) {
	s.manager = mgr
}

// SetStreamReady registers an upstream RTSP stream as ready to be read by downstream clients.
// If an existing stream is already open for this cameraID, it re-uses the existing stream
// to preserve all connected downstream client sessions (Item 2).
func (s *RTSPServer) SetStreamReady(cameraID string, desc *description.Session) (*gortsplib.ServerStream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If stream already exists, keep it alive and reload description if needed
	if existing, exists := s.streams[cameraID]; exists && existing != nil {
		s.logger.Info("RTSP downstream stream already active; keeping client sessions attached",
			slog.String("camera_id", cameraID),
		)
		return existing, nil
	}

	stream := &gortsplib.ServerStream{
		Server: s.server,
		Desc:   desc,
	}

	if err := stream.Initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize ServerStream for camera %q: %w", cameraID, err)
	}

	s.streams[cameraID] = stream
	s.logger.Info("RTSP downstream stream registered and available",
		slog.String("camera_id", cameraID),
		slog.String("rtsp_path", "/"+cameraID),
	)

	return stream, nil
}

// SetStreamUnready removes a broadcast stream when its upstream disconnects.
// If downstream clients are connected or synthetic keepalives are active,
// the stream is kept intact so downstream NVRs are NOT disconnected.
func (s *RTSPServer) SetStreamUnready(cameraID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if this camera has active downstream clients
	hasDownstream := false
	s.sessionsMu.RLock()
	for _, sess := range s.sessions {
		if sess.cameraID == cameraID {
			hasDownstream = true
			break
		}
	}
	s.sessionsMu.RUnlock()

	if hasDownstream {
		s.logger.Info("Upstream disconnected, but keeping downstream RTSP stream open for active clients",
			slog.String("camera_id", cameraID),
		)
		return
	}

	if stream, exists := s.streams[cameraID]; exists {
		stream.Close()
		delete(s.streams, cameraID)
		s.logger.Info("RTSP downstream stream unready and closed", slog.String("camera_id", cameraID))
	}
}

// Start launches the RTSP server.
func (s *RTSPServer) Start() error {
	s.mu.Lock()
	s.started = true
	s.mu.Unlock()

	s.logger.Info("Starting RTSP relay server", slog.Int("port", s.cfg.Server.RTSPPort))
	return s.server.Start()
}

// Stop gracefully stops the RTSP server.
func (s *RTSPServer) Stop() {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return
	}
	s.started = false
	s.mu.Unlock()

	s.logger.Info("Shutting down RTSP relay server")
	s.server.Close()
}

// OnConnOpen handles incoming network connections.
func (s *RTSPServer) OnConnOpen(_ *gortsplib.ServerHandlerOnConnOpenCtx) {
	s.logger.Debug("Downstream RTSP TCP connection opened")
}

// OnConnClose handles closing network connections.
func (s *RTSPServer) OnConnClose(_ *gortsplib.ServerHandlerOnConnCloseCtx) {
	s.logger.Debug("Downstream RTSP TCP connection closed")
}

// OnSessionOpen handles RTSP session creation.
func (s *RTSPServer) OnSessionOpen(ctx *gortsplib.ServerHandlerOnSessionOpenCtx) {
	s.logger.Debug("Downstream RTSP session opened")
}

// OnDescribe handles DESCRIBE requests.
func (s *RTSPServer) OnDescribe(ctx *gortsplib.ServerHandlerOnDescribeCtx) (*base.Response, *gortsplib.ServerStream, error) {
	camID := strings.TrimPrefix(ctx.Path, "/")
	s.logger.Debug("RTSP DESCRIBE request received", slog.String("path", ctx.Path), slog.String("camera_id", camID))

	if s.manager == nil {
		return &base.Response{StatusCode: base.StatusServiceUnavailable}, nil, nil
	}

	cam, ok := s.manager.Get(camID)
	if !ok {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}

	// Downstream client authentication check (AuthMode: none, custom, or passthrough)
	if expUser, expPass, required := cam.GetExpectedCredentials(); required {
		if !ctx.Conn.VerifyCredentials(ctx.Request, expUser, expPass) {
			return &base.Response{
				StatusCode: base.StatusUnauthorized,
			}, nil, liberrors.ErrServerAuth{}
		}
	}

	// If camera is on-demand and not connected yet, trigger connection and wait
	if cam.Config.Mode == config.ModeOnDemand {
		cam.OnClientConnected(context.Background(), "rtsp")

		// Wait up to 5 seconds for stream to be ready
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			s.mu.RLock()
			st, ready := s.streams[camID]
			s.mu.RUnlock()
			if ready && st != nil {
				return &base.Response{StatusCode: base.StatusOK}, st, nil
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	s.mu.RLock()
	stream, exists := s.streams[camID]
	s.mu.RUnlock()

	// If no stream initialized yet, but camera has cached description, initialize stream immediately
	if (!exists || stream == nil) && cam != nil {
		if rtspDrv, ok := cam.Driver().(*upstream.RTSPDriver); ok {
			if cachedDesc := rtspDrv.CachedDesc(); cachedDesc != nil {
				if sStream, err := s.SetStreamReady(camID, cachedDesc); err == nil {
					stream = sStream
					exists = true
				}
			}
		}
	}

	if !exists || stream == nil {
		return &base.Response{StatusCode: base.StatusServiceUnavailable}, nil, nil
	}

	return &base.Response{StatusCode: base.StatusOK}, stream, nil
}

// OnSetup handles SETUP requests.
func (s *RTSPServer) OnSetup(ctx *gortsplib.ServerHandlerOnSetupCtx) (*base.Response, *gortsplib.ServerStream, error) {
	camID := strings.TrimPrefix(ctx.Path, "/")

	if s.manager != nil {
		if cam, ok := s.manager.Get(camID); ok {
			if expUser, expPass, required := cam.GetExpectedCredentials(); required {
				if !ctx.Conn.VerifyCredentials(ctx.Request, expUser, expPass) {
					return &base.Response{
						StatusCode: base.StatusUnauthorized,
					}, nil, liberrors.ErrServerAuth{}
				}
			}
		}
	}

	s.mu.RLock()
	stream, exists := s.streams[camID]
	s.mu.RUnlock()

	if !exists || stream == nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}

	return &base.Response{StatusCode: base.StatusOK}, stream, nil
}

// OnPlay handles PLAY requests.
func (s *RTSPServer) OnPlay(ctx *gortsplib.ServerHandlerOnPlayCtx) (*base.Response, error) {
	camID := strings.TrimPrefix(ctx.Session.Path(), "/")
	if camID == "" && ctx.Path != "" {
		camID = strings.TrimPrefix(ctx.Path, "/")
	}
	clientID := uuid.New().String()[:8]

	var tracker *rtpengine.ClientEvictionTracker
	var cursor *ringbuffer.ReaderCursor
	if s.manager != nil {
		if cam, ok := s.manager.Get(camID); ok {
			cam.IncRTSPClient(context.Background())
			watermark := cam.Config.LatencyWatermark.Duration()
			degStep := true
			if cam.Config.DegradationStep != nil {
				degStep = *cam.Config.DegradationStep
			}
			tracker = rtpengine.NewClientEvictionTracker(clientID, watermark, degStep)
			if ring := cam.SharedRing(); ring != nil {
				cursor = ring.NewReaderCursor(clientID, watermark)
			}
		}
	}

	s.sessionsMu.Lock()
	s.sessions[ctx.Session] = &rtspSessionData{
		cameraID:        camID,
		clientID:        clientID,
		evictionTracker: tracker,
		cursor:          cursor,
	}
	s.sessionsMu.Unlock()

	s.logger.Info("Downstream RTSP client started playing",
		slog.String("camera_id", camID),
		slog.String("client_id", clientID),
	)

	return &base.Response{StatusCode: base.StatusOK}, nil
}

// OnSessionClose handles RTSP session teardown.
func (s *RTSPServer) OnSessionClose(ctx *gortsplib.ServerHandlerOnSessionCloseCtx) {
	s.sessionsMu.Lock()
	sessionData, exists := s.sessions[ctx.Session]
	if exists {
		delete(s.sessions, ctx.Session)
	}
	s.sessionsMu.Unlock()

	if exists && s.manager != nil {
		if cam, ok := s.manager.Get(sessionData.cameraID); ok {
			cam.DecRTSPClient()
			if ring := cam.SharedRing(); ring != nil {
				ring.RemoveReaderCursor(sessionData.clientID)
			}
			s.metrics.RemoveClientMetrics(cam.Config.Name, sessionData.clientID)
		}
		s.logger.Info("Downstream RTSP client disconnected",
			slog.String("camera_id", sessionData.cameraID),
			slog.String("client_id", sessionData.clientID),
		)
	}
}

// OnStreamWriteError is called when a ServerStream fails to write to a slow reader session.
func (s *RTSPServer) OnStreamWriteError(ctx *gortsplib.ServerHandlerOnStreamWriteErrorCtx) {
	s.sessionsMu.Lock()
	sessionData, exists := s.sessions[ctx.Session]
	if exists {
		sessionData.lastWriteErr = time.Now()
		if sessionData.evictionTracker != nil {
			// Trigger GOP eviction on write stalls/buffer overflow
			sessionData.evictionTracker.State = rtpengine.StateEvicted
			sessionData.evictionTracker.EvictionCount++
		}
	}
	s.sessionsMu.Unlock()

	if exists && s.manager != nil {
		if cam, ok := s.manager.Get(sessionData.cameraID); ok {
			s.metrics.IncDownstreamDropped(cam.Config.Name, sessionData.clientID)
			s.metrics.IncGOPEvictions(cam.Config.Name, sessionData.clientID)
			if sessionData.cursor != nil {
				lagMs := float64(sessionData.cursor.LagMs())
				s.metrics.SetDownstreamLagMs(cam.Config.Name, sessionData.clientID, lagMs)
			}
		}
		s.logger.Warn("Downstream RTSP client stalled (slow consumer drop, GOP eviction triggered)",
			slog.String("camera_id", sessionData.cameraID),
			slog.String("client_id", sessionData.clientID),
			slog.Any("error", ctx.Error),
		)
	}
}
