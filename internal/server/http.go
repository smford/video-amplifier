package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/egress"
	"github.com/smford/video-amplifier/internal/logging"
	"github.com/smford/video-amplifier/internal/upstream"
)

// HTTPServer handles HTTP MJPEG streaming, snapshot caching, and API/health endpoints.
type HTTPServer struct {
	cfg     *config.Config
	manager *upstream.Manager
	logger  *slog.Logger
	server  *http.Server
	whep    *egress.WHEPManager
	fmp4    *egress.FMP4Manager

	mu        sync.RWMutex
	clientIDs map[string]context.CancelFunc
	startTime time.Time
}

// NewHTTPServer initializes the HTTP streaming server.
func NewHTTPServer(cfg *config.Config, manager *upstream.Manager, logger *slog.Logger) *HTTPServer {
	if logger == nil {
		logger = slog.Default()
	}

	s := &HTTPServer{
		cfg:       cfg,
		manager:   manager,
		logger:    logger.With(slog.String("component", "http_server")),
		whep:      egress.NewWHEPManager(logger),
		fmp4:      egress.NewFMP4Manager(logger),
		clientIDs: make(map[string]context.CancelFunc),
		startTime: time.Now(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/cameras", s.handleCamerasList)
	mux.HandleFunc("/cameras/", s.handleCameraRoute)
	mux.HandleFunc("/snapshot.jpg", s.handleRootSnapshot)
	mux.HandleFunc("/ui", s.handleDashboard)
	mux.HandleFunc("/dashboard", s.handleDashboard)

	// Direct short routes e.g. /{id}/mjpeg and /{id}/snapshot.jpg, plus / for dashboard
	mux.HandleFunc("/", s.handleCatchAll)

	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.HTTPPort),
		Handler:      mux,
		ReadTimeout:  cfg.Server.ReadTimeout.Duration(),
		WriteTimeout: 0, // Streaming endpoints write continuously without timeout
	}

	return s
}

// Start runs the HTTP server listener.
func (s *HTTPServer) Start() error {
	s.logger.Info("Starting HTTP streaming server", slog.Int("port", s.cfg.Server.HTTPPort))
	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server failed: %w", err)
	}
	return nil
}

// Stop gracefully shuts down active HTTP connections within the provided context.
func (s *HTTPServer) Stop(ctx context.Context) error {
	s.logger.Info("Shutting down HTTP streaming server")

	// Cancel all active streaming clients
	s.mu.Lock()
	for _, cancel := range s.clientIDs {
		cancel()
	}
	s.clientIDs = make(map[string]context.CancelFunc)
	s.mu.Unlock()

	return s.server.Shutdown(ctx)
}

// handleHealthz handles liveness probes returning JSON detailing stream resolution, GOP interval, and uptime.
func (s *HTTPServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	uptime := time.Since(s.startTime)

	var camHealth []CameraHealthDetail
	if s.manager != nil {
		for _, cam := range s.manager.List() {
			bitrate, fps := cam.LiveTelemetry()
			gop := cam.GOPInterval()
			gopStr := "n/a"
			if gop > 0 {
				gopStr = gop.Round(time.Millisecond).String()
			}
			camHealth = append(camHealth, CameraHealthDetail{
				ID:          cam.Config.ID,
				Name:        cam.Config.Name,
				State:       string(cam.State()),
				Resolution:  cam.Resolution(),
				GOPInterval: gopStr,
				BitrateBps:  bitrate,
				FPS:         fps,
				Clients:     cam.TotalActiveClients(),
			})
		}
	}

	resp := HealthResponse{
		Status:    "healthy",
		Uptime:    uptime.Round(time.Second).String(),
		UptimeSec: int64(uptime.Seconds()),
		Cameras:   camHealth,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// handleReadyz handles readiness probes.
func (s *HTTPServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.manager.IsReady() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("READY\n"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("NOT READY\n"))
	}
}

// handleCamerasList returns a JSON list of configured cameras and statuses.
func (s *HTTPServer) handleCamerasList(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/cameras" {
		http.NotFound(w, r)
		return
	}

	summaries := s.manager.Summaries()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(summaries)
}

// handleCameraRoute handles /cameras/{id}/... routes.
func (s *HTTPServer) handleCameraRoute(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/cameras/")
	parts := strings.Split(path, "/")

	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	camID := parts[0]
	cam, ok := s.manager.Get(camID)
	if !ok {
		http.Error(w, fmt.Sprintf("camera %q not found", camID), http.StatusNotFound)
		return
	}

	if len(parts) == 1 {
		// GET /cameras/{id} -> camera status JSON
		w.Header().Set("Content-Type", "application/json")
		summary := upstream.CameraSummary{
			ID:            cam.Config.ID,
			Name:          cam.Config.Name,
			UpstreamURL:   logging.RedactCredentials(cam.Config.UpstreamURL),
			Mode:          cam.Config.Mode,
			State:         cam.State(),
			ActiveClients: cam.TotalActiveClients(),
			BytesReceived: cam.BytesReceived(),
		}
		_ = json.NewEncoder(w).Encode(summary)
		return
	}

	// Validate downstream credentials (AuthMode: none, custom, or passthrough)
	if !s.authorizeCameraRequest(w, r, cam) {
		return
	}

	action := parts[1]
	switch action {
	case "mjpeg", "stream.mjpg":
		s.streamMJPEG(w, r, cam)
	case "snapshot.jpg", "snapshot":
		s.serveSnapshot(w, r, cam)
	case "whep":
		if len(parts) >= 3 {
			s.whep.HandleWHEPResource(w, r, camID, parts[2])
		} else {
			s.whep.HandleWHEPOffer(w, r, cam)
		}
	case "fmp4", "live.mp4":
		s.fmp4.HandleFMP4Stream(w, r, cam)
	case "ws", "fmp4.ws":
		s.fmp4.HandleWebSocketStream(w, r, cam)
	case "onvif", "device_service":
		s.handleONVIF(w, r, cam)
	default:
		http.NotFound(w, r)
	}
}

// handleONVIF serves cached ONVIF XML responses (ITEM 7: ONVIF & Metadata Decoupling).
func (s *HTTPServer) handleONVIF(w http.ResponseWriter, r *http.Request, cam *upstream.CameraStream) {
	w.Header().Set("Content-Type", "application/soap+xml; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// If request body contains GetProfiles, return cached profiles
	buf := make([]byte, 1024)
	n, _ := r.Body.Read(buf)
	bodyStr := string(buf[:n])

	if strings.Contains(bodyStr, "GetProfiles") {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(cam.GetONVIFProfiles())
		return
	}

	// Default to cached capabilities
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(cam.GetONVIFCapabilities())
}

// handleRootSnapshot handles /snapshot.jpg?camera={id}
func (s *HTTPServer) handleRootSnapshot(w http.ResponseWriter, r *http.Request) {
	camID := r.URL.Query().Get("camera")
	if camID == "" {
		// If only 1 camera configured, default to it
		cams := s.manager.List()
		if len(cams) == 1 {
			camID = cams[0].Config.ID
		} else {
			http.Error(w, "missing ?camera={id} query parameter", http.StatusBadRequest)
			return
		}
	}

	cam, ok := s.manager.Get(camID)
	if !ok {
		http.Error(w, fmt.Sprintf("camera %q not found", camID), http.StatusNotFound)
		return
	}
	if !s.authorizeCameraRequest(w, r, cam) {
		return
	}
	s.serveSnapshot(w, r, cam)
}

// handleCatchAll handles /{id}/mjpeg or /{id}/snapshot.jpg, or / for dashboard
func (s *HTTPServer) handleCatchAll(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" || r.URL.Path == "" {
		s.handleDashboard(w, r)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	if len(parts) == 2 {
		camID := parts[0]
		action := parts[1]

		cam, ok := s.manager.Get(camID)
		if ok {
			if !s.authorizeCameraRequest(w, r, cam) {
				return
			}
			switch action {
			case "mjpeg", "stream.mjpg":
				s.streamMJPEG(w, r, cam)
				return
			case "snapshot.jpg", "snapshot":
				s.serveSnapshot(w, r, cam)
				return
			case "fmp4", "live.mp4":
				s.fmp4.HandleFMP4Stream(w, r, cam)
				return
			case "ws", "fmp4.ws":
				s.fmp4.HandleWebSocketStream(w, r, cam)
				return
			case "whep":
				s.whep.HandleWHEPOffer(w, r, cam)
				return
			}
		}
	}

	http.NotFound(w, r)
}

// authorizeCameraRequest validates downstream HTTP credentials against camera authentication policy.
func (s *HTTPServer) authorizeCameraRequest(w http.ResponseWriter, r *http.Request, cam *upstream.CameraStream) bool {
	expectedUser, _, required := cam.GetExpectedCredentials()
	if !required {
		return true // Auth is disabled
	}

	user, pass, ok := r.BasicAuth()
	if !ok || !cam.ValidateCredentials(user, pass) {
		w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Basic realm="video-amplifier (%s)"`, cam.Config.Name))
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}

	_ = expectedUser
	return true
}

// streamMJPEG streams multipart/x-mixed-replace JPEG frames to a downstream HTTP client.
func (s *HTTPServer) streamMJPEG(w http.ResponseWriter, r *http.Request, cam *upstream.CameraStream) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported by client connection", http.StatusInternalServerError)
		return
	}

	clientID := uuid.New().String()[:8]
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	s.mu.Lock()
	s.clientIDs[clientID] = cancel
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clientIDs, clientID)
		s.mu.Unlock()
	}()

	s.logger.Info("Downstream MJPEG client connected",
		slog.String("camera", cam.Config.Name),
		slog.String("client_id", clientID),
		slog.String("remote_addr", r.RemoteAddr),
	)

	// Subscribe client to camera's bounded ring buffer
	queue := cam.SubscribeMJPEG(clientID, ctx)
	defer cam.UnsubscribeMJPEG(clientID)

	boundary := "videoamplifierframe"
	w.Header().Set("Content-Type", "multipart/x-mixed-replace; boundary="+boundary)
	w.Header().Set("Cache-Control", "no-cache, private, max-age=0, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Connection", "close")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for {
		// Try popping from ring buffer with 2s timeout
		popCtx, popCancel := context.WithTimeout(ctx, 2*time.Second)
		frame, err := queue.Pop(popCtx)
		popCancel()

		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			// If ring buffer timed out, fall back to fetching snapshot on-demand
			snapCtx, snapCancel := context.WithTimeout(ctx, 3*time.Second)
			snapFrame, _, snapErr := cam.GetOrFetchSnapshot(snapCtx)
			snapCancel()
			if snapErr != nil || len(snapFrame) == 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(500 * time.Millisecond):
					continue
				}
			}
			frame = snapFrame
		}

		// Write multipart JPEG frame
		header := fmt.Sprintf("\r\n--%s\r\nContent-Type: image/jpeg\r\nContent-Length: %d\r\n\r\n", boundary, len(frame))
		if _, err := w.Write([]byte(header)); err != nil {
			return
		}
		if _, err := w.Write(frame); err != nil {
			return
		}
		flusher.Flush()
	}
}

// serveSnapshot serves the cached snapshot or fetches one on-demand.
func (s *HTTPServer) serveSnapshot(w http.ResponseWriter, r *http.Request, cam *upstream.CameraStream) {
	snapCtx, snapCancel := context.WithTimeout(r.Context(), 6*time.Second)
	defer snapCancel()

	frame, ts, err := cam.GetOrFetchSnapshot(snapCtx)
	if err == nil && len(frame) > 0 {
		s.writeJPEGResponse(w, frame, ts)
		return
	}

	s.logger.Warn("Failed to serve snapshot", slog.String("camera", cam.Config.Name), slog.Any("error", err))
	http.Error(w, "Snapshot not available (camera warming up or offline)", http.StatusServiceUnavailable)
}

func (s *HTTPServer) writeJPEGResponse(w http.ResponseWriter, frame []byte, ts time.Time) {
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(frame)))
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Last-Modified", ts.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(frame)
}
