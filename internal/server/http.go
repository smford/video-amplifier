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
	"github.com/smford/video-amplifier/internal/upstream"
)

// HTTPServer handles HTTP MJPEG streaming, snapshot caching, and API/health endpoints.
type HTTPServer struct {
	cfg     *config.Config
	manager *upstream.Manager
	logger  *slog.Logger
	server  *http.Server

	mu        sync.RWMutex
	clientIDs map[string]context.CancelFunc
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
		clientIDs: make(map[string]context.CancelFunc),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/cameras", s.handleCamerasList)
	mux.HandleFunc("/cameras/", s.handleCameraRoute)
	mux.HandleFunc("/snapshot.jpg", s.handleRootSnapshot)

	// Direct short routes e.g. /{id}/mjpeg and /{id}/snapshot.jpg
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

// handleHealthz handles liveness probes.
func (s *HTTPServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK\n"))
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
			UpstreamURL:   cam.Config.UpstreamURL,
			Mode:          cam.Config.Mode,
			State:         cam.State(),
			ActiveClients: cam.TotalActiveClients(),
			BytesReceived: cam.BytesReceived(),
		}
		_ = json.NewEncoder(w).Encode(summary)
		return
	}

	action := parts[1]
	switch action {
	case "mjpeg", "stream.mjpg":
		s.streamMJPEG(w, r, cam)
	case "snapshot.jpg", "snapshot":
		s.serveSnapshot(w, r, cam)
	default:
		http.NotFound(w, r)
	}
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
	s.serveSnapshot(w, r, cam)
}

// handleCatchAll handles /{id}/mjpeg or /{id}/snapshot.jpg
func (s *HTTPServer) handleCatchAll(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	if len(parts) == 2 {
		camID := parts[0]
		action := parts[1]

		cam, ok := s.manager.Get(camID)
		if ok {
			switch action {
			case "mjpeg", "stream.mjpg":
				s.streamMJPEG(w, r, cam)
				return
			case "snapshot.jpg", "snapshot":
				s.serveSnapshot(w, r, cam)
				return
			}
		}
	}

	http.NotFound(w, r)
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
		frame, err := queue.Pop(ctx)
		if err != nil {
			s.logger.Debug("Downstream MJPEG client stream ended",
				slog.String("camera", cam.Config.Name),
				slog.String("client_id", clientID),
				slog.Any("reason", err),
			)
			return
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

// serveSnapshot serves the cached snapshot or fetches one if on-demand and idle.
func (s *HTTPServer) serveSnapshot(w http.ResponseWriter, r *http.Request, cam *upstream.CameraStream) {
	// Try getting from cache first
	frame, ts, err := cam.GetLatestSnapshot()
	if err == nil && len(frame) > 0 {
		s.writeJPEGResponse(w, frame, ts)
		return
	}

	// If no snapshot in cache and snapshot_url is configured, fetch single frame directly
	if cam.Config.SnapshotURL != "" {
		snapCtx, snapCancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer snapCancel()

		fetchedFrame, fetchErr := upstream.FetchSingleSnapshot(snapCtx, cam.Config.SnapshotURL)
		if fetchErr == nil && len(fetchedFrame) > 0 {
			cam.UpdateSnapshot(fetchedFrame)
			s.writeJPEGResponse(w, fetchedFrame, time.Now())
			return
		}
	}

	// If camera is HTTP and no snapshot yet, try fetching single frame from UpstreamURL
	if strings.HasPrefix(cam.Config.UpstreamURL, "http://") || strings.HasPrefix(cam.Config.UpstreamURL, "https://") {
		// Wake up camera stream
		cam.OnClientConnected(r.Context(), "mjpeg")
		defer cam.OnClientDisconnected("mjpeg")

		// Poll for first frame up to 3 seconds
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(100 * time.Millisecond):
				frame, ts, err := cam.GetLatestSnapshot()
				if err == nil && len(frame) > 0 {
					s.writeJPEGResponse(w, frame, ts)
					return
				}
			}
		}
	}

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
