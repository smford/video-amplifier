package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/metrics"
	"github.com/smford/video-amplifier/internal/upstream"
)

// HealthResponse represents detailed service and stream health (ITEM 6).
type HealthResponse struct {
	Status    string                 `json:"status"`
	Uptime    string                 `json:"uptime"`
	UptimeSec int64                  `json:"uptime_seconds"`
	Cameras   []CameraHealthDetail   `json:"cameras"`
}

type CameraHealthDetail struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	State       string  `json:"state"`
	Resolution  string  `json:"resolution"`
	GOPInterval string  `json:"gop_interval"`
	BitrateBps  int64   `json:"bitrate_bps"`
	FPS         int64   `json:"fps"`
	Clients     int     `json:"clients"`
}

// MetricsServer hosts Prometheus /metrics and health probes on the dedicated metrics_port.
type MetricsServer struct {
	cfg       *config.Config
	metrics   *metrics.Metrics
	manager   *upstream.Manager
	logger    *slog.Logger
	server    *http.Server
	startTime time.Time
}

// NewMetricsServer creates a new metrics server.
func NewMetricsServer(cfg *config.Config, m *metrics.Metrics, mgr *upstream.Manager, logger *slog.Logger) *MetricsServer {
	if logger == nil {
		logger = slog.Default()
	}

	ms := &MetricsServer{
		cfg:       cfg,
		metrics:   m,
		manager:   mgr,
		logger:    logger.With(slog.String("component", "metrics_server")),
		startTime: time.Now(),
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.HTTPHandler())
	mux.HandleFunc("/healthz", ms.handleHealthz)
	mux.HandleFunc("/readyz", ms.handleReadyz)

	ms.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Server.MetricsPort),
		Handler: mux,
	}

	return ms
}

func (ms *MetricsServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	uptime := time.Since(ms.startTime)

	var camHealth []CameraHealthDetail
	if ms.manager != nil {
		for _, cam := range ms.manager.List() {
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

func (ms *MetricsServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if ms.manager != nil && ms.manager.IsReady() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("READY\n"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("NOT READY\n"))
	}
}

// Start launches the metrics HTTP server.
func (ms *MetricsServer) Start() error {
	ms.logger.Info("Starting Prometheus metrics server", slog.Int("port", ms.cfg.Server.MetricsPort))
	if err := ms.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("metrics server failed: %w", err)
	}
	return nil
}

// Stop cleanly terminates the metrics server.
func (ms *MetricsServer) Stop(ctx context.Context) error {
	ms.logger.Info("Shutting down metrics server")
	return ms.server.Shutdown(ctx)
}
