package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/metrics"
	"github.com/smford/video-amplifier/internal/upstream"
)

// MetricsServer hosts Prometheus /metrics and health probes on the dedicated metrics_port.
type MetricsServer struct {
	cfg     *config.Config
	metrics *metrics.Metrics
	manager *upstream.Manager
	logger  *slog.Logger
	server  *http.Server
}

// NewMetricsServer creates a new metrics server.
func NewMetricsServer(cfg *config.Config, m *metrics.Metrics, mgr *upstream.Manager, logger *slog.Logger) *MetricsServer {
	if logger == nil {
		logger = slog.Default()
	}

	ms := &MetricsServer{
		cfg:     cfg,
		metrics: m,
		manager: mgr,
		logger:  logger.With(slog.String("component", "metrics_server")),
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
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK\n"))
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
