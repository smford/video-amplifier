package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/metrics"
	"github.com/smford/video-amplifier/internal/server"
	"github.com/smford/video-amplifier/internal/upstream"
)

// App manages the lifecycle of all video-amplifier subsystems.
type App struct {
	Config        *config.Config
	Logger        *slog.Logger
	Metrics       *metrics.Metrics
	RTSPServer    *server.RTSPServer
	HTTPServer    *server.HTTPServer
	MetricsServer *server.MetricsServer
	Manager       *upstream.Manager
}

// New constructs and links all components of video-amplifier.
func New(cfg *config.Config, logger *slog.Logger) (*App, error) {
	if logger == nil {
		logger = slog.Default()
	}

	m := metrics.NewMetrics(nil)

	rtspSrv := server.NewRTSPServer(cfg, m, logger)
	mgr := upstream.NewManager(cfg, m, logger, rtspSrv)
	rtspSrv.SetManager(mgr)

	httpSrv := server.NewHTTPServer(cfg, mgr, logger)

	var metricsSrv *server.MetricsServer
	if cfg.Server.MetricsPort != cfg.Server.HTTPPort {
		metricsSrv = server.NewMetricsServer(cfg, m, mgr, logger)
	}

	return &App{
		Config:        cfg,
		Logger:        logger,
		Metrics:       m,
		RTSPServer:    rtspSrv,
		HTTPServer:    httpSrv,
		MetricsServer: metricsSrv,
		Manager:       mgr,
	}, nil
}

// Run starts all servers and blocks until the context is canceled (SIGTERM/SIGINT).
// On cancellation, it conducts a coordinated graceful shutdown within a 10s deadline.
func (a *App) Run(ctx context.Context) error {
	a.Logger.Info("Initializing video-amplifier services")

	// 1. Start RTSP server
	if err := a.RTSPServer.Start(); err != nil {
		return fmt.Errorf("failed to start RTSP server on port %d: %w", a.Config.Server.RTSPPort, err)
	}

	// 2. Start Upstream Manager
	if err := a.Manager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start upstream manager: %w", err)
	}

	errChan := make(chan error, 2)

	// 3. Start HTTP Streaming Server
	go func() {
		if err := a.HTTPServer.Start(); err != nil {
			errChan <- fmt.Errorf("http server error: %w", err)
		}
	}()

	// 4. Start Metrics Server if on separate port
	if a.MetricsServer != nil {
		go func() {
			if err := a.MetricsServer.Start(); err != nil {
				errChan <- fmt.Errorf("metrics server error: %w", err)
			}
		}()
	}

	a.Logger.Info("video-amplifier running successfully",
		slog.Int("http_port", a.Config.Server.HTTPPort),
		slog.Int("rtsp_port", a.Config.Server.RTSPPort),
		slog.Int("metrics_port", a.Config.Server.MetricsPort),
		slog.Int("configured_cameras", len(a.Config.Cameras)),
	)

	// Wait for shutdown trigger or fatal runtime error
	select {
	case <-ctx.Done():
		a.Logger.Info("Shutdown signal received, initiating graceful teardown (10s deadline)")
	case err := <-errChan:
		a.Logger.Error("Fatal server error occurred", slog.Any("error", err))
		return err
	}

	return a.GracefulShutdown(10 * time.Second)
}

// GracefulShutdown conducts teardown within the specified deadline:
// 1. Closes downstream HTTP client connections.
// 2. Closes downstream RTSP sessions.
// 3. Closes upstream camera ingest sockets.
// 4. Closes metrics server.
func (a *App) GracefulShutdown(deadline time.Duration) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	var wg sync.WaitGroup

	// Step 1: Disconnect downstream HTTP clients
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := a.HTTPServer.Stop(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
			a.Logger.Warn("HTTP server shutdown warning", slog.Any("error", err))
		}
	}()

	// Step 2: Stop RTSP downstream relay
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.RTSPServer.Stop()
	}()

	// Step 3: Stop metrics server
	if a.MetricsServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.MetricsServer.Stop(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
				a.Logger.Warn("Metrics server shutdown warning", slog.Any("error", err))
			}
		}()
	}

	// Step 4: Stop all upstream cameras
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.Manager.Stop()
	}()

	// Wait for all teardown routines to complete or deadline to expire
	doneChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneChan)
	}()

	select {
	case <-doneChan:
		a.Logger.Info("Graceful shutdown completed cleanly")
		return nil
	case <-shutdownCtx.Done():
		a.Logger.Warn("Shutdown deadline exceeded (10s limit), forcing termination")
		return shutdownCtx.Err()
	}
}
