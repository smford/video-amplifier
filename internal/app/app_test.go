package app

import (
	"context"
	"testing"
	"time"

	"github.com/smford/video-amplifier/internal/config"
)

func TestApp_LifecycleAndGracefulShutdown(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Server.HTTPPort = 18080
	cfg.Server.RTSPPort = 18554
	cfg.Server.MetricsPort = 19090

	application, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("failed to create app: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	runErrChan := make(chan error, 1)
	go func() {
		runErrChan <- application.Run(ctx)
	}()

	// Allow servers to start
	time.Sleep(100 * time.Millisecond)

	// Trigger graceful shutdown
	cancel()

	select {
	case err := <-runErrChan:
		if err != nil {
			t.Fatalf("Run returned error on graceful shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Graceful shutdown did not complete within 3 seconds")
	}
}
