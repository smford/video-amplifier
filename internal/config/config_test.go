package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Server.HTTPPort != 8080 {
		t.Fatalf("expected default HTTP port 8080, got %d", cfg.Server.HTTPPort)
	}
	if cfg.Server.RTSPPort != 8554 {
		t.Fatalf("expected default RTSP port 8554, got %d", cfg.Server.RTSPPort)
	}
	if cfg.Server.MetricsPort != 9090 {
		t.Fatalf("expected default Metrics port 9090, got %d", cfg.Server.MetricsPort)
	}
}

func TestLoadConfig_YAML(t *testing.T) {
	yamlData := `
server:
  http_port: 8081
  rtsp_port: 8555
  metrics_port: 9091
  read_timeout: 15s
  write_timeout: 20s
  log_level: DEBUG

cameras:
  - id: "front-door"
    name: "Front Door 4K"
    upstream_url: "rtsp://admin:secret@192.168.1.50:554/h264Preview_01_main"
    mode: "always-on"
    idle_timeout: "45s"
    client_buffer_size: 120
    retry_interval: "3s"

  - id: "workshop-mjpeg"
    name: "Workshop Overhead"
    upstream_url: "http://192.168.1.60/video.mjpg"
    mode: "on-demand"
    client_buffer_size: 30
`
	tmpDir := t.TempDir()
	cfgFile := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(cfgFile, []byte(yamlData), 0644); err != nil {
		t.Fatalf("failed to write test config file: %v", err)
	}

	cfg, err := LoadConfig(cfgFile)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Server.HTTPPort != 8081 {
		t.Errorf("expected HTTP port 8081, got %d", cfg.Server.HTTPPort)
	}
	if cfg.Server.RTSPPort != 8555 {
		t.Errorf("expected RTSP port 8555, got %d", cfg.Server.RTSPPort)
	}
	if cfg.Server.MetricsPort != 9091 {
		t.Errorf("expected Metrics port 9091, got %d", cfg.Server.MetricsPort)
	}
	if cfg.Server.LogLevel != "DEBUG" {
		t.Errorf("expected LogLevel DEBUG, got %s", cfg.Server.LogLevel)
	}
	if len(cfg.Cameras) != 2 {
		t.Fatalf("expected 2 cameras, got %d", len(cfg.Cameras))
	}

	cam1 := cfg.Cameras[0]
	if cam1.ID != "front-door" || cam1.Name != "Front Door 4K" {
		t.Errorf("cam1 mismatch: %+v", cam1)
	}
	if cam1.Mode != ModeAlwaysOn {
		t.Errorf("expected ModeAlwaysOn, got %s", cam1.Mode)
	}
	if cam1.IdleTimeout.Duration() != 45*time.Second {
		t.Errorf("expected 45s idle timeout, got %v", cam1.IdleTimeout.Duration())
	}
	if cam1.ClientBufferSize != 120 {
		t.Errorf("expected 120 buffer size, got %d", cam1.ClientBufferSize)
	}
	if cam1.RTSPTransport != TransportTCP {
		t.Errorf("expected default TCP transport, got %s", cam1.RTSPTransport)
	}

	cam2 := cfg.Cameras[1]
	if cam2.ID != "workshop-mjpeg" || cam2.Mode != ModeOnDemand {
		t.Errorf("cam2 mismatch: %+v", cam2)
	}
	if cam2.ClientBufferSize != 30 {
		t.Errorf("expected 30 buffer size, got %d", cam2.ClientBufferSize)
	}
	// defaults filled
	if cam2.IdleTimeout.Duration() != 30*time.Second {
		t.Errorf("expected default 30s idle timeout, got %v", cam2.IdleTimeout.Duration())
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	t.Setenv("VIDEO_AMPLIFIER_SERVER_HTTP_PORT", "9999")
	t.Setenv("VIDEO_AMPLIFIER_LOG_LEVEL", "error")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Server.HTTPPort != 9999 {
		t.Errorf("expected overridden HTTP port 9999, got %d", cfg.Server.HTTPPort)
	}
	if cfg.Server.LogLevel != "ERROR" {
		t.Errorf("expected overridden LogLevel ERROR, got %s", cfg.Server.LogLevel)
	}
}

func TestConfigValidation_DuplicateID(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Cameras = []CameraConfig{
		{ID: "cam1", UpstreamURL: "rtsp://10.0.0.1/live"},
		{ID: "cam1", UpstreamURL: "rtsp://10.0.0.2/live"},
	}

	if err := cfg.ValidateAndSetDefaults(); err == nil {
		t.Fatalf("expected error on duplicate camera ID, got nil")
	}
}

func TestConfigValidation_InvalidURL(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Cameras = []CameraConfig{
		{ID: "cam1", UpstreamURL: "ftp://10.0.0.1/live"},
	}

	if err := cfg.ValidateAndSetDefaults(); err == nil {
		t.Fatalf("expected error on invalid URL scheme, got nil")
	}
}

func TestWriteSampleConfig(t *testing.T) {
	tmpDir := t.TempDir()
	targetPath := filepath.Join(tmpDir, "generated.yaml")

	// Initial write should succeed
	if err := WriteSampleConfig(targetPath, false); err != nil {
		t.Fatalf("failed to write sample config: %v", err)
	}

	// Loading the written file should parse cleanly
	cfg, err := LoadConfig(targetPath)
	if err != nil {
		t.Fatalf("failed to load generated sample config: %v", err)
	}
	if len(cfg.Cameras) == 0 {
		t.Fatalf("expected cameras in sample config, got 0")
	}

	// Second write without force should fail
	if err := WriteSampleConfig(targetPath, false); err == nil {
		t.Fatalf("expected error when writing without force to existing file")
	}

	// Write with force should succeed
	if err := WriteSampleConfig(targetPath, true); err != nil {
		t.Fatalf("expected success with force=true: %v", err)
	}
}
