package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Mode represents camera upstream connection mode.
type Mode string

const (
	ModeAlwaysOn Mode = "always-on"
	ModeOnDemand Mode = "on-demand"
)

// RTSPTransport specifies RTSP transport protocol.
type RTSPTransport string

const (
	TransportTCP  RTSPTransport = "tcp"
	TransportUDP  RTSPTransport = "udp"
	TransportAuto RTSPTransport = "auto"
)

// Duration is a wrapper around time.Duration that supports YAML string parsing (e.g., "10s", "1m").
type Duration time.Duration

// Duration returns the underlying time.Duration.
func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

// UnmarshalYAML implements yaml.Unmarshaler for Duration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err == nil {
		parsed, err := time.ParseDuration(s)
		if err != nil {
			// Try parsing as integer seconds
			sec, errSec := strconv.Atoi(s)
			if errSec != nil {
				return fmt.Errorf("invalid duration %q: %w", s, err)
			}
			*d = Duration(time.Duration(sec) * time.Second)
			return nil
		}
		*d = Duration(parsed)
		return nil
	}

	var sec int
	if err := value.Decode(&sec); err == nil {
		*d = Duration(time.Duration(sec) * time.Second)
		return nil
	}

	return fmt.Errorf("cannot unmarshal duration from node kind %v", value.Kind)
}

// MarshalYAML implements yaml.Marshaler for Duration.
func (d Duration) MarshalYAML() (interface{}, error) {
	return time.Duration(d).String(), nil
}

// ServerConfig holds HTTP, RTSP, and Metrics server settings.
type ServerConfig struct {
	HTTPPort     int      `yaml:"http_port"`
	RTSPPort     int      `yaml:"rtsp_port"`
	MetricsPort  int      `yaml:"metrics_port"`
	ReadTimeout  Duration `yaml:"read_timeout"`
	WriteTimeout Duration `yaml:"write_timeout"`
	LogLevel     string   `yaml:"log_level"`
	LogFormat    string   `yaml:"log_format"` // "text" or "json"
}

// CameraConfig holds configuration for an individual camera stream.
type CameraConfig struct {
	ID               string        `yaml:"id"`
	Name             string        `yaml:"name"`
	UpstreamURL      string        `yaml:"upstream_url"`
	Mode             Mode          `yaml:"mode"`
	IdleTimeout      Duration      `yaml:"idle_timeout"`
	ClientBufferSize int           `yaml:"client_buffer_size"`
	RetryInterval    Duration      `yaml:"retry_interval"`
	RTSPTransport    RTSPTransport `yaml:"rtsp_transport"`
	ClientTimeout    Duration      `yaml:"client_timeout"`
	SnapshotURL      string        `yaml:"snapshot_url"`

	// GOP-Aware Intelligent Frame Eviction (ITEM 1)
	LatencyWatermark Duration      `yaml:"latency_watermark"` // Max buffer latency before eviction (default: 1500ms)
	DegradationStep  *bool         `yaml:"degradation_step"`  // Drop non-ref frames before full keyframe eviction (default: true)

	// Upstream Decoupling & Synthetic Keep-Alives (ITEM 2)
	DisconnectTimeout   Duration   `yaml:"disconnect_timeout"`   // Upstream timeout before synthetic keep-alive (default: 3s)
	SyntheticKeepAlives *bool      `yaml:"synthetic_keepalives"` // Enable synthetic keep-alive stream injection (default: true)
}

// Config is the top-level configuration structure.
type Config struct {
	Server  ServerConfig   `yaml:"server"`
	Cameras []CameraConfig `yaml:"cameras"`
}

// DefaultConfig returns a Config populated with production-safe defaults.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			HTTPPort:     8080,
			RTSPPort:     8554,
			MetricsPort:  9090,
			ReadTimeout:  Duration(10 * time.Second),
			WriteTimeout: Duration(10 * time.Second),
			LogLevel:     "INFO",
			LogFormat:    "text",
		},
		Cameras: make([]CameraConfig, 0),
	}
}

// LoadConfig reads config from a YAML file, applies defaults, and overrides with environment variables.
func LoadConfig(filePath string) (*Config, error) {
	cfg := DefaultConfig()

	if filePath != "" {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file %q: %w", filePath, err)
		}

		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse YAML config: %w", err)
		}
	}

	// Apply environment variable overrides
	applyEnvOverrides(cfg)

	// Set camera defaults and validate
	if err := cfg.ValidateAndSetDefaults(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// applyEnvOverrides allows 12-factor configuration via environment variables.
func applyEnvOverrides(cfg *Config) {
	if val := os.Getenv("VIDEO_AMPLIFIER_SERVER_HTTP_PORT"); val != "" {
		if p, err := strconv.Atoi(val); err == nil && p > 0 {
			cfg.Server.HTTPPort = p
		}
	}
	if val := os.Getenv("VIDEO_AMPLIFIER_SERVER_RTSP_PORT"); val != "" {
		if p, err := strconv.Atoi(val); err == nil && p > 0 {
			cfg.Server.RTSPPort = p
		}
	}
	if val := os.Getenv("VIDEO_AMPLIFIER_SERVER_METRICS_PORT"); val != "" {
		if p, err := strconv.Atoi(val); err == nil && p > 0 {
			cfg.Server.MetricsPort = p
		}
	}
	if val := os.Getenv("VIDEO_AMPLIFIER_LOG_LEVEL"); val != "" {
		cfg.Server.LogLevel = strings.ToUpper(strings.TrimSpace(val))
	}
	if val := os.Getenv("VIDEO_AMPLIFIER_LOG_FORMAT"); val != "" {
		cfg.Server.LogFormat = strings.ToLower(strings.TrimSpace(val))
	}
	if val := os.Getenv("VIDEO_AMPLIFIER_SERVER_READ_TIMEOUT"); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			cfg.Server.ReadTimeout = Duration(d)
		}
	}
	if val := os.Getenv("VIDEO_AMPLIFIER_SERVER_WRITE_TIMEOUT"); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			cfg.Server.WriteTimeout = Duration(d)
		}
	}
}

// ValidateAndSetDefaults validates all configuration values and populates sensible per-camera defaults.
func (c *Config) ValidateAndSetDefaults() error {
	if c.Server.HTTPPort <= 0 || c.Server.HTTPPort > 65535 {
		return fmt.Errorf("server.http_port must be between 1 and 65535, got %d", c.Server.HTTPPort)
	}
	if c.Server.RTSPPort <= 0 || c.Server.RTSPPort > 65535 {
		return fmt.Errorf("server.rtsp_port must be between 1 and 65535, got %d", c.Server.RTSPPort)
	}
	if c.Server.MetricsPort <= 0 || c.Server.MetricsPort > 65535 {
		return fmt.Errorf("server.metrics_port must be between 1 and 65535, got %d", c.Server.MetricsPort)
	}
	if c.Server.ReadTimeout.Duration() <= 0 {
		c.Server.ReadTimeout = Duration(10 * time.Second)
	}
	if c.Server.WriteTimeout.Duration() <= 0 {
		c.Server.WriteTimeout = Duration(10 * time.Second)
	}

	seenIDs := make(map[string]bool)

	for i := range c.Cameras {
		cam := &c.Cameras[i]

		if cam.ID == "" {
			return fmt.Errorf("cameras[%d]: id must not be empty", i)
		}
		if seenIDs[cam.ID] {
			return fmt.Errorf("cameras[%d]: duplicate camera id %q", i, cam.ID)
		}
		seenIDs[cam.ID] = true

		if cam.Name == "" {
			cam.Name = cam.ID
		}

		if cam.UpstreamURL == "" {
			return fmt.Errorf("camera %q: upstream_url must not be empty", cam.ID)
		}

		parsedURL, err := url.Parse(cam.UpstreamURL)
		if err != nil || (parsedURL.Scheme != "rtsp" && parsedURL.Scheme != "rtsps" && parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
			return fmt.Errorf("camera %q: upstream_url %q must have valid scheme (rtsp, rtsps, http, https)", cam.ID, cam.UpstreamURL)
		}

		switch cam.Mode {
		case "":
			cam.Mode = ModeAlwaysOn
		case ModeAlwaysOn, ModeOnDemand:
			// valid
		default:
			return fmt.Errorf("camera %q: invalid mode %q (must be %q or %q)", cam.ID, cam.Mode, ModeAlwaysOn, ModeOnDemand)
		}

		if cam.IdleTimeout.Duration() <= 0 {
			cam.IdleTimeout = Duration(30 * time.Second)
		}
		if cam.ClientBufferSize <= 0 {
			cam.ClientBufferSize = 60
		}
		if cam.RetryInterval.Duration() <= 0 {
			cam.RetryInterval = Duration(5 * time.Second)
		}
		if cam.ClientTimeout.Duration() <= 0 {
			cam.ClientTimeout = Duration(10 * time.Second)
		}

		switch cam.RTSPTransport {
		case "":
			cam.RTSPTransport = TransportTCP
		case TransportTCP, TransportUDP, TransportAuto:
			// valid
		default:
			return fmt.Errorf("camera %q: invalid rtsp_transport %q (must be tcp, udp, or auto)", cam.ID, cam.RTSPTransport)
		}

		if cam.LatencyWatermark.Duration() <= 0 {
			cam.LatencyWatermark = Duration(1500 * time.Millisecond)
		}
		if cam.DegradationStep == nil {
			defaultTrue := true
			cam.DegradationStep = &defaultTrue
		}
		if cam.DisconnectTimeout.Duration() <= 0 {
			cam.DisconnectTimeout = Duration(3 * time.Second)
		}
		if cam.SyntheticKeepAlives == nil {
			defaultTrue := true
			cam.SyntheticKeepAlives = &defaultTrue
		}
	}

	return nil
}

// SampleConfigYAML returns a production-ready, fully commented configuration file template.
func SampleConfigYAML() string {
	return `# video-amplifier Configuration File
# High-performance streaming proxy for IP cameras with zero-transcode fan-out,
# upstream isolation, circuit breaking, and backpressure protection.

server:
  http_port: 8080         # HTTP streaming port (MJPEG, snapshots, status API, web UI)
  rtsp_port: 8554         # Downstream RTSP relay server port
  metrics_port: 9090      # Dedicated Prometheus /metrics and health probes port
  read_timeout: 10s       # Network socket read timeout
  write_timeout: 10s      # Network socket write timeout
  log_level: "INFO"       # Log level: DEBUG, INFO, WARN, ERROR
  log_format: "text"      # Log format: "text" or "json"

cameras:
  # Example 1: 24/7 Security camera (always-on persistent ingest)
  - id: "front-door"
    name: "Front Door 4K"
    upstream_url: "rtsp://admin:secret@192.168.1.50:554/h264Preview_01_main"
    mode: "always-on"            # "always-on" or "on-demand"
    idle_timeout: "30s"          # Idle grace period before stopping on-demand stream
    client_buffer_size: 120      # Max frames/packets queued per downstream client
    retry_interval: "5s"         # Base reconnect interval before exponential backoff
    rtsp_transport: "tcp"        # "tcp", "udp", or "auto" (tcp recommended for reliability)
    client_timeout: "10s"        # Inactivity threshold before dropping lagged client
    latency_watermark: "1500ms"  # Buffer latency threshold before GOP keyframe eviction
    degradation_step: true       # Drop non-ref frames prior to full keyframe eviction
    disconnect_timeout: "3s"     # Time without packets before synthetic keep-alive stream
    synthetic_keepalives: true   # Maintain downstream NVR sessions during upstream dropouts

  # Example 2: Backyard PTZ camera (on-demand connection saves bandwidth)
  - id: "backyard"
    name: "Backyard PTZ"
    upstream_url: "rtsp://admin:secret@192.168.1.51:554/stream1"
    mode: "on-demand"            # Ingests only when >= 1 downstream viewer is active
    idle_timeout: "30s"          # Keeps feed warm for 30s after last client disconnects
    client_buffer_size: 60
    retry_interval: "5s"
    rtsp_transport: "tcp"
    client_timeout: "10s"

  # Example 3: Workshop Overhead MJPEG camera
  - id: "workshop-mjpeg"
    name: "Workshop Overhead"
    upstream_url: "http://192.168.1.60/video.mjpg"
    mode: "on-demand"
    idle_timeout: "30s"
    client_buffer_size: 30
    retry_interval: "5s"
`
}

// WriteSampleConfig writes the sample configuration to targetPath.
// If force is false and the file already exists, it returns an error.
func WriteSampleConfig(targetPath string, force bool) error {
	if !force {
		if _, err := os.Stat(targetPath); err == nil {
			return fmt.Errorf("file %q already exists (use --force to overwrite)", targetPath)
		}
	}
	return os.WriteFile(targetPath, []byte(SampleConfigYAML()), 0644)
}
