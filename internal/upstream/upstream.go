package upstream

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/smford/video-amplifier/internal/circuitbreaker"
	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/logging"
	"github.com/smford/video-amplifier/internal/metrics"
	"github.com/smford/video-amplifier/internal/ringbuffer"
)

var (
	ErrCameraStopped   = errors.New("camera stream stopped")
	ErrNotConnected    = errors.New("upstream camera not connected")
	ErrNoSnapshotFrame = errors.New("no snapshot frame available yet")
)

// StreamState represents operational status of an upstream feed.
type StreamState string

const (
	StateIdle       StreamState = "idle"
	StateConnecting StreamState = "connecting"
	StateStreaming  StreamState = "streaming"
	StateBackoff    StreamState = "backoff"
	StateStopped    StreamState = "stopped"
)

// CameraStream coordinates ingest, resilience, client subscriptions, and caching for a camera feed.
type CameraStream struct {
	Config  config.CameraConfig
	metrics *metrics.Metrics
	logger  *slog.Logger
	breaker *circuitbreaker.CircuitBreaker

	mu         sync.RWMutex
	state      StreamState
	cancelFunc context.CancelFunc

	// Active downstream clients
	activeClientsMu sync.RWMutex
	mjpegClients    map[string]*ringbuffer.RingBuffer[[]byte]
	rtspClientCount int

	// Idle shutdown timer for on-demand feeds
	idleTimer *time.Timer
	idleMu    sync.Mutex

	// Snapshot caching
	snapshotMu   sync.RWMutex
	lastSnapshot []byte
	snapshotTime time.Time

	// Stats
	bytesReceived atomic.Int64
	reconnects    atomic.Int64
	lastConnected atomic.Int64 // Unix timestamp

	// Concrete driver (RTSP or MJPEG)
	driver Driver
}

// Driver defines protocol-specific upstream ingest operations.
type Driver interface {
	Start(ctx context.Context) error
	Stop()
}

// NewCameraStream initializes a new stream manager for a camera.
func NewCameraStream(cfg config.CameraConfig, m *metrics.Metrics, logger *slog.Logger) *CameraStream {
	if logger == nil {
		logger = slog.Default()
	}

	camLogger := logger.With(
		slog.String("camera_id", cfg.ID),
		slog.String("camera_name", cfg.Name),
		slog.String("upstream_url", logging.RedactCredentials(cfg.UpstreamURL)),
	)

	cbConfig := circuitbreaker.DefaultConfig()
	if cfg.RetryInterval.Duration() > 0 {
		cbConfig.BaseDelay = cfg.RetryInterval.Duration()
	}

	cs := &CameraStream{
		Config:       cfg,
		metrics:      m,
		logger:       camLogger,
		breaker:      circuitbreaker.New(cbConfig),
		state:        StateIdle,
		mjpegClients: make(map[string]*ringbuffer.RingBuffer[[]byte]),
	}

	return cs
}

// SetDriver binds the protocol-specific driver (RTSP or MJPEG) to this camera stream.
func (cs *CameraStream) SetDriver(d Driver) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.driver = d
}

// State returns the current stream state.
func (cs *CameraStream) State() StreamState {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.state
}

// SetState updates the current stream state and updates the connected metric.
func (cs *CameraStream) SetState(s StreamState) {
	cs.mu.Lock()
	cs.state = s
	cs.mu.Unlock()

	connected := (s == StateStreaming)
	cs.metrics.SetUpstreamConnected(cs.Config.Name, connected)
	if connected {
		cs.lastConnected.Store(time.Now().Unix())
	}
}

// Start launches the camera stream if mode is always-on. For on-demand, it enters idle state.
func (cs *CameraStream) Start(parentCtx context.Context) {
	if cs.Config.Mode == config.ModeAlwaysOn {
		cs.logger.Info("Starting always-on camera stream")
		cs.ensureRunning(parentCtx)
	} else {
		cs.logger.Info("Initialized on-demand camera stream (standing by for clients)")
	}
}

// ensureRunning starts the upstream driver worker if not already running.
func (cs *CameraStream) ensureRunning(parentCtx context.Context) {
	cs.mu.Lock()
	if cs.state == StateStreaming || cs.state == StateConnecting {
		cs.mu.Unlock()
		return
	}

	ctx, cancel := context.WithCancel(parentCtx)
	cs.cancelFunc = cancel
	cs.state = StateConnecting
	driver := cs.driver
	cs.mu.Unlock()

	// Stop any active idle timer
	cs.idleMu.Lock()
	if cs.idleTimer != nil {
		cs.idleTimer.Stop()
		cs.idleTimer = nil
	}
	cs.idleMu.Unlock()

	go cs.runLoop(ctx, driver)

	if snapURL := cs.ResolvedSnapshotURL(); snapURL != "" && snapURL != cs.Config.UpstreamURL && strings.HasPrefix(snapURL, "rtsp://") {
		StartSnapshotIngest(ctx, cs, snapURL)
	}
}

// runLoop supervises the driver with exponential backoff and circuit breaking.
func (cs *CameraStream) runLoop(ctx context.Context, driver Driver) {
	defer func() {
		cs.SetState(StateIdle)
	}()

	for {
		select {
		case <-ctx.Done():
			cs.logger.Debug("Stream context cancelled, terminating ingest loop")
			return
		default:
		}

		if !cs.breaker.Allow() {
			cs.SetState(StateBackoff)
			cs.logger.Warn("Circuit breaker open, waiting cooldown before retry",
				slog.Int("failures", cs.breaker.ConsecutiveFailures()),
			)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
				continue
			}
		}

		cs.SetState(StateConnecting)
		cs.logger.Debug("Connecting to upstream camera feed")

		err := driver.Start(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}

			cs.metrics.IncUpstreamReconnects(cs.Config.Name)
			cs.reconnects.Add(1)
			delay := cs.breaker.ReportFailure(err)

			cs.SetState(StateBackoff)
			cs.logger.Warn("Upstream connection failure, backing off",
				slog.Any("error", err),
				slog.Duration("retry_delay", delay),
				slog.Int("failures", cs.breaker.ConsecutiveFailures()),
			)

			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
				continue
			}
		}

		// Clean stop
		return
	}
}

// OnClientConnected notifies the stream that a downstream client connected.
func (cs *CameraStream) OnClientConnected(parentCtx context.Context, protocol string) {
	cs.metrics.IncDownstreamActive(cs.Config.Name, protocol)

	cs.idleMu.Lock()
	if cs.idleTimer != nil {
		cs.logger.Debug("Cancelled on-demand idle shutdown timer due to new client connection")
		cs.idleTimer.Stop()
		cs.idleTimer = nil
	}
	cs.idleMu.Unlock()

	if cs.Config.Mode == config.ModeOnDemand {
		cs.ensureRunning(parentCtx)
	}
}

// OnClientDisconnected notifies the stream that a downstream client disconnected.
func (cs *CameraStream) OnClientDisconnected(protocol string) {
	cs.metrics.DecDownstreamActive(cs.Config.Name, protocol)

	if cs.Config.Mode != config.ModeOnDemand {
		return
	}

	cs.activeClientsMu.RLock()
	totalClients := len(cs.mjpegClients) + cs.rtspClientCount
	cs.activeClientsMu.RUnlock()

	if totalClients == 0 {
		cs.idleMu.Lock()
		defer cs.idleMu.Unlock()

		if cs.idleTimer != nil {
			cs.idleTimer.Stop()
		}

		timeout := cs.Config.IdleTimeout.Duration()
		cs.logger.Info("All clients disconnected, scheduling on-demand stream idle shutdown",
			slog.Duration("idle_timeout", timeout),
		)

		cs.idleTimer = time.AfterFunc(timeout, func() {
			cs.logger.Info("Idle timeout expired without new clients, stopping upstream stream")
			cs.Stop()
		})
	}
}

// Stop cleanly terminates the upstream connection.
func (cs *CameraStream) Stop() {
	cs.mu.Lock()
	if cs.cancelFunc != nil {
		cs.cancelFunc()
		cs.cancelFunc = nil
	}
	if cs.driver != nil {
		cs.driver.Stop()
	}
	cs.state = StateStopped
	cs.mu.Unlock()

	cs.SetState(StateIdle)
}

// SubscribeMJPEG registers a downstream MJPEG client and returns its bounded ring buffer.
func (cs *CameraStream) SubscribeMJPEG(clientID string, parentCtx context.Context) *ringbuffer.RingBuffer[[]byte] {
	buf := ringbuffer.NewRingBuffer[[]byte](cs.Config.ClientBufferSize, cs.Config.ClientTimeout.Duration())

	cs.activeClientsMu.Lock()
	cs.mjpegClients[clientID] = buf
	cs.activeClientsMu.Unlock()

	cs.OnClientConnected(parentCtx, "mjpeg")
	return buf
}

// UnsubscribeMJPEG unregisters an MJPEG client and cleans up resources.
func (cs *CameraStream) UnsubscribeMJPEG(clientID string) {
	cs.activeClientsMu.Lock()
	buf, exists := cs.mjpegClients[clientID]
	if exists {
		delete(cs.mjpegClients, clientID)
		buf.Close()
	}
	cs.activeClientsMu.Unlock()

	if exists {
		cs.metrics.RemoveClientMetrics(cs.Config.Name, clientID)
		cs.OnClientDisconnected("mjpeg")
	}
}

// BroadcastMJPEGFrame delivers a JPEG frame to all downstream MJPEG subscribers.
func (cs *CameraStream) BroadcastMJPEGFrame(frame []byte) {
	// Cache snapshot atomically
	cs.UpdateSnapshot(frame)

	cs.activeClientsMu.RLock()
	defer cs.activeClientsMu.RUnlock()

	for clientID, buf := range cs.mjpegClients {
		if buf.Push(frame) {
			cs.metrics.IncDownstreamDropped(cs.Config.Name, clientID)
		}
	}
}

// UpdateSnapshot updates the latest in-memory cached JPEG snapshot.
func (cs *CameraStream) UpdateSnapshot(frame []byte) {
	cs.snapshotMu.Lock()
	defer cs.snapshotMu.Unlock()

	cs.lastSnapshot = frame
	cs.snapshotTime = time.Now()
}

// GetLatestSnapshot returns the cached snapshot frame or error if none exists.
func (cs *CameraStream) GetLatestSnapshot() ([]byte, time.Time, error) {
	cs.snapshotMu.RLock()
	defer cs.snapshotMu.RUnlock()

	if len(cs.lastSnapshot) == 0 {
		return nil, time.Time{}, ErrNoSnapshotFrame
	}
	return cs.lastSnapshot, cs.snapshotTime, nil
}

// ResolvedSnapshotURL returns the configured SnapshotURL or auto-derives stream8 for RTSP cameras.
func (cs *CameraStream) ResolvedSnapshotURL() string {
	if cs.Config.SnapshotURL != "" {
		return cs.Config.SnapshotURL
	}
	if strings.Contains(cs.Config.UpstreamURL, "/stream1") {
		return strings.Replace(cs.Config.UpstreamURL, "/stream1", "/stream8", 1)
	}
	if strings.Contains(cs.Config.UpstreamURL, "/stream2") {
		return strings.Replace(cs.Config.UpstreamURL, "/stream2", "/stream8", 1)
	}
	return ""
}

// GetOrFetchSnapshot returns the latest cached snapshot if fresh (< 2s old),
// or fetches a fresh snapshot frame on-demand from SnapshotURL or UpstreamURL.
func (cs *CameraStream) GetOrFetchSnapshot(ctx context.Context) ([]byte, time.Time, error) {
	cs.snapshotMu.RLock()
	if len(cs.lastSnapshot) > 0 && time.Since(cs.snapshotTime) < 2*time.Second {
		frame := cs.lastSnapshot
		ts := cs.snapshotTime
		cs.snapshotMu.RUnlock()
		return frame, ts, nil
	}
	cs.snapshotMu.RUnlock()

	targetURL := cs.ResolvedSnapshotURL()
	if targetURL == "" {
		targetURL = cs.Config.UpstreamURL
	}

	frame, err := FetchSingleSnapshot(ctx, targetURL)
	if err != nil {
		cs.snapshotMu.RLock()
		if len(cs.lastSnapshot) > 0 {
			f := cs.lastSnapshot
			t := cs.snapshotTime
			cs.snapshotMu.RUnlock()
			return f, t, nil
		}
		cs.snapshotMu.RUnlock()
		return nil, time.Time{}, err
	}

	cs.UpdateSnapshot(frame)
	return frame, time.Now(), nil
}

// IncRTSPClient increments the RTSP active client count.
func (cs *CameraStream) IncRTSPClient(ctx context.Context) {
	cs.activeClientsMu.Lock()
	cs.rtspClientCount++
	cs.activeClientsMu.Unlock()

	cs.OnClientConnected(ctx, "rtsp")
}

// DecRTSPClient decrements the RTSP active client count.
func (cs *CameraStream) DecRTSPClient() {
	cs.activeClientsMu.Lock()
	if cs.rtspClientCount > 0 {
		cs.rtspClientCount--
	}
	cs.activeClientsMu.Unlock()

	cs.OnClientDisconnected("rtsp")
}

// TotalActiveClients returns total active downstream consumers.
func (cs *CameraStream) TotalActiveClients() int {
	cs.activeClientsMu.RLock()
	defer cs.activeClientsMu.RUnlock()
	return len(cs.mjpegClients) + cs.rtspClientCount
}

// AddBytesReceived increments upstream received bytes.
func (cs *CameraStream) AddBytesReceived(n int64) {
	cs.bytesReceived.Add(n)
	cs.metrics.AddUpstreamBytes(cs.Config.Name, n)
}

// BytesReceived returns total received bytes.
func (cs *CameraStream) BytesReceived() int64 {
	return cs.bytesReceived.Load()
}

// ReportSuccess notifies the circuit breaker that upstream streaming is healthy.
func (cs *CameraStream) ReportSuccess() {
	cs.breaker.ReportSuccess()
}
