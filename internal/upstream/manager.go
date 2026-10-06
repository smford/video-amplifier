package upstream

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/metrics"
)

// Manager coordinates all configured camera streams.
type Manager struct {
	mu        sync.RWMutex
	cameras   map[string]*CameraStream
	cameraIDs []string
	metrics   *metrics.Metrics
	logger    *slog.Logger
	publisher RTSPStreamPublisher
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewManager initializes the camera stream manager.
func NewManager(cfg *config.Config, m *metrics.Metrics, logger *slog.Logger, publisher RTSPStreamPublisher) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	mgr := &Manager{
		cameras:   make(map[string]*CameraStream),
		cameraIDs: make([]string, 0, len(cfg.Cameras)),
		metrics:   m,
		logger:    logger,
		publisher: publisher,
	}

	for _, camCfg := range cfg.Cameras {
		cs := NewCameraStream(camCfg, m, logger)

		// Determine driver based on URL scheme
		if strings.HasPrefix(camCfg.UpstreamURL, "rtsp://") || strings.HasPrefix(camCfg.UpstreamURL, "rtsps://") {
			driver := NewRTSPDriver(cs, publisher)
			cs.SetDriver(driver)
		} else {
			driver := NewMJPEGDriver(cs)
			cs.SetDriver(driver)
		}

		mgr.cameras[camCfg.ID] = cs
		mgr.cameraIDs = append(mgr.cameraIDs, camCfg.ID)
	}

	return mgr
}

// Start launches all configured camera streams.
func (m *Manager) Start(parentCtx context.Context) error {
	m.mu.Lock()
	m.ctx, m.cancel = context.WithCancel(parentCtx)
	m.mu.Unlock()

	m.logger.Info("Starting upstream camera manager", slog.Int("cameras", len(m.cameras)))

	for _, id := range m.cameraIDs {
		cam := m.cameras[id]
		cam.Start(m.ctx)
	}

	return nil
}

// Stop terminates all camera streams cleanly.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}

	for _, cam := range m.cameras {
		cam.Stop()
	}

	m.logger.Info("All upstream camera streams stopped")
}

// Get returns the CameraStream for a given camera ID.
func (m *Manager) Get(id string) (*CameraStream, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cam, ok := m.cameras[id]
	return cam, ok
}

// List returns all configured CameraStreams.
func (m *Manager) List() []*CameraStream {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list := make([]*CameraStream, 0, len(m.cameraIDs))
	for _, id := range m.cameraIDs {
		list = append(list, m.cameras[id])
	}
	return list
}

// IsReady returns true if the manager is initialized and healthy.
func (m *Manager) IsReady() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ctx != nil && m.ctx.Err() == nil
}

// CameraSummary represents a snapshot of camera status for API reporting.
type CameraSummary struct {
	ID            string      `json:"id"`
	Name          string      `json:"name"`
	UpstreamURL   string      `json:"upstream_url"`
	Mode          config.Mode `json:"mode"`
	State         StreamState `json:"state"`
	ActiveClients int         `json:"active_clients"`
	BytesReceived int64       `json:"bytes_received"`
}

// Summaries returns current operational status for all cameras.
func (m *Manager) Summaries() []CameraSummary {
	streams := m.List()
	summaries := make([]CameraSummary, len(streams))

	for i, s := range streams {
		summaries[i] = CameraSummary{
			ID:            s.Config.ID,
			Name:          s.Config.Name,
			UpstreamURL:   fmt.Sprintf("%s", s.Config.UpstreamURL),
			Mode:          s.Config.Mode,
			State:         s.State(),
			ActiveClients: s.TotalActiveClients(),
			BytesReceived: s.BytesReceived(),
		}
	}
	return summaries
}
