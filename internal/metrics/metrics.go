package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics manages all Prometheus telemetry for video-amplifier.
type Metrics struct {
	registry *prometheus.Registry

	upstreamConnected     *prometheus.GaugeVec
	upstreamReconnects    *prometheus.CounterVec
	upstreamBytesReceived *prometheus.CounterVec
	downstreamActive      *prometheus.GaugeVec
	downstreamDropped     *prometheus.CounterVec

	mu sync.RWMutex
}

// NewMetrics initializes Prometheus metrics according to the specification.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	// Register standard Go runtime and process collectors
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		registry: reg,
		upstreamConnected: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "video_amplifier_upstream_connected",
				Help: "Upstream camera connection state (1 for connected, 0 for disconnected).",
			},
			[]string{"camera"},
		),
		upstreamReconnects: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "video_amplifier_upstream_reconnects_total",
				Help: "Total count of upstream camera reconnections.",
			},
			[]string{"camera"},
		),
		upstreamBytesReceived: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "video_amplifier_upstream_bytes_received_total",
				Help: "Total bytes received from upstream camera feed.",
			},
			[]string{"camera"},
		),
		downstreamActive: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "video_amplifier_downstream_active_clients",
				Help: "Number of active downstream consumers connected to camera.",
			},
			[]string{"camera", "protocol"},
		),
		downstreamDropped: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "video_amplifier_downstream_dropped_frames_total",
				Help: "Total frames dropped for a downstream client due to backpressure.",
			},
			[]string{"camera", "client_id"},
		),
	}

	reg.MustRegister(
		m.upstreamConnected,
		m.upstreamReconnects,
		m.upstreamBytesReceived,
		m.downstreamActive,
		m.downstreamDropped,
	)

	return m
}

// Registry returns the Prometheus registry.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

// HTTPHandler returns an http.Handler that exposes Prometheus metrics.
func (m *Metrics) HTTPHandler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// SetUpstreamConnected sets the connection status for a camera.
func (m *Metrics) SetUpstreamConnected(camera string, connected bool) {
	val := 0.0
	if connected {
		val = 1.0
	}
	m.upstreamConnected.WithLabelValues(camera).Set(val)
}

// IncUpstreamReconnects increments the reconnect counter for a camera.
func (m *Metrics) IncUpstreamReconnects(camera string) {
	m.upstreamReconnects.WithLabelValues(camera).Inc()
}

// AddUpstreamBytes adds received bytes to the upstream counter for a camera.
func (m *Metrics) AddUpstreamBytes(camera string, n int64) {
	if n > 0 {
		m.upstreamBytesReceived.WithLabelValues(camera).Add(float64(n))
	}
}

// IncDownstreamActive increments active downstream client count.
func (m *Metrics) IncDownstreamActive(camera string, protocol string) {
	m.downstreamActive.WithLabelValues(camera, protocol).Inc()
}

// DecDownstreamActive decrements active downstream client count.
func (m *Metrics) DecDownstreamActive(camera string, protocol string) {
	m.downstreamActive.WithLabelValues(camera, protocol).Dec()
}

// IncDownstreamDropped increments dropped frame counter for a client.
func (m *Metrics) IncDownstreamDropped(camera string, clientID string) {
	m.downstreamDropped.WithLabelValues(camera, clientID).Inc()
}

// RemoveClientMetrics removes the client_id series to prevent memory leaks when client disconnects.
func (m *Metrics) RemoveClientMetrics(camera string, clientID string) {
	m.downstreamDropped.DeleteLabelValues(camera, clientID)
}
