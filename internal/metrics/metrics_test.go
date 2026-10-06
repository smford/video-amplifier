package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsRegistrationAndScrape(t *testing.T) {
	m := NewMetrics(nil)

	// Set sample metrics
	m.SetUpstreamConnected("Front Door 4K", true)
	m.IncUpstreamReconnects("Front Door 4K")
	m.AddUpstreamBytes("Front Door 4K", 1024)
	m.IncDownstreamActive("Front Door 4K", "rtsp")
	m.IncDownstreamDropped("Front Door 4K", "client-123")

	server := httptest.NewServer(m.HTTPHandler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("failed to query /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	output := string(body)

	expectedMetrics := []string{
		"video_amplifier_upstream_connected{camera=\"Front Door 4K\"} 1",
		"video_amplifier_upstream_reconnects_total{camera=\"Front Door 4K\"} 1",
		"video_amplifier_upstream_bytes_received_total{camera=\"Front Door 4K\"} 1024",
		"video_amplifier_downstream_active_clients{camera=\"Front Door 4K\",protocol=\"rtsp\"} 1",
		"video_amplifier_downstream_dropped_frames_total{camera=\"Front Door 4K\",client_id=\"client-123\"} 1",
		"go_goroutines",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(output, metric) {
			t.Errorf("scraped metrics missing expected string %q", metric)
		}
	}

	// Test client cleanup
	m.RemoveClientMetrics("Front Door 4K", "client-123")
	m.DecDownstreamActive("Front Door 4K", "rtsp")
}
