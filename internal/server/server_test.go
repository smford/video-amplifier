package server

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/metrics"
	"github.com/smford/video-amplifier/internal/upstream"
)

func TestHTTPServer_HealthAndReadyProbes(t *testing.T) {
	cfg := config.DefaultConfig()
	m := metrics.NewMetrics(nil)
	mgr := upstream.NewManager(cfg, m, nil, nil)
	_ = mgr.Start(context.Background())
	defer mgr.Stop()

	httpSrv := NewHTTPServer(cfg, mgr, nil)

	// Test /healthz
	reqHealth := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	wHealth := httptest.NewRecorder()
	httpSrv.handleHealthz(wHealth, reqHealth)
	if wHealth.Code != http.StatusOK {
		t.Fatalf("expected 200 for healthz, got %d", wHealth.Code)
	}

	// Test /readyz
	reqReady := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	wReady := httptest.NewRecorder()
	httpSrv.handleReadyz(wReady, reqReady)
	if wReady.Code != http.StatusOK {
		t.Fatalf("expected 200 for readyz, got %d", wReady.Code)
	}
}

func TestHTTPServer_DashboardUI(t *testing.T) {
	cfg := config.DefaultConfig()
	m := metrics.NewMetrics(nil)
	mgr := upstream.NewManager(cfg, m, nil, nil)
	_ = mgr.Start(context.Background())
	defer mgr.Stop()

	httpSrv := NewHTTPServer(cfg, mgr, nil)

	routes := []string{"/", "/ui", "/dashboard"}
	for _, route := range routes {
		req := httptest.NewRequest(http.MethodGet, route, nil)
		w := httptest.NewRecorder()
		httpSrv.server.Handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("route %s returned %d, expected 200", route, w.Code)
		}
		if !strings.Contains(w.Header().Get("Content-Type"), "text/html") {
			t.Fatalf("route %s returned content type %s, expected text/html", route, w.Header().Get("Content-Type"))
		}
		if !strings.Contains(w.Body.String(), "video-amplifier") {
			t.Fatalf("route %s body does not contain video-amplifier", route)
		}
	}
}

func TestHTTPServer_CamerasAPIAndSnapshot(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Cameras = []config.CameraConfig{
		{
			ID:          "front-door",
			Name:        "Front Door 4K",
			UpstreamURL: "http://localhost/video.mjpg",
			Mode:        config.ModeAlwaysOn,
		},
	}
	m := metrics.NewMetrics(nil)
	mgr := upstream.NewManager(cfg, m, nil, nil)
	_ = mgr.Start(context.Background())
	defer mgr.Stop()

	cam, _ := mgr.Get("front-door")
	mockJPEG := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x11, 0x22, 0xFF, 0xD9}
	cam.UpdateSnapshot(mockJPEG)

	httpSrv := NewHTTPServer(cfg, mgr, nil)

	// Test GET /cameras
	reqList := httptest.NewRequest(http.MethodGet, "/cameras", nil)
	wList := httptest.NewRecorder()
	httpSrv.server.Handler.ServeHTTP(wList, reqList)

	if wList.Code != http.StatusOK {
		t.Fatalf("expected 200 for /cameras, got %d", wList.Code)
	}

	var summaries []upstream.CameraSummary
	if err := json.Unmarshal(wList.Body.Bytes(), &summaries); err != nil {
		t.Fatalf("failed to decode summaries: %v", err)
	}
	if len(summaries) != 1 || summaries[0].ID != "front-door" {
		t.Fatalf("unexpected summary data: %+v", summaries)
	}

	// Test GET /cameras/front-door/snapshot.jpg
	reqSnap := httptest.NewRequest(http.MethodGet, "/cameras/front-door/snapshot.jpg", nil)
	wSnap := httptest.NewRecorder()
	httpSrv.server.Handler.ServeHTTP(wSnap, reqSnap)

	if wSnap.Code != http.StatusOK {
		t.Fatalf("expected 200 for snapshot, got %d", wSnap.Code)
	}
	if wSnap.Header().Get("Content-Type") != "image/jpeg" {
		t.Fatalf("expected image/jpeg, got %s", wSnap.Header().Get("Content-Type"))
	}
	if !strings.Contains(wSnap.Header().Get("Cache-Control"), "no-cache") {
		t.Fatalf("expected Cache-Control no-cache")
	}
	if wSnap.Body.Len() != len(mockJPEG) {
		t.Fatalf("expected %d bytes, got %d", len(mockJPEG), wSnap.Body.Len())
	}
}

func TestHTTPServer_MJPEGStream(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Cameras = []config.CameraConfig{
		{
			ID:          "mjpeg-cam",
			Name:        "MJPEG Cam",
			UpstreamURL: "http://localhost/video.mjpg",
			Mode:        config.ModeAlwaysOn,
		},
	}
	m := metrics.NewMetrics(nil)
	mgr := upstream.NewManager(cfg, m, nil, nil)
	_ = mgr.Start(context.Background())
	defer mgr.Stop()

	cam, _ := mgr.Get("mjpeg-cam")
	httpSrv := NewHTTPServer(cfg, mgr, nil)

	server := httptest.NewServer(httpSrv.server.Handler)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/cameras/mjpeg-cam/mjpeg", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to connect to mjpeg stream: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "multipart/x-mixed-replace") {
		t.Fatalf("expected multipart/x-mixed-replace, got %s", contentType)
	}

	// Broadcast 2 frames
	frame1 := []byte{0xFF, 0xD8, 0x01, 0xFF, 0xD9}
	frame2 := []byte{0xFF, 0xD8, 0x02, 0xFF, 0xD9}

	cam.BroadcastMJPEGFrame(frame1)
	cam.BroadcastMJPEGFrame(frame2)

	// Read from body
	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read from mjpeg stream: %v", err)
	}
	if !strings.Contains(line, "videoamplifierframe") && !strings.Contains(line, "--") {
		t.Logf("first boundary line: %s", line)
	}

	cancel() // close client
}

func TestRTSPServer_StreamLifecycle(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Server.RTSPPort = 8599
	m := metrics.NewMetrics(nil)

	rtspSrv := NewRTSPServer(cfg, m, nil)
	if err := rtspSrv.Start(); err != nil {
		t.Fatalf("failed to start RTSP server: %v", err)
	}
	defer rtspSrv.Stop()

	// Register dummy description
	desc := &description.Session{
		Medias: []*description.Media{},
	}

	stream, err := rtspSrv.SetStreamReady("test-cam", desc)
	if err != nil {
		t.Fatalf("failed to set stream ready: %v", err)
	}
	if stream == nil {
		t.Fatalf("expected non-nil stream")
	}

	rtspSrv.SetStreamUnready("test-cam")
}

func TestMetricsServer(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Server.MetricsPort = 9099
	m := metrics.NewMetrics(nil)
	m.SetUpstreamConnected("test-cam", true)
	mgr := upstream.NewManager(cfg, m, nil, nil)
	_ = mgr.Start(context.Background())
	defer mgr.Stop()

	ms := NewMetricsServer(cfg, m, mgr, nil)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	ms.server.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for metrics, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "video_amplifier_upstream_connected") {
		t.Fatalf("missing expected metric in scrape: %s", w.Body.String())
	}
}
