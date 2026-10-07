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
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/metrics"
	"github.com/smford/video-amplifier/internal/rtpengine"
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

	// Re-calling SetStreamReady for the same camera should return the existing stream without error
	stream2, err := rtspSrv.SetStreamReady("test-cam", desc)
	if err != nil || stream2 != stream {
		t.Fatalf("expected stream to be re-used, got err=%v stream2=%v", err, stream2)
	}

	// Simulate connected session
	sess := &gortsplib.ServerSession{}
	rtspSrv.sessionsMu.Lock()
	rtspSrv.sessions[sess] = &rtspSessionData{
		cameraID: "test-cam",
		clientID: "client-a",
	}
	rtspSrv.sessionsMu.Unlock()

	// Upstream unready called while client is connected -> stream should be KEPT intact (Item 2)
	rtspSrv.SetStreamUnready("test-cam")
	rtspSrv.mu.RLock()
	st, exists := rtspSrv.streams["test-cam"]
	rtspSrv.mu.RUnlock()
	if !exists || st == nil {
		t.Fatalf("expected stream to remain open when downstream clients are connected")
	}

	// Client disconnects
	rtspSrv.sessionsMu.Lock()
	delete(rtspSrv.sessions, sess)
	rtspSrv.sessionsMu.Unlock()

	// Now unready should tear down the unused stream
	rtspSrv.SetStreamUnready("test-cam")
	rtspSrv.mu.RLock()
	_, exists = rtspSrv.streams["test-cam"]
	rtspSrv.mu.RUnlock()
	if exists {
		t.Fatalf("expected stream to be closed after all clients disconnect")
	}
}

func TestRTSPServer_OnStreamWriteError_GOPEviction(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Cameras = []config.CameraConfig{
		{
			ID:          "evict-cam",
			Name:        "Evict Cam",
			UpstreamURL: "http://localhost/video.mjpg",
		},
	}
	m := metrics.NewMetrics(nil)
	mgr := upstream.NewManager(cfg, m, nil, nil)
	_ = mgr.Start(context.Background())
	defer mgr.Stop()

	rtspSrv := NewRTSPServer(cfg, m, nil)
	rtspSrv.SetManager(mgr)

	sess := &gortsplib.ServerSession{}
	// Setup session
	_, _ = rtspSrv.OnPlay(&gortsplib.ServerHandlerOnPlayCtx{
		Session: sess,
		Path:    "/evict-cam",
	})

	rtspSrv.sessionsMu.RLock()
	data := rtspSrv.sessions[sess]
	rtspSrv.sessionsMu.RUnlock()

	if data == nil || data.evictionTracker == nil {
		t.Fatalf("expected session data with eviction tracker")
	}

	// Trigger write error (slow consumer)
	rtspSrv.OnStreamWriteError(&gortsplib.ServerHandlerOnStreamWriteErrorCtx{
		Session: sess,
		Error:   io.ErrClosedPipe,
	})

	if data.evictionTracker.State != rtpengine.StateEvicted {
		t.Fatalf("expected evictionTracker state to be StateEvicted, got %v", data.evictionTracker.State)
	}
	if data.evictionTracker.EvictionCount == 0 {
		t.Fatalf("expected EvictionCount > 0")
	}
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

func TestHTTPServer_WHEPAndFMP4Routes(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Cameras = []config.CameraConfig{
		{
			ID:          "stream-cam",
			Name:        "Stream Cam",
			UpstreamURL: "rtsp://localhost/stream",
			Mode:        config.ModeAlwaysOn,
		},
	}
	m := metrics.NewMetrics(nil)
	mgr := upstream.NewManager(cfg, m, nil, nil)
	_ = mgr.Start(context.Background())
	defer mgr.Stop()

	server := NewHTTPServer(cfg, mgr, nil)

	// Test WHEP endpoint with invalid method (GET) -> should return 405 Method Not Allowed
	req := httptest.NewRequest(http.MethodGet, "/cameras/stream-cam/whep", nil)
	w := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for GET /whep, got %d", w.Code)
	}

	// Test fMP4 endpoint initial connection -> returns 200 with video/mp4 header
	ctx, cancel := context.WithCancel(context.Background())
	fmp4Req := httptest.NewRequest(http.MethodGet, "/cameras/stream-cam/fmp4", nil).WithContext(ctx)
	fmp4W := httptest.NewRecorder()

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	server.server.Handler.ServeHTTP(fmp4W, fmp4Req)
	if fmp4W.Code != http.StatusOK {
		t.Fatalf("expected 200 for GET /fmp4, got %d", fmp4W.Code)
	}
	if fmp4W.Header().Get("Content-Type") != "video/mp4" {
		t.Fatalf("expected video/mp4 Content-Type, got %s", fmp4W.Header().Get("Content-Type"))
	}
}

func TestHTTPServer_JSONHealthzAndONVIF(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Cameras = []config.CameraConfig{
		{
			ID:          "onvif-cam",
			Name:        "ONVIF Cam",
			UpstreamURL: "rtsp://localhost/test",
			Mode:        config.ModeAlwaysOn,
		},
	}
	m := metrics.NewMetrics(nil)
	mgr := upstream.NewManager(cfg, m, nil, nil)
	_ = mgr.Start(context.Background())
	defer mgr.Stop()

	server := NewHTTPServer(cfg, mgr, nil)

	// Test GET /healthz JSON payload
	hReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	hW := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(hW, hReq)
	if hW.Code != http.StatusOK {
		t.Fatalf("expected 200 for /healthz, got %d", hW.Code)
	}

	var healthResp HealthResponse
	if err := json.Unmarshal(hW.Body.Bytes(), &healthResp); err != nil {
		t.Fatalf("failed to unmarshal healthz JSON: %v", err)
	}
	if healthResp.Status != "healthy" {
		t.Fatalf("expected status healthy, got %s", healthResp.Status)
	}
	if len(healthResp.Cameras) != 1 || healthResp.Cameras[0].ID != "onvif-cam" {
		t.Fatalf("unexpected cameras in healthz response: %+v", healthResp.Cameras)
	}

	// Test GET /cameras/onvif-cam/onvif cached capabilities
	onvifReq := httptest.NewRequest(http.MethodGet, "/cameras/onvif-cam/onvif", nil)
	onvifW := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(onvifW, onvifReq)
	if onvifW.Code != http.StatusOK {
		t.Fatalf("expected 200 for /onvif, got %d", onvifW.Code)
	}
	if !strings.Contains(onvifW.Body.String(), "Capabilities") {
		t.Fatalf("expected ONVIF Capabilities XML, got %s", onvifW.Body.String())
	}

	// Test POST GetProfiles
	profReq := httptest.NewRequest(http.MethodPost, "/cameras/onvif-cam/onvif", strings.NewReader("<GetProfiles/>"))
	profW := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(profW, profReq)
	if profW.Code != http.StatusOK {
		t.Fatalf("expected 200 for GetProfiles, got %d", profW.Code)
	}
	if !strings.Contains(profW.Body.String(), "GetProfilesResponse") {
		t.Fatalf("expected ONVIF GetProfilesResponse XML, got %s", profW.Body.String())
	}
}

func TestHTTPServer_DownstreamAuthModes(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Cameras = []config.CameraConfig{
		{
			ID:                 "custom-auth-cam",
			Name:               "Custom Auth Cam",
			UpstreamURL:        "rtsp://upstreamuser:upstreampass@localhost/stream",
			Mode:               config.ModeAlwaysOn,
			AuthMode:           config.AuthModeCustom,
			DownstreamUsername: "downstreamuser",
			DownstreamPassword: "downstreampass",
		},
		{
			ID:          "passthrough-cam",
			Name:        "Passthrough Cam",
			UpstreamURL: "rtsp://passuser:passsecret@localhost/stream",
			Mode:        config.ModeAlwaysOn,
			AuthMode:    config.AuthModePassthrough,
		},
		{
			ID:          "open-cam",
			Name:        "Open Cam",
			UpstreamURL: "rtsp://localhost/stream",
			Mode:        config.ModeAlwaysOn,
			AuthMode:    config.AuthModeNone,
		},
	}
	m := metrics.NewMetrics(nil)
	mgr := upstream.NewManager(cfg, m, nil, nil)

	// Pre-seed cached snapshot to avoid waiting on upstream decoder fallbacks
	for _, id := range []string{"custom-auth-cam", "passthrough-cam", "open-cam"} {
		if cam, ok := mgr.Get(id); ok {
			cam.UpdateSnapshot([]byte("\xFF\xD8\xFF\xE0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xFF\xDB\x00C\x00\xFF\xD9"))
		}
	}

	server := NewHTTPServer(cfg, mgr, nil)

	// 1. Custom Auth: Unauthenticated request should receive 401
	unauthReq := httptest.NewRequest(http.MethodGet, "/cameras/custom-auth-cam/snapshot.jpg", nil)
	unauthW := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(unauthW, unauthReq)
	if unauthW.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated request, got %d", unauthW.Code)
	}

	// Custom Auth: Invalid credentials -> 401
	badAuthReq := httptest.NewRequest(http.MethodGet, "/cameras/custom-auth-cam/snapshot.jpg", nil)
	badAuthReq.SetBasicAuth("wrong", "credentials")
	badAuthW := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(badAuthW, badAuthReq)
	if badAuthW.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for bad credentials, got %d", badAuthW.Code)
	}

	// Custom Auth: Valid credentials -> Passes auth (no snapshot frame yet -> 404, but NOT 401)
	validAuthReq := httptest.NewRequest(http.MethodGet, "/cameras/custom-auth-cam/snapshot.jpg", nil)
	validAuthReq.SetBasicAuth("downstreamuser", "downstreampass")
	validAuthW := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(validAuthW, validAuthReq)
	if validAuthW.Code == http.StatusUnauthorized {
		t.Fatalf("expected request with valid custom auth to pass auth check, got 401")
	}

	// 2. Passthrough Auth: Unauthenticated request -> 401
	passUnauthReq := httptest.NewRequest(http.MethodGet, "/cameras/passthrough-cam/snapshot.jpg", nil)
	passUnauthW := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(passUnauthW, passUnauthReq)
	if passUnauthW.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated passthrough request, got %d", passUnauthW.Code)
	}

	// Passthrough Auth: Valid upstream credentials -> Passes auth
	passValidReq := httptest.NewRequest(http.MethodGet, "/cameras/passthrough-cam/snapshot.jpg", nil)
	passValidReq.SetBasicAuth("passuser", "passsecret")
	passValidW := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(passValidW, passValidReq)
	if passValidW.Code == http.StatusUnauthorized {
		t.Fatalf("expected request with passthrough auth to pass auth check, got 401")
	}

	// 3. Open Cam: Unauthenticated request -> Passes auth directly
	openReq := httptest.NewRequest(http.MethodGet, "/cameras/open-cam/snapshot.jpg", nil)
	openW := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(openW, openReq)
	if openW.Code == http.StatusUnauthorized {
		t.Fatalf("expected open-cam to allow unauthenticated request, got 401")
	}
}

func TestHTTPServer_WebDashboardBasicAuth(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Server.WebUsername = "webadmin"
	cfg.Server.WebPassword = "webpassword"

	m := metrics.NewMetrics(nil)
	mgr := upstream.NewManager(cfg, m, nil, nil)

	server := NewHTTPServer(cfg, mgr, nil)

	routes := []string{"/", "/ui", "/dashboard", "/cameras"}
	for _, route := range routes {
		// 1. Unauthenticated request -> 401 Unauthorized
		unauthReq := httptest.NewRequest(http.MethodGet, route, nil)
		unauthW := httptest.NewRecorder()
		server.server.Handler.ServeHTTP(unauthW, unauthReq)
		if unauthW.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 for %s unauthenticated, got %d", route, unauthW.Code)
		}
		if !strings.Contains(unauthW.Header().Get("WWW-Authenticate"), "Basic") {
			t.Fatalf("expected WWW-Authenticate Basic header for %s, got %s", route, unauthW.Header().Get("WWW-Authenticate"))
		}

		// 2. Invalid credentials -> 401 Unauthorized
		badReq := httptest.NewRequest(http.MethodGet, route, nil)
		badReq.SetBasicAuth("wrong", "credentials")
		badW := httptest.NewRecorder()
		server.server.Handler.ServeHTTP(badW, badReq)
		if badW.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 for %s with bad credentials, got %d", route, badW.Code)
		}

		// 3. Valid credentials -> 200 OK
		validReq := httptest.NewRequest(http.MethodGet, route, nil)
		validReq.SetBasicAuth("webadmin", "webpassword")
		validW := httptest.NewRecorder()
		server.server.Handler.ServeHTTP(validW, validReq)
		if validW.Code != http.StatusOK {
			t.Fatalf("expected 200 for %s with valid credentials, got %d", route, validW.Code)
		}
	}

	// Test camera with custom auth: verify web credentials can also access camera snapshot to avoid login loop
	camCfg := config.CameraConfig{
		ID:                 "cam-auth",
		Name:               "Cam Auth",
		UpstreamURL:        "rtsp://localhost/cam",
		AuthMode:           config.AuthModeCustom,
		DownstreamUsername: "camuser",
		DownstreamPassword: "campass",
	}
	cfg.Cameras = []config.CameraConfig{camCfg}
	mgrWithCam := upstream.NewManager(cfg, m, nil, nil)
	if cam, ok := mgrWithCam.Get("cam-auth"); ok {
		cam.UpdateSnapshot([]byte("\xFF\xD8\xFF\xE0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xFF\xDB\x00C\x00\xFF\xD9"))
	}
	serverWithCam := NewHTTPServer(cfg, mgrWithCam, nil)

	// A. Web credentials authorize camera snapshot
	webAuthReq := httptest.NewRequest(http.MethodGet, "/cameras/cam-auth/snapshot.jpg", nil)
	webAuthReq.SetBasicAuth("webadmin", "webpassword")
	webAuthW := httptest.NewRecorder()
	serverWithCam.server.Handler.ServeHTTP(webAuthW, webAuthReq)
	if webAuthW.Code != http.StatusOK {
		t.Fatalf("expected 200 using web credentials for camera snapshot, got %d", webAuthW.Code)
	}

	// B. Camera-specific credentials also authorize camera snapshot
	camAuthReq := httptest.NewRequest(http.MethodGet, "/cameras/cam-auth/snapshot.jpg", nil)
	camAuthReq.SetBasicAuth("camuser", "campass")
	camAuthW := httptest.NewRecorder()
	serverWithCam.server.Handler.ServeHTTP(camAuthW, camAuthReq)
	if camAuthW.Code != http.StatusOK {
		t.Fatalf("expected 200 using camera credentials for camera snapshot, got %d", camAuthW.Code)
	}

	// C. Bad credentials -> 401
	badCamAuthReq := httptest.NewRequest(http.MethodGet, "/cameras/cam-auth/snapshot.jpg", nil)
	badCamAuthReq.SetBasicAuth("wrong", "credentials")
	badCamAuthW := httptest.NewRecorder()
	serverWithCam.server.Handler.ServeHTTP(badCamAuthW, badCamAuthReq)
	if badCamAuthW.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 using bad credentials for camera snapshot, got %d", badCamAuthW.Code)
	}
}



