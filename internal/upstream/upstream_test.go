package upstream

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/metrics"
)

type mockDriver struct {
	started   atomic.Bool
	stopped   atomic.Bool
	startErr  error
	startChan chan struct{}
}

func newMockDriver() *mockDriver {
	return &mockDriver{
		startChan: make(chan struct{}, 10),
	}
}

func (m *mockDriver) Start(ctx context.Context) error {
	m.started.Store(true)
	m.startChan <- struct{}{}
	if m.startErr != nil {
		return m.startErr
	}
	<-ctx.Done()
	return ctx.Err()
}

func (m *mockDriver) Stop() {
	m.stopped.Store(true)
}

func TestCameraStream_AlwaysOnLifecycle(t *testing.T) {
	m := metrics.NewMetrics(nil)
	cfg := config.CameraConfig{
		ID:          "cam-1",
		Name:        "Test Camera 1",
		UpstreamURL: "rtsp://localhost/test",
		Mode:        config.ModeAlwaysOn,
	}

	cs := NewCameraStream(cfg, m, nil)
	driver := newMockDriver()
	cs.SetDriver(driver)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs.Start(ctx)

	select {
	case <-driver.startChan:
		// Driver successfully started
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("driver did not start for always-on camera")
	}

	cs.Stop()
	if !driver.stopped.Load() {
		t.Fatalf("expected driver to be stopped")
	}
}

func TestCameraStream_OnDemandLifecycleAndIdleTimer(t *testing.T) {
	m := metrics.NewMetrics(nil)
	cfg := config.CameraConfig{
		ID:               "cam-demand",
		Name:             "Demand Camera",
		UpstreamURL:      "http://localhost/mjpg",
		Mode:             config.ModeOnDemand,
		IdleTimeout:      config.Duration(50 * time.Millisecond),
		ClientBufferSize: 10,
	}

	cs := NewCameraStream(cfg, m, nil)
	driver := newMockDriver()
	cs.SetDriver(driver)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs.Start(ctx)

	// In on-demand mode, driver should NOT start immediately
	if driver.started.Load() {
		t.Fatalf("on-demand driver should not start before clients connect")
	}

	// Downstream client connects
	clientID := "client-sub-1"
	sub := cs.SubscribeMJPEG(clientID, ctx)
	defer cs.UnsubscribeMJPEG(clientID)

	select {
	case <-driver.startChan:
		// Driver started on client arrival!
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("driver failed to start after on-demand client connected")
	}

	if cs.TotalActiveClients() != 1 {
		t.Fatalf("expected 1 active client, got %d", cs.TotalActiveClients())
	}

	// Client receives frame
	frame := []byte{0xFF, 0xD8, 0x01, 0x02, 0xFF, 0xD9}
	cs.BroadcastMJPEGFrame(frame)

	popCtx, popCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer popCancel()

	gotFrame, err := sub.Pop(popCtx)
	if err != nil {
		t.Fatalf("failed to pop frame: %v", err)
	}
	if !bytes.Equal(gotFrame, frame) {
		t.Fatalf("frame mismatch")
	}

	// Disconnect client -> triggers idle timer
	cs.UnsubscribeMJPEG(clientID)

	// Stream should still be running during grace period
	if cs.TotalActiveClients() != 0 {
		t.Fatalf("expected 0 active clients after unsubscribe")
	}

	// Wait for idle timer (50ms) to elapse
	time.Sleep(80 * time.Millisecond)

	// After idle timeout, driver should be stopped
	if !driver.stopped.Load() {
		t.Fatalf("expected driver to be stopped after idle timeout")
	}
}

func TestCameraStream_OnDemandGracePeriodCancelledByNewClient(t *testing.T) {
	m := metrics.NewMetrics(nil)
	cfg := config.CameraConfig{
		ID:          "cam-demand-cancel",
		Name:        "Demand Cancel",
		UpstreamURL: "http://localhost/mjpg",
		Mode:        config.ModeOnDemand,
		IdleTimeout: config.Duration(80 * time.Millisecond),
	}

	cs := NewCameraStream(cfg, m, nil)
	driver := newMockDriver()
	cs.SetDriver(driver)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs.Start(ctx)

	// Client 1 connects then disconnects
	_ = cs.SubscribeMJPEG("client-1", ctx)
	<-driver.startChan

	cs.UnsubscribeMJPEG("client-1")

	// Sleep less than idle timeout (30ms < 80ms)
	time.Sleep(30 * time.Millisecond)
	if driver.stopped.Load() {
		t.Fatalf("driver stopped prematurely before idle timeout")
	}

	// Client 2 connects before timeout expires
	_ = cs.SubscribeMJPEG("client-2", ctx)

	// Sleep beyond the original 80ms
	time.Sleep(70 * time.Millisecond)

	// Driver should STILL be running because client 2 arrived!
	if driver.stopped.Load() {
		t.Fatalf("driver was stopped even though new client arrived within grace period")
	}

	cs.UnsubscribeMJPEG("client-2")
}

func TestCameraStream_SnapshotCaching(t *testing.T) {
	m := metrics.NewMetrics(nil)
	cfg := config.CameraConfig{
		ID:          "cam-snap",
		Name:        "Snapshot Cam",
		UpstreamURL: "http://localhost/test",
		Mode:        config.ModeAlwaysOn,
	}

	cs := NewCameraStream(cfg, m, nil)

	_, _, err := cs.GetLatestSnapshot()
	if err != ErrNoSnapshotFrame {
		t.Fatalf("expected ErrNoSnapshotFrame, got %v", err)
	}

	mockJPEG := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0xFF, 0xD9}
	cs.UpdateSnapshot(mockJPEG)

	cached, ts, err := cs.GetLatestSnapshot()
	if err != nil {
		t.Fatalf("unexpected snapshot err: %v", err)
	}
	if !bytes.Equal(cached, mockJPEG) {
		t.Fatalf("cached frame mismatch")
	}
	if time.Since(ts) > 1*time.Second {
		t.Fatalf("timestamp too old: %v", ts)
	}
}

func TestCameraStream_BackpressureDropping(t *testing.T) {
	m := metrics.NewMetrics(nil)
	cfg := config.CameraConfig{
		ID:               "cam-bp",
		Name:             "Backpressure Cam",
		UpstreamURL:      "http://localhost/test",
		Mode:             config.ModeAlwaysOn,
		ClientBufferSize: 2, // tiny buffer
	}

	cs := NewCameraStream(cfg, m, nil)
	ctx := context.Background()

	// Client 1 never reads (slow consumer)
	_ = cs.SubscribeMJPEG("slow-client", ctx)
	defer cs.UnsubscribeMJPEG("slow-client")

	// Client 2 reads immediately (fast consumer)
	fastSub := cs.SubscribeMJPEG("fast-client", ctx)
	defer cs.UnsubscribeMJPEG("fast-client")

	// Push 5 frames
	for i := 1; i <= 5; i++ {
		cs.BroadcastMJPEGFrame([]byte{0xFF, 0xD8, byte(i), 0xFF, 0xD9})
	}

	// Verify fast consumer can pop without delay
	readFast := 0
	for {
		item, ok := fastSub.TryPop()
		if !ok {
			break
		}
		if len(item) > 0 {
			readFast++
		}
	}

	if readFast == 0 {
		t.Fatalf("fast client should have received frames")
	}

	// Slow client should have experienced dropped frames
	slowBuf := cs.mjpegClients["slow-client"]
	if slowBuf.DroppedCount() == 0 {
		t.Fatalf("slow client should have dropped frames")
	}
}

func TestIsH264Keyframe(t *testing.T) {
	// IDR NAL (type 5)
	idrPayload := []byte{0x05, 0x88, 0x84, 0x00}
	if !IsH264Keyframe(idrPayload) {
		t.Errorf("expected IDR payload to be detected as keyframe")
	}

	// Non-IDR NAL (type 1)
	nonIDR := []byte{0x01, 0x88, 0x84, 0x00}
	if IsH264Keyframe(nonIDR) {
		t.Errorf("expected non-IDR payload NOT to be keyframe")
	}

	// FU-A starting with IDR (type 28, indicator=28, header=start+5 = 0x85)
	fuaIDR := []byte{0x1C, 0x85, 0x00}
	if !IsH264Keyframe(fuaIDR) {
		t.Errorf("expected FU-A start IDR to be keyframe")
	}

	// Empty payload
	if IsH264Keyframe(nil) {
		t.Errorf("expected empty payload NOT to be keyframe")
	}
}

func TestManager_SummariesAndLookup(t *testing.T) {
	cfg := &config.Config{
		Cameras: []config.CameraConfig{
			{ID: "c1", Name: "Camera 1", UpstreamURL: "rtsp://10.0.0.1/live"},
			{ID: "c2", Name: "Camera 2", UpstreamURL: "http://10.0.0.2/mjpeg"},
		},
	}
	m := metrics.NewMetrics(nil)
	mgr := NewManager(cfg, m, nil, nil)

	if len(mgr.List()) != 2 {
		t.Fatalf("expected 2 cameras in manager")
	}

	c1, ok := mgr.Get("c1")
	if !ok || c1.Config.Name != "Camera 1" {
		t.Fatalf("failed to retrieve c1")
	}

	summaries := mgr.Summaries()
	if len(summaries) != 2 {
		t.Fatalf("expected 2 summaries, got %d", len(summaries))
	}
	if summaries[0].ID != "c1" || summaries[1].ID != "c2" {
		t.Fatalf("unexpected summary order or data")
	}
}
