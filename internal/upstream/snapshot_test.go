package upstream

import (
	"bytes"
	"testing"

	"github.com/smford/video-amplifier/internal/config"
)

func TestCleanJPEG(t *testing.T) {
	// Case 1: Normal valid JPEG
	normalJPEG := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 'J', 'F', 'I', 'F', 0x00, 0xFF, 0xD9}
	cleaned := CleanJPEG(normalJPEG)
	if !bytes.Equal(cleaned, normalJPEG) {
		t.Fatalf("expected normal JPEG to be untouched, got %v", cleaned)
	}

	// Case 2: Concatenated headers (Tapo camera RTP MJPEG artifact)
	dummyHeader := []byte{0xFF, 0xD8, 0xFF, 0xDB, 0x00, 0x84, 0x01, 0x02, 0xFF, 0xDA, 0x00, 0x0C}
	realJPEG := []byte{0xFF, 0xD8, 0xFF, 0xC0, 0x00, 0x11, 0x08, 0x01, 0x68, 0xFF, 0xD9}
	concatenated := append(dummyHeader, realJPEG...)

	cleaned = CleanJPEG(concatenated)
	if !bytes.Equal(cleaned, realJPEG) {
		t.Fatalf("expected stripped duplicate prefix, got %v", cleaned)
	}

	// Case 3: Empty / short data
	short := []byte{0xFF, 0xD8}
	if !bytes.Equal(CleanJPEG(short), short) {
		t.Fatalf("expected short data untouched")
	}

	// Case 4: Non-JPEG data
	random := []byte{0x01, 0x02, 0x03, 0x04}
	if !bytes.Equal(CleanJPEG(random), random) {
		t.Fatalf("expected random data untouched")
	}
}

func TestResolvedSnapshotURL(t *testing.T) {
	tests := []struct {
		name        string
		upstreamURL string
		snapshotURL string
		expected    string
	}{
		{
			name:        "explicit snapshot URL overrides",
			upstreamURL: "rtsp://cam/stream1",
			snapshotURL: "http://cam/snap.jpg",
			expected:    "http://cam/snap.jpg",
		},
		{
			name:        "derive stream8 from stream1",
			upstreamURL: "rtsp://cam/stream1",
			snapshotURL: "",
			expected:    "rtsp://cam/stream8",
		},
		{
			name:        "derive stream8 from stream2",
			upstreamURL: "rtsp://cam/stream2",
			snapshotURL: "",
			expected:    "rtsp://cam/stream8",
		},
		{
			name:        "derive snapshot from ONVIF device_service",
			upstreamURL: "http://127.0.0.1:8000/onvif/cam0/device_service",
			snapshotURL: "",
			expected:    "http://127.0.0.1:8000/onvif/cam0/snapshot",
		},
		{
			name:        "derive snapshot from standard ONVIF device_service",
			upstreamURL: "http://192.168.1.50/onvif/device_service",
			snapshotURL: "",
			expected:    "http://192.168.1.50/onvif/snapshot",
		},
		{
			name:        "unmatched URL returns empty",
			upstreamURL: "rtsp://cam/live/main",
			snapshotURL: "",
			expected:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs := &CameraStream{
				Config: config.CameraConfig{
					UpstreamURL: tc.upstreamURL,
					SnapshotURL: tc.snapshotURL,
				},
			}
			actual := cs.ResolvedSnapshotURL()
			if actual != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, actual)
			}
		})
	}
}

