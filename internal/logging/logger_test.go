package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestRedactCredentials(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "RTSP URL with user and password",
			input:    "rtsp://admin:secret123@192.168.1.50:554/h264Preview_01_main",
			expected: "rtsp://admin:*****@192.168.1.50:554/h264Preview_01_main",
		},
		{
			name:     "HTTP URL with user and password",
			input:    "http://camera-user:super$ecret@10.0.0.5/video.mjpg",
			expected: "http://camera-user:*****@10.0.0.5/video.mjpg",
		},
		{
			name:     "URL without credentials",
			input:    "rtsp://192.168.1.50:554/live",
			expected: "rtsp://192.168.1.50:554/live",
		},
		{
			name:     "Arbitrary string without URLs",
			input:    "Normal log message without password",
			expected: "Normal log message without password",
		},
		{
			name:     "Message containing embedded URL",
			input:    "Connecting to upstream rtsp://operator:p@ssword!@cam:554/ch0 failed",
			expected: "Connecting to upstream rtsp://operator:*****@cam:554/ch0 failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := RedactCredentials(tc.input)
			if actual != tc.expected {
				t.Fatalf("expected %q, got %q", tc.expected, actual)
			}
		})
	}
}

func TestRedactingHandler(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "DEBUG", "json")

	rawURL := "rtsp://alice:topsecret@10.10.10.1:554/live"
	logger.Info("Ingest started",
		slog.String("camera", "front-door"),
		slog.String("url", rawURL),
	)

	out := buf.String()
	if strings.Contains(out, "topsecret") {
		t.Fatalf("Log output contains unredacted secret password: %s", out)
	}
	if !strings.Contains(out, "rtsp://alice:*****@10.10.10.1:554/live") {
		t.Fatalf("Log output missing redacted URL: %s", out)
	}
}

func TestRedactingHandler_MessageRedaction(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "INFO", "text")

	logger.Warn("Failed connecting to rtsp://admin:secret999@camera.local/feed")

	out := buf.String()
	if strings.Contains(out, "secret999") {
		t.Fatalf("Log output contains raw secret in message: %s", out)
	}
	if !strings.Contains(out, "rtsp://admin:*****@camera.local/feed") {
		t.Fatalf("Log output missing expected redacted message: %s", out)
	}
}

func TestRedactingHandler_GroupAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "INFO", "json")

	logger.WithGroup("upstream").Info("camera configured",
		slog.String("target", "http://user:pass123@host/stream"),
	)

	out := buf.String()
	if strings.Contains(out, "pass123") {
		t.Fatalf("Log output contains raw secret in group attr: %s", out)
	}
	if !strings.Contains(out, "http://user:*****@host/stream") {
		t.Fatalf("Log output missing redacted group attr: %s", out)
	}
}
