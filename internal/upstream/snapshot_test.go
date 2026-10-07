package upstream

import (
	"bytes"
	"testing"
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
