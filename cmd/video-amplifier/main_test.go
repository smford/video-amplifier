package main

import (
	"path/filepath"
	"testing"

	"github.com/smford/video-amplifier/internal/config"
)

func TestVersionVariables(t *testing.T) {
	if Version == "" {
		t.Fatalf("expected non-empty Version")
	}
	if GitCommit == "" {
		t.Fatalf("expected non-empty GitCommit")
	}
}

func TestSampleConfigGeneration(t *testing.T) {
	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, "config.yaml")

	if err := config.WriteSampleConfig(outPath, false); err != nil {
		t.Fatalf("WriteSampleConfig failed: %v", err)
	}

	cfg, err := config.LoadConfig(outPath)
	if err != nil {
		t.Fatalf("LoadConfig failed on sample output: %v", err)
	}

	if len(cfg.Cameras) < 2 {
		t.Fatalf("expected at least 2 sample cameras, got %d", len(cfg.Cameras))
	}
}
