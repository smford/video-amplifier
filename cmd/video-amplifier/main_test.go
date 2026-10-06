package main

import (
	"testing"
)

func TestVersionVariables(t *testing.T) {
	if Version == "" {
		t.Fatalf("expected non-empty Version")
	}
	if GitCommit == "" {
		t.Fatalf("expected non-empty GitCommit")
	}
}
