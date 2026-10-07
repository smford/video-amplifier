package server

import (
	_ "embed"
	"fmt"
	"net/http"
)

// IndexHTML embeds the responsive web interface for video-amplifier.
//
//go:embed index.html
var IndexHTML string

// handleDashboard serves the web interface.
func (s *HTTPServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, IndexHTML)
}
