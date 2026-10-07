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

// authorizeWebRequest checks if web dashboard authentication is configured and valid.
func (s *HTTPServer) authorizeWebRequest(w http.ResponseWriter, r *http.Request) bool {
	if s.cfg.Server.WebUsername == "" && s.cfg.Server.WebPassword == "" {
		return true // No dashboard auth required
	}

	user, pass, ok := r.BasicAuth()
	if !ok || user != s.cfg.Server.WebUsername || pass != s.cfg.Server.WebPassword {
		w.Header().Set("WWW-Authenticate", `Basic realm="video-amplifier"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return false
	}

	return true
}

// handleDashboard serves the web interface.
func (s *HTTPServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeWebRequest(w, r) {
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, IndexHTML)
}

