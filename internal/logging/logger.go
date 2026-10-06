package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
)

// credentialRegex matches URLs with passwords in authority (e.g. scheme://user:password@host:port/path).
// It greedily matches user:password up to the last @ before host.
var credentialRegex = regexp.MustCompile(`((?:rtsp|rtsps|http|https)://[^:\s/@]+:)([^/\s]+)(@[^/\s]+)`)

// RedactCredentials replaces passwords in URLs with asterisks.
// Example: "rtsp://admin:secret123@192.168.1.50:554/feed" -> "rtsp://admin:*****@192.168.1.50:554/feed"
func RedactCredentials(s string) string {
	if !strings.Contains(s, "://") || !strings.Contains(s, "@") {
		return s
	}
	return credentialRegex.ReplaceAllString(s, "${1}*****${3}")
}

// RedactingHandler is an slog.Handler that redacts credentials from messages and attributes.
type RedactingHandler struct {
	next slog.Handler
}

// NewRedactingHandler wraps an existing slog.Handler.
func NewRedactingHandler(next slog.Handler) *RedactingHandler {
	return &RedactingHandler{next: next}
}

func (h *RedactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *RedactingHandler) Handle(ctx context.Context, r slog.Record) error {
	// Sanitize message
	newRecord := slog.NewRecord(r.Time, r.Level, RedactCredentials(r.Message), r.PC)

	// Sanitize attributes
	r.Attrs(func(a slog.Attr) bool {
		newRecord.AddAttrs(sanitizeAttr(a))
		return true
	})

	return h.next.Handle(ctx, newRecord)
}

func (h *RedactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	sanitized := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		sanitized[i] = sanitizeAttr(a)
	}
	return &RedactingHandler{next: h.next.WithAttrs(sanitized)}
}

func (h *RedactingHandler) WithGroup(name string) slog.Handler {
	return &RedactingHandler{next: h.next.WithGroup(name)}
}

func sanitizeAttr(a slog.Attr) slog.Attr {
	if a.Value.Kind() == slog.KindString {
		return slog.String(a.Key, RedactCredentials(a.Value.String()))
	}
	if a.Value.Kind() == slog.KindGroup {
		groupAttrs := a.Value.Group()
		sanitized := make([]slog.Attr, len(groupAttrs))
		for i, child := range groupAttrs {
			sanitized[i] = sanitizeAttr(child)
		}
		return slog.Group(a.Key, anySlice(sanitized)...)
	}
	return a
}

func anySlice(attrs []slog.Attr) []any {
	res := make([]any, len(attrs))
	for i, a := range attrs {
		res[i] = a
	}
	return res
}

// ParseLevel converts a string log level to slog.Level.
func ParseLevel(lvl string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(lvl)) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// NewLogger initializes a production-grade slog.Logger with credential redaction.
func NewLogger(w io.Writer, levelStr, format string) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}

	opts := &slog.HandlerOptions{
		Level: ParseLevel(levelStr),
	}

	var handler slog.Handler
	if strings.ToLower(format) == "json" {
		handler = slog.NewJSONHandler(w, opts)
	} else {
		handler = slog.NewTextHandler(w, opts)
	}

	redactingHandler := NewRedactingHandler(handler)
	logger := slog.New(redactingHandler)
	return logger
}

// SetupDefaultLogger configures the default global slog logger.
func SetupDefaultLogger(levelStr, format string) *slog.Logger {
	logger := NewLogger(os.Stdout, levelStr, format)
	slog.SetDefault(logger)
	return logger
}
