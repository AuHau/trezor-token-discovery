// Package obs wires up observability: structured logging and the Prometheus
// metric set. It is imported by every layer, so it depends on nothing but the
// standard library and the Prometheus client.
package obs

import (
	"io"
	"log/slog"
)

// NewLogger builds the process logger. Format is "text" for humans or "json"
// for log pipelines.
func NewLogger(w io.Writer, level slog.Level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if format == "json" {
		h = slog.NewJSONHandler(w, opts)
	} else {
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h)
}
