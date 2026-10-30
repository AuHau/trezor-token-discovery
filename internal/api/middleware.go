package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// observe records request count, latency and body size per route.
//
// The label is chi's *route pattern*, never the path. An account address in a
// metric label would mint a new time series per user — 5,000 users would mean
// 5,000 series on one counter — so the pattern is the only safe choice.
func (d Deps) observe(next http.Handler) http.Handler {
	if d.Metrics == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			// Unmatched paths share one label rather than minting a series per
			// scanner probe.
			route = "unmatched"
		}
		status := statusLabel(ww.Status())
		d.Metrics.HTTPRequests.WithLabelValues(route, status).Inc()
		d.Metrics.HTTPDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
		d.Metrics.HTTPBodyBytes.WithLabelValues(route).Add(float64(ww.BytesWritten()))
	})
}

// statusLabel is the numeric code: low cardinality in practice, and it keeps
// "which error" answerable without a second metric.
func statusLabel(code int) string {
	if code == 0 {
		code = http.StatusOK // the handler never wrote, so Go sent 200
	}
	return strconv.Itoa(code)
}
