// Package api serves the REST contract. Phase 2 wires only the operational
// endpoints — the discovery handlers land in phase 7, against the same router.
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Readiness reports whether this instance should receive traffic.
//
// It is deliberately separate from liveness: a process that is up but still
// catching up to the tip must not be sent requests, because it would answer
// with a stale head. /readyz is what a load balancer gates on.
type Readiness interface {
	// Ready returns nil when the instance is caught up and serving, or an
	// error explaining what it is still waiting for.
	Ready() error
}

// Deps is what the router needs. Fields are added as later phases land.
type Deps struct {
	Registry prometheus.Registerer
	Gatherer prometheus.Gatherer
	Ready    Readiness
	Version  string
}

// NewRouter builds the HTTP handler.
func NewRouter(d Deps) http.Handler {
	r := chi.NewRouter()

	// RequestID first so every later log line and error body can carry it;
	// Recoverer so a handler panic becomes a 500 rather than killing the
	// process mid-flight.
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(requestIDHeader)
	r.Use(middleware.Timeout(30 * time.Second))

	// Liveness: the process is running. Deliberately does not consult the
	// indexer — if this returned 503 while catching up, an orchestrator would
	// restart the pod and it would never finish catching up.
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "version": d.Version})
	})

	// Readiness: the instance is caught up enough to answer.
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if d.Ready == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"status": "not_ready", "reason": "no readiness source configured",
			})
			return
		}
		if err := d.Ready.Ready(); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"status": "not_ready", "reason": err.Error(),
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
	})

	if d.Gatherer != nil {
		r.Handle("/metrics", promhttp.HandlerFor(d.Gatherer, promhttp.HandlerOpts{}))
	}

	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, r, http.StatusNotFound, "not-found", "Not found", "no route matches this path")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, r, http.StatusMethodNotAllowed, "method-not-allowed", "Method not allowed", "")
	})

	return r
}

// requestIDHeader echoes chi's request ID back to the caller so a user can
// quote it when reporting a problem.
func requestIDHeader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id := middleware.GetReqID(r.Context()); id != "" {
			w.Header().Set("X-Request-Id", id)
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
