package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"tokendiscovery/internal/obs"
)

type readyFunc func() error

func (f readyFunc) Ready() error { return f() }

func newTestRouter(ready Readiness) http.Handler {
	reg := prometheus.NewRegistry()
	return NewRouter(Deps{Registry: reg, Gatherer: reg, Ready: ready, Version: "test"})
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestHealthzIgnoresReadiness(t *testing.T) {
	// Liveness must stay 200 even when the indexer is behind: if it failed
	// while catching up, an orchestrator would restart the process and it
	// would never finish catching up.
	h := newTestRouter(readyFunc(func() error { return errors.New("still catching up") }))
	rec := get(t, h, "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", rec.Code)
	}
}

func TestReadyzReflectsReadiness(t *testing.T) {
	t.Run("not ready", func(t *testing.T) {
		h := newTestRouter(readyFunc(func() error { return errors.New("indexer not implemented") }))
		rec := get(t, h, "/readyz")
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("/readyz = %d, want 503", rec.Code)
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("body is not JSON: %v", err)
		}
		if body["reason"] != "indexer not implemented" {
			t.Errorf("reason = %v, want the readiness error surfaced", body["reason"])
		}
	})

	t.Run("ready", func(t *testing.T) {
		h := newTestRouter(readyFunc(func() error { return nil }))
		if rec := get(t, h, "/readyz"); rec.Code != http.StatusOK {
			t.Fatalf("/readyz = %d, want 200", rec.Code)
		}
	})

	t.Run("no source configured fails closed", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		h := NewRouter(Deps{Registry: reg, Gatherer: reg, Version: "test"})
		if rec := get(t, h, "/readyz"); rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("/readyz with no readiness source = %d, want 503", rec.Code)
		}
	})
}

func TestMetricsExposed(t *testing.T) {
	// Use the real metric set rather than a bare registry: an empty registry
	// legitimately renders no output, so that would assert nothing.
	m := obs.NewMetrics()
	h := NewRouter(Deps{Registry: m.Registry, Gatherer: m.Registry, Version: "test",
		Ready: readyFunc(func() error { return nil })})

	rec := get(t, h, "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "# HELP") {
		t.Fatal("/metrics did not return Prometheus exposition format")
	}
	// A plain gauge always has a sample, so its presence proves the service
	// metric set (not just the Go collector) is registered and exposed.
	if !strings.Contains(body, "td_head_block") {
		t.Error("/metrics is missing the service metric set (td_head_block)")
	}
}

func TestRequestIDHeaderAlwaysSet(t *testing.T) {
	h := newTestRouter(readyFunc(func() error { return nil }))
	for _, path := range []string{"/healthz", "/readyz", "/nope"} {
		if got := get(t, h, path).Header().Get("X-Request-Id"); got == "" {
			t.Errorf("%s: X-Request-Id not set", path)
		}
	}
}

func TestUnknownRouteReturnsProblemJSON(t *testing.T) {
	h := newTestRouter(readyFunc(func() error { return nil }))
	rec := get(t, h, "/v1/does-not-exist")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json", ct)
	}
	var p problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("body is not a problem document: %v", err)
	}
	if p.Type != problemBase+"not-found" || p.Status != 404 {
		t.Errorf("unexpected problem document: %+v", p)
	}
	if p.RequestID == "" {
		t.Error("problem document is missing requestId")
	}
}
