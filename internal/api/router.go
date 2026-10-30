// Package api serves the REST contract: discovery, status and the operational
// endpoints.
//
// Handlers read the store and the balance cache and nothing else — there is no
// path from an HTTP request to an RPC call except through the balance
// resolver, and that one is answered from the cache whenever the cache is
// clean. That is what makes the read path scale with the store rather than
// with the provider's rate limit.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"tokendiscovery/internal/balances"
	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/core/ports"
	"tokendiscovery/internal/obs"
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

// Indexing reports the indexer's position so a response can state how fresh it
// is. Declared here as the narrow thing the API needs, rather than importing
// the indexer, so the dependency runs one way only.
type Indexing interface {
	Readiness
	LagBlocks() uint64
	LatestSeenBlock() uint64
}

// Store is the read side of ports.Store, which is all the API touches: no
// handler writes to the index.
type Store interface {
	Cursor(ctx context.Context, chainID uint64) (domain.Cursor, error)
	AccountTokens(ctx context.Context, q ports.AccountTokensQuery) (ports.AccountTokensPage, error)
	TokenMeta(ctx context.Context, chainID uint64, tokens []domain.Address) (map[domain.Address]domain.TokenMeta, error)
}

// Deps is what the router needs. The discovery routes mount only when Chains
// and Store are both supplied, so the operational surface can stand alone.
type Deps struct {
	Registry prometheus.Registerer
	Gatherer prometheus.Gatherer
	Metrics  *obs.Metrics

	// Ready gates /readyz. When nil, Indexer is used instead.
	Ready   Readiness
	Indexer Indexing

	Chains   ports.Registry
	Store    Store
	Balances balances.Reader

	Version string
	Logger  *slog.Logger
}

// NewRouter builds the HTTP handler.
func NewRouter(d Deps) http.Handler {
	if d.Logger == nil {
		d.Logger = slog.New(slog.DiscardHandler)
	}
	r := chi.NewRouter()

	// RequestID first so every later log line and error body can carry it;
	// Recoverer so a handler panic becomes a 500 rather than killing the
	// process mid-flight.
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(requestIDHeader)
	r.Use(middleware.Timeout(30 * time.Second))
	r.Use(d.observe)

	// Liveness: the process is running. Deliberately does not consult the
	// indexer — if this returned 503 while catching up, an orchestrator would
	// restart the pod and it would never finish catching up.
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "version": d.Version})
	})

	// Readiness: the instance is caught up enough to answer.
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ready := d.readiness()
		if ready == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"status": "not_ready", "reason": "no readiness source configured",
			})
			return
		}
		if err := ready.Ready(); err != nil {
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

	if d.Chains != nil && d.Store != nil {
		r.Route("/v1", func(v1 chi.Router) {
			v1.Get("/status", d.handleStatus)
			v1.Get("/chains", d.handleChains)
			// Chain-first, then account: the path reads as the question —
			// "on this chain, for this account, which tokens?" — and keeps the
			// chain a first-class segment rather than a query parameter, which
			// is what makes a second network a routing no-op.
			v1.Get("/chains/{chain}/accounts/{address}/tokens", d.handleAccountTokens)
		})
	}

	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, r, http.StatusNotFound, "not-found", "Not found", "no route matches this path")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, r, http.StatusMethodNotAllowed, "method-not-allowed", "Method not allowed", "")
	})

	return r
}

func (d Deps) readiness() Readiness {
	if d.Ready != nil {
		return d.Ready
	}
	if d.Indexer != nil {
		return d.Indexer
	}
	return nil
}

// chainInfo resolves a slug from the URL path.
func (d Deps) chainInfo(slug string) (domain.ChainInfo, bool) {
	if d.Chains == nil {
		return domain.ChainInfo{}, false
	}
	adapter, ok := d.Chains.Adapter(slug)
	if !ok {
		return domain.ChainInfo{}, false
	}
	return adapter.Chain(), true
}

// freshness describes how far behind the chain this instance is.
//
// It is in every discovery response, not just /readyz, because a client that
// asked a *specific* question deserves to know the answer may be behind — and
// because "no tokens" and "not caught up yet" must not look the same.
func (d Deps) freshness() freshnessInfo {
	out := freshnessInfo{Ready: true}
	if ready := d.readiness(); ready != nil {
		if err := ready.Ready(); err != nil {
			out.Ready, out.Reason = false, err.Error()
		}
	}
	if d.Indexer != nil {
		out.LagBlocks = d.Indexer.LagBlocks()
		out.LatestSeenBlock = d.Indexer.LatestSeenBlock()
	}
	return out
}

// serverError logs the cause and returns a problem document that does not leak
// it. An internal failure is an operator's problem, not a caller's.
func (d Deps) serverError(w http.ResponseWriter, r *http.Request, what string, err error) {
	d.Logger.Error("request failed",
		"what", what, "error", err,
		"path", r.URL.Path, "request_id", middleware.GetReqID(r.Context()))
	writeProblem(w, r, http.StatusInternalServerError, "internal",
		"Internal error", "the request could not be completed; quote the requestId when reporting this")
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
