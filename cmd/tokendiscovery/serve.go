package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"tokendiscovery/internal/api"
	"tokendiscovery/internal/config"
	"tokendiscovery/internal/obs"
)

// notReady is the phase-2 readiness source. Once the indexer lands it is
// replaced by one backed by the cursor and the tip lag; until then it reports
// honestly that this instance must not take traffic.
type notReady struct{ reason string }

func (n notReady) Ready() error { return errors.New(n.reason) }

func cmdServe(ctx context.Context, args []string, cfg config.Config, log *slog.Logger, m *obs.Metrics) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", "", "override HTTP_ADDR")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *addr != "" {
		cfg.HTTPAddr = *addr
	}

	// Phases 5–8 open the store, start the indexer and mount the discovery
	// handlers here. The operational surface is wired now so the logging and
	// metrics stack is verifiable end to end rather than on trust.
	log.Warn("discovery endpoints are not wired yet",
		"phase", 7, "available", []string{"/healthz", "/readyz", "/metrics"})

	handler := api.NewRouter(api.Deps{
		Registry: m.Registry,
		Gatherer: m.Registry,
		Ready:    notReady{reason: "indexer not implemented (phase 6)"},
		Version:  buildVersion(),
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Serve in the background so the main goroutine can wait on ctx, which is
	// cancelled by SIGINT/SIGTERM.
	errc := make(chan error, 1)
	go func() {
		log.Info("http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
			return
		}
		errc <- nil
	}()

	select {
	case err := <-errc:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case <-ctx.Done():
		log.Info("shutdown signal received, draining")
	}

	// Drain with a bounded grace period, independent of the cancelled ctx.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	log.Info("stopped cleanly")
	return nil
}
