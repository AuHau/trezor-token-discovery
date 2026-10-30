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
	"tokendiscovery/internal/indexer"
	"tokendiscovery/internal/obs"
)

func cmdServe(ctx context.Context, args []string, cfg config.Config, log *slog.Logger, m *obs.Metrics) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", "", "override HTTP_ADDR")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *addr != "" {
		cfg.HTTPAddr = *addr
	}

	svc, err := openService(cfg, log, m)
	if err != nil {
		return err
	}
	defer svc.Close()

	cursor, err := svc.store.Cursor(ctx, cfg.Chain.ChainID)
	if err != nil {
		return err
	}
	// Refusing to boot is the point. Serving a half-indexed store would answer
	// with confidently wrong *empty* token lists — the one failure mode that
	// genuinely misleads a user — so it is made impossible rather than
	// described in the API.
	if !cursor.Seeded() {
		return fmt.Errorf("store at %s has never been seeded: run `tokendiscovery backfill` first", cfg.DataDir)
	}
	log.Info("store opened",
		"data_dir", cfg.DataDir,
		"bytes", svc.store.DiskUsage(),
		"head_block", cursor.HeadBlock,
		"seeded_from_block", cursor.SeededFromBlock,
		"parser_version", cursor.ParserVersion,
	)

	// Phases 7–8 mount the discovery handlers here. Readiness is already real:
	// it is backed by the indexer's own position, so /readyz reports whether
	// this instance is caught up rather than merely running.
	log.Warn("discovery endpoints are not wired yet",
		"phase", 7, "available", []string{"/healthz", "/readyz", "/metrics"})

	handler := api.NewRouter(api.Deps{
		Registry: m.Registry,
		Gatherer: m.Registry,
		Ready:    svc.indexer,
		Version:  buildVersion(),
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// The follower and the HTTP server run side by side; either one failing
	// fatally must bring the process down rather than leave it half alive, so
	// both report into one channel.
	errc := make(chan error, 2)
	go func() {
		log.Info("http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("http server: %w", err)
			return
		}
		errc <- nil
	}()
	go func() {
		// Follow returns nil on cancellation. Anything else is fatal: a reorg
		// deeper than the indexer can resolve, or a store it must not touch.
		if err := svc.indexer.Follow(ctx); err != nil {
			errc <- fmt.Errorf("indexer: %w", err)
			return
		}
		errc <- nil
	}()

	var fatal error
	select {
	case err := <-errc:
		fatal = err
		if fatal != nil {
			log.Error("shutting down after a fatal error", "error", fatal)
		}
	case <-ctx.Done():
		log.Info("shutdown signal received, draining")
	}

	// Drain with a bounded grace period, independent of the cancelled ctx.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return errors.Join(fatal, fmt.Errorf("graceful shutdown: %w", err))
	}
	if err := svc.Close(); err != nil {
		return errors.Join(fatal, fmt.Errorf("close store: %w", err))
	}
	if fatal != nil {
		return fatal
	}
	log.Info("stopped cleanly", "head_block", svc.indexer.Status().HeadBlock)
	return nil
}

// Keep the readiness source honest: a compile-time check that the indexer is
// what /readyz consults, rather than a placeholder that always answers yes.
var _ api.Readiness = (*indexer.Indexer)(nil)
