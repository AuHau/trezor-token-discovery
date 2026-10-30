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
	"tokendiscovery/internal/chains/evm"
	"tokendiscovery/internal/config"
	"tokendiscovery/internal/obs"
	"tokendiscovery/internal/store"
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

	st, err := store.Open(cfg.DataDir, store.Options{Logger: log})
	if err != nil {
		return err
	}
	defer st.Close()

	cursor, err := st.Cursor(ctx, cfg.Chain.ChainID)
	if err != nil {
		return err
	}
	// A decoder fix that changes what would be indexed must not be applied to
	// an index built by the old one: a store is rebuildable in ~1.8k RPC calls,
	// so refusing is cheap and a silently mixed index is not.
	if cursor.Seeded() && cursor.ParserVersion != evm.ParserVersion {
		return fmt.Errorf("store was indexed by parser v%d, this build is v%d: delete %s and re-run backfill",
			cursor.ParserVersion, evm.ParserVersion, cfg.DataDir)
	}
	log.Info("store opened",
		"data_dir", cfg.DataDir,
		"bytes", st.DiskUsage(),
		"seeded", cursor.Seeded(),
		"head_block", cursor.HeadBlock,
	)
	if !cursor.Seeded() {
		// Phase 6 turns this into a refusal to boot: serving a half-indexed
		// store returns confidently wrong *empty* token lists. It stays a
		// warning only while `backfill` cannot yet satisfy it, so that the
		// operational surface remains reachable.
		log.Warn("store has never been seeded; discovery would return empty token lists",
			"remedy", "run backfill (phase 6)")
	}

	// Phases 6–8 start the indexer and mount the discovery handlers here. The
	// operational surface is wired now so the logging and metrics stack is
	// verifiable end to end rather than on trust.
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
	if err := st.Close(); err != nil {
		return fmt.Errorf("close store: %w", err)
	}
	log.Info("stopped cleanly")
	return nil
}
