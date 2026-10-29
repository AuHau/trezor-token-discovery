// Command tokendiscovery is the token discovery service.
//
//	tokendiscovery backfill     seed the store from the chain's start block
//	tokendiscovery serve         follow the tip and answer queries
//	tokendiscovery checkpoint    write a consistent snapshot of the store
//	tokendiscovery version       print build information
//
// backfill must complete before serve will start. Serving a half-indexed store
// would return confidently wrong *empty* token lists, which is the one failure
// mode that genuinely misleads a user, so it is made impossible rather than
// described in the API.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"tokendiscovery/internal/config"
	"tokendiscovery/internal/obs"
)

// errNotImplemented marks a subcommand whose implementation lands in a later
// phase. It is returned explicitly so the binary fails loudly instead of
// appearing to succeed while doing nothing.
var errNotImplemented = errors.New("not implemented yet")

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `tokendiscovery — ERC-20/ERC-721 token discovery over plain JSON-RPC

usage:
  tokendiscovery <command> [flags]

commands:
  backfill      seed the store from the chain's start block to the tip
  serve         follow the chain tip and serve the REST API
  checkpoint    write a consistent snapshot of the store to a directory
  version       print build information

configuration is read from the environment; see .env.example
`)
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("a command is required")
	}

	cmd, rest := args[0], args[1:]
	switch cmd {
	case "version":
		return cmdVersion()
	case "backfill", "serve", "checkpoint":
		// fall through
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}

	cfg, overrides, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}

	log := obs.NewLogger(os.Stderr, cfg.LogLevel, cfg.LogFormat)
	metrics := obs.NewMetrics()

	log.Info("starting",
		"command", cmd,
		"version", buildVersion(),
		"chain", cfg.Chain.Slug,
		"chain_id", cfg.Chain.ChainID,
		"start_block", cfg.Chain.StartBlock,
		"data_dir", cfg.DataDir,
		"rpc", cfg.RedactedRPCURL(), // never the raw URL: it carries the API key
		"log_range_blocks", cfg.Chain.LogRangeBlocks,
		"watermark_strategy", cfg.Chain.Watermarks,
		"env_overrides", overrides,
	)

	// A single context cancelled by SIGINT/SIGTERM, so every long-running loop
	// shuts down from one signal path.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "backfill":
		return cmdBackfill(ctx, rest, cfg, log, metrics)
	case "serve":
		return cmdServe(ctx, rest, cfg, log, metrics)
	case "checkpoint":
		return cmdCheckpoint(ctx, rest, cfg, log, metrics)
	}
	panic("unreachable")
}

func cmdVersion() error {
	fmt.Printf("tokendiscovery %s\n", buildVersion())
	if bi, ok := debug.ReadBuildInfo(); ok {
		fmt.Printf("go: %s\n", bi.GoVersion)
	}
	return nil
}

// buildVersion reports the VCS revision stamped in by the Go toolchain.
func buildVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "(unknown)"
	}
	var rev, dirty string
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			if len(s.Value) > 12 {
				rev = s.Value[:12]
			} else {
				rev = s.Value
			}
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		}
	}
	if rev == "" {
		return "(devel)"
	}
	return rev + dirty
}

func cmdBackfill(ctx context.Context, args []string, cfg config.Config, log *slog.Logger, m *obs.Metrics) error {
	fs := flag.NewFlagSet("backfill", flag.ContinueOnError)
	fromBlock := fs.Uint64("from-block", 0, "override the configured start block (for a fast demo)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *fromBlock != 0 {
		cfg.Chain.StartBlock = *fromBlock
		log.Info("start block overridden", "from_block", *fromBlock)
	}
	_ = ctx
	_ = m
	// Phase 6. Needs the RPC client (3), decoder (4) and store (5).
	return fmt.Errorf("backfill: %w (phase 6)", errNotImplemented)
}

func cmdCheckpoint(ctx context.Context, args []string, cfg config.Config, log *slog.Logger, m *obs.Metrics) error {
	fs := flag.NewFlagSet("checkpoint", flag.ContinueOnError)
	out := fs.String("out", "", "directory to write the checkpoint to (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("checkpoint: -out is required")
	}
	_, _, _ = ctx, cfg, log
	_ = m
	// Phase 5, with the store.
	return fmt.Errorf("checkpoint: %w (phase 5)", errNotImplemented)
}
