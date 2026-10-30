package main

import (
	"fmt"
	"log/slog"
	"time"

	"tokendiscovery/internal/chains/evm"
	"tokendiscovery/internal/config"
	"tokendiscovery/internal/indexer"
	"tokendiscovery/internal/obs"
	"tokendiscovery/internal/rpc"
	"tokendiscovery/internal/store"
)

// service is the wiring every command shares: one RPC client, one chain
// adapter, one store, one indexer.
//
// It exists so that `backfill` and `serve` cannot drift apart. They must agree
// on the store layout, the decoder version and the chain configuration, because
// one writes what the other continues from.
type service struct {
	client  *rpc.Client
	adapter *evm.Adapter
	store   *store.Store
	indexer *indexer.Indexer
}

func openService(cfg config.Config, log *slog.Logger, m *obs.Metrics) (*service, error) {
	client, err := rpc.New(rpc.Options{
		URL:     cfg.RPCURL,
		Rate:    cfg.Chain.RPCRate,
		Burst:   cfg.Chain.RPCBurst,
		Timeout: cfg.Chain.RPCTimeout,
		MethodTimeouts: map[string]time.Duration{
			// A 200-block getLogs moves multiple megabytes; a header read is
			// a few hundred bytes and should fail fast so a tick can retry.
			"eth_getLogs":          cfg.Chain.RPCTimeout,
			"eth_getBlockByNumber": 15 * time.Second,
		},
		Retry:   rpc.DefaultRetry(),
		Metrics: m,
		Logger:  log,
	})
	if err != nil {
		return nil, fmt.Errorf("rpc client: %w", err)
	}

	adapter, err := evm.NewAdapter(client, evm.Config{
		Chain:                 cfg.Chain.ChainInfo(),
		WatermarkPollInterval: cfg.Chain.WatermarkPollInterval,
		FinalityStallBlocks:   cfg.Chain.FinalityStallBlocks,
		ConfirmationDepth:     cfg.Chain.ConfirmationDepth,
		Logger:                log,
		Metrics:               m,
	})
	if err != nil {
		return nil, fmt.Errorf("chain adapter: %w", err)
	}

	st, err := store.Open(cfg.DataDir, store.Options{Logger: log})
	if err != nil {
		return nil, err
	}

	ix, err := indexer.New(adapter, st, indexer.Options{
		LogRangeBlocks:  cfg.Chain.LogRangeBlocks,
		TipPollInterval: cfg.Chain.TipPollInterval,
		HeaderRingSize:  cfg.Chain.HeaderRingSize,
		ParserVersion:   evm.ParserVersion,
		Logger:          log,
		Metrics:         m,
	})
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("indexer: %w", err)
	}

	return &service{client: client, adapter: adapter, store: st, indexer: ix}, nil
}

func (s *service) Close() error { return s.store.Close() }
