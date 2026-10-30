package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"tokendiscovery/internal/balances"
	"tokendiscovery/internal/chains"
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
	client   *rpc.Client
	adapter  *evm.Adapter
	registry *chains.Registry
	store    *store.Store
	indexer  *indexer.Indexer
	balances *balances.Live
}

func openService(ctx context.Context, cfg config.Config, log *slog.Logger, m *obs.Metrics) (*service, error) {
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
			// Requests stop waiting on a balance read at BalanceBudget, while the
			// read goes on to fill the cache; this bounds each of its calls.
			"eth_call": 15 * time.Second,
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
		MulticallAddress:      cfg.Chain.MulticallAddress,
		MulticallChunk:        cfg.Chain.MulticallChunk,
		Logger:                log,
		Metrics:               m,
	})
	if err != nil {
		return nil, fmt.Errorf("chain adapter: %w", err)
	}

	// Before the store is opened, so a mismatched endpoint cannot write a
	// single key.
	if err := adapter.VerifyChainID(ctx); err != nil {
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

	registry, err := chains.NewRegistry(adapter)
	if err != nil {
		_ = st.Close()
		return nil, err
	}

	bal, err := balances.NewLive(st, registry, balances.LiveOptions{
		Budget: cfg.Chain.BalanceBudget,
		// About a second of queue at the RPC rate, so an admitted read still
		// starts soon and the indexer sharing the bucket is not held up.
		MaxReads: int(math.Ceil(cfg.Chain.RPCRate)),
		Logger:   log,
		Metrics:  m,
	})
	if err != nil {
		_ = st.Close()
		return nil, err
	}

	return &service{
		client:   client,
		adapter:  adapter,
		registry: registry,
		store:    st,
		indexer:  ix,
		balances: bal,
	}, nil
}

// Close stops the balance reads first, because a read ends by writing to the
// store and Pebble panics on a closed database.
func (s *service) Close() error {
	s.balances.Close()
	return s.store.Close()
}
