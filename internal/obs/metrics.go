package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics is the service's metric set, held explicitly rather than in package
// globals so tests can build an isolated registry.
//
// The set is deliberately small and chosen to *prove the design's claims*:
// RPCCalls backs the "efficient in RPC requests" requirement, the balance-cache
// counters back "steady-state RPC tracks on-chain activity, not request rate",
// and the HTTP 304 counters back the separate claim that the ETag saves
// bandwidth rather than RPC.
type Metrics struct {
	Registry *prometheus.Registry

	// RPC
	RPCCalls      *prometheus.CounterVec   // method, outcome
	RPCDuration   *prometheus.HistogramVec // method
	RPCBytes      *prometheus.CounterVec   // method; decoded response size
	RPCRateLimits prometheus.Counter

	// Indexer
	HeadBlock         prometheus.Gauge
	SafeBlock         prometheus.Gauge
	FinalizedBlock    prometheus.Gauge
	FinalityLagBlocks prometheus.Gauge
	LagSeconds        prometheus.Gauge
	BlocksIndexed     prometheus.Counter
	LogsDecoded       prometheus.Counter
	PairsWritten      prometheus.Counter
	Reorgs            prometheus.Counter
	ReorgDepth        prometheus.Histogram
	NonCanonicalLogs  prometheus.Counter

	// Balances
	BalanceCacheHits    prometheus.Counter
	BalanceCacheMisses  prometheus.Counter
	BalanceCacheDirty   prometheus.Counter
	BalanceCacheFlushes prometheus.Counter
	MulticallCalls      prometheus.Counter
	MulticallSlots      prometheus.Counter
	BalanceCoalesced    prometheus.Counter
	BalanceDegraded     prometheus.Counter

	// API
	HTTPRequests   *prometheus.CounterVec   // route, status
	HTTPDuration   *prometheus.HistogramVec // route
	HTTPBodyBytes  *prometheus.CounterVec   // route; bytes actually written
	HTTPSavedBy304 prometheus.Counter       // bytes a 304 avoided writing
}

// NewMetrics registers the metric set on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
		reg.MustRegister(c)
		return c
	}
	counterVec := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
		reg.MustRegister(c)
		return c
	}
	gauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		reg.MustRegister(g)
		return g
	}
	histVec := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
		reg.MustRegister(h)
		return h
	}
	hist := func(name, help string, buckets []float64) prometheus.Histogram {
		h := prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets})
		reg.MustRegister(h)
		return h
	}

	// RPC calls are seconds-scale (a 200-block getLogs took ~1.4s measured),
	// so the default latency buckets are too fast to be useful here.
	rpcBuckets := []float64{.05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 120}
	httpBuckets := []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5}

	return &Metrics{
		Registry: reg,

		RPCCalls:      counterVec("td_rpc_calls_total", "JSON-RPC calls by method and outcome.", "method", "outcome"),
		RPCDuration:   histVec("td_rpc_duration_seconds", "JSON-RPC call latency.", rpcBuckets, "method"),
		RPCBytes:      counterVec("td_rpc_response_bytes_total", "Decoded JSON-RPC response bytes.", "method"),
		RPCRateLimits: counter("td_rpc_rate_limited_total", "Responses rejected by the provider's rate limiter."),

		HeadBlock:         gauge("td_head_block", "Highest block indexed (the live edge)."),
		SafeBlock:         gauge("td_safe_block", "Safe (justified) block height."),
		FinalizedBlock:    gauge("td_finalized_block", "Finalized block height."),
		FinalityLagBlocks: gauge("td_finality_lag_blocks", "latest minus finalized; a growing value means finality has stalled."),
		LagSeconds:        gauge("td_lag_seconds", "Wall-clock age of the indexed head block."),
		BlocksIndexed:     counter("td_blocks_indexed_total", "Blocks successfully indexed."),
		LogsDecoded:       counter("td_logs_decoded_total", "Transfer logs decoded."),
		PairsWritten:      counter("td_pairs_written_total", "Account-token pairs written."),
		Reorgs:            counter("td_reorgs_total", "Reorganisations detected."),
		ReorgDepth:        hist("td_reorg_depth_blocks", "Depth of detected reorganisations.", []float64{1, 2, 3, 5, 10, 20, 50, 128}),
		NonCanonicalLogs:  counter("td_non_canonical_logs_total", "Logs rejected because their blockHash was not canonical."),

		BalanceCacheHits:    counter("td_balance_cache_hits_total", "Balance reads served from a clean cache entry."),
		BalanceCacheMisses:  counter("td_balance_cache_misses_total", "Balance reads with no cache entry."),
		BalanceCacheDirty:   counter("td_balance_cache_dirty_total", "Balance reads whose cache entry was invalidated by a transfer."),
		BalanceCacheFlushes: counter("td_balance_cache_flushes_total", "Full balance-cache flushes, one per detected reorg."),
		MulticallCalls:      counter("td_multicall_calls_total", "Multicall3 eth_call batches issued."),
		MulticallSlots:      counter("td_multicall_slots_total", "Individual contract calls packed into those batches."),
		BalanceCoalesced:    counter("td_balance_coalesced_total", "Balance refreshes that shared an in-flight read with another request."),
		BalanceDegraded:     counter("td_balance_degraded_total", "Tokens a live read could not refresh, served stale or unavailable instead."),

		HTTPRequests:   counterVec("td_http_requests_total", "HTTP requests by route and status.", "route", "status"),
		HTTPDuration:   histVec("td_http_duration_seconds", "HTTP request latency by route.", httpBuckets, "route"),
		HTTPBodyBytes:  counterVec("td_http_body_bytes_total", "Response body bytes written.", "route"),
		HTTPSavedBy304: counter("td_http_bytes_saved_by_304_total", "Body bytes not written because a conditional request matched."),
	}
}
