// Package config loads configuration from the environment. Defaults that came
// out of the phase-1 measurement run are marked; see docs/measurements.md.
//
// Env-only and stdlib-only on purpose: the service takes a handful of settings,
// so a config library would be more machinery than the problem needs.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"tokendiscovery/internal/core/domain"
)

// Multicall3 is deployed at the same address on Ethereum and most EVM chains.
const defaultMulticall3 = "0xcA11bde05977b3631167028862bE2a173976CA11"

// Config is the whole service configuration.
type Config struct {
	// RPCURL is the JSON-RPC endpoint. Required. Contains a provider API key,
	// so it is read from the environment and never logged.
	RPCURL string

	// DataDir holds the Pebble store.
	DataDir string

	// HTTPAddr is the listen address for the API, health and metrics.
	HTTPAddr string

	LogLevel  slog.Level
	LogFormat string // "text" or "json"

	Chain ChainConfig
}

// ChainConfig is the per-network configuration. Adding an EVM network means
// another one of these — the generic EVM adapter is parameterised entirely by
// these values.
type ChainConfig struct {
	Slug       string
	ChainID    uint64
	StartBlock uint64

	// LogRangeBlocks is how many blocks one eth_getLogs request covers.
	//
	// Measured: the endpoint caps the *range* at 10,000 blocks but imposes no
	// result cap (365k logs in a single response), so this is chosen for a sane
	// response size, not to dodge a result limit. At ~365 KB/block decoded,
	// 200 blocks is ~73 MB decoded / ~4 MB gzipped.
	LogRangeBlocks uint64

	// TipPollInterval is how often the head is polled. One
	// eth_getBlockByNumber("latest") per tick yields both the head number and
	// the parent hash the reorg detector needs.
	TipPollInterval time.Duration

	// WatermarkPollInterval is how often safe/finalized are refreshed. They
	// only move on epoch boundaries, so this is deliberately much slower than
	// the tip poll.
	WatermarkPollInterval time.Duration

	// HeaderRingSize is how many recent headers are kept in memory for reorg
	// detection. Reorgs deeper than this are not auto-detected.
	HeaderRingSize int

	// MulticallAddress is the Multicall3 deployment used to batch balanceOf.
	MulticallAddress domain.Address

	// MulticallChunk is how many balanceOf calls go into one eth_call.
	MulticallChunk int

	// BalanceBudget bounds balance resolution per request; on expiry the
	// handler serves cached values with an honest staleness flag rather than
	// hanging.
	BalanceBudget time.Duration

	// RPCRate and RPCBurst configure the client-side token bucket, so the
	// service self-throttles below the provider's limit instead of being
	// throttled by it.
	RPCRate  float64
	RPCBurst int

	// RPCTimeout bounds a single RPC call.
	RPCTimeout time.Duration

	Watermarks domain.WatermarkStrategy

	// FinalityStallBlocks is the latest-minus-finalized gap past which the
	// chain is treated as not finalising and the block-count fallback engages.
	FinalityStallBlocks uint64

	// ConfirmationDepth is the fallback safety depth for the block-count
	// watermark strategy.
	ConfirmationDepth uint64

	Standards []domain.Standard
}

// DefaultChain is the network selected when CHAIN is unset.
const DefaultChain = "ethereum"

// Default returns the configuration with every non-secret value filled in.
func Default() Config {
	ch, _ := Preset(DefaultChain)
	return Config{
		DataDir:   defaultDataDir(DefaultChain),
		HTTPAddr:  ":8080",
		LogLevel:  slog.LevelInfo,
		LogFormat: "text",
		Chain:     ch,
	}
}

// Preset returns the built-in configuration for a known network. Every EVM
// network is served by the same adapter, so a preset is the whole cost of
// adding one; the values that differ between chains are the ones measured in
// docs/measurements.md.
func Preset(slug string) (ChainConfig, bool) {
	switch strings.ToLower(slug) {
	case "ethereum":
		return ChainConfig{
			Slug:       "ethereum",
			ChainID:    1,
			StartBlock: 25_933_634, // from the assignment

			LogRangeBlocks:        200, // measured; see docs/measurements.md
			TipPollInterval:       4 * time.Second,
			WatermarkPollInterval: 6 * time.Minute, // ~1 epoch
			HeaderRingSize:        128,

			MulticallAddress: domain.MustParseAddress(defaultMulticall3),
			MulticallChunk:   300,
			BalanceBudget:    800 * time.Millisecond,

			RPCRate:    25,
			RPCBurst:   50,
			RPCTimeout: 120 * time.Second, // a 200-block getLogs is multi-MB

			Watermarks: domain.WatermarkConsensusTags,
			// 8 epochs. Healthy mainnet reaches 96 blocks behind just before an
			// epoch finalises, plus up to 30 more while the tag is cached, so
			// anything near that would call every other cycle a stall.
			FinalityStallBlocks: 256,
			ConfirmationDepth:   12,

			Standards: []domain.Standard{domain.StandardERC20, domain.StandardERC721},
		}, true

	case "linea":
		// Linea is a zk-rollup with a single sequencer. It publishes the same
		// block tags as Ethereum, so it uses the same strategy, but `safe` and
		// `finalized` are both "proven on L1" and trail the head by hours rather
		// than minutes. That is the one value here that is not a copy of
		// Ethereum's; see docs/measurements.md.
		return ChainConfig{
			Slug:    "linea",
			ChainID: 59144,
			// The Linea block with the same timestamp as the assignment's
			// Ethereum start block, so both chains cover the same window.
			StartBlock: 31_972_391,

			// Measured: ~1.7 Transfer logs and ~1 KB per block, against ~574 and
			// ~365 KB on Ethereum. 2,000 blocks is ~2 MB and ~3.4k logs, under
			// the public endpoint's 10,000-result cap with room for bursts; a
			// burst past it is halved like any other result-cap error.
			LogRangeBlocks:        2_000,
			TipPollInterval:       4 * time.Second,
			WatermarkPollInterval: 6 * time.Minute,
			HeaderRingSize:        128,

			MulticallAddress: domain.MustParseAddress(defaultMulticall3), // verified deployed
			MulticallChunk:   300,
			BalanceBudget:    800 * time.Millisecond,

			RPCRate:    25,
			RPCBurst:   50,
			RPCTimeout: 60 * time.Second,

			Watermarks: domain.WatermarkConsensusTags,
			// Measured: finalized trails latest by ~1,700 blocks (~4 h) while
			// healthy. Ethereum's 256 would declare a stall on every poll and
			// replace L1 finality with a 12-block guess. 21,600 blocks is about
			// two days at the measured ~9 s block time.
			FinalityStallBlocks: 21_600,
			ConfirmationDepth:   12,

			Standards: []domain.Standard{domain.StandardERC20, domain.StandardERC721},
		}, true
	}
	return ChainConfig{}, false
}

// Presets lists the networks Preset knows, for error messages.
func Presets() []string { return []string{"ethereum", "linea"} }

// defaultDataDir keeps each network in its own store. Ethereum keeps the
// original ./data so existing stores stay where they are; every other chain
// gets a sibling, so selecting a chain can never write into another chain's
// store by default.
func defaultDataDir(slug string) string {
	if slug == DefaultChain {
		return "./data"
	}
	return "./data-" + slug
}

// Load builds a Config from defaults overlaid with environment variables, then
// validates it. Every override is reported in the returned slice so startup can
// log what was changed without ever logging the RPC URL.
func Load() (Config, []string, error) {
	cfg := Default()
	var overrides []string
	var errs []error

	note := func(key, val string) { overrides = append(overrides, key+"="+val) }
	fail := func(key string, err error) { errs = append(errs, fmt.Errorf("%s: %w", key, err)) }

	cfg.RPCURL = strings.TrimSpace(os.Getenv("RPC_URL"))

	// CHAIN goes first: it replaces the whole chain block, so the per-value
	// overrides below apply on top of the selected network.
	if v, ok := lookup("CHAIN"); ok {
		ch, known := Preset(v)
		if !known {
			fail("CHAIN", fmt.Errorf("unknown chain %q (known: %s)", v, strings.Join(Presets(), ", ")))
		} else {
			cfg.Chain = ch
			cfg.DataDir = defaultDataDir(ch.Slug)
			note("CHAIN", v)
		}
	}
	if v, ok := lookup("DATA_DIR"); ok {
		cfg.DataDir = v
		note("DATA_DIR", v)
	}
	if v, ok := lookup("HTTP_ADDR"); ok {
		cfg.HTTPAddr = v
		note("HTTP_ADDR", v)
	}
	if v, ok := lookup("LOG_LEVEL"); ok {
		lvl, err := parseLevel(v)
		if err != nil {
			fail("LOG_LEVEL", err)
		} else {
			cfg.LogLevel = lvl
			note("LOG_LEVEL", v)
		}
	}
	if v, ok := lookup("LOG_FORMAT"); ok {
		if v != "text" && v != "json" {
			fail("LOG_FORMAT", errors.New(`must be "text" or "json"`))
		} else {
			cfg.LogFormat = v
			note("LOG_FORMAT", v)
		}
	}
	if v, ok := lookup("CHAIN_START_BLOCK"); ok {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			fail("CHAIN_START_BLOCK", err)
		} else {
			cfg.Chain.StartBlock = n
			note("CHAIN_START_BLOCK", v)
		}
	}
	if v, ok := lookup("CHAIN_LOG_RANGE_BLOCKS"); ok {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			fail("CHAIN_LOG_RANGE_BLOCKS", err)
		} else {
			cfg.Chain.LogRangeBlocks = n
			note("CHAIN_LOG_RANGE_BLOCKS", v)
		}
	}
	if v, ok := lookup("CHAIN_TIP_POLL"); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			fail("CHAIN_TIP_POLL", err)
		} else {
			cfg.Chain.TipPollInterval = d
			note("CHAIN_TIP_POLL", v)
		}
	}
	if v, ok := lookup("CHAIN_WATERMARK_STRATEGY"); ok {
		s := domain.WatermarkStrategy(v)
		if !s.Valid() {
			fail("CHAIN_WATERMARK_STRATEGY", errors.New("unknown strategy"))
		} else {
			cfg.Chain.Watermarks = s
			note("CHAIN_WATERMARK_STRATEGY", v)
		}
	}
	if v, ok := lookup("RPC_RATE"); ok {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			fail("RPC_RATE", err)
		} else {
			cfg.Chain.RPCRate = f
			note("RPC_RATE", v)
		}
	}

	if len(errs) > 0 {
		return cfg, overrides, errors.Join(errs...)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, overrides, err
	}
	return cfg, overrides, nil
}

func lookup(key string) (string, bool) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}
	return v, true
}

func parseLevel(s string) (slog.Level, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return 0, fmt.Errorf("must be debug, info, warn or error")
	}
	return l, nil
}

// Validate checks the configuration is internally coherent. It is separate from
// Load so tests can validate hand-built configs.
func (c Config) Validate() error {
	var errs []error
	if c.RPCURL == "" {
		errs = append(errs, errors.New("RPC_URL is required (see .env.example)"))
	} else if !strings.HasPrefix(c.RPCURL, "http://") && !strings.HasPrefix(c.RPCURL, "https://") {
		errs = append(errs, errors.New("RPC_URL must be an http(s) URL"))
	}
	if c.DataDir == "" {
		errs = append(errs, errors.New("DATA_DIR must not be empty"))
	}
	if c.HTTPAddr == "" {
		errs = append(errs, errors.New("HTTP_ADDR must not be empty"))
	}

	ch := c.Chain
	if ch.Slug == "" {
		errs = append(errs, errors.New("chain slug must not be empty"))
	}
	if ch.ChainID == 0 {
		errs = append(errs, errors.New("chain id must not be zero"))
	}
	if ch.StartBlock == 0 {
		errs = append(errs, errors.New("chain start block must not be zero"))
	}
	if ch.LogRangeBlocks == 0 || ch.LogRangeBlocks > 10_000 {
		// 10,000 is the measured provider range cap.
		errs = append(errs, errors.New("log range must be between 1 and 10000 blocks"))
	}
	if ch.TipPollInterval <= 0 {
		errs = append(errs, errors.New("tip poll interval must be positive"))
	}
	if ch.HeaderRingSize < 2 {
		errs = append(errs, errors.New("header ring must hold at least 2 headers"))
	}
	if ch.MulticallChunk <= 0 {
		errs = append(errs, errors.New("multicall chunk must be positive"))
	}
	if ch.BalanceBudget <= 0 {
		errs = append(errs, errors.New("balance budget must be positive"))
	}
	if ch.RPCRate <= 0 {
		errs = append(errs, errors.New("rpc rate must be positive"))
	}
	if ch.RPCBurst <= 0 {
		errs = append(errs, errors.New("rpc burst must be positive"))
	}
	if !ch.Watermarks.Valid() {
		errs = append(errs, fmt.Errorf("unknown watermark strategy %q", ch.Watermarks))
	}
	if len(ch.Standards) == 0 {
		errs = append(errs, errors.New("at least one token standard must be enabled"))
	}
	for _, s := range ch.Standards {
		if !s.Valid() {
			errs = append(errs, fmt.Errorf("unsupported standard %q", s))
		}
	}
	return errors.Join(errs...)
}

// ChainInfo projects the config into the domain description an adapter exposes.
func (c ChainConfig) ChainInfo() domain.ChainInfo {
	return domain.ChainInfo{
		Slug:       c.Slug,
		ChainID:    c.ChainID,
		StartBlock: c.StartBlock,
		Standards:  c.Standards,
		Watermarks: c.Watermarks,
	}
}

// RedactedRPCURL is the RPC endpoint with its credential removed, safe to log.
// Provider URLs commonly carry the API key in the path or query, so everything
// after the host is dropped rather than selectively masked.
func (c Config) RedactedRPCURL() string {
	s := c.RPCURL
	if s == "" {
		return "(unset)"
	}
	scheme := ""
	for _, p := range []string{"https://", "http://"} {
		if rest, ok := strings.CutPrefix(s, p); ok {
			scheme, s = p, rest
			break
		}
	}
	host, _, _ := strings.Cut(s, "/")
	if host == "" {
		return "(malformed)"
	}
	return scheme + host + "/<redacted>"
}
