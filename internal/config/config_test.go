package config

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultIsValidWithRPCURL(t *testing.T) {
	// Default() plus the one required secret must be a working configuration;
	// anything else means a default is missing.
	cfg := Default()
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to require RPC_URL")
	}
	cfg.RPCURL = "https://example.invalid/key"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Default()+RPCURL should validate, got: %v", err)
	}
}

func TestValidateRejectsBadValues(t *testing.T) {
	tests := map[string]func(*Config){
		"empty rpc url":    func(c *Config) { c.RPCURL = "" },
		"non-http rpc url": func(c *Config) { c.RPCURL = "ws://example.invalid" },
		"empty data dir":   func(c *Config) { c.DataDir = "" },
		"zero start block": func(c *Config) { c.Chain.StartBlock = 0 },
		// 10,000 is the measured provider range cap; above it every request fails.
		"log range above provider cap": func(c *Config) { c.Chain.LogRangeBlocks = 10_001 },
		"zero log range":               func(c *Config) { c.Chain.LogRangeBlocks = 0 },
		"negative tip poll":            func(c *Config) { c.Chain.TipPollInterval = -1 },
		"tiny header ring":             func(c *Config) { c.Chain.HeaderRingSize = 1 },
		"zero multicall chunk":         func(c *Config) { c.Chain.MulticallChunk = 0 },
		"zero rpc rate":                func(c *Config) { c.Chain.RPCRate = 0 },
		"unknown watermark strategy":   func(c *Config) { c.Chain.Watermarks = "guesswork" },
		"no standards":                 func(c *Config) { c.Chain.Standards = nil },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := Default()
			cfg.RPCURL = "https://example.invalid/key"
			mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected %s to fail validation", name)
			}
		})
	}
}

func TestLoadReadsEnv(t *testing.T) {
	t.Setenv("RPC_URL", "https://example.invalid/secret")
	t.Setenv("LOG_FORMAT", "json")
	t.Setenv("CHAIN_LOG_RANGE_BLOCKS", "500")

	cfg, overrides, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.LogFormat != "json" {
		t.Errorf("LogFormat = %q, want json", cfg.LogFormat)
	}
	if cfg.Chain.LogRangeBlocks != 500 {
		t.Errorf("LogRangeBlocks = %d, want 500", cfg.Chain.LogRangeBlocks)
	}
	// Overrides are logged at startup, so the RPC URL must never appear there.
	joined := strings.Join(overrides, " ")
	if strings.Contains(joined, "secret") || strings.Contains(joined, "RPC_URL") {
		t.Errorf("overrides leaked the RPC URL: %q", joined)
	}
}

func TestLoadReportsBadEnv(t *testing.T) {
	t.Setenv("RPC_URL", "https://example.invalid/key")
	t.Setenv("LOG_LEVEL", "chatty")
	if _, _, err := Load(); err == nil {
		t.Fatal("expected an invalid LOG_LEVEL to be reported")
	}
}

func TestRedactedRPCURL(t *testing.T) {
	tests := []struct{ in, want string }{
		// The credential lives in the path for QuickNode-style endpoints, so
		// everything after the host is dropped rather than masked selectively.
		{"https://example-node.ethereum-mainnet.provider.example/abc123secret",
			"https://example-node.ethereum-mainnet.provider.example/<redacted>"},
		{"https://host.example/a/b/c?key=zzz", "https://host.example/<redacted>"},
		{"http://localhost:8545", "http://localhost:8545/<redacted>"},
		{"", "(unset)"},
	}
	for _, tc := range tests {
		cfg := Config{RPCURL: tc.in}
		if got := cfg.RedactedRPCURL(); got != tc.want {
			t.Errorf("RedactedRPCURL(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if tc.in != "" && strings.Contains(cfg.RedactedRPCURL(), "secret") {
			t.Errorf("credential survived redaction: %q", cfg.RedactedRPCURL())
		}
	}
}

func TestStallThresholdClearsHealthyFinalityLag(t *testing.T) {
	// A healthy chain must never trip the fallback, which replaces finality
	// with a guess. The healthy lag is the chain's own finality delay plus how
	// far the cached tag falls behind while it is cached.
	tests := map[string]struct {
		finalityLag uint64 // healthy latest-minus-finalized, in blocks
		blockTime   time.Duration
	}{
		// Two epochs.
		"ethereum": {finalityLag: 96, blockTime: 12 * time.Second},
		// Measured ~1,700 blocks behind (~4 h) between L1 proofs. Doubled,
		// because one sample is not a distribution. 2 s is Linea's nominal
		// block time; the measured ~9 s would make the cache term smaller.
		"linea": {finalityLag: 2 * 1_720, blockTime: 2 * time.Second},
	}
	for _, slug := range Presets() {
		tc, ok := tests[slug]
		if !ok {
			t.Fatalf("preset %q has no healthy finality lag recorded here", slug)
		}
		ch, _ := Preset(slug)
		healthy := tc.finalityLag + uint64(ch.WatermarkPollInterval/tc.blockTime)
		if ch.FinalityStallBlocks <= healthy {
			t.Errorf("%s: stall threshold %d is within healthy lag %d", slug, ch.FinalityStallBlocks, healthy)
		}
	}
}

func TestPresetsAreValidAndDistinct(t *testing.T) {
	slugs := map[string]bool{}
	ids := map[uint64]bool{}
	for _, slug := range Presets() {
		ch, ok := Preset(slug)
		if !ok {
			t.Fatalf("Presets lists %q but Preset does not know it", slug)
		}
		cfg := Default()
		cfg.RPCURL = "https://example.invalid/key"
		cfg.Chain = ch
		if err := cfg.Validate(); err != nil {
			t.Errorf("%s: %v", slug, err)
		}
		// The registry would reject these anyway, but only at boot.
		if slugs[ch.Slug] || ids[ch.ChainID] {
			t.Errorf("%s: duplicate slug or chain id", slug)
		}
		slugs[ch.Slug], ids[ch.ChainID] = true, true
	}
}

func TestLoadSelectsChain(t *testing.T) {
	t.Setenv("RPC_URL", "https://example.invalid/key")
	t.Setenv("CHAIN", "Linea")
	t.Setenv("CHAIN_LOG_RANGE_BLOCKS", "500")

	cfg, _, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Chain.Slug != "linea" || cfg.Chain.ChainID != 59144 {
		t.Errorf("chain = %s/%d, want linea/59144", cfg.Chain.Slug, cfg.Chain.ChainID)
	}
	// Per-value overrides apply on top of the preset, not underneath it.
	if cfg.Chain.LogRangeBlocks != 500 {
		t.Errorf("LogRangeBlocks = %d, want the override 500", cfg.Chain.LogRangeBlocks)
	}
	// Selecting a chain must never default into another chain's store.
	if cfg.DataDir == Default().DataDir {
		t.Errorf("linea defaulted into the ethereum store %q", cfg.DataDir)
	}
}

func TestLoadKeepsExplicitDataDir(t *testing.T) {
	t.Setenv("RPC_URL", "https://example.invalid/key")
	t.Setenv("CHAIN", "linea")
	t.Setenv("DATA_DIR", "/srv/linea")

	cfg, _, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DataDir != "/srv/linea" {
		t.Errorf("DataDir = %q, want the explicit /srv/linea", cfg.DataDir)
	}
}

func TestLoadRejectsUnknownChain(t *testing.T) {
	t.Setenv("RPC_URL", "https://example.invalid/key")
	t.Setenv("CHAIN", "dogechain")
	if _, _, err := Load(); err == nil {
		t.Fatal("expected an unknown CHAIN to be reported")
	}
}
