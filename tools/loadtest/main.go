// Command loadtest is the assignment's scaling claim, made measurable: N users
// each polling their tokens and balances once a period, against a running
// service, on real accounts.
//
// It reports each period separately, because the two numbers worth having are
// different periods: the first is a cold cache, and every later one is the steady
// state. The RPC columns come from the service's own /metrics, so they count what
// the provider was actually asked.
//
// Usage: RPC_URL=... go run ./tools/loadtest -users 5000 -duration 5m
//
// RPC_URL is only used to harvest accounts from recent logs. -accounts takes a
// file of addresses instead, one per line, and -save-accounts writes one.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const transferTopic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

func main() {
	base := flag.String("base", "http://localhost:8080", "service URL")
	chain := flag.String("chain", "ethereum", "chain slug")
	users := flag.Int("users", 5000, "simulated users")
	period := flag.Duration("period", time.Minute, "how often each user polls")
	duration := flag.Duration("duration", 5*time.Minute, "how long to run")
	limit := flag.Int("limit", 100, "page size")
	accountsFile := flag.String("accounts", "", "file of account addresses (default: harvest from RPC_URL)")
	saveAccounts := flag.String("save-accounts", "", "write the accounts used to this file")
	harvestBlocks := flag.Uint64("harvest-blocks", 50, "recent blocks to harvest accounts from")
	warmup := flag.Int("warmup", 1, "periods left out of the steady-state total while the cache fills")
	flag.Parse()

	accounts, err := loadAccounts(*accountsFile, *harvestBlocks, *users)
	if err != nil {
		log.Fatal(err)
	}
	if *saveAccounts != "" {
		if err := os.WriteFile(*saveAccounts, []byte(strings.Join(accounts, "\n")+"\n"), 0o644); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("%d users on %d accounts, one poll per %s for %s\n\n", *users, len(accounts), *period, *duration)

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        512,
			MaxIdleConnsPerHost: 512,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	rec := &recorder{}
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var wg sync.WaitGroup
	for i := range *users {
		url := fmt.Sprintf("%s/v1/chains/%s/accounts/%s/tokens?balances=true&limit=%d", *base, *chain, accounts[i%len(accounts)], *limit)
		wg.Go(func() { poll(ctx, client, url, *period, rec) })
	}

	report(ctx, *base, *period, *warmup, rec)
	wg.Wait()
}

// poll is one user: a random start inside the period so the load is even, then
// one request per period with a little jitter, revalidating what it last saw.
func poll(ctx context.Context, client *http.Client, url string, period time.Duration, rec *recorder) {
	wait := time.Duration(rand.Int64N(int64(period)))
	etag := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		etag = request(ctx, client, url, etag, rec)
		wait = period + time.Duration(rand.Int64N(int64(period)/10)) - period/20
	}
}

func request(ctx context.Context, client *http.Client, url, etag string, rec *recorder) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return etag
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			rec.record(sample{failed: true, latency: time.Since(start)})
		}
		return etag
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	s := sample{status: resp.StatusCode, latency: time.Since(start), bytes: len(body), failed: err != nil}
	if resp.StatusCode == http.StatusOK {
		var page struct {
			Tokens []struct {
				Balance *struct {
					Status string `json:"status"`
				} `json:"balance"`
			} `json:"tokens"`
		}
		if json.Unmarshal(body, &page) == nil {
			for _, t := range page.Tokens {
				s.tokens++
				if t.Balance != nil && t.Balance.Status != "ok" {
					s.degraded++
				}
			}
		}
		etag = resp.Header.Get("ETag")
	}
	rec.record(s)
	return etag
}

type sample struct {
	status   int
	latency  time.Duration
	bytes    int
	tokens   int
	degraded int
	failed   bool
}

type recorder struct {
	mu      sync.Mutex
	samples []sample
}

func (r *recorder) record(s sample) {
	r.mu.Lock()
	r.samples = append(r.samples, s)
	r.mu.Unlock()
}

func (r *recorder) drain() []sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.samples
	r.samples = nil
	return out
}

// report prints one row per period, then a total for the steady state: every
// period after the warm-up.
func report(ctx context.Context, base string, period time.Duration, warmup int, rec *recorder) {
	fmt.Printf("%-7s %7s %6s %6s %6s %6s %5s %7s %8s %8s %8s %6s\n",
		"period", "reqs", "rps", "p50", "p95", "p99", "fail", "304%", "tokens", "degraded", "eth_call", "getLogs")

	first, err := scrape(base)
	if err != nil {
		log.Fatalf("read /metrics: %v", err)
	}
	last := first
	var steady []sample
	var steadyStart metrics
	started := time.Now()
	for n := 1; ; n++ {
		end := false
		select {
		case <-ctx.Done():
			end = true
		case <-time.After(period):
		}
		samples := rec.drain()
		now, err := scrape(base)
		if err != nil {
			log.Printf("read /metrics: %v", err)
			now = last
		}
		printRow(strconv.Itoa(n), samples, period, now.sub(last))
		if n <= warmup {
			steadyStart = now
		} else {
			steady = append(steady, samples...)
		}
		last = now
		if end {
			break
		}
	}

	if len(steady) == 0 {
		return
	}
	elapsed := time.Since(started) - time.Duration(warmup)*period
	fmt.Println()
	printRow("steady", steady, elapsed, last.sub(steadyStart))
	delta := last.sub(steadyStart)
	fmt.Printf("\nsteady state: %.4f eth_call per request · %.0f bytes saved by 304s · cache hits %.0f, dirty %.0f, misses %.0f, degraded %.0f, coalesced %.0f\n",
		delta["eth_call"]/float64(len(steady)), delta["saved"],
		delta["hits"], delta["dirty"], delta["misses"], delta["degradedTokens"], delta["coalesced"])
}

func printRow(name string, samples []sample, span time.Duration, m metrics) {
	if len(samples) == 0 {
		fmt.Printf("%-7s no requests\n", name)
		return
	}
	lat := make([]time.Duration, 0, len(samples))
	var failed, notModified, tokens, degraded int
	for _, s := range samples {
		lat = append(lat, s.latency)
		if s.failed || s.status >= 500 {
			failed++
		}
		if s.status == http.StatusNotModified {
			notModified++
		}
		tokens += s.tokens
		degraded += s.degraded
	}
	slices.Sort(lat)
	at := func(p float64) string {
		return lat[min(int(float64(len(lat))*p), len(lat)-1)].Round(100 * time.Microsecond).String()
	}
	fmt.Printf("%-7s %7d %6.1f %6s %6s %6s %5d %6.1f%% %8d %8d %8.0f %6.0f\n",
		name, len(samples), float64(len(samples))/span.Seconds(),
		at(.5), at(.95), at(.99), failed,
		100*float64(notModified)/float64(len(samples)), tokens, degraded,
		m["eth_call"], m["eth_getLogs"])
}

// metrics is the handful of counters the report needs from /metrics.
type metrics map[string]float64

func (m metrics) sub(o metrics) metrics {
	out := make(metrics, len(m))
	for k, v := range m {
		out[k] = v - o[k]
	}
	return out
}

func scrape(base string) (metrics, error) {
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	out := metrics{}
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		name, value, ok := splitSample(line)
		if !ok {
			continue
		}
		// A call cancelled while queueing for the rate limiter never reached the
		// provider, and this report counts what the provider was asked.
		if strings.Contains(name, `outcome="canceled"`) {
			continue
		}
		switch {
		case strings.HasPrefix(name, `td_rpc_calls_total{method="eth_call"`):
			out["eth_call"] += value
		case strings.HasPrefix(name, `td_rpc_calls_total{method="eth_getLogs"`):
			out["eth_getLogs"] += value
		case name == "td_http_bytes_saved_by_304_total":
			out["saved"] = value
		case name == "td_balance_cache_hits_total":
			out["hits"] = value
		case name == "td_balance_cache_dirty_total":
			out["dirty"] = value
		case name == "td_balance_cache_misses_total":
			out["misses"] = value
		case name == "td_balance_degraded_total":
			out["degradedTokens"] = value
		case name == "td_balance_coalesced_total":
			out["coalesced"] = value
		}
	}
	return out, sc.Err()
}

func splitSample(line string) (string, float64, bool) {
	if strings.HasPrefix(line, "#") {
		return "", 0, false
	}
	i := strings.LastIndexByte(line, ' ')
	if i < 0 {
		return "", 0, false
	}
	v, err := strconv.ParseFloat(line[i+1:], 64)
	if err != nil {
		return "", 0, false
	}
	return line[:i], v, true
}

func loadAccounts(path string, blocks uint64, users int) ([]string, error) {
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		accounts := strings.Fields(string(raw))
		if len(accounts) == 0 {
			return nil, fmt.Errorf("%s holds no accounts", path)
		}
		return accounts, nil
	}
	url := os.Getenv("RPC_URL")
	if url == "" {
		return nil, fmt.Errorf("set RPC_URL to harvest accounts, or pass -accounts")
	}
	return harvest(url, blocks, users)
}

// harvest takes the recipients of recent transfers, so the accounts are real and
// present in the index. They lean toward the active: a production population has
// more dormant wallets, which only makes its steady state cheaper than this one.
func harvest(url string, blocks uint64, users int) ([]string, error) {
	var head string
	if err := call(url, &head, "eth_blockNumber"); err != nil {
		return nil, err
	}
	tip, err := strconv.ParseUint(strings.TrimPrefix(head, "0x"), 16, 64)
	if err != nil {
		return nil, err
	}
	// Back from the tip, so the harvest does not race a service still catching up.
	const margin = 20
	var logs []struct {
		Topics []string `json:"topics"`
	}
	err = call(url, &logs, "eth_getLogs", map[string]any{
		"fromBlock": fmt.Sprintf("0x%x", tip-margin-blocks),
		"toBlock":   fmt.Sprintf("0x%x", tip-margin),
		"topics":    []string{transferTopic0},
	})
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	var accounts []string
	for _, l := range logs {
		if len(l.Topics) < 3 || len(l.Topics[2]) != 66 {
			continue
		}
		a := "0x" + l.Topics[2][26:]
		if a == "0x0000000000000000000000000000000000000000" {
			continue
		}
		if _, dup := seen[a]; !dup {
			seen[a] = struct{}{}
			accounts = append(accounts, a)
		}
	}
	if len(accounts) == 0 {
		return nil, fmt.Errorf("harvested no accounts from %d blocks", blocks)
	}
	rand.Shuffle(len(accounts), func(i, j int) { accounts[i], accounts[j] = accounts[j], accounts[i] })
	if len(accounts) > users {
		accounts = accounts[:users]
	}
	return accounts, nil
}

func call(url string, out any, method string, params ...any) error {
	if params == nil {
		params = []any{}
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return err
	}
	// Go adds and undoes gzip itself as long as the header is not set by hand.
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		// A *url.Error quotes the URL, and this one carries the API key.
		return fmt.Errorf("%s: request failed", method)
	}
	defer resp.Body.Close()
	var env struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return fmt.Errorf("%s: decode: %w", method, err)
	}
	if env.Error != nil {
		return fmt.Errorf("%s: %s", method, env.Error.Message)
	}
	return json.Unmarshal(env.Result, out)
}
