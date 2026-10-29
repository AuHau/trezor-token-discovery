// Package rpc is the JSON-RPC transport: one endpoint, plus the reliability
// behaviour the assignment asks about — rate limiting, retries with backoff,
// per-method deadlines, and classification of provider-specific failures.
//
// It deliberately knows nothing about Ethereum semantics. Method wrappers and
// log decoding live in internal/chains/evm, so this package stays usable by any
// JSON-RPC chain.
//
// Single endpoint by design: a provider pool is the highest-value hardening
// step but it is a subsystem, and with one node a reported chain tip is just
// that node's opinion of itself. See docs/limits.md.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"tokendiscovery/internal/obs"
)

// maxResponseBytes bounds a single response. Measured: a 200-block eth_getLogs
// is ~73 MB decoded, and an accidental 10,000-block request would be ~3.6 GB,
// so an explicit ceiling turns a memory blow-up into a clean error.
const maxResponseBytes = 512 << 20 // 512 MiB

// Options configures a Client.
type Options struct {
	URL string

	// Rate and Burst size the client-side token bucket. The bucket adapts
	// downward on 429s, so Rate is a ceiling rather than a target.
	Rate  float64
	Burst int

	// Timeout is the default per-call deadline.
	Timeout time.Duration

	// MethodTimeouts overrides Timeout per method. eth_getLogs moves tens of
	// megabytes and needs far longer than eth_blockNumber.
	MethodTimeouts map[string]time.Duration

	Retry   RetryPolicy
	Metrics *obs.Metrics
	Logger  *slog.Logger

	// HTTPClient is injectable for tests. If nil a client with a tuned
	// transport is built.
	HTTPClient *http.Client

	// sleep is injectable so tests exercise backoff without real delays.
	sleep func(context.Context, time.Duration) error
}

// Client is a JSON-RPC client over HTTP. Safe for concurrent use.
type Client struct {
	url     string
	hc      *http.Client
	lim     *adaptiveLimiter
	retry   RetryPolicy
	rec     recorder
	log     *slog.Logger
	timeout time.Duration
	perMeth map[string]time.Duration
	sleep   func(context.Context, time.Duration) error

	nextID atomic.Uint64

	mu          sync.Mutex
	lastErr     string
	lastErrAt   time.Time
	consecutive int
}

// New builds a Client.
func New(o Options) (*Client, error) {
	if o.URL == "" {
		return nil, errors.New("rpc: URL is required")
	}
	if o.Rate <= 0 {
		return nil, errors.New("rpc: Rate must be positive")
	}
	if o.Burst <= 0 {
		o.Burst = 1
	}
	if o.Timeout <= 0 {
		o.Timeout = 60 * time.Second
	}
	if o.Retry.MaxAttempts == 0 {
		o.Retry = DefaultRetry()
	}
	hc := o.HTTPClient
	if hc == nil {
		// No Timeout on the http.Client: deadlines come from the per-call
		// context, so a slow method does not inherit a global limit.
		hc = &http.Client{Transport: defaultTransport()}
	}
	sleep := o.sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	log := o.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Client{
		url:     o.URL,
		hc:      hc,
		lim:     newAdaptiveLimiter(o.Rate, o.Burst),
		retry:   o.Retry,
		rec:     recorder{m: o.Metrics},
		log:     log,
		timeout: o.Timeout,
		perMeth: o.MethodTimeouts,
		sleep:   sleep,
	}, nil
}

func defaultTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	// The backfill is one long-lived sequential stream against a single host,
	// so a small warm pool is all that is needed.
	t.MaxIdleConnsPerHost = 8
	t.IdleConnTimeout = 90 * time.Second
	// DisableCompression stays false on purpose. The transport then adds
	// Accept-Encoding: gzip itself and decompresses transparently, which was
	// measured at 14.5x smaller and 12x faster on eth_getLogs. Setting that
	// header by hand would silently make us responsible for decompression.
	return t
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type request struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// Call invokes a JSON-RPC method and decodes the result into out, which may be
// nil to discard it.
//
// It retries transient failures with exponential backoff and full jitter, and
// returns a typed error for the cases a caller must act on: SplitRange(err) for
// a request that must be made smaller, errors.Is(err, ErrBeyondHead) for one
// that must be retargeted.
func (c *Client) Call(ctx context.Context, out any, method string, params ...any) error {
	if params == nil {
		params = []any{}
	}
	body, err := json.Marshal(request{
		JSONRPC: "2.0",
		ID:      c.nextID.Add(1),
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return fmt.Errorf("rpc %s: encode request: %w", method, err)
	}

	attempts := c.retry.attempts()
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			delay := c.retry.Delay(attempt - 1)
			c.log.Debug("retrying rpc call",
				"method", method, "attempt", attempt+1, "of", attempts,
				"delay", delay, "cause", lastErr)
			if err := c.sleep(ctx, delay); err != nil {
				return errors.Join(lastErr, err)
			}
		}

		raw, err := c.attempt(ctx, method, body)
		if err == nil {
			c.succeeded()
			if out == nil {
				return nil
			}
			if err := json.Unmarshal(raw, out); err != nil {
				return fmt.Errorf("rpc %s: decode result: %w", method, err)
			}
			return nil
		}

		lastErr = err
		c.failed(err)

		// Stop immediately when the caller's context is done, or when another
		// identical attempt cannot help.
		if ctx.Err() != nil {
			return errors.Join(err, ctx.Err())
		}
		if !retryable(err) {
			return err
		}
	}
	return fmt.Errorf("rpc %s: giving up after %d attempts: %w", method, attempts, lastErr)
}

// attempt performs one request/response cycle.
func (c *Client) attempt(ctx context.Context, method string, body []byte) (json.RawMessage, error) {
	// Throttle before spending a request. Blocking here is the point: it is
	// what keeps us below the provider's limit rather than discovering it.
	if err := c.lim.Wait(ctx); err != nil {
		c.rec.call(method, outcomeCanceled, 0, 0)
		return nil, fmt.Errorf("rpc %s: %w", method, err)
	}

	timeout := c.timeout
	if d, ok := c.perMeth[method]; ok && d > 0 {
		timeout = d
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	raw, size, err := c.do(callCtx, method, body)
	dur := time.Since(start)

	if err != nil {
		c.rec.call(method, outcomeFor(err), dur, size)
		if is(err, ErrRateLimited) {
			// Tighten the bucket so the next attempt is not another rejection.
			c.lim.Penalize()
		}
		return nil, err
	}
	c.rec.call(method, outcomeOK, dur, size)
	c.lim.Reward()
	return raw, nil
}

func (c *Client) do(ctx context.Context, method string, body []byte) (json.RawMessage, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, 0, fmt.Errorf("rpc %s: build request: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	// Accept-Encoding is deliberately NOT set; see defaultTransport.

	resp, err := c.hc.Do(req)
	if err != nil {
		// Transport failures (DNS, refused, reset, timeout) are all worth
		// another attempt.
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return nil, 0, fmt.Errorf("rpc %s: %w: %w", method, ErrUnavailable, err)
		}
		return nil, 0, fmt.Errorf("rpc %s: %w: %w", method, ErrUnavailable, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		resp.Body.Close()
	}()

	if err := httpStatusError(method, resp); err != nil {
		return nil, 0, err
	}

	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return nil, 0, fmt.Errorf("rpc %s: %w: read body: %w", method, ErrUnavailable, err)
	}
	if len(payload) > maxResponseBytes {
		return nil, len(payload), fmt.Errorf("rpc %s: response exceeds %d bytes: %w",
			method, maxResponseBytes, ErrPermanent)
	}

	var env response
	if err := json.Unmarshal(payload, &env); err != nil {
		// Malformed bodies are usually a proxy or gateway page rather than the
		// node, so treat them as transient.
		return nil, len(payload), fmt.Errorf("rpc %s: %w: decode envelope: %w",
			method, ErrUnavailable, err)
	}
	if env.Error != nil {
		return nil, len(payload), &Error{
			Method:  method,
			Code:    env.Error.Code,
			Message: env.Error.Message,
			class:   classify(env.Error.Code, env.Error.Message),
		}
	}
	if len(env.Result) == 0 {
		return nil, len(payload), fmt.Errorf("rpc %s: response had neither result nor error: %w",
			method, ErrUnavailable)
	}
	return env.Result, len(payload), nil
}

// httpStatusError maps a non-2xx response onto a behavioural class.
func httpStatusError(method string, resp *http.Response) error {
	switch {
	case resp.StatusCode == http.StatusOK:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests:
		return fmt.Errorf("rpc %s: http 429 (retry-after %s): %w",
			method, retryAfter(resp), ErrRateLimited)

	case resp.StatusCode == http.StatusRequestEntityTooLarge:
		// 413 on a tiny request body means the provider is rejecting the
		// *response* size or the span being asked for, before it ever produces
		// a JSON-RPC error. Measured: a 10k-block over-request returns a
		// -32614 JSON error, but a 200k-block one returns a bare 413. The
		// correct response to both is to make the request smaller, so it must
		// not fall through to the permanent branch.
		return fmt.Errorf("rpc %s: http 413 (request spans too much data): %w",
			method, ErrResultTooLarge)

	case resp.StatusCode == http.StatusRequestTimeout:
		return fmt.Errorf("rpc %s: http 408: %w", method, ErrUnavailable)

	case resp.StatusCode >= 500:
		return fmt.Errorf("rpc %s: http %d: %w", method, resp.StatusCode, ErrUnavailable)
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		// A bad or revoked API key. Retrying cannot fix it and would just burn
		// requests, so fail loudly.
		return fmt.Errorf("rpc %s: http %d (check the endpoint credential): %w",
			method, resp.StatusCode, ErrPermanent)
	default:
		return fmt.Errorf("rpc %s: http %d: %w", method, resp.StatusCode, ErrPermanent)
	}
}

func retryAfter(resp *http.Response) string {
	if v := resp.Header.Get("Retry-After"); v != "" {
		if _, err := strconv.Atoi(v); err == nil {
			return v + "s"
		}
		return v
	}
	return "unset"
}

func (c *Client) succeeded() {
	c.mu.Lock()
	c.consecutive = 0
	c.mu.Unlock()
}

func (c *Client) failed(err error) {
	c.mu.Lock()
	c.consecutive++
	c.lastErr = err.Error()
	c.lastErrAt = time.Now()
	c.mu.Unlock()
}

// Stats is a snapshot of client health, surfaced by /status.
type Stats struct {
	Rate                float64
	ConsecutiveFailures int
	LastError           string
	LastErrorAt         time.Time
}

// Stats reports current client health.
func (c *Client) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Rate:                c.lim.Rate(),
		ConsecutiveFailures: c.consecutive,
		LastError:           c.lastErr,
		LastErrorAt:         c.lastErrAt,
	}
}

// is is errors.Is, named short because the error paths use it heavily.
func is(err, target error) bool { return errors.Is(err, target) }
