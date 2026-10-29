package rpc

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"tokendiscovery/internal/obs"
)

// fakeRPC is a programmable JSON-RPC server. Each handler gets the attempt
// number so a test can script a sequence like "429, 429, then succeed".
type fakeRPC struct {
	*httptest.Server
	calls atomic.Int64

	// acceptEncoding records what the client sent, so a test can prove the
	// client leaves compression to the transport.
	acceptEncoding atomic.Value
}

func newFakeRPC(t *testing.T, h func(w http.ResponseWriter, r *http.Request, attempt int)) *fakeRPC {
	t.Helper()
	f := &fakeRPC{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(f.calls.Add(1))
		f.acceptEncoding.Store(r.Header.Get("Accept-Encoding"))
		h(w, r, n)
	}))
	t.Cleanup(f.Close)
	return f
}

func okResult(w http.ResponseWriter, result string) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":%s}`, result)
}

func rpcErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"error":{"code":%d,"message":%q}}`, code, msg)
}

// newTestClient builds a client with no real sleeping, so retry tests are fast
// and deterministic.
func newTestClient(t *testing.T, url string, mutate ...func(*Options)) *Client {
	t.Helper()
	o := Options{
		URL:     url,
		Rate:    1000,
		Burst:   1000,
		Timeout: 2 * time.Second,
		Retry:   RetryPolicy{MaxAttempts: 4, BaseDelay: time.Microsecond, MaxDelay: time.Millisecond},
		sleep:   func(context.Context, time.Duration) error { return nil },
	}
	for _, m := range mutate {
		m(&o)
	}
	c, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestCallDecodesResult(t *testing.T) {
	f := newFakeRPC(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		okResult(w, `"0x1914c06"`)
	})
	c := newTestClient(t, f.URL)

	var got string
	if err := c.Call(context.Background(), &got, "eth_blockNumber"); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if got != "0x1914c06" {
		t.Errorf("result = %q, want 0x1914c06", got)
	}
}

func TestCallDiscardsResultWhenOutIsNil(t *testing.T) {
	f := newFakeRPC(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		okResult(w, `{"big":"payload"}`)
	})
	c := newTestClient(t, f.URL)
	if err := c.Call(context.Background(), nil, "eth_whatever"); err != nil {
		t.Fatalf("Call with nil out: %v", err)
	}
}

// Compression is the single biggest measured win (14.5x smaller, 12x faster on
// eth_getLogs), and it works only because the client leaves Accept-Encoding to
// the transport. This test fails if anyone "helpfully" sets that header.
func TestTransparentGzip(t *testing.T) {
	f := newFakeRPC(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			t.Error("client did not advertise gzip; transparent decompression is lost")
			okResult(w, `"uncompressed"`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		zw := gzip.NewWriter(w)
		defer zw.Close()
		fmt.Fprint(zw, `{"jsonrpc":"2.0","id":1,"result":"compressed"}`)
	})
	c := newTestClient(t, f.URL)

	var got string
	if err := c.Call(context.Background(), &got, "eth_getLogs"); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if got != "compressed" {
		t.Errorf("result = %q; gzip response was not transparently decoded", got)
	}
	if ae, _ := f.acceptEncoding.Load().(string); !strings.Contains(ae, "gzip") {
		t.Errorf("Accept-Encoding = %q, want it set by the transport", ae)
	}
}

func TestRetriesTransientFailures(t *testing.T) {
	tests := []struct {
		name  string
		fail  func(w http.ResponseWriter)
		class error
	}{
		{"http 429", func(w http.ResponseWriter) {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
		}, ErrRateLimited},
		{"http 500", func(w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) }, ErrUnavailable},
		{"http 503", func(w http.ResponseWriter) { w.WriteHeader(http.StatusServiceUnavailable) }, ErrUnavailable},
		{"rpc rate limit code", func(w http.ResponseWriter) { rpcErr(w, -32005, "Rate limit exceeded") }, ErrRateLimited},
		{"rpc internal error", func(w http.ResponseWriter) { rpcErr(w, -32603, "internal error") }, ErrUnavailable},
		{"garbage body", func(w http.ResponseWriter) { fmt.Fprint(w, "<html>gateway timeout</html>") }, ErrUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeRPC(t, func(w http.ResponseWriter, r *http.Request, attempt int) {
				if attempt < 3 {
					tc.fail(w)
					return
				}
				okResult(w, `"recovered"`)
			})
			c := newTestClient(t, f.URL)

			var got string
			if err := c.Call(context.Background(), &got, "eth_call"); err != nil {
				t.Fatalf("Call should have recovered: %v", err)
			}
			if got != "recovered" {
				t.Errorf("result = %q, want recovered", got)
			}
			if n := f.calls.Load(); n != 3 {
				t.Errorf("attempts = %d, want 3", n)
			}
		})
	}
}

// A slow provider must be bounded by the per-method deadline rather than
// hanging the caller, and must be retried once it recovers.
func TestPerMethodTimeoutAndRecovery(t *testing.T) {
	f := newFakeRPC(t, func(w http.ResponseWriter, r *http.Request, attempt int) {
		if attempt == 1 {
			time.Sleep(300 * time.Millisecond) // exceeds the method deadline
		}
		okResult(w, `"fast enough"`)
	})
	c := newTestClient(t, f.URL, func(o *Options) {
		o.MethodTimeouts = map[string]time.Duration{"eth_getLogs": 40 * time.Millisecond}
	})

	var got string
	if err := c.Call(context.Background(), &got, "eth_getLogs"); err != nil {
		t.Fatalf("Call should have recovered after the slow attempt: %v", err)
	}
	if got != "fast enough" {
		t.Errorf("result = %q", got)
	}
	if n := f.calls.Load(); n < 2 {
		t.Errorf("attempts = %d, want the slow attempt to be retried", n)
	}
}

// Errors the caller must act on are returned immediately: another identical
// attempt cannot succeed, and each one costs a paid request.
func TestNonRetryableErrorsFailFast(t *testing.T) {
	tests := []struct {
		name       string
		respond    func(w http.ResponseWriter)
		class      error
		splitRange bool
	}{
		// Measured on the real endpoint.
		{"range cap", func(w http.ResponseWriter) {
			rpcErr(w, -32614, "eth_getLogs is limited to a 10,000 range")
		}, ErrRangeTooLarge, true},
		{"beyond head", func(w http.ResponseWriter) {
			rpcErr(w, -32602, "block range extends beyond current head block")
		}, ErrBeyondHead, false},
		{"future range", func(w http.ResponseWriter) {
			rpcErr(w, -32602, "requested block range is in the future")
		}, ErrBeyondHead, false},
		// Other providers do impose result caps, so halving stays supported.
		{"result cap", func(w http.ResponseWriter) {
			rpcErr(w, -32005, "query returned more than 10000 results")
		}, ErrResultTooLarge, true},
		{"state pruned", func(w http.ResponseWriter) {
			rpcErr(w, -32000, "missing trie node")
		}, ErrBeyondHead, false},
		{"method not found", func(w http.ResponseWriter) {
			rpcErr(w, -32601, "the method does not exist")
		}, ErrPermanent, false},
		{"bad credential", func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusUnauthorized)
		}, ErrPermanent, false},
		// Found against the real endpoint: a very large range is rejected with
		// a bare HTTP 413 rather than a JSON-RPC error. It must be splittable,
		// not permanent, or the indexer gives up instead of halving.
		{"http 413 oversized", func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		}, ErrResultTooLarge, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeRPC(t, func(w http.ResponseWriter, r *http.Request, _ int) { tc.respond(w) })
			c := newTestClient(t, f.URL)

			err := c.Call(context.Background(), nil, "eth_getLogs")
			if !errors.Is(err, tc.class) {
				t.Fatalf("error = %v, want class %v", err, tc.class)
			}
			if got := SplitRange(err); got != tc.splitRange {
				t.Errorf("SplitRange = %v, want %v", got, tc.splitRange)
			}
			if Retryable(err) {
				t.Error("error should not be retryable")
			}
			if n := f.calls.Load(); n != 1 {
				t.Errorf("attempts = %d, want exactly 1 (no wasted requests)", n)
			}
		})
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	f := newFakeRPC(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		w.WriteHeader(http.StatusBadGateway)
	})
	c := newTestClient(t, f.URL, func(o *Options) {
		o.Retry = RetryPolicy{MaxAttempts: 3, BaseDelay: time.Microsecond, MaxDelay: time.Millisecond}
	})

	err := c.Call(context.Background(), nil, "eth_blockNumber")
	if err == nil {
		t.Fatal("expected failure")
	}
	if !strings.Contains(err.Error(), "giving up after 3 attempts") {
		t.Errorf("error should report the attempt budget, got: %v", err)
	}
	if n := f.calls.Load(); n != 3 {
		t.Errorf("attempts = %d, want 3", n)
	}
}

func TestContextCancellationStopsRetrying(t *testing.T) {
	f := newFakeRPC(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	// A real sleep here, so cancelling mid-backoff is what ends the loop.
	c := newTestClient(t, f.URL, func(o *Options) {
		o.sleep = sleepCtx
		o.Retry = RetryPolicy{MaxAttempts: 50, BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := c.Call(ctx, nil, "eth_blockNumber")
	if err == nil {
		t.Fatal("expected failure")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error should wrap the context cause, got: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %v; cancellation should end the retry loop promptly", elapsed)
	}
}

func TestEmptyResponseIsTransient(t *testing.T) {
	f := newFakeRPC(t, func(w http.ResponseWriter, r *http.Request, attempt int) {
		if attempt == 1 {
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1}`) // neither result nor error
			return
		}
		okResult(w, `"ok"`)
	})
	c := newTestClient(t, f.URL)
	var got string
	if err := c.Call(context.Background(), &got, "eth_blockNumber"); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if got != "ok" {
		t.Errorf("result = %q", got)
	}
}

// A 429 must not only be retried, it must tighten the bucket — otherwise the
// next call walks into the same rejection and pays for it.
func TestRateLimitTightensTheBucket(t *testing.T) {
	f := newFakeRPC(t, func(w http.ResponseWriter, r *http.Request, attempt int) {
		if attempt == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		okResult(w, `"ok"`)
	})
	c := newTestClient(t, f.URL, func(o *Options) { o.Rate = 100 })

	before := c.Stats().Rate
	if err := c.Call(context.Background(), nil, "eth_call"); err != nil {
		t.Fatalf("Call: %v", err)
	}
	after := c.Stats().Rate
	if after >= before {
		t.Errorf("rate %v -> %v; a 429 should reduce the client-side rate", before, after)
	}
}

func TestStatsTracksLastError(t *testing.T) {
	f := newFakeRPC(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		rpcErr(w, -32601, "nope")
	})
	c := newTestClient(t, f.URL)

	if s := c.Stats(); s.LastError != "" || s.ConsecutiveFailures != 0 {
		t.Fatalf("fresh client should have clean stats, got %+v", s)
	}
	_ = c.Call(context.Background(), nil, "eth_nope")

	s := c.Stats()
	if s.ConsecutiveFailures != 1 {
		t.Errorf("ConsecutiveFailures = %d, want 1", s.ConsecutiveFailures)
	}
	if !strings.Contains(s.LastError, "nope") {
		t.Errorf("LastError = %q, want it to mention the provider message", s.LastError)
	}
	if s.LastErrorAt.IsZero() {
		t.Error("LastErrorAt not set")
	}
}

func TestMetricsRecordOutcomes(t *testing.T) {
	f := newFakeRPC(t, func(w http.ResponseWriter, r *http.Request, attempt int) {
		if attempt == 1 {
			rpcErr(w, -32614, "eth_getLogs is limited to a 10,000 range")
			return
		}
		okResult(w, `"ok"`)
	})
	m := obs.NewMetrics()
	c := newTestClient(t, f.URL, func(o *Options) { o.Metrics = m })

	_ = c.Call(context.Background(), nil, "eth_getLogs") // range_cap
	_ = c.Call(context.Background(), nil, "eth_getLogs") // ok

	if got := counterValue(t, m, "td_rpc_calls_total", "eth_getLogs", outcomeRangeCap); got != 1 {
		t.Errorf("range_cap count = %v, want 1", got)
	}
	if got := counterValue(t, m, "td_rpc_calls_total", "eth_getLogs", outcomeOK); got != 1 {
		t.Errorf("ok count = %v, want 1", got)
	}
}

func counterValue(t *testing.T, m *obs.Metrics, name string, labels ...string) float64 {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			var got []string
			for _, lp := range metric.GetLabel() {
				got = append(got, lp.GetValue())
			}
			if strings.Join(got, "|") == strings.Join(labels, "|") {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestNewValidatesOptions(t *testing.T) {
	if _, err := New(Options{Rate: 1}); err == nil {
		t.Error("expected an error without a URL")
	}
	if _, err := New(Options{URL: "http://x", Rate: 0}); err == nil {
		t.Error("expected an error without a positive rate")
	}
}
