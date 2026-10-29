package rpc

import (
	"errors"
	"strings"
	"testing"
)

// Classification is pure logic and was the source of a real bug: the pattern
// "limit exceeded" matched "rate limit exceeded", turning a throttling response
// into an endless range-halving loop that never backed off. These cases pin the
// distinctions down, especially where a provider code is ambiguous.
func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		code int
		msg  string
		want error
	}{
		// Measured against the real endpoint.
		{"quicknode range cap", -32614, "eth_getLogs is limited to a 10,000 range", ErrRangeTooLarge},
		{"beyond head", -32602, "block range extends beyond current head block", ErrBeyondHead},
		{"future range", -32602, "requested block range is in the future", ErrBeyondHead},

		// The -32005 collision: same code, opposite handling.
		{"32005 as rate limit", -32005, "Rate limit exceeded", ErrRateLimited},
		{"32005 as result cap", -32005, "query returned more than 10000 results", ErrResultTooLarge},

		// Other providers' wording.
		{"alchemy response size", -32602, "Log response size exceeded, retry with a smaller range", ErrResultTooLarge},
		{"too many logs", -32000, "too many logs in response", ErrResultTooLarge},
		{"throughput", -32000, "Your app has exceeded its throughput limit", ErrRateLimited},
		{"http-ish too many requests", -32000, "too many requests", ErrRateLimited},
		{"capacity", -32000, "capacity exceeded", ErrRateLimited},

		// Pruned state must retarget, not retry: the node will never have it.
		{"header not found", -32000, "header not found", ErrBeyondHead},
		{"missing trie node", -32000, "missing trie node 0xabc", ErrBeyondHead},

		// Transient.
		{"internal error", -32603, "internal error", ErrUnavailable},
		{"timeout text", -32000, "request timeout", ErrUnavailable},

		// Permanent.
		{"method not found", -32601, "method eth_nope does not exist", ErrPermanent},
		{"invalid params", -32602, "invalid argument 0", ErrPermanent},
		{"unknown", -31337, "something nobody has seen", ErrPermanent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classify(tc.code, tc.msg)
			if !errors.Is(got, tc.want) {
				t.Fatalf("classify(%d, %q) = %v, want %v", tc.code, tc.msg, got, tc.want)
			}
		})
	}
}

// The two helpers the indexer branches on must stay consistent with the classes:
// a class is either "wait and try again" or "change the request", never both.
func TestRetryableAndSplitRangeArePartitioned(t *testing.T) {
	cases := map[error]struct{ retryable, split bool }{
		ErrRateLimited:    {true, false},
		ErrUnavailable:    {true, false},
		ErrRangeTooLarge:  {false, true},
		ErrResultTooLarge: {false, true},
		ErrBeyondHead:     {false, false},
		ErrPermanent:      {false, false},
	}
	for class, want := range cases {
		err := &Error{Method: "eth_getLogs", Code: -1, Message: "x", class: class}
		if got := Retryable(err); got != want.retryable {
			t.Errorf("Retryable(%v) = %v, want %v", class, got, want.retryable)
		}
		if got := SplitRange(err); got != want.split {
			t.Errorf("SplitRange(%v) = %v, want %v", class, got, want.split)
		}
	}
}

func TestErrorMessageIncludesContext(t *testing.T) {
	err := &Error{Method: "eth_getLogs", Code: -32614, Message: "limited to a 10,000 range", class: ErrRangeTooLarge}
	got := err.Error()
	for _, want := range []string{"eth_getLogs", "-32614", "limited to a 10,000 range"} {
		if !strings.Contains(got, want) {
			t.Errorf("Error() = %q, missing %q", got, want)
		}
	}
}
