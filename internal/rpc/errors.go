package rpc

import (
	"errors"
	"fmt"
	"strings"
)

// Error is a JSON-RPC error object returned by the provider.
type Error struct {
	Method  string
	Code    int
	Message string

	// class is the behavioural category Classify assigned, which is what the
	// retry loop and the range-splitting caller branch on.
	class error
}

func (e *Error) Error() string {
	return fmt.Sprintf("rpc %s: %s (code %d)", e.Method, e.Message, e.Code)
}

// Unwrap exposes the behavioural class so callers can use errors.Is.
func (e *Error) Unwrap() error { return e.class }

// Behavioural error classes. Callers branch on these rather than on provider
// specific codes and message text, which differ between providers.
var (
	// ErrRateLimited means the provider throttled us. Retryable after a delay,
	// and it also shrinks the client-side token bucket so we settle below the
	// provider's limit instead of repeatedly bouncing off it.
	ErrRateLimited = errors.New("rate limited")

	// ErrUnavailable covers transport failures, timeouts and 5xx responses.
	// Retryable.
	ErrUnavailable = errors.New("provider unavailable")

	// ErrRangeTooLarge means the requested block range exceeds the provider's
	// cap. Measured on QuickNode: code -32614, "eth_getLogs is limited to a
	// 10,000 range". NOT retryable as-is — the caller must split the range.
	ErrRangeTooLarge = errors.New("block range exceeds provider limit")

	// ErrResultTooLarge means the response would exceed a result cap. The
	// measured endpoint has no such cap (365k logs in one response), but other
	// providers do, so range halving is kept as a fallback. Caller must split.
	ErrResultTooLarge = errors.New("result set exceeds provider limit")

	// ErrBeyondHead means the request referenced blocks the node does not have
	// yet. Measured: code -32602, "block range extends beyond current head
	// block" / "requested block range is in the future". This is why the tip
	// follower clamps to the known head: over-requesting is an error, not an
	// empty result.
	ErrBeyondHead = errors.New("requested range is beyond the chain head")

	// ErrPermanent is a request the provider will never accept: a bad method,
	// malformed params, or a rejected call. Retrying is pointless.
	ErrPermanent = errors.New("permanent rpc error")
)

// Standard and widely-used JSON-RPC error codes.
const (
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603
	codeRateLimited    = -32005 // not standard, but common across providers
	codeRangeLimit     = -32614 // QuickNode's eth_getLogs range cap
)

// classify maps a provider error onto a behavioural class.
//
// Message patterns are checked before codes, and each pattern is kept narrow,
// because neither signal is reliable alone:
//
//   - Code -32005 means "rate limited" on some providers and "too many results"
//     on others (Infura), which need opposite handling — back off versus split
//     the range.
//   - Code -32602 (invalid params) covers both genuinely malformed requests and
//     "you asked for blocks past my head".
//
// A broad pattern is actively dangerous here: "limit exceeded" matches "rate
// limit exceeded", which would turn a throttling response into an endless
// range-halving loop that never backs off.
func classify(code int, msg string) error {
	m := strings.ToLower(msg)

	// 1. Block-range cap. Measured on QuickNode: -32614, "eth_getLogs is
	//    limited to a 10,000 range".
	switch {
	case code == codeRangeLimit,
		strings.Contains(m, "is limited to a"),
		strings.Contains(m, "block range too large"),
		strings.Contains(m, "exceed maximum block range"),
		strings.Contains(m, "up to a") && strings.Contains(m, "block range"):
		return ErrRangeTooLarge
	}

	// 2. Result-set cap. Patterns are specific: each names a result or
	//    response size, never a bare "limit exceeded".
	switch {
	case strings.Contains(m, "query returned more than"),
		strings.Contains(m, "more than 10000 results"),
		strings.Contains(m, "response size exceeded"),
		strings.Contains(m, "result set too large"),
		strings.Contains(m, "too many logs"):
		return ErrResultTooLarge
	}

	// 3. Throttling.
	switch {
	case strings.Contains(m, "rate limit"),
		strings.Contains(m, "too many requests"),
		strings.Contains(m, "capacity exceeded"),
		strings.Contains(m, "throughput"),
		strings.Contains(m, "request limit"):
		return ErrRateLimited
	}

	// 4. The node does not hold what we asked for: past its head, or state it
	//    has pruned. Either way the caller must retarget, not retry.
	switch {
	case strings.Contains(m, "beyond current head"),
		strings.Contains(m, "range is in the future"),
		strings.Contains(m, "future block"),
		strings.Contains(m, "header not found"),
		strings.Contains(m, "missing trie node"):
		return ErrBeyondHead
	}

	// 5. Fall back to codes, now that the ambiguous messages are resolved.
	switch {
	case code == codeRateLimited:
		return ErrRateLimited
	case code == codeInternalError,
		strings.Contains(m, "timeout"),
		strings.Contains(m, "try again"):
		return ErrUnavailable
	case code == codeMethodNotFound, code == codeInvalidParams, code == codeInvalidRequest:
		return ErrPermanent
	}

	// Unknown provider errors are treated as permanent. Retrying something we
	// do not understand burns paid requests for no reason; the indexer's own
	// loop will retry the range on its next tick if it was transient.
	return ErrPermanent
}

// retryable reports whether another attempt could plausibly succeed without the
// caller changing the request.
func retryable(err error) bool {
	return errors.Is(err, ErrRateLimited) || errors.Is(err, ErrUnavailable)
}

// Retryable is the exported form, so the indexer can decide whether to retry a
// range now or on its next tick.
func Retryable(err error) bool { return retryable(err) }

// SplitRange reports whether the error means "the same request, but smaller".
// Both range and result caps are answered by halving the block range.
func SplitRange(err error) bool {
	return errors.Is(err, ErrRangeTooLarge) || errors.Is(err, ErrResultTooLarge)
}
