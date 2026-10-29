package rpc

import (
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy bounds how hard the client retries a failed call.
//
// Attempts are capped rather than unbounded because every attempt costs a paid
// request; a provider that is genuinely down should surface quickly and let the
// indexer retry on its next tick, which is already a natural backoff.
type RetryPolicy struct {
	// MaxAttempts counts the first try. 1 disables retrying.
	MaxAttempts int
	// BaseDelay is the first backoff step.
	BaseDelay time.Duration
	// MaxDelay caps the exponential growth.
	MaxDelay time.Duration
}

// DefaultRetry is tuned for a service whose indexer will retry anyway: a few
// quick attempts to ride out a blip, then give up and let the caller decide.
func DefaultRetry() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 4,
		BaseDelay:   200 * time.Millisecond,
		MaxDelay:    5 * time.Second,
	}
}

// Delay returns the wait before the given zero-based attempt index, using
// exponential backoff with **full jitter**: a uniform draw from [0, cap).
//
// Full jitter rather than a fixed or half-jittered delay because many requests
// fail together (one rate-limit window, one provider blip) and would otherwise
// retry in lockstep, reproducing the burst that caused the failure.
func (p RetryPolicy) Delay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	base := float64(p.BaseDelay)
	if base <= 0 {
		base = float64(100 * time.Millisecond)
	}
	backoff := base * math.Pow(2, float64(attempt))
	if max := float64(p.MaxDelay); max > 0 && backoff > max {
		backoff = max
	}
	if backoff <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(backoff)))
}

// attempts normalises MaxAttempts.
func (p RetryPolicy) attempts() int {
	if p.MaxAttempts < 1 {
		return 1
	}
	return p.MaxAttempts
}
