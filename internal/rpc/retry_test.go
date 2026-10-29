package rpc

import (
	"testing"
	"time"
)

func TestDelayUsesFullJitterWithinBounds(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 6, BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}

	for attempt := range 6 {
		cap := min(time.Duration(float64(p.BaseDelay)*pow2(attempt)), p.MaxDelay)
		var sawBelowHalf bool
		for range 200 {
			d := p.Delay(attempt)
			if d < 0 || d >= cap {
				t.Fatalf("attempt %d: delay %v outside [0,%v)", attempt, d, cap)
			}
			if d < cap/2 {
				sawBelowHalf = true
			}
		}
		// Full jitter draws from [0,cap), so low values must occur. Half
		// jitter would never produce them, and many callers failing together
		// would then retry in lockstep and reproduce the original burst.
		if !sawBelowHalf {
			t.Errorf("attempt %d: never drew below cap/2; jitter looks wrong", attempt)
		}
	}
}

func TestDelayIsCapped(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 40, BaseDelay: time.Second, MaxDelay: 2 * time.Second}
	for attempt := range 40 {
		if d := p.Delay(attempt); d >= 2*time.Second {
			t.Fatalf("attempt %d: delay %v exceeded MaxDelay", attempt, d)
		}
	}
}

func TestAttemptsNormalised(t *testing.T) {
	for _, tc := range []struct{ in, want int }{{0, 1}, {-5, 1}, {1, 1}, {4, 4}} {
		if got := (RetryPolicy{MaxAttempts: tc.in}).attempts(); got != tc.want {
			t.Errorf("attempts(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func pow2(n int) float64 {
	out := 1.0
	for range n {
		out *= 2
	}
	return out
}
