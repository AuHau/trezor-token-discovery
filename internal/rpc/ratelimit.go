package rpc

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

// adaptiveLimiter is a token bucket that tightens when the provider pushes back
// and loosens again once calls are succeeding.
//
// A fixed bucket has to be tuned pessimistically, because the provider's real
// limit is unknown and varies by plan and method cost. Reacting to 429s instead
// means the client converges on whatever the provider actually allows and stops
// paying for rejected requests — a rejected call still costs a request.
type adaptiveLimiter struct {
	mu sync.Mutex

	lim *rate.Limiter

	base  float64 // configured ceiling; never exceeded
	cur   float64 // current rate
	floor float64 // never throttle below this, or we would deadlock progress

	// successes counts consecutive successful calls since the last penalty,
	// used to decide when it is safe to open up again.
	successes int
}

const (
	// penaltyFactor is how hard a 429 tightens the bucket. Halving converges
	// quickly; smaller steps spend several more rejections getting there.
	penaltyFactor = 0.5
	// recoveryFactor is how gently it reopens. Deliberately much slower than
	// the penalty: overshooting costs real money in rejected requests.
	recoveryFactor = 1.25
	// recoveryAfter is how many consecutive successes justify one step up.
	recoveryAfter = 20
	// floorFraction bounds how far below the configured rate a sustained
	// penalty can push us.
	floorFraction = 0.05
)

func newAdaptiveLimiter(ratePerSec float64, burst int) *adaptiveLimiter {
	floor := ratePerSec * floorFraction
	if floor <= 0 {
		floor = 0.1
	}
	return &adaptiveLimiter{
		lim:   rate.NewLimiter(rate.Limit(ratePerSec), burst),
		base:  ratePerSec,
		cur:   ratePerSec,
		floor: floor,
	}
}

// Wait blocks until a token is available or ctx is done.
func (a *adaptiveLimiter) Wait(ctx context.Context) error {
	a.mu.Lock()
	lim := a.lim
	a.mu.Unlock()
	return lim.Wait(ctx)
}

// Penalize tightens the bucket after the provider rejected a call.
func (a *adaptiveLimiter) Penalize() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.successes = 0
	next := a.cur * penaltyFactor
	if next < a.floor {
		next = a.floor
	}
	if next == a.cur {
		return
	}
	a.cur = next
	a.lim.SetLimit(rate.Limit(a.cur))
}

// Reward records a success and steps the rate back up once a streak shows the
// tighter rate is comfortable.
func (a *adaptiveLimiter) Reward() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cur >= a.base {
		return // already at the configured ceiling
	}
	a.successes++
	if a.successes < recoveryAfter {
		return
	}
	a.successes = 0
	next := a.cur * recoveryFactor
	if next > a.base {
		next = a.base
	}
	a.cur = next
	a.lim.SetLimit(rate.Limit(a.cur))
}

// Rate reports the current rate, for /status and metrics.
func (a *adaptiveLimiter) Rate() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cur
}
