package rpc

import "testing"

func TestPenalizeHalvesDownToFloor(t *testing.T) {
	a := newAdaptiveLimiter(100, 10)
	if got := a.Rate(); got != 100 {
		t.Fatalf("initial rate = %v, want 100", got)
	}

	a.Penalize()
	if got := a.Rate(); got != 50 {
		t.Errorf("after one penalty = %v, want 50", got)
	}

	// Sustained pushback must not throttle to zero, or the indexer would stall
	// permanently instead of crawling.
	for range 50 {
		a.Penalize()
	}
	floor := 100 * floorFraction
	if got := a.Rate(); got < floor {
		t.Errorf("rate %v fell below the floor %v", got, floor)
	}
	if a.Rate() <= 0 {
		t.Error("rate must stay positive")
	}
}

func TestRewardRecoversSlowlyAndNeverExceedsCeiling(t *testing.T) {
	a := newAdaptiveLimiter(100, 10)
	a.Penalize() // 50

	// Recovery is deliberately gated on a success streak: stepping up on every
	// single success would oscillate straight back into the provider's limit.
	for range recoveryAfter - 1 {
		a.Reward()
	}
	if got := a.Rate(); got != 50 {
		t.Errorf("rate = %v; recovery should need a full streak", got)
	}
	a.Reward()
	if got := a.Rate(); got <= 50 {
		t.Errorf("rate = %v; a completed streak should step up", got)
	}

	// The configured rate is a ceiling.
	for range 10000 {
		a.Reward()
	}
	if got := a.Rate(); got > 100 {
		t.Errorf("rate = %v exceeded the configured ceiling of 100", got)
	}
}

func TestRewardIsNoopAtCeiling(t *testing.T) {
	a := newAdaptiveLimiter(100, 10)
	for range 100 {
		a.Reward()
	}
	if got := a.Rate(); got != 100 {
		t.Errorf("rate = %v, want it pinned at the ceiling", got)
	}
}
