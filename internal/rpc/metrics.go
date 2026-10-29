package rpc

import (
	"time"

	"tokendiscovery/internal/obs"
)

// recorder funnels call outcomes into the metric set, tolerating a nil Metrics
// so tests and tools can construct a client without observability.
type recorder struct{ m *obs.Metrics }

// Outcome labels. Kept to a small closed set so the metric stays low
// cardinality — provider message text must never reach a label.
const (
	outcomeOK          = "ok"
	outcomeRateLimited = "rate_limited"
	outcomeUnavailable = "unavailable"
	outcomeRangeCap    = "range_cap"
	outcomeBeyondHead  = "beyond_head"
	outcomePermanent   = "permanent"
	outcomeCanceled    = "canceled"
)

func (r recorder) call(method, outcome string, dur time.Duration, respBytes int) {
	if r.m == nil {
		return
	}
	r.m.RPCCalls.WithLabelValues(method, outcome).Inc()
	r.m.RPCDuration.WithLabelValues(method).Observe(dur.Seconds())
	if respBytes > 0 {
		r.m.RPCBytes.WithLabelValues(method).Add(float64(respBytes))
	}
	if outcome == outcomeRateLimited {
		r.m.RPCRateLimits.Inc()
	}
}

// outcomeFor maps an error to its metric label.
func outcomeFor(err error) string {
	switch {
	case err == nil:
		return outcomeOK
	case is(err, ErrRateLimited):
		return outcomeRateLimited
	case is(err, ErrUnavailable):
		return outcomeUnavailable
	case is(err, ErrRangeTooLarge), is(err, ErrResultTooLarge):
		return outcomeRangeCap
	case is(err, ErrBeyondHead):
		return outcomeBeyondHead
	default:
		return outcomePermanent
	}
}
