package middleware

import (
	"context"
	"math"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
)

// RateLimitConfig configures RateLimit.
type RateLimitConfig struct {
	// Rate is the sustained rate in deliveries per second. It must be
	// positive.
	Rate float64
	// Burst is how many deliveries may arrive at once before the rate is
	// enforced. It must be at least 1.
	Burst int
	// Clock is the time source. Nil selects the wall clock.
	Clock mq.Clock
	// MaxDelay caps the redelivery delay the middleware asks for. Zero means
	// no cap beyond what the bucket computes.
	MaxDelay time.Duration
}

// RateLimit refuses a delivery that would exceed the configured rate and
// postpones it for the delay the bucket needs.
//
// # It does not sleep in the handler
//
// The obvious implementation sleeps until a token is free. This one does not,
// and the reason is the lease: a wait long enough to matter outlives a
// typical ack deadline, after which the message is redelivered underneath the
// handler still sleeping on it, and a sleeping handler cannot tell.
//
// So an over-rate delivery returns mq.Postpone(ErrRateLimited, d), and the
// runner, which can see the lease, decides where the wait happens: it holds
// the delivery and offers it to the chain again after d while the lease
// allows, and otherwise gives it back to the broker with d as the redelivery
// delay (see mq.Postpone). The middleware itself needs no timer at all, and
// rule 3 forbids a timer per message. A held delivery keeps its in-flight
// slot, so a limiter set well below the arrival rate fills MaxInFlight with
// waiting deliveries and the consumer stops fetching until the limit lets
// them through. Size MaxInFlight so that what it admits drains at the limited
// rate within a lease, as for any slow handler.
//
// It is a postponement and not mq.RetryAfter because the handler never ran,
// so the refusal is not a failed attempt. As RetryAfter it spent one of
// WithMaxAttempts each time, and every refused delivery was given the same
// delay, so one of them won each round and a backlog of a few dozen messages
// exhausted the rest unhandled: the runner halted, dead-lettered them or,
// under FailureSkip, acked them.
//
// RateLimit fails with ErrInvalidConfig on a non-positive Rate or a Burst
// below 1, and on a Rate whose interval per token is not representable as a
// time.Duration: above 1e9/s it rounds to zero, and far below one per year it
// overflows.
func RateLimit(cfg RateLimitConfig) (mq.Middleware, error) {
	if cfg.Rate <= 0 || math.IsInf(cfg.Rate, 0) || math.IsNaN(cfg.Rate) || cfg.Burst < 1 || cfg.MaxDelay < 0 {
		return nil, ErrInvalidConfig
	}
	perToken := float64(time.Second) / cfg.Rate
	if perToken < 1 || perToken >= math.MaxInt64 {
		return nil, ErrInvalidConfig
	}
	b := &bucket{
		clk:      clockOr(cfg.Clock),
		perToken: time.Duration(perToken),
		burst:    int64(cfg.Burst),
		maxDelay: cfg.MaxDelay,
	}
	b.tokens = b.burst
	b.last = b.clk.Now()
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			if wait, ok := b.take(); !ok {
				return mq.Postpone(ErrRateLimited, wait)
			}
			return next.Handle(ctx, d)
		})
	}, nil
}

// bucket is a token bucket over an injected clock.
//
// It is one mutex and two words, taken once per delivery. A striped or atomic
// bucket would be faster under contention and would also stop being a rate
// limit: the whole point is one shared allowance, and the striped
// counters win precisely because a counter does not have to be exact.
type bucket struct {
	mu       sync.Mutex
	tokens   int64
	last     time.Time
	clk      mq.Clock
	perToken time.Duration
	burst    int64
	maxDelay time.Duration
}

// take removes one token. It reports false and how long the caller should
// wait when there is none.
func (b *bucket) take() (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clk.Now()
	if elapsed := now.Sub(b.last); elapsed > 0 {
		gained := int64(elapsed / b.perToken)
		if gained > 0 {
			b.tokens += gained
			// Advance by exactly the tokens granted, so the remainder is
			// not thrown away: dropping it would make the effective rate
			// lower than the configured one by up to one token per call.
			b.last = b.last.Add(time.Duration(gained) * b.perToken)
			if b.tokens >= b.burst {
				// A full bucket accrues nothing, and that includes the
				// remainder: it was earned while the bucket was already
				// full, and carrying it granted the next token early, so a
				// burst after an idle spell let Burst+1 deliveries through
				// in less than one token's interval.
				b.tokens = b.burst
				b.last = now
			}
		}
	}
	if b.tokens > 0 {
		b.tokens--
		return 0, true
	}
	d := b.perToken - now.Sub(b.last)
	if d < 0 {
		d = 0
	}
	if b.maxDelay > 0 && d > b.maxDelay {
		d = b.maxDelay
	}
	return d, false
}
