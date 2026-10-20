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
// gives it back to the broker with the delay the bucket needs.
//
// # It hands the message back rather than holding it
//
// The obvious implementation sleeps until a token is free. This one does not,
// and the reason is the lease: a delivery held inside the handler occupies an
// in-flight slot for the whole wait, so a limiter set well below the arrival
// rate fills MaxInFlight with sleeping deliveries and the consumer stops
// fetching altogether — and a wait long enough to matter outlives a typical
// ack deadline, after which the message is redelivered underneath the handler
// still sleeping on it.
//
// So an over-rate delivery returns mq.RetryAfter(ErrRateLimited, d): the
// lease is released, the broker owns the wait, the delivery is redelivered
// after d, and the in-flight slot is free the whole time. That is the same
// shape the runner's own retry takes , and it needs no
// timer at all — rule 3 forbids a timer per message and this creates
// none.
//
// The cost is that the limiter needs a broker that can delay a redelivery. A
// driver that cannot answers ErrUnsupported to the nack, which surfaces as a
// failed settlement rather than as silence.
//
// RateLimit fails with ErrInvalidConfig on a non-positive Rate or a Burst
// below 1.
func RateLimit(cfg RateLimitConfig) (mq.Middleware, error) {
	if cfg.Rate <= 0 || math.IsInf(cfg.Rate, 0) || math.IsNaN(cfg.Rate) || cfg.Burst < 1 || cfg.MaxDelay < 0 {
		return nil, ErrInvalidConfig
	}
	b := &bucket{
		clk:      clockOr(cfg.Clock),
		perToken: time.Duration(float64(time.Second) / cfg.Rate),
		burst:    int64(cfg.Burst),
		maxDelay: cfg.MaxDelay,
	}
	b.tokens = b.burst
	b.last = b.clk.Now()
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			if wait, ok := b.take(); !ok {
				return mq.RetryAfter(ErrRateLimited, wait)
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
			if b.tokens > b.burst {
				b.tokens = b.burst
			}
			// Advance by exactly the tokens granted, so the remainder is
			// not thrown away: dropping it would make the effective rate
			// lower than the configured one by up to one token per call.
			b.last = b.last.Add(time.Duration(gained) * b.perToken)
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
