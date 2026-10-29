package middleware

import (
	"context"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
)

// BreakerState is a circuit breaker's state.
type BreakerState uint8

// The breaker states.
const (
	// BreakerClosed passes every delivery to the handler. It is the zero
	// value and the state a healthy breaker is in.
	BreakerClosed BreakerState = iota
	// BreakerOpen refuses every delivery without calling the handler.
	BreakerOpen
	// BreakerHalfOpen lets exactly one delivery through to find out whether
	// the dependency has recovered.
	BreakerHalfOpen

	breakerStateCount
)

var breakerStateNames = [breakerStateCount]string{"closed", "open", "halfopen"}

// String returns the state's lower-case name.
func (s BreakerState) String() string {
	if s < breakerStateCount {
		return breakerStateNames[s]
	}
	return "BreakerState(?)"
}

// Valid reports whether s names a state.
func (s BreakerState) Valid() bool { return s < breakerStateCount }

// BreakerConfig configures CircuitBreaker.
type BreakerConfig struct {
	// Threshold is how many consecutive failures open the circuit. It must
	// be at least 1.
	Threshold int
	// Cooldown is how long the circuit stays open before one probe is let
	// through. It must be positive.
	Cooldown time.Duration
	// Failure decides which errors count against the threshold. Nil selects
	// the codes on Transient's retry list, ambiguous or not: a handler that
	// reports invalid input is reporting on the message, not on the
	// dependency, and must not trip a breaker, while a connection that died
	// after the write -- an ambiguous CodeUnavailable, which Transient refuses
	// to RETRY -- is the dependency failing and must.
	//
	// A postponement (mq.Postpone, which RateLimit, Dedup and another
	// breaker return for a delivery they refused) is never passed to it and
	// never counts: the handler did not run, so it says nothing about the
	// dependency. Its code is CodeThrottled or CodeUnavailable, and counting
	// it opened the circuit behind a rate limit with a healthy dependency.
	Failure func(error) bool
	// Clock is the time source. Nil selects the wall clock.
	Clock mq.Clock
	// OnStateChange, when non-nil, is called with the new state whenever the
	// circuit moves. It is called while the breaker's lock is NOT held, so it
	// may call back into the breaker without deadlocking.
	OnStateChange func(from, to BreakerState)
}

// CircuitBreaker stops calling the handler once it has failed too often, and
// lets one delivery through after a cooldown to find out whether it has
// recovered.
//
// A refused delivery returns mq.Postpone(ErrCircuitOpen, remaining), and
// the runner offers it again when the cooldown is up, holding it or giving it
// back to the broker as mq.Postpone describes. The breaker itself creates no
// timer, for the same reason as RateLimit. A breaker that swallowed the
// delivery would be dropping messages because a dependency was slow, and one
// that returned mq.RetryAfter spent an attempt on every refusal, so an outage
// longer than the attempts added up to exhausted messages the handler never
// saw.
//
// Only one delivery probes a half-open circuit; the rest are refused until it
// answers. Without that a burst of a thousand deliveries would all probe a
// dependency that has just been given a moment to recover. A probe that a
// middleware behind the breaker postponed has not answered, so it neither
// closes nor re-opens the circuit, and the next delivery probes instead.
//
// CircuitBreaker fails with ErrInvalidConfig on a Threshold below 1 or a
// non-positive Cooldown.
func CircuitBreaker(cfg BreakerConfig) (mq.Middleware, error) {
	if cfg.Threshold < 1 || cfg.Cooldown <= 0 {
		return nil, ErrInvalidConfig
	}
	b := &breaker{
		clk:       clockOr(cfg.Clock),
		threshold: cfg.Threshold,
		cooldown:  cfg.Cooldown,
		failure:   cfg.Failure,
		onChange:  cfg.OnStateChange,
	}
	if b.failure == nil {
		b.failure = transientCode
	}
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			gen, left, ok := b.allow()
			if !ok {
				return mq.Postpone(ErrCircuitOpen, left)
			}
			// A panicking handler is a failure, and it is recorded on the
			// way out: otherwise a panicking probe would leave the circuit
			// half-open with its one probe never answering.
			returned := false
			defer func() {
				if !returned {
					b.record(gen, true)
				}
			}()
			err := next.Handle(ctx, d)
			returned = true
			if postponed(err) {
				b.pass(gen)
				return err
			}
			b.record(gen, err != nil && b.failure(err))
			return err
		})
	}, nil
}

// breaker is the circuit breaker's state machine.
type breaker struct {
	mu       sync.Mutex
	state    BreakerState
	failures int
	openedAt time.Time
	probing  bool
	// gen counts state changes. A call is admitted under one generation and
	// its outcome only counts while the breaker is still in it, so a slow
	// call admitted while closed cannot close a circuit that has opened
	// since and skip the cooldown.
	gen       uint64
	clk       mq.Clock
	threshold int
	cooldown  time.Duration
	failure   func(error) bool
	onChange  func(from, to BreakerState)
}

// allow reports whether a delivery may reach the handler, the generation it
// was admitted under, and how long the caller should wait when it may not.
func (b *breaker) allow() (uint64, time.Duration, bool) {
	b.mu.Lock()
	from, to := b.state, b.state
	var left time.Duration
	ok := false
	switch b.state {
	case BreakerClosed:
		ok = true
	case BreakerOpen:
		if elapsed := b.clk.Now().Sub(b.openedAt); elapsed >= b.cooldown {
			b.state, to = BreakerHalfOpen, BreakerHalfOpen
			b.gen++
			b.probing = true
			ok = true
		} else {
			left = b.cooldown - elapsed
		}
	case BreakerHalfOpen:
		if !b.probing {
			b.probing = true
			ok = true
		} else {
			left = b.cooldown
		}
	default:
		// Unreachable: the state is only ever one of the three above. A
		// breaker in a state it does not recognise refuses rather than
		// passing traffic to a handler it cannot account for.
		left = b.cooldown
	}
	gen := b.gen
	b.mu.Unlock()
	b.announce(from, to)
	return gen, left, ok
}

// record folds the outcome of one call admitted under gen into the state.
func (b *breaker) record(gen uint64, failed bool) {
	b.mu.Lock()
	from := b.state
	switch {
	case gen != b.gen:
		// The breaker has moved since this call was admitted, and what it
		// says is about a state that is gone.
	case b.state == BreakerHalfOpen && failed:
		b.probing = false
		b.state = BreakerOpen
		b.openedAt = b.clk.Now()
		b.gen++
	case b.state == BreakerHalfOpen:
		b.failures = 0
		b.probing = false
		b.state = BreakerClosed
		b.gen++
	case !failed:
		b.failures = 0
	default:
		b.failures++
		if b.failures >= b.threshold {
			b.state = BreakerOpen
			b.openedAt = b.clk.Now()
			b.gen++
		}
	}
	to := b.state
	b.mu.Unlock()
	b.announce(from, to)
}

// pass folds in a call admitted under gen that never reached the handler. It
// counts as neither a success nor a failure; a half-open probe is released so
// that the next delivery can find out whether the dependency has recovered.
func (b *breaker) pass(gen uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if gen == b.gen && b.state == BreakerHalfOpen {
		b.probing = false
	}
}

// postponed reports whether err asks the runner to offer the message again
// without counting an attempt, which means the handler never ran.
func postponed(err error) bool {
	dir, _ := mq.DirectiveOf(err)
	return dir == mq.DirectivePostpone
}

// announce reports a state change outside the lock.
func (b *breaker) announce(from, to BreakerState) {
	if from != to && b.onChange != nil {
		b.onChange(from, to)
	}
}
