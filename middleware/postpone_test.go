package middleware_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/middleware"
)

// These are the refusals a middleware makes before the handler runs: a rate
// limit, a duplicate whose first copy is still in the handler, an open
// circuit. None of them is a failed attempt. Each used to come back to the
// runner as one, so a backlog spent WithMaxAttempts on messages nobody had
// handled, and the runner halted with a *PoisonError, dead-lettered them or,
// under FailureSkip, acked them unhandled. They run through a real runner over
// the memory driver, because the attempt count is the runner's and the
// broker's, and a middleware on its own has none.

func TestRateLimitRefusalsDoNotUseUpTheAttempts(t *testing.T) {
	h := newHarness(t, memory.TestRealClock())
	rl := mustRateLimit(t, middleware.RateLimitConfig{Rate: 100, Burst: 1})
	c := &calls{}
	const n = 30
	r := h.run(rl(c), mq.WithConcurrency(4), mq.WithMaxAttempts(2))
	h.publish(n, nil)
	r.runUntil("every message to be handled", func() bool { return c.count() == n })
	if err := r.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if s := h.snapshot(); s.Acked != n || s.DeadLettered != 0 {
		t.Fatalf("snapshot = %+v, want every message acked once its handler ran", s)
	}
}

func TestDedupInFlightRefusalsDoNotUseUpTheAttempts(t *testing.T) {
	h := newHarness(t, memory.TestRealClock())
	// The first copy stays in the handler long enough for the second to be
	// refused many more times than WithMaxAttempts allows.
	c := &calls{fn: func(int, *mq.Delivery) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}}
	var offered atomic.Int64
	counting := func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			offered.Add(1)
			return next.Handle(ctx, d)
		})
	}
	r := h.run(counting(middleware.Dedup(middleware.DedupConfig{})(c)),
		mq.WithConcurrency(16),
		mq.WithMaxAttempts(2),
		mq.WithRetryBackoff(mq.ConstantBackoff(time.Millisecond)))
	h.publish(2, func(int) string { return "same-id" })
	r.runUntil("both copies to be settled", func() bool { return h.snapshot().Acked == 2 })
	if err := r.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got := offered.Load(); got < 4 {
		t.Fatalf("the chain was offered a delivery %d times, want at least 4: the second copy refused more often than WithMaxAttempts(2) allows while the first was in the handler", got)
	}
	if got := c.count(); got != 1 {
		t.Fatalf("the handler ran %d times, want 1: the second copy is a duplicate once the first has been handled", got)
	}
}

func TestCircuitBreakerDoesNotCountARefusalBehindIt(t *testing.T) {
	clk := newFakeClock()
	var moved []middleware.BreakerState
	br := mustCircuitBreaker(t, middleware.BreakerConfig{
		Threshold:     1,
		Cooldown:      time.Minute,
		Clock:         clk,
		OnStateChange: func(_, to middleware.BreakerState) { moved = append(moved, to) },
	})
	rl := mustRateLimit(t, middleware.RateLimitConfig{Rate: 1, Burst: 1, Clock: clk})
	c := &calls{}
	h := mq.Chain(br, rl)(c)
	if err := h.Handle(context.Background(), delivery("x")); err != nil {
		t.Fatalf("the first delivery = %v, want nil", err)
	}
	for i := range 3 {
		if err := h.Handle(context.Background(), delivery("x")); !errors.Is(err, middleware.ErrRateLimited) {
			t.Fatalf("over-rate delivery %d = %v, want ErrRateLimited and not an open circuit", i, err)
		}
	}
	clk.advance(time.Second)
	if err := h.Handle(context.Background(), delivery("x")); err != nil {
		t.Fatalf("the delivery after the bucket refilled = %v, want nil: a rate limit opened the circuit", err)
	}
	if len(moved) != 0 {
		t.Fatalf("the circuit moved to %v, want it closed throughout: the dependency never failed", moved)
	}
	if got := c.count(); got != 2 {
		t.Fatalf("the handler ran %d times, want 2", got)
	}
}

func TestCircuitBreakerLetsAnotherDeliveryProbeWhenTheProbeIsPostponed(t *testing.T) {
	// A probe that a middleware behind the breaker postponed never reached
	// the dependency. Counting it as a success closed the circuit untested,
	// counting it as a failure re-opened it for another cooldown, and doing
	// neither while keeping it as the probe refused every delivery for ever.
	clk := newFakeClock()
	postpone := atomic.Bool{}
	c := &calls{fn: func(int, *mq.Delivery) error { return errUnavailable }}
	inner := func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			if postpone.Load() {
				return mq.Postpone(errUnavailable, time.Second)
			}
			return next.Handle(ctx, d)
		})
	}
	var moved []middleware.BreakerState
	br := mustCircuitBreaker(t, middleware.BreakerConfig{
		Threshold:     1,
		Cooldown:      time.Second,
		Clock:         clk,
		OnStateChange: func(_, to middleware.BreakerState) { moved = append(moved, to) },
	})
	h := mq.Chain(br, inner)(c)
	_ = h.Handle(context.Background(), delivery("x")) // trips it
	clk.advance(time.Second)
	postpone.Store(true)
	if err := h.Handle(context.Background(), delivery("x")); !errors.Is(err, errUnavailable) {
		t.Fatalf("the postponed probe = %v, want the inner postponement", err)
	}
	postpone.Store(false)
	if err := h.Handle(context.Background(), delivery("x")); !errors.Is(err, errUnavailable) || c.count() != 2 {
		t.Fatalf("the next delivery = %v (handler ran %d times), want it let through as the probe", err, c.count())
	}
	want := []middleware.BreakerState{middleware.BreakerOpen, middleware.BreakerHalfOpen, middleware.BreakerOpen}
	if !slices.Equal(moved, want) {
		t.Fatalf("the circuit moved %v, want %v: the postponed probe answered nothing", moved, want)
	}
}

func TestCircuitBreakerIgnoresAPostponementWhateverItsFailurePredicate(t *testing.T) {
	// A predicate that counts every error never sees a postponement: what
	// is behind the breaker turned the delivery away, so nothing was asked of
	// the dependency.
	clk := newFakeClock()
	c := &calls{fn: func(int, *mq.Delivery) error { return mq.Postpone(errBoom, 0) }}
	h := mustCircuitBreaker(t, middleware.BreakerConfig{
		Threshold: 1,
		Cooldown:  time.Minute,
		Clock:     clk,
		Failure:   func(error) bool { return true },
	})(c)
	for i := range 3 {
		if err := h.Handle(context.Background(), delivery("x")); errors.Is(err, middleware.ErrCircuitOpen) {
			t.Fatalf("delivery %d = %v, want the circuit closed: a postponement says nothing about the dependency", i, err)
		}
	}
	if got := c.count(); got != 3 {
		t.Fatalf("the handler ran %d times, want 3", got)
	}
}

func TestRetryAroundARateLimitStillExhaustsAFailingMessage(t *testing.T) {
	// Retry's second attempt meets the rate limit behind it, which postpones.
	// The first attempt ran the handler and failed, so passing the
	// postponement on told the runner the handler never ran: no attempt was
	// counted, and a message that failed every time was offered for ever.
	cases := []struct {
		name string
		err  error
		opts []mq.RunOption
	}{
		{"once its attempts are used up", errBoom, []mq.RunOption{
			mq.WithMaxAttempts(2), mq.WithRetryBackoff(mq.ConstantBackoff(time.Millisecond)),
		}},
		{"on its first failure under OrderingStopOnFailure", mq.ErrBackpressure, []mq.RunOption{
			mq.WithOrderingByKey(mq.OrderingStopOnFailure),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, memory.TestRealClock())
			rl := mustRateLimit(t, middleware.RateLimitConfig{Rate: 50, Burst: 1})
			retry := mustRetry(t, middleware.RetryConfig{MaxAttempts: 3, Backoff: mq.ConstantBackoff(time.Millisecond)})
			c := &calls{fn: func(int, *mq.Delivery) error { return tc.err }}
			r := h.run(retry(rl(c)), tc.opts...)
			h.publish(1, nil)
			select {
			case err := <-r.done:
				var pe *mq.PoisonError
				if !errors.As(err, &pe) {
					t.Fatalf("Run = %v after the handler ran %d times, want a *PoisonError", err, c.count())
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("after 10s the handler had failed %d times and the runner had not stopped", c.count())
			}
		})
	}
}

func TestABreakerOutsideRetryCountsTheFailureBeforeAPostponement(t *testing.T) {
	// The failure Retry reports for an attempt that ran is the dependency
	// failing, whatever the next attempt met. Passed on as a postponement, it
	// never reached the breaker, which stayed closed against a dependency
	// that failed every call.
	clk := newFakeClock()
	var moved []middleware.BreakerState
	br := mustCircuitBreaker(t, middleware.BreakerConfig{
		Threshold:     2,
		Cooldown:      time.Minute,
		Clock:         clk,
		OnStateChange: func(_, to middleware.BreakerState) { moved = append(moved, to) },
	})
	retry := mustRetry(t, middleware.RetryConfig{MaxAttempts: 3, Clock: clk})
	rl := mustRateLimit(t, middleware.RateLimitConfig{Rate: 1, Burst: 1, Clock: clk})
	c := &calls{fn: func(int, *mq.Delivery) error { return errUnavailable }}
	h := mq.Chain(br, retry, rl)(c)
	for i := range 2 {
		err := h.Handle(context.Background(), delivery("x"))
		if !errors.Is(err, errUnavailable) || postponedErr(err) {
			t.Fatalf("delivery %d = %v, want the handler's failure and no postponement: the handler ran", i, err)
		}
		clk.advance(time.Second)
	}
	if got := c.count(); got != 2 {
		t.Fatalf("the handler ran %d times, want 2", got)
	}
	if want := []middleware.BreakerState{middleware.BreakerOpen}; !slices.Equal(moved, want) {
		t.Fatalf("the circuit moved %v, want %v after two failed calls", moved, want)
	}
}

func TestDedupKeepOnFailureForgetsAPostponedKey(t *testing.T) {
	// KeepOnFailure keeps the key of a handler that failed after its side
	// effect. A postponement behind Dedup is not that: the handler never
	// ran. Kept, the key made the next offer a duplicate, acked without the
	// handler ever seeing the message.
	clk := newFakeClock()
	var duplicates atomic.Int64
	dedup := middleware.Dedup(middleware.DedupConfig{
		Store:         mustNewLRUStore(t, 16),
		KeepOnFailure: true,
		OnDuplicate:   func(context.Context, *mq.Delivery, string) { duplicates.Add(1) },
	})
	rl := mustRateLimit(t, middleware.RateLimitConfig{Rate: 1, Burst: 1, Clock: clk})
	c := &calls{}
	h := mq.Chain(dedup, rl)(c)
	if err := h.Handle(context.Background(), delivery2("x", "id-1")); err != nil {
		t.Fatalf("the first message = %v, want nil", err)
	}
	if err := h.Handle(context.Background(), delivery2("x", "id-2")); !postponedErr(err) {
		t.Fatalf("the over-rate message = %v, want it postponed", err)
	}
	clk.advance(time.Second)
	_ = h.Handle(context.Background(), delivery2("x", "id-2"))
	if got := c.count(); got != 2 || duplicates.Load() != 0 {
		t.Fatalf("the handler ran %d times and %d offer(s) were taken for duplicates, want 2 and none", got, duplicates.Load())
	}
}

// postponedErr reports whether err asks the runner for a postponement.
func postponedErr(err error) bool {
	dir, _ := mq.DirectiveOf(err)
	return dir == mq.DirectivePostpone
}

// runUntil polls cond for up to ten seconds, failing the test if the runner
// returns first.
func (r *running) runUntil(what string, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		select {
		case err := <-r.done:
			r.t.Fatalf("the runner returned while waiting for %s: %v", what, err)
		default:
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
