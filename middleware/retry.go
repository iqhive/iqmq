package middleware

import (
	"context"
	"time"

	mq "github.com/iqhive/iqmq"
)

// RetryConfig configures Retry. The zero value is usable and retries nothing:
// MaxAttempts defaults to 1, which is one attempt and no retry.
type RetryConfig struct {
	// MaxAttempts is how many times the handler is called, including the
	// first. Zero and one both mean "no retry".
	MaxAttempts int
	// Backoff decides the wait before attempt n, counting from 1 exactly as
	// mq.Backoff does: Delay(2) is the wait before the second call. Nil
	// selects mq.ExponentialBackoff's own default (ADR-011: 200 ms base,
	// 30 s cap, doubling, full jitter).
	Backoff mq.Backoff
	// Retryable decides whether an error is worth another attempt. Nil
	// selects Transient.
	Retryable func(error) bool
	// Clock is the time source the wait uses, and the one the delivery's
	// lease is read on: give it the clock the client runs on. Nil selects
	// the wall clock.
	Clock mq.Clock
}

// Transient is Retry's default predicate: it reports whether err names a
// condition that may succeed on another attempt (the specification's "transient-only").
//
// Retryable: Unavailable, Throttled, ResourceExhausted, Backpressure and an
// unclassified failure, which is what an application error usually is.
//
// Not retryable, and each for its own reason:
//
//   - Anything ambiguous, WHATEVER its code: mq.IsAmbiguous is asked before
//     the code is read. The specification special-cases ambiguity and says why: an
//     ambiguous publish MAY have reached the broker, so retrying it is how one
//     message becomes two. The code does not settle it. A connection that
//     died after the write is CodeUnavailable, Retryable and Ambiguous all at
//     once -- driverkit.ErrorMapper.MapNet's answer for an EOF, a closed
//     connection or a reset on a publication that may have left the socket --
//     and a predicate that read only the code retried it. Ambiguity anywhere
//     in err counts: in a *BatchError's entries or an errors.Join's branches
//     as much as at the top. A caller who can de-duplicate retries it
//     deliberately with a Retryable of their own; nothing may decide that for
//     them.
//   - The codes Ambiguous and SettlementUnknown, for the same reason.
//   - InvalidArgument, InvalidConfig, Unsupported, Unauthorized, Forbidden,
//     NotFound, MessageTooLarge, CorruptMessage. Nothing about the next
//     attempt is different.
//   - Canceled and DeadlineExceeded. The caller's context ended; retrying
//     inside it would ignore that, and the wait would return immediately
//     anyway.
//   - AlreadySettled, SettlementInProgress, LeaseLost, NotSettleable. The
//     delivery is gone or its outcome is fixed.
//   - Partial. A *BatchError reports a batch that was not uniformly refused,
//     and the specification forbids blindly retrying a partially successful batch.
//   - PoisonMessage. It has already exhausted its attempts elsewhere.
func Transient(err error) bool {
	if err == nil || mq.IsAmbiguous(err) {
		return false
	}
	return transientCode(err)
}

// transientCode reports whether err's code names a condition that may clear:
// Transient's retry list, without Transient's refusal of ambiguity.
//
// It is the question CircuitBreaker asks by default, which is a different one
// from Retry's. Retry asks whether the failed work may be repeated, and an
// ambiguous failure may not. The breaker asks whether the dependency is
// failing, and a connection that died after the write is exactly that: a
// breaker that stopped counting ambiguous failures would stay closed against a
// broker that drops every connection mid-publish.
func transientCode(err error) bool {
	switch mq.CodeOf(err) {
	case mq.CodeUnavailable, mq.CodeThrottled, mq.CodeResourceExhausted,
		mq.CodeBackpressure, mq.CodeUnknown:
		return true
	default:
		return false
	}
}

// Retry calls the rest of the chain again when it fails transiently.
//
// # This is an in-handler retry, and it is bounded by the lease
//
// The runner's own retry gives the message back to the broker: the lease is released, the broker owns the wait, and the attempt
// count is the broker's. This middleware is the other kind — it holds the
// delivery and calls the handler again — and the two are not interchangeable.
// An in-handler retry is right for a short transient failure a handler is
// certain to recover from in milliseconds, and wrong for anything longer,
// because holding a delivery across a 30-second backoff outlives a typical
// ack deadline and the message is redelivered underneath the handler that
// still holds it: two handlers, one message.
//
// So the wait is bounded by the delivery's lease, which Retry reads from the
// delivery itself (DeliveryInfo.LeaseExpires), whatever the run options: the
// runner narrows the handler's context to the lease only under
// WithLeaseDeadline. A wait that would take more than half of what is left
// of the lease is not slept, and the last failure is returned, so the
// attempt after a wait and its settlement always have the other half. An
// attempt that overran the lease is not followed by another at all. That is
// the guard, and it is what makes the middleware safe to install with a
// default backoff. The wait is also bounded by the handler's context.
//
// Two cases leave only the handler's context to bound the wait. Under a
// runner that keeps the lease alive (WithLeaseExtension, which
// Delivery.LeaseKept reports), the lease the driver granted says nothing
// about how long the runner holds the delivery, so Retry sleeps every wait,
// as the runner does for a postponed delivery. And the lease is read on
// Clock: when Clock does not read a time inside the lease as the delivery
// reaches Retry, as a wall-clock Retry does against a memory broker on its
// test clock, Retry cannot tell a lease that is over from a clock it was not
// granted on, and does not guard that delivery. That includes a delivery
// whose lease ran out before it reached Retry.
//
// A control error (mq.RetryAfter, mq.DeadLetter, mq.Discard, mq.Halt,
// mq.Postpone) is never retried: a handler that named its own disposition
// has already said what should happen. Nor is a delivery the handler settled
// before it failed, for the same reason. A postponement that turns away a
// second or later attempt is not passed on, because an earlier attempt did
// run the handler: Retry returns that attempt's failure instead, so the
// runner counts it and a breaker outside Retry sees it.
//
// Each attempt starts from the message Retry was given, so a middleware
// behind it that rewrites the delivery (Encrypt, Compress, a decrypting or
// claim-checking one) decodes that message again rather than its own output.
//
// Retry fails with ErrInvalidConfig on a negative MaxAttempts.
func Retry(cfg RetryConfig) (mq.Middleware, error) {
	if cfg.MaxAttempts < 0 {
		return nil, ErrInvalidConfig
	}
	attempts := cfg.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	back := cfg.Backoff
	if back == nil {
		back = mq.ExponentialBackoff{}
	}
	retryable := cfg.Retryable
	if retryable == nil {
		retryable = Transient
	}
	clk := clockOr(cfg.Clock)
	if attempts == 1 {
		// Nothing to do: an unconfigured Retry costs the chain nothing.
		return func(next mq.Handler) mq.Handler { return next }, nil
	}
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			// Every attempt runs the rest of the chain again, including any
			// middleware in it that rewrote the delivery, so each one has to
			// start from the message this one was given. Handed its own
			// output, a decoder that requires a sealed or framed body refused
			// it, and the transient failure the retry was for came back as a
			// corrupt message.
			given := d.Message
			expires := leaseOf(d, clk)
			var err, failed error
			for attempt := 1; attempt <= attempts; attempt++ {
				d.Message = given
				err = next.Handle(ctx, d)
				if err == nil {
					return nil
				}
				if dir, _ := mq.DirectiveOf(err); dir != mq.DirectiveNone {
					if dir == mq.DirectivePostpone && failed != nil {
						// This attempt was turned away before the handler,
						// but an earlier one ran it and failed. Passed on,
						// the postponement told the runner the handler never
						// ran, so no attempt was counted and a message that
						// failed every time was offered for ever.
						return failed
					}
					return err
				}
				if d.Settled() {
					// The handler settled the delivery before it failed, so
					// another call would repeat its work on a message that is
					// already acked, or requeued and on its way to another
					// handler. A settlement is a disposition as much as a
					// control error is.
					return err
				}
				if attempt == attempts || !retryable(err) {
					return err
				}
				delay := back.Delay(attempt + 1)
				if !leaseAllows(expires, clk, delay) {
					// The next attempt would start with less of the lease
					// left than the wait took, or after the lease, and its
					// settlement could land after the broker had handed the
					// message to another handler. The runner gives it back
					// now, while the lease still holds, or learns from its
					// settlement that the lease is gone.
					return err
				}
				failed = err
				if werr := wait(ctx, clk, delay); werr != nil {
					// The lease, or the caller, ran out before the next
					// attempt could start. The handler's own failure is
					// what the runner acts on, not the wait's.
					return err
				}
			}
			return err
		})
	}, nil
}

// leaseOf is the end of the lease Retry's waits on d must fit inside, or the
// zero time when only the handler's context bounds them.
//
// It is zero when the runner keeps d's lease alive: the lease the driver
// granted then says nothing about how long the runner holds d, and a wait
// measured against it gave back a message the runner was holding safely.
//
// It is zero, too, when clk reads a time outside the lease as d reaches Retry,
// which is the one moment clk can tell a lease that is over from a clock the
// lease was not granted on. A clock that reads inside the lease here and past
// it after an attempt has watched the lease end. One that reads outside it
// here, as a wall-clock Retry does against a memory broker's test clock, would
// read every lease as over and cut every in-handler retry against that broker.
// The cost is that a delivery reaching Retry after its lease ended on the
// broker's own clock is not guarded either.
func leaseOf(d *mq.Delivery, clk mq.Clock) time.Time {
	if d.LeaseKept() {
		return time.Time{}
	}
	info := d.Info()
	if info.LeaseExpires.IsZero() {
		return time.Time{}
	}
	if now := clk.Now(); now.Before(info.ReceivedAt) || !now.Before(info.LeaseExpires) {
		return time.Time{}
	}
	return info.LeaseExpires
}

// leaseAllows reports whether Retry may wait delay before another attempt on
// a delivery whose lease ends at expires (see leaseOf): the lease is not over,
// and the wait takes at most half of what is left of it, so the attempt after
// it and its settlement have the other half. It is the rule a relaxed run
// holds a postponed delivery to when it does not keep the lease alive.
func leaseAllows(expires time.Time, clk mq.Clock, delay time.Duration) bool {
	if expires.IsZero() {
		return true
	}
	now := clk.Now()
	return now.Before(expires) && delay <= expires.Sub(now)/2
}

// wait sleeps for d on clk, or returns ctx's error if that comes first. A
// non-positive d returns at once without building a timer.
func wait(ctx context.Context, clk mq.Clock, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	t := clk.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
