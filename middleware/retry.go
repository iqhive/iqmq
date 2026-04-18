package middleware

import (
	"context"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
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
	// Clock is the time source the wait uses. Nil selects the wall clock.
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
// So the wait is bounded by the handler's context, which the runner has
// already narrowed to the lease deadline. A retry whose wait would
// outlive the lease is cut short and the last failure is returned, rather
// than being slept through. That is the guard, and it is what makes the
// middleware safe to install with a default backoff.
//
// A control error (mq.RetryAfter, mq.DeadLetter, mq.Discard, mq.Halt) is
// never retried: a handler that named its own disposition has already said
// what should happen.
//
// Retry panics on a negative MaxAttempts.
func Retry(cfg RetryConfig) mq.Middleware {
	if cfg.MaxAttempts < 0 {
		panic(ErrInvalidConfig)
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
		return func(next mq.Handler) mq.Handler { return next }
	}
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			var err error
			for attempt := 1; attempt <= attempts; attempt++ {
				err = next.Handle(ctx, d)
				if err == nil {
					return nil
				}
				if dir, _ := mq.DirectiveOf(err); dir != mq.DirectiveNone {
					return err
				}
				if attempt == attempts || !retryable(err) {
					return err
				}
				if werr := wait(ctx, clk, back.Delay(attempt+1)); werr != nil {
					// The lease, or the caller, ran out before the next
					// attempt could start. The handler's own failure is
					// what the runner acts on, not the wait's.
					return err
				}
			}
			return err
		})
	}
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
