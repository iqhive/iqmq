package middleware

import (
	"context"
	"errors"
	"time"

	mq "github.com/iqhive/iqmq"
)

// Outcome is what a handler did with a delivery, as reported to a Recorder.
// The values are the label values of the canonical
// iqmq_handler_duration_seconds{outcome}.
const (
	// OutcomeSuccess is a handler that returned nil.
	OutcomeSuccess = "success"
	// OutcomeFailure is a handler that returned an error.
	OutcomeFailure = "failure"
	// OutcomePanic is a handler that panicked, which only a Recover placed
	// INSIDE this middleware can report: a panic that escapes Metrics
	// unwinds past it.
	OutcomePanic = "panic"
)

// Recorder receives what Metrics measures. Implementations must be safe for
// concurrent use and must not block: they are called on the goroutine that
// handled the delivery.
type Recorder interface {
	// HandlerDuration records one handler call. topic and subscription come
	// from the delivery's own metadata, and outcome is one of OutcomeSuccess,
	// OutcomeFailure or OutcomePanic.
	//
	// A message id is deliberately not a parameter: the specification forbids message
	// identifiers as metric labels, because one series per message is how a
	// metrics backend falls over.
	HandlerDuration(topic, subscription, outcome string, d time.Duration)
}

// Metrics records the duration and outcome of everything inside it (
// the iqmq_handler_duration_seconds{outcome}).
//
// A nil Recorder returns the handler unchanged, so an uninstrumented program
// pays nothing — not a clock read, not a branch a message. That is the specification's
// "nil hooks ⇒ no event allocation, no time.Now" applied to the chain.
//
// A nil clock selects the wall clock.
//
// Place it outermost to measure the whole chain, or next to the handler to
// measure the handler alone; both are useful and the position is the
// caller's. To have a panic recorded as OutcomePanic rather than escaping,
// put a Recover INSIDE it.
func Metrics(r Recorder, c mq.Clock) mq.Middleware {
	if r == nil {
		return func(next mq.Handler) mq.Handler { return next }
	}
	clk := clockOr(c)
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			start := clk.Now()
			err := next.Handle(ctx, d)
			info := d.Info()
			r.HandlerDuration(info.Topic, info.Subscription, outcomeOf(err), clk.Now().Sub(start))
			return err
		})
	}
}

// outcomeOf maps a handler result to its label value.
func outcomeOf(err error) string {
	switch {
	case err == nil:
		return OutcomeSuccess
	case isPanic(err):
		return OutcomePanic
	default:
		return OutcomeFailure
	}
}

// isPanic reports whether err is a recovered panic.
func isPanic(err error) bool {
	var pe *mq.PanicError
	return errors.As(err, &pe)
}
