package middleware

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq"
)

// Validator checks a delivery before the handler sees it. It returns nil for
// a delivery the handler may process and the reason otherwise.
type Validator func(d *mq.Delivery) error

// Validate refuses a delivery that fails a check, before the handler runs
//
// # An invalid delivery is DEAD-LETTERED, not acked and not retried
//
// Validation failure is not transient: the same bytes fail the same check on
// every redelivery, so nacking is an infinite loop with a message in it. And
// it is not noise: something published a message this subscription cannot
// process, which is exactly what a dead-letter destination is for.
//
// So Validate calls Reject, which is Nack(NackDeadLetter). A backend that has
// no dead-letter destination and no enabled emulation answers ErrUnsupported
// and settles NOTHING, leaving the delivery settleable — and Validate then
// returns mq.DeadLetter(err), which hands the decision to the runner: its own
// dead-letter route if one is configured, and ADR-011's halt if there is not.
// Either way the message is not lost and the lease is not stranded, which is
// what a middleware that returned without settling would do.
//
// onInvalid, when non-nil, is called with the reason before the delivery is
// settled.
//
// A nil validator returns the handler unchanged.
func Validate(v Validator) mq.Middleware { return ValidateWith(v, nil) }

// ValidateWith is Validate with an observer.
func ValidateWith(v Validator, onInvalid func(ctx context.Context, d *mq.Delivery, err error)) mq.Middleware {
	if v == nil {
		return func(next mq.Handler) mq.Handler { return next }
	}
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			err := v(d)
			if err == nil {
				return next.Handle(ctx, d)
			}
			if onInvalid != nil {
				onInvalid(ctx, d, err)
			}
			return rejectAndStop(d, err)
		})
	}
}
