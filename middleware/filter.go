package middleware

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq"
)

// Predicate decides whether a delivery is for this subscription.
type Predicate func(d *mq.Delivery) bool

// Filter drops the deliveries a predicate does not want, before the handler
// runs.
//
// # A filtered delivery is SETTLED, not merely skipped
//
// Filter acks it. A middleware that short-circuited the handler and returned
// nil would settle nothing under mq.SettlementManual — where the handler is
// what settles — and the lease would be held until it expired, after which
// the message is redelivered, filtered again, and held again: a message that
// nothing wants, redelivered for ever. Under the automatic settlement modes
// the runner sees a delivery that is already settled and does not overrule it
// so one implementation is right in both.
//
// The ack is deliberate and it is the caller's decision to make: a filter is
// the application saying "this subscription is not interested in this
// message". That is not ADR-011's forbidden silent skip — which is about a
// FAILURE being dropped — but it does destroy the message for this
// subscription, which is why the predicate is the caller's and there is no
// default.
//
// onFiltered, when non-nil, is called before the ack, so a caller can count
// or log what was dropped.
//
// A nil predicate returns the handler unchanged: a filter that keeps
// everything is not a filter, and costs the chain nothing.
func Filter(keep Predicate) mq.Middleware { return FilterWith(keep, nil) }

// FilterWith is Filter with an observer.
func FilterWith(keep Predicate, onFiltered func(ctx context.Context, d *mq.Delivery)) mq.Middleware {
	if keep == nil {
		return func(next mq.Handler) mq.Handler { return next }
	}
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			if keep(d) {
				return next.Handle(ctx, d)
			}
			if onFiltered != nil {
				onFiltered(ctx, d)
			}
			return ackAndStop(d)
		})
	}
}
