package middleware

import (
	"context"

	mq "github.com/iqhive/iqmq"
)

// Timestamp stamps Properties.CreatedAt on every message being published that
// carries none.
//
// It is a publish middleware for the same reason ID is: "created at" is a
// fact about the sender, and a consumer that filled it in would be recording
// when it received the message under a name that says when the application
// made it. The specification already has the receive-side answer —
// DeliveryInfo.ReceivedAt — and it is the driver's, not a middleware's.
//
// A message that already carries a CreatedAt keeps it.
//
// The clock is read once per message and only because this middleware is
// installed: the rule is that nothing reads the clock unless an observer
// asked for it, and asking for it is what putting this in the chain means. A
// nil clock selects the wall clock.
func Timestamp(c mq.Clock) mq.PublishMiddleware {
	clk := clockOr(c)
	return func(next mq.PublishFunc) mq.PublishFunc {
		return func(ctx context.Context, topic string, ms []*mq.Message) error {
			// One clock read for the whole batch: every message in one
			// PublishBatch was created by the same call, so giving them
			// different timestamps would be inventing an ordering the
			// caller never expressed.
			now := clk.Now()
			for _, m := range ms {
				if m != nil && m.Properties.CreatedAt.IsZero() {
					m.Properties.CreatedAt = now
				}
			}
			return next(ctx, topic, ms)
		}
	}
}
