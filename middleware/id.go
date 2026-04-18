package middleware

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq"
)

// ID stamps Message.ID on every message being published that carries none
//
// It is a publish middleware and not a handler one, deliberately. An
// identifier means something only if it was assigned by the sender: it is
// what a de-duplicating consumer keys on, what a dead-letter record names,
// and what a log line correlates. An identifier a CONSUMER invented names
// nothing — the redelivery of the same message would get a different one,
// which is precisely the case de-duplication exists for — so there is no
// handler-side ID and inventing one would be worse than having none.
//
// A message that already carries an id keeps it: the caller's identifier wins
// over a generated one, always, because the caller may be re-publishing a
// message whose identity must not change.
//
// PublisherConfig.AutoID does the same job inside the core with an
// mq.IDGenerator. This is the chain-level form, for a program that assembles
// its publish pipeline out of middleware rather than out of publisher
// configuration.
//
// A nil generator returns the publish function unchanged.
func ID(gen IDFunc) mq.PublishMiddleware {
	if gen == nil {
		return func(next mq.PublishFunc) mq.PublishFunc { return next }
	}
	return func(next mq.PublishFunc) mq.PublishFunc {
		return func(ctx context.Context, topic string, ms []*mq.Message) error {
			for _, m := range ms {
				if m != nil && m.ID == "" {
					m.ID = gen()
				}
			}
			return next(ctx, topic, ms)
		}
	}
}
