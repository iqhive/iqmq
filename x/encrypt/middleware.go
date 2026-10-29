package encrypt

import (
	"context"

	mq "github.com/iqhive/iqmq"
)

// Decrypt opens a sealed inbound body.
//
// The delivery's body is replaced for the rest of the chain, never opened in
// place. The runner keeps the envelope it received: a message the handler
// fails is dead-lettered sealed, never as the plaintext, and a strictly
// ordered retry opens the envelope again rather than meeting its own
// plaintext and refusing it. A body that is not sealed is passed through, or
// refused with [ErrNotSealed] when [Config.RequireSealed] is set.
//
// Every failure is a *[mq.Delivery]'s problem and not the connection's:
// [ErrNotAuthentic] and [ErrNotSealed] carry CodeCorruptMessage,
// [ErrUnknownSuite] carries CodeUnsupported and [ErrNoKey] carries
// CodeInvalidConfig. The runner's configured failure policy then decides
// (ADR-011), and the choice of policy matters more here than for most
// middleware: a topic whose key has been lost produces a handler failure for
// every message, and a runner with no dead-letter topic will Halt, which is
// the correct answer and a loud one.
func Decrypt(cfg Config) (mq.Middleware, error) {
	o, err := NewOpener(cfg)
	if err != nil {
		return nil, err
	}
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			if !Sealed(d.Body) {
				if cfg.RequireSealed {
					return ErrNotSealed
				}
				return next.Handle(ctx, d)
			}
			plain, err := o.Open(ctx, d.Body)
			if err != nil {
				return err
			}
			d.Body = plain
			return next.Handle(ctx, d)
		})
	}, nil
}

// PublishEncrypt seals an outbound body.
//
// A body that already looks like an envelope fails the publish with
// [ErrAlreadySealed], as [Sealer.Seal] does. It is not left alone: [Sealed]
// only reads the magic and the length, so it cannot tell an envelope from a
// plaintext that happens to start with one, and skipping it would publish
// that plaintext in the clear.
//
// The caller's messages are not sealed: the sealed copies go to the next
// publish function, so a message published again is sealed again rather
// than mistaken for its own envelope. What the publisher stamps on a copy
// comes back to the caller's message, as it would without this middleware:
// an ID (PublisherConfig.AutoID, middleware.ID) and a Properties.CreatedAt
// (PublisherConfig.Timestamp, middleware.Timestamp), each only where the
// caller's message had none. A caller that publishes again after an
// ambiguous failure then sends the same ID, which is what a deduplicating
// consumer keys on. A nil message in the batch is skipped.
func PublishEncrypt(cfg Config) (mq.PublishMiddleware, error) {
	s, err := NewSealer(cfg)
	if err != nil {
		return nil, err
	}
	return func(next mq.PublishFunc) mq.PublishFunc {
		return func(ctx context.Context, topic string, ms []*mq.Message, opts []mq.PublishOption) error {
			sealed := make([]*mq.Message, len(ms))
			for i, m := range ms {
				if m == nil {
					continue
				}
				body, err := s.Seal(ctx, m.Body)
				if err != nil {
					return err
				}
				c := *m
				c.Body = body
				// A copy of Message shares its header storage, so a Set or
				// Delete further down the chain -- a trace context stamped
				// over the one a forwarded message carried -- would rewrite
				// the caller's message through it.
				c.Headers = m.Headers.Clone()
				sealed[i] = &c
			}
			err := next(ctx, topic, sealed, opts)
			// The stamps are written before any I/O, so they come back
			// whatever the outcome: a failed publication is the one whose
			// ID the caller needs for the retry.
			stampBack(ms, sealed)
			return err
		}
	}, nil
}

// stampBack gives each of the caller's messages the ID and CreatedAt the
// publish chain stamped on its sealed copy, where the caller left them empty.
func stampBack(ms, sealed []*mq.Message) {
	for i, m := range ms {
		c := sealed[i]
		if c == nil {
			continue
		}
		if m.ID == "" {
			m.ID = c.ID
		}
		if m.Properties.CreatedAt.IsZero() {
			m.Properties.CreatedAt = c.Properties.CreatedAt
		}
	}
}
