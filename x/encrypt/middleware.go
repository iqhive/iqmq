package encrypt

import (
	"context"

	mq "github.com/iqhive/iqmq"
)

// Decrypt opens a sealed inbound body.
//
// The delivery's body is replaced for the rest of the chain and the envelope
// is not kept. A body that is not sealed is passed through, or refused with
// [ErrNotSealed] when [Config.RequireSealed] is set.
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
// The caller's messages are not modified: the sealed copies go to the next
// publish function, so a message published again is sealed again rather
// than mistaken for its own envelope. A nil message in the batch is skipped.
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
				sealed[i] = &c
			}
			return next(ctx, topic, sealed, opts)
		}
	}, nil
}
