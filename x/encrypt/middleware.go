package encrypt

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq"
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
// A message whose body is already an envelope is left alone rather than
// refused, which is the one place this differs from [Sealer.Seal]: a batch
// that a caller has partly sealed itself is a reasonable thing to publish,
// and failing the whole batch for it would be worse than the alternative.
// A nil message in the batch is skipped.
func PublishEncrypt(cfg Config) (mq.PublishMiddleware, error) {
	s, err := NewSealer(cfg)
	if err != nil {
		return nil, err
	}
	return func(next mq.PublishFunc) mq.PublishFunc {
		return func(ctx context.Context, topic string, ms []*mq.Message) error {
			for _, m := range ms {
				if m == nil || Sealed(m.Body) {
					continue
				}
				body, err := s.Seal(ctx, m.Body)
				if err != nil {
					return err
				}
				m.Body = body
			}
			return next(ctx, topic, ms)
		}
	}, nil
}
