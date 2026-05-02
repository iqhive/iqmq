package compress

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq"
)

// Unframed is what [Decompress] does with a delivery whose body carries no
// frame.
type Unframed int

const (
	// UnframedPass runs the handler on the body as it arrived. It is the
	// default, and it is what lets a topic be migrated: publishers switch to
	// framing one at a time while the consumer reads both.
	UnframedPass Unframed = iota
	// UnframedFail refuses the delivery with [ErrNotFramed].
	//
	// Switch it on once the migration is done. A consumer that accepts an
	// unframed body accepts a body nothing has said anything about, and on a
	// topic where framing is meant to be universal that is the one case
	// worth knowing about.
	UnframedFail
)

// Decompress unframes an inbound body.
//
// The delivery's body is replaced for the rest of the chain and the framed
// original is not kept: nothing downstream wants it, and holding a second
// copy of every message is how a decompressing consumer runs out of memory
// honestly.
//
// Every failure is a *[mq.Delivery]'s problem and not the connection's:
// [ErrTooLarge] carries CodeMessageTooLarge, [ErrSizeMismatch] and
// [ErrNotFramed] carry CodeCorruptMessage and [ErrUnknownCodec] carries
// CodeUnsupported, all of which the runner treats as ordinary handler
// failures and its configured failure policy then decides (ADR-011).
func Decompress(cfg Config, onUnframed Unframed) (mq.Middleware, error) {
	d, err := NewDecoder(cfg)
	if err != nil {
		return nil, err
	}
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, del *mq.Delivery) error {
			if !Framed(del.Body) {
				if onUnframed == UnframedFail {
					return ErrNotFramed
				}
				return next.Handle(ctx, del)
			}
			body, err := d.Decode(del.Body)
			if err != nil {
				return err
			}
			del.Body = body
			return next.Handle(ctx, del)
		})
	}, nil
}

// PublishCompress frames an outbound body.
//
// A nil message in the batch is skipped rather than refused, which is what
// every other publish middleware in this repository does with one.
func PublishCompress(cfg Config) (mq.PublishMiddleware, error) {
	e, err := NewEncoder(cfg)
	if err != nil {
		return nil, err
	}
	return func(next mq.PublishFunc) mq.PublishFunc {
		return func(ctx context.Context, topic string, ms []*mq.Message) error {
			for _, m := range ms {
				if m == nil {
					continue
				}
				body, err := e.Encode(m.Body)
				if err != nil {
					return err
				}
				m.Body = body
			}
			return next(ctx, topic, ms)
		}
	}, nil
}
