package middleware

import (
	"context"
	"sync"

	mq "github.com/iqhive/iqmq"
)

// Publisher is an mq.Publisher with a publish middleware chain composed
// around it (PublishMiddleware).
//
// The core has no publisher option that takes a publish chain — the specification's
// PublisherConfig predates the type and is frozen — so this is what makes
// ID, Timestamp, PublishCompress and PublishEncrypt usable. It implements
// mq.Publisher, so it drops straight into typed.NewPublisher or anywhere else
// a publisher is taken.
//
// The zero value is not usable; WrapPublisher returns a ready one. It is safe
// for concurrent use if the publisher under it is, and it does not own that
// publisher: Close closes it, because a caller who wrapped a handle expects
// closing the wrapper to close the handle, and a caller who does not want
// that keeps the original and never calls Close on this.
type Publisher struct {
	pub  mq.Publisher
	send mq.PublishFunc
	// ones recycles the one-element slices a single publication presents
	// itself as. PublishFunc takes a batch, and a slice of one that
	// crosses a function value escapes, so without this every publication
	// through a wrapped publisher would allocate one — which is exactly the
	// kind of per-message cost rule 2 exists to keep out.
	//
	// It is why a middleware must not retain the slice it is given past the
	// call, which is the same ownership rule the specification already states for the
	// messages in it.
	ones sync.Pool
}

// WrapPublisher composes mw around p, once (rule 6).
//
// The chain is built here and never again: a publication walks the composed
// mq.PublishFunc and composes nothing. A nil middleware is skipped, and a
// wrapper with no middleware at all forwards straight to p.
//
// # How a single publication and a batch are the same call
//
// PublishFunc takes a batch, so Publish presents its one message as a
// batch of one and the terminal unwraps it again: a single publication
// reaches p.Publish and a batch reaches p.PublishBatch, which is what lets a
// driver's native batch path work underneath a chain.
//
// # Per-call options
//
// A call's mq.PublishOptions are handed down the chain as PublishFunc's
// last argument, so every middleware sees them and must pass them on, and
// the terminal gives them to p.
func WrapPublisher(p mq.Publisher, mw ...mq.PublishMiddleware) (*Publisher, error) {
	if p == nil {
		return nil, ErrInvalidConfig
	}
	w := &Publisher{pub: p}
	w.ones.New = func() any { return new([1]*mq.Message) }
	w.send = w.terminal
	for i := len(mw) - 1; i >= 0; i-- {
		if mw[i] != nil {
			w.send = mw[i](w.send)
		}
	}
	return w, nil
}

// terminal is the innermost PublishFunc: the one that actually publishes.
func (w *Publisher) terminal(ctx context.Context, _ string, ms []*mq.Message, opts []mq.PublishOption) error {
	if len(ms) == 1 {
		return w.pub.Publish(ctx, ms[0], opts...)
	}
	return w.pub.PublishBatch(ctx, ms, opts...)
}

// Publish sends one message through the chain.
//
// The message is the caller's and the chain may modify it in place — that is
// what stamping an id, a timestamp or a compressed body means — so ownership
// follows the message belongs to the publisher until Publish returns.
func (w *Publisher) Publish(ctx context.Context, m *mq.Message, opts ...mq.PublishOption) error {
	one, _ := w.ones.Get().(*[1]*mq.Message)
	one[0] = m
	err := w.send(ctx, topicOf(m), one[:], opts)
	one[0] = nil
	w.ones.Put(one)
	return err
}

// PublishBatch sends a batch through the chain, as one call.
//
// The chain sees the whole batch, which is what makes a batch-aware
// middleware possible: Timestamp gives every message of one batch the same
// timestamp rather than a spread of them, and a future one could refuse a
// batch as a unit.
func (w *Publisher) PublishBatch(ctx context.Context, ms []*mq.Message, opts ...mq.PublishOption) error {
	if len(ms) == 0 {
		return nil
	}
	topic := ""
	if ms[0] != nil {
		topic = ms[0].Topic
	}
	return w.send(ctx, topic, ms, opts)
}

// Flush flushes the publisher underneath. Nothing in the chain is buffered:
// a middleware either published or it did not.
func (w *Publisher) Flush(ctx context.Context) error { return w.pub.Flush(ctx) }

// Close closes the publisher underneath.
func (w *Publisher) Close() error { return w.pub.Close() }

// Unwrap returns the publisher this one wraps, for a caller that needs the
// handle's own methods.
func (w *Publisher) Unwrap() mq.Publisher { return w.pub }

var _ mq.Publisher = (*Publisher)(nil)

// topicOf returns the message's topic, which is what a middleware keyed by
// destination reads. A bound publisher ignores it, so it may be empty.
func topicOf(m *mq.Message) string {
	if m == nil {
		return ""
	}
	return m.Topic
}
