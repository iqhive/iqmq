// Package memory supplies a bounded queue for local work and tests.
package memory

import (
	"context"
	mq "github.com/iqhive/iqmq/legacy"
	"sync"
)

type Backend struct {
	queue chan mq.Message
	done  chan struct{}
	once  sync.Once
}

func New(capacity int) *Backend {
	if capacity < 1 {
		panic("memory: capacity must be positive")
	}
	return &Backend{queue: make(chan mq.Message, capacity), done: make(chan struct{})}
}
func (b *Backend) Publish(ctx context.Context, m mq.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-b.done:
		return mq.ErrClosed
	default:
	}
	m.Body = append([]byte(nil), m.Body...)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.done:
		return mq.ErrClosed
	case b.queue <- m:
		return nil
	}
}
func (b *Backend) Receive(ctx context.Context) (*mq.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-b.done:
		return nil, mq.ErrClosed
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.done:
		return nil, mq.ErrClosed
	case m := <-b.queue:
		return mq.NewDelivery(m, func(context.Context) error { return nil }, func(c context.Context) error { return b.Publish(c, m) }), nil
	}
}
func (b *Backend) Close() error { b.once.Do(func() { close(b.done) }); return nil }
