// Package gcppubsub maps messages to its native broker client.
package gcppubsub

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub"
	mq "bitbucket.org/iqhive/iqmq"
)

// New starts one subscription receiver. Close cancels and waits for callbacks.
func New(topic *pubsub.Topic, sub *pubsub.Subscription) mq.Backend {
	ctx, cancel := context.WithCancel(context.Background())
	queue := make(chan *mq.Delivery, 64)
	done := make(chan struct{})
	var receiveErr error
	go func() {
		defer close(done)
		receiveErr = sub.Receive(ctx, func(c context.Context, m *pubsub.Message) {
			settled := make(chan struct{})
			var once sync.Once
			finish := func() { once.Do(func() { close(settled) }) }
			d := mq.NewDelivery(mq.Message{Body: append([]byte(nil), m.Data...)}, func(context.Context) error { m.Ack(); finish(); return nil }, func(context.Context) error { m.Nack(); finish(); return nil })
			select {
			case queue <- d:
			case <-c.Done():
				m.Nack()
				return
			}
			select {
			case <-settled:
			case <-c.Done():
				m.Nack()
			}
		})
	}()
	return &mq.FuncBackend{
		PublishFunc: func(ctx context.Context, m mq.Message) error {
			_, err := topic.Publish(ctx, encode(m)).Get(ctx)
			return err
		},
		ReceiveFunc: func(c context.Context) (*mq.Delivery, error) {
			select {
			case <-c.Done():
				return nil, c.Err()
			case d := <-queue:
				return d, nil
			case <-done:
				if receiveErr != nil {
					return nil, receiveErr
				}
				return nil, mq.ErrClosed
			}
		},
		CloseFunc: func() error { cancel(); topic.Stop(); <-done; return nil },
	}
}
