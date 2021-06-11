// Package jetstream maps messages to its native broker client.
package jetstream

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq"
	"github.com/nats-io/nats.go"
)

// New requires an existing stream and a manually acknowledged pull subscription.
func New(js nats.JetStreamContext, sub *nats.Subscription, subject string) mq.Backend {
	return &mq.FuncBackend{
		PublishFunc: func(ctx context.Context, m mq.Message) error {
			_, err := js.PublishMsg(encode(subject, m), nats.Context(ctx))
			return err
		},
		ReceiveFunc: func(ctx context.Context) (*mq.Delivery, error) {
			messages, err := sub.Fetch(1, nats.Context(ctx))
			if err != nil {
				return nil, err
			}
			if len(messages) == 0 {
				return nil, mq.ErrClosed
			}
			m := messages[0]
			return mq.NewDelivery(mq.Message{Body: append([]byte(nil), m.Data...)}, func(context.Context) error { return m.Ack() }, func(context.Context) error { return m.Nak() }), nil
		},
		CloseFunc: sub.Unsubscribe,
	}
}
