// Package nats maps messages to its native broker client.
package nats

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq/legacy"
	"github.com/nats-io/nats.go"
)

// New uses a caller-owned connection. Core NATS has no durable settlement.
func New(conn *nats.Conn, sub *nats.Subscription, subject string) mq.Backend {
	return &mq.FuncBackend{
		PublishFunc: func(ctx context.Context, m mq.Message) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return conn.PublishMsg(encode(subject, m))
		},
		ReceiveFunc: func(ctx context.Context) (*mq.Delivery, error) {
			m, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				return nil, err
			}
			return mq.NewDelivery(mq.Message{Body: append([]byte(nil), m.Data...)}, func(context.Context) error { return nil }, nil), nil
		},
		CloseFunc: func() error { return sub.Unsubscribe() },
	}
}
