// Package stomp maps messages to its native broker client.
package stomp

import (
	"context"

	"github.com/go-stomp/stomp/v3"
	mq "bitbucket.org/iqhive/iqmq/legacy"
)

// New expects a subscription using client-individual acknowledgement mode.
func New(conn *stomp.Conn, sub *stomp.Subscription, destination string) mq.Backend {
	return &mq.FuncBackend{
		PublishFunc: func(ctx context.Context, m mq.Message) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return conn.Send(destination, "application/octet-stream", encode(m))
		},
		ReceiveFunc: func(ctx context.Context) (*mq.Delivery, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case m, ok := <-sub.C:
				if !ok {
					return nil, mq.ErrClosed
				}
				if m.Err != nil {
					return nil, m.Err
				}
				return mq.NewDelivery(mq.Message{Body: append([]byte(nil), m.Body...)}, func(context.Context) error { return conn.Ack(m) }, func(context.Context) error { return conn.Nack(m) }), nil
			}
		},
		CloseFunc: func() error { return sub.Unsubscribe() },
	}
}
