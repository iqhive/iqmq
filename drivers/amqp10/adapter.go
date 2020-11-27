// Package amqp10 maps messages to its native broker client.
package amqp10

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq"
	"pack.ag/amqp"
)

func New(sender *amqp.Sender, receiver *amqp.Receiver) mq.Backend {
	return &mq.FuncBackend{
		PublishFunc: func(ctx context.Context, m mq.Message) error { return sender.Send(ctx, encode(m)) },
		ReceiveFunc: func(ctx context.Context) (*mq.Delivery, error) {
			m, err := receiver.Receive(ctx)
			if err != nil {
				return nil, err
			}
			return mq.NewDelivery(mq.Message{Body: append([]byte(nil), m.GetData()...)}, func(context.Context) error { return m.Accept() }, func(context.Context) error { return m.Release() }), nil
		},
		CloseFunc: func() error {
			a := sender.Close(context.Background())
			b := receiver.Close(context.Background())
			if a != nil {
				return a
			}
			return b
		},
	}
}
