// Package pulsar maps messages to its native broker client.
package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	mq "bitbucket.org/iqhive/iqmq"
)

func New(producer pulsar.Producer, consumer pulsar.Consumer) mq.Backend {
	return &mq.FuncBackend{
		PublishFunc: func(ctx context.Context, m mq.Message) error { _, err := producer.Send(ctx, encode(m)); return err },
		ReceiveFunc: func(ctx context.Context) (*mq.Delivery, error) {
			m, err := consumer.Receive(ctx)
			if err != nil {
				return nil, err
			}
			return mq.NewDelivery(mq.Message{Body: append([]byte(nil), m.Payload()...)}, func(context.Context) error { consumer.Ack(m); return nil }, func(context.Context) error { consumer.Nack(m); return nil }), nil
		},
		CloseFunc: func() error { producer.Close(); consumer.Close(); return nil },
	}
}
