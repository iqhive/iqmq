// Package rabbitmq maps messages to its native broker client.
package rabbitmq

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq"
	"github.com/streadway/amqp"
)

// New consumes a caller-declared queue with explicit acknowledgements.
func New(ch *amqp.Channel, exchange, key, queue string) (mq.Backend, error) {
	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	return &mq.FuncBackend{
		PublishFunc: func(ctx context.Context, m mq.Message) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return ch.Publish(exchange, key, false, false, encode(m))
		},
		ReceiveFunc: func(ctx context.Context) (*mq.Delivery, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case d, ok := <-deliveries:
				if !ok {
					return nil, mq.ErrClosed
				}
				return mq.NewDelivery(mq.Message{Body: append([]byte(nil), d.Body...)}, func(context.Context) error { return d.Ack(false) }, func(context.Context) error { return d.Nack(false, true) }), nil
			}
		},
		CloseFunc: ch.Close,
	}, nil
}
