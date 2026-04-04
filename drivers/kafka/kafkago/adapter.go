// Package kafkago maps messages to its native broker client.
package kafkago

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq/legacy"
	"github.com/segmentio/kafka-go"
)

// New leaves reader and writer construction to the caller. Commit order is the caller's responsibility.
func New(writer *kafka.Writer, reader *kafka.Reader) mq.Backend {
	return &mq.FuncBackend{
		PublishFunc: func(ctx context.Context, m mq.Message) error { return writer.WriteMessages(ctx, encode(m)) },
		ReceiveFunc: func(ctx context.Context) (*mq.Delivery, error) {
			m, err := reader.FetchMessage(ctx)
			if err != nil {
				return nil, err
			}
			return mq.NewDelivery(mq.Message{Body: append([]byte(nil), m.Value...)}, func(c context.Context) error { return reader.CommitMessages(c, m) }, nil), nil
		},
		CloseFunc: func() error {
			a := reader.Close()
			b := writer.Close()
			if a != nil {
				return a
			}
			return b
		},
	}
}
