// Package kafka maps messages to its native broker client.
package kafka

import (
	"context"
	"sync"

	mq "bitbucket.org/iqhive/iqmq/legacy"
	"github.com/twmb/kafka-go/pkg/kgo"
)

// New requires manual partition assignment. Consumer-group commits are not offered by this early adapter.
func New(client *kgo.Client, topic string) mq.Backend {
	var mu sync.Mutex
	var pending []*kgo.Record
	return &mq.FuncBackend{
		PublishFunc: func(ctx context.Context, m mq.Message) error {
			done := make(chan error, 1)
			if err := client.Produce(ctx, encode(topic, m), func(_ *kgo.Record, err error) { done <- err }); err != nil {
				return err
			}
			select {
			case err := <-done:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		ReceiveFunc: func(ctx context.Context) (*mq.Delivery, error) {
			mu.Lock()
			defer mu.Unlock()
			for len(pending) == 0 {
				fetches := client.PollFetches(ctx)
				if errs := fetches.Errors(); len(errs) > 0 {
					return nil, errs[0].Err
				}
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				it := fetches.RecordIter()
				for !it.Done() {
					pending = append(pending, it.Next())
				}
			}
			m := pending[0]
			pending = pending[1:]
			return mq.NewDelivery(mq.Message{Body: append([]byte(nil), m.Value...)}, nil, nil), nil
		},
		CloseFunc: func() error { client.Close(); return nil },
	}
}
