// Package goredis maps messages to its native broker client.
package goredis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
	mq "bitbucket.org/iqhive/iqmq"
)

// New requires an existing stream and consumer group. Pending recovery is caller-managed.
func New(client *redis.Client, stream, group, consumer string) mq.Backend {
	return &mq.FuncBackend{
		PublishFunc: func(ctx context.Context, m mq.Message) error {
			return client.WithContext(ctx).XAdd(&redis.XAddArgs{Stream: stream, Values: encode(m)}).Err()
		},
		ReceiveFunc: func(ctx context.Context) (*mq.Delivery, error) {
			for {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				rows, err := client.WithContext(ctx).XReadGroup(&redis.XReadGroupArgs{Group: group, Consumer: consumer, Streams: []string{stream, ">"}, Count: 1, Block: time.Second}).Result()
				if err == redis.Nil {
					continue
				}
				if err != nil {
					return nil, err
				}
				if len(rows) == 0 || len(rows[0].Messages) == 0 {
					continue
				}
				m := rows[0].Messages[0]
				return mq.NewDelivery(mq.Message{Body: []byte(fmt.Sprint(m.Values["body"]))}, func(c context.Context) error { return client.WithContext(c).XAck(stream, group, m.ID).Err() }, nil), nil
			}
		},
		CloseFunc: client.Close,
	}
}
