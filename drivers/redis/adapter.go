// Package redis maps messages to its native broker client.
package redis

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq"
	"github.com/rueian/rueidis"
)

// New requires an existing consumer group. Pending recovery is caller-managed.
func New(client rueidis.Client, stream, group, consumer string) mq.Backend {
	return &mq.FuncBackend{
		PublishFunc: func(ctx context.Context, m mq.Message) error {
			return client.Do(ctx, client.B().Xadd().Key(stream).Id("*").FieldValue().FieldValue("body", encode(m)).Build()).Error()
		},
		ReceiveFunc: func(ctx context.Context) (*mq.Delivery, error) {
			for {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				result, err := client.Do(ctx, client.B().Xreadgroup().Group(group, consumer).Count(1).Block(1000).Streams().Key(stream).Id(">").Build()).ToMap()
				if rueidis.IsRedisNil(err) {
					continue
				}
				if err != nil {
					return nil, err
				}
				streamResult, ok := result[stream]
				if !ok {
					continue
				}
				rows, err := streamResult.ToArray()
				if err != nil {
					return nil, err
				}
				if len(rows) == 0 {
					continue
				}
				entry, err := rows[0].ToArray()
				if err != nil {
					return nil, err
				}
				if len(entry) != 2 {
					return nil, mq.ErrUnsupported
				}
				id, err := entry[0].ToString()
				if err != nil {
					return nil, err
				}
				fields, err := entry[1].AsStrMap()
				if err != nil {
					return nil, err
				}
				return mq.NewDelivery(mq.Message{Body: []byte(fields["body"])}, func(c context.Context) error {
					return client.Do(c, client.B().Xack().Key(stream).Group(group).Id(id).Build()).Error()
				}, nil), nil
			}
		},
		CloseFunc: func() error { client.Close(); return nil },
	}
}
