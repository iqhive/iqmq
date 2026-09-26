// Package typed binds a Go value type to a JSON message publisher.
package typed

import (
	"context"
	mq "github.com/iqhive/iqmq/legacy"
	"github.com/iqhive/iqmq/legacy/codec"
)

type Publisher[T any] struct{ client *mq.Client }

func NewPublisher[T any](client *mq.Client) *Publisher[T] { return &Publisher[T]{client: client} }
func (p *Publisher[T]) Publish(ctx context.Context, value T) error {
	m, err := codec.Encode(value)
	if err != nil {
		return err
	}
	return p.client.Publish(ctx, m)
}
