// Package iqmq provides a small message API shared by broker adapters.
package iqmq

import (
	"context"
	"errors"
	"sync"
)

var ErrUnsupported = errors.New("mq: unsupported operation")
var ErrClosed = errors.New("mq: closed")
var ErrSettled = errors.New("mq: delivery already settled")

// Message owns its body. Brokers may redeliver a message after a failure.
type Message struct{ Body []byte }

// Backend is configured for one destination by its driver.
type Backend interface {
	Publish(context.Context, Message) error
	Receive(context.Context) (*Delivery, error)
	Close() error
}

// Delivery settles at most once. A failed settlement may be retried.
type Delivery struct {
	Message   Message
	mu        sync.Mutex
	settled   bool
	ack, nack func(context.Context) error
}

func NewDelivery(message Message, ack, nack func(context.Context) error) *Delivery {
	return &Delivery{Message: message, ack: ack, nack: nack}
}

func (d *Delivery) settle(ctx context.Context, action func(context.Context) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return ErrSettled
	}
	if action == nil {
		return ErrUnsupported
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := action(ctx); err != nil {
		return err
	}
	d.settled = true
	return nil
}

func (d *Delivery) Ack(ctx context.Context) error  { return d.settle(ctx, d.ack) }
func (d *Delivery) Nack(ctx context.Context) error { return d.settle(ctx, d.nack) }

type Client struct{ backend Backend }

func NewClient(backend Backend) *Client { return &Client{backend: backend} }
func (c *Client) Publish(ctx context.Context, message Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	message.Body = append([]byte(nil), message.Body...)
	return c.backend.Publish(ctx, message)
}
func (c *Client) Receive(ctx context.Context) (*Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.backend.Receive(ctx)
}
func (c *Client) Close() error { return c.backend.Close() }
