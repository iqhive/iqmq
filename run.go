package iqmq

import "context"

type Handler func(context.Context, Message) error

// Run processes one delivery at a time until cancelled or an operation fails.
func (c *Client) Run(ctx context.Context, handler Handler) error {
	for {
		d, err := c.Receive(ctx)
		if err != nil {
			return err
		}
		if err = handler(ctx, d.Message); err != nil {
			if nackErr := d.Nack(ctx); nackErr != nil {
				return nackErr
			}
			return err
		}
		if err = d.Ack(ctx); err != nil {
			return err
		}
	}
}
