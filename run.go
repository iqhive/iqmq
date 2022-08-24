package iqmq

import (
	"context"
	"fmt"
)

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
				return &ProcessingError{Cause: err, Settlement: nackErr}
			}
			return err
		}
		if err = d.Ack(ctx); err != nil {
			return err
		}
	}
}

// ProcessingError retains both the handler failure and its failed settlement.
type ProcessingError struct{ Cause, Settlement error }

func (e *ProcessingError) Error() string {
	return fmt.Sprintf("handler: %v; settlement: %v", e.Cause, e.Settlement)
}
func (e *ProcessingError) Unwrap() error { return e.Cause }
