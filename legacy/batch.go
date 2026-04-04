package iqmq

import (
	"context"
	"errors"
)

var ErrBatchTooLarge = errors.New("mq: batch exceeds 1024 messages")

// PublishBatch returns the completed prefix. Publication is not atomic.
func (c *Client) PublishBatch(ctx context.Context, messages []Message) (int, error) {
	if len(messages) > 1024 {
		return 0, ErrBatchTooLarge
	}
	for i, m := range messages {
		if err := c.Publish(ctx, m); err != nil {
			return i, err
		}
	}
	return len(messages), nil
}
