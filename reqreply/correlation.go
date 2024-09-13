// Package reqreply matches replies to bounded, cancellable requests.
package reqreply

import (
	"context"
	"errors"
	"fmt"
	mq "bitbucket.org/iqhive/iqmq"
	"sync"
	"sync/atomic"
)

var ErrCapacity = errors.New("reqreply: pending limit reached")

type Client struct {
	mu      sync.Mutex
	pending map[string]chan mq.Message
	limit   int
	next    uint64
}

func New(limit int) *Client {
	if limit < 1 {
		panic("reqreply: limit must be positive")
	}
	return &Client{limit: limit, pending: make(map[string]chan mq.Message)}
}
func (c *Client) Request(ctx context.Context, message mq.Message, publish func(context.Context, string, mq.Message) error) (mq.Message, error) {
	if err := ctx.Err(); err != nil {
		return mq.Message{}, err
	}
	id := fmt.Sprint(atomic.AddUint64(&c.next, 1))
	reply := make(chan mq.Message, 1)
	c.mu.Lock()
	if len(c.pending) >= c.limit {
		c.mu.Unlock()
		return mq.Message{}, ErrCapacity
	}
	c.pending[id] = reply
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, id); c.mu.Unlock() }()
	if err := publish(ctx, id, message); err != nil {
		return mq.Message{}, err
	}
	select {
	case m := <-reply:
		return m, nil
	case <-ctx.Done():
		return mq.Message{}, ctx.Err()
	}
}
func (c *Client) Reply(id string, message mq.Message) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply, ok := c.pending[id]
	if !ok {
		return false
	}
	message.Body = append([]byte(nil), message.Body...)
	select {
	case reply <- message:
		delete(c.pending, id)
		return true
	default:
		return false
	}
}
