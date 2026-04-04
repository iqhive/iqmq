package iqmq

import (
	"context"
	"sync"
)

type asyncJob struct {
	ctx     context.Context
	message Message
	done    chan error
}

// AsyncPublisher has one worker and a bounded input queue. Close drains accepted work.
type AsyncPublisher struct {
	client *Client
	queue  chan asyncJob
	done   chan struct{}
	mu     sync.RWMutex
	closed bool
}

func NewAsyncPublisher(client *Client, capacity int) *AsyncPublisher {
	if capacity < 1 {
		panic("mq: async capacity must be positive")
	}
	p := &AsyncPublisher{client: client, queue: make(chan asyncJob, capacity), done: make(chan struct{})}
	go func() {
		defer close(p.done)
		for job := range p.queue {
			job.done <- client.Publish(job.ctx, job.message)
			close(job.done)
		}
	}()
	return p
}
func (p *AsyncPublisher) Publish(ctx context.Context, m Message) (<-chan error, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.Body = append([]byte(nil), m.Body...)
	result := make(chan error, 1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case p.queue <- asyncJob{ctx: ctx, message: m, done: result}:
		return result, nil
	}
}
func (p *AsyncPublisher) Close() error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()
	<-p.done
	return nil
}
