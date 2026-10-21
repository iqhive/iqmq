package wait

import (
	"context"
	"sync"
)

// condStrategy parks waiters on a sync.Cond guarding an epoch. Because a
// sync.Cond cannot select on a context, Wait registers a Broadcast with
// context.AfterFunc so cancellation wakes every waiter to re-check. The
// Broadcast takes the mutex: a waiter that has read ctx.Err() but not yet
// entered cond.Wait would otherwise miss it and sleep through the cancel.
type condStrategy struct {
	mu      sync.Mutex
	cond    sync.Cond
	epoch   uint64
	waiters int
}

func newCondStrategy() *condStrategy {
	s := &condStrategy{}
	s.cond.L = &s.mu
	return s
}

func (s *condStrategy) Prepare() Ticket {
	s.mu.Lock()
	s.waiters++
	e := s.epoch
	s.mu.Unlock()
	return Ticket{epoch: e}
}

func (s *condStrategy) Wait(ctx context.Context, t Ticket) error {
	s.mu.Lock()
	if s.epoch != t.epoch {
		s.waiters--
		s.mu.Unlock()
		return nil
	}
	if ctx.Err() != nil {
		s.waiters--
		s.mu.Unlock()
		return ctx.Err()
	}
	stop := context.AfterFunc(ctx, s.wakeAll)
	for s.epoch == t.epoch && ctx.Err() == nil {
		s.cond.Wait()
	}
	s.waiters--
	err := ctx.Err()
	if s.epoch != t.epoch {
		err = nil
	}
	s.mu.Unlock()
	stop()
	return err
}

// wakeAll is the cancellation Broadcast, under the mutex (condStrategy).
func (s *condStrategy) wakeAll() {
	s.mu.Lock()
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *condStrategy) Cancel(Ticket) {
	s.mu.Lock()
	s.waiters--
	s.mu.Unlock()
}

func (s *condStrategy) Notify() {
	s.mu.Lock()
	s.epoch++
	s.mu.Unlock()
	s.cond.Signal()
}

func (s *condStrategy) NotifyAll() {
	s.mu.Lock()
	s.epoch++
	s.mu.Unlock()
	s.cond.Broadcast()
}

func (s *condStrategy) HasWaiters() bool {
	s.mu.Lock()
	n := s.waiters
	s.mu.Unlock()
	return n > 0
}
