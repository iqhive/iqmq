package wait

import (
	"context"
	"sync"
	"sync/atomic"
)

// generation is one epoch of an event count: waiters of the generation park
// on ch, which the next notification closes.
type generation struct {
	ch chan struct{}
}

// eventCount is a monotonic event counter. Prepare records the current
// epoch and generation; a notification advances the epoch and, when anyone
// is waiting, closes the generation's channel and installs a fresh one.
// Every notification is a broadcast, so one Notify with many waiters wakes
// them all; in exchange the producer fast path with no waiters is one
// atomic add and one atomic load.
type eventCount struct {
	epoch   counter
	waiters atomic.Int64
	mu      sync.Mutex // serialises generation replacement
	gen     atomic.Pointer[generation]
}

func newEventCount() *eventCount {
	s := &eventCount{}
	s.gen.Store(&generation{ch: make(chan struct{})})
	return s
}

func (s *eventCount) Prepare() Ticket {
	s.waiters.Add(1)
	return Ticket{epoch: s.epoch.v.Load(), g: s.gen.Load()}
}

func (s *eventCount) Wait(ctx context.Context, t Ticket) error {
	defer s.waiters.Add(-1)
	if s.epoch.v.Load() != t.epoch {
		return nil
	}
	select {
	case <-t.g.ch:
		return nil
	case <-ctx.Done():
		select {
		case <-t.g.ch:
			return nil
		default:
			return ctx.Err()
		}
	}
}

func (s *eventCount) Cancel(Ticket) { s.waiters.Add(-1) }

func (s *eventCount) Notify() { s.NotifyAll() }

func (s *eventCount) NotifyAll() {
	s.epoch.v.Add(1)
	if s.waiters.Load() == 0 {
		return
	}
	s.mu.Lock()
	old := s.gen.Load()
	s.gen.Store(&generation{ch: make(chan struct{})})
	close(old.ch)
	s.mu.Unlock()
}

func (s *eventCount) HasWaiters() bool { return s.waiters.Load() > 0 }
