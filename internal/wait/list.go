package wait

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

// Waiter states.
const (
	stWaiting  uint32 = iota // registered; owner spinning or about to park
	stParked                 // owner blocked on ch
	stNotified               // a notification was delivered
	stCanceled               // owner withdrew before being notified
)

// waiter is one registered consumer. Waiters are pooled per strategy and
// keep their channel across reuse.
type waiter struct {
	state      atomic.Uint32
	ch         chan struct{} // buffered 1; at most one send per registration
	next, prev *waiter       // FIFO list links, guarded by listStrategy.mu
	listed     bool          // in the FIFO, guarded by listStrategy.mu
}

// listStrategy backs Chan, SpinPark and Hybrid: a FIFO of waiters under a
// mutex, wake-one Notify, and an optional spin and yield phase before
// parking on the waiter's channel.
type listStrategy struct {
	mu         sync.Mutex
	head, tail *waiter
	free       *waiter // singly linked through next
	waiters    atomic.Int64
	spinN      int
	yieldN     int
	adaptive   bool
	spinBudget atomic.Int64
}

func newList(spinN, yieldN int, adaptive bool) *listStrategy {
	s := &listStrategy{spinN: spinN, yieldN: yieldN, adaptive: adaptive}
	s.spinBudget.Store(int64(spinN))
	return s
}

func (s *listStrategy) Prepare() Ticket {
	s.mu.Lock()
	w := s.free
	if w != nil {
		s.free = w.next
		w.next = nil
	} else {
		w = &waiter{ch: make(chan struct{}, 1)}
	}
	w.state.Store(stWaiting)
	w.listed = true
	w.prev = s.tail
	if s.tail != nil {
		s.tail.next = w
	} else {
		s.head = w
	}
	s.tail = w
	s.mu.Unlock()
	s.waiters.Add(1)
	return Ticket{w: w}
}

// unlinkLocked removes w from the FIFO; the caller holds mu.
func (s *listStrategy) unlinkLocked(w *waiter) {
	if !w.listed {
		return
	}
	w.listed = false
	if w.prev != nil {
		w.prev.next = w.next
	} else {
		s.head = w.next
	}
	if w.next != nil {
		w.next.prev = w.prev
	} else {
		s.tail = w.prev
	}
	w.next, w.prev = nil, nil
}

// release returns w to the free list and drops the waiter count.
func (s *listStrategy) release(w *waiter) {
	s.mu.Lock()
	s.unlinkLocked(w)
	w.next = s.free
	s.free = w
	s.mu.Unlock()
	s.waiters.Add(-1)
}

func (s *listStrategy) Wait(ctx context.Context, t Ticket) error {
	w := t.w
	budget := int(s.spinBudget.Load())
	for i := range budget {
		if w.state.Load() == stNotified {
			s.spun(true)
			s.release(w)
			return nil
		}
		if i&ctxCheckMask == ctxCheckMask && ctx.Err() != nil {
			return s.cancelWait(ctx, w)
		}
	}
	for range s.yieldN {
		if w.state.Load() == stNotified {
			s.spun(true)
			s.release(w)
			return nil
		}
		runtime.Gosched()
	}
	s.spun(false)
	if !w.state.CompareAndSwap(stWaiting, stParked) {
		s.release(w) // notified between the last poll and the CAS
		return nil
	}
	select {
	case <-w.ch:
		s.release(w)
		return nil
	case <-ctx.Done():
		if !w.state.CompareAndSwap(stParked, stCanceled) {
			<-w.ch // Notify won the race; consume its token
			s.release(w)
			return nil
		}
		s.release(w) // Notify, if it already dequeued w, forwards on seeing stCanceled
		return ctx.Err()
	}
}

// cancelWait withdraws a still-spinning waiter because ctx is done.
func (s *listStrategy) cancelWait(ctx context.Context, w *waiter) error {
	if !w.state.CompareAndSwap(stWaiting, stCanceled) {
		s.release(w)
		return nil // notified concurrently
	}
	s.release(w)
	return ctx.Err()
}

// spun adapts the Hybrid spin budget: double it after a spin-satisfied wait,
// halve it after a wait that had to park.
func (s *listStrategy) spun(satisfied bool) {
	if !s.adaptive {
		return
	}
	b := s.spinBudget.Load()
	if satisfied {
		if b < int64(s.spinN)*4 {
			s.spinBudget.Store(b * 2)
		}
	} else if b > 8 {
		s.spinBudget.Store(b / 2)
	}
}

func (s *listStrategy) Cancel(t Ticket) {
	w := t.w
	if !w.state.CompareAndSwap(stWaiting, stCanceled) {
		// Already notified: hand the notification to the next waiter.
		s.release(w)
		s.Notify()
		return
	}
	s.release(w)
}

func (s *listStrategy) Notify() {
	for {
		s.mu.Lock()
		w := s.head
		if w == nil {
			s.mu.Unlock()
			return
		}
		s.unlinkLocked(w)
		s.mu.Unlock()
		switch w.state.Swap(stNotified) {
		case stParked:
			w.ch <- struct{}{}
			return
		case stCanceled:
			continue // owner withdrew concurrently; wake the next one
		default:
			return // owner is spinning and will observe stNotified
		}
	}
}

func (s *listStrategy) NotifyAll() {
	// Detach the whole FIFO under the lock and snapshot the waiters: once
	// unlocked, an owner may release (and Prepare may reuse) any of them, so
	// their links must not be touched again.
	var buf [16]*waiter
	ws := buf[:0]
	s.mu.Lock()
	for w := s.head; w != nil; {
		next := w.next
		w.listed = false
		w.next, w.prev = nil, nil
		ws = append(ws, w)
		w = next
	}
	s.head, s.tail = nil, nil
	s.mu.Unlock()
	for _, w := range ws {
		if w.state.Swap(stNotified) == stParked {
			w.ch <- struct{}{}
		}
	}
}

func (s *listStrategy) HasWaiters() bool { return s.waiters.Load() > 0 }
