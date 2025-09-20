package wait

import (
	"context"
	"runtime"
	"sync/atomic"
)

// epochStrategy backs Spin and Yield: Prepare snapshots an epoch, Notify
// advances it, and Wait polls until the epoch moves or ctx is done. Both
// Notify and NotifyAll are broadcasts.
type epochStrategy struct {
	epoch   counter
	waiters atomic.Int64
	spin    func(ctx context.Context, epoch *atomic.Uint64, want uint64) error
}

func (s *epochStrategy) Prepare() Ticket {
	s.waiters.Add(1)
	return Ticket{epoch: s.epoch.v.Load()}
}

func (s *epochStrategy) Wait(ctx context.Context, t Ticket) error {
	err := s.spin(ctx, &s.epoch.v, t.epoch)
	s.waiters.Add(-1)
	return err
}

func (s *epochStrategy) Cancel(Ticket) { s.waiters.Add(-1) }

func (s *epochStrategy) Notify() { s.epoch.v.Add(1) }

func (s *epochStrategy) NotifyAll() { s.epoch.v.Add(1) }

func (s *epochStrategy) HasWaiters() bool { return s.waiters.Load() > 0 }

// ctxCheckMask bounds how often a spinner consults ctx.
const ctxCheckMask = 63

func spinForever(ctx context.Context, epoch *atomic.Uint64, want uint64) error {
	for i := 0; ; i++ {
		if epoch.Load() != want {
			return nil
		}
		if i&ctxCheckMask == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		// A pure spinner still has to let the runtime preempt it on GOMAXPROCS=1;
		// Go's asynchronous preemption handles that without a Gosched.
	}
}

func yieldForever(ctx context.Context, epoch *atomic.Uint64, want uint64) error {
	for i := 0; ; i++ {
		if epoch.Load() != want {
			return nil
		}
		if i&ctxCheckMask == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		runtime.Gosched()
	}
}
