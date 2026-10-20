package dispatch

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

// chanPool is the ChanPerLane kind: one buffered channel per lane and one
// goroutine receiving from each. It is the shape most Go programs reach for,
// and the baseline the queue-based kinds have to beat: the runtime's channel
// is already a bounded ring with a park/wake protocol, so this kind pays no
// wait strategy at all, but it also cannot batch a lane, steal from one, or
// tell a producer which lane freed a slot.
//
// The item channels are never closed. A producer that is inside Submit holds
// a slot on the admission gate, so the workers cannot stop while a send may
// still land, and closing the pool wakes producers through a separate
// done channel instead of through a close that a live sender would fault on.
type chanPool[T any] struct {
	kind  Kind
	cfg   Config
	h     Handler[T]
	chans []chan T
	done  chan struct{}
	idle  idleWait
	g     gate
	rr    atomic.Uint64
	wg    sync.WaitGroup
	once  closeOnce
}

func newChanPool[T any](k Kind, cfg Config, h Handler[T]) (*chanPool[T], error) {
	idle, err := newIdleStrategy(cfg)
	if err != nil {
		return nil, err
	}
	p := &chanPool[T]{
		kind:  k,
		cfg:   cfg,
		h:     h,
		chans: make([]chan T, cfg.Lanes),
		done:  make(chan struct{}),
		idle:  idleWait{s: idle},
	}
	for i := range p.chans {
		p.chans[i] = make(chan T, cfg.Capacity)
	}
	p.wg.Add(cfg.Workers)
	for w := range cfg.Workers {
		go p.worker(w)
	}
	return p, nil
}

// Kind implements Pool.
func (p *chanPool[T]) Kind() Kind { return p.kind }

// Workers implements Pool.
func (p *chanPool[T]) Workers() int { return p.cfg.Workers }

// Lanes implements Router.
func (p *chanPool[T]) Lanes() int { return len(p.chans) }

// Route implements Router: equal keys reach one channel, and therefore one
// receiving goroutine, which is what preserves their order.
func (p *chanPool[T]) Route(r Route) int {
	n := len(p.chans)
	if n == 1 {
		return 0
	}
	if !r.Keyed {
		return int(p.rr.Add(1) % uint64(n))
	}
	return int(mix(r.Key) % uint64(n))
}

// Pending implements Pool.
func (p *chanPool[T]) Pending() int { return int(p.g.load()) }

// TrySubmit implements Pool.
func (p *chanPool[T]) TrySubmit(r Route, v T) bool {
	if !p.enter() {
		return false
	}
	select {
	case p.chans[p.Route(r)] <- v:
		return true
	default:
		p.release()
		return false
	}
}

// Submit implements Pool.
func (p *chanPool[T]) Submit(ctx context.Context, r Route, v T) error {
	if !p.enter() {
		return ErrClosed
	}
	ch := p.chans[p.Route(r)]
	select {
	case ch <- v:
		return nil
	default:
	}
	select {
	case ch <- v:
		return nil
	case <-ctx.Done():
		p.release()
		return ctx.Err()
	case <-p.done:
		p.release()
		return ErrClosed
	}
}

// Drain implements Pool.
func (p *chanPool[T]) Drain(ctx context.Context) error {
	return p.idle.drain(ctx, p.g.load)
}

// Close implements Pool.
func (p *chanPool[T]) Close() error {
	p.once.do(func() {
		p.g.closing.Store(true)
		close(p.done)
		p.wg.Wait()
		p.idle.s.NotifyAll()
	})
	return nil
}

// enter reserves an admission slot for one submission and reports whether the
// pool took it.
func (p *chanPool[T]) enter() bool {
	if p.g.reserve() {
		return true
	}
	p.withdraw()
	return false
}

// withdraw gives back a reservation the closing flag refused. This kind's
// workers poll the counter rather than parking on it, so only a Drain caller
// can be waiting for it to fall — but that caller does park, and the
// withdrawal is the last event that can wake it.
func (p *chanPool[T]) withdraw() {
	if p.g.withdraw() {
		p.idle.notifyIdle()
	}
}

// release accounts for one item that left the pool.
func (p *chanPool[T]) release() {
	if p.g.leave() == 0 {
		p.idle.notifyIdle()
	}
}

// worker receives from one channel until the pool is closed and nothing is
// left anywhere. The counter, not the channel, decides when it is safe to
// stop: a producer that already passed the admission gate may still be
// choosing between its send and the done channel.
func (p *chanPool[T]) worker(w int) {
	defer p.wg.Done()
	ch := p.chans[w%len(p.chans)]
	for {
		select {
		case v := <-ch:
			p.h(v)
			p.release()
			continue
		default:
		}
		if p.g.closed() {
			if p.g.load() == 0 {
				return
			}
			// Closing, but a producer or another worker is still busy.
			runtime.Gosched()
			continue
		}
		select {
		case v := <-ch:
			p.h(v)
			p.release()
		case <-p.done:
		}
	}
}
