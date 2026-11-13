package dispatch

import (
	"context"
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
	// empty is closed once the count falls to zero while the pool is
	// closing, which is when a worker may stop. Nothing can be admitted
	// after that (gate), so it stays true once closed.
	empty     chan struct{}
	emptyOnce sync.Once
	idle      idleWait
	g         gate
	rr        atomic.Uint64
	wg        sync.WaitGroup
	once      closeOnce
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
		empty: make(chan struct{}),
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

// withdraw gives back a reservation the closing flag refused. The
// reservation was visible to load while it stood, so a worker may have read
// the pair "closing, and the count is not zero" and parked on it, and a Drain
// caller may have parked on the count alone; this withdrawal is then the last
// event that can ever wake either of them.
func (p *chanPool[T]) withdraw() {
	if p.g.withdraw() {
		p.fellIdle()
	}
}

// release accounts for one item that left the pool.
func (p *chanPool[T]) release() {
	if p.g.leave() == 0 {
		p.fellIdle()
	}
}

// fellIdle announces that the pool's count has reached zero: it releases
// Drain callers, and when the pool is closing it lets every worker stop,
// because a zero count is half of the worker exit condition and a worker that
// read a non-zero one is parked on empty. A worker parks only once it has read
// the closing flag and then that count, so the fall to zero it waits for
// comes after the flag was set, and this reads the flag after the fall.
func (p *chanPool[T]) fellIdle() {
	p.idle.notifyIdle()
	if p.g.closed() {
		p.emptied()
	}
}

// emptied closes empty, once.
func (p *chanPool[T]) emptied() { p.emptyOnce.Do(func() { close(p.empty) }) }

// worker receives from one channel until the pool is closed and nothing is
// left anywhere. The counter, not the channel, decides when it is safe to
// stop: a producer that already passed the admission gate may still be
// choosing between its send and the done channel.
//
// Once the pool is closing, a worker with nothing in its channel parks until
// either something arrives there or the count falls to zero (empty). It used
// to poll the count with runtime.Gosched, so every idle worker burnt a CPU
// for as long as the slowest handler ran, one a drain timeout had given up
// on say, and inside a testing/synctest bubble a goroutine that polls is
// never durably blocked, so the bubble's clock never reached that handler's
// wake-up.
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
			select {
			case v := <-ch:
				p.h(v)
				p.release()
			case <-p.empty:
				return
			}
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
