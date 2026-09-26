package dispatch

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/iqhive/iqmq/internal/queue"
	"github.com/iqhive/iqmq/internal/wait"
)

// Lane claim states, used by the stealing kind. Holding the claim across the
// handler call is what keeps per-key order under stealing: two workers never
// run items of one lane at the same time. Wanted records that somebody was
// turned away while the lane was held, so that whoever lets go knows it has
// to wake the lane's owner — which is what stops an item from sitting in a
// lane its owner already scanned and found locked.
const (
	laneFree int32 = iota
	laneHeld
	laneWanted
)

// lane is one routing bucket: a bounded queue plus the claim.
type lane[T any] struct {
	_     pad
	q     queue.Queue[T]
	claim atomic.Int32
	_     pad
}

// tryClaim takes the lane, or records that the caller was turned away.
func (l *lane[T]) tryClaim() bool {
	for {
		switch l.claim.Load() {
		case laneFree:
			if l.claim.CompareAndSwap(laneFree, laneHeld) {
				return true
			}
		case laneHeld:
			if l.claim.CompareAndSwap(laneHeld, laneWanted) {
				return false
			}
		default: // laneWanted: somebody is already waiting for it
			return false
		}
	}
}

// release lets the lane go and reports whether somebody was turned away
// while it was held and must therefore be woken.
func (l *lane[T]) release() bool { return l.claim.Swap(laneFree) == laneWanted }

// lanePool implements every kind whose lanes are queues: HashLanes,
// PerPartition, SharedQueue, WorkStealing and RoundRobin. They differ in the
// router, in whether one lane is shared by all workers, and in whether an
// idle worker may take a foreign lane; everything else — the admission gate,
// the wake protocol, draining and closing — is one implementation, so the
// bake-off measures the routing and stealing decisions rather than accidental
// differences between five hand-written pools.
type lanePool[T any] struct {
	kind   Kind
	cfg    Config
	h      Handler[T]
	lanes  []lane[T]
	wq     []waiterSet // one per worker; one in total when shared
	nfull  wait.Strategy
	idle   idleWait
	g      gate
	rr     atomic.Uint64 // keyless round-robin cursor
	srr    atomic.Uint64 // steal probe rotation
	shared bool          // every worker pops the one lane
	claim  bool          // a lane must be claimed before it is run
	steal  int           // foreign lanes probed by an idle worker
	wg     sync.WaitGroup
	once   closeOnce
}

func newLanePool[T any](k Kind, cfg Config, h Handler[T]) *lanePool[T] {
	p := &lanePool[T]{
		kind:   k,
		cfg:    cfg,
		h:      h,
		lanes:  make([]lane[T], cfg.Lanes),
		nfull:  wait.New(cfg.Wait),
		idle:   idleWait{s: newIdleStrategy(cfg)},
		shared: k == SharedQueue,
		claim:  k.Steals(),
	}
	if k.Steals() {
		p.steal = cfg.Steal
	}
	for i := range p.lanes {
		p.lanes[i].q = queue.New[T](cfg.Queue, cfg.Capacity)
	}
	waiters := cfg.Workers
	if p.shared {
		waiters = 1
	}
	p.wq = make([]waiterSet, waiters)
	for i := range p.wq {
		p.wq[i].s = wait.New(cfg.Wait)
	}
	p.wg.Add(cfg.Workers)
	for w := range cfg.Workers {
		go p.worker(w)
	}
	return p
}

// Kind implements Pool.
func (p *lanePool[T]) Kind() Kind { return p.kind }

// Workers implements Pool.
func (p *lanePool[T]) Workers() int { return p.cfg.Workers }

// Lanes implements Router.
func (p *lanePool[T]) Lanes() int { return len(p.lanes) }

// Route implements Router. The switch is one well-predicted branch per item;
// it is what the family compares.
func (p *lanePool[T]) Route(r Route) int {
	n := len(p.lanes)
	if n == 1 {
		return 0
	}
	switch p.kind {
	case PerPartition:
		if r.Partition >= 0 {
			return int(uint64(uint32(r.Partition)) % uint64(n))
		}
	case RoundRobin:
		return p.nextLane(n)
	default:
	}
	if !r.Keyed {
		return p.nextLane(n)
	}
	return int(mix(r.Key) % uint64(n))
}

// nextLane hands out lanes in turn for items that have no ordering key.
func (p *lanePool[T]) nextLane(n int) int { return int(p.rr.Add(1) % uint64(n)) }

// Pending implements Pool.
func (p *lanePool[T]) Pending() int { return int(p.g.load()) }

// TrySubmit implements Pool.
func (p *lanePool[T]) TrySubmit(r Route, v T) bool {
	if !p.enter() {
		return false
	}
	i := p.Route(r)
	if !p.lanes[i].q.TryPush(v) {
		p.release()
		return false
	}
	p.wake(i)
	return true
}

// Submit implements Pool.
func (p *lanePool[T]) Submit(ctx context.Context, r Route, v T) error {
	for {
		if p.g.closed() {
			return ErrClosed
		}
		if p.TrySubmit(r, v) {
			return nil
		}
		// Announce the wait before the last look at the lane: a worker that
		// frees a slot after that look is guaranteed to wake this ticket.
		t := p.nfull.Prepare()
		if p.g.closed() {
			p.nfull.Cancel(t)
			return ErrClosed
		}
		if p.TrySubmit(r, v) {
			p.nfull.Cancel(t)
			return nil
		}
		if err := p.nfull.Wait(ctx, t); err != nil {
			return err
		}
	}
}

// Drain implements Pool.
func (p *lanePool[T]) Drain(ctx context.Context) error {
	return p.idle.drain(ctx, p.g.load)
}

// Close implements Pool.
func (p *lanePool[T]) Close() error {
	p.once.do(func() {
		p.g.closing.Store(true)
		p.nfull.NotifyAll()
		p.wakeAll()
		p.wg.Wait()
		p.idle.s.NotifyAll()
	})
	return nil
}

// waiterFor returns the parking spot of the worker that owns lane i.
func (p *lanePool[T]) waiterFor(i int) int {
	if p.shared {
		return 0
	}
	return i % len(p.wq)
}

// wake notifies the worker that owns lane i, or — when that worker is busy
// and the kind steals — one other parked worker, which will find the item on
// its bounded steal probe.
func (p *lanePool[T]) wake(i int) {
	ws := p.wq[p.waiterFor(i)].s
	if ws.HasWaiters() {
		ws.Notify()
		return
	}
	if p.steal == 0 {
		return
	}
	for probe := 1; probe <= 2 && probe < len(p.wq); probe++ {
		other := p.wq[(p.waiterFor(i)+probe)%len(p.wq)].s
		if other.HasWaiters() {
			other.Notify()
			return
		}
	}
}

// wakeAll releases every parked worker so it can re-check the pool state.
func (p *lanePool[T]) wakeAll() {
	for i := range p.wq {
		p.wq[i].s.NotifyAll()
	}
}

// enter reserves an admission slot for one submission and reports whether the
// pool took it.
func (p *lanePool[T]) enter() bool {
	if p.g.reserve() {
		return true
	}
	p.withdraw()
	return false
}

// withdraw gives back a reservation the closing flag refused. The reservation
// was visible to load while it stood, so a worker may have read the pair
// "closing, and the count is not zero" and parked on it, and a Drain caller
// may have parked on the count alone; this withdrawal is then the last event
// that can ever wake either of them.
func (p *lanePool[T]) withdraw() {
	if p.g.withdraw() {
		p.fellIdle()
	}
}

// release accounts for one item that left the pool, either because it was
// handled or because its submission failed.
func (p *lanePool[T]) release() {
	if p.g.leave() == 0 {
		p.fellIdle()
	}
}

// fellIdle announces that the pool's count has reached zero: it releases
// Drain callers, and when the pool is closing it wakes every worker, because
// a zero count is half of the worker exit condition and a worker that
// sampled a non-zero one is already parked.
func (p *lanePool[T]) fellIdle() {
	p.idle.notifyIdle()
	if p.g.closed() {
		p.wakeAll()
	}
}

// worker is one dispatch goroutine. It runs its own lanes, steals when the
// kind allows it, and parks on its own strategy when there is nothing to do.
func (p *lanePool[T]) worker(w int) {
	defer p.wg.Done()
	ws := p.wq[p.waiterFor(w)].s
	for {
		if p.scan(w) {
			continue
		}
		t := ws.Prepare()
		if p.scan(w) {
			ws.Cancel(t)
			continue
		}
		// Nothing anywhere and nothing on its way in: the pool is closing
		// and everything accepted has been handled.
		if p.g.closed() && p.g.load() == 0 {
			ws.Cancel(t)
			return
		}
		_ = ws.Wait(context.Background(), t)
	}
}

// scan runs whatever work worker w may take, and reports whether it ran any.
func (p *lanePool[T]) scan(w int) bool {
	if p.shared {
		return p.runLane(0)
	}
	found := false
	for i := w; i < len(p.lanes); i += p.cfg.Workers {
		if p.runLane(i) {
			found = true
		}
	}
	if found || p.steal == 0 {
		return found
	}
	start := int(p.srr.Add(1) % uint64(len(p.lanes)))
	for probe := range p.steal {
		i := (start + probe) % len(p.lanes)
		if i%p.cfg.Workers == w {
			continue
		}
		// Looking before claiming keeps idle workers off each other's cache
		// lines: a lane that looks empty is left alone. The reading is a
		// hint, and a hint is enough here — a lane this probe passes over is
		// still its owner's to run, and the owner never uses the hint.
		if p.lanes[i].q.Len() == 0 {
			continue
		}
		if p.runLane(i) {
			return true
		}
	}
	return false
}

// runLane handles up to Batch items from lane i and reports whether it
// handled any. Batch bounds how long one lane may starve
// the worker's other lanes; the claim, where the kind needs one, is held
// across the handler so that two workers never run one lane at once.
func (p *lanePool[T]) runLane(i int) bool {
	l := &p.lanes[i]
	if p.claim && !l.tryClaim() {
		// Somebody is running this lane. The claim now records that it was
		// wanted, so whoever holds it wakes the lane's owner on the way out:
		// an owner that scanned the lane here and found it locked would
		// otherwise park without seeing what is in it.
		return false
	}
	n := 0
	for n < p.cfg.Batch {
		v, ok := l.q.TryPop()
		if !ok {
			break
		}
		n++
		// The slot is free the moment the item is popped, so a producer
		// waiting for room is released now and not after the handler, which
		// may run for as long as the handler deadline allows.
		if p.nfull.HasWaiters() {
			p.nfull.NotifyAll()
		}
		p.h(v)
		p.release()
	}
	if p.claim && l.release() {
		// Somebody wanted this lane while it was held. Its owner registers
		// its ticket before it re-scans, so this notification either finds
		// that ticket or arrives before the scan that finds the work itself.
		p.wakeOwner(i)
	}
	return n > 0
}

// wakeOwner notifies the worker that owns lane i, if it is parked.
func (p *lanePool[T]) wakeOwner(i int) {
	ws := p.wq[p.waiterFor(i)].s
	if ws.HasWaiters() {
		ws.Notify()
	}
}
