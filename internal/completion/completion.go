// Package completion delivers the outcome of an asynchronous publish to the
// caller (the API).
//
// An async publish returns immediately and finishes later, on whatever
// goroutine the driver's I/O loop runs on. The contract is narrow and the
// whole family exists to meet it at zero allocations: a non-nil error from
// Start means the callback will never run; a nil error means it runs exactly
// once; the message stays borrowed until it does; there is no goroutine and
// no timer per message; and a callback must never block the driver's
// progress.
//
// The kinds differ in where the outcome is delivered and in what the caller
// gets back. Inline runs the callback on the goroutine that completed the
// publish, which costs nothing and puts user code on the driver's I/O path.
// Deferred hands the outcome to one completion goroutine through a bounded
// ring, so a slow callback cannot stall the driver. Future and Channel are
// the adapters (`x/future` and a per-call channel): they hand the caller
// something to wait on, and pay an allocation per message to do it, which is
// exactly what the bake-off has to weigh.
//
// Re-entrancy is part of the contract, not an accident: a callback may start
// and complete further publishes. No lock is held while a callback runs, and
// the slot a completion used is free before it is called, so a callback that
// publishes again cannot deadlock or run out of slots that its own completion
// just freed.
package completion

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/queue"
	"github.com/iqhive/iqmq/internal/wait"
)

// Kind identifies a completion variant. Auto resolves to AutoKind.
type Kind uint8

// Kinds. Inline is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// Inline runs the callback on the goroutine that completed the publish:
	// the reference, and the cheapest possible delivery.
	Inline
	// Deferred hands the outcome to one completion goroutine through a
	// bounded multi-producer, single-consumer ring, so user code never runs
	// on the driver's I/O goroutine.
	Deferred
	// Future hands the caller a handle it can await, resolved when the
	// publish completes.
	Future
	// Channel hands the caller a buffered channel of one, which the
	// completion is sent to.
	Channel

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "inline", "deferred", "future", "channel"}

var kindIdents = [kindCount]string{"Auto", "Inline", "Deferred", "Future", "Channel"}

// String returns the kind's slug as used in benchmark names and ADRs.
func (k Kind) String() string {
	if k < kindCount {
		return kindSlugs[k]
	}
	return "kind(?)"
}

// Ident returns the kind's Go identifier.
func (k Kind) Ident() string {
	if k < kindCount {
		return kindIdents[k]
	}
	return ""
}

// Valid reports whether k names a concrete kind or Auto.
func (k Kind) Valid() bool { return k < kindCount }

// Resolve maps Auto to AutoKind and returns every other kind unchanged.
func (k Kind) Resolve() Kind {
	if k == Auto {
		return AutoKind
	}
	return k
}

// ParseKind returns the kind with the given slug.
func ParseKind(slug string) (Kind, bool) {
	for i, s := range kindSlugs {
		if s == slug {
			return Kind(i), true
		}
	}
	return 0, false
}

// Kinds lists every concrete kind in declaration order (Auto excluded).
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount-1)
	for k := Inline; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// HandsBackHandle reports whether Start returns a Handle the caller can wait
// on, which is what makes a kind an adapter rather than a plain callback.
func (k Kind) HandsBackHandle() bool {
	switch k.Resolve() {
	case Future, Channel:
		return true
	default:
		return false
	}
}

// FixedDelivery reports whether the kind pins where the outcome is delivered
// and therefore ignores Config.Defer.
func (k Kind) FixedDelivery() bool {
	switch k.Resolve() {
	case Inline, Deferred:
		return true
	default:
		return false
	}
}

// Errors reported by a Completer.
var (
	// ErrClosed reports a Start on a closed completer.
	ErrClosed = errcode.New(errcode.Closed, "iqmq: completion table is closed")
	// ErrFull reports that the bounded table of outstanding publishes is
	// exhausted; the caller must wait or apply backpressure.
	ErrFull = errcode.New(errcode.Backpressure, "iqmq: too many outstanding asynchronous publishes")
	// ErrInvalidConfig reports a Config that Validate rejects.
	ErrInvalidConfig = errcode.New(errcode.InvalidConfig, "iqmq: invalid completion configuration")
	// ErrAbandoned is the outcome of a publish that was still outstanding
	// when the completer closed: the callback runs, as the contract promises,
	// and says the outcome is unknown.
	ErrAbandoned = errcode.New(errcode.SettlementUnknown, "iqmq: asynchronous publish abandoned by close")
)

// DefaultCapacity is the number of outstanding publishes a completer admits
// when Config leaves Capacity at zero.
const DefaultCapacity = 1024

// Config bounds a completer. The zero value is usable.
type Config struct {
	// Capacity is the number of publishes that may be outstanding at once.
	// Zero selects DefaultCapacity. Start refuses beyond it, which is the
	// bound the specification requires on async storage.
	Capacity int
	// Defer moves delivery to the completion goroutine for the adapter
	// kinds. Inline and Deferred pin it and ignore this.
	Defer bool
	// Queue selects the ring the deferred delivery uses. It must
	// be bounded, ordered and multi-producer.
	Queue queue.Kind
	// Wait selects the strategy the completion goroutine parks on ().
	Wait wait.Kind
}

// Normalize fills in the zero values and applies the kind's fixed delivery.
func (c Config) Normalize(k Kind) Config {
	if c.Capacity <= 0 {
		c.Capacity = DefaultCapacity
	}
	switch k.Resolve() {
	case Inline:
		c.Defer = false
	case Deferred:
		c.Defer = true
	default:
	}
	return c
}

// Validate reports whether the configuration is usable. It is pure.
func (c Config) Validate() error {
	if c.Capacity < 0 {
		return ErrInvalidConfig
	}
	if !c.Queue.Valid() || !c.Wait.Valid() {
		return ErrInvalidConfig
	}
	// The hand-over ring has one consumer — the completion goroutine — and
	// any number of producers: Complete may be called from every I/O
	// goroutine a driver has, and a callback may complete a further publish
	// from the completion goroutine itself. It must also be bounded and
	// ordered, so that outcomes arrive in the order the broker gave them.
	q := c.Queue.Resolve()
	if !q.Modes().Has(queue.ModeMPSC) || !q.Bounded() || !q.Ordered() {
		return ErrInvalidConfig
	}
	return nil
}

// Token identifies one outstanding publish. It carries the slot it occupies
// and the generation of that slot, so a driver that completes a stale token —
// a retry of a reply that already arrived — is ignored rather than believed.
type Token uint64

// Slot returns the table slot the token refers to.
func (t Token) Slot() uint32 { return uint32(t) }

// Gen returns the generation the slot had when the token was issued.
func (t Token) Gen() uint32 { return uint32(t >> 32) }

// Result is the outcome of one asynchronous publish.
type Result struct {
	// Token is the token Start returned.
	Token Token
	// Err is the outcome: nil when the broker accepted the publish.
	Err error
}

// Callback receives the outcome of one publish, exactly once.
type Callback func(Result)

// Handle is what the adapter kinds hand back: something the caller can wait
// on instead of supplying a callback. The callback kinds return a nil Handle.
type Handle interface {
	// Await blocks until the publish completes and returns its outcome, or
	// returns ctx.Err() if the caller gives up first. It may be called more
	// than once and always reports the same outcome.
	Await(ctx context.Context) error
	// Poll reports whether the publish has completed, and its outcome when
	// it has.
	Poll() (bool, error)
}

// Completer tracks outstanding asynchronous publishes and delivers their
// outcomes.
//
// Completers are safe for concurrent use, and safe to call from inside a
// callback.
type Completer interface {
	// Start registers cb for a new publish and returns its token. cb may be
	// nil for the adapter kinds, where the caller waits on the handle
	// instead. A non-nil error means the callback will not run: the caller
	// still owns the message.
	Start(cb Callback) (Token, Handle, error)
	// Complete delivers the outcome of the publish t identifies. It reports
	// false when t is stale, already completed or never issued, in which
	// case nothing is delivered. It never blocks on user code.
	Complete(t Token, err error) bool
	// Pending returns the number of publishes that have started and whose
	// callback has not yet run.
	Pending() int
	// Drain blocks until Pending is zero or ctx is done.
	Drain(ctx context.Context) error
	// Close stops accepting publishes, completes everything still
	// outstanding with ErrAbandoned so that no callback is lost, and stops
	// the completion goroutine. It is idempotent.
	Close() error
	// Kind returns the variant this completer implements.
	Kind() Kind
}

// New builds a completer of kind k. It panics on an invalid kind or
// configuration; both are programming errors. The caller must Close it.
func New(k Kind, cfg Config) Completer {
	k = k.Resolve()
	if !k.Valid() || k == Auto {
		panic("completion: invalid kind")
	}
	if err := cfg.Validate(); err != nil {
		panic("completion: invalid configuration")
	}
	cfg = cfg.Normalize(k)
	c := &completer{
		kind:  k,
		cfg:   cfg,
		slots: make([]slot, cfg.Capacity),
		free:  queue.New[uint32](queue.MutexRing, cfg.Capacity),
		idle:  wait.New(cfg.Wait),
	}
	for i := range c.slots {
		c.free.TryPush(uint32(i))
	}
	if cfg.Defer {
		// One goroutine for the whole completer, never one per message. The
		// ring holds as many entries as there are slots, so a completion can
		// always be handed over without blocking the driver.
		c.ring = queue.New[completion](cfg.Queue, cfg.Capacity)
		c.ready = wait.New(cfg.Wait)
		c.done = make(chan struct{})
		c.wg.Add(1)
		go c.deliverLoop()
	}
	return c
}

// cacheLine is the assumed cache-line size used to pad contended slots.
const cacheLine = 64

// pad separates hot per-slot state so neighbouring slots do not false-share.
type pad [cacheLine]byte

// slot is one outstanding publish. Its state word holds the generation and
// the active bit together, so that claiming a completion is one
// compare-and-swap against the exact generation the token was issued for: a
// stale token cannot settle the publish that took the slot over, however the
// two interleave. The callback is written before the slot is published as
// active and read after the claim succeeds, which is what orders them.
type slot struct {
	_  pad
	st atomic.Uint32 // generation<<1 | active
	cb Callback
	h  *handle
	_  pad
}

// active reports whether a state word describes an outstanding publish.
func active(st uint32) bool { return st&1 == 1 }

// activeState is the state word of an outstanding publish of generation gen.
func activeState(gen uint32) uint32 { return gen<<1 | 1 }

// freeState is the state word a slot has after the publish of generation gen
// completed: the next occupant is a new generation.
func freeState(gen uint32) uint32 { return (gen + 1) << 1 }

// completion is one outcome on its way to a callback.
type completion struct {
	cb  Callback
	h   *handle
	res Result
}

// completer implements every kind: the slot table, the exactly-once
// completion and the bounded hand-over are one implementation, and the kinds
// differ in where the outcome is delivered and whether the caller gets a
// handle. That is what the bake-off is comparing, so it is all that varies.
type completer struct {
	kind  Kind
	cfg   Config
	slots []slot
	free  queue.Queue[uint32]

	pending atomic.Int64
	closing atomic.Bool
	idle    wait.Strategy

	ring  queue.Queue[completion]
	ready wait.Strategy
	done  chan struct{}
	wg    sync.WaitGroup
	once  sync.Once
}

// Kind implements Completer.
func (c *completer) Kind() Kind { return c.kind }

// Pending implements Completer.
func (c *completer) Pending() int { return int(c.pending.Load()) }

// Start implements Completer.
func (c *completer) Start(cb Callback) (Token, Handle, error) {
	if c.closing.Load() {
		return 0, nil, ErrClosed
	}
	idx, ok := c.free.TryPop()
	if !ok {
		return 0, nil, ErrFull
	}
	s := &c.slots[idx]
	gen := s.st.Load() >> 1
	var h *handle
	if c.kind.HandsBackHandle() {
		h = newHandle(c.kind)
	}
	// The slot came off the free list, so nobody else may touch it until it
	// is published as active.
	s.cb, s.h = cb, h
	c.pending.Add(1)
	s.st.Store(activeState(gen))
	// A close may have started between the check above and the claim: the
	// two atomics are sequentially consistent, so either this sees the flag
	// or Close's sweep sees the active slot. Withdrawing here keeps the
	// promise that a non-nil error means the callback will never run.
	if c.closing.Load() && c.claim(s, gen) {
		s.cb, s.h = nil, nil
		c.free.TryPush(idx)
		c.finish()
		return 0, nil, ErrClosed
	}
	return token(idx, gen), handleOrNil(h), nil
}

// claim takes ownership of an outstanding publish of generation gen, and
// reports whether this caller is the one that got it. Exactly one caller
// ever does, which is what makes the callback run exactly once.
func (c *completer) claim(s *slot, gen uint32) bool {
	return s.st.CompareAndSwap(activeState(gen), freeState(gen))
}

// finish accounts for one publish that will not be delivered and wakes Drain
// when the completer falls idle.
func (c *completer) finish() {
	if c.pending.Add(-1) == 0 && c.idle.HasWaiters() {
		c.idle.NotifyAll()
	}
}

// handleOrNil keeps a nil *handle out of a non-nil Handle interface, which is
// the classic way to hand a caller something that looks non-nil and is not.
func handleOrNil(h *handle) Handle {
	if h == nil {
		return nil
	}
	return h
}

// token packs a slot index and a generation.
func token(idx, gen uint32) Token { return Token(uint64(gen)<<32 | uint64(idx)) }

// Complete implements Completer.
func (c *completer) Complete(t Token, err error) bool { return c.complete(t, err) }

// complete claims the slot, frees it and delivers the outcome. Claiming and
// freeing happen before the callback runs, so a callback that publishes again
// finds the slot it just used available and no lock held.
func (c *completer) complete(t Token, err error) bool {
	idx := t.Slot()
	if int(idx) >= len(c.slots) {
		return false
	}
	s := &c.slots[idx]
	if !c.claim(s, t.Gen()) {
		return false
	}
	cb, h := s.cb, s.h
	s.cb, s.h = nil, nil
	// A second token for this slot is stale from here on, and the slot is
	// available to the next publish — including to a publish the callback
	// below starts.
	c.free.TryPush(idx)

	item := completion{cb: cb, h: h, res: Result{Token: t, Err: err}}
	if c.cfg.Defer {
		if c.ring.TryPush(item) {
			c.wakeDelivery()
			return true
		}
		// The ring is as large as the table, so this cannot happen; delivering
		// inline is still better than losing a callback.
	}
	c.deliver(item)
	return true
}

// deliver resolves the handle and runs the callback, in that order, with no
// lock held.
func (c *completer) deliver(item completion) {
	if item.h != nil {
		item.h.resolve(item.res.Err)
	}
	if item.cb != nil {
		item.cb(item.res)
	}
	c.finish()
}

// wakeDelivery nudges the completion goroutine.
func (c *completer) wakeDelivery() {
	if c.ready.HasWaiters() {
		c.ready.Notify()
	}
}

// deliverLoop is the one completion goroutine of the deferred kinds.
func (c *completer) deliverLoop() {
	defer c.wg.Done()
	for {
		item, ok := c.ring.TryPop()
		if ok {
			c.deliver(item)
			continue
		}
		t := c.ready.Prepare()
		if item, ok := c.ring.TryPop(); ok {
			c.ready.Cancel(t)
			c.deliver(item)
			continue
		}
		if c.closing.Load() && c.pending.Load() == 0 {
			c.ready.Cancel(t)
			return
		}
		select {
		case <-c.done:
			c.ready.Cancel(t)
			// Drain whatever is left before stopping: a completion that was
			// handed over is never dropped.
			for {
				item, ok := c.ring.TryPop()
				if !ok {
					return
				}
				c.deliver(item)
			}
		default:
		}
		_ = c.ready.Wait(context.Background(), t)
	}
}

// Drain implements Completer.
func (c *completer) Drain(ctx context.Context) error {
	for {
		if c.pending.Load() == 0 {
			return nil
		}
		t := c.idle.Prepare()
		if c.pending.Load() == 0 {
			c.idle.Cancel(t)
			return nil
		}
		if err := c.idle.Wait(ctx, t); err != nil {
			return err
		}
	}
}

// Close implements Completer.
func (c *completer) Close() error {
	c.once.Do(func() {
		c.closing.Store(true)
		// Everything still outstanding is completed, so that the promise
		// "the callback runs exactly once" holds through a shutdown.
		for i := range c.slots {
			s := &c.slots[i]
			if st := s.st.Load(); active(st) {
				c.complete(token(uint32(i), st>>1), ErrAbandoned)
			}
		}
		if c.cfg.Defer {
			close(c.done)
			c.ready.NotifyAll()
			c.wg.Wait()
			// A Complete that raced this close may hand its outcome over
			// after the delivery goroutine has stopped. Deliver whatever is
			// left, and keep looking until nothing is outstanding, so that
			// the promise "the callback runs exactly once" survives a
			// shutdown that a driver's I/O goroutine is still publishing
			// into. Every racer either hands over here or delivers inline,
			// so the loop ends.
			for c.pending.Load() > 0 {
				if item, ok := c.ring.TryPop(); ok {
					c.deliver(item)
					continue
				}
				runtime.Gosched()
			}
		}
		c.idle.NotifyAll()
	})
	return nil
}

// handle is the adapter the Future and Channel kinds hand back. The future
// resolves by closing its channel; the channel adapter sends the result on a
// buffered channel of one, which is the per-message allocation the family
// exists to measure.
type handle struct {
	kind Kind
	done chan struct{}
	ch   chan Result

	mu     sync.Mutex
	err    error
	filled bool
}

func newHandle(k Kind) *handle {
	h := &handle{kind: k}
	if k == Channel {
		h.ch = make(chan Result, 1)
		return h
	}
	h.done = make(chan struct{})
	return h
}

// resolve records the outcome and releases whoever is waiting.
func (h *handle) resolve(err error) {
	if h.kind == Channel {
		select {
		case h.ch <- Result{Err: err}:
		default:
		}
		return
	}
	h.mu.Lock()
	if !h.filled {
		h.err = err
		h.filled = true
		close(h.done)
	}
	h.mu.Unlock()
}

// Await implements Handle.
func (h *handle) Await(ctx context.Context) error {
	if h.kind == Channel {
		h.mu.Lock()
		if h.filled {
			err := h.err
			h.mu.Unlock()
			return err
		}
		h.mu.Unlock()
		select {
		case res := <-h.ch:
			return h.take(res)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case <-h.done:
		return h.outcome()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Poll implements Handle.
func (h *handle) Poll() (bool, error) {
	if h.kind == Channel {
		h.mu.Lock()
		if h.filled {
			err := h.err
			h.mu.Unlock()
			return true, err
		}
		h.mu.Unlock()
		select {
		case res := <-h.ch:
			return true, h.take(res)
		default:
			return false, nil
		}
	}
	select {
	case <-h.done:
		return true, h.outcome()
	default:
		return false, nil
	}
}

// take caches a result received from the channel adapter, so that a second
// Await answers instead of blocking forever on a channel nobody will send to
// again.
func (h *handle) take(res Result) error {
	h.mu.Lock()
	if !h.filled {
		h.err = res.Err
		h.filled = true
	}
	err := h.err
	h.mu.Unlock()
	return err
}

// outcome returns the resolved error of a future handle.
func (h *handle) outcome() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}
