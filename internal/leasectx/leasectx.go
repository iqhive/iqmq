// Package leasectx derives the handler's context from a delivery's lease
// (the message contract).
//
// A handler runs under the runner's context narrowed to the lease: it must be
// done by the time the broker takes the delivery back, minus a margin, so
// that a handler which overruns is cancelled rather than acking a delivery
// somebody else already has. The obvious way to do that is
// context.WithTimeout per delivery, and the obvious way is what the hot-path
// rules forbid (rule 4): a runtime timer per message, created and
// stopped on a path that runs millions of times a second.
//
// The family therefore has two kinds. WithTimeout is the reference: honest,
// three lines, one runtime timer per derived context. Clock is the candidate:
// one scheduler for the whole pool, holding the deadlines in an
// internal/timer scheduler and arming a single timer for the earliest of
// them, so a delivery costs a scheduler insert instead of a timer. Timers
// reports the difference, which the benchmarks publish as timers/op.
//
// Contract, identical for both kinds. Derive returns a context whose Deadline
// is the lease deadline less the margin; it becomes done at that deadline
// with context.DeadlineExceeded, when the parent is done with the parent's
// error, or when Release is called with context.Canceled — whichever happens
// first, and once. Release must be called exactly once for every context
// derived, as the cancel function of context.WithCancel must be; it is what
// takes the deadline back out of the scheduler. Values come from the parent.
// Sources are safe for concurrent use. Deriving from a closed source yields a
// context that is already done; what happens to contexts derived before the
// close is where the kinds differ, and the difference is the cost of the
// bookkeeping: the Clock kind holds them in its scheduler and cancels them,
// while a WithTimeout context carries its own timer and ends at its own
// deadline whatever the source does.
package leasectx

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/timer"
)

// Kind identifies a deadline-derivation variant. Auto resolves to AutoKind.
type Kind uint8

// Kinds. WithTimeout is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// WithTimeout derives every handler context from the lease deadline
	// with the context package, which arms one runtime timer per delivery:
	// the reference.
	WithTimeout
	// Clock keeps the deadlines in one scheduler with a single timer armed
	// for the earliest of them, so a delivery costs no timer at all.
	Clock

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "withtimeout", "clock"}

var kindIdents = [kindCount]string{"Auto", "WithTimeout", "Clock"}

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
	for k := WithTimeout; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// PerDeliveryTimer reports whether the kind arms one timer for every context
// it derives.
func (k Kind) PerDeliveryTimer() bool { return k.Resolve() == WithTimeout }

// ErrInvalidConfig reports a Config that Validate rejects.
var ErrInvalidConfig = errcode.New(errcode.InvalidConfig, "iqmq: invalid lease context configuration")

// DefaultMargin is the safety margin subtracted from a lease deadline when
// Config leaves it at zero: the handler is cancelled this long before the
// broker may hand the delivery to somebody else.
const DefaultMargin = 0

// Config describes a Source. The zero value is usable: the real clock, the
// default timer scheduler and no margin.
type Config struct {
	// Clock is the time source. Nil selects clock.Real, which is what makes
	// a Source follow the bubble clock inside testing/synctest.
	Clock clock.Clock
	// Timer selects the scheduler the Clock kind orders deadlines with. Other kinds ignore it.
	Timer timer.Kind
	// Margin is subtracted from every lease deadline. It must not be
	// negative.
	Margin time.Duration
}

// Normalize fills in the zero values and returns the effective configuration.
func (c Config) Normalize() Config {
	if c.Clock == nil {
		c.Clock = clock.Real{}
	}
	if c.Margin <= 0 {
		c.Margin = DefaultMargin
	}
	return c
}

// Validate reports whether the configuration is usable. It is pure.
func (c Config) Validate() error {
	if c.Margin < 0 || !c.Timer.Valid() {
		return ErrInvalidConfig
	}
	return nil
}

// Release ends a derived context. It must be called exactly once, and doing
// so is what releases the resources the source holds for the lease.
type Release func()

// Source derives handler contexts from lease deadlines.
//
// Implementations are safe for concurrent use.
type Source interface {
	// Derive returns a context that is done at deadline less the configured
	// margin, and the function that releases it. A deadline that has already
	// passed yields a context that is done immediately.
	Derive(parent context.Context, deadline time.Time) (context.Context, Release)
	// Live returns the number of derived contexts that have not been
	// released.
	Live() int
	// Timers returns the number of timers the source has armed since it was
	// created. It is the metric the family exists to compare: the reference
	// arms one per delivery, the candidate one per scheduler wake.
	Timers() int64
	// Close cancels every context still outstanding and stops the source. It
	// is idempotent.
	Close() error
	// Kind returns the variant this source implements.
	Kind() Kind
}

// New builds a source of kind k. It panics on an invalid kind or
// configuration; both are programming errors. The caller must Close it.
func New(k Kind, cfg Config) Source {
	k = k.Resolve()
	if !k.Valid() || k == Auto {
		panic("leasectx: invalid kind")
	}
	if err := cfg.Validate(); err != nil {
		panic("leasectx: invalid configuration")
	}
	cfg = cfg.Normalize()
	switch k {
	case WithTimeout:
		return &timeoutSource{cfg: cfg}
	case Clock:
		return newClockSource(cfg)
	default:
		panic("leasectx: unreachable")
	}
}

// timeoutSource is the WithTimeout kind: one runtime timer per delivery, and
// nothing else. It keeps no table of the contexts it derived — a table would
// put a map write on the very path the family is measuring — which is why a
// context it derived outlives Close and ends at its own deadline.
type timeoutSource struct {
	cfg    Config
	timers atomic.Int64
	live   atomic.Int64
	closed atomic.Bool
}

// Derive implements Source.
func (s *timeoutSource) Derive(parent context.Context, deadline time.Time) (context.Context, Release) {
	s.live.Add(1)
	if s.closed.Load() {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, s.releaseOnce(func() {})
	}
	s.timers.Add(1)
	// A lease deadline is absolute, so this is context.WithTimeout with the
	// subtraction already done — WithTimeout is WithDeadline(parent,
	// now+d) — and it reports a deadline that has already passed as it is
	// rather than rounding it up to now. Either way it is the per-delivery
	// runtime timer that rule 4 forbids and that this kind exists to
	// measure.
	ctx, cancel := context.WithDeadline(parent, deadline.Add(-s.cfg.Margin))
	return ctx, s.releaseOnce(cancel)
}

// releaseOnce wraps cancel so that the release is idempotent and the live
// count moves exactly once, as the contract requires.
func (s *timeoutSource) releaseOnce(cancel context.CancelFunc) Release {
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			s.live.Add(-1)
		})
	}
}

// Live implements Source.
func (s *timeoutSource) Live() int { return int(s.live.Load()) }

// Timers implements Source.
func (s *timeoutSource) Timers() int64 { return s.timers.Load() }

// Close implements Source.
func (s *timeoutSource) Close() error {
	s.closed.Store(true)
	return nil
}

// Kind implements Source.
func (s *timeoutSource) Kind() Kind { return WithTimeout }

// clockSource is the Clock kind: one scheduler and one timer for the whole
// pool. Deadlines are ordered by an internal/timer scheduler; the run
// goroutine arms a single timer for the earliest of them and cancels
// everything that is due when it fires.
type clockSource struct {
	cfg    Config
	timers atomic.Int64
	live   atomic.Int64

	mu      sync.Mutex
	sched   timer.Scheduler[*leaseCtx]
	armedAt int64 // the deadline the timer is set for, MaxInt64 when idle
	tmr     clock.Timer
	closed  bool

	wake chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
}

func newClockSource(cfg Config) *clockSource {
	s := &clockSource{
		cfg:     cfg,
		sched:   timer.New[*leaseCtx](cfg.Timer, cfg.Clock.Now().UnixNano()),
		armedAt: math.MaxInt64,
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	s.wg.Add(1)
	go s.run()
	return s
}

// Derive implements Source.
func (s *clockSource) Derive(parent context.Context, deadline time.Time) (context.Context, Release) {
	c := &leaseCtx{parent: parent, deadline: deadline.Add(-s.cfg.Margin), src: s}
	c.entry.Value = c
	s.live.Add(1)

	// The context package's order, which this kind matches so that the two
	// kinds are indistinguishable to a caller: a parent that is already done
	// cancels the child before its deadline is looked at, and a deadline that
	// has already passed ends the context there and then rather than through
	// the scheduler. Neither case ever reaches the scheduler at all.
	if parent.Done() != nil {
		if err := parent.Err(); err != nil {
			c.cancel(err)
			return c, c.release
		}
		// The parent is watched without a timer: AfterFunc needs none.
		c.stopParent = context.AfterFunc(parent, func() { c.cancel(parent.Err()) })
	}
	due := c.deadline.UnixNano()
	if due <= s.cfg.Clock.Now().UnixNano() {
		c.cancel(context.DeadlineExceeded)
		return c, c.release
	}

	s.mu.Lock()
	closed := s.closed
	if !closed {
		s.sched.Add(&c.entry, due)
	}
	// Only a deadline earlier than the one the timer is set for makes the
	// run goroutine re-arm; a later one is already covered, which is why a
	// steady stream of leases arms no timers at all.
	rearm := !closed && due < s.armedAt
	s.mu.Unlock()

	switch {
	case closed:
		c.cancel(context.Canceled)
	case rearm:
		s.signal()
	}
	return c, c.release
}

// signal nudges the run goroutine.
func (s *clockSource) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// run is the single scheduler goroutine: it fires the deadlines that are due
// and arms one timer for the next one.
func (s *clockSource) run() {
	defer s.wg.Done()
	var fired []*timer.Entry[*leaseCtx]
	for {
		now := s.cfg.Clock.Now().UnixNano()
		s.mu.Lock()
		fired = s.sched.Fire(now, fired[:0])
		next, ok := s.sched.Next()
		if ok {
			s.armedAt = next
		} else {
			s.armedAt = math.MaxInt64
		}
		s.mu.Unlock()
		for _, e := range fired {
			e.Value.cancel(context.DeadlineExceeded)
		}
		if len(fired) > 0 {
			continue
		}
		var ch <-chan time.Time
		if ok {
			ch = s.arm(time.Duration(next - now))
		}
		select {
		case <-ch:
		case <-s.wake:
		case <-s.done:
			return
		}
	}
}

// arm sets the one timer the source owns and returns its channel. The timer
// is held under the lock because Close stops it from another goroutine.
func (s *clockSource) arm(d time.Duration) <-chan time.Time {
	s.timers.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tmr == nil {
		s.tmr = s.cfg.Clock.NewTimer(d)
	} else {
		s.tmr.Reset(d)
	}
	return s.tmr.C()
}

// remove takes a lease out of the scheduler; it reports whether the lease was
// still scheduled, which is false once its deadline has fired.
func (s *clockSource) remove(c *leaseCtx) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sched.Cancel(&c.entry)
}

// Live implements Source.
func (s *clockSource) Live() int { return int(s.live.Load()) }

// Timers implements Source.
func (s *clockSource) Timers() int64 { return s.timers.Load() }

// Close implements Source.
func (s *clockSource) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	var left []*timer.Entry[*leaseCtx]
	left = s.sched.Fire(math.MaxInt64, left)
	if s.tmr != nil {
		s.tmr.Stop()
	}
	s.mu.Unlock()
	close(s.done)
	s.wg.Wait()
	for _, e := range left {
		e.Value.cancel(context.Canceled)
	}
	return nil
}

// Kind implements Source.
func (s *clockSource) Kind() Kind { return Clock }

// closedchan is a reusable closed channel, as the standard library's context
// package uses: a context that is asked for its Done channel after it is
// already done needs no allocation.
var closedchan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// leaseCtx is the context the Clock kind derives. It carries no timer of its
// own: its deadline lives in the source's scheduler, and its Done channel is
// only created if somebody asks for it.
type leaseCtx struct {
	parent     context.Context //nolint:containedctx // this is a context implementation
	deadline   time.Time
	src        *clockSource
	entry      timer.Entry[*leaseCtx]
	stopParent func() bool

	mu   sync.Mutex
	done chan struct{}
	err  error
	once bool
}

// Deadline implements context.Context.
func (c *leaseCtx) Deadline() (time.Time, bool) { return c.deadline, true }

// Done implements context.Context. The channel is created on demand: a
// handler that never selects on it costs nothing.
func (c *leaseCtx) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done == nil {
		if c.err != nil {
			c.done = closedchan
		} else {
			c.done = make(chan struct{})
		}
	}
	return c.done
}

// Err implements context.Context.
func (c *leaseCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Value implements context.Context: values come from the parent unchanged.
func (c *leaseCtx) Value(key any) any { return c.parent.Value(key) }

// String makes a lease context readable in a %v of a context chain, as the
// standard library's contexts are.
func (c *leaseCtx) String() string { return "iqmq.leaseCtx" }

// cancel ends the context with err, if it has not already ended.
func (c *leaseCtx) cancel(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	if c.done == nil {
		c.done = closedchan
	} else {
		close(c.done)
	}
	c.mu.Unlock()
}

// release is the Release of a lease context: it takes the deadline out of the
// scheduler, stops watching the parent and cancels. Calling it twice is
// harmless, calling it once is required.
func (c *leaseCtx) release() {
	c.mu.Lock()
	if c.once {
		c.mu.Unlock()
		return
	}
	c.once = true
	c.mu.Unlock()
	c.src.remove(c)
	if c.stopParent != nil {
		c.stopParent()
	}
	c.cancel(context.Canceled)
	c.src.live.Add(-1)
}
