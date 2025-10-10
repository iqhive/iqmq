// Package dispatch places fetched deliveries on lanes and runs them on a
// fixed pool of workers (the API).
//
// The runner has one bounded receive loop. Everything it fetches is handed to
// a Pool, which decides two things: the lane an item belongs to (the Router
// half of the family) and the worker that runs it (the Pool half). Both
// decisions are on the per-message hot path, which is why the family exists:
// the kinds here differ in how lanes are chosen and how workers find work,
// and the bake-off decides which becomes Auto.
//
// Ordering. A keyed item must be handled after every earlier item with the
// same key, and never concurrently with one: that is the per-key ordering the
// engine promises. A kind provides it by sending equal keys to one lane and
// letting at most one worker run a lane at a time; Kind.PreservesKeyOrder
// reports which kinds do. SharedQueue and RoundRobin deliberately do not —
// they exist to measure what ordering costs.
//
// Bounds. Every lane is bounded, there is no goroutine or timer per message,
// and Submit never allocates: a full lane blocks the caller (or is refused by
// TrySubmit) instead of growing. Workers park on the injected wait strategy
// (internal/wait) rather than spinning, and a parked worker is woken only by
// a push to a lane it owns.
//
// Lifecycle. New starts the workers; Close stops accepting, lets the workers
// drain what was accepted and joins them; Drain waits for the pool to fall
// idle without closing it. A Submit that races Close either wins (the item is
// handled before the workers stop) or returns ErrClosed; an accepted item is
// never dropped.
package dispatch

import (
	"context"
	"sync"
	"sync/atomic"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/queue"
	"bitbucket.org/iqhive/iqmq/internal/wait"
)

// Kind identifies a dispatch variant. Auto resolves to AutoKind.
type Kind uint8

// Kinds. HashLanes is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// HashLanes routes a keyed item by the hash of its key and a keyless one
	// round-robin, over a fixed set of lanes that are statically assigned to
	// workers: the reference implementation.
	HashLanes
	// PerPartition gives every source partition its own lane and worker, so
	// a driver that already partitions (Kafka, JetStream) keeps its order
	// without re-hashing.
	PerPartition
	// ChanPerLane replaces the lane queues with buffered channels, one
	// goroutine receiving from each: the shape most Go programs reach for.
	ChanPerLane
	// SharedQueue puts every item in one MPMC queue that all workers pop
	// from. It maximises worker utilisation and provides no per-key order.
	SharedQueue
	// WorkStealing routes like HashLanes but lets an idle worker claim a
	// bounded number of foreign lanes exclusively, so a skewed key
	// distribution does not idle the pool while order is preserved.
	WorkStealing
	// RoundRobin ignores keys entirely and hands out lanes in turn: the
	// keyless dispatch path, and the ordering-free baseline.
	RoundRobin

	kindCount
)

var kindSlugs = [kindCount]string{
	"auto", "hashlanes", "perpartition", "chanperlane", "sharedqueue", "workstealing", "roundrobin",
}

var kindIdents = [kindCount]string{
	"Auto", "HashLanes", "PerPartition", "ChanPerLane", "SharedQueue", "WorkStealing", "RoundRobin",
}

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
	for k := HashLanes; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// PreservesKeyOrder reports whether the kind handles items that share a key
// in submission order and never two of them at once. Kinds that do not are
// only usable for unordered subscriptions.
func (k Kind) PreservesKeyOrder() bool {
	switch k.Resolve() {
	case SharedQueue, RoundRobin:
		return false
	default:
		return true
	}
}

// RoutesByPartition reports whether the kind sends an item to the lane of the
// partition it arrived on rather than to the hash of its key.
func (k Kind) RoutesByPartition() bool { return k.Resolve() == PerPartition }

// FixedLanes reports whether the kind derives its lane count from the worker
// count and ignores Config.Lanes: ChanPerLane runs one receiving goroutine
// per lane, and SharedQueue has exactly one lane.
func (k Kind) FixedLanes() bool {
	switch k.Resolve() {
	case ChanPerLane, SharedQueue:
		return true
	default:
		return false
	}
}

// Steals reports whether idle workers take work from foreign lanes.
func (k Kind) Steals() bool { return k.Resolve() == WorkStealing }

// Errors reported by a Pool. They carry the specification's codes so callers
// compare with errcode.Is rather than by identity.
var (
	// ErrClosed reports a Submit or Drain on a closed pool.
	ErrClosed = errcode.New(errcode.Closed, "iqmq: dispatch pool is closed")
	// ErrInvalidConfig reports a Config that Validate rejects.
	ErrInvalidConfig = errcode.New(errcode.InvalidConfig, "iqmq: invalid dispatch configuration")
)

// Defaults for the zero Config.
const (
	// DefaultCapacity is the number of items one lane holds.
	DefaultCapacity = 64
	// DefaultBatch is the number of items a worker handles before it looks
	// at its other lanes, which bounds how long one lane can starve another.
	DefaultBatch = 32
	// DefaultSteal is the number of foreign lanes an idle WorkStealing
	// worker probes before it parks.
	DefaultSteal = 4
)

// NoPartition is the Route partition of an item whose driver has none.
const NoPartition int32 = -1

// Config describes a pool. The zero value is usable: one worker, one lane of
// DefaultCapacity items, the bake-off defaults for the lane queue and the
// wait strategy.
type Config struct {
	// Workers is the number of goroutines that run handlers. Zero means one.
	Workers int
	// Lanes is the number of routing buckets. Zero means Workers. Kinds for
	// which FixedLanes reports true ignore it.
	Lanes int
	// Capacity is the number of items one lane holds. Zero selects
	// DefaultCapacity. SharedQueue holds Capacity items in total.
	Capacity int
	// Batch is the number of items a worker takes from one lane before
	// moving on. Zero selects DefaultBatch.
	Batch int
	// Steal is the number of foreign lanes an idle WorkStealing worker
	// probes. Zero selects DefaultSteal; the probe count is always bounded.
	Steal int
	// Queue selects the lane queue implementation. ChanPerLane
	// ignores it.
	Queue queue.Kind
	// Wait selects the strategy an idle worker and a blocked producer park
	// on. ChanPerLane ignores it.
	Wait wait.Kind
}

// Normalize fills in the zero values and applies the kind's fixed lane count.
func (c Config) Normalize(k Kind) Config {
	if c.Workers <= 0 {
		c.Workers = 1
	}
	if c.Lanes <= 0 {
		c.Lanes = c.Workers
	}
	if c.Capacity <= 0 {
		c.Capacity = DefaultCapacity
	}
	if c.Batch <= 0 {
		c.Batch = DefaultBatch
	}
	if c.Steal <= 0 {
		c.Steal = DefaultSteal
	}
	switch k.Resolve() {
	case SharedQueue:
		c.Lanes = 1
	case ChanPerLane:
		c.Lanes = c.Workers
	default:
	}
	return c
}

// Validate reports whether the configuration is usable. It is pure.
func (c Config) Validate() error {
	if c.Workers < 0 || c.Lanes < 0 || c.Capacity < 0 || c.Batch < 0 || c.Steal < 0 {
		return ErrInvalidConfig
	}
	if !c.Queue.Valid() || !c.Wait.Valid() {
		return ErrInvalidConfig
	}
	if c.Queue.Resolve() == queue.SPSC || c.Queue.Resolve() == queue.MPSC {
		// A lane is popped by whichever worker owns it at the time, and the
		// shared queue is popped by every worker at once.
		return ErrInvalidConfig
	}
	return nil
}

// Route is what the router knows about an item: the hash of its ordering key
// and the partition it arrived on. The engine hashes keys with internal/hash
// before it gets here, so a lane decision costs no hashing.
type Route struct {
	// Key is the hash of the item's ordering key.
	Key uint64
	// Partition is the source partition, or NoPartition.
	Partition int32
	// Keyed reports whether the item has an ordering key at all. An unkeyed
	// item may be handled on any lane.
	Keyed bool
}

// Keyed returns the Route of an item with ordering key hash h.
func Keyed(h uint64) Route { return Route{Key: h, Partition: NoPartition, Keyed: true} }

// Keyless returns the Route of an item with no ordering key.
func Keyless() Route { return Route{Partition: NoPartition} }

// InPartition returns r with its source partition set to p.
func (r Route) InPartition(p int32) Route {
	r.Partition = p
	return r
}

// Router maps an item to a lane. It is the routing half and is
// pure: the same Route yields the same lane, except for keyless items, which
// are spread over the lanes in turn.
type Router interface {
	// Route returns the lane index for r, in [0, Lanes).
	Route(r Route) int
	// Lanes returns the number of lanes.
	Lanes() int
}

// Handler runs one item. It is called on a worker goroutine and must not
// retain the item past its return; outcomes (ack, nack, retry) are the
// runner's business, so a handler here reports nothing.
type Handler[T any] func(item T)

// Pool routes items to lanes and runs them on a fixed set of workers.
//
// Pools are safe for concurrent use. Submit and TrySubmit never allocate.
type Pool[T any] interface {
	Router
	// Submit routes v and blocks until the lane has room, ctx is done or the
	// pool closes. It returns nil once the item is queued, ctx.Err() on
	// cancellation and ErrClosed once the pool is closing. An item that is
	// queued is always handled, even when the pool closes immediately after.
	Submit(ctx context.Context, r Route, v T) error
	// TrySubmit queues v without blocking and reports whether it was queued.
	// It reports false for a full lane and for a closed pool.
	TrySubmit(r Route, v T) bool
	// Pending returns the number of items that are queued or in a handler.
	// A producer blocked in Submit is not counted until its item is
	// accepted. Pending falls to zero when the pool is idle.
	Pending() int
	// Drain blocks until Pending is zero or ctx is done. It does not stop
	// the pool and does not stop new submissions.
	Drain(ctx context.Context) error
	// Close stops accepting items, waits for everything already accepted to
	// be handled and joins the workers. It is idempotent.
	Close() error
	// Workers returns the number of worker goroutines.
	Workers() int
	// Kind returns the variant this pool implements.
	Kind() Kind
}

// New starts a pool of kind k with cfg workers running h. It panics on an
// invalid kind or configuration; both are programming errors. The caller must
// Close the pool.
func New[T any](k Kind, cfg Config, h Handler[T]) Pool[T] {
	k = k.Resolve()
	if !k.Valid() || k == Auto {
		panic("dispatch: invalid kind")
	}
	if err := cfg.Validate(); err != nil {
		panic("dispatch: invalid configuration")
	}
	if h == nil {
		panic("dispatch: nil handler")
	}
	cfg = cfg.Normalize(k)
	if k == ChanPerLane {
		return newChanPool(k, cfg, h)
	}
	return newLanePool(k, cfg, h)
}

// gate is the admission counter shared by the pool kinds. It counts items
// that are queued, being submitted or being handled, in one word, so that
// Drain, the worker exit condition and the close race all read the same
// number.
//
// A submitter announces itself with reserve before it looks at the closing
// flag, and Close sets the flag before it reads the counter. The two atomics
// are sequentially consistent, so at least one of the pair observes the
// other: either the submitter sees the pool closing and withdraws, or Close
// sees a non-zero count and waits. No item can therefore be accepted after
// the workers have stopped.
//
// The counter is a shared condition and not private bookkeeping: a worker
// parks on "closing and the count is not zero" and a Drain caller parks on
// "the count is not zero", so every fall to zero has to be announced —
// including the one that gives back a reservation the closing flag refused.
// reserve and withdraw are two calls for that reason.
type gate struct {
	n       atomic.Int64
	closing atomic.Bool
}

// announce counts one submission in progress. reserve is announce plus the
// look at the closing flag, and the two halves are separable on purpose: the
// deadlock of the specification lived in the window between them, and the test
// that guards it drives the halves apart.
func (g *gate) announce() { g.n.Add(1) }

// reserve announces one submission in progress and reports whether the pool
// will have it. Every reserve is followed by exactly one of: the item being
// counted until it is handled (and then released), or withdraw.
func (g *gate) reserve() bool {
	g.announce()
	return !g.closing.Load()
}

// withdraw gives back a reservation and reports whether the count fell to
// zero, which the caller must announce exactly as it announces the fall to
// zero after leave.
func (g *gate) withdraw() bool {
	refusalWindow()
	return g.n.Add(-1) == 0
}

// leave releases a slot and returns the new count.
func (g *gate) leave() int64 { return g.n.Add(-1) }

// load returns the current count.
func (g *gate) load() int64 { return g.n.Load() }

// closed reports whether the pool has started closing.
func (g *gate) closed() bool { return g.closing.Load() }

// mix spreads a key hash over the whole word before it is reduced to a lane.
// Hashes arrive from internal/hash already mixed, but a caller that supplies
// a counter or a small integer key must not end up with every item on one
// lane. It is splitmix64's finaliser.
func mix(h uint64) uint64 {
	h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
	h = (h ^ (h >> 27)) * 0x94d049bb133111eb
	return h ^ (h >> 31)
}

// cacheLine is the assumed cache-line size used to pad contended lanes.
const cacheLine = 64

// pad separates hot per-lane state so neighbouring lanes do not false-share.
type pad [cacheLine]byte

// waiterSet is one worker's parking spot. Each worker parks on its own
// strategy so that a push wakes the worker that owns the lane and nobody
// else; a shared strategy would wake an arbitrary worker, which would then
// find no work in its own lanes and park again while the item waited.
type waiterSet struct {
	_ pad
	s wait.Strategy
	_ pad
}

// idleWait is the parking spot of Drain: it is notified whenever the pool's
// count falls to zero.
type idleWait struct {
	s wait.Strategy
}

// notifyIdle wakes Drain callers when the pool has fallen idle.
func (i *idleWait) notifyIdle() {
	if i.s.HasWaiters() {
		i.s.NotifyAll()
	}
}

// drain blocks until pending returns zero, ctx is done, or the pool closes
// with nothing left.
func (i *idleWait) drain(ctx context.Context, pending func() int64) error {
	for {
		if pending() == 0 {
			return nil
		}
		t := i.s.Prepare()
		if pending() == 0 {
			i.s.Cancel(t)
			return nil
		}
		if err := i.s.Wait(ctx, t); err != nil {
			return err
		}
	}
}

// closeOnce guards the idempotent half of Close.
type closeOnce struct{ once sync.Once }

func (c *closeOnce) do(fn func()) { c.once.Do(fn) }

// newIdleStrategy builds the strategy Drain parks on. It is shared by the
// pool kinds, including the one that has no other use for a wait strategy.
func newIdleStrategy(cfg Config) wait.Strategy { return wait.New(cfg.Wait) }
