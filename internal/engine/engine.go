// Package engine is the in-memory message engine of iqmq: the generic,
// API-agnostic kernel that the memory engine describes and that the public
// memory adapter, the API bake-off and the differential test suites are all
// built on.
//
// The engine is generic in the payload type P and never inspects it. Every
// property it needs — routing key, accounted size, partition, delivery time
// and the owned-release callback — is passed explicitly in Publish, so the
// same kernel serves the public *mq.Message, the bake-off's candidate
// message shapes and the models' plain integers.
//
// # Shape
//
//	Engine ─► Topic ─► Subscription ─► Consumer
//	                     └─ lanes[partition] = ready + in-flight + delayed
//
// A Topic hashes each publication to a partition and hands it to every
// subscription; each subscription keeps its own envelope, its own settlement
// state and its own capacity accounting, so subscriptions never interfere
// (the memory engine). Within a subscription the consumers of a lane
// compete for its records.
//
// # Variants
//
// The engine is assembled from the variant families: the
// fan-out storage architecture, the ready queue, the wait strategy
// the delay scheduler, the key hash, the object pools,
// the topic registry, the statistics counters and the retry
// placement, with the fan-out admission policy and the overflow
// policy selected per topic in TopologyConfig. KindSet selects one kind per
// family and KindSet.Safe selects the mutex-backed reference kind of every
// family at once, which is the configuration the differential tests compare
// against.
//
// # Slice
package engine

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/hash"
	"github.com/iqhive/iqmq/internal/lifecycle"
	"github.com/iqhive/iqmq/internal/pool"
	"github.com/iqhive/iqmq/internal/registry"
	"github.com/iqhive/iqmq/internal/topicname"
)

// Engine-specific sentinel errors. Everything else the engine returns is an
// internal/errcode sentinel: ErrClosed, ErrQueueFull, ErrPartial,
// ErrMessageTooLarge, ErrTopicNotFound, ErrInvalidTopic, ErrLeaseLost,
// ErrAlreadySettled, ErrSettlementInProgress and the context errors.
var (
	// ErrInvalidConfig reports a Config or TopologyConfig the engine cannot
	// honour.
	ErrInvalidConfig = errcode.New(errcode.InvalidConfig, "iqmq/engine: invalid configuration")
	// ErrNoSubscription reports an operation on a subscription that does not
	// exist.
	ErrNoSubscription = errcode.New(errcode.NotFound, "iqmq/engine: no such subscription")
)

// Defaults for the zero fields of TopologyConfig (the memory engine).
const (
	// DefaultPartitions is one lane per subscription: strict order.
	DefaultPartitions = 1
	// DefaultCapacityMessages is the per-lane record capacity.
	DefaultCapacityMessages = 65536
	// DefaultAckDeadline is the lease duration of a delivery.
	DefaultAckDeadline = 30 * time.Second
	// DefaultDrainTimeout is how long Shutdown waits for the in-flight
	// deliveries and the dead-letter publications (the message contract).
	DefaultDrainTimeout = lifecycle.DefaultDrainTimeout
)

// Config configures an engine. The zero value is usable: it selects every
// family's Auto kind, the real clock and the default topology.
type Config[P any] struct {
	// Kinds selects the variant kind of every family.
	Kinds KindSet
	// Clock is the time source. Nil means clock.Real, which follows the
	// bubble clock under testing/synctest.
	Clock clock.Clock
	// DefaultTopology is the topology CreateTopic uses for its zero fields.
	DefaultTopology TopologyConfig
	// MaxTotalMessages bounds the admitted records of the whole engine,
	// counted once per subscription copy. Zero means no limit.
	MaxTotalMessages int
	// MaxTotalBytes bounds the accounted payload bytes of the whole engine
	// the same way. Zero means no limit.
	MaxTotalBytes int64
	// Clone copies a payload. It is required when Kinds.Safe is set, which
	// is the configuration that models safe-copy publication: every
	// subscription then observes an independent copy.
	Clone func(P) P
	// Zero, when set, is applied to a payload as it is released, so that
	// sensitive bytes do not survive in a pooled record (ZeroOnRelease,
	// the memory engine). It is not applied to a borrowed payload, whose
	// storage belongs to the caller (CopyBorrowed).
	Zero func(P)
	// Poison, when set, is the use-after-free canary of the zero-copy
	// receive mode (the memory engine): it fills a payload with a
	// recognisable pattern as the engine releases the record, so a consumer
	// that reads a delivery it has already settled sees the canary rather
	// than plausible data. It replaces Zero on the records it applies to,
	// being the stronger scrub of the two.
	Poison func(P)
	// CopyMode is the ownership contract of a publication that does not name
	// one. Zero means CopySafe on an engine whose kinds are the
	// Safe reference set and CopyOwned on every other, which is the mode
	// each configuration was already publishing in.
	CopyMode CopyMode
	// Receive is how a delivery hands its payload to the consumer. Zero
	// means ReceiveShared, which copies nothing.
	Receive ReceiveMode
	// DebugOwnership makes the engine check the release contract of owned
	// publications: a second release of one record's payload panics with
	// DoubleReleasePanic rather than corrupting the caller's free list. It
	// is on unconditionally in iqmq_debug builds, and is honoured by the
	// iqmq_debug pool canary as well.
	DebugOwnership bool
	// Workers is the concurrency hint the statistics counters are striped
	// for. Zero means GOMAXPROCS.
	Workers int
	// DrainTimeout bounds what Shutdown waits for: the in-flight deliveries
	// and then the dead-letter publications still in flight
	// (the message contract). Zero means DefaultDrainTimeout and a negative
	// value means no bound. Close waits for neither, whatever this says.
	DrainTimeout time.Duration
}

// Engine owns the topics, the pools and the accounting of one in-memory
// broker. It is safe for concurrent use.
type Engine[P any] struct {
	kinds  KindSet
	clock  clock.Clock
	cfg    Config[P]
	hashfn hash.Func

	topics  registry.Registry[*Topic[P]]
	tmu     sync.Mutex
	records pool.ObjectPool[record[P]]

	msgs     atomic.Int64
	bytes    atomic.Int64
	maxMsgs  int64
	maxBytes int64
	noSubs   atomic.Int64

	sched *scheduler[P]
	lc    *lifecycle.Lifecycle
	idle  notifyGate
	// room wakes the publishers parked on an engine-wide limit. A wait
	// whose parking condition includes the engine's own budget
	// (logRing.hasRoom) needs the return of that budget to be a wake of its
	// own: the settlement that returns it may belong to another topic
	// altogether, and even on the log the publisher is waiting for it is
	// returned after the slot itself is reclaimed (lane.discard).
	room   notifyGate
	placer retryPlacer[P]

	// ctx is the engine's own lifetime context: it is what the internal
	// dead-letter publication is made under, so that a Close ends one that
	// is in flight rather than leaving it to finish against a closing
	// engine. The publication itself never parks (Topic.publishInternal).
	ctx    context.Context
	cancel context.CancelFunc

	faults Faults[P]
}

// New builds an engine. It fails with ErrInvalidConfig when a kind is
// undefined, when Kinds.Safe is set without a Clone, or when the default
// topology is invalid.
func New[P any](cfg Config[P]) (*Engine[P], error) {
	if !cfg.Kinds.Valid() {
		return nil, ErrInvalidConfig
	}
	if cfg.Kinds.Safe && cfg.Clone == nil {
		return nil, ErrInvalidConfig
	}
	if !cfg.CopyMode.Valid() || !cfg.Receive.Valid() {
		return nil, ErrInvalidConfig
	}
	if cfg.CopyMode == CopyDefault {
		// The engine's historical default: the Safe reference set publishes
		// safely, every other set shares one reference-counted record and
		// releases it once.
		cfg.CopyMode = CopyOwned
		if cfg.Kinds.Safe {
			cfg.CopyMode = CopySafe
		}
	}
	if cfg.CopyMode == CopySafe && cfg.Clone == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.Receive == ReceiveCopy && cfg.Clone == nil {
		// A copying receive is defined by the copy it makes.
		return nil, ErrInvalidConfig
	}
	if cfg.Kinds.Arch.Copies() && cfg.Clone == nil {
		// FullCopy is defined by the copy it makes; without one it would
		// silently be the shared-record architecture.
		return nil, ErrInvalidConfig
	}
	top, err := cfg.DefaultTopology.normalise(TopologyConfig{})
	if err != nil {
		return nil, err
	}
	cfg.DefaultTopology = top
	ks := cfg.Kinds.Resolve()
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	h, err := hash.New(ks.Hash)
	if err != nil {
		return nil, err
	}
	topics, err := registry.New[*Topic[P]](ks.Registry)
	if err != nil {
		return nil, err
	}
	records, err := pool.New(ks.Pool, pool.Config[record[P]]{Reset: resetRecord[P]})
	if err != nil {
		return nil, err
	}
	e := &Engine[P]{
		kinds:    ks,
		placer:   newRetryPlacer[P](ks.RetryLane),
		clock:    cfg.Clock,
		cfg:      cfg,
		hashfn:   h.Bytes,
		topics:   topics,
		records:  records,
		maxMsgs:  int64(cfg.MaxTotalMessages),
		maxBytes: cfg.MaxTotalBytes,
	}
	// The cancel is kept on the engine and called by Close, which is the
	// only thing that ends its lifetime.
	e.ctx, e.cancel = context.WithCancel(context.Background())
	e.faults.e = e
	if e.sched, err = newScheduler(e, e.now()); err != nil {
		e.cancel()
		return nil, err
	}
	e.lc = lifecycle.New(lifecycle.Config{
		Clock:        cfg.Clock,
		DrainTimeout: cfg.DrainTimeout,
		Shards:       cfg.Workers,
		Steps:        e.shutdownSteps(),
	})
	return e, nil
}

// Kinds returns the resolved variant kinds the engine was built with.
func (e *Engine[P]) Kinds() KindSet { return e.kinds }

// Clock returns the engine's time source.
func (e *Engine[P]) Clock() clock.Clock { return e.clock }

// Faults returns the fault injector (the memory engine).
func (e *Engine[P]) Faults() *Faults[P] { return &e.faults }

// now reads the engine clock as nanoseconds on the clock's own origin.
func (e *Engine[P]) now() int64 { return e.clock.Now().UnixNano() }

// CreateTopic returns the topic with the given name, creating it with cfg
// merged over the engine's default topology when it does not exist. A second
// call with the same name returns the existing topic and ignores cfg.
func (e *Engine[P]) CreateTopic(name string, cfg TopologyConfig) (*Topic[P], error) {
	if e.closing() {
		return nil, errcode.ErrClosed
	}
	if err := topicname.Validate(name); err != nil {
		return nil, err
	}
	key := registry.Intern(name)
	if t, ok := e.topics.Load(key); ok {
		return t, nil
	}
	top, err := cfg.normalise(e.cfg.DefaultTopology)
	if err != nil {
		return nil, err
	}
	if (top.Priority || top.Wildcards || top.Kind == TopologyStream) && e.kinds.Arch.Shared() {
		// A shared partition log decides admission and the subscription set
		// for every subscription at once, in slot order: it has no
		// per-priority ring to serve first, no per-subscription routing
		// decision to make, and no way to admit a replayed record, whose
		// slot the log has already handed to every cursor. Refusing says so
		// rather than ignoring the field (adr/ADR-017-assumptions.md,
		// ).
		return nil, ErrInvalidConfig
	}
	if top.Priority && top.Ordering.Strict() {
		// Priorities reorder a lane by construction: a strict-ordering topic
		// that also asked for them would advertise an order it cannot keep,
		// which is by another route.
		return nil, ErrInvalidConfig
	}
	if top.Ordering.Strict() && !e.kinds.QueueOrdered() {
		// Strict ordering is a promise about the lane, and the engine keeps
		// it by blocking a lane on its redeliveries. It cannot
		// keep it on top of a ready queue, a scheduler or an architecture
		// that does not order what it holds, so the configuration is
		// refused rather than advertising an order the engine would not
		// deliver (adr/ADR-017-assumptions.md).
		return nil, ErrInvalidConfig
	}
	e.tmu.Lock()
	defer e.tmu.Unlock()
	if t, ok := e.topics.Load(key); ok {
		return t, nil
	}
	t := &Topic[P]{name: name, key: key, eng: e, cfg: top}
	// Generation zero is the empty set: a publication made before anyone
	// subscribed is stamped below every subscription's own generation, so
	// every one of them replays it (subSet).
	t.subs.Store(&subSet[P]{subs: make([]*Subscription[P], 0, 1)})
	if top.Kind == TopologyStream {
		t.retained = make([]*retainedLog[P], top.Partitions)
		for i := range t.retained {
			t.retained[i] = &retainedLog[P]{}
		}
	}
	if top.DedupWindow > 0 {
		t.dedup = newDedupWindow(top.DedupWindow)
	}
	if e.kinds.Arch.Shared() {
		// The logs read the topic's subscription set, so it exists first.
		t.rings = make([]*logRing[P], top.Partitions)
		for i := range t.rings {
			t.rings[i] = newLogRing(t, top)
		}
	}
	// The registry publication takes a publish ticket, so that a topic
	// cannot become visible after the settling phase has walked the
	// registry: the phase bars the gate and waits for it before the walk,
	// and a topic registered past that point is refused instead. Without it
	// a topic created under a close is one Close never visits — harmless
	// today, because nothing can be published into it, but exactly the shape
	// of the defect class the contributor notes names.
	tk := e.lc.Enter(lifecycle.OpPublish)
	defer e.lc.Leave(tk)
	if !tk.OK() {
		return nil, errcode.ErrClosed
	}
	actual, loaded := e.topics.LoadOrStore(key, t)
	if loaded {
		return actual, nil
	}
	return t, nil
}

// Topic returns an existing topic. It fails with ErrTopicNotFound when the
// name is unknown.
func (e *Engine[P]) Topic(name string) (*Topic[P], error) {
	t, ok := e.topics.Load(registry.Intern(name))
	if !ok {
		return nil, errcode.ErrTopicNotFound
	}
	return t, nil
}

// Inspect returns the snapshot of one subscription of one topic
// (the memory engine). It fails with ErrTopicNotFound and ErrNoSubscription
// for names the engine does not know.
func (e *Engine[P]) Inspect(topic, sub string) (Snapshot, error) {
	t, err := e.Topic(topic)
	if err != nil {
		return Snapshot{}, err
	}
	s, err := t.Subscription(sub)
	if err != nil {
		return Snapshot{}, err
	}
	return s.Inspect(), nil
}

// Topics calls fn for every topic until it returns false.
func (e *Engine[P]) Topics(fn func(*Topic[P]) bool) {
	e.topics.Range(func(_ registry.Key, t *Topic[P]) bool { return fn(t) })
}

// Closed reports whether Close or Shutdown has been called. It is true from
// the first phase of the shutdown, not only from the last: an operation
// refused in any phase but Running is refused because the engine is closing.
func (e *Engine[P]) Closed() bool { return e.lc.Closing() }

// closing is Closed under the name the engine's internals use. It is one
// atomic load, the same as the closed flag it replaced.
func (e *Engine[P]) closing() bool { return e.lc.Closing() }

// Lifecycle returns the engine's shutdown state machine, which the adapter
// wraps and the tests read the phase from.
func (e *Engine[P]) Lifecycle() *lifecycle.Lifecycle { return e.lc }

// reserveTotal books n records and size bytes against the engine-wide limits.
// A refusal leaves the ledger as it found it and announces the withdrawal of
// the record slots it had to take first, exactly as releaseTotal announces a
// release.
func (e *Engine[P]) reserveTotal(n, size int64) bool {
	if e.maxMsgs > 0 {
		for {
			v := e.msgs.Load()
			if v+n > e.maxMsgs {
				return false
			}
			if e.msgs.CompareAndSwap(v, v+n) {
				break
			}
		}
	} else {
		e.msgs.Add(n)
	}
	if e.maxBytes > 0 {
		for {
			v := e.bytes.Load()
			if v+size > e.maxBytes {
				// The record slots go back through the function that
				// announces a release rather than beside it, which is what
				// keeps the withdrawal and the release announcing the same
				// things: a publisher parked in Topic.waitLogRoom re-checks
				// hasTotalRoom, which reads the count these slots were part
				// of, and a WaitIdle parked in IdleIncludingDelayed reads
				// that count as its whole condition. No bytes were ever taken, so the byte
				// half of the release is zero.
				e.releaseTotal(n, 0)
				return false
			}
			if e.bytes.CompareAndSwap(v, v+size) {
				return true
			}
		}
	}
	e.bytes.Add(size)
	return true
}

// fits reports whether a record of size bytes could ever be admitted to the
// engine, whatever else is settled first (lane.fits).
func (e *Engine[P]) fits(size int64) bool { return e.fitsTotal(1, size) }

// fitsTotal reports whether an engine-wide reservation of n records and size
// bytes could ever succeed, whatever else is settled first. It is the
// reservation, not the record, that a publisher waits for: a shared log books
// one record and its bytes for every subscription that will read the slot
// (logRing.reserve), so a topic with more subscriptions than the engine-wide
// record limit is a wait that can never end and is refused instead.
func (e *Engine[P]) fitsTotal(n, size int64) bool {
	return (e.maxMsgs <= 0 || n <= e.maxMsgs) && (e.maxBytes <= 0 || size <= e.maxBytes)
}

// roomAfterDrops reports whether an engine-wide reservation of n records and
// size bytes would succeed once the lanes it is for had dropped what they can
// give up, freeMsgs records and freeBytes bytes (lane.droppable). What the
// rest of the engine holds is out of their reach, so lanes that would empty
// themselves and still leave the reservation refused have nothing worth
// displacing (lane.roomAfterDrops is the same rule for a lane's own
// capacity).
func (e *Engine[P]) roomAfterDrops(n, size, freeMsgs, freeBytes int64) bool {
	return e.fitsTotal(n, size) &&
		(e.maxMsgs <= 0 || e.msgs.Load()-freeMsgs+n <= e.maxMsgs) &&
		(e.maxBytes <= 0 || e.bytes.Load()-freeBytes+size <= e.maxBytes)
}

// limitsTotal reports whether the engine has an engine-wide limit at all. An
// engine that has none can refuse no reservation for one, so a publisher
// waiting for room never has to be woken by a release of it — which keeps
// the wake of the common configuration on the log that is actually full
// (Topic.waitLogRoom).
func (e *Engine[P]) limitsTotal() bool { return e.maxMsgs > 0 || e.maxBytes > 0 }

// hasTotalRoom reports whether an engine-wide reservation of n records and
// size bytes would succeed now. It reserves nothing: it is what a publisher
// parked under OverflowBlock re-checks before it parks again.
func (e *Engine[P]) hasTotalRoom(n, size int64) bool {
	return (e.maxMsgs <= 0 || e.msgs.Load()+n <= e.maxMsgs) &&
		(e.maxBytes <= 0 || e.bytes.Load()+size <= e.maxBytes)
}

// releaseTotal returns an engine-wide reservation and wakes whoever is
// parked on the limit it was taken against. The budget is given back before
// the wake, so a publisher that looks again sees the room this release made;
// off the waiting path each wake is one atomic load (notifyGate.notify).
//
// It announces the fall on BOTH gates that read the engine-wide record
// count, because both are conditions somebody parks on: e.room is the room a
// refused publisher waits for, and e.msgs reaching zero is the whole of
// WaitIdle's IdleIncludingDelayed condition (isIdle). The idle gate used to
// be notified by eng.settled() alone, which the settle path calls beside
// this release and no rollback path calls at all -- so a publication rolled
// back by Topic.unreserveAll, by reserveAll's own refusal, by
// Consumer.duplicateTo or by either retain rollback lowered the count with no
// idle wake, and a WaitIdle that had sampled the count while the reservation
// stood waited for its context rather than for the engine. Every caller that lowers the count routes through here, which is
// why the announcement belongs here rather than beside each of them.
func (e *Engine[P]) releaseTotal(n, size int64) {
	e.msgs.Add(-n)
	e.bytes.Add(-size)
	e.room.notify()
	e.idle.notify()
}

// Totals returns the admitted record count and accounted bytes of the whole
// engine, plus the number of publications discarded because their topic had
// no subscription.
func (e *Engine[P]) Totals() (messages, bytes, discarded int64) {
	return e.msgs.Load(), e.bytes.Load(), e.noSubs.Load()
}

// releasePayload runs the publisher's release callback exactly once, after
// scrubbing the payload if the record's copy and receive modes ask for it.
//
// Which scrub runs is decided by the two contracts: Config.Zero is the
// ZeroOnRelease of the memory engine and applies to every payload the engine
// owns, and Config.Poison is the use-after-free canary and applies to
// the records a consumer was handed by reference. The canary is the stronger
// of the two — it writes a pattern nothing produces by accident — so it
// replaces the zeroing rather than following it. Neither touches a borrowed
// payload: that storage is the caller's, and the engine was only lent it.
func (e *Engine[P]) releasePayload(r *record[P]) {
	if e.ownership() && r.released {
		panic(DoubleReleasePanic)
	}
	r.released = true
	e.scrub(r.v, r.mode)
	if r.release != nil {
		r.release(r.v)
	}
}

// scrub applies the release-time scrub of a payload the engine owns under
// copy mode m. It is factored out of releasePayload because a copy the engine
// made and then never used — the retained log's, when the publication that
// asked for it was refused (publication.finish) — gets the same treatment
// without a record to hang it on.
func (e *Engine[P]) scrub(v P, m CopyMode) {
	switch {
	case !m.Scrubs():
	case e.cfg.Poison != nil && e.cfg.Receive.poisons():
		e.cfg.Poison(v)
	case e.cfg.Zero != nil:
		e.cfg.Zero(v)
	}
}

// ownership reports whether the release contract of an owned publication is
// checked on every release.
func (e *Engine[P]) ownership() bool { return debugCanary || e.cfg.DebugOwnership }

// dropRefs drops one reference on each of recs. It is what a section that
// ran under a lifecycle ticket calls once the ticket is home: the drop may
// release a payload, and a release is the caller's code (Topic.after).
func (e *Engine[P]) dropRefs(recs []*record[P]) {
	for _, rec := range recs {
		e.dropRecordRef(rec, true)
	}
}

// dropRecordRef drops one subscription's reference to rec and releases the
// payload when it was the last.
func (e *Engine[P]) dropRecordRef(rec *record[P], recycle bool) {
	if !rec.releaseOne() {
		return
	}
	e.releasePayload(rec)
	if recycle {
		e.records.Put(rec)
	}
}

// Close shuts the engine down promptly: no further publication or attachment
// is admitted, every waiting consumer is woken with ErrClosed, and every
// record still held by a lane is dropped, which releases its payload exactly
// once. It waits for no handler and for no dead-letter publication.
//
// What it does not wait for is a call of the caller's own. A publication or a
// fetch that Close raced hands back what it displaced — a record a retention
// trim or a full lane took the place of, a delivery a closing consumer could
// not put back, a record that outlived its time to live — and it does so
// after it has given its ticket back, because a release is the caller's code
// and no barrier may wait on that (
// adr/ADR-017-assumptions.md). A payload can therefore still
// come home after Close has returned, on the goroutine of a Publish or a
// Fetch that has not returned either. A caller that needs every payload back
// before it frees the pool they came from waits for its own calls, which it
// is the only one that can do; what the engine promises is that nothing is
// released twice, nothing is left unreleased, and no goroutine of the
// engine's releases anything after Close has returned
// (TestAcceptedPublicationIsPlacedBeforeCloseReturns).
//
// Close is idempotent, safe for concurrent use and never returns an error:
// the engine has no native client to close and no boundary to commit, so
// none of its steps can fail.
func (e *Engine[P]) Close() error { return e.lc.Close() }

// Shutdown is the graceful form of the message contract: the same phases, but
// the drain waits for the in-flight deliveries and the settling phase waits
// for the dead-letter publications, both bounded by Config.DrainTimeout and
// by ctx. It returns lifecycle.ErrDrainTimeout when the deadline passed with
// work still in flight, or ctx's error when the caller ended the wait.
//
// The engine is closed when it returns nil. When it returns one of those two
// errors the run is still finishing — it never abandons a placement, because
// a record placed behind the drain is one nothing releases — and Shutdown has
// stopped waiting for it rather than the other way round. Lifecycle().Done()
// is what says the run is over, and a later Close joins it.
//
// Close and Shutdown are the same run: the first to arrive performs it and
// the other returns its outcome.
func (e *Engine[P]) Shutdown(ctx context.Context) error { return e.lc.Shutdown(ctx) }

// shutdownSteps is what the engine does in each phase of that run.
//
// The order is the message contract's and the one Engine.Close has always
// used; what the lifecycle adds is the barrier between the phases. By the
// time the settling step runs, the publish and fetch gates have been barred
// and drained, so no publication is between its reservation and its
// placement and no fetch is between its claim and its hand-over. That is what
// makes the walk below the last thing to touch a lane, rather than a walk
// another goroutine can still place a record behind.
func (e *Engine[P]) shutdownSteps() lifecycle.Steps {
	return lifecycle.Steps{
		// Stopping acceptance ends the engine's own lifetime context, which
		// is what an internal dead-letter publication in flight is made
		// under, and wakes every publisher parked on a full lane.
		StopAccept: func() {
			e.cancel()
			e.wakeParked()
		},
		// The scheduler is the engine's own fetcher: it promotes delayed
		// records, expires leases and sweeps times to live. Stopping it here
		// is where Engine.Close has always stopped it.
		StopFetch: func() { e.sched.close() },
		// Draining waits for the leases the consumers still hold. It is the
		// IdleSettled condition, which scheduler.go already documents as
		// "the condition a rebalance or a shutdown drain waits for".
		Drain: func(ctx context.Context) error { return e.WaitIdle(ctx, IdleSettled) },
		// Settling drops what the lanes hold, which releases each payload
		// exactly once, and drops what a stream retains: the retained log
		// holds a reference of its own on every record it keeps, so a stream
		// that is never trimmed would otherwise leave the last publications
		// unreleased (Topic.retain).
		Settle: e.drainAll,
		// There is no native client to close; the last step wakes whatever
		// is still parked, so that a caller blocked in WaitIdle or on a full
		// lane returns rather than waiting for an engine that has gone.
		CloseNative: func() error {
			e.idle.notify()
			e.wakeParked()
			return nil
		},
	}
}

// drainAll releases every record the engine still holds.
//
// It walks the topics once and takes the subscription snapshot of each once,
// where Close used to walk twice: a subscription that became visible between
// the two walks was woken but never closed or drained. It cannot happen now —
// Topic.Subscribe publishes the subscription under a publish ticket, so the
// gate this phase has already drained excludes it — and taking one snapshot
// means the two halves cannot disagree even if it could.
func (e *Engine[P]) drainAll() {
	e.Topics(func(t *Topic[P]) bool {
		for _, s := range t.subs.Load().subs {
			s.closeAll()
		}
		t.dropAllRetained()
		return true
	})
	e.idle.notify()
}

// wakeParked wakes every publisher parked on a lane or on the engine-wide
// budget, so that a close never leaves one waiting for room that will not
// come.
func (e *Engine[P]) wakeParked() {
	e.room.notify()
	e.Topics(func(t *Topic[P]) bool {
		for _, s := range t.subs.Load().subs {
			s.room.notify()
			s.waiters.NotifyAll()
		}
		return true
	})
}
