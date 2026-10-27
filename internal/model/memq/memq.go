// Package memq is the executable reference model of the in-memory queue
// engine (the memory engine, the specification D). It mirrors internal/engine in
// Go: a pure, single-goroutine state machine over topics, subscriptions,
// lanes, consumers and records, driven by Step, with Invariants checked after
// every step and a Snapshot for traces.
//
// The model is the oracle of the differential fuzz target FuzzEngineModel: an
// op script is applied to the engine and to the model, and every result, every
// counter and every invariant must agree. models/tla/MemoryLease.tla is the
// formal counterpart and models/tla/TRACE.md maps both onto the engine.
//
// # Generations
//
// The model numbers delivery generations from 1 per record, while the engine
// continues the generation of the pooled envelope it recycles, so generations
// are compared relatively: a handle taken at delivery d settles the record if
// and only if it is presented before delivery d+1, which both sides agree on.
// The harness therefore carries the model generation of every handle it holds
// and perturbs both sides by the same offset.
package memq

import (
	"time"

	"github.com/iqhive/iqmq/internal/subject"
)

// State is the logical state of a record within one subscription
// (the memory engine).
type State uint8

// The record states. Exactly one holds at any time.
const (
	// Ready is queued and waiting for a consumer.
	Ready State = iota
	// Delayed is waiting for its due time.
	Delayed
	// InFlight is leased to a consumer.
	InFlight
	// Acked is terminally settled and released.
	Acked
	// DeadLettered was routed to a dead-letter topic and settled there
	// because the record used up its delivery attempts.
	DeadLettered
	// Dropped was discarded without settlement (engine close).
	Dropped

	stateCount
)

var stateNames = [stateCount]string{"ready", "delayed", "inflight", "acked", "deadlettered", "dropped"}

// String returns the lower-case state name.
func (s State) String() string {
	if s < stateCount {
		return stateNames[s]
	}
	return "state(?)"
}

// Terminal reports whether the state is final for the record.
func (s State) Terminal() bool { return s == Acked || s == DeadLettered || s == Dropped }

// Kind identifies an operation.
type Kind uint8

// The operations, mirroring the engine's API.
const (
	// Subscribe creates (or returns) a subscription of the topic.
	Subscribe Kind = iota
	// Attach creates a consumer of a subscription.
	Attach
	// CloseConsumer detaches a consumer and requeues its leases.
	CloseConsumer
	// Publish admits a record to every subscription.
	Publish
	// Fetch claims ready records for a consumer.
	Fetch
	// Ack settles a delivery terminally.
	Ack
	// Nack settles a delivery negatively and requeues the record.
	Nack
	// Extend moves a lease deadline.
	Extend
	// Advance moves the clock.
	Advance
	// ExpireLeases reclaims every lease whose deadline has passed.
	ExpireLeases
	// CloseSubscription detaches a subscription and drops its records.
	CloseSubscription
	// CloseEngine shuts the engine down.
	CloseEngine
	// Seek replays a stream subscription from a start position, discarding
	// what it holds.
	Seek
	// Rebalance recomputes a sticky subscription's partition assignment and
	// revokes the leases the new table no longer allows.
	Rebalance

	kindCount
)

var kindNames = [kindCount]string{
	"subscribe", "attach", "closeconsumer", "publish", "fetch", "ack", "nack",
	"extend", "advance", "expireleases", "closesubscription", "closeengine",
	"seek", "rebalance",
}

// String returns the lower-case operation name.
func (k Kind) String() string {
	if k < kindCount {
		return kindNames[k]
	}
	return "op(?)"
}

// Retry is the redelivery placement of the engine under test.
type Retry uint8

// The placements. RetryBlock is the reference kind and always seals its lane.
const (
	// RetryBlock holds the lane on the redelivery (engine BlockLane).
	RetryBlock Retry = iota
	// RetryFront serves the newest redelivery first (engine PushFront).
	RetryFront
	// RetrySide serves the redeliveries in the order they failed (engine
	// SideQueue).
	RetrySide
)

// Seals reports whether the placement makes a redelivery block its lane,
// which BlockLane always does and the other two do under strict ordering.
func (r Retry) Seals(strict bool) bool { return r == RetryBlock || strict }

// Overflow is what a publication no subscription had room for does.
type Overflow uint8

// The overflow policies, mirroring engine.OverflowPolicy.
const (
	// OverflowBlock parks the publisher; the model treats it as a refusal.
	OverflowBlock Overflow = iota
	// OverflowReject refuses the publication with ErrQueueFull.
	OverflowReject
	// OverflowDropNewest discards the publication, counts it and reports
	// success.
	OverflowDropNewest
	// OverflowDropOldest makes room by discarding the oldest record a full
	// lane still holds.
	OverflowDropOldest
)

// Config configures the model to match the engine under test.
type Config struct {
	// Partitions is the number of lanes per subscription.
	Partitions int
	// CapacityMessages bounds ready, in-flight and delayed records per lane;
	// a non-positive value means unbounded.
	CapacityMessages int
	// CapacityBytes bounds the same set by size; a non-positive value means
	// unbounded.
	CapacityBytes int64
	// AckDeadline is the lease duration; a non-positive value means leases
	// never expire.
	AckDeadline time.Duration
	// MaxTotalMessages bounds admitted records engine-wide, counted once per
	// subscription copy. A non-positive value means unbounded.
	MaxTotalMessages int
	// MaxTotalBytes bounds the accounted bytes engine-wide the same way.
	MaxTotalBytes int64
	// Ordered reports whether the ready queue and the scheduler that feeds it
	// both preserve order. When it is false a fetch may take any ready record
	// of the lane. When it is true the queue is checked group by group:
	// records that came due at the same instant were promoted together and no
	// scheduler orders such a tie, so they may be taken in any order, while
	// every other admission keeps its place exactly.
	Ordered bool
	// SafeCopy reports that the engine clones the payload once per
	// subscription and hands the publisher's own payload back as soon as the
	// clones are taken, rather than reference-counting one shared record.
	SafeCopy bool
	// Retry is where an immediate redelivery is placed.
	Retry Retry
	// Strict reports that the topic asks for strict ordering, under which
	// every placement blocks its lane until the redelivery is settled.
	Strict bool
	// Overflow is what a publication no subscription had room for does.
	// OverflowBlock is modelled as OverflowReject: the model is a state
	// machine and does not park, and a blocked publisher's retry is an
	// operation the harness drives itself.
	Overflow Overflow
	// MaxAttempts is the delivery budget of a record; zero means no limit.
	MaxAttempts uint32
	// DeadLetter reports that the subscription has a usable dead-letter
	// topic. The model's dead-letter topic is a sink — a topic with no
	// subscription — so a dead-lettered record leaves the subscription and
	// its payload reference is dropped at once, which is what the
	// differential harness configures so that the release ledger stays
	// comparable.
	DeadLetter bool
	// Priority reports that the lanes hold one ready ring per priority
	// level and serve the most urgent first (engine TopologyConfig.Priority).
	// The levels order the ready queue only: a redelivery is served before
	// the store whatever its level, which is what the specification promises.
	Priority bool
	// Levels is the number of priority rings, which must match the engine's
	// PriorityLevels. Zero means priorities are not in use.
	Levels int
	// Wildcards reports that a publication is routed by its subject to the
	// subscriptions whose filter matches it.
	Wildcards bool
	// DedupWindow discards a publication carrying a key the topic has seen
	// inside the window. Zero means no deduplication.
	DedupWindow time.Duration
	// Sticky reports that each lane belongs to one consumer exclusively and
	// that a join or a leave rebalances (the memory engine).
	Sticky bool
	// Retention bounds the retained log of a stream topic. A zero value
	// means the topology retains nothing and Seek is not available.
	Retention Retention
	// SharedLog reports that the engine stores one log per partition, shared
	// by every subscription, rather than one lane per subscription (the
	// LogCursor architecture). A slot is then reclaimed only when every
	// subscription that was subscribed when it was published has settled it,
	// so capacity is shared and the slowest subscription decides how much
	// room the publishers have. The per-subscription gauges are unchanged: a
	// subscription holds a subset of the slots the log holds, so its own
	// depth can never exceed the capacity either.
	SharedLog bool
}

// Retention bounds the retained log of a stream topic, mirroring
// engine.Retention.
type Retention struct {
	// MaxMessages is the number of records one partition retains.
	MaxMessages int
	// MaxBytes is the accounted size one partition retains.
	MaxBytes int64
	// MaxAge is how long a record is retained after its publication.
	MaxAge time.Duration
}

// Enabled reports whether any bound is set.
func (r Retention) Enabled() bool {
	return r.MaxMessages > 0 || r.MaxBytes > 0 || r.MaxAge > 0
}

// StartKind selects where a Seek replays from, mirroring engine.StartKind.
type StartKind uint8

// The start positions.
const (
	// StartLatest replays nothing.
	StartLatest StartKind = iota
	// StartEarliest replays every retained record.
	StartEarliest
	// StartAtSeq replays from a publication sequence, inclusive.
	StartAtSeq
	// StartAtTime replays from a publication time, inclusive.
	StartAtTime
)

// StartPosition is where a Seek begins.
type StartPosition struct {
	// Kind selects how the position is expressed.
	Kind StartKind
	// Seq is the first publication sequence to replay under StartAtSeq.
	Seq uint64
	// At is the first publication time to replay under StartAtTime, on the
	// model clock.
	At time.Duration
}

// Op is one operation applied by Step.
type Op struct {
	// Kind selects the operation.
	Kind Kind
	// Sub names the subscription the operation applies to.
	Sub string
	// Consumer identifies the consumer for Attach (the id to assign),
	// Fetch, CloseConsumer and the settlement operations.
	Consumer uint32
	// Partition is the lane a Publish targets, or -1 for round-robin.
	Partition int
	// Size is the accounted payload size of a Publish.
	Size int64
	// Delay holds a published or nacked record back for this long.
	Delay time.Duration
	// Dur is the lease duration of an Extend.
	Dur time.Duration
	// Max bounds the claims a Fetch takes.
	Max int
	// Seq identifies the record a settlement operation applies to.
	Seq uint64
	// Gen is the generation the settlement operation presents.
	Gen uint32
	// Observed lists the record sequences a Fetch returned, in order. The
	// model validates them against what it would allow and adopts them, so
	// that unordered ready queues stay comparable.
	Observed []uint64
	// Dt is the amount an Advance moves the clock by.
	Dt time.Duration
	// Filter is the subject pattern a Subscribe gives the subscription on a
	// topic with wildcards. An empty filter takes every publication.
	Filter string
	// Subject is the routing subject of a Publish on a topic with wildcards.
	Subject string
	// DedupKey is the deduplication key of a Publish.
	DedupKey string
	// Start is where a Seek replays from.
	Start StartPosition
	// Priority is the ring a Publish goes into on a topic with priorities.
	Priority uint8
}

// Claim is the model's view of one delivery.
type Claim struct {
	// Seq is the model's identity for the record within its subscription. A
	// replayed record is a new admission with a new identity, so it differs
	// from Origin.
	Seq uint64
	// Origin is the publication sequence the engine reports for the record
	// (engine Info.Seq). It is the identity a differential harness compares
	// against, because it is the one both sides can name.
	Origin uint64
	// Gen is the model's delivery generation.
	Gen uint32
	// Attempts counts deliveries including this one.
	Attempts uint32
	// Partition is the lane the record was claimed from.
	Partition int
	// Size is the accounted payload size.
	Size int64
}

// Result is what Step returns; it is what the engine must produce.
type Result struct {
	// Err is the error the engine must return, or nil.
	Err error
	// N is the count returned by Fetch and ExpireLeases.
	N int
	// Claims are the deliveries a Fetch produced.
	Claims []Claim
	// Dropped reports that an overflow policy discarded the publication
	// before admission: the engine reports no error and releases the
	// payload, but no subscription ever holds the record.
	Dropped bool
	// Deduped reports that the topic's dedup window discarded the
	// publication. Like Dropped it is a success that admitted nothing and
	// handed the payload straight back.
	Deduped bool
	// Orphan reports that the publication reached no subscription — the
	// topic has none, or none whose filter matches its subject — so it was
	// discarded and its payload handed straight back.
	Orphan bool
	// Retained reports that the topic's retained log took a publication no
	// subscription holds — an orphan, or one an overflow policy discarded.
	// A stream's log is a property of the topic, so such a publication is
	// still replayable and its payload is the engine's until the log has
	// trimmed it, which is why it is neither a release nor a loss.
	Retained bool
	// Stale reports that the handle the operation presented is not the live
	// delivery of the record. The engine recycles envelopes, so a handle to
	// a record it has already settled may name a recycled envelope whose
	// generation has moved on: the engine is then allowed to answer
	// ErrLeaseLost where the model names the more specific sentinel.
	Stale bool
}

// cellState mirrors the settlement states internal/engine drives a delivery
// through. The memory engine settles locally, so Settling and Unknown never
// occur: Begin and Commit are back to back and no outcome is ambiguous.
type cellState uint8

const (
	// cellIdle holds no lease: a fresh record, or one whose lease expiry or
	// a sticky reassignment revoked it (settlement.Cell.Revoke).
	cellIdle cellState = iota
	// cellActive is leased to a consumer.
	cellActive
	// cellSettled was terminally settled by an ack or a nack.
	cellSettled
	// cellClosed was leased when its consumer or the engine closed.
	cellClosed
)

// rec is one record within one subscription.
type rec struct {
	seq      uint64
	origin   uint64
	size     int64
	lane     int
	state    State
	cell     cellState
	gen      uint32
	attempts uint32
	owner    uint32
	deadline time.Duration
	dueAt    time.Duration
	// reordered reports that the record was held back at some point — a
	// delayed publication or a delayed redelivery — so its place in the
	// lane is its promotion time rather than its publication order. Strict
	// ordering says nothing about such a record: the publisher asked for it
	// to be moved.
	reordered bool
	// overBudget reports that something other than a failed delivery put the
	// record back in its lane — a consumer that closed, or a dead-letter
	// publication that failed — so it may be delivered again even though it
	// has used up its attempts.
	overBudget bool
	// sealed reports that the delivery came out of the retry queue while
	// the lane was sealed, so the lane serves nothing else until it is
	// settled.
	sealed bool
	// prio is the priority ring the record was published into.
	prio uint8
}

// readyQueue models a lane's ready queue as a sequence of groups. Groups are
// consumed in order; the records inside one group may be taken in any order,
// which is what a scheduler leaves unspecified when several records come due
// at the same instant. Every other admission — a publication, a nack, a
// requeue — forms a group of its own, so ordering stays exact for them.
type readyQueue struct {
	seqs   []uint64
	groups []int32
	prios  []uint8
	next   int32
}

// newGroup returns a fresh group identifier.
func (q *readyQueue) newGroup() int32 { q.next++; return q.next }

// push appends seq in a group of its own.
func (q *readyQueue) push(seq uint64, prio uint8) { q.pushGroup(seq, q.newGroup(), prio) }

// pushGroup appends seq to the named group.
func (q *readyQueue) pushGroup(seq uint64, g int32, prio uint8) {
	q.seqs = append(q.seqs, seq)
	q.groups = append(q.groups, g)
	q.prios = append(q.prios, prio)
}

// topPrio returns the highest priority level the queue holds, which is the
// ring a claim is served from.
func (q *readyQueue) topPrio() uint8 {
	top := uint8(0)
	for _, p := range q.prios {
		if p > top {
			top = p
		}
	}
	return top
}

// firstAt returns the index of the first queued record at level p.
func (q *readyQueue) firstAt(p uint8) int {
	for i, got := range q.prios {
		if got == p {
			return i
		}
	}
	return -1
}

// len returns the number of queued records.
func (q *readyQueue) len() int { return len(q.seqs) }

// removeAt takes the record at index i.
func (q *readyQueue) removeAt(i int) uint64 {
	seq := q.seqs[i]
	q.seqs = append(q.seqs[:i], q.seqs[i+1:]...)
	q.groups = append(q.groups[:i], q.groups[i+1:]...)
	q.prios = append(q.prios[:i], q.prios[i+1:]...)
	return seq
}

// reset empties the queue.
func (q *readyQueue) reset() { q.seqs, q.groups, q.prios = nil, nil, nil }

// logSlot is one entry of a shared partition log: the record, its size and
// the number of subscriptions that have not settled it yet.
type logSlot struct {
	seq  uint64
	size int64
	refs int
}

// partLog is the shared log of one partition. Slots are appended at the head
// and reclaimed from the tail, so the occupancy — and therefore the room a
// publisher has — is decided by the slowest subscription.
type partLog struct {
	slots []logSlot
	bytes int64
}

// reserve books one slot of size bytes, or reports false when the log is
// full.
func (l *partLog) reserve(cfg Config, size int64) bool {
	if cfg.CapacityMessages > 0 && len(l.slots) >= cfg.CapacityMessages {
		return false
	}
	if cfg.CapacityBytes > 0 && l.bytes+size > cfg.CapacityBytes {
		return false
	}
	return true
}

// append writes the reserved slot.
func (l *partLog) append(seq uint64, size int64, refs int) {
	l.slots = append(l.slots, logSlot{seq: seq, size: size, refs: refs})
	l.bytes += size
}

// settle drops one subscription's reference to the slot holding seq and
// reclaims every slot at the tail that no subscription holds any more.
func (l *partLog) settle(seq uint64) {
	for i := range l.slots {
		if l.slots[i].seq == seq {
			if l.slots[i].refs > 0 {
				l.slots[i].refs--
			}
			break
		}
	}
	n := 0
	for n < len(l.slots) && l.slots[n].refs == 0 {
		l.bytes -= l.slots[n].size
		n++
	}
	l.slots = append(l.slots[:0], l.slots[n:]...)
}

// retryRec is one record waiting for redelivery in a lane's retry queue,
// with the time it becomes deliverable again (0 for immediately).
type retryRec struct {
	seq   uint64
	dueAt time.Duration
}

// lane is one subscription-partition. Retry is the redelivery queue, served before the ready queue and — while the placement seals
// the lane — instead of it; out counts the sealed redeliveries currently
// leased, of which there is never more than one.
type lane struct {
	ready   readyQueue
	retry   []retryRec
	out     int
	live    []uint64 // in-flight, in deadline order
	delayed []uint64
	depth   int
	bytes   int64
}

// counters mirror the engine's per-subscription statistics.
type counters struct {
	published, acked, nacked, requeued, expired, dropped, rejected, attempts int64
	deadLettered, overflowed, dlqFailed                                      int64
	// displaced counts the records an overflow policy discarded to make
	// room for a newer one; they are counted in dropped as well, which is
	// what the engine reports.
	displaced int64
}

// Orphans returns the number of publications discarded because their topic
// had no subscription.
func (m *Model) Orphans() int64 { return m.orphans }

// sub is one subscription.
type sub struct {
	name      string
	filter    *subject.Pattern
	lanes     []lane
	recs      map[uint64]*rec
	consumers map[uint32]bool
	members   []uint32 // attached consumer ids, in id order
	owner     []uint32 // lane -> consumer under a sticky assignment
	version   uint64   // rebalance generation
	st        counters
	closed    bool
	order     []uint64 // records in admission order, for stable reporting
}

// Model is the reference model of one engine holding one topic.
type Model struct {
	cfg    Config
	subs   []*sub
	byName map[string]*sub

	now time.Duration
	// seq is the publication sequence, which is what the engine reports as
	// Info.Seq and what the release ledger is keyed by; rid is the model's
	// own record identity, which a replay also consumes because a replayed
	// record is a second, independently settled admission of one
	// publication (see stream.go).
	seq     uint64
	rid     uint64
	rr      int
	msgs    int64
	bytes   int64
	closed  bool
	steps   int
	refs    map[uint64]int // record sequence -> subscriptions yet to settle
	release map[uint64]int // record sequence -> release callbacks issued
	orphans int64          // publications released for want of a subscription
	fetchRR map[uint64]int // consumer key -> next lane rotation index
	logs    []partLog      // shared partition logs, only when SharedLog is set
	dlqFail bool           // the dead-letter fault of engine.Faults.FailDeadLetter

	// retained is the replayable log of a stream topic, one per partition,
	// and dedup is the publication-key window; both are empty unless the
	// configuration asks for them.
	retained []retLog
	dedup    map[string]time.Duration
	deduped  int64
}

// retSlot is one entry of a partition's retained log.
type retSlot struct {
	// subj is the subject the publication carried, which a replay filters
	// on: a replayed record reaches only the subscriptions whose filter
	// matches it, exactly as a live one does.
	subj string
	seq  uint64
	size int64
	at   time.Duration
	// dueAt is the time the publication was due, or 0 when it was due at
	// once: a replay before it admits the record delayed until then.
	dueAt time.Duration
	// prio is the priority ring the publication went into, which a replay
	// puts the record back in: a replayed record keeps its urgency.
	prio uint8
}

// retLog is the retained log of one partition: entries in publication order,
// trimmed from the tail by the retention bounds.
type retLog struct {
	ents  []retSlot
	bytes int64
}

// FailDeadLetter arms or disarms the dead-letter fault, mirroring
// engine.Faults.FailDeadLetter: while it is armed every dead-letter
// publication fails and its record goes back to its lane.
func (m *Model) FailDeadLetter(on bool) { m.dlqFail = on }

// New returns an empty model. Partitions below one are treated as one.
func New(cfg Config) *Model {
	if cfg.Partitions < 1 {
		cfg.Partitions = 1
	}
	m := &Model{
		cfg:     cfg,
		byName:  map[string]*sub{},
		refs:    map[uint64]int{},
		release: map[uint64]int{},
		fetchRR: map[uint64]int{},
	}
	if cfg.SharedLog {
		m.logs = make([]partLog, cfg.Partitions)
	}
	if cfg.Retention.Enabled() {
		m.retained = make([]retLog, cfg.Partitions)
	}
	if cfg.DedupWindow > 0 {
		m.dedup = map[string]time.Duration{}
	}
	if cfg.Levels <= 0 {
		cfg.Levels = 1
		m.cfg.Levels = 1
	}
	return m
}

// Now returns the model clock.
func (m *Model) Now() time.Duration { return m.now }

// Steps returns the number of operations applied.
func (m *Model) Steps() int { return m.steps }

// Advance moves the model clock forward and does, in time order, the work the
// engine's scheduler does while it passes: a record that comes due is
// promoted into its lane's ready queue at its own instant, and a lease whose
// deadline arrives is reclaimed at its own instant. Doing both at the end of
// the step instead would put them in the wrong order whenever a promotion and
// an expiry fall on either side of one another, which is exactly what a lane
// with a delayed record and a live lease produces. A non-positive delta is
// ignored.
func (m *Model) Advance(dt time.Duration) {
	if dt <= 0 {
		return
	}
	target := m.now + dt
	for {
		at, ok := m.nextEvent(target)
		if !ok {
			break
		}
		m.now = at
		m.tick()
	}
	m.now = target
	m.tick()
}

// tick applies everything that is due at the model clock.
func (m *Model) tick() {
	for _, s := range m.subs {
		if s.closed {
			continue
		}
		for li := range s.lanes {
			m.promote(s, &s.lanes[li])
		}
	}
	m.expire()
}

// nextEvent returns the earliest time strictly after the model clock and not
// after target at which a record comes due or a lease expires.
func (m *Model) nextEvent(target time.Duration) (time.Duration, bool) {
	best, found := time.Duration(0), false
	consider := func(at time.Duration) {
		if at <= m.now || at > target {
			return
		}
		if !found || at < best {
			best, found = at, true
		}
	}
	for _, s := range m.subs {
		if s.closed {
			continue
		}
		for li := range s.lanes {
			ln := &s.lanes[li]
			for _, seq := range ln.delayed {
				consider(s.recs[seq].dueAt)
			}
			for _, ent := range ln.retry {
				consider(ent.dueAt)
			}
			if len(ln.live) > 0 {
				consider(s.recs[ln.live[0]].deadline)
			}
		}
	}
	return best, found
}

// Subscriptions returns the subscription names in creation order.
func (m *Model) Subscriptions() []string {
	out := make([]string, 0, len(m.subs))
	for _, s := range m.subs {
		out = append(out, s.name)
	}
	return out
}

// Closed reports whether the engine has been closed.
func (m *Model) Closed() bool { return m.closed }

// Deduplicated returns the number of publications the dedup window
// discarded.
func (m *Model) Deduplicated() int64 { return m.deduped }

// Retained returns the number of records and the accounted bytes the retained
// logs hold, after applying the age bound.
func (m *Model) Retained() (count int, bytes int64) {
	m.trimRetention()
	for i := range m.retained {
		count += len(m.retained[i].ents)
		bytes += m.retained[i].bytes
	}
	return count, bytes
}

// Assignment returns a subscription's rebalance generation and lane owners.
func (m *Model) Assignment(name string) (uint64, []uint32) {
	_, s := m.find(name)
	if s == nil {
		return 0, nil
	}
	out := make([]uint32, len(s.owner))
	copy(out, s.owner)
	return s.version, out
}

// Released returns the number of times the payload of the record with the
// given sequence was released. The conservation invariant requires it never
// to exceed one.
func (m *Model) Released(seq uint64) int { return m.release[seq] }

// Sequence returns the publication sequence the last admitted publication
// took, which is the identity both sides of a differential run can name: the
// engine reports it as Info.Seq and Claim.Origin carries it. A publication
// the model refused, deduplicated or discarded does not advance it.
func (m *Model) Sequence() uint64 { return m.seq }
