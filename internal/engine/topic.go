package engine

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/lifecycle"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/hash"
	"github.com/iqhive/iqmq/internal/registry"
	"github.com/iqhive/iqmq/internal/subject"
	"github.com/iqhive/iqmq/internal/topicname"
	"github.com/iqhive/iqmq/internal/wait"
)

// TopologyKind is the topology profile of a topic (the memory engine).
type TopologyKind uint8

// The topology profiles. The zero value is TopologyDefault, which normalise
// reads as TopologyTopic.
const (
	// TopologyDefault leaves the profile to the engine default, which is
	// TopologyTopic.
	TopologyDefault TopologyKind = iota
	// TopologyQueue is competing consumers over one shared subscription:
	// exactly one subscription, any number of consumers, and a record that
	// leaves the engine when it is acknowledged. A second, differently named
	// Subscribe is a configuration error rather than a silent fan-out.
	TopologyQueue
	// TopologyTopic is fan-out to independent subscriptions: each keeps its
	// own envelopes, its own settlement state and its own capacity, and a
	// subscription created after a publication does not receive it.
	TopologyTopic
	// TopologyStream is the retained, replayable log: fan-out to
	// independent subscriptions like a topic, plus a bounded per-partition
	// log every subscription can seek in. A subscription may start at the
	// earliest retained record, at a sequence, at a time, or at the live
	// end, and may replay at any time with Seek. A stream topic with no
	// Retention bound retains one lane's worth of records.
	TopologyStream

	topologyCount
)

var topologyNames = [topologyCount]string{"default", "queue", "topic", "stream"}

// String returns the lower-case profile name.
func (k TopologyKind) String() string {
	if k < topologyCount {
		return topologyNames[k]
	}
	return "topology(?)"
}

// TopologyConfig configures one topic (the memory engine). Zero fields take
// the engine's default topology, and after that the package defaults.
type TopologyConfig struct {
	// Kind is the topology profile. Zero means the engine default, which is
	// TopologyTopic.
	Kind TopologyKind
	// Fanout is the admission policy a publication follows when one of its
	// target subscriptions has no room. Zero means FanoutAuto,
	// which resolves to the recorded default.
	Fanout FanoutPolicy
	// Overflow decides what a publication the fan-out policy could not place
	// does: wait, fail, be discarded, or displace the oldest record. Zero
	// means OverflowDefault, which the engine reads as OverflowReject
	// (adr/ADR-017-assumptions.md).
	Overflow OverflowPolicy
	// Ordering is the delivery-order guarantee of a lane. Zero means
	// OrderingDefault, which the engine reads as OrderingRelaxed; under
	// OrderingStrict a redelivery blocks its lane until it is settled or
	// dead-lettered. CreateTopic refuses OrderingStrict on an engine whose
	// kinds do not order what they hold (KindSet.QueueOrdered).
	Ordering OrderingMode
	// Partitions is the number of lanes per subscription. Zero means
	// DefaultPartitions, which is one lane and therefore strict order.
	Partitions int
	// CapacityMessages bounds the ready, in-flight and delayed records of
	// one lane. Zero means DefaultCapacityMessages; a negative value means
	// unbounded, which forces the unbounded ready-queue kind.
	CapacityMessages int
	// CapacityBytes bounds the same set by accounted payload size. A
	// non-positive value means no byte limit.
	CapacityBytes int64
	// AckDeadline is the lease duration of a delivery. Zero means
	// DefaultAckDeadline; a negative value means leases never expire.
	AckDeadline time.Duration
	// MaxMessageSize rejects larger publications with ErrMessageTooLarge. A
	// non-positive value means no limit.
	MaxMessageSize int
	// MaxAttempts is the number of deliveries a record may have before a
	// failed one routes it to DeadLetterTopic. Zero means no limit, which
	// is redelivery until the record is acknowledged or expires.
	MaxAttempts uint32
	// DeadLetterTopic names the topic a record is published to once it has
	// used up its attempts. The topic must already exist: the engine never
	// creates one behind the operator's back. A record whose attempts run
	// out with no usable dead-letter topic is discarded and counted in
	// Snapshot.Dropped.
	DeadLetterTopic string
	// Retention bounds the retained log of a stream topic (the API contract
	// The specification). It is meaningful only under TopologyStream, where a zero value
	// retains CapacityMessages records per partition; naming it on any
	// other topology is a configuration error, because a topic that
	// advertises no replay must not quietly hold records for one.
	Retention Retention
	// Priority gives each lane one ready ring per priority level and serves
	// the most urgent first (the memory engine). A publication names its
	// level in Publish.Priority.
	Priority bool
	// Wildcards routes each publication by its Publish.Subject to the
	// subscriptions whose SubscriptionConfig.Filter matches it, using the
	// dot-separated `*`/`>` patterns of internal/subject. A subscription
	// with no filter takes every publication, which is what every topic
	// without wildcards does.
	Wildcards bool
	// DedupWindow discards a publication carrying a Publish.DedupKey the
	// topic has already seen within the window, which runs from the
	// admission of the publication that carried the key (dedupWindow.commit).
	// Zero means no deduplication.
	DedupWindow time.Duration
}

// normalise merges c over base and then over the package defaults, and
// rejects contradictory values.
func (c TopologyConfig) normalise(base TopologyConfig) (TopologyConfig, error) {
	if c.Kind == TopologyDefault {
		c.Kind = base.Kind
	}
	if c.Fanout == FanoutAuto {
		c.Fanout = base.Fanout
	}
	if c.Overflow == OverflowDefault {
		c.Overflow = base.Overflow
	}
	if c.Ordering == OrderingDefault {
		c.Ordering = base.Ordering
	}
	if c.Partitions == 0 {
		c.Partitions = base.Partitions
	}
	if c.CapacityMessages == 0 {
		c.CapacityMessages = base.CapacityMessages
	}
	if c.CapacityBytes == 0 {
		c.CapacityBytes = base.CapacityBytes
	}
	if c.AckDeadline == 0 {
		c.AckDeadline = base.AckDeadline
	}
	if c.MaxMessageSize == 0 {
		c.MaxMessageSize = base.MaxMessageSize
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = base.MaxAttempts
	}
	if c.DeadLetterTopic == "" {
		c.DeadLetterTopic = base.DeadLetterTopic
	}
	if c.Retention == (Retention{}) {
		c.Retention = base.Retention
	}
	if !c.Priority {
		c.Priority = base.Priority
	}
	if !c.Wildcards {
		c.Wildcards = base.Wildcards
	}
	if c.DedupWindow == 0 {
		c.DedupWindow = base.DedupWindow
	}
	if c.Partitions == 0 {
		c.Partitions = DefaultPartitions
	}
	if c.CapacityMessages == 0 {
		c.CapacityMessages = DefaultCapacityMessages
	}
	if c.AckDeadline == 0 {
		c.AckDeadline = DefaultAckDeadline
	}
	if c.Kind == TopologyDefault {
		c.Kind = TopologyTopic
	}
	if !c.Overflow.Valid() || !c.Ordering.Valid() {
		return c, ErrInvalidConfig
	}
	c.Fanout = c.Fanout.Resolve()
	c.Overflow = c.Overflow.Resolve()
	c.Ordering = c.Ordering.Resolve()
	if c.Partitions < 0 || c.Partitions > maxPartitions {
		return c, ErrInvalidConfig
	}
	if !c.Fanout.Valid() || c.Kind >= topologyCount {
		return c, ErrInvalidConfig
	}
	if c.Retention.MaxMessages < 0 || c.Retention.MaxBytes < 0 || c.Retention.MaxAge < 0 {
		return c, ErrInvalidConfig
	}
	if c.Kind == TopologyStream {
		if !c.Retention.Enabled() {
			// A stream is a bounded retained log (the memory engine), so a
			// stream that names no bound takes one lane's worth of records
			// per partition rather than growing without limit
			// (adr/ADR-017-assumptions.md).
			c.Retention.MaxMessages = c.CapacityMessages
			if c.Retention.MaxMessages <= 0 {
				c.Retention.MaxMessages = DefaultCapacityMessages
			}
		}
	} else if c.Retention.Enabled() {
		// Retention on a topology that advertises no replay would keep
		// records nothing can read, which is the silent-stream failure of
		// adr/ADR-017-assumptions.md by another route.
		return c, ErrInvalidConfig
	}
	if c.DedupWindow < 0 {
		return c, ErrInvalidConfig
	}
	if c.DeadLetterTopic != "" && c.DeadLetterTopic != noDeadLetter {
		if err := topicname.Validate(c.DeadLetterTopic); err != nil {
			return c, err
		}
	}
	return c, nil
}

// noDeadLetter is the DeadLetterTopic value that turns a dead-letter topic
// off: for one subscription against its topic, and for one topic against the
// engine's default topology. An empty value means "inherit" and every other
// value is a topic name, so the "none" case needs a spelling of its own.
//
// It must be a spelling that can never name a topic, or a legal
// configuration would be silently reinterpreted. A leading separator is
// never part of a portable name (the message contract, internal/topicname), so
// topicname.Validate rejects this one and CreateTopic can never make a topic
// it collides with; TestNoDeadLetterSentinelCannotName pins that. The
// previous spelling, "-", is a perfectly good one-segment name.
const noDeadLetter = "/none"

// maxPartitions bounds Partitions so that a misconfigured topic cannot ask
// for an unbounded number of lanes.
const maxPartitions = 1024

// Topic is a named destination: partitions on the publish side, independent
// subscriptions on the delivery side (the memory engine).
type Topic[P any] struct {
	name string
	key  registry.Key
	eng  *Engine[P]
	cfg  TopologyConfig

	// rings is the shared partition log of a shared architecture
	// (Arch.Shared), one per partition, and nil for every other
	// architecture.
	rings []*logRing[P]

	// retained is the replayable log of a stream topic, one per partition,
	// and nil for every other topology.
	retained []*retainedLog[P]

	// dedup is the publication-key window of a topic with DedupWindow set.
	dedup *dedupWindow

	// deduped counts the publications the window discarded.
	deduped atomic.Int64

	subs atomic.Pointer[subSet[P]]
	mu   sync.Mutex // writer lock for the copy-on-write subs slice

	// room wakes the publishers parked under OverflowBlock that any lane of
	// the topic can satisfy: a fan-out that admits a record wherever it fits
	// (Topic.waitAnyRoom) and, under DropSlow, a shared log whose
	// subscriptions may displace records again (Topic.waitLogRoom). Every
	// lane announces room it gives back here as well as on its
	// subscription's gate (lane.roomBack), and under DropSlow a record a
	// lease gave back unsettled too (lane.requeue).
	room notifyGate

	// join is the boundary between a publication in flight and a
	// subscription joining under it. Only a topology with a retained log
	// has one to keep (joinBoundary).
	join joinBoundary

	seq atomic.Uint64
	rr  atomic.Uint64
}

// Name returns the topic name.
func (t *Topic[P]) Name() string { return t.name }

// ring returns the shared partition log of partition i.
func (t *Topic[P]) ring(i int) *logRing[P] { return t.rings[i] }

// Topology returns the normalised topology of the topic.
func (t *Topic[P]) Topology() TopologyConfig { return t.cfg }

// Subscriptions returns the current subscriptions in creation order.
func (t *Topic[P]) Subscriptions() []*Subscription[P] {
	subs := t.subs.Load().subs
	out := make([]*Subscription[P], len(subs))
	copy(out, subs)
	return out
}

// Subscription returns the named subscription of the topic.
func (t *Topic[P]) Subscription(name string) (*Subscription[P], error) {
	for _, s := range t.subs.Load().subs {
		if s.name == name {
			return s, nil
		}
	}
	return nil, ErrNoSubscription
}

// Subscribe returns the named subscription, creating it when it does not
// exist. A second call with the same name returns the existing subscription
// and ignores cfg, which must still be one the engine could honour. A
// subscription created after a publication does not receive it: the queue
// topology retains nothing.
//
// A subscription of a stream that asks for a replay is given every record
// exactly once — the ones it replays and the ones published while it joined,
// with no gap and no duplicate, whatever the two goroutines do (joinBoundary)
// — but *not* necessarily in publication order. The replay walks the retained
// log with the join boundary released, because it runs the caller's Clone and
// Release callbacks and no callback of the caller's runs under an internal
// lock (the message contract), so a publication made while it runs can reach
// the lane between two replayed records: a one-partition topic with one
// publisher served [1 9 2 3 4 5 6 7 8 10] in 48 rounds of 300. It is the same
// caveat Seek carries and for the same reason — the join is a control
// operation, and a caller that wants the stream's order from its beginning
// quiesces the topic before it subscribes. Once the join has returned, the
// lane is ordered: the memory engine's one ordered lane holds for every
// record admitted after it (adr/ADR-017-assumptions.md).
//
// It fails with ErrInvalidConfig when cfg asks for something the engine's
// architecture cannot give — a per-subscription capacity under a shared
// partition log — and when a second subscription is added to a queue topic.
func (t *Topic[P]) Subscribe(name string, cfg SubscriptionConfig) (*Subscription[P], error) {
	// The subscription becoming visible and the replay into it are one
	// placement as far as a close is concerned: the ticket keeps the settling
	// phase from walking the subscription set until both are done, so a
	// subscription cannot become visible — with its closed flag unset and its
	// lanes full of replayed records — after the walk that would have drained
	// it. It is taken here rather than in subscribe because the replay is the
	// other half of the placement and deliberately runs outside the topic
	// mutex.
	//
	// The trim runs before the ticket: it drops the entries the age bound no
	// longer covers, and a drop runs the publisher's Release callback.
	t.trimRetention(t.eng.now())
	tk := t.eng.lc.Enter(lifecycle.OpPublish)
	if !tk.OK() {
		t.eng.lc.Leave(tk)
		return nil, errcode.ErrClosed
	}
	s, gen, created, err := t.subscribe(name, cfg)
	if err != nil || !created {
		t.eng.lc.Leave(tk)
		return s, err
	}
	// The replay runs with the topic mutex released, and gives nothing back
	// while it runs: the subscription is already visible to publishers at
	// this point (storeSubs), which is what makes the boundary hold without
	// the mutex, and the records it gives up — the entries it had no room
	// for and, on a copying topology, the retained record each copy
	// replaces — come back here. Each may be the last reference on a record,
	// and the last reference runs the publisher's Release callback, which
	// the message contract keeps out of the engine's locks and the specification keeps
	// out of a section Close waits for. The ticket is a section Close waits
	// for, so the drops follow it (Topic.after).
	_, gone := s.replay(cfg.Start, gen, nil)
	// The close is read again after the admissions, for lane.requeueOrDrop's
	// reason: a close that landed between subscribe's own look at it and the
	// replay above has already drained these lanes, and Subscription.Close
	// detaches before it drains, so nothing visits them again and every
	// replayed record, its payload and its engine-wide reservation would be
	// leaked. The engine's own close can only find lanes the settling drain
	// has yet to walk — that walk runs after this ticket is home — so that
	// half of the look is belt-and-braces rather than load-bearing.
	//
	// The re-drain itself releases payloads, so like the drops it runs once
	// the ticket is home rather than inside it.
	redrain := s.closed.Load() || t.eng.closing()
	t.eng.lc.Leave(tk)
	t.eng.dropRefs(gone)
	if redrain {
		s.drain()
	}
	return s, nil
}

// subscribe creates the named subscription under the topic mutex and reports
// whether it created it; the replay its start position asks for is the
// caller's, so that no part of it runs under the mutex. The generation it
// returns is the boundary the replay is bounded by.
func (t *Topic[P]) subscribe(name string, cfg SubscriptionConfig) (*Subscription[P], uint64, bool, error) {
	if t.eng.closing() {
		return nil, 0, false, errcode.ErrClosed
	}
	if err := topicname.Validate(name); err != nil {
		return nil, 0, false, err
	}
	if t.eng.kinds.Arch.Shared() && (cfg.CapacityMessages != 0 || cfg.CapacityBytes != 0) {
		// One log slot carries the record for every subscription, so the
		// capacity of a shared architecture belongs to the log and there is
		// no per-subscription reservation to bound (adr/ADR-017-assumptions). Honouring the field would mean refusing a
		// publication the log has room for, which is the isolation this
		// architecture does not offer; refusing the configuration says so
		// instead of ignoring it.
		return nil, 0, false, ErrInvalidConfig
	}
	if s, err := t.Subscription(name); err == nil {
		return s, 0, false, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, err := t.Subscription(name); err == nil {
		return s, 0, false, nil
	}
	if t.cfg.Kind == TopologyQueue && len(t.subs.Load().subs) > 0 {
		// A queue is competing consumers over one subscription; a second
		// subscription would make it a fan-out behind the caller's back.
		return nil, 0, false, ErrInvalidConfig
	}
	filter, err := t.compileFilter(cfg.Filter)
	if err != nil {
		return nil, 0, false, err
	}
	if cfg.Start.Kind != StartLatest && !t.retains() {
		// A start position on a topology that retains nothing would be
		// silently ignored, which is the silent-stream failure of
		// adr/ADR-017-assumptions.md.
		return nil, 0, false, ErrInvalidConfig
	}
	if !cfg.Start.Kind.Valid() {
		return nil, 0, false, ErrInvalidConfig
	}
	st, err := newSubStats(t.eng.kinds.Stats, t.eng.cfg.Workers)
	if err != nil {
		return nil, 0, false, err
	}
	s := &Subscription[P]{
		name:        name,
		topic:       t,
		eng:         t.eng,
		capMessages: t.cfg.CapacityMessages,
		capBytes:    t.cfg.CapacityBytes,
		ackDeadline: t.cfg.AckDeadline,
		maxAttempts: t.cfg.MaxAttempts,
		strict:      t.cfg.Ordering.Strict(),
		stream:      t.retains(),
		filter:      filter,
		// Strict ordering over more than one lane is only a promise if each
		// lane has one consumer: competing consumers interleave the records
		// of the lane they share (the memory engine).
		sticky:  cfg.Sticky || (t.cfg.Ordering.Strict() && t.cfg.Partitions > 1),
		dlqName: t.cfg.DeadLetterTopic,
		st:      st,
	}
	switch {
	case cfg.MaxAttempts < 0:
		s.maxAttempts = 0
	case cfg.MaxAttempts > 0:
		s.maxAttempts = uint32(cfg.MaxAttempts)
	}
	switch cfg.DeadLetterTopic {
	case "":
	case noDeadLetter:
		s.dlqName = ""
	default:
		if err := topicname.Validate(cfg.DeadLetterTopic); err != nil {
			return nil, 0, false, err
		}
		s.dlqName = cfg.DeadLetterTopic
	}
	if s.dlqName == noDeadLetter {
		s.dlqName = ""
	}
	if cfg.CapacityMessages != 0 {
		s.capMessages = cfg.CapacityMessages
	}
	if cfg.CapacityBytes != 0 {
		s.capBytes = cfg.CapacityBytes
	}
	if cfg.AckDeadline != 0 {
		s.ackDeadline = cfg.AckDeadline
	}
	if s.waiters, err = wait.New(t.eng.kinds.Wait); err != nil {
		return nil, 0, false, err
	}
	now := t.eng.now()
	s.lanes = make([]*lane[P], t.cfg.Partitions)
	for i := range s.lanes {
		if s.lanes[i], err = newLane(s, i, now); err != nil {
			return nil, 0, false, err
		}
	}
	s.assign.Store(&assignment{owner: make([]uint32, len(s.lanes))})
	old := t.subs.Load().subs
	next := make([]*Subscription[P], len(old), len(old)+1)
	copy(next, old)
	next = append(next, s)
	// The set is stored inside the topic's join boundary, so that a record
	// on either side of it is admitted exactly once: a publication that
	// routed on a set holding s delivers it live and is stamped at or above
	// s's own generation, which the replay skips; one that routed on an
	// older set did not deliver it and had reached the retained log before
	// this store, so the replay below finds it there (joinBoundary).
	gen := t.storeJoined(next, s)
	return s, gen, true, nil
}

// compileFilter validates a subscription's subject pattern against the
// topic's wildcard setting.
//
// A nil pattern is the "matches everything" filter, which is what an empty
// pattern means and what every subscription of a topic without wildcards
// has; it is a value, not a missing one, so the pair (nil, nil) is the
// success case here.
func (t *Topic[P]) compileFilter(pat string) (*subject.Pattern, error) {
	if pat == "" {
		return nil, nil //nolint:nilnil // a nil pattern is the match-everything filter
	}
	if !t.cfg.Wildcards {
		// A filter the topic would ignore is refused rather than dropped:
		// a subscription that asked to see a subset must not silently see
		// everything.
		return nil, ErrInvalidConfig
	}
	p, err := subject.Compile(pat)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// storeSubs publishes a new subscription set and, under a shared partition
// log, gives the joining subscription its cursor at the same time.
//
// Both happen under the mutex of every partition log, which is the mutex a
// publication reserves its slot and reads the subscription set under. A
// publication is therefore either wholly before this — its slot index is
// below the cursor the new subscription starts from, so the record it wrote
// for an older set is not delivered to a subscription that was never
// accounted for it — or wholly after it, in which case the new subscription
// is part of it. The cursor is the index past every slot the log holds and
// every reservation it has handed out, because a reservation always becomes
// the append of the next free index.
//
// The joining subscription is not reachable by any other goroutine yet, so
// its stores are written without their own mutexes; taking those here would
// invert the order every claim takes them in (store first, then ring).
func (t *Topic[P]) storeSubs(next []*Subscription[P], s *Subscription[P]) uint64 {
	set := &subSet[P]{gen: t.subs.Load().gen + 1, subs: next}
	if t.rings == nil {
		t.subs.Store(set)
		return set.gen
	}
	for _, r := range t.rings {
		r.mu.Lock()
	}
	if s != nil {
		for i, r := range t.rings {
			if ls, ok := s.lanes[i].store.(*logStore[P]); ok {
				ls.cursor = r.nextIndexLocked()
			}
		}
	}
	t.subs.Store(set)
	for i := len(t.rings) - 1; i >= 0; i-- {
		t.rings[i].mu.Unlock()
	}
	// A reservation of a shared log is one slot and one engine-wide record
	// for every subscription the log has, so the set this stores is part of
	// the condition a parked publisher waits on: a subscription that leaves
	// makes a reservation smaller and can satisfy it without anything being
	// settled (Topic.waitLogRoom).
	for _, r := range t.rings {
		r.room.notify()
	}
	return set.gen
}

// Publish is one publication (the memory engine). The engine never inspects
// Payload; everything it routes or accounts on is a field of this struct.
type Publish[P any] struct {
	// Payload is the value delivered to every subscription.
	Payload P
	// Key selects the partition. An empty key uses round-robin.
	Key []byte
	// Size is the accounted payload size in bytes.
	Size int
	// Partition selects the lane directly when it is not negative: a value
	// past the last lane wraps, modulo Partitions, as the reference model
	// does. A negative value —
	// the conventional -1 — asks the engine to choose, by Key when there is
	// one and round-robin otherwise. Note that the zero value therefore
	// names lane 0: a publisher that wants the engine to spread its records
	// must say so.
	Partition int
	// Delay holds the record back for this long. It is ignored when
	// DeliverAt is set.
	Delay time.Duration
	// DeliverAt holds the record back until this time.
	DeliverAt time.Time
	// TTL discards the record when it has still not been delivered this long
	// after it became due. A record that is in flight is governed by its
	// lease, not by its time to live; one that comes back from a negative
	// acknowledgement or a lease expiry after its time to live has run out
	// is dropped rather than redelivered. Zero means the record never
	// expires.
	TTL time.Duration
	// Release, when set, transfers ownership of Payload to the engine when
	// the engine admits the record: it is then called exactly once, after
	// the last subscription that was admitted has settled the record or the
	// record is dropped.
	//
	// Admission is what the outcome of Publish reports, so the caller can
	// always tell which side owns the payload. A nil error and
	// errcode.ErrPartial — the partial fan-out of the Independent policy —
	// both mean the engine took the record and will release it; every other
	// error means nothing was admitted and ownership never left the caller,
	// who may release the payload or publish it again. A topic with no
	// subscription discards the publication and releases it, which is an
	// admission of nothing to nobody and reports no error.
	//
	// the message contract asks PublishOwned to release even when the
	// publication is rejected; the adapter above this engine completes that
	// contract by releasing what the engine handed back, and cannot do so
	// unless the engine says which publications those are
	// (adr/ADR-017-assumptions.md).
	//
	// It never runs under an engine lock and never inside the
	// admission gate a shutdown waits for, so on
	// every route a caller drives — a publication, a subscription, a fetch,
	// a settlement — it may take as long as it likes and may call back into
	// the engine, Close included, without blocking anyone but its own
	// goroutine.
	//
	// The engine's own TTL sweep is the exception, and it is the engine's
	// goroutine rather than a gate: a record that outlives its time to live
	// with no consumer fetching is given up by the scheduler, and
	// Engine.Close waits for that goroutine to stop, so a callback that
	// never returns there is one Close waits for. The wait is what keeps the
	// sweep's release ordered before Close returns; it is recorded in
	// the maintenance notes rather than traded away here.
	Release func(P)
	// Stop, when set, ends a wait for room early: a publication parked
	// under OverflowBlock returns errcode.ErrClosed once Stop is closed,
	// having admitted nothing, so the payload is still the caller's. It is
	// the close of the handle that publishes, which the engine's own close
	// does not cover: a driver closes one publisher while the engine stays
	// open, and a publisher's Close that could not reach a parked
	// publication would wait for as long as nobody drained the lane. A nil
	// Stop never fires.
	Stop <-chan struct{}
	// Mode is the ownership contract of the publication. Zero
	// means the engine's Config.CopyMode. CopySafe copies the payload and
	// hands the caller's own back at once; CopyOwned transfers it and
	// releases it once; CopyBorrowed leaves it with the caller, which must
	// keep it valid until Release is called.
	Mode CopyMode
	// Priority selects the ring the record waits in on a topic with
	// priorities enabled, from 0 (lowest) to PriorityLevels-1 (highest); a
	// larger value is clamped. It is ignored on a topic without them.
	Priority uint8
	// DedupKey identifies the publication within the topic's DedupWindow. A
	// second publication carrying the same key inside the window is
	// discarded, released and counted rather than admitted. An empty key
	// means the publication is never deduplicated.
	DedupKey []byte
	// Subject is the routing subject a topic with Wildcards matches against
	// each subscription's Filter. It is ignored on a topic without them.
	Subject string
}

// mode returns the ownership contract the publication is admitted under.
func (t *Topic[P]) mode(p *Publish[P]) CopyMode {
	if p.Mode == CopyDefault {
		return t.eng.cfg.CopyMode
	}
	return p.Mode
}

// targets narrows a publication to the subscriptions whose filter matches its
// subject. A topic without wildcards routes to every subscription and the
// snapshot is returned untouched, so the publish fast path allocates nothing
// for a feature it does not use (adr/ADR-017-assumptions.md).
//
// A topic *with* wildcards allocates nothing either when every subscription
// takes the publication — a subscription with no filter takes everything, and
// a hierarchy is usually published to subscribers of it — so the count is
// taken first and the snapshot returned untouched when it is complete. Only a
// publication some subscription does not take pays for a slice of its own,
// which is what the lock-free publish path is allowed to cost.
func (t *Topic[P]) targets(subs []*Subscription[P], subj string) []*Subscription[P] {
	if !t.cfg.Wildcards {
		return subs
	}
	n := 0
	for _, s := range subs {
		if s.deliverable(subj) {
			n++
		}
	}
	if n == len(subs) {
		return subs
	}
	out := make([]*Subscription[P], 0, n)
	for _, s := range subs {
		if s.deliverable(subj) {
			out = append(out, s)
		}
	}
	return out
}

// Deduplicated returns the number of publications the topic's dedup window
// discarded.
func (t *Topic[P]) Deduplicated() int64 { return t.deduped.Load() }

// partition picks the lane index for p.
func (t *Topic[P]) partition(p *Publish[P]) int {
	n := t.cfg.Partitions
	if n <= 1 {
		return 0
	}
	if p.Partition >= 0 {
		return p.Partition % n
	}
	if len(p.Key) > 0 {
		return hash.Lane(t.eng.hashfn(0, p.Key), n)
	}
	return int(t.rr.Add(1)-1) % n
}

// due returns the delivery time of p as a scheduler time, or 0 when it is
// due immediately.
func due[P any](p *Publish[P], now int64) int64 {
	switch {
	case !p.DeliverAt.IsZero():
		at := p.DeliverAt.UnixNano()
		if at <= now {
			return 0
		}
		return at
	case p.Delay > 0:
		if int64(p.Delay) > math.MaxInt64-now {
			// A delay past the end of the scheduler's time would wrap to
			// the past and deliver at once; it saturates to never.
			return math.MaxInt64
		}
		return now + int64(p.Delay)
	default:
		return 0
	}
}

// Publish admits one record to the subscriptions of the topic under the
// topic's fan-out policy (the memory engine). Under the default
// Atomic policy capacity is reserved in every target lane in a stable order
// and the record is enqueued only when every reservation succeeded, so a
// subscription at its limit rejects the publication for all of them; the
// other policies admit the record where it fits and account for the rest.
// A topic without subscriptions discards the record and releases it.
//
// It returns nil when every target took the record, errcode.ErrPartial when
// the Independent policy admitted it to some of them and refused the rest,
// and errcode.ErrQueueFull when no subscription took it: ErrNeverFits, which
// carries the same code, when no settlement could ever make room for it.
// Publish.Release documents which of those hand the payload to the engine.
func (t *Topic[P]) Publish(ctx context.Context, p Publish[P]) error {
	// The publication fault of the memory engine refuses the caller's
	// publications, not the ones the engine makes for itself: a dead-letter
	// publication has its own fault (Faults.FailDeadLetter), and letting
	// this one spend that budget would make the two indistinguishable.
	if err := t.eng.faults.publish(); err != nil {
		return err
	}
	return t.publish(ctx, p, t.cfg.Overflow)
}

// PublishNoWait is Publish for a caller that must not wait for room: where
// the topic's overflow policy would park the publisher, the publication is
// refused with errcode.ErrQueueFull instead, as the engine's own dead-letter
// publication is (publishInternal). The memory driver's explicit dead-letter
// action is that caller. It routes the copy the attempt-limit route would,
// so the two give one answer to a dead-letter topic with no room, and a
// rejecting handler is not held for as long as nobody drains that topic.
//
// It takes the dead-letter fault (Faults.FailDeadLetter) for the same reason,
// and not the publication fault Publish takes: an explicit Reject spent
// RejectPublish's budget, so a test that armed one refusal for its own next
// publication saw a Reject fail instead and its publication admitted.
func (t *Topic[P]) PublishNoWait(ctx context.Context, p Publish[P]) error {
	if err := t.eng.faults.dlqPublish(); err != nil {
		return err
	}
	return t.publishInternal(ctx, p)
}

// publishInternal is the publication the engine makes for itself — today the
// dead-letter publication of lane.deadLetter — and it never parks. A
// publisher that blocks is a caller waiting for room, but an internal one is
// the engine's own scheduler goroutine or a Consumer.Nack that takes no
// context: neither has anyone to wait for it, and the memory engine requires
// the shared scheduler's callbacks to do bounded work. The topic's own
// overflow policy therefore reads as OverflowReject wherever it would park,
// which the caller treats as the dead-letter failure it is
// (adr/ADR-017-assumptions.md).
func (t *Topic[P]) publishInternal(ctx context.Context, p Publish[P]) error {
	ov := t.cfg.Overflow
	if ov.Blocks() {
		ov = OverflowReject
	}
	return t.publish(ctx, p, ov)
}

// publish is the body of both, under the overflow policy the caller is
// entitled to. Only the Block arm differs between the two: a policy that
// discards or refuses does the same thing whoever published.
func (t *Topic[P]) publish(ctx context.Context, p Publish[P], ov OverflowPolicy) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.eng.closing() {
		return errcode.ErrClosed
	}
	if t.cfg.MaxMessageSize > 0 && p.Size > t.cfg.MaxMessageSize {
		return errcode.ErrMessageTooLarge
	}
	mode := t.mode(&p)
	if !mode.Valid() || mode == CopyDefault ||
		(t.copiesFor(mode) && t.eng.cfg.Clone == nil) {
		// A copying publication is defined by the copy it makes; without a
		// Clone it would silently be a shared, engine-owned record and the
		// caller would keep a payload the engine is still reading.
		return ErrInvalidConfig
	}
	if t.cfg.Wildcards && p.Subject != "" {
		if err := subject.ValidateSubject(p.Subject); err != nil {
			return err
		}
	}
	if t.dedup != nil {
		dup, err := t.dedup.admit(p.DedupKey, t.eng.now())
		if err != nil {
			return err
		}
		if dup {
			// The topic has seen this key inside its window: the
			// publication is discarded, the payload handed back and the
			// duplicate counted, which is the "reported as accepted,
			// delivered once" reading of the message contract.
			t.deduped.Add(1)
			if p.Release != nil {
				p.Release(p.Payload)
			}
			return nil
		}
	}
	// The copy the retained log will keep is cloned here, before the
	// boundary below is taken: Config.Clone is the caller's code, and no
	// code of the caller's runs inside the boundary a joining subscription
	// waits on (the message contract). A publication the engine then refuses
	// has made a copy nobody reads, which publication.finish scrubs — one
	// clone on the refusal path is the price of the caller never being able
	// to deadlock the join.
	var logCopy P
	copied := false
	if t.retains() && t.copiesFor(mode) {
		logCopy, copied = t.eng.cfg.Clone(p.Payload), true
	}
	// The publication holds the topic's join boundary from here until it has
	// appended to the retained log, so that a subscription joining under it
	// either takes part in it or replays it, and never neither
	// (joinBoundary).
	pb := t.begin(mode, logCopy, copied)
	defer pb.finish()
	subs := pb.targets(p.Subject)
	size := int64(p.Size)
	if len(subs) == 0 {
		// A stream's retained log is a property of the topic, not of its
		// subscription set, so a publication no subscription took is still
		// retained and still replayable (adr/ADR-017-assumptions.md,
		// ).
		if !t.retains() {
			if err := t.dedupSettle(&p, nil); err != nil {
				return err
			}
			t.eng.noSubs.Add(1)
			if p.Release != nil {
				p.Release(p.Payload)
			}
			return nil
		}
		// The partition is drawn only here, so a topic that retains nothing
		// still spends no round-robin turn on a publication it discards.
		//
		// The append is gated like every other placement: this is the route
		// a publication takes when its topic has no subscription at all, and
		// when the subjects of the ones it has do not match — ordinary
		// stream traffic, not a corner. Ungated, the record lands in a log
		// the settling phase has already emptied.
		if err := t.dedupSettle(&p, t.retainOnlyGated(&pb, t.partition(&p), &p, size)); err != nil {
			return err
		}
		// Counted only once the publication was taken: a refused ticket is a
		// refusal, and a refusal is not a discard.
		t.eng.noSubs.Add(1)
		return nil
	}
	part := t.partition(&p)
	if t.rings != nil {
		// The shared path takes its own subscription set, under the log's
		// mutex and together with the slot it reserves; the one loaded here
		// only answers whether the topic has any subscription at all.
		return t.dedupSettle(&p, t.publishShared(ctx, &pb, part, &p, size, ov))
	}
	return t.dedupSettle(&p, t.publishPerSub(ctx, &pb, subs, part, &p, size, ov))
}

// dedupSettle settles the publication's pending dedup key by the engine's
// answer, and returns err unchanged.
//
// The window is written before any capacity is reserved, because two
// publications of one key must not both pass the check; the key waits there
// as pending until this decides it. The rule is the ownership rule of
// adr/ADR-017-assumptions.md: nil and errcode.ErrPartial mean the engine took
// the record and the key is committed, so a later publication of it is a
// duplicate; every other error means the caller still holds the payload and
// the key is given back, so the retry the error asks for is not discarded as
// a duplicate — reported as success, its payload released and nothing
// admitted, which is a lost message.
//
// The key's window starts here, at the admission, and not where the key was
// registered: a publication under OverflowBlock may have waited longer than
// the window in between (dedupWindow.admit).
func (t *Topic[P]) dedupSettle(p *Publish[P], err error) error {
	if t.dedup == nil || len(p.DedupKey) == 0 {
		return err
	}
	if err != nil && !errors.Is(err, errcode.ErrPartial) {
		t.dedup.forget(p.DedupKey)
		return err
	}
	t.dedup.commit(p.DedupKey, t.eng.now())
	return err
}

// publishPerSub admits a publication to the per-subscription lanes of the
// three architectures that keep one, under the topic's fan-out policy
// and its overflow policy.
//
// The two compose: the fan-out policy decides which subscriptions the record
// must reach, and the overflow policy decides what a publication none of them
// had room for does — wait, fail, or be discarded and counted. A publication
// that reached at least one subscription is never an overflow: it was
// admitted, and the fan-out policy has already said what the publisher is
// told about the rest.
func (t *Topic[P]) publishPerSub(ctx context.Context, pb *publication[P], subs []*Subscription[P], part int, p *Publish[P], size int64, ov OverflowPolicy) error {
	for {
		adm, refuser := t.reserveAll(pb, subs, part, size, ov)
		if adm.admitted > 0 {
			for i, s := range subs {
				if !adm.has(i) {
					s.st.rejected.Add(1)
				}
			}
			// The placement runs under a publish ticket: the shutdown's
			// first phase bars this gate and waits for it, so a record is
			// either in a lane before the settling drain walks it or
			// refused outright — never placed behind the drain and dropped
			// afterwards by a goroutine the shutdown is no longer waiting
			// for. reap stays because the ticket says nothing about a single
			// subscription closing under the publication.
			tk := t.eng.lc.Enter(lifecycle.OpPublish)
			if !tk.OK() {
				t.eng.lc.Leave(tk)
				t.unreserveAll(subs, &adm, part, size)
				t.reject(subs)
				return errcode.ErrClosed
			}
			give := t.commit(pb, subs, &adm, part, p, size)
			t.eng.lc.Leave(tk)
			t.after(pb, give, p, subs)
			return adm.err(t.cfg.Fanout)
		}
		switch ov {
		case OverflowDropNewest:
			// The publication is discarded rather than refused: it is
			// counted on every subscription that had no room for it and the
			// publisher is told the record was taken, so an owned payload is
			// released by the engine (Publish.Release). The counting follows
			// the ticket: a publication the gate refuses is refused, and a
			// refusal is not an overflow.
			if err := t.retainOnlyGated(pb, part, p, size); err != nil {
				return err
			}
			for _, s := range subs {
				s.st.overflowed.Add(1)
			}
			return nil
		case OverflowBlock:
			// The boundary is given up while the publisher waits: a
			// subscription joining must not wait for a publication that is
			// itself waiting for room, and the snapshot the publisher holds
			// is stale the moment it parks anyway. What the reservation pass
			// displaced goes home now rather than at the end of a wait that
			// has no bound but the caller's context: no ticket is held here,
			// the pass runs before the publication takes one.
			pb.end()
			pb.dropQueued()
			if err := t.waitFor(ctx, p.Stop, subs, refuser, part, size); err != nil {
				return err
			}
			pb.resume()
			// The set is re-read because subscriptions may have come and
			// gone while the publisher was parked, and it is narrowed
			// again because the retry is an admission like any other: a
			// record admitted after a wait must reach exactly the
			// subscriptions whose filter matches its subject, which is the
			// set the first pass reserved for
			// (adr/ADR-017-assumptions.md). A subscription that has closed
			// but not yet left the set is left out too: it would refuse the
			// record again until its Close detached it, and the publisher
			// would spin on it rather than wait.
			next := openSubs(pb.targets(p.Subject))
			if len(next) == 0 {
				// Every subscription the publication was for left while the
				// publisher waited. The record has nowhere to go, which is
				// the same outcome as publishing to a topic that never had
				// one — a stream retains it all the same.
				if err := t.retainOnlyGated(pb, part, p, size); err != nil {
					return err
				}
				t.eng.noSubs.Add(1)
				return nil
			}
			subs = next
			continue
		default:
			return t.refuse(subs, part, size)
		}
	}
}

// refuse counts a refused publication on every target subscription and
// returns what its publisher is told: errcode.ErrQueueFull, which room coming
// back may cure, or ErrNeverFits when no settlement ever can (neverFits). The
// two used to be one answer, which told a caller to retry a publication that
// every retry would find refused the same way.
func (t *Topic[P]) refuse(subs []*Subscription[P], part int, size int64) error {
	t.reject(subs)
	if t.neverFits(subs, part, size) {
		return ErrNeverFits
	}
	return errcode.ErrQueueFull
}

// neverFits reports whether no settlement can ever make room for a
// publication of size bytes to subs in partition part. On a shared log, and
// under the Atomic fan-out, that is the log or any one lane being too small
// for the record, or the engine-wide limits for one record and its bytes for
// each subscription. Under the fan-outs that admit wherever a record fits it
// is every lane being too small, or the limits for a single record.
func (t *Topic[P]) neverFits(subs []*Subscription[P], part int, size int64) bool {
	if t.rings != nil || t.cfg.Fanout == Atomic {
		n := int64(len(subs))
		if !t.eng.fitsTotal(n, size*n) {
			return true
		}
		if t.rings != nil {
			return !t.rings[part].fits(size)
		}
		for _, s := range subs {
			if !s.lanes[part].fits(size) {
				return true
			}
		}
		return false
	}
	if !t.eng.fitsTotal(1, size) {
		return true
	}
	for _, s := range subs {
		if s.lanes[part].fits(size) {
			return false
		}
	}
	return true
}

// reserveAll runs the reservation pass of one publication and returns the
// reservations it holds together with the index of the first subscription
// that had no room, which is the one a blocked Atomic publisher waits on.
// Under the Atomic policy a single refusal gives every reservation back, so
// the result is all or nothing.
func (t *Topic[P]) reserveAll(pb *publication[P], subs []*Subscription[P], part int, size int64, ov OverflowPolicy) (admission, int) {
	policy := t.cfg.Fanout
	var adm admission
	adm.reset(len(subs))
	k := int64(len(subs))
	var first []bool
	switch {
	case policy == Atomic && ov == OverflowDropOldest:
		// One refusal refuses every subscription, so no lane may displace a
		// record for the publication until every lane is known to take it.
		for i, s := range subs {
			if ln := s.lanes[part]; !ln.hasRoom(size) && !ln.roomAfterDrops(size) {
				adm.refused = 1
				return adm, i
			}
		}
		// The engine-wide limit is the publication's as a whole, and the
		// lanes make room for all of it together before any of them books
		// its share (makeRoomTogether).
		if !t.fitsTogether(subs, part, size) {
			adm.refused = 1
			return adm, 0
		}
		t.makeRoomTogether(pb, subs, part, k, size)
	case policy == DropSlow && !t.eng.hasTotalRoom(k, size*k):
		// The engine-wide room for the lanes that take the record is made
		// the same way, by every subscription of the publication in turn,
		// which is what the shared log does under the same limit. Each lane
		// paying its own share let a subscription that holds nothing, the
		// one keeping up, be refused while the slow one kept its backlog.
		first = t.makeRoomForTakers(pb, subs, part, size)
	}
	refuser := 0
	// The lanes book their shares in subscription order, in one pass, unless
	// the room was made for only some of the lanes that take the record:
	// then the ones first marks book theirs in a pass of their own before
	// the rest (makeRoomForTakers).
	for pass := range 2 {
		for i, s := range subs {
			if !booksIn(first, i, pass) {
				continue
			}
			ln := s.lanes[part]
			if t.reserveTotalIn(pb, ln, size, ov) {
				if t.reserveLane(pb, ln, size, ov) {
					adm.set(i)
					continue
				}
				t.eng.releaseTotal(1, size)
			}
			if adm.refused == 0 {
				refuser = i
			}
			adm.refused++
			if policy == Atomic {
				// Nothing is admitted, so there is nothing to learn from
				// the remaining subscriptions.
				break
			}
		}
		if len(first) == 0 {
			break
		}
	}
	if policy == Atomic && adm.refused > 0 {
		t.unreserveAll(subs, &adm, part, size)
	}
	return adm, refuser
}

// booksIn reports whether lane i books its share in pass of reserveAll: every
// lane books in the first pass when first is empty, and otherwise the lanes
// first marks book in the first pass and the rest in the second.
func booksIn(first []bool, i, pass int) bool {
	if len(first) == 0 {
		return pass == 0
	}
	return first[i] == (pass == 0)
}

// unreserveAll gives back every reservation adm holds, in the lanes and in
// the engine-wide ledger, and empties it. It is the rollback of a reservation
// pass whose publication does not happen: the Atomic policy's all-or-nothing
// refusal, and a publication the shutdown stopped accepting between its own
// look at the phase and its placement.
func (t *Topic[P]) unreserveAll(subs []*Subscription[P], adm *admission, part int, size int64) {
	for i, s := range subs {
		if adm.has(i) {
			s.lanes[part].unreserve(size)
			t.eng.releaseTotal(1, size)
		}
	}
	adm.clear()
}

// reserveTotalIn books one record of size bytes against the engine-wide
// limits for a publication to ln, first making room for it in ln when the
// topic's policies allow a record to be displaced, as reserveLane does for
// the lane's own capacity. A record the lane drops gives back its share of
// the engine-wide limits as well as the lane's, so a full engine is room the
// lane can make as much as a full lane is. Taking the engine-wide share first
// and refusing when it was not there refused a DropOldest or DropSlow
// publication to a lane holding records it could discard, as soon as the
// engine was at its limit, which the shared log never did
// (Topic.publishShared).
//
// What the lane can give back here is its own share, and the rest of the
// engine is out of its reach. A lane whose records would not make room even
// if it dropped all of them displaces nothing: an engine-wide
// byte limit filled by another topic emptied the lane and refused the record
// all the same. The drops made here are lost just as surely when reserveLane
// then refuses the record, so the lane's own guard (lane.roomAfterDrops)
// comes first: a record larger than the lane, or one its leases leave no
// room for, was given engine-wide room by emptying the lane and then refused
// by it. The Atomic fan-out has every lane pass that guard before any drop
// (reserveAll), and the other fan-outs reach this with no such look.
//
// The Atomic DropOldest publication and the DropSlow one reach this with the
// engine-wide room already made, by every lane of the publication in turn
// (makeRoomTogether, makeRoomForTakers), so a lane drops here only when a
// concurrent publication took that room.
//
// DropSlow displaces under the same guards as DropOldest. It traded one
// record for one, so under a byte limit a drop of a small record did not
// make room for a larger one, and the lane refused the publication with the
// record already gone; and a lane its guard refused fell through to that one
// drop all the same.
func (t *Topic[P]) reserveTotalIn(pb *publication[P], ln *lane[P], size int64, ov OverflowPolicy) bool {
	if t.eng.reserveTotal(1, size) {
		return true
	}
	if !t.displaces(ov) || !t.dropsMakeRoom(ln, size) {
		return false
	}
	for makeRoom(pb, ln) {
		if t.eng.reserveTotal(1, size) {
			return true
		}
	}
	return false
}

// displaces reports whether a publication under ov may displace records to
// make room for itself: the DropOldest overflow policy does under every
// fan-out, and the DropSlow fan-out does under every overflow policy.
func (t *Topic[P]) displaces(ov OverflowPolicy) bool {
	return ov == OverflowDropOldest || t.cfg.Fanout == DropSlow
}

// totalRoomAfterDrops reports whether the engine-wide limits would take one
// more record of size bytes once ln had dropped everything it can give up.
func (t *Topic[P]) totalRoomAfterDrops(ln *lane[P], size int64) bool {
	n, b := ln.droppable(t.eng.maxBytes > 0)
	return t.eng.roomAfterDrops(1, size, n, b)
}

// dropsMakeRoom reports whether ln's own drops could make room for one
// record of size bytes both in the lane and under the engine-wide limits,
// which is what DropOldest asks before it displaces anything for its share
// (reserveTotalIn).
func (t *Topic[P]) dropsMakeRoom(ln *lane[P], size int64) bool {
	return ln.roomAfterDrops(size) && t.totalRoomAfterDrops(ln, size)
}

// fitsTogether reports whether the engine-wide limits take an Atomic
// publication to subs, one record and its bytes for each, now or once the
// lanes together have dropped everything they can give up.
func (t *Topic[P]) fitsTogether(subs []*Subscription[P], part int, size int64) bool {
	k := int64(len(subs))
	return t.eng.hasTotalRoom(k, size*k) || t.publicationRoomAfterDrops(subs, part, size)
}

// makeRoomTogether makes the engine-wide room an Atomic DropOldest
// publication needs before any lane books its share: every lane of the
// publication gives up its oldest record in turn, one per pass, until the
// limit would take a record and its bytes for each of them. That is the
// shared log's rule under the same limit (makeRoomShared), so both
// architectures spread the cost over the subscriptions the same way, and a
// lane that holds nothing it may drop is paid for by the others.
//
// Each lane making room for its own share alone was the rule before, and
// under Atomic a lane with nothing to drop then refused its share after the
// lanes before it had displaced records for theirs, which the refusal lost.
// fitsTogether has checked that the lanes can make the room together. It is
// a look rather than a lock: a concurrent publication may take the room
// before the lanes book it, and each lane is then back to its own share
// (reserveTotalIn).
//
// k is the number of records the room is made for: one for every
// subscription under Atomic, and one for every lane that takes the record
// under DropSlow (makeRoomForTakers).
func (t *Topic[P]) makeRoomTogether(pb *publication[P], subs []*Subscription[P], part int, k, size int64) {
	for !t.eng.hasTotalRoom(k, size*k) {
		if !dropEach(pb, subs, part) {
			return
		}
	}
}

// makeRoomForTakers makes the engine-wide room a DropSlow publication needs
// before any lane books its share, as makeRoomTogether does for an Atomic
// one: for as many of the lanes that would take the record (lane.takes) as
// the subscriptions' drops together can make room for, and for none when
// they cannot make room for one. A lane that does not take the record still
// gives up its records in turn: it is the subscription not keeping up that
// loses its backlog under DropSlow.
//
// When the room is for only some of the lanes that take the record, it
// returns which of them book their shares first (reserveAll): the ones that
// held nothing a drop could take, which are the subscriptions keeping up.
// The lanes booked in subscription order alone, so a slow subscription first
// in the set dropped a record, took the new one itself and left the one
// keeping up refused. It returns nil when the room is for every taker, or
// for none, where the order decides nothing.
func (t *Topic[P]) makeRoomForTakers(pb *publication[P], subs []*Subscription[P], part int, size int64) []bool {
	takers := int64(0)
	for _, s := range subs {
		if s.lanes[part].takes(size) {
			takers++
		}
	}
	n, b := t.droppableIn(subs, part)
	k := takers
	for k > 0 && !t.eng.roomAfterDrops(k, size*k, n, b) {
		k--
	}
	if k == 0 {
		return nil
	}
	var first []bool
	if k < takers {
		// Read before the drops below, which leave the slow lanes holding
		// nothing too.
		first = make([]bool, len(subs))
		for i, s := range subs {
			ln := s.lanes[part]
			dn, _ := ln.droppable(false)
			first[i] = dn == 0 && ln.takes(size)
		}
	}
	t.makeRoomTogether(pb, subs, part, k, size)
	return first
}

// reserveLane books one record in the lane, first making room for it when the
// topic's policies allow a record to be displaced: the DropSlow fan-out
// policy (the specification) drops the oldest records of a subscription that
// is holding the publisher up, and the DropOldest overflow policy does the
// same for every full lane whatever the fan-out policy is.
//
// The lane makes as much room as the new record needs: one drop frees one
// record and its bytes, and a byte-bounded lane may have to give up several.
// A record the lane could not hold even with everything but its leases
// dropped displaces nothing: emptying a lane for a record that will not fit
// is the one drop that buys nothing. DropSlow used to trade one record for
// one, which under a byte capacity dropped a small record for a larger one
// and refused it all the same.
func (t *Topic[P]) reserveLane(pb *publication[P], ln *lane[P], size int64, ov OverflowPolicy) bool {
	if ln.reserve(size) {
		return true
	}
	if !t.displaces(ov) || !ln.roomAfterDrops(size) {
		return false
	}
	for makeRoom(pb, ln) {
		if ln.reserve(size) {
			return true
		}
	}
	return false
}

// waitRoom parks a publisher under OverflowBlock until the reservation that
// refused it could succeed, its context ends, stop closes, or the engine
// closes. It holds no reservation while it waits — every one taken by the
// failed pass has already been given back — so publishers blocked on
// different subscriptions cannot wait on each other.
//
// The condition it re-checks is the whole reservation, the lane's capacity
// and the engine-wide limit together, and it parks on a gate for each half,
// for the reason waitLogRoom gives below: a wait that is not woken by every
// release that can satisfy it is a stall, and a wait that returns on a
// condition that does not admit the record is a busy loop. reserveAll
// records a refuser whether the refusal came from reserveTotal or from the
// lane, so a publisher refused because the engine is at its total limit, on
// a lane that has room, used to return from here at once, retry, be refused
// again and spin for as long as the engine stayed full.
// Both gates are registered before the last look, so a release between the
// two closes a channel this wait is already holding.
//
// The engine's gate is only worth holding when the engine has a limit that
// can refuse the reservation: without one, every settlement of every topic
// would wake a publisher waiting for this lane alone. A nil channel never
// fires, which is what a select wants for the arm that cannot happen.
//
// A publication that could never fit, whatever leaves the lanes, is refused
// with ErrNeverFits rather than parked for ever (neverFits).
//
// The engine-wide half is room for n records of size bytes, one for each
// subscription of an Atomic publication. The wait asked for room for one:
// with fewer free records than subscriptions it returned at once and the
// publisher spun, and when the limit could never take all n it spun for ever.
// Such a publication is refused, as waitLogRoom refuses one.
//
// It is the Atomic fan-out's wait, which needs every subscription, so the
// first one that refused is the one to wait for. The fan-outs that admit a
// record wherever it fits wait for any of them (waitAnyRoom).
func (t *Topic[P]) waitRoom(ctx context.Context, stop <-chan struct{}, subs []*Subscription[P], refuser, part int, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.eng.closing() || stopped(stop) {
		return errcode.ErrClosed
	}
	s := subs[refuser]
	if s.closed.Load() {
		// The subscription that refused the publication has closed and the
		// topic has not. A publisher already parked here is woken by that
		// close (Subscription.Close) and re-reads the subscription set, so
		// one that arrives after it does the same instead of being refused
		// with ErrClosed, which is not retryable.
		return nil
	}
	if t.neverFits(subs, part, size) {
		return ErrNeverFits
	}
	n := int64(len(subs))
	s.room.waiting.Add(1)
	defer s.room.waiting.Add(-1)
	ch := s.room.chanFor()
	var engCh <-chan struct{}
	if t.eng.limitsTotal() {
		t.eng.room.waiting.Add(1)
		defer t.eng.room.waiting.Add(-1)
		engCh = t.eng.room.chanFor()
	}
	if s.lanes[part].hasRoom(size) && t.eng.hasTotalRoom(n, size*n) {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-engCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-stop:
		return errcode.ErrClosed
	case <-t.eng.ctx.Done():
		return errcode.ErrClosed
	}
}

// waitFor parks a publisher of a per-subscription topic under OverflowBlock
// until the reservation pass that refused its publication could admit it, or
// the wait ends: the Atomic fan-out waits for the subscription that refused
// it (waitRoom), and the others for any of them (waitAnyRoom).
func (t *Topic[P]) waitFor(ctx context.Context, stop <-chan struct{}, subs []*Subscription[P], refuser, part int, size int64) error {
	if t.cfg.Fanout == Atomic {
		return t.waitRoom(ctx, stop, subs, refuser, part, size)
	}
	return t.waitAnyRoom(ctx, stop, subs, part, size)
}

// waitAnyRoom parks a publisher under OverflowBlock whose fan-out admits a
// record wherever it fits (Independent, BestEffort and DropSlow) until one of
// the subscriptions it targets could take the record, its context ends, stop
// closes, or the engine closes. Like waitRoom it holds no reservation while
// it waits.
//
// It waited on the first subscription that refused it, as the Atomic wait
// does, and for these fan-outs that is the wrong one: room in any other
// subscription admits the record and woke nobody, so a publisher behind a
// subscription that never reads stayed parked beside one that had drained.
// It parks on the topic's gate instead, which every lane of the topic
// announces room on (lane.roomBack), and on the engine's for the engine-wide
// half, and its last look is the whole of what the reservation pass would
// admit (admitsSomewhere), so a wake that admits nothing does not send the
// publisher round a busy loop.
//
// The refusal is the same reading of the whole set: a record is refused with
// ErrNeverFits only when none of the subscriptions could ever hold it. The
// first refuser decided that too, and refused a record larger than its own
// byte capacity that another subscription, full only for now, would have
// taken once it drained.
func (t *Topic[P]) waitAnyRoom(ctx context.Context, stop <-chan struct{}, subs []*Subscription[P], part int, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.eng.closing() || stopped(stop) {
		return errcode.ErrClosed
	}
	if anyClosed(subs) {
		// As in waitRoom: the caller re-reads the subscription set.
		return nil
	}
	if t.neverFits(subs, part, size) {
		return ErrNeverFits
	}
	t.room.waiting.Add(1)
	defer t.room.waiting.Add(-1)
	ch := t.room.chanFor()
	var engCh <-chan struct{}
	if t.eng.limitsTotal() {
		t.eng.room.waiting.Add(1)
		defer t.eng.room.waiting.Add(-1)
		engCh = t.eng.room.chanFor()
	}
	if t.admitsSomewhere(subs, part, size) {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-engCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-stop:
		return errcode.ErrClosed
	case <-t.eng.ctx.Done():
		return errcode.ErrClosed
	}
}

// anyClosed reports whether one of subs has closed.
func anyClosed[P any](subs []*Subscription[P]) bool {
	for _, s := range subs {
		if s.closed.Load() {
			return true
		}
	}
	return false
}

// admitsSomewhere reports whether a reservation pass of a fan-out that admits
// wherever a record fits would now admit a record of size bytes to one of
// subs at least: a lane with room and the engine-wide room for one record.
// Under DropSlow a lane takes the record when its own drops can make the room
// (lane.takes), and the engine-wide limit does when the drops of the
// publication's subscriptions together can (makeRoomForTakers), and a lease
// that ends unsettled is a wake as much as a settlement is (lane.requeue): the
// record it gives back is one a drop may take.
func (t *Topic[P]) admitsSomewhere(subs []*Subscription[P], part int, size int64) bool {
	if t.cfg.Fanout == DropSlow {
		if !t.roomAfterDropsIn(subs, part, 1, size) {
			return false
		}
		for _, s := range subs {
			if s.lanes[part].takes(size) {
				return true
			}
		}
		return false
	}
	if !t.eng.hasTotalRoom(1, size) {
		return false
	}
	for _, s := range subs {
		if s.lanes[part].hasRoom(size) {
			return true
		}
	}
	return false
}

// stopped reports whether a publication's Stop has closed. It is checked
// before a wait as well as inside it: a select picks at random among the
// cases that are ready, and a publisher whose handle has closed must not go
// round again because a gate happened to be closed too.
func stopped(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

// waitLogRoom parks a publisher under OverflowBlock until the shared log has
// room for the record, its context ends, stop closes, or the engine closes.
// It holds no reservation while it waits.
//
// It waits on the log rather than on a subscription, which the per-lane
// waitRoom cannot do for it: the log's capacity is shared, so the settlement
// that reclaims the slot may come from any subscription, and a publisher
// woken by a subscription that has room while another still holds the log's
// tail would find the reservation refused again and again — a busy loop, not
// a wait. The condition it re-checks is the whole reservation, log capacity
// and engine-wide limit together, for the same reason.
//
// It therefore parks on two gates, because that condition has two halves and
// a wait that is not woken by every release that can satisfy it is a stall:
// the log's own gate, closed by the settlement that reclaims a slot, and the
// engine's, closed by every return of an engine-wide reservation — including
// one made by another topic, and including the one the settlement of this
// very log makes after it has reclaimed the slot (lane.discard settles
// before it releases). Both gates are registered before the last look, so a
// release between the two closes a channel this wait is already holding.
//
// A reservation that could never succeed, whatever leaves the log, is refused
// with ErrNeverFits rather than parked for ever, and the reservation is the
// whole publication, one slot of the log and one engine-wide record and its
// bytes for every subscription of subs that will read it, not the single
// record the per-lane wait asks about.
//
// Under DropSlow the publisher is also waiting for a record it may displace
// (publishShared), and the room its displacement would make is part of the
// last look (displacementMakesRoom). Without it the look read the log as full
// and parked: a lease on the base settled, given back or expired between
// the displacement the publisher tried and its registration here left it
// parked behind a record any later publication displaced at once. That
// half reads the lanes' gauges, which every lane announces on the topic's
// gate (lane.roomBack), as it does a record a lease gave back unsettled
// (lane.requeue), so a DropSlow publisher parks on that gate too.
func (t *Topic[P]) waitLogRoom(ctx context.Context, stop <-chan struct{}, ring *logRing[P], subs []*Subscription[P], part int, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.eng.closing() || stopped(stop) {
		return errcode.ErrClosed
	}
	if t.neverFits(subs, part, size) {
		return ErrNeverFits
	}
	ring.room.waiting.Add(1)
	defer ring.room.waiting.Add(-1)
	logCh := ring.room.chanFor()
	// The engine's gate is only worth holding when the engine has a limit
	// that can refuse the reservation: without one, every settlement of
	// every topic would wake a publisher that is waiting for this log alone.
	// A nil channel never fires, which is what a select wants for the arm
	// that cannot happen.
	var engCh <-chan struct{}
	if t.eng.limitsTotal() {
		t.eng.room.waiting.Add(1)
		defer t.eng.room.waiting.Add(-1)
		engCh = t.eng.room.chanFor()
	}
	var topCh <-chan struct{}
	dropSlow := t.cfg.Fanout == DropSlow
	if dropSlow {
		t.room.waiting.Add(1)
		defer t.room.waiting.Add(-1)
		topCh = t.room.chanFor()
	}
	if ring.hasRoom(size) || (dropSlow && t.displacementMakesRoom(ring, subs, part, size)) {
		return nil
	}
	select {
	case <-logCh:
		return nil
	case <-engCh:
		return nil
	case <-topCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-stop:
		return errcode.ErrClosed
	case <-t.eng.ctx.Done():
		return errcode.ErrClosed
	}
}

// publishShared admits a publication to the shared partition log, which
// decides for every subscription at once: one slot is reserved, one record is
// written and every cursor sees it. The fan-out policies that let one
// subscription refuse for itself have nothing to decide here — there is a
// single reservation — so every policy but DropSlow behaves like Atomic, and
// DropSlow reclaims the oldest slot instead of refusing.
//
// The subscriptions the record is for are the ones the reservation returns,
// not the ones the caller saw: the log decides both under one hold of its
// mutex, so a Subscribe that lands in the middle of this function either took
// part in the reservation or starts reading past the slot it takes
// (logRing.reserve).
func (t *Topic[P]) publishShared(ctx context.Context, pb *publication[P], part int, p *Publish[P], size int64, ov OverflowPolicy) error {
	ring := t.rings[part]
	subs, res := ring.reserve(size)
	// A record that a displaced one would make room for: the log's own
	// capacity and the engine-wide limit are both freed by a drop.
	full := func(r reserveResult) bool { return r == reserveLogFull || r == reserveEngineFull }
	// A record the log could never hold displaces nothing (lane.fits).
	displace := t.displaces(ov) && ring.fits(size)
	// DropOldest and DropSlow keep making room until the record fits or the
	// log holds nothing that may be displaced. DropSlow traded one record for
	// one, which under a byte capacity dropped a small record for a larger
	// one and refused it all the same.
	for full(res) && displace && t.makeRoomShared(pb, subs, part, res, size) {
		subs, res = ring.reserve(size)
	}
	for full(res) && ov == OverflowBlock && len(subs) > 0 {
		// The log is full and nothing could be displaced: the publisher
		// waits for a slot to be reclaimed rather than being refused. What a
		// displacement queued goes home before the wait, as in
		// publishPerSub, rather than at the end of a wait that has no bound
		// but the caller's context.
		pb.dropQueued()
		if err := t.waitLogRoom(ctx, p.Stop, ring, subs, part, size); err != nil {
			return err
		}
		subs, res = ring.reserve(size)
		// DropSlow looks again on every wake, as the per-subscription path
		// does. A lease on the base displaces nothing (reclaimBase), and
		// once it is settled or given back the base may be held only by the
		// slow subscription, which is the one the policy drops. The
		// publisher used to go back to sleep behind it until it consumed.
		for full(res) && displace && t.makeRoomShared(pb, subs, part, res, size) {
			subs, res = ring.reserve(size)
		}
	}
	switch res {
	case reserveOK:
	case reserveNoSubs:
		// Every subscription left between the check in Publish and the
		// reservation. There is nothing to admit the record to, which is
		// the same outcome as publishing to a topic that never had one.
		if err := t.retainOnlyGated(pb, part, p, size); err != nil {
			return err
		}
		t.eng.noSubs.Add(1)
		return nil
	default:
		if ov == OverflowDropNewest {
			if err := t.retainOnlyGated(pb, part, p, size); err != nil {
				return err
			}
			for _, s := range subs {
				s.st.overflowed.Add(1)
			}
			return nil
		}
		return t.refuse(subs, part, size)
	}
	// As in publishPerSub: the placement — the slot, every lane that reads
	// it and the retained log — happens under one publish ticket, and a
	// refused ticket gives the slot back rather than appending it to a log
	// the settling phase has already drained.
	tk := t.eng.lc.Enter(lifecycle.OpPublish)
	if !tk.OK() {
		t.eng.lc.Leave(tk)
		ring.unreserve(size, len(subs))
		t.reject(subs)
		return errcode.ErrClosed
	}
	now := t.eng.now()
	at := due(p, now)
	mode := t.mode(p)
	rec := t.newRecord(p, 0, part, size, ttlAt(p, now), mode)
	rec.v = p.Payload
	if mode.Copies() {
		// A shared log cannot hold one copy per subscription, so a safe
		// publication clones once and shares the clone: the caller's payload
		// is still never retained, which is what safe copy promises.
		rec.v = t.eng.cfg.Clone(p.Payload)
	} else {
		rec.release = p.Release
	}
	refs := int32(len(subs))
	if t.retains() {
		refs++
	}
	rec.retain(refs)
	if t.retains() {
		// Unreachable today — CreateTopic refuses a stream under a shared
		// log  — but the rule is the log's either way: the
		// retained log draws the sequence when it appends, before the record
		// is anywhere a reader can reach it.
		_ = t.retain(pb, rec, at, now)
	} else {
		rec.seq = t.seq.Add(1)
	}
	pb.end()
	expire := rec.expireAt
	// Account for the record in every subscription before the log makes it
	// visible: a consumer of another goroutine may claim and settle it the
	// instant the slot is appended, and a settlement must never overtake the
	// admission it settles.
	for _, s := range subs {
		ln := s.lanes[part]
		ln.account(size)
		s.st.published.Add(1)
	}
	ring.append(rec, at, size, int32(len(subs)))
	for _, s := range subs {
		ln := s.lanes[part]
		ln.store.Publish(rec, at, now)
		// The expiry is noted from the copy taken above and only once the
		// slot is appended, for the reason lane.admit gives: a sweep that
		// scanned before the record was there must not be able to overwrite
		// the note.
		ln.noteTTL(expire)
		ln.rearm()
	}
	for _, s := range subs {
		s.wakeReady()
	}
	t.eng.lc.Leave(tk)
	t.after(pb, mode.Copies(), p, subs)
	return nil
}

// makeRoom drops the oldest record a lane holds so that a publication under
// the DropSlow policy can take its place. It reports false when the lane
// holds nothing to drop, which is the case when every record it has is in
// flight.
//
// The lane's room is freed here and now — that is what the caller is waiting
// for — but the displaced record's payload goes back to its publisher only
// once pb has given up the join boundary and the publish ticket that
// brackets its placement is home, because that release is the caller's
// callback and a reservation runs inside the boundary
// (publication.dropQueued, the message contract and).
func makeRoom[P any](pb *publication[P], ln *lane[P]) bool {
	// The lane mutex is held while the record is chosen because a
	// time-to-live sweep holds it while the store's records are out
	// (expireByDrain): a lane full of ready records then looked as if every
	// record were in flight, and the publication was refused. The drop
	// itself runs with the mutex released (lane.drop).
	ln.mu.Lock()
	e, ok := ln.store.DropOldest()
	if !ok {
		// The lane's redeliveries are the last thing given up: a record that
		// has already been delivered once is worth more than one that never
		// has, and a lane holding nothing but redeliveries is as full as one
		// holding nothing but leases.
		e, ok = ln.retries.oldest()
	}
	ln.mu.Unlock()
	if !ok {
		return false
	}
	ln.dropLater(e, pb)
	return true
}

// makeRoomShared makes room in a shared log for the reservation res refused,
// and reports whether it dropped anything.
//
// A full log has room only its base can make, since the log reclaims from
// there (reclaimBase). Under the engine-wide limit any record of the topic
// makes room, because each one returns its subscription's share of the
// limit, so there every subscription gives up its own oldest record, as a
// lane does under the same limit (Topic.reserveTotalIn) and as the lanes of
// an Atomic or DropSlow publication do in turn (makeRoomTogether). Nothing is
// dropped when even emptying every subscription would leave the limit
// refusing the reservation (publicationRoomAfterDrops). That guard used to
// be DropOldest's alone, and DropSlow dropped records for a publication the
// limit then refused all the same.
func (t *Topic[P]) makeRoomShared(pb *publication[P], subs []*Subscription[P], part int, res reserveResult, size int64) bool {
	if res == reserveLogFull {
		return t.reclaimBase(pb, subs, part, size)
	}
	if !t.publicationRoomAfterDrops(subs, part, size) {
		return false
	}
	return dropEach(pb, subs, part)
}

// dropEach has every subscription give up its oldest record in partition
// part, one each, and reports whether any of them had one to give.
func dropEach[P any](pb *publication[P], subs []*Subscription[P], part int) bool {
	freed := false
	for _, s := range subs {
		if makeRoom(pb, s.lanes[part]) {
			freed = true
		}
	}
	return freed
}

// reclaimBase gives up the oldest slot of a full shared log for every
// subscription still holding it, and reports whether it took any.
//
// The slot goes once every subscription has let it go, and a record from any
// other slot frees nothing. Dropping each subscription's own oldest record
// took the newest record of a subscription that had caught up, and with the
// base out on a lease it emptied every subscription and then refused the
// publication anyway.
//
// So nothing is dropped while a lease holds the base, which no drop reclaims
// until it is settled or given back. Nothing is dropped either when the
// slots ahead of the oldest leased one would not free enough of the log for
// the record (logRing.roomAfterDrops), or when the engine-wide limit would
// refuse the reservation once the log had room (publicationRoomAfterDrops).
// Those two guards used to be DropOldest's alone, so under a byte capacity
// DropSlow gave up the base for a record the log still could not hold.
// These are looks rather than locks: a consumer that claims the base between
// the look and the drops keeps it, and the next look refuses the publication.
func (t *Topic[P]) reclaimBase(pb *publication[P], subs []*Subscription[P], part int, size int64) bool {
	ring := t.rings[part]
	base, ok := ring.oldest()
	if !ok {
		return false
	}
	leased := oldestLeased(subs, part)
	if leased <= base || !ring.roomAfterDrops(size, leased) || !t.publicationRoomAfterDrops(subs, part, size) {
		return false
	}
	freed := false
	for _, s := range subs {
		ln := s.lanes[part]
		if e, ok := ln.takeSlot(base); ok {
			ln.dropLater(e, pb)
			freed = true
		}
	}
	return freed
}

// publicationRoomAfterDrops reports whether the engine-wide limits would take
// a publication of size bytes that books one record and its bytes for each of
// subs, a shared-log publication or an Atomic one, once every subscription
// had dropped everything it can give up (totalRoomAfterDrops).
func (t *Topic[P]) publicationRoomAfterDrops(subs []*Subscription[P], part int, size int64) bool {
	return t.roomAfterDropsIn(subs, part, int64(len(subs)), size)
}

// roomAfterDropsIn reports whether the engine-wide limits would take k
// records of size bytes once every one of subs had dropped everything it can
// give up in partition part.
func (t *Topic[P]) roomAfterDropsIn(subs []*Subscription[P], part int, k, size int64) bool {
	if !t.eng.limitsTotal() {
		// No engine-wide limit can refuse it, and a DropOldest log that is
		// full asks this on every publication.
		return true
	}
	n, b := t.droppableIn(subs, part)
	return t.eng.roomAfterDrops(k, size*k, n, b)
}

// droppableIn sums what a drop could take from the lanes of subs in
// partition part (lane.droppable), with the bytes when the engine has a byte
// limit to hold them to.
func (t *Topic[P]) droppableIn(subs []*Subscription[P], part int) (n, b int64) {
	for _, s := range subs {
		dn, db := s.lanes[part].droppable(t.eng.maxBytes > 0)
		n += dn
		b += db
	}
	return n, b
}

// oldestLeased returns the oldest shared-log slot any of subs holds on a lease
// in partition part, or math.MaxUint64 when none holds one
// (lane.oldestLeasedSlot).
func oldestLeased[P any](subs []*Subscription[P], part int) uint64 {
	leased := uint64(math.MaxUint64)
	for _, s := range subs {
		leased = min(leased, s.lanes[part].oldestLeasedSlot())
	}
	return leased
}

// displacementMakesRoom reports whether the displacement a shared log's
// publication may make (makeRoomShared) would give it room for a record of
// size bytes: the slots ahead of the oldest leased one free enough of the
// log (logRing.roomAfterDrops), and the subscriptions' drops free enough of
// the engine-wide limits (publicationRoomAfterDrops). It is what a DropSlow
// publisher parked on the log looks at besides the room the log has now
// (waitLogRoom).
//
// It reads a base slot whose record is between two states as one a drop can
// take: claimed by a fetch and not yet on the lease list, or off its lease
// for an Ack, a Nack or an expiry and not yet settled or back in its store.
// reclaimBase takes nothing then, so for that window the publisher goes
// round, wait, displace, wait, without parking. The window ends with a
// settlement or a requeue, which wakes the gate, or with a lease, which
// leaves nothing to displace, so the loop is bounded by it; under a stress
// of acks, nacks and expiries one publication went round about a thousand
// times. A look that named only what reclaimBase can take would have to
// read each store's delayed records without firing them, which no timer
// kind offers, and parking after a displacement that took nothing would
// lose the wake of a lease given back in between, which is what this look
// is for. lane.takes has the same window on a per-subscription lane, whose
// depth counts a record in transit as droppable.
func (t *Topic[P]) displacementMakesRoom(ring *logRing[P], subs []*Subscription[P], part int, size int64) bool {
	return ring.roomAfterDrops(size, oldestLeased(subs, part)) && t.publicationRoomAfterDrops(subs, part, size)
}

// reject counts one refused publication on every target subscription.
func (t *Topic[P]) reject(subs []*Subscription[P]) {
	for _, s := range subs {
		s.st.rejected.Add(1)
	}
}

// reap re-drains the subscriptions that closed under a publication. A close
// may have run between the snapshot of subs and the commit, in which case the
// record was admitted to a lane nothing will visit again: Engine.Close only
// walks the topics it can still see, and Subscription.Close has already
// detached this subscription and drained it.
func (t *Topic[P]) reap(subs []*Subscription[P]) {
	for _, s := range subs {
		if s.closed.Load() {
			s.drain()
		}
	}
}

// openSubs returns subs without the subscriptions that have closed. It copies
// only when one has, so the set a publication shares with the topic is never
// written to.
func openSubs[P any](subs []*Subscription[P]) []*Subscription[P] {
	for i, s := range subs {
		if !s.closed.Load() {
			continue
		}
		out := slices.Clone(subs[:i])
		for _, o := range subs[i+1:] {
			if !o.closed.Load() {
				out = append(out, o)
			}
		}
		return out
	}
	return subs
}

// after is everything a publication defers until it has given its ticket
// back: the retained entries a trim displaced, the caller's own payload, and
// the subscriptions that closed under it. Every one of them runs a callback
// of the caller's, and a shutdown barrier waits for the ticket — so a Release
// inside the section would make Engine.Close wait for user code, which
// the message contract forbids and internal/lifecycle's own contract states
// ("the section between Enter and Leave must not block"). The specification says the same
// thing for the engine's mutexes. The placement is what the gate is for; a
// release is not a placement.
//
// What it does not do is re-read the engine's own close. That re-read is the
// second line the contributor notes describes, and the ticket is what replaces it
// here: while the ticket is held the settling drain has not started, so the
// drain walks everything this publication *placed*, and a re-read could only
// find what it had already walked.
//
// A record this publication took out of a structure is the other half, and
// there the ticket buys less. The trim removed its entries from the retained
// log while the ticket was held, so the drain cannot see them, and their
// release lands here — which can be after Close has returned. That is not an
// oversight to be fixed by dropping them inside the ticket: a release is the
// caller's own code, and a barrier that waited for it is the deadlock
// The specification exists to remove. The two properties are not both available,
// and this is the side the engine takes: nothing is released twice, nothing
// is left unreleased, and a late release is always the caller's own Publish
// finishing its work rather than a goroutine of the engine's acting behind
// the drain (Engine.Close, TestAcceptedPublicationIsPlacedBeforeCloseReturns).
//
// A subscription that closed under the publication is a different matter and
// reap still handles it: that drain is not the engine's, and it detaches the
// subscription from the set the engine's drain walks.
func (t *Topic[P]) after(pb *publication[P], give bool, p *Publish[P], subs []*Subscription[P]) {
	pb.dropQueued()
	if give && p.Release != nil {
		p.Release(p.Payload)
	}
	t.reap(subs)
}

// ttlAt returns the absolute time a publication's time to live runs out, or
// 0 when it has none. It is measured from the record's delivery time, so a
// delayed publication with a time to live is not born expired.
func ttlAt[P any](p *Publish[P], now int64) int64 {
	if p.TTL <= 0 {
		return 0
	}
	from := now
	if at := due(p, now); at > from {
		from = at
	}
	return from + int64(p.TTL)
}

// commit builds the record (or, in a copying configuration, one clone per
// subscription) and admits one envelope to every subscription that holds a
// reservation. Every reservation is already held, so nothing here can fail.
//
// It runs inside the publish ticket, so it releases nothing: the entries a
// retention trim displaced and the records a reservation had to displace are
// queued on the publication, and it reports whether the caller's own payload
// is to be handed back — all three for Topic.after to do once the ticket is
// home.
func (t *Topic[P]) commit(pb *publication[P], subs []*Subscription[P], adm *admission, part int, p *Publish[P], size int64) bool {
	now := t.eng.now()
	at := due(p, now)
	expire := ttlAt(p, now)
	mode := t.mode(p)
	retain := t.retains()
	// A stream draws the publication sequence when it appends to its retained
	// log, so that the log is ordered by the sequence its readers index it by
	// (Topic.retain); every other topology draws it here.
	var seq uint64
	if !retain {
		seq = t.seq.Add(1)
	}
	if t.copiesFor(mode) {
		// Safe or full-copy publication: every subscription observes an
		// independent copy and the caller's own payload is handed straight
		// back, which is what lets a safe publisher keep its buffer.
		if retain {
			// The retained log keeps a copy of its own: it outlives every
			// subscription's, and a replay of it must not hand one
			// subscription the copy another is holding. It goes first
			// because it is the copy that draws the sequence the others
			// carry.
			rec := t.newRecord(p, 0, part, size, expire, mode)
			rec.v = pb.takeCopy()
			rec.retain(1)
			seq = t.retain(pb, rec, at, now)
		}
		// The boundary is given up as soon as the record is in the log:
		// everything below hands payloads back — the caller's own, and any a
		// closing subscription gives up — and a callback of the caller's
		// must not be what a joining subscription waits for.
		pb.end()
		for i, s := range subs {
			if !adm.has(i) {
				continue
			}
			rec := t.newRecord(p, seq, part, size, expire, mode)
			rec.v = t.eng.cfg.Clone(p.Payload)
			rec.retain(1)
			s.lanes[part].admit(rec, at, now)
			s.st.published.Add(1)
		}
	} else {
		// Owned or borrowed publication: one reference-counted record is
		// shared by every subscription, so fan-out copies no payload. The
		// retained log holds a reference of its own, which is why a stream
		// hands an owned payload back only once it has been trimmed.
		rec := t.newRecord(p, seq, part, size, expire, mode)
		rec.v = p.Payload
		rec.release = p.Release
		n := int32(adm.admitted)
		if retain {
			n++
		}
		rec.retain(n)
		if retain {
			// The append draws the sequence, so it runs before the record
			// reaches a lane: a consumer of another goroutine may claim it
			// the instant it is admitted, and a claim reports the sequence.
			_ = t.retain(pb, rec, at, now)
		}
		pb.end()
		for i, s := range subs {
			if !adm.has(i) {
				continue
			}
			s.lanes[part].admit(rec, at, now)
			s.st.published.Add(1)
		}
	}
	for i, s := range subs {
		if adm.has(i) {
			s.wakeReady()
		}
	}
	// A copying publication keeps nothing of the caller's, so its payload
	// goes home; an owned or borrowed one is the record's until the last
	// reference goes.
	return t.copiesFor(mode)
}

// newRecord takes a pooled record and fills in everything a publication
// decides about it but the payload itself.
func (t *Topic[P]) newRecord(p *Publish[P], seq uint64, part int, size, expire int64, mode CopyMode) *record[P] {
	rec := t.eng.records.Get()
	rec.key, rec.seq, rec.size, rec.part = p.Key, seq, size, int32(part)
	// The subject is kept only where it means something: a topic without
	// wildcards routes to every subscription and has nothing to filter or to
	// resolve, so the field stays empty and costs the fast path nothing.
	if t.cfg.Wildcards {
		rec.subj = p.Subject
	}
	rec.expireAt = expire
	rec.mode = mode
	rec.prio = t.priorityOf(p)
	return rec
}

// copiesFor reports whether a publication in the given ownership mode makes
// one payload copy per subscription rather than sharing one
// reference-counted record. A safe publication does by definition, and the
// full-copy architecture (the specification) does whatever the publication asked for.
func (t *Topic[P]) copiesFor(m CopyMode) bool {
	return m.Copies() || t.eng.kinds.Arch.Copies()
}

// PublishBatch publishes each element of ps in order and returns the number
// of elements the engine took. It stops at the first element the engine
// refused and returns its error, so the caller can retry from the returned
// index; every element before it is the engine's.
//
// A partial admission is not a refusal. Under the Independent policy Publish
// reports errcode.ErrPartial for an element it admitted to some of the
// topic's subscriptions and refused for the rest: the engine took that
// payload and released it (Publish.Release, adr/ADR-017-assumptions.md
// The specification), so stopping there and reporting its index would tell the
// caller to retry a record that is already delivered — and, for an owned
// publication, that it still owns a payload the engine has freed. The batch
// counts such an element as taken and carries on, returning
// errcode.ErrPartial with the whole length once every element has been
// offered, unless a later element is refused outright and stops it.
//
// A refusal that stops the batch takes the place of that ErrPartial, so the
// batch's own answer cannot say which of the elements before it were partial.
// When partial is not nil it is at least as long as ps, and partial[i] is set
// for every element i the engine took under a partial fan-out, whatever the
// batch then answers. A caller that answers per element needs it: without it
// the memory driver reported a partially admitted message of a batch with a
// refusal in it as admitted everywhere.
func (t *Topic[P]) PublishBatch(ctx context.Context, ps []Publish[P], partial []bool) (int, error) {
	anyPartial := false
	for i := range ps {
		switch err := t.Publish(ctx, ps[i]); {
		case err == nil:
		case isPartial(err):
			anyPartial = true
			if partial != nil {
				partial[i] = true
			}
		default:
			return i, err
		}
	}
	if anyPartial {
		return len(ps), errcode.ErrPartial
	}
	return len(ps), nil
}

// isPartial reports whether err is the partial fan-out of the Independent
// policy, which is an admission rather than a refusal.
func isPartial(err error) bool { return errors.Is(err, errcode.ErrPartial) }

// removeSub detaches s from the topic's copy-on-write subscription slice.
func (t *Topic[P]) removeSub(s *Subscription[P]) {
	t.mu.Lock()
	defer t.mu.Unlock()
	old := t.subs.Load().subs
	next := make([]*Subscription[P], 0, len(old))
	for _, o := range old {
		if o != s {
			next = append(next, o)
		}
	}
	// A subscription that leaves needs no cursor. It may still be named by a
	// publication that reserved its slot before this, whose record its own
	// drain releases (Topic.reap).
	t.storeSubs(next, nil)
}
