package memory

import (
	"runtime"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/engine"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Defaults of the memory engine, fixed by ADR-022 (rows and).
const (
	// DefaultCapacityMessages is the per-subscription-partition record
	// capacity a topology takes when it names none. It is bounded on
	// purpose: an unbounded default would turn a consumer that stops
	// consuming into a memory leak instead of a backpressure signal, and
	// would leave the overflow policies untested by the double that
	// exists to test them (ADR-022).
	DefaultCapacityMessages = engine.DefaultCapacityMessages
	// DefaultPartitions is one lane per subscription, which is what makes
	// the ordering guarantee hold without the caller asking for it
	// (ADR-022).
	DefaultPartitions = engine.DefaultPartitions
	// DefaultAckDeadline is the lease duration of a delivery.
	DefaultAckDeadline = engine.DefaultAckDeadline
)

// Config configures a memory broker (the memory engine).
//
// The zero value is usable and is the bounded, ordered, strict configuration:
// one partition per subscription, 65,536 records per lane, a 30-second lease,
// the real clock, and every variant family at its recorded bake-off winner.
// AutoCreate is the one thing a caller almost always sets.
type Config struct {
	// AutoCreate creates a topic the first time a publisher or a consumer
	// names it. It is false by default, which is what every network driver
	// does — Open never creates production topology — and true in
	// NewForTest (ADR-016).
	AutoCreate bool
	// DefaultTopology is the topology every topic is created with. Its zero
	// fields take the package defaults above.
	DefaultTopology TopologyConfig
	// MaxTotalMessages bounds the records admitted across the whole broker,
	// counted once per subscription copy. Zero means no limit. A publication
	// that needs more than the limit can ever hold, one record for each
	// subscription under FanoutAtomic or on a shared partition log, is
	// refused with ErrQueueFull under OverflowBlock rather than parked, and
	// mq.IsRetryable reports the refusal as not retryable, because every
	// retry is refused the same way. Its code is still ResourceExhausted.
	MaxTotalMessages int
	// MaxTotalBytes bounds the accounted payload bytes the same way, with
	// the same refusal. Zero means no limit.
	MaxTotalBytes int64
	// SchedulerResolution is the granularity the delayed-delivery scheduler
	// is asked for. Zero means the resolution of the selected timer kind;
	// a value coarser than the kind's own is not honoured, and one finer
	// than it cannot be.
	SchedulerResolution time.Duration
	// Clock is the time source the broker and its engine read. Nil means the
	// wall clock, which follows the bubble clock inside testing/synctest.
	//
	// A *TestClock is taken as the engine's own clock, which is what makes
	// Broker.Advance move time deterministically; any other implementation
	// is bridged, and Broker.Advance then reports ErrUnsupported because the
	// broker cannot move a clock it does not own.
	Clock mq.Clock
	// ZeroOnRelease overwrites a payload's bytes as the engine releases the
	// record, so that sensitive data does not survive in a pooled one
	// It does not apply to a borrowed payload, whose storage
	// belongs to the caller.
	ZeroOnRelease bool
	// DebugOwnership checks the release contract of owned publications: a
	// second release of one record's payload panics rather than corrupting
	// the caller's free list. It is on unconditionally in iqmq_debug builds.
	DebugOwnership bool
	// Engine selects the variant kind of every family the engine is
	// assembled from. The zero value is every family's Auto, which resolves
	// to the winner ADR-014 records.
	Engine EngineConfig
	// Hooks are the client-wide observation points every handle the client
	// opens is built with. They are Config's because Open returns a
	// client rather than taking options for one.
	Hooks mq.Hooks
}

// TopologyConfig configures one topic (the memory engine).
//
// Every zero field takes a documented default, and the two capacity fields
// are deliberately asymmetric: zero means "I did not say" and a negative
// value means "I want no limit", so the two cannot be confused (ADR-022 row
// The specification, and the rule applied to the public surface).
type TopologyConfig struct {
	// Kind is the topology profile. Zero means TopologyTopic.
	Kind TopologyKind
	// CapacityMessages bounds the ready, in-flight and delayed records of
	// one subscription-partition. Zero means DefaultCapacityMessages and a
	// negative value means unbounded, which selects the unbounded chunked
	// ready queue (ADR-022).
	CapacityMessages int
	// CapacityBytes bounds the same set by accounted payload size. A
	// non-positive value means no byte limit.
	CapacityBytes int64
	// MaxInFlight would bound the leased records of one lane on its own.
	// The engine bounds ready, in-flight and delayed records together
	// through CapacityMessages, so a non-zero value here is refused rather
	// than ignored.
	MaxInFlight int
	// MaxDelayed would bound the records waiting for a due time on their
	// own, and is refused for the same reason as MaxInFlight.
	MaxDelayed int
	// Partitions is the number of lanes per subscription. Zero means
	// DefaultPartitions — one lane, and therefore strict order — and a
	// negative value means GOMAXPROCS lanes, which is the throughput
	// opt-out of the ordering guarantee (ADR-022). More than 1024 lanes is
	// refused.
	Partitions int
	// Ordering is the delivery-order guarantee of a lane. Zero means
	// OrderingRelaxed; under OrderingStrict a redelivery blocks its lane
	// until it is settled or dead-lettered.
	Ordering OrderingMode
	// Overflow decides what a publication the lane has no room for does.
	// Zero means OverflowBlock, which is the default: a publisher waits
	// for room rather than losing a record or being refused.
	Overflow OverflowPolicy
	// Fanout is the admission policy a publication follows when one of its
	// target subscriptions has no room. Zero means FanoutAuto, which
	// resolves to ADR-014's recorded default.
	Fanout FanoutPolicy
	// AckDeadline is the lease duration of a delivery. Zero means
	// DefaultAckDeadline and a negative value means leases never expire.
	AckDeadline time.Duration
	// MaxAttempts is the number of deliveries a record may have before a
	// failure routes it to DeadLetterTopic. Zero means no limit.
	MaxAttempts uint32
	// Retention bounds the retained log of a stream topic. It is
	// meaningful only under TopologyStream; naming it on any other profile
	// is a configuration error, because a topic that advertises no replay
	// must not quietly hold records for one.
	Retention Retention
	// Priority gives each lane one ready ring per priority level and serves
	// the most urgent first. A publication names its level with
	// [github.com/iqhive/iqmq.Priority]. It is refused beside
	// OrderingStrict, because serving the most urgent first reorders the
	// lane that ordering promises to keep.
	Priority bool
	// Wildcards routes each publication by its subject to the subscriptions
	// whose filter matches it. The SPI has no per-source filter field, so a
	// wildcard subscription cannot be opened through it yet
	// ; a topic may still be created with wildcards, and
	// a delivery from one reports the resolved subject as its topic.
	Wildcards bool
	// DedupWindow discards a publication carrying a dedup key the topic has
	// already seen within the window. The window runs from the admission of
	// the publication that carried the key, however long it waited for room
	// first. Zero means no deduplication.
	DedupWindow time.Duration
	// DeadLetterTopic names the topic a record is published to once it has
	// used up its attempts, and the destination an explicit Reject routes
	// to. The topic must already exist: the engine never creates one behind
	// the operator's back.
	DeadLetterTopic string
	// MaxMessageSize rejects a larger publication with ErrMessageTooLarge. A
	// non-positive value means no limit.
	MaxMessageSize int
}

// Retention bounds the retained log of a stream topic (the memory engine).
//
// The three bounds are independent and all of them are enforced: the log is
// trimmed until it holds at most MaxMessages records, at most MaxBytes
// accounted bytes and no record older than MaxAge.
type Retention struct {
	// MaxMessages is the number of records one partition retains. A
	// non-positive value means the bound is not applied.
	MaxMessages int
	// MaxBytes is the accounted payload size one partition retains. A
	// non-positive value means the bound is not applied.
	MaxBytes int64
	// MaxAge is how long a record is retained after its publication. A
	// non-positive value means the bound is not applied.
	MaxAge time.Duration
}

// Enabled reports whether any bound is set.
func (r Retention) Enabled() bool { return r.MaxMessages > 0 || r.MaxBytes > 0 || r.MaxAge > 0 }

// engineRetention is the internal form.
func (r Retention) engineRetention() engine.Retention {
	return engine.Retention{MaxMessages: r.MaxMessages, MaxBytes: r.MaxBytes, MaxAge: r.MaxAge}
}

// resolve validates cfg and returns the engine topology it names.
//
// Every default is applied here, so nothing below this point has to ask what
// a zero field means.
func (t TopologyConfig) resolve() (engine.TopologyConfig, error) {
	switch {
	case !t.Kind.Valid():
		return engine.TopologyConfig{}, errTopologyKind
	case !t.Ordering.Valid():
		return engine.TopologyConfig{}, errTopologyOrdering
	case !t.Overflow.Valid():
		return engine.TopologyConfig{}, errTopologyOverflow
	case !t.Fanout.Valid():
		return engine.TopologyConfig{}, errTopologyFanout
	case t.MaxInFlight != 0:
		return engine.TopologyConfig{}, errTopologyMaxInFlight
	case t.MaxDelayed != 0:
		return engine.TopologyConfig{}, errTopologyMaxDelayed
	case t.Retention.Enabled() && t.Kind != TopologyStream:
		// A topic that advertises no replay must not quietly hold records
		// for one. The engine refuses it too; refusing it here is
		// what makes a DSN that names it fail where it was written.
		return engine.TopologyConfig{}, errTopologyRetention
	}
	out := engine.TopologyConfig{
		Kind:             t.Kind.engineKind(),
		Fanout:           t.Fanout.enginePolicy(),
		Overflow:         t.Overflow.enginePolicy(),
		Ordering:         t.Ordering.engineMode(),
		Partitions:       resolvePartitions(t.Partitions),
		CapacityMessages: t.CapacityMessages,
		CapacityBytes:    t.CapacityBytes,
		AckDeadline:      t.AckDeadline,
		MaxMessageSize:   t.MaxMessageSize,
		MaxAttempts:      t.MaxAttempts,
		DeadLetterTopic:  t.DeadLetterTopic,
		Retention:        t.Retention.engineRetention(),
		Priority:         t.Priority,
		Wildcards:        t.Wildcards,
		DedupWindow:      t.DedupWindow,
	}
	// The engine refuses both of these too, but later than a caller can act
	// on: too many lanes when the broker is built, and priority beside strict
	// ordering only when the first topic is created, so a DSN naming either
	// passed ParseDSN and failed at Open or at the first publisher. Refusing
	// them here is what makes them fail where they were written.
	switch {
	case out.Partitions > maxPartitions:
		return engine.TopologyConfig{}, errTopologyPartitions
	case out.Priority && out.Ordering.Strict():
		return engine.TopologyConfig{}, errTopologyPriority
	}
	return out, nil
}

// maxPartitions is the engine's bound on the lanes of one subscription,
// repeated here because the engine keeps it unexported. A bound lower than
// the engine's would refuse a topology the engine accepts, and
// TestTopologyIsCheckedWhereItIsWritten holds the two at the same number
// from this side.
const maxPartitions = 1024

// resolvePartitions applies the default and its opt-out.
//
// Zero is the default rather than the opt-out: the memory engine's comment
// reads "default 1 (strict order); 0 = auto (GOMAXPROCS)", which would make
// the zero value of the struct withdraw the ordering guarantee ADR-022 says
// the default gives "without the caller asking". The asymmetry of the
// capacity fields is the resolution — a negative value is the opt-out — and it
// is recorded.
func resolvePartitions(n int) int {
	switch {
	case n < 0:
		return runtime.GOMAXPROCS(0)
	case n == 0:
		return DefaultPartitions
	default:
		return n
	}
}

// TopologyKind is the topology profile of a topic (the memory engine).
type TopologyKind uint8

// The topology profiles. The zero value is TopologyDefault, which reads as
// TopologyTopic.
const (
	// TopologyDefault leaves the profile to the package default, which is
	// TopologyTopic.
	TopologyDefault TopologyKind = iota
	// TopologyQueue is competing consumers over one shared subscription: a
	// record leaves the broker when it is acknowledged, and a second,
	// differently named subscription is a configuration error rather than a
	// silent fan-out.
	TopologyQueue
	// TopologyTopic is fan-out to independent subscriptions, each with its
	// own envelopes, settlement state and capacity. A subscription created
	// after a publication does not receive it.
	TopologyTopic
	// TopologyStream is the retained, replayable log: fan-out like a topic,
	// plus a bounded per-partition log a subscription can start reading
	// anywhere in.
	TopologyStream

	topologyKindCount
)

var topologyKindNames = [topologyKindCount]string{"default", "queue", "topic", "stream"}

// String returns the profile's lower-case name.
func (k TopologyKind) String() string {
	if k < topologyKindCount {
		return topologyKindNames[k]
	}
	return "TopologyKind(?)"
}

// Valid reports whether k names a profile or the default.
func (k TopologyKind) Valid() bool { return k < topologyKindCount }

// engineKind is the internal form.
func (k TopologyKind) engineKind() engine.TopologyKind {
	switch k {
	case TopologyQueue:
		return engine.TopologyQueue
	case TopologyTopic:
		return engine.TopologyTopic
	case TopologyStream:
		return engine.TopologyStream
	case TopologyDefault, topologyKindCount:
		return engine.TopologyDefault
	default:
		return engine.TopologyDefault
	}
}

// OrderingMode is the delivery-order guarantee of a subscription's lane
// (the message contract).
type OrderingMode uint8

// The ordering modes. The zero value reads as OrderingRelaxed.
const (
	// OrderingDefault leaves the mode to the package default, which is
	// OrderingRelaxed.
	OrderingDefault OrderingMode = iota
	// OrderingRelaxed lets the records behind a failed one carry on.
	OrderingRelaxed
	// OrderingStrict makes a redelivery block its lane until it has been
	// settled or dead-lettered.
	OrderingStrict

	orderingModeCount
)

var orderingModeNames = [orderingModeCount]string{"default", "relaxed", "strict"}

// String returns the mode's lower-case name.
func (m OrderingMode) String() string {
	if m < orderingModeCount {
		return orderingModeNames[m]
	}
	return "OrderingMode(?)"
}

// Valid reports whether m names a mode or the default.
func (m OrderingMode) Valid() bool { return m < orderingModeCount }

// engineMode is the internal form.
func (m OrderingMode) engineMode() engine.OrderingMode {
	switch m {
	case OrderingRelaxed:
		return engine.OrderingRelaxed
	case OrderingStrict:
		return engine.OrderingStrict
	case OrderingDefault, orderingModeCount:
		return engine.OrderingDefault
	default:
		return engine.OrderingDefault
	}
}

// OverflowPolicy decides what a publication does when the lane it targets has
// no room for it (the memory engine).
type OverflowPolicy uint8

// The overflow policies. The zero value is OverflowDefault, which this
// package reads as OverflowBlock — the default, applied above an engine
// whose own default is a refusal.
const (
	// OverflowDefault leaves the policy to the package default, which is
	// OverflowBlock.
	OverflowDefault OverflowPolicy = iota
	// OverflowBlock parks the publisher until the record fits, its context
	// ends or the publisher closes. It never loses a record and refuses one
	// only when it could never fit, at the price of making Publish blocking.
	// That refusal is ErrQueueFull, and mq.IsRetryable reports it as not
	// retryable; a caller that decides by the code alone still reads it as
	// a full queue that clears (docs/drivers/memory.md). Under the fan-outs
	// that admit a record wherever it fits, the publisher waits until any
	// one of the subscriptions could take the record, and the record could
	// never fit only when none of them could ever hold it.
	OverflowBlock
	// OverflowReject refuses the publication with ErrQueueFull and leaves
	// an owned payload with its publisher.
	OverflowReject
	// OverflowDropNewest discards the publication the lane had no room for,
	// counts it, and tells the publisher the record was taken.
	OverflowDropNewest
	// OverflowDropOldest makes room by discarding the record the lane would
	// have served next, and refuses only when it holds nothing to discard
	// that would make room. When a shared partition log is full, the
	// record discarded is the log's oldest, and a lease on it leaves
	// nothing to discard. Under FanoutAtomic with an engine-wide limit
	// (Config.MaxTotalMessages, Config.MaxTotalBytes), a publication that
	// every subscription's lane together can make room for is admitted, and
	// the lanes that hold records give them up in turn, one each per pass,
	// for a lane that holds none.
	OverflowDropOldest

	overflowPolicyCount
)

var overflowPolicyNames = [overflowPolicyCount]string{"default", "block", "reject", "dropnew", "dropold"}

// String returns the policy's benchmark slug.
func (o OverflowPolicy) String() string {
	if o < overflowPolicyCount {
		return overflowPolicyNames[o]
	}
	return "OverflowPolicy(?)"
}

// Valid reports whether o names a policy or the default.
func (o OverflowPolicy) Valid() bool { return o < overflowPolicyCount }

// enginePolicy is the internal form. The default becomes an explicit Block,
// which is what makes the default the adapter's rather than the engine's.
func (o OverflowPolicy) enginePolicy() engine.OverflowPolicy {
	switch o {
	case OverflowBlock, OverflowDefault:
		return engine.OverflowBlock
	case OverflowReject:
		return engine.OverflowReject
	case OverflowDropNewest:
		return engine.OverflowDropNewest
	case OverflowDropOldest:
		return engine.OverflowDropOldest
	case overflowPolicyCount:
		return engine.OverflowBlock
	default:
		return engine.OverflowBlock
	}
}

// FanoutPolicy decides what a publication does when one of the subscriptions
// it fans out to has no room for it (the memory engine).
type FanoutPolicy uint8

// The admission policies. The zero value is FanoutAuto, which resolves to
// ADR-014's recorded default.
const (
	// FanoutAuto selects the recorded bake-off winner.
	FanoutAuto FanoutPolicy = iota
	// FanoutAtomic admits the record to every subscription or to none.
	FanoutAtomic
	// FanoutIndependent admits the record wherever it fits and reports the
	// partial outcome with ErrPartial.
	FanoutIndependent
	// FanoutBestEffort admits the record wherever it fits, silently, and
	// fails only when no subscription took it.
	FanoutBestEffort
	// FanoutDropSlow makes room in a full subscription by dropping its
	// oldest records, as many as the record needs, so it never refuses
	// while it holds anything that would make room. A subscription whose
	// leases leave no room for the record whatever it drops refuses it and
	// drops nothing. When a shared partition log is full, it drops the
	// log's oldest records, from the subscriptions still holding them, and
	// refuses while a lease holds the oldest. Under an engine-wide limit
	// (Config.MaxTotalMessages, Config.MaxTotalBytes) the subscriptions that
	// hold records give them up in turn, one each per pass, for every
	// subscription that takes the record. When that makes room for only some
	// of them, the ones that held nothing to drop take it first, so the one
	// keeping up is not the one refused. A shared partition log admits a
	// publication to every subscription or to none, so there such a
	// publication is refused and nothing is dropped for it.
	FanoutDropSlow

	fanoutPolicyCount
)

var fanoutPolicyNames = [fanoutPolicyCount]string{"auto", "atomic", "independent", "besteffort", "dropslow"}

// String returns the policy's benchmark slug.
func (f FanoutPolicy) String() string {
	if f < fanoutPolicyCount {
		return fanoutPolicyNames[f]
	}
	return "FanoutPolicy(?)"
}

// Valid reports whether f names a policy or Auto.
func (f FanoutPolicy) Valid() bool { return f < fanoutPolicyCount }

// enginePolicy is the internal form.
func (f FanoutPolicy) enginePolicy() engine.FanoutPolicy {
	switch f {
	case FanoutAtomic:
		return engine.Atomic
	case FanoutIndependent:
		return engine.Independent
	case FanoutBestEffort:
		return engine.BestEffort
	case FanoutDropSlow:
		return engine.DropSlow
	case FanoutAuto, fanoutPolicyCount:
		return engine.FanoutAuto
	default:
		return engine.FanoutAuto
	}
}

// The configuration failures. The specification names no sentinel for an invalid
// configuration and errors.Is matches by code, so a caller compares
// mq.CodeOf(err) against mq.CodeInvalidConfig.
var (
	errTopologyKind        = errcode.New(errcode.InvalidConfig, "iqmq/memory: TopologyConfig.Kind is not a topology profile")
	errTopologyOrdering    = errcode.New(errcode.InvalidConfig, "iqmq/memory: TopologyConfig.Ordering is not an ordering mode")
	errTopologyOverflow    = errcode.New(errcode.InvalidConfig, "iqmq/memory: TopologyConfig.Overflow is not an overflow policy")
	errTopologyFanout      = errcode.New(errcode.InvalidConfig, "iqmq/memory: TopologyConfig.Fanout is not a fan-out policy")
	errTopologyMaxInFlight = errcode.New(errcode.InvalidConfig, "iqmq/memory: TopologyConfig.MaxInFlight is not bounded separately; CapacityMessages bounds ready, in-flight and delayed records together ")
	errTopologyMaxDelayed  = errcode.New(errcode.InvalidConfig, "iqmq/memory: TopologyConfig.MaxDelayed is not bounded separately; CapacityMessages bounds ready, in-flight and delayed records together ")
	errTopologyRetention   = errcode.New(errcode.InvalidConfig, "iqmq/memory: TopologyConfig.Retention is meaningful only under TopologyStream")
	errSchedulerResolution = errcode.New(errcode.InvalidConfig, "iqmq/memory: Config.SchedulerResolution is negative")
	errTotalMessages       = errcode.New(errcode.InvalidConfig, "iqmq/memory: Config.MaxTotalMessages is negative")
	errTotalBytes          = errcode.New(errcode.InvalidConfig, "iqmq/memory: Config.MaxTotalBytes is negative")
)

// The topologies the engine would refuse later than the configuration is
// read, refused by resolve instead.
var (
	errTopologyPartitions = errcode.New(errcode.InvalidConfig, "iqmq/memory: TopologyConfig.Partitions asks for more than the engine's 1024 lanes per subscription")
	errTopologyPriority   = errcode.New(errcode.InvalidConfig, "iqmq/memory: TopologyConfig.Priority reorders a lane, which OrderingStrict promises to keep in order")
)
