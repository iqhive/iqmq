package iqmq

import "time"

// Level is the support level of one feature. Levels are categories,
// not an ordinal scale: compare them with Rank, which is the documented
// ordinal RequireAtLeast uses, and never by the constants' own order.
type Level uint8

// The support levels, in the declaration order.
const (
	// Unsupported means the feature is absent.
	Unsupported Level = iota
	// Conditional means the feature is available only with a profile or an
	// option (SQS FIFO, Pub/Sub ordering). A conditional support states its
	// condition in Support.Note.
	Conditional
	// EmulatedVolatile means iqmq emulates the feature in process. It is lost
	// on a crash and NEVER satisfies a durability requirement.
	EmulatedVolatile
	// EmulatedDurable means iqmq emulates the feature using durable broker
	// features, such as a dead-letter topic publish.
	EmulatedDurable
	// Native means the broker provides the feature itself.
	Native

	levelCount
)

var levelNames = [levelCount]string{
	"unsupported", "conditional", "emulatedvolatile", "emulateddurable", "native",
}

// String returns the level's lower-case name.
func (l Level) String() string {
	if l < levelCount {
		return levelNames[l]
	}
	return "level(?)"
}

// Valid reports whether l names a level.
func (l Level) Valid() bool { return l < levelCount }

// rankOf is the documented ordinal the specification promises for RequireAtLeast. It is
// deliberately not the declaration order: volatile emulation "NEVER satisfies
// a durability requirement", while a conditional feature is the broker's own
// once its condition is met, so volatile emulation ranks below conditional
// support.
var rankOf = [levelCount]int{
	Unsupported:      0,
	EmulatedVolatile: 1,
	Conditional:      2,
	EmulatedDurable:  3,
	Native:           4,
}

// Rank returns the documented ordinal RequireAtLeast compares. A level outside
// the defined range ranks zero.
func (l Level) Rank() int {
	if l < levelCount {
		return rankOf[l]
	}
	return 0
}

// Durable reports whether the level can satisfy a durability requirement.
// EmulatedVolatile never can.
func (l Level) Durable() bool {
	return l == Native || l == EmulatedDurable || l == Conditional
}

// Scope is the extent over which a feature holds.
type Scope uint8

// The scopes.
const (
	// ScopeNone means the feature has no scope of its own.
	ScopeNone Scope = iota
	// ScopeKey is per message key.
	ScopeKey
	// ScopePartition is per partition or lane.
	ScopePartition
	// ScopeGroup is per consumer group.
	ScopeGroup
	// ScopeTopic is the whole topic.
	ScopeTopic

	scopeCount
)

var scopeNames = [scopeCount]string{"none", "key", "partition", "group", "topic"}

// String returns the scope's lower-case name.
func (s Scope) String() string {
	if s < scopeCount {
		return scopeNames[s]
	}
	return "scope(?)"
}

// Valid reports whether s names a scope.
func (s Scope) Valid() bool { return s < scopeCount }

// Support is what a driver advertises about one feature.
type Support struct {
	// Level is the support level.
	Level Level
	// Scope is the extent the support holds over.
	Scope Scope
	// Note names the condition of a conditional support, which the specification requires a
	// driver to state, or explains an emulation. It is empty otherwise.
	Note string
}

// Limit is a limit a driver may or may not advertise.
//
// The specification prototyped Support only, so nothing in the bake-off exercised this
// half.
//
// Known is the whole point: an unknown limit is never an overloaded zero.
// Limit[T]{Value: 0, Known: false} means the driver does not advertise the
// limit and nothing may be enforced from it; Limit[T]{Value: 0, Known: true}
// means the driver advertises a limit of zero. No caller may read one as the
// other, and no code in iqmq enforces a limit whose Known is false.
type Limit[T any] struct {
	// Value is the limit, meaningful only when Known is true.
	Value T
	// Known reports whether the driver advertises this limit at all.
	Known bool
}

// Limits are the numeric limits a driver advertises. Every field is a
// Limit, so "not advertised" and "advertised as zero" stay distinct.
type Limits struct {
	// MaxMessageBytes is the largest single message the broker accepts.
	MaxMessageBytes Limit[int64]
	// MaxBatchMessages is the most messages one batch may carry.
	MaxBatchMessages Limit[int]
	// MaxBatchBytes is the largest batch the broker accepts.
	MaxBatchBytes Limit[int64]
	// MaxHeaderCount is the most headers one message may carry.
	MaxHeaderCount Limit[int]
	// MaxHeaderBytes is the largest header set, counting key and value bytes.
	MaxHeaderBytes Limit[int64]
	// MaxDelay is the furthest into the future a publication may be scheduled.
	MaxDelay Limit[time.Duration]
	// MaxLease is the longest acknowledgement deadline the broker grants.
	MaxLease Limit[time.Duration]
}

// LimitKey names one field of Limits, so that a caller can ask for a limit it
// was given the name of.
type LimitKey uint8

// The limit keys, one per field of Limits.
const (
	// LimitMaxMessageBytes is Limits.MaxMessageBytes, in bytes.
	LimitMaxMessageBytes LimitKey = iota
	// LimitMaxBatchMessages is Limits.MaxBatchMessages, a count.
	LimitMaxBatchMessages
	// LimitMaxBatchBytes is Limits.MaxBatchBytes, in bytes.
	LimitMaxBatchBytes
	// LimitMaxHeaderCount is Limits.MaxHeaderCount, a count.
	LimitMaxHeaderCount
	// LimitMaxHeaderBytes is Limits.MaxHeaderBytes, in bytes.
	LimitMaxHeaderBytes
	// LimitMaxDelay is Limits.MaxDelay, in nanoseconds.
	LimitMaxDelay
	// LimitMaxLease is Limits.MaxLease, in nanoseconds.
	LimitMaxLease

	limitKeyCount
)

// LimitKeyCount is the number of limit keys.
const LimitKeyCount = int(limitKeyCount)

var limitKeyNames = [limitKeyCount]string{
	"MaxMessageBytes", "MaxBatchMessages", "MaxBatchBytes",
	"MaxHeaderCount", "MaxHeaderBytes", "MaxDelay", "MaxLease",
}

// String returns the key's field name in Limits.
func (k LimitKey) String() string {
	if k < limitKeyCount {
		return limitKeyNames[k]
	}
	return "LimitKey(?)"
}

// Valid reports whether k names a limit.
func (k LimitKey) Valid() bool { return k < limitKeyCount }

// Capabilities is what one client can and cannot do. It is constant for
// the life of a Client: the driver builds it at open from its profile and a
// cheap probe, and nothing mutates it afterwards.
//
// The representation is the winner (ADR-007, Array): an array of Support
// indexed by Feature, so Has is a bounds check and a load, with room for the
// scope and the note a conditional support must state. The array lives behind
// a pointer so that Capabilities is cheap to copy by value.
//
// The zero value advertises nothing: Has is false for every feature, Support
// is the zero Support, and every Limit is unknown. That is the honest answer
// for a driver that has not said, and it is why an unadvertised limit can
// never be read as a limit of zero.
type Capabilities struct {
	set *capabilitySet
}

// capabilitySet is the storage behind a Capabilities value.
type capabilitySet struct {
	features [featureCount]Support
	limits   Limits
}

// Has reports whether the feature is available at all, at any level. A feature
// outside the inventory is not available; Has never panics (target 9).
func (c Capabilities) Has(f Feature) bool {
	return c.Support(f).Level != Unsupported
}

// Support returns what the driver advertises about the feature. An
// unadvertised or unknown feature returns the zero Support, whose Level is
// Unsupported.
func (c Capabilities) Support(f Feature) Support {
	if c.set == nil || !f.Valid() {
		return Support{}
	}
	return c.set.features[f]
}

// Limits returns every limit the driver advertises.
func (c Capabilities) Limits() Limits {
	if c.set == nil {
		return Limits{}
	}
	return c.set.limits
}

// Limit returns one limit by key, normalised to int64: byte and count limits
// as they stand, durations in nanoseconds. An unadvertised limit, and any key
// outside the inventory, returns a Limit whose Known is false.
func (c Capabilities) Limit(l LimitKey) Limit[int64] {
	if c.set == nil {
		return Limit[int64]{}
	}
	lim := &c.set.limits
	switch l {
	case LimitMaxMessageBytes:
		return lim.MaxMessageBytes
	case LimitMaxBatchMessages:
		return Limit[int64]{Value: int64(lim.MaxBatchMessages.Value), Known: lim.MaxBatchMessages.Known}
	case LimitMaxBatchBytes:
		return lim.MaxBatchBytes
	case LimitMaxHeaderCount:
		return Limit[int64]{Value: int64(lim.MaxHeaderCount.Value), Known: lim.MaxHeaderCount.Known}
	case LimitMaxHeaderBytes:
		return lim.MaxHeaderBytes
	case LimitMaxDelay:
		return Limit[int64]{Value: int64(lim.MaxDelay.Value), Known: lim.MaxDelay.Known}
	case LimitMaxLease:
		return Limit[int64]{Value: int64(lim.MaxLease.Value), Known: lim.MaxLease.Known}
	case limitKeyCount:
	}
	return Limit[int64]{}
}

// Set records what the driver advertises about one feature. It reports
// CodeInvalidArgument for a feature, level or scope outside its range: a
// capability set that quietly stored an unknown level would answer
// requirements about it later.
//
// Set is a construction-time operation. The specification makes capabilities constant per
// Client, so a driver fills them at open and never calls Set again; a
// Capabilities value copied after construction shares the storage the copy was
// made from.
func (c *Capabilities) Set(f Feature, s Support) error {
	if !f.Valid() || !s.Level.Valid() || !s.Scope.Valid() {
		return errInvalidCapability
	}
	if c.set == nil {
		c.set = &capabilitySet{}
	}
	c.set.features[f] = s
	return nil
}

// SetLimits records the limits the driver advertises. It is, like Set, a
// construction-time operation.
func (c *Capabilities) SetLimits(l Limits) {
	if c.set == nil {
		c.set = &capabilitySet{}
	}
	c.set.limits = l
}

// Feature identifies one capability of a backend.
type Feature uint16

// The feature inventory, in the order the specification lists it.
// Features are appended, never renumbered.
const (
	// FeatureDurableStorage is persistence of accepted messages.
	FeatureDurableStorage Feature = iota
	// FeatureAtMostOnce is delivery that may lose but never duplicates.
	FeatureAtMostOnce
	// FeatureAtLeastOnce is delivery that may duplicate but never loses.
	FeatureAtLeastOnce
	// FeatureExactlyOnceDelivery is de-duplicated at-least-once delivery.
	FeatureExactlyOnceDelivery
	// FeatureExplicitAck is caller-driven positive settlement.
	FeatureExplicitAck
	// FeatureNack is caller-driven negative settlement.
	FeatureNack
	// FeatureDelayedRedelivery is a nack with a delay before redelivery.
	FeatureDelayedRedelivery
	// FeatureScheduledPublish is a publication that becomes visible later.
	FeatureScheduledPublish
	// FeatureMessageTTL is a per-message expiry.
	FeatureMessageTTL
	// FeaturePriority is per-message priority.
	FeaturePriority
	// FeatureDeadLetter is a dead-letter destination.
	FeatureDeadLetter
	// FeatureConsumerGroups is competing consumers sharing a subscription.
	FeatureConsumerGroups
	// FeatureDurableSubscriptions is a subscription that survives disconnect.
	FeatureDurableSubscriptions
	// FeatureFanOut is independent delivery to several subscriptions.
	FeatureFanOut
	// FeatureReplay is re-reading messages already delivered.
	FeatureReplay
	// FeatureSeekByTime is positioning a subscription at a timestamp.
	FeatureSeekByTime
	// FeatureSeekBySequence is positioning a subscription at a sequence.
	FeatureSeekBySequence
	// FeatureOrderedByKey is order preserved per message key.
	FeatureOrderedByKey
	// FeatureOrderedByPartition is order preserved per partition.
	FeatureOrderedByPartition
	// FeatureOrderedGlobally is order preserved across the whole topic.
	FeatureOrderedGlobally
	// FeatureNativeHeaders is broker-side header storage.
	FeatureNativeHeaders
	// FeatureBinaryHeaders is header values that are not valid UTF-8.
	FeatureBinaryHeaders
	// FeatureDuplicateHeaders is several values under one header key.
	FeatureDuplicateHeaders
	// FeatureBatchPublish is publishing several messages in one call.
	FeatureBatchPublish
	// FeatureAtomicBatchPublish is a batch that is accepted all or nothing.
	FeatureAtomicBatchPublish
	// FeatureBatchReceive is fetching several messages in one call.
	FeatureBatchReceive
	// FeatureBatchSettle is settling several deliveries in one call.
	FeatureBatchSettle
	// FeatureExtendLease is extending an acknowledgement deadline.
	FeatureExtendLease
	// FeatureIdempotentPublish is broker-side producer de-duplication.
	FeatureIdempotentPublish
	// FeatureDeduplication is broker-side de-duplication by a caller key.
	FeatureDeduplication
	// FeatureTransactions is atomic multi-operation commit.
	FeatureTransactions
	// FeatureServerFilter is broker-side subscription filtering.
	FeatureServerFilter
	// FeatureWildcardSubscribe is subscribing to a pattern of topics.
	FeatureWildcardSubscribe
	// FeatureAdmin is topology management.
	FeatureAdmin
	// FeatureRequestReply is native request/reply.
	FeatureRequestReply
	// FeaturePublisherConfirm is broker acknowledgement of a publication.
	FeaturePublisherConfirm
	// FeatureDurableConfirm is a publisher confirm that implies persistence.
	FeatureDurableConfirm
	// FeatureNativeTracing is broker-side trace propagation.
	FeatureNativeTracing
	// FeatureReceipts is broker-assigned identifiers returned on publish.
	FeatureReceipts

	featureCount
)

// FeatureCount is the size of the feature inventory.
const FeatureCount = int(featureCount)

var featureNames = [featureCount]string{
	"DurableStorage", "AtMostOnce", "AtLeastOnce", "ExactlyOnceDelivery", "ExplicitAck",
	"Nack", "DelayedRedelivery", "ScheduledPublish", "MessageTTL", "Priority",
	"DeadLetter", "ConsumerGroups", "DurableSubscriptions", "FanOut", "Replay",
	"SeekByTime", "SeekBySequence", "OrderedByKey", "OrderedByPartition", "OrderedGlobally",
	"NativeHeaders", "BinaryHeaders", "DuplicateHeaders", "BatchPublish", "AtomicBatchPublish",
	"BatchReceive", "BatchSettle", "ExtendLease", "IdempotentPublish", "Deduplication",
	"Transactions", "ServerFilter", "WildcardSubscribe", "Admin", "RequestReply",
	"PublisherConfirm", "DurableConfirm", "NativeTracing", "Receipts",
}

// String returns the feature's specification name, without the Feature
// prefix, or "Feature(?)" for a value outside the inventory.
func (f Feature) String() string {
	if f < featureCount {
		return featureNames[f]
	}
	return "Feature(?)"
}

// Valid reports whether f names a feature of the inventory.
func (f Feature) Valid() bool { return f < featureCount }

// ParseFeature maps a name returned by Feature.String back to its feature. It
// is what the generated capability matrix and capabilities.json read.
func ParseFeature(s string) (Feature, bool) {
	for i, n := range &featureNames {
		if n == s {
			return Feature(i), true
		}
	}
	return 0, false
}
