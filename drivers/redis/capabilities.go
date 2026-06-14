package redis

import (
	"strconv"
	"strings"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/streams"
)

// serverProfile is what the driver learned about the server it connected to,
// and it is what the capability table branches on.
//
// The version is not a decoration. Three claims flip between the versions in
// the matrix, and a driver that guessed would either promise what it may not
// have or promise nothing:
//
//   - WAITAOF arrived in 7.0, so ConfirmDurable exists at all only from
//     there. On 6.2 there is no durable boundary to define.
//   - XAUTOCLAIM arrived in 6.2, and the mandatory reclaim loop is built on
//     it. Below 6.2 this driver has no redelivery, so at-least-once is false;
//     the driver refuses to open rather than advertise it.
//   - XAUTOCLAIM's third reply element — the identifiers it dropped because
//     the stream entries are gone — arrived in 7.0. Without it, an entry
//     trimmed while it was pending stays in the pending list for ever, which
//     is the unbounded PEL growth the specification asks about.
type serverProfile struct {
	// Version is the server's version string, empty when unknown.
	Version string
	// Software is "redis" or "valkey", or whatever the server called itself.
	Software string
	// Major and Minor are Version parsed, both zero when it could not be.
	Major, Minor int
	// AOF reports whether the server has an append-only file, which is
	// WAITAOF's condition.
	AOF bool
	// Cluster reports whether the driver is talking to a cluster.
	Cluster bool
}

// atLeast reports whether the server is at or above major.minor.
//
// An unknown version is NOT at least anything: the specification forbids reading an
// unadvertised fact as a favourable one, and a driver that assumed the newest
// server on a version it could not parse would advertise WAITAOF against a
// 6.2 that has never heard of it.
func (p serverProfile) atLeast(major, minor int) bool {
	if p.Major == 0 && p.Minor == 0 {
		return false
	}
	if p.Major != major {
		return p.Major > major
	}
	return p.Minor >= minor
}

// parseVersion splits "7.4.2" into 7 and 4. A version it cannot parse leaves
// both zero, which atLeast reads as "we do not know".
func parseVersion(v string) (major, minor int) {
	majorStr, rest, found := strings.Cut(v, ".")
	if !found {
		return 0, 0
	}
	minorStr, _, _ := strings.Cut(rest, ".")
	m, err := strconv.Atoi(majorStr)
	if err != nil {
		return 0, 0
	}
	n, err := strconv.Atoi(minorStr)
	if err != nil {
		return m, 0
	}
	return m, n
}

// MinVersion is the oldest server this driver will open against.
//
// It is 6.2 because XAUTOCLAIM is, and the reclaim loop that XAUTOCLAIM drives
// is not optional on this broker: a Redis consumer group hands an entry to a
// consumer and then does nothing about it for ever, so without the loop
// nothing redelivers and FeatureAtLeastOnce is a lie. Refusing to open is the
// only honest answer — the alternative is a driver that advertises
// at-least-once delivery and quietly has at-most-once.
const (
	MinVersionMajor = 6
	MinVersionMinor = 2
)

// capabilities builds the driver table once, at open, from what the
// connected server said and how the driver was configured.
//
// Two of the claims are CONDITIONAL, and a conditional claim has to state its
// condition — the specification requires it and [driverkit.CapabilityBuilder] refuses a
// conditional support with no note, which is what makes "conditional" mean
// something a program can act on rather than "might work, under conditions
// nobody wrote down".
func capabilities(p serverProfile, cfg Config) (mq.Capabilities, error) {
	b := driverkit.NewCapabilities()
	nack := cfg.Nack.Resolve()

	// What the broker does itself.
	b.Native(mq.FeatureDurableStorage, mq.ScopeTopic)
	b.Native(mq.FeatureAtLeastOnce, mq.ScopeTopic)
	b.Native(mq.FeatureExplicitAck, mq.ScopeTopic)
	b.Native(mq.FeatureConsumerGroups, mq.ScopeGroup)
	b.Native(mq.FeatureDurableSubscriptions, mq.ScopeGroup)
	b.Native(mq.FeatureFanOut, mq.ScopeTopic)
	b.Native(mq.FeatureReplay, mq.ScopeGroup)
	b.Native(mq.FeatureSeekByTime, mq.ScopeGroup)
	b.Native(mq.FeatureSeekBySequence, mq.ScopeGroup)
	b.Native(mq.FeatureBatchPublish, mq.ScopeTopic)
	b.Native(mq.FeatureBatchReceive, mq.ScopeTopic)
	b.Native(mq.FeatureBatchSettle, mq.ScopeTopic)
	b.Native(mq.FeatureAdmin, mq.ScopeTopic)

	// Headers, binary values and duplicate keys are all NATIVE, and that is
	// not the usual answer for a broker with no header field. A stream entry
	// is an ORDERED field-value list of binary-safe strings, so the entry
	// layout ([streams.Encode]) carries the whole promise in the entry
	// itself: positional "h:<n>" fields keep the order and allow a repeated
	// key, and a RESP bulk string carries any byte.
	//
	// The consequence is worth stating because it is the opposite of most
	// drivers': the portable envelope is never NEEDED here.
	// driverkit.Envelope.Needed answers false for every message, so
	// MetadataAdaptive behaves exactly like MetadataNativeStrict. The
	// envelope is still reachable, because the specification makes
	// MetadataPortableEnvelope a caller's choice rather than a driver's, and
	// that is the mode whose limits this driver has to check before any I/O.
	b.Native(mq.FeatureNativeHeaders, mq.ScopeTopic)
	b.Native(mq.FeatureBinaryHeaders, mq.ScopeTopic)
	b.Native(mq.FeatureDuplicateHeaders, mq.ScopeTopic)

	// A publication is confirmed when XADD's reply arrives, which means the
	// entry is in the server's data structure and will be served to a reader.
	// It does NOT mean it is on disk: that is WAITAOF's boundary below.
	b.Support(mq.FeaturePublisherConfirm, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "XADD's reply is the confirmation: the entry is in the server's memory and will be served to a reader. Nothing about disk is implied; ConfirmDurable is WAITAOF",
	})

	// The durable boundary. It is CONDITIONAL because it has two conditions
	// and both can be absent: WAITAOF needs Redis 7.0 or later, and it needs
	// the server to have an append-only file. Stating them is what the specification
	// requires of a conditional claim, and it is also what the note is FOR —
	// a caller reading "conditional" with no condition learns nothing.
	switch {
	case !p.atLeast(7, 0), !p.AOF:
		// Unsupported and not conditional, and the distinction matters. The specification
		// makes a conditional claim one whose condition a CALLER can satisfy
		// — "you need a replica", "you need to ask for it" — and neither of
		// these is: the server's version and whether it keeps an append-only
		// file are facts about the deployment that this client cannot change.
		// Advertising Conditional here would tell a program the feature might
		// work when the driver already knows it cannot, and
		// Capabilities.Has would answer true for a boundary OpenSink refuses.
		b.Unsupported(mq.FeatureDurableConfirm)
	default:
		b.Conditional(mq.FeatureDurableConfirm, mq.ScopeTopic,
			"ConfirmDurable is WAITAOF "+strconv.Itoa(cfg.DurableLocal)+" "+
				strconv.Itoa(cfg.DurableReplicas)+": the write has reached that many append-only files. "+
				"It is conditional on the server keeping one (`appendonly yes`, which this one does) and, "+
				"for a non-zero replica count, on that many replicas being connected and fsyncing")
	}

	// The lease. Redis grants none: nothing expires, nothing redelivers, and
	// the pending-entries list is a list of entries somebody was handed. The
	// deadline and the redelivery are therefore the DRIVER's, built out of
	// XAUTOCLAIM with min-idle equal to the deadline — which is an emulation
	// over a durable structure, and the condition is the reclaim loop
	// running.
	b.EmulatedDurable(mq.FeatureExtendLease, mq.ScopeTopic,
		"Redis grants no lease: the acknowledgement deadline is this driver's ("+cfg.resolve().AckDeadline.String()+
			") and it is enforced by the mandatory XAUTOCLAIM reclaim loop. An extension is XCLAIM ... JUSTID, "+
			"which resets the entry's idle clock without incrementing its delivery counter, so extending a lease "+
			"does not make a slow handler look like a failing message")

	// Negative settlement, and the delay. Both depend on the bound strategy,
	// which is the whole point: three of the four arms cannot delay
	// a redelivery at all, and a table that advertised a delay the bound
	// strategy refuses would be a table a caller acts on wrongly.
	b.EmulatedDurable(mq.FeatureNack, mq.ScopeTopic,
		"Redis Streams has no nack: an entry is pending or acknowledged and nothing else. A negative settlement "+
			"is the \""+nack.String()+"\" strategy — "+nackNote(nack))
	if strategyDelays(nack) {
		b.EmulatedDurable(mq.FeatureDelayedRedelivery, mq.ScopePartition,
			"the \""+nack.String()+"\" strategy acknowledges the entry and parks it in a sorted set scored by the "+
				"instant it becomes deliverable, both in one Lua script, and a mover returns it to the stream. "+
				"It is durable to the extent the server's persistence is: a parked entry lives in a sorted set, "+
				"so it survives exactly what the append-only file or the snapshot preserves. The delay is honoured "+
				"to within the mover's interval ("+cfg.resolve().MoverInterval.String()+")")
	} else {
		b.Conditional(mq.FeatureDelayedRedelivery, mq.ScopePartition,
			"the bound strategy is \""+nack.String()+"\", which cannot hold a redelivery back: a nack asking for a "+
				"delay it cannot honour is refused with ErrUnsupported rather than redelivered at some other time. "+
				"Configure nack=zsetmover for an arbitrary delay")
	}

	// Dead-letter. The move is a Lua script when the destination shares the
	// topic's hash slot and two commands when it does not, and the difference
	// is a duplicate window — which is a condition, so it is stated.
	b.EmulatedDurable(mq.FeatureDeadLetter, mq.ScopeTopic,
		"a Reject copies the entry to the dead-letter stream and acknowledges it in the source, in one Lua script, "+
			"so a crash cannot leave the message in both places or in neither. When the dead-letter destination does "+
			"NOT share the topic's cluster hash slot the script cannot run at all (CROSSSLOT) and the move becomes two "+
			"commands: a crash between them dead-letters the message twice. The default destination is the topic's own "+
			"key plus \""+cfg.resolve().DeadLetterSuffix+"\", which shares the slot by construction")

	// Ordering. A stream is a total order and a consumer group hands its
	// entries out in that order, so ordering here is per SHARD and the three
	// features differ in scope rather than in substance. What breaks it is
	// named rather than omitted: more than one consumer of a group
	// interleaves, and two of the four nack strategies move a retried message
	// to the end of its shard.
	orderNote := "a stream is a total order and a consumer group hands out entries in it, so one consumer of a " +
		"subscription sees a shard in publication order. Two consumers of one subscription interleave across the " +
		"shard, and a retried message keeps its position only under a nack strategy that does not re-add it"
	if strategyReorders(nack) {
		orderNote += " — the bound strategy is \"" + nack.String() + "\", which DOES re-add, so a retried message " +
			"is delivered after everything published since it failed"
	} else {
		orderNote += " — the bound strategy is \"" + nack.String() + "\", which leaves the entry in place, so a " +
			"retry keeps its position"
	}
	b.Conditional(mq.FeatureOrderedByPartition, mq.ScopePartition, orderNote)
	b.Conditional(mq.FeatureOrderedByKey, mq.ScopeKey,
		"a message's key chooses its shard, so messages sharing a key share a shard and are ordered within it: "+orderNote)
	if cfg.resolve().Partitions == 1 {
		b.Conditional(mq.FeatureOrderedGlobally, mq.ScopeTopic,
			"this topic has one shard, so its shard order IS its global order: "+orderNote)
	} else {
		b.Unsupported(mq.FeatureOrderedGlobally)
	}

	// Everything else, stated rather than omitted. CapabilityBuilder.Missing
	// is what turns "the driver decided it does not have this" into something
	// a test can tell apart from "nobody looked".
	for _, f := range [...]mq.Feature{
		mq.FeatureAtMostOnce,
		mq.FeatureExactlyOnceDelivery,
		mq.FeatureScheduledPublish,
		mq.FeatureMessageTTL,
		mq.FeaturePriority,
		mq.FeatureAtomicBatchPublish,
		mq.FeatureIdempotentPublish,
		mq.FeatureDeduplication,
		mq.FeatureTransactions,
		mq.FeatureServerFilter,
		mq.FeatureWildcardSubscribe,
		mq.FeatureRequestReply,
		mq.FeatureNativeTracing,
		mq.FeatureReceipts,
	} {
		b.Unsupported(f)
	}

	// The limits. Only what the driver really knows: the specification forbids reading an
	// unadvertised limit as zero, so an honest "we do not know" is a Limit
	// whose Known is false.
	//
	// The two that are known are the driver's own. MaxHeaderCount and the
	// message size are the entry layout's bounds, which are the portable
	// envelope's ([streams.DefaultLimits]) — a message over them would be
	// accepted and then be undecodable, so the core refuses it before any
	// I/O. MaxLease is the acknowledgement deadline's ceiling, which is the
	// driver's too because the lease is.
	//
	// Redis's own proto-max-bulk-len (512 MiB by default) is NOT advertised:
	// it is a server setting this driver does not read, and advertising a
	// default as a fact is what the specification forbids.
	lim := streams.DefaultLimits
	b.Limits(mq.Limits{
		MaxMessageBytes: mq.Limit[int64]{Value: int64(lim.MaxBody), Known: true},
		MaxHeaderCount:  mq.Limit[int]{Value: lim.MaxHeaders, Known: true},
	})

	if missing := b.Missing(); len(missing) > 0 {
		// A feature nobody mentioned renders as unsupported, which is the
		// safe answer — but it is also indistinguishable from a feature the
		// driver forgot, and TestCapabilitiesConsidersEveryFeature is what
		// keeps that from happening quietly as the inventory grows.
		return mq.Capabilities{}, errIncompleteTable
	}
	return b.Build()
}

// nackNote is the sentence each strategy's capability note ends with.
func nackNote(k streams.Kind) string {
	switch k {
	case streams.LeavePending:
		return "the entry is left in the pending-entries list and the reclaim loop redelivers it once it has been " +
			"idle for the acknowledgement deadline. Nothing is written, so nothing can be half-written, and the " +
			"entry keeps its identifier, its position and its delivery counter"
	case streams.ZSetMover:
		return "the entry is acknowledged and parked in a sorted set scored by the instant it becomes deliverable, " +
			"in one Lua script, and a mover returns it to the stream. It is the arm that honours an arbitrary delay"
	case streams.ReAdd:
		return "the entry is acknowledged and added back under a new identifier, in one Lua script. The redelivery " +
			"is immediate and the message loses its position in the shard"
	case streams.Stop:
		return "the entry is left pending AND the source stops reading new entries from its shard until it is " +
			"settled, so nothing behind the failed message is delivered before it. One poisonous message stops its " +
			"shard until MaxAttempts dead-letters it"
	case streams.Auto:
		return "the strategy is chosen by the bake-off"
	}
	return ""
}

// strategyDelays and strategyReorders answer the two questions the capability
// table asks of the bound strategy, without building one.
//
// They are switches rather than a call to [streams.Strategy] because the table
// is built before a source exists and a strategy needs a topic's key naming.
// The pairing is held to the real strategies by
// TestCapabilityNotesMatchTheStrategy, so a kind whose behaviour changed
// cannot leave the table saying the old thing.
func strategyDelays(k streams.Kind) bool { return k.Resolve() == streams.ZSetMover }

func strategyReorders(k streams.Kind) bool {
	switch k.Resolve() {
	case streams.ZSetMover, streams.ReAdd:
		return true
	case streams.LeavePending, streams.Stop, streams.Auto:
		return false
	}
	return false
}
