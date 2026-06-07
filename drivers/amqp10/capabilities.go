package amqp10

import (
	"strconv"
	"strings"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
)

// CoreCeiling is the strongest support this core can honour for each feature,
// whatever a profile claims.
//
// It is the enforcement half of the [Profile.Capabilities] seam and it is the
// reason that seam is safe to hand to somebody else. A profile is written by
// whoever knows the broker, and the obvious mistake is to claim a feature the
// BROKER has and this CORE has no code for: Artemis really does extend a
// message's lease, and this driver has no Extend to offer, so a profile that
// claimed FeatureExtendLease would publish a capability the core answers
// ErrUnsupported to. The specification makes the table the thing a caller acts on — a
// program reads it and decides whether it may rely on a behaviour — so a table
// that can lie is worse than a table that says nothing.
//
// A ceiling of [bitbucket.org/iqhive/iqmq.Unsupported] means the core cannot do it
// at all and no profile may claim it. A ceiling of
// [bitbucket.org/iqhive/iqmq.Native] means the answer is a fact about the broker
// and the profile is the only thing that knows it. Ranks are compared with
// [bitbucket.org/iqhive/iqmq.Level.Rank], which is the documented ordinal and
// deliberately not the constants' declaration order.
//
// Three ceilings are conditional on the profile itself rather than fixed, and
// they are checked alongside this table: FeatureAdmin needs [Profile.Admin],
// FeatureDurableConfirm needs [Profile.Durable], and FeatureDeduplication and
// FeatureIdempotentPublish need [Profile.Dedup] — because in each case the core
// does have the code and the profile has to supply the one value it needs.
func CoreCeiling(f mq.Feature) mq.Level {
	if !f.Valid() {
		return mq.Unsupported
	}
	return coreCeiling[f]
}

// coreCeiling is CoreCeiling's table. Everything not named here is Unsupported,
// which is the safe default as the inventory grows: a feature added
// after this file was written cannot be claimed by a profile until somebody has
// decided what this core does about it.
var coreCeiling = func() [mq.FeatureCount]mq.Level {
	var t [mq.FeatureCount]mq.Level
	// Facts about the BROKER. The core has the code; the profile is the only
	// thing that knows whether this deployment has the behaviour.
	for _, f := range [...]mq.Feature{
		mq.FeatureDurableStorage,
		mq.FeatureAtMostOnce,
		mq.FeatureAtLeastOnce,
		mq.FeatureExplicitAck,
		mq.FeatureNack,
		mq.FeatureDelayedRedelivery,
		mq.FeatureScheduledPublish,
		mq.FeatureMessageTTL,
		mq.FeaturePriority,
		mq.FeatureDeadLetter,
		mq.FeatureConsumerGroups,
		mq.FeatureDurableSubscriptions,
		mq.FeatureFanOut,
		mq.FeatureOrderedByKey,
		mq.FeatureOrderedByPartition,
		mq.FeatureOrderedGlobally,
		mq.FeatureNativeHeaders,
		mq.FeatureBinaryHeaders,
		mq.FeatureBatchReceive,
		mq.FeatureIdempotentPublish,
		mq.FeatureDeduplication,
		mq.FeatureServerFilter,
		mq.FeatureWildcardSubscribe,
		mq.FeatureAdmin,
		mq.FeaturePublisherConfirm,
		mq.FeatureDurableConfirm,
		mq.FeatureReceipts,
	} {
		t[f] = mq.Native
	}
	// Everything else stays Unsupported, and each has a reason worth stating
	// once:
	//
	//   - FeatureExactlyOnceDelivery: AMQP 1.0's `received` state resumes a
	//     partial transfer and does not de-duplicate a redelivery. A broker
	//     that de-duplicates PUBLICATIONS claims FeatureDeduplication above,
	//     which is a different promise.
	//   - FeatureReplay, FeatureSeekByTime, FeatureSeekBySequence: a receiving
	//     link attaches to an address and reads what is there. There is no
	//     portable position and no portable seek, so the core refuses a start
	//     position of a time or a sequence rather than pretending to one.
	//   - FeatureDuplicateHeaders: application-properties is a MAP, so a
	//     repeated key cannot exist in it. No profile can make it not a map;
	//     MetadataAdaptive envelopes such a message instead.
	//   - FeatureAtomicBatchPublish: a transfer carries one message and
	//     go-amqp has no batched transfer, so there is nothing that could be
	//     all-or-nothing. FeatureBatchPublish is a different answer and is an
	//     EMULATION below: the batch works, because the core loops.
	//   - FeatureBatchSettle: a disposition frame CAN cover a range of
	//     delivery ids, and go-amqp does not expose one — Receiver's
	//     range-disposition method is unexported and only the per-message
	//     wrappers are public. This is therefore a client-library ceiling
	//     rather than a protocol one, and it is the one row in this table
	//     that a go-amqp release could raise.
	//   - FeatureExtendLease: AMQP 1.0 has no lease at all. An unsettled
	//     delivery is the receiver's for as long as the link lives, so there
	//     is no deadline to extend — which is also why this driver's
	//     DeliveryInfo.LeaseExpires is the zero time.
	//   - FeatureTransactions: go-amqp implements no transactional resource.
	//   - FeatureRequestReply: reply-to is carried as a message property and
	//     a native request/reply over a dynamic reply address is not
	//     implemented here.
	//   - FeatureNativeTracing: nothing in this core propagates a trace
	//     context of its own.
	return t
}()

// capabilityBuilder builds this backend table: the core's own base, then
// the profile's claims over it, then the limits.
//
// It returns the builder rather than the table so that the driver's own test can
// ask [bitbucket.org/iqhive/iqmq/driverkit.CapabilityBuilder.Missing] whether every
// feature of the inventory was considered — which is the difference between a
// feature this driver decided it does not have and one nobody looked at.
func capabilityBuilder(p Profile, f Facts) (*driverkit.CapabilityBuilder, error) {
	table := baseTable(p, f)
	if p.Capabilities != nil {
		for _, c := range p.Capabilities(f) {
			if err := checkClaim(p, c); err != nil {
				return nil, err
			}
			table[c.Feature] = c.Support
		}
	}
	b := driverkit.NewCapabilities()
	for i := range mq.FeatureCount {
		b.Support(mq.Feature(i), table[i])
	}
	lim := coreLimits(p, f)
	if p.Limits != nil {
		lim = p.Limits(f, lim)
	}
	b.Limits(lim)
	return b, nil
}

// capabilities is the table itself, refusing an incomplete one.
func capabilities(p Profile, f Facts) (mq.Capabilities, error) {
	b, err := capabilityBuilder(p, f)
	if err != nil {
		return mq.Capabilities{}, err
	}
	if missing := b.Missing(); len(missing) > 0 {
		// Every feature is set explicitly above, so this is unreachable
		// until the inventory grows — which is exactly when it should
		// fire, because a feature nobody has decided about renders as
		// unsupported and is indistinguishable from one the driver forgot.
		return mq.Capabilities{}, errIncompleteTable
	}
	return b.Build()
}

// baseTable is what AMQP 1.0 and this implementation do, before any profile
// speaks.
//
// It says something about every feature of the inventory, because the specification makes the
// absent half of a table as load-bearing as the present half: a caller reads it
// to decide whether it may rely on a behaviour, and "nobody mentioned it" and
// "the driver decided it does not have it" are not the same answer.
func baseTable(p Profile, f Facts) [mq.FeatureCount]mq.Support {
	var t [mq.FeatureCount]mq.Support
	// Every native claim in this table is topic-scoped, because that is what
	// the PROTOCOL's own guarantees are about: a transfer, a disposition and a
	// credit window all belong to one link on one address. A narrower scope is
	// a fact about the broker, which is the profile's to claim.
	native := func(feat mq.Feature) { t[feat] = mq.Support{Level: mq.Native, Scope: mq.ScopeTopic} }
	note := func(feat mq.Feature, lvl mq.Level, scope mq.Scope, n string) {
		t[feat] = mq.Support{Level: lvl, Scope: scope, Note: n}
	}

	// The protocol's own settlement, which is the whole reason this core
	// exists. A transfer is unsettled until the receiver's disposition
	// arrives, and the four terminal outcomes are accepted, released,
	// rejected and modified.
	native(mq.FeatureAtLeastOnce)
	native(mq.FeatureExplicitAck)
	native(mq.FeatureNack)
	native(mq.FeatureNativeHeaders)
	native(mq.FeatureBatchReceive)

	// A binary application-property value is a simple AMQP type, so a header
	// that is not valid UTF-8 travels as binary and comes back byte for byte.
	// A repeated KEY cannot, because application-properties is a map.
	note(mq.FeatureBinaryHeaders, mq.Native, mq.ScopeTopic,
		"a header value that is not valid UTF-8 travels as an AMQP binary application property and comes back byte for byte; a value that is valid UTF-8 travels as a string, so a non-iqmq consumer reads it")

	// The confirmation boundary. The accepted disposition is the peer's
	// answer that it has taken the transfer, and it is what ConfirmAccepted
	// waits for.
	note(mq.FeaturePublisherConfirm, mq.Native, mq.ScopeTopic,
		"the peer's `accepted` disposition for the transfer, which ConfirmAccepted waits for; ConfirmNone sends the transfer pre-settled and waits for nothing")

	// A receipt carries the boundary and the time and NOT an identifier: AMQP
	// 1.0's disposition says `accepted` and nothing else, so a broker-assigned
	// identifier is something this protocol does not have. Claiming
	// FeatureReceipts and then filling MessageID with the identifier the
	// PUBLISHER sent would be reporting the caller's own value back as the
	// broker's.
	note(mq.FeatureReceipts, mq.Conditional, mq.ScopeTopic,
		"the receipt carries the confirmation boundary and the time the disposition arrived; AMQP 1.0's disposition assigns no identifier, so PublishReceipt.MessageID stays empty and Partition, Offset and Sequence stay zero")

	// Per-message TTL and priority are fields of the AMQP message header, so
	// they are the core's and not a profile's. What a broker DOES with them
	// is the broker's business, which is why a profile may narrow both.
	note(mq.FeatureMessageTTL, mq.Native, mq.ScopeTopic,
		"PublishOptions.TTL travels as the AMQP message header's `ttl` in milliseconds; what the node does when it expires is the broker's policy")
	note(mq.FeaturePriority, mq.Native, mq.ScopeTopic,
		"PublishOptions.Priority travels as the AMQP message header's `priority`; a broker that does not order by priority accepts the field and ignores it, which is why a profile that knows so withdraws this claim")

	// Server-side filtering. The portable API can only name a source address,
	// which IS a server-side filter on every broker whose addressing puts the
	// subscription in the address; a selector needs the driver's own
	// ConsumerOptions.
	note(mq.FeatureServerFilter, mq.Conditional, mq.ScopeTopic,
		"the receiving link's source address is the only filter the portable API can express; an AMQP 1.0 selector filter goes through ConsumerOptions.Filters, which is this driver's own typed configuration")

	// Everything the core leaves to the profile, stated as absent until one
	// speaks. A base table that claimed any of these would be claiming a fact
	// about a broker it has not connected to yet.
	for _, feat := range [...]mq.Feature{
		mq.FeatureDurableStorage,
		mq.FeatureDelayedRedelivery,
		mq.FeatureScheduledPublish,
		mq.FeatureDeadLetter,
		mq.FeatureConsumerGroups,
		mq.FeatureDurableSubscriptions,
		mq.FeatureFanOut,
		mq.FeatureOrderedByKey,
		mq.FeatureOrderedByPartition,
		mq.FeatureOrderedGlobally,
		mq.FeatureWildcardSubscribe,
		mq.FeatureIdempotentPublish,
		mq.FeatureDeduplication,
	} {
		t[feat] = mq.Support{Level: mq.Unsupported}
	}

	// Everything the CORE cannot do, with the reason in the note where a
	// reader would otherwise ask. CoreCeiling's own documentation carries the
	// full list; these are the four a caller is most likely to want.
	note(mq.FeatureExtendLease, mq.Unsupported, mq.ScopeNone,
		"AMQP 1.0 has no lease: an unsettled delivery is this receiver's for as long as the link lives, so there is no deadline to extend and DeliveryInfo.LeaseExpires is the zero time")
	note(mq.FeatureBatchSettle, mq.Unsupported, mq.ScopeNone,
		"a disposition frame can cover a range of delivery ids and go-amqp exposes only the per-message wrappers, so a batched settlement here would be a loop")
	note(mq.FeatureDuplicateHeaders, mq.Unsupported, mq.ScopeNone,
		"application-properties is a map, so one key cannot carry two values; MetadataPortableEnvelope and MetadataAdaptive carry a repeated key in the envelope instead")
	note(mq.FeatureReplay, mq.Unsupported, mq.ScopeNone,
		"a receiving link attaches to an address and reads what is there; AMQP 1.0 has no position, so there is nothing to rewind")
	// An EMULATION and not an absence, which is the distinction the specification's
	// levels exist for. A transfer carries one message and go-amqp has no
	// batched transfer, so there is no batch FRAME — and `PublisherHandle.
	// PublishBatch` still works, because the core loops and this driver
	// awaits every disposition. Claiming Unsupported would be refuting
	// something a caller can do, which the conformance kit catches outright
	// ("the driver advertises no BatchPublish and the feature worked
	// anyway"). It is volatile rather than durable because the durability of
	// one publication is the PROFILE's answer (Profile.Durable), and a claim
	// here cannot know it.
	note(mq.FeatureBatchPublish, mq.EmulatedVolatile, mq.ScopeTopic,
		"AMQP 1.0 has no batched transfer: the core publishes every message of the batch and this driver awaits each disposition, so a batch is NOT atomic -- a failure part-way through leaves the messages before it published")
	for _, feat := range [...]mq.Feature{
		mq.FeatureAtMostOnce,
		mq.FeatureExactlyOnceDelivery,
		mq.FeatureSeekByTime,
		mq.FeatureSeekBySequence,
		mq.FeatureAtomicBatchPublish,
		mq.FeatureTransactions,
		mq.FeatureRequestReply,
		mq.FeatureNativeTracing,
	} {
		t[feat] = mq.Support{Level: mq.Unsupported}
	}

	// The three the profile has to supply a VALUE for, not merely a claim.
	// Each is the core's code plus one thing only the broker-specific module
	// knows, so the base table reads the profile rather than the connection.
	t[mq.FeatureAdmin] = mq.Support{Level: mq.Unsupported}
	if p.Admin != nil {
		note(mq.FeatureAdmin, mq.Conditional, mq.ScopeTopic,
			"AMQP 1.0 has no topology API; this profile supplies one of its own, and Client.Admin reaches it")
	}
	if p.Durable.Supported {
		note(mq.FeatureDurableConfirm, mq.Native, mq.ScopeTopic, p.Durable.Note)
	} else {
		note(mq.FeatureDurableConfirm, mq.Unsupported, mq.ScopeNone,
			"this profile defines no durable confirmation boundary, so a publisher that asks for ConfirmDurable is refused when its sink is opened ")
	}
	if p.Delay.Supported() {
		note(mq.FeatureScheduledPublish, mq.Conditional, mq.ScopeTopic,
			"PublishOptions.Delay and DeliverAt travel as the "+p.Delay.Slot.phrase()+" "+quoteFact(p.Delay.Key)+
				delayMaxNote(p.Delay.Max))
	}
	if p.NackDelay.carries() {
		note(mq.FeatureDelayedRedelivery, mq.Conditional, mq.ScopeTopic,
			"NackOptions.Delay travels as the `modified` outcome with the "+p.NackDelay.Slot.phrase()+" "+quoteFact(p.NackDelay.Key)+
				" merged in"+delayMaxNote(p.NackDelay.Max))
	} else if p.NackDelay.Supported {
		note(mq.FeatureDelayedRedelivery, mq.Conditional, mq.ScopeNone,
			"the `modified` outcome makes the message available again at once; this broker cannot hold a redelivery back, so a non-zero NackOptions.Delay is refused with ErrUnsupported and settles nothing")
	}
	if p.Dedup.Supported() {
		dedupNote := "PublishOptions.DedupKey travels as the " + p.Dedup.Slot.phrase() + " " + quoteFact(p.Dedup.Key) +
			"; it de-duplicates PUBLICATIONS inside whatever window the broker keeps and never de-duplicates deliveries"
		note(mq.FeatureDeduplication, mq.Conditional, mq.ScopeTopic, dedupNote)
		note(mq.FeatureIdempotentPublish, mq.Conditional, mq.ScopeTopic, dedupNote)
	}
	_ = f
	return t
}

// delayMaxNote states a delay ceiling in a capability note, or says nothing
// when the profile does not know one. The specification forbids reading an unadvertised
// limit as zero, and a note that said "up to 0s" would do exactly that.
func delayMaxNote(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return ", up to " + d.String()
}

// coreLimits are the limits this core can read for itself.
//
// The message ceiling is the sender link's declared maximum, which the peer
// sends on attach and is the only size limit AMQP 1.0 has. The specification forbids reading
// an unadvertised limit as zero, so a peer that declared none leaves it unknown
// rather than guessing — and never reports zero, which would refuse every
// message.
func coreLimits(p Profile, f Facts) mq.Limits {
	var lim mq.Limits
	if f.MaxMessageBytes > 0 && f.MaxMessageBytes <= 1<<62 {
		lim.MaxMessageBytes = mq.Limit[int64]{Value: int64(f.MaxMessageBytes), Known: true}
	}
	if p.Delay.Supported() && p.Delay.Max > 0 {
		lim.MaxDelay = mq.Limit[time.Duration]{Value: p.Delay.Max, Known: true}
	}
	// MaxLease is deliberately left unknown rather than set to zero: AMQP 1.0
	// grants no lease at all, and an advertised limit of zero would read as
	// "every lease expires immediately".
	return lim
}

// checkClaim refuses a profile claim the core cannot honour.
//
// The three conditional ceilings are here rather than in [CoreCeiling] because
// each depends on the profile itself: the core has the code and the profile has
// to supply the one value it needs, so "may this be claimed" is a question about
// this profile and not about the feature.
func checkClaim(p Profile, c Claim) error {
	if !c.Feature.Valid() {
		return errUnknownFeature
	}
	if !c.Support.Level.Valid() || !c.Support.Scope.Valid() {
		return errBadClaim
	}
	if c.Support.Level.Rank() > CoreCeiling(c.Feature).Rank() {
		return errOverclaim
	}
	if c.Support.Level == mq.Unsupported {
		// Withdrawing is always allowed: a profile that knows its broker
		// lacks something the core can do is the narrowing half of the seam.
		return nil
	}
	switch c.Feature {
	case mq.FeatureAdmin:
		if p.Admin == nil {
			return errClaimNeedsAdmin
		}
	case mq.FeatureDurableConfirm:
		if !p.Durable.Supported {
			return errClaimNeedsDurable
		}
	case mq.FeatureScheduledPublish:
		if !p.Delay.Supported() {
			return errClaimNeedsDelay
		}
	case mq.FeatureDelayedRedelivery:
		if !p.NackDelay.Supported {
			return errClaimNeedsNackDelay
		}
	case mq.FeatureDeduplication, mq.FeatureIdempotentPublish:
		if !p.Dedup.Supported() {
			return errClaimNeedsDedup
		}
	default:
		// Every other feature needs nothing of the profile beyond staying
		// inside [CoreCeiling], which was checked above. The default is
		// deliberate rather than an omission: a feature added later
		// gets the ceiling's answer, which is Unsupported until somebody
		// decides what this core does about it.
	}
	return nil
}

// DescribeCapabilities renders a capability table one feature per line, sorted
// by the inventory's own order.
//
// It is exported because a capability claim that cannot be read is a claim on
// trust: the golden test in this module compares this rendering against a
// committed file, so a change to any claim is a diff a reviewer sees rather than
// a cell in a generated matrix nobody regenerated.
func DescribeCapabilities(caps mq.Capabilities) string {
	var b strings.Builder
	b.Grow(4 << 10)
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		s := caps.Support(f)
		b.WriteString(f.String())
		b.WriteByte('\t')
		b.WriteString(s.Level.String())
		b.WriteByte('\t')
		b.WriteString(s.Scope.String())
		b.WriteByte('\t')
		b.WriteString(s.Note)
		b.WriteByte('\n')
	}
	// The limits follow in LimitKey order, which is the inventory's own and
	// therefore already deterministic: a golden file does not need a sort.
	for i := range mq.LimitKeyCount {
		k := mq.LimitKey(i)
		v := caps.Limit(k)
		b.WriteString("limit\t")
		b.WriteString(k.String())
		b.WriteByte('\t')
		if !v.Known {
			b.WriteString("unknown\n")
			continue
		}
		b.WriteString(strconv.FormatInt(v.Value, 10))
		b.WriteByte('\n')
	}
	return b.String()
}
