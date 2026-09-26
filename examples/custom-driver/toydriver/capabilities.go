package toydriver

import (
	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// buildCapabilities is the table for this broker, built once at
// construction because the specification makes capabilities constant per client.
//
// Every one of the features is mentioned, including the ones this broker
// does not have, and [CapabilityBuilder.Missing] is asserted empty by the
// driver's own test. The difference that makes is the difference between a
// feature the author decided this driver does not have and one nobody looked
// at — and the conformance kit holds the second kind to the same standard as
// the first: an unclaimed feature whose probe WORKS fails the run, because a
// caller would then be relying on behaviour the driver may remove.
//
// The builder is used rather than mq.Capabilities.Set because it refuses a
// dishonest table: the specification requires a conditional support to state its condition
// and an emulation to say what it is, and an empty note is a construction
// failure here instead of an empty cell in the generated matrix.
func buildCapabilities(cfg Config) (mq.Capabilities, error) {
	return newCapabilityBuilder(cfg).Build()
}

// newCapabilityBuilder fills the builder. It is separate from
// buildCapabilities so that this package's own test can ask
// [driverkit.CapabilityBuilder.Missing] which features the table never
// mentioned.
func newCapabilityBuilder(cfg Config) *driverkit.CapabilityBuilder {
	b := driverkit.NewCapabilities()

	// ---- what this broker really does -----------------------------------
	b.Native(mq.FeatureAtLeastOnce, mq.ScopeTopic)
	b.Native(mq.FeatureExplicitAck, mq.ScopeTopic)
	b.Native(mq.FeatureNack, mq.ScopeTopic)
	b.Native(mq.FeatureExtendLease, mq.ScopeTopic)
	b.Native(mq.FeatureFanOut, mq.ScopeTopic)
	b.Native(mq.FeatureConsumerGroups, mq.ScopeGroup)
	// A publication is in every subscription's queue when Publish returns,
	// which is the whole of the confirmation an in-process broker can offer
	b.Native(mq.FeaturePublisherConfirm, mq.ScopeTopic)
	// One Fetch fills as many slots as the prefetch allows.
	b.Native(mq.FeatureBatchReceive, mq.ScopeTopic)
	// Headers are stored, but only the ones this broker can represent:
	// single-valued and UTF-8, like an SQS message attribute or a Pub/Sub
	// attribute. The other two header features are unsupported below, and the strict mode is what refuses a message that needs them.
	b.Native(mq.FeatureNativeHeaders, mq.ScopeTopic)
	// One FIFO queue per subscription, so within a subscription the
	// delivery order is the publication order — which is the strongest of the three orderings and implies the other two. All three are
	// claimed and not just the widest: the table describes what a CALLER
	// may rely on, so a guarantee that falls out of the implementation is
	// claimed as strongly as one that was built, and the conformance kit's
	// refusal scenario for an unclaimed ordering would fail against a FIFO
	// queue anyway. It is not conditional on a partition
	// count, because this broker has no partitions to count.
	b.Native(mq.FeatureOrderedGlobally, mq.ScopeTopic)
	b.Native(mq.FeatureOrderedByPartition, mq.ScopePartition)
	b.Native(mq.FeatureOrderedByKey, mq.ScopeKey)

	// ---- what it does, but not durably ----------------------------------
	b.EmulatedVolatile(mq.FeatureDurableSubscriptions, mq.ScopeGroup,
		"a subscription outlives its consumers but not the process; nothing is written anywhere")

	// ---- what it does, under a condition --------------------------------
	b.Conditional(mq.FeatureDeadLetter, mq.ScopeTopic,
		"TopicSpec.DeadLetterTopic, which must name a topic this broker also creates; an explicit Reject moves the message and then settles it")
	// A batch publication is iqmq's own loop over Sink.Publish:
	// this driver implements no BatchSink, and the core supplies the loop.
	// Advertising it Native would claim a native batch API that does not
	// exist, and advertising nothing would be a lie in the other direction —
	// PublisherHandle.PublishBatch works, and the kit's refusal scenario
	// would catch a driver that said it did not.
	b.EmulatedVolatile(mq.FeatureBatchPublish, mq.ScopeTopic,
		"iqmq's own loop over Sink.Publish; this broker has no native batch admission, so a batch is not atomic")

	// ---- what it does not do -------------------------------------------
	// Naming them is not decoration. Missing() below is asserted empty by
	// this driver's test, so a feature added to the inventory after this
	// was written makes the test fail rather than silently becoming an
	// unconsidered claim of "no".
	for _, f := range []mq.Feature{
		mq.FeatureDurableStorage,
		mq.FeatureAtMostOnce,
		mq.FeatureExactlyOnceDelivery,
		mq.FeatureDelayedRedelivery,
		mq.FeatureScheduledPublish,
		mq.FeatureMessageTTL,
		mq.FeaturePriority,
		mq.FeatureReplay,
		mq.FeatureSeekByTime,
		mq.FeatureSeekBySequence,
		mq.FeatureBinaryHeaders,
		mq.FeatureDuplicateHeaders,
		mq.FeatureAtomicBatchPublish,
		mq.FeatureBatchSettle,
		mq.FeatureIdempotentPublish,
		mq.FeatureDeduplication,
		mq.FeatureTransactions,
		mq.FeatureServerFilter,
		mq.FeatureWildcardSubscribe,
		mq.FeatureAdmin,
		mq.FeatureRequestReply,
		mq.FeatureDurableConfirm,
		mq.FeatureNativeTracing,
		mq.FeatureReceipts,
	} {
		b.Unsupported(f)
	}

	b.Limits(limitsOf(cfg))
	return b
}

// limitsOf is the limit half only what this broker really knows.
//
// The advertised maximum message size is enforced by the CORE, before a
// publication reaches the sink, which is worth knowing when writing a driver:
// a driver that also checked it would be the second owner of one rule, and a
// driver that advertises a limit it does not mean makes the core refuse
// messages the broker would have taken.
func limitsOf(cfg Config) mq.Limits {
	maxBytes := int64(cfg.MaxMessageBytes)
	if maxBytes <= 0 {
		maxBytes = DefaultMaxMessageBytes
	}
	lim := mq.Limits{
		MaxMessageBytes: mq.Limit[int64]{Value: maxBytes, Known: true},
		// The header COUNT is the one header bound the specification has a field for, so
		// it is advertised and the core refuses before any I/O. The length
		// of one key and of one value have no field, and checkHeaders in
		// sink.go is where the driver checks them.
		MaxHeaderCount: mq.Limit[int]{Value: envelopeLimits.MaxHeaders, Known: true},
	}
	// MaxLease is deliberately NOT advertised, and this is the distinction
	// a driver author is most likely to get wrong.
	//
	// MaxLease is the longest acknowledgement deadline the broker
	// GRANTS. `Config.AckDeadline` is the lease a delivery STARTS with --
	// a default -- and Limits has no field for that. This broker has
	// no maximum: `settler.Extend` sets `rec.lease = now + by` for any by.
	//
	// Advertising the default as the ceiling is not a harmless
	// approximation, and the conformance kit shows why twice over. It asks
	// a driver that advertises the limit to REFUSE an extension past it,
	// and it caught both this driver and `memory` granting `Extend(11s)`
	// against a claim of ten seconds. It also extends by `30s` against an
	// `AckDeadline` of `10s` in its own positive arm and expects that to
	// succeed -- so a driver whose MaxLease equalled its AckDeadline could
	// not pass either direction. The two are different numbers about
	// different things.
	//
	// A driver over a broker that DOES cap -- SQS at twelve hours, Pub/Sub
	// at ten minutes, nsqd at its `--max-msg-timeout` -- advertises that
	// number here and refuses past it, which is what those three do.
	return lim
}
