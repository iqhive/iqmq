package nats

import (
	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
)

// capabilities builds the driver table once, at open, from what the
// connected server said.
//
// Every claim below is a claim about NATS **Core**. The long unsupported list
// is not laziness: it is the honest shape of an at-most-once broker that stores
// nothing, and the specification makes the absent half of a table as load-bearing as the
// present half — a caller reads it to decide whether it may rely on a
// behaviour, and the conformance kit proves both directions.
//
// maxPayload is the server's own max_payload from its INFO, which is the one
// limit this driver really knows. Everything else is left unadvertised rather
// than guessed: the specification forbids reading an unadvertised limit as zero, so an
// honest "we do not know" is a Limit whose Known is false.
func capabilities(maxPayload int64) (mq.Capabilities, error) {
	b := driverkit.NewCapabilities()

	// What the broker does itself.
	b.Native(mq.FeatureAtMostOnce, mq.ScopeTopic)
	b.Native(mq.FeatureConsumerGroups, mq.ScopeGroup)
	b.Native(mq.FeatureFanOut, mq.ScopeTopic)
	b.Native(mq.FeatureNativeHeaders, mq.ScopeTopic)
	b.Native(mq.FeatureDuplicateHeaders, mq.ScopeTopic)
	b.Native(mq.FeatureBatchReceive, mq.ScopeTopic)
	b.Native(mq.FeatureWildcardSubscribe, mq.ScopeTopic)
	b.Native(mq.FeatureRequestReply, mq.ScopeTopic)

	// A publication is confirmed at the socket and never beyond it. The note
	// is not required for a native claim and is here anyway, because
	// "publisher confirm" means something much weaker on this broker than a
	// caller coming from Kafka or JetStream expects.
	b.Support(mq.FeaturePublisherConfirm, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "ConfirmAccepted round-trips PING/PONG, so the server has parsed the publication; it stores nothing, so nothing is durable",
	})

	// A batch is the driver's own: every message is written and one flush
	// confirms them all, which saves the round trips but is not atomic and
	// survives nothing. The specification ranks that below conditional support, which is
	// exactly right here.
	b.EmulatedVolatile(mq.FeatureBatchPublish, mq.ScopeTopic,
		"the driver writes every message of the batch and confirms them with a single flush; NATS Core has no batch verb, so a batch is not atomic")

	// Ordering. NATS Core delivers a subject's messages to one subscriber in
	// the order one publisher connection wrote them, and that is the whole
	// of it: a queue group with more than one member interleaves across its
	// members, and two publisher connections are not ordered against each
	// other. The three ordering features differ only in scope, and on a
	// broker with no partitions and no keys they all reduce to "per subject",
	// which is what the notes say.
	const orderNote = "one publisher connection to one subject delivered to a single subscriber; a queue group with more than one member interleaves, and two connections are not ordered against each other"
	b.Conditional(mq.FeatureOrderedGlobally, mq.ScopeTopic, orderNote)
	b.Conditional(mq.FeatureOrderedByKey, mq.ScopeKey,
		"NATS Core has no message key, so a key orders only in so far as its messages share a subject: "+orderNote)
	b.Conditional(mq.FeatureOrderedByPartition, mq.ScopePartition,
		"NATS Core has no partitions; a subject is the only partition there is, so: "+orderNote)

	// Everything else, stated rather than omitted. CapabilityBuilder.Missing
	// is what turns "the driver decided it does not have this" into
	// something a test can tell apart from "nobody looked".
	for _, f := range [...]mq.Feature{
		mq.FeatureDurableStorage,
		mq.FeatureAtLeastOnce,
		mq.FeatureExactlyOnceDelivery,
		mq.FeatureExplicitAck,
		mq.FeatureNack,
		mq.FeatureDelayedRedelivery,
		mq.FeatureScheduledPublish,
		mq.FeatureMessageTTL,
		mq.FeaturePriority,
		mq.FeatureDeadLetter,
		mq.FeatureDurableSubscriptions,
		mq.FeatureReplay,
		mq.FeatureSeekByTime,
		mq.FeatureSeekBySequence,
		mq.FeatureBinaryHeaders,
		mq.FeatureAtomicBatchPublish,
		mq.FeatureBatchSettle,
		mq.FeatureExtendLease,
		mq.FeatureIdempotentPublish,
		mq.FeatureDeduplication,
		mq.FeatureTransactions,
		mq.FeatureServerFilter,
		mq.FeatureAdmin,
		mq.FeatureDurableConfirm,
		mq.FeatureNativeTracing,
		mq.FeatureReceipts,
	} {
		b.Unsupported(f)
	}

	var lim mq.Limits
	if maxPayload > 0 {
		lim.MaxMessageBytes = mq.Limit[int64]{Value: maxPayload, Known: true}
	}
	b.Limits(lim)

	if missing := b.Missing(); len(missing) > 0 {
		// A feature nobody mentioned renders as unsupported, which is the
		// safe answer — but it is also indistinguishable from a feature the
		// driver forgot, and TestCapabilitiesConsidersEveryFeature is what
		// keeps that from happening quietly as the inventory grows.
		return mq.Capabilities{}, errIncompleteTable
	}
	return b.Build()
}
