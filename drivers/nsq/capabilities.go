package nsq

import (
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// capabilities builds the driver table once, at open.
//
// Every claim here is either something NSQ does itself or something stated as
// unsupported. Nothing is emulated: the envelope is the one emulation this
// driver performs and it is a metadata mode rather than a capability, so no
// feature is advertised on the strength of it.
//
// The three that need saying out loud:
//
//   - NO ORDERING SCOPE AT ALL. The specification says "ordering never claimed" and that is
//     literal: nsqd makes no promise about the order in which a channel's
//     messages are delivered, not even for one producer, one consumer and no
//     failures — an in-memory queue and a disk-backed queue are drained
//     independently, and a requeue puts a message back wherever there is room.
//     A Conditional claim with a note would be the wrong answer here, because
//     there is no condition under which it holds.
//   - NO NATIVE HEADERS, and none of the three header features. It is the
//     whole reason this driver exists in the plan: the promise is kept by
//     the envelope and by nothing else.
//   - NO DEAD-LETTER ROUTE. FeatureDeadLetter means the DRIVER knows where a
//     dead letter goes and can get it there in one native settlement, which
//     nsqd has no verb for. The specification's "ED" for this broker is the CORE's
//     publish-then-acknowledge emulation, reached with mq.WithDeadLetter, and
//     that needs no capability from the driver.
//
// The three header features are the exception to "nothing is emulated", and
// the exception is what the specification asks for rather than a hole in it. When
// [Config.Metadata] envelopes — which is the header-less-backend emulation,
// switched on deliberately by the deployment — ordered, duplicate-preserving,
// binary header values DO travel, inside iqmq's portable envelope. The specification's
// levels have a word for that and it is EmulatedDurable: the headers are as
// durable as the message, and the cost is interoperability rather than
// durability. Advertising them as unsupported while the envelope carried them
// would be the mirror image of the silent emulation the specification forbids — a caller
// planning against the table would be told a message cannot carry a binary
// header when on this client it can.
//
// cfg is the RESOLVED configuration. Every limit here mirrors an nsqd flag
// rather than a value nsqd reports: nsqd publishes none of its limits over
// either protocol.
func capabilities(cfg Config) (mq.Capabilities, error) {
	b := driverkit.NewCapabilities()
	enveloped := cfg.Metadata == mq.MetadataPortableEnvelope || cfg.Metadata == mq.MetadataAdaptive

	// What nsqd does itself.
	b.Native(mq.FeatureAtLeastOnce, mq.ScopeTopic)
	b.Native(mq.FeatureExplicitAck, mq.ScopeTopic)
	b.Native(mq.FeatureNack, mq.ScopeTopic)
	b.Native(mq.FeatureDelayedRedelivery, mq.ScopeTopic)
	b.Native(mq.FeatureScheduledPublish, mq.ScopeTopic)
	b.Native(mq.FeatureExtendLease, mq.ScopeTopic)
	b.Native(mq.FeatureConsumerGroups, mq.ScopeGroup)
	b.Native(mq.FeatureDurableSubscriptions, mq.ScopeGroup)
	b.Native(mq.FeatureFanOut, mq.ScopeTopic)
	b.Native(mq.FeatureBatchPublish, mq.ScopeTopic)
	b.Native(mq.FeatureBatchReceive, mq.ScopeTopic)
	b.Native(mq.FeatureAdmin, mq.ScopeTopic)

	b.Support(mq.FeaturePublisherConfirm, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "PUB, MPUB and DPUB are answered with OK once the message is in the topic's queue; that queue is in memory up to nsqd's --mem-queue-size, so OK is an acceptance and never a durable boundary",
	})

	b.Conditional(mq.FeatureDurableStorage, mq.ScopeTopic,
		"nsqd keeps a topic's queue in memory up to --mem-queue-size and spills the rest to disk, so a message survives a consumer's loss but not nsqd's unless --mem-queue-size is 0")

	const envelopeNote = "carried in iqmq's portable binary envelope, which Config.Metadata switched on: the emulation for a header-less backend. The headers are as durable as the message and the cost is interoperability — a non-iqmq consumer reads the envelope rather than the body"
	if enveloped {
		for _, f := range [...]mq.Feature{
			mq.FeatureNativeHeaders, mq.FeatureBinaryHeaders, mq.FeatureDuplicateHeaders,
		} {
			b.EmulatedDurable(f, mq.ScopeTopic, envelopeNote)
		}
	} else {
		for _, f := range [...]mq.Feature{
			mq.FeatureNativeHeaders, mq.FeatureBinaryHeaders, mq.FeatureDuplicateHeaders,
		} {
			b.Unsupported(f)
		}
	}

	// Everything else, stated rather than omitted. CapabilityBuilder.Missing
	// is what turns "the driver decided it does not have this" into
	// something a test can tell apart from "nobody looked".
	for _, f := range [...]mq.Feature{
		mq.FeatureAtMostOnce,
		mq.FeatureExactlyOnceDelivery,
		mq.FeatureMessageTTL,
		mq.FeaturePriority,
		mq.FeatureDeadLetter,
		mq.FeatureReplay,
		mq.FeatureSeekByTime,
		mq.FeatureSeekBySequence,
		mq.FeatureOrderedByKey,
		mq.FeatureOrderedByPartition,
		mq.FeatureOrderedGlobally,
		mq.FeatureAtomicBatchPublish,
		mq.FeatureBatchSettle,
		mq.FeatureIdempotentPublish,
		mq.FeatureDeduplication,
		mq.FeatureTransactions,
		mq.FeatureServerFilter,
		mq.FeatureWildcardSubscribe,
		mq.FeatureRequestReply,
		mq.FeatureDurableConfirm,
		mq.FeatureNativeTracing,
		mq.FeatureReceipts,
	} {
		b.Unsupported(f)
	}

	var lim mq.Limits
	if cfg.MaxMessageBytes > 0 {
		lim.MaxMessageBytes = mq.Limit[int64]{Value: cfg.MaxMessageBytes, Known: true}
	}
	if cfg.MaxBodyBytes > 0 {
		lim.MaxBatchBytes = mq.Limit[int64]{Value: cfg.MaxBodyBytes, Known: true}
	}
	if cfg.MaxRequeueDelay > 0 {
		lim.MaxDelay = mq.Limit[time.Duration]{Value: cfg.MaxRequeueDelay, Known: true}
	}
	if cfg.MaxAckDeadline > 0 {
		lim.MaxLease = mq.Limit[time.Duration]{Value: cfg.MaxAckDeadline, Known: true}
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

var errIncompleteTable = errcode.New(errcode.InvalidConfig,
	"iqmq/nsq: the capability table does not mention every feature of the inventory")
