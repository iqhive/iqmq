package kafka

import (
	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/driverkit/nack"
)

// capabilities builds the driver table once, at open, from the
// configuration and from what the cluster said.
//
// Two things about the table are worth reading before the list. First, the specification
// makes the absent half as load-bearing as the present half: a caller reads it
// to decide whether it may rely on a behaviour, and the conformance kit proves
// both directions, so every feature of the inventory is mentioned here even
// when the answer is no. Second, three claims depend on the CONFIGURATION
// rather than on the broker — idempotent publication, the dead-letter route
// and delayed redelivery — because on Kafka all three are things iqmq does and
// not things the cluster offers, and a table that claimed them unconditionally
// would be claiming behaviour the caller switched off.
//
// maxMessageBytes is the cluster's own message.max.bytes when the driver could
// read it, and zero when it could not: the specification forbids reading an unadvertised
// limit as zero, so an honest "we do not know" is a Limit whose Known is
// false.
func capabilities(cfg Config, maxMessageBytes int64) (mq.Capabilities, error) {
	b := driverkit.NewCapabilities()

	// The log itself.
	b.Native(mq.FeatureDurableStorage, mq.ScopeTopic)
	b.Native(mq.FeatureAtLeastOnce, mq.ScopeGroup)
	b.Native(mq.FeatureExplicitAck, mq.ScopeGroup)
	b.Native(mq.FeatureConsumerGroups, mq.ScopeGroup)
	b.Native(mq.FeatureDurableSubscriptions, mq.ScopeGroup)
	b.Native(mq.FeatureFanOut, mq.ScopeTopic)
	b.Native(mq.FeatureReplay, mq.ScopeTopic)
	b.Native(mq.FeatureSeekByTime, mq.ScopeTopic)
	b.Native(mq.FeatureSeekBySequence, mq.ScopeTopic)
	b.Native(mq.FeatureNativeHeaders, mq.ScopeTopic)
	b.Native(mq.FeatureBinaryHeaders, mq.ScopeTopic)
	b.Native(mq.FeatureDuplicateHeaders, mq.ScopeTopic)
	b.Native(mq.FeatureBatchPublish, mq.ScopeTopic)
	b.Native(mq.FeatureBatchReceive, mq.ScopeTopic)
	b.Native(mq.FeatureAdmin, mq.ScopeTopic)
	b.Native(mq.FeatureReceipts, mq.ScopeTopic)

	// A commit is one round trip for every delivery it covers, because the
	// acknowledgement is cumulative: acknowledging the hundredth record
	// acknowledges the ninety-nine below it. That is batched settlement by
	// construction rather than by coalescing.
	b.Support(mq.FeatureBatchSettle, mq.Support{
		Level: mq.Native, Scope: mq.ScopeGroup,
		Note: "settlement is a cumulative OffsetCommit, so a batch of acknowledgements is one round trip whatever its size",
	})

	// Both confirmation boundaries, and they are the same boundary. acks=all
	// means every in-sync replica has the record in its log, which is
	// already a durable answer, so ConfirmDurable costs no extra round trip
	// and is not refused. Saying so is what stops a caller reaching for
	// something stronger that does not exist.
	const confirmNote = "acks=all: the promise fires when every in-sync replica has the record in its log, which is why ConfirmAccepted and ConfirmDurable are the same boundary here"
	b.Support(mq.FeaturePublisherConfirm, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic, Note: confirmNote})
	b.Support(mq.FeatureDurableConfirm, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic, Note: confirmNote})

	// Ordering. A partition is the unit, and a key maps to a partition by
	// murmur2, which is what makes per-key order hold across a mixed-client
	// fleet. Global order across a topic holds only for a topic with one
	// partition, which is a topology the deployment chooses and this driver
	// cannot know — so it is conditional, with the condition stated.
	b.Native(mq.FeatureOrderedByPartition, mq.ScopePartition)
	b.Support(mq.FeatureOrderedByKey, mq.Support{
		Level: mq.Native, Scope: mq.ScopeKey,
		Note: "the key is hashed with Kafka's own murmur2 partitioner, so one key's records share a partition and a partition is delivered in order",
	})
	b.Conditional(mq.FeatureOrderedGlobally, mq.ScopeTopic,
		"only for a topic with a single partition: Kafka orders within a partition and never across partitions")

	// What iqmq does on top of Kafka, and only when it is configured to.
	// The dead-letter route is advertised unconditionally, and the note
	// carries the condition.
	//
	// It is tempting to claim it only when Config.NackConfig.DeadLetterTopic
	// is set, and that is wrong twice over. The specification makes a capability table
	// CONSTANT for the life of a client, and a claim that depends on a
	// field the caller may set per client is still constant per client —
	// but the behaviour a caller has to plan for is not "the feature
	// appears and disappears", it is "Reject routes when a destination is
	// configured and is REFUSED when one is not". That is one behaviour,
	// and it is what the note says. The conformance kit made the same point
	// from the other side: its two scenarios for the claim shape the
	// topology differently, so a table that answered differently to each
	// of them left the claim proved in neither direction.
	b.EmulatedDurable(mq.FeatureDeadLetter, mq.ScopeTopic,
		"the driver publishes the record to Config.NackConfig.DeadLetterTopic with the provenance headers and commits the original offset only after that publication is confirmed, so the route is a durable Kafka topic and survives a restart; a Reject on a client with no destination configured is refused with ErrUnsupported rather than destroying the message")
	if cfg.Nack.Resolve() == nack.BlockPartition || cfg.Nack.Resolve() == nack.Custom {
		b.EmulatedVolatile(mq.FeatureDelayedRedelivery, mq.ScopeGroup,
			"Kafka has no delayed redelivery: the block-partition nack strategy holds the record in this process with its offset uncommitted and redelivers it after the delay, so the delay is lost if the process dies and the record is redelivered immediately by whoever takes the partition")
	} else {
		b.Unsupported(mq.FeatureDelayedRedelivery)
	}
	// A negative settlement always does something — that is what the
	// strategy is for — so Nack itself is offered whatever the strategy.
	b.EmulatedVolatile(mq.FeatureNack, mq.ScopeGroup,
		"Kafka has no per-record negative acknowledgement: what a Nack does is Config.Nack's strategy, which by default holds the record in process and leaves its offset uncommitted")

	if cfg.DisableIdempotency {
		b.Unsupported(mq.FeatureIdempotentPublish)
	} else {
		b.Support(mq.FeatureIdempotentPublish, mq.Support{
			Level: mq.Native, Scope: mq.ScopeTopic,
			Note: "the producer is idempotent, so its OWN retries of a record are de-duplicated by the broker within a producer session; it does not de-duplicate two publications a caller made",
		})
	}
	if cfg.TransactionalID != "" {
		b.Conditional(mq.FeatureTransactions, mq.ScopeTopic,
			"with Config.TransactionalID set and the transaction driven through a separate transactions helper; the portable API has no transaction verb, so a caller reaches it through that helper and the native client")
	} else {
		b.Unsupported(mq.FeatureTransactions)
	}

	// Everything else, stated rather than omitted. CapabilityBuilder.Missing
	// is what turns "the driver decided it does not have this" into
	// something a test can tell apart from "nobody looked".
	for _, f := range [...]mq.Feature{
		// Kafka is at-least-once. At-most-once would mean committing
		// before the handler runs, which this driver does not offer:
		// a caller who wants it acknowledges on receipt itself.
		mq.FeatureAtMostOnce,
		// The idempotent producer de-duplicates a producer's retries, not
		// an application's DedupKey, and nothing here makes a delivery
		// exactly-once.
		mq.FeatureExactlyOnceDelivery,
		mq.FeatureDeduplication,
		// Kafka has topic retention and no per-message TTL, no scheduled
		// publication, and no priority: a partition is a queue, not a heap.
		mq.FeatureScheduledPublish,
		mq.FeatureMessageTTL,
		mq.FeaturePriority,
		// A produce request is atomic per partition, and a batch that
		// spans partitions is not — not without a transaction, which is
		// FeatureTransactions above.
		mq.FeatureAtomicBatchPublish,
		// There is no per-record lease to extend. A Kafka consumer's
		// deadline is the group's own max poll interval, which belongs to
		// the member and not to a delivery.
		mq.FeatureExtendLease,
		// franz-go can subscribe to a regular expression and this driver
		// does not wire it (see docs/drivers/kafka.md); a broker-side
		// filter does not exist at all.
		mq.FeatureWildcardSubscribe,
		mq.FeatureServerFilter,
		// Kafka has no request/reply verb and no native tracing.
		mq.FeatureRequestReply,
		mq.FeatureNativeTracing,
	} {
		b.Unsupported(f)
	}

	var lim mq.Limits
	if maxMessageBytes > 0 {
		lim.MaxMessageBytes = mq.Limit[int64]{Value: maxMessageBytes, Known: true}
	}
	if maxMessageBytes > 0 {
		// One produce request carries many records, and the broker bounds
		// the batch by the same message.max.bytes it bounds a record by.
		lim.MaxBatchBytes = mq.Limit[int64]{Value: maxMessageBytes, Known: true}
	}
	// MaxHeaderCount, MaxHeaderBytes, MaxBatchMessages, MaxDelay and
	// MaxLease are deliberately left unknown: Kafka bounds none of them on
	// its own -- headers and batches are bounded by the message size, there
	// is no delay and there is no per-record lease -- and the specification forbids
	// reading an unadvertised limit as zero.
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
