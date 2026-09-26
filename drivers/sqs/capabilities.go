package sqs

import (
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// capabilities builds the driver table once, at open, from the
// configuration.
//
// Two things about the table are worth reading before the list. First, the specification
// makes the absent half as load-bearing as the present half: a caller reads it
// to decide whether it may rely on a behaviour, and the conformance kit proves
// both directions, so every feature of the inventory is mentioned here even
// when the answer is no. Second, SEVEN claims differ between the two profiles,
// and that is the whole reason [Config.FIFO] is reported as
// DriverInfo.Profile rather than hidden in a flag: ordering by key,
// deduplication, idempotent publication and scheduled publication are FIFO's,
// and a caller who read "sqs" and got one profile's table for the other would
// be reading a capability table that belongs to a different queue.
func capabilities(cfg Config) (mq.Capabilities, error) {
	b := driverkit.NewCapabilities()

	// The queue itself. A message is stored redundantly across availability
	// zones before SendMessage answers, which is what makes both
	// confirmation boundaries the same boundary here.
	b.Native(mq.FeatureDurableStorage, mq.ScopeTopic)
	b.Native(mq.FeatureAtLeastOnce, mq.ScopeTopic)
	b.Native(mq.FeatureExplicitAck, mq.ScopeTopic)
	b.Native(mq.FeatureDurableSubscriptions, mq.ScopeTopic)
	b.Native(mq.FeatureNativeHeaders, mq.ScopeTopic)
	b.Native(mq.FeatureBatchPublish, mq.ScopeTopic)
	b.Native(mq.FeatureAdmin, mq.ScopeTopic)
	b.Native(mq.FeatureReceipts, mq.ScopeTopic)

	const confirmNote = "the SendMessage or SendMessageBatch response: the service has stored the message redundantly across availability zones before it answers, so ConfirmAccepted and ConfirmDurable are the same boundary and neither costs an extra round trip"
	b.Support(mq.FeaturePublisherConfirm, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic, Note: confirmNote})
	b.Support(mq.FeatureDurableConfirm, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic, Note: confirmNote})

	// A queue is a competing-consumer group by construction: every consumer
	// of a queue takes from the same messages, and the visibility timeout is
	// what keeps two of them from taking the same one. SourceConfig.
	// Subscription is therefore ignored, which is stated rather than hidden.
	b.Support(mq.FeatureConsumerGroups, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "a queue IS the group: every consumer of one topic competes for its messages whatever SourceConfig.Subscription says, because a queue has exactly one set of messages. Two independent subscriptions of one topic need drivers/snssqs, where the topic is an SNS topic and the subscription is a queue",
	})

	// Binary header values are carried in the service's own Binary
	// attribute type, so they need no envelope. A REPEATED key cannot be:
	// an attribute set is a map.
	b.Support(mq.FeatureBinaryHeaders, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "a message attribute of the service's Binary type carries an arbitrary byte value; the attribute NAME still has to be a legal attribute name, and a header whose key is not takes the envelope under MetadataAdaptive",
	})

	// Settlement. A delete is a round trip the service charges for, so the
	// batched surface is the one that earns its keep, and the coalescing
	// strategy is.
	b.Support(mq.FeatureBatchSettle, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "DeleteMessageBatch and ChangeMessageVisibilityBatch take ten entries each, every entry with its own outcome; Config.Settle is the flush policy and decides when a coalesced batch leaves",
	})
	b.Support(mq.FeatureNack, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "ChangeMessageVisibility with a timeout of zero makes the message immediately visible again, which is a native negative acknowledgement and increments ApproximateReceiveCount",
	})
	b.Support(mq.FeatureDelayedRedelivery, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "ChangeMessageVisibility with the delay, bounded by the service's twelve-hour visibility timeout; the delay survives a process death, because it is the queue that holds it",
	})
	b.Support(mq.FeatureExtendLease, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "ChangeMessageVisibility moves the window, bounded by the service's twelve hours measured from the call and not from the delivery",
	})

	// The dead-letter route is TWO routes, and the note says both because a
	// caller has to plan for both. It is advertised unconditionally with the
	// condition in the note, which is the lesson: the kit's
	// two scenarios for one claim shape the topology differently, so a table
	// that answered differently to each of them proves the claim in neither
	// direction.
	b.EmulatedDurable(mq.FeatureDeadLetter, mq.ScopeTopic,
		"two routes. The queue's own redrive policy moves a message the service has delivered Config.MaxReceiveCount times, which is native and needs no settlement at all; an explicit Reject sends the message to Config.DeadLetterQueue with its provenance and deletes the original only once that send is confirmed, which is durable because the destination is an SQS queue. A Reject on a client with no DeadLetterQueue configured is refused with ErrUnsupported rather than destroying the message")

	// What the profile decides.
	if cfg.FIFO {
		// A FIFO queue holds a group's later messages while one is in
		// flight, so one ReceiveMessage returns at most one message per
		// GROUP: a batch receive is as wide as the number of groups with
		// nothing in flight, and on a queue whose messages share one group
		// it returns one at a time. That is a real condition rather than an
		// absence — ReceiveMessage still takes ten — and the specification requires the
		// condition to be stated.
		b.Conditional(mq.FeatureBatchReceive, mq.ScopeTopic,
			"one ReceiveMessage returns at most one message per message group, so a batch receive is as wide as the number of groups with nothing in flight; messages that share a group are delivered one at a time whatever the batch size")
		b.Support(mq.FeatureOrderedByKey, mq.Support{
			Level: mq.Native, Scope: mq.ScopeKey,
			Note: "the key becomes MessageGroupId and a FIFO queue delivers one group in order, holding a group's later messages until the one in flight is settled; a key that is not short printable ASCII is hashed into a group, which can only MERGE groups and never split one",
		})
		b.Conditional(mq.FeatureDeduplication, mq.ScopeTopic,
			"with a DedupKey, which becomes MessageDeduplicationId: the service discards a second publication of the same identifier for five minutes. The window is the service's and is not configurable. A publication with no DedupKey is given a unique identifier, so it is never deduplicated by accident")
		b.Conditional(mq.FeatureIdempotentPublish, mq.ScopeTopic,
			"with a DedupKey: a retried publication carrying the same MessageDeduplicationId is discarded by the service for five minutes. Without one the driver generates a unique identifier per publication, so a retry publishes a second message")
		// A FIFO queue takes no per-message timer at all: only the
		// queue-level DelaySeconds, which is not a publication control.
		b.Unsupported(mq.FeatureScheduledPublish)
	} else {
		b.Native(mq.FeatureBatchReceive, mq.ScopeTopic)
		b.Unsupported(mq.FeatureOrderedByKey)
		b.Unsupported(mq.FeatureDeduplication)
		b.Unsupported(mq.FeatureIdempotentPublish)
		b.Support(mq.FeatureScheduledPublish, mq.Support{
			Level: mq.Native, Scope: mq.ScopeTopic,
			Note: "DelaySeconds, bounded by the service's fifteen-minute message timer and rounded UP to whole seconds so that a sub-second delay is never withdrawn",
		})
	}

	// Everything else, stated rather than omitted. CapabilityBuilder.Missing
	// is what turns "the driver decided it does not have this" into
	// something a test can tell apart from "nobody looked".
	for _, f := range [...]mq.Feature{
		// At-most-once would mean deleting before the handler runs, which
		// this driver does not offer: a caller who wants it acknowledges on
		// receipt itself.
		mq.FeatureAtMostOnce,
		// Nothing here makes a DELIVERY exactly-once. FIFO's
		// deduplication is publication-side and has a five-minute window.
		mq.FeatureExactlyOnceDelivery,
		// A queue has retention and no per-message expiry, and no
		// priority: a queue is a queue, not a heap.
		mq.FeatureMessageTTL,
		mq.FeaturePriority,
		// One queue, one set of messages. Fan-out is an SNS topic in front
		// of several queues, which is drivers/snssqs.
		mq.FeatureFanOut,
		// A settled message is deleted, so there is no history to replay
		// and nothing to seek in.
		mq.FeatureReplay,
		mq.FeatureSeekByTime,
		mq.FeatureSeekBySequence,
		// A queue has no partitions. The FIFO profile's message group is
		// the ordering unit and is reported as FeatureOrderedByKey.
		mq.FeatureOrderedByPartition,
		// A FIFO queue orders a GROUP and never the queue; a standard
		// queue orders nothing.
		mq.FeatureOrderedGlobally,
		// An attribute set is a map, so a repeated header key has no
		// native representation. MetadataAdaptive carries it in the
		// envelope, which is an envelope decision rather than a
		// capability: the claim here is about what the BROKER can hold.
		mq.FeatureDuplicateHeaders,
		// Each entry of a SendMessageBatch succeeds or fails on its own,
		// which is the opposite of atomic — and is exactly why a partial
		// result has to be correlated accurately.
		mq.FeatureAtomicBatchPublish,
		// No transactions, no broker-side filter, no subject wildcards,
		// no request/reply verb.
		mq.FeatureTransactions,
		mq.FeatureServerFilter,
		mq.FeatureWildcardSubscribe,
		mq.FeatureRequestReply,
		// The service carries an AWSTraceHeader system attribute for
		// X-Ray, and this driver does not wire it: integrations/otel is
		// where tracing lives and the semantic conventions are its (plan
		// The specification). Docs/drivers/sqs.md says so.
		mq.FeatureNativeTracing,
	} {
		b.Unsupported(f)
	}

	b.Limits(limitsFor(cfg))

	if missing := b.Missing(); len(missing) > 0 {
		// A feature nobody mentioned renders as unsupported, which is the
		// safe answer — but it is also indistinguishable from a feature the
		// driver forgot, and TestCapabilitiesConsidersEveryFeature is what
		// keeps that from happening quietly as the inventory grows.
		return mq.Capabilities{}, errIncompleteTable
	}
	return b.Build()
}

// limitsFor is the numeric half of the table.
//
// Every number here was read out of the pinned SDK's generated documentation
// and is recorded in this driver rather than in the portable API, which is the specification's "service limits discovered/documented from the tested API version,
// not hard-coded into the portable API". What is NOT known is left unknown:
// The specification forbids reading an unadvertised limit as zero, and a header-byte
// ceiling the service does not state separately from the message limit is a
// limit this driver does not know.
func limitsFor(cfg Config) mq.Limits {
	resolved := cfg.resolve()
	lim := mq.Limits{
		MaxMessageBytes:  mq.Limit[int64]{Value: resolved.MaxMessageBytes, Known: true},
		MaxBatchMessages: mq.Limit[int]{Value: MaxBatchEntries, Known: true},
		MaxBatchBytes:    mq.Limit[int64]{Value: resolved.MaxMessageBytes, Known: true},
		// Ten attributes, one of which this driver always reserves for the
		// body-encoding marker. Advertising ten would tell a caller it may
		// send ten headers, and the eleventh attribute would then push the
		// message into the envelope or into a refusal.
		MaxHeaderCount: mq.Limit[int]{Value: MaxAttributes - 1, Known: true},
		MaxLease:       mq.Limit[time.Duration]{Value: MaxVisibility, Known: true},
	}
	if !cfg.FIFO {
		lim.MaxDelay = mq.Limit[time.Duration]{Value: MaxDelay, Known: true}
	}
	return lim
}

var errIncompleteTable = errcode.New(errcode.InvalidConfig,
	"iqmq/sqs: the capability table does not mention every feature of the inventory")
