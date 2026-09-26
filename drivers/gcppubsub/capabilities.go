package gcppubsub

import (
	"strconv"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// capabilities builds the driver table once, at open, from the
// configuration.
//
// Three things about the table are worth reading before the list.
//
// First, the specification makes the absent half as load-bearing as the present half: a
// caller reads the table to decide whether it may rely on a behaviour, and the
// conformance kit proves both directions, so every feature of the inventory is
// mentioned here even when the answer is no.
//
// Second, five claims are CONDITIONAL on a configuration field, and the specification
// requires a conditional support to state its condition —
// [driverkit.CapabilityBuilder] refuses one that does not. Ordering keys need
// [Config.Ordering] because the client library's EnableMessageOrdering is a
// construction flag on the publisher; the dead-letter route needs
// [Config.DeadLetterTopic]; seeking and replay need [Config.RetainAcked]; a
// server-side filter needs [Config.Filter]; and the attempt count comes with
// the dead-letter policy rather than on its own.
//
// Third, the table does NOT depend on the configuration. The specification says
// capabilities are constant per client, and a table computed from Config
// breaks the conformance kit in a way that looks like a passing report: the
// kit runs two scenarios per claim and shapes the topology differently for
// each, so a driver that advertised "unsupported" to the scenario whose Need
// does not configure the feature would have its positive probe gated off and
// its negative probe asked to refute a denial it does not make — the claim
// proved in neither direction (which `drivers/sqs` cites for the same reason). So every conditional claim is advertised
// unconditionally with the condition in the note.
//
// Fourth, two claims are DELIBERATELY NOT MADE where the service's own
// vocabulary would suggest they should be, and each carries its reason in the
// table itself:
//
//   - FeatureExactlyOnceDelivery. Pub/Sub's "exactly-once delivery" is a
//     guarantee about a SETTLEMENT — an acknowledged message is never
//     redelivered, and AckWithResult reports whether the acknowledgement took
//     — and the feature is a guarantee about DELIVERY MULTIPLICITY. An
//     unacknowledged message whose deadline expires is redelivered on an
//     exactly-once subscription exactly as on any other, so claiming it would
//     be claiming something the service does not promise.
//     [Config.ExactlyOnce] is still worth setting and the note says why.
//   - FeatureDelayedRedelivery is EMULATED and not native. Pub/Sub has no
//     nack delay at all: a nack makes the message deliverable at once. A
//     delayed negative settlement is therefore an IN-PROCESS HOLD — the
//     driver keeps the delivery unacknowledged, lets the client library go on
//     extending its deadline, and nacks it when the delay elapses — which
//     does not survive a process death and must never satisfy a durability
//     requirement.
func capabilities(cfg Config) (mq.Capabilities, error) {
	resolved := cfg.resolve()
	b := driverkit.NewCapabilities()

	// The service itself.
	b.Native(mq.FeatureDurableStorage, mq.ScopeTopic)
	b.Native(mq.FeatureAtLeastOnce, mq.ScopeTopic)
	b.Native(mq.FeatureExplicitAck, mq.ScopeTopic)
	b.Native(mq.FeatureDurableSubscriptions, mq.ScopeGroup)
	b.Native(mq.FeatureNativeHeaders, mq.ScopeTopic)
	b.Native(mq.FeatureAdmin, mq.ScopeTopic)
	b.Native(mq.FeatureBatchReceive, mq.ScopeGroup)

	const confirmNote = "PublishResult.Get: the service answers with a message identifier once it has stored the message in at least two zones, so ConfirmAccepted and ConfirmDurable are the same boundary and neither costs an extra round trip. The client library BUNDLES publications, so the boundary is reached for a batch and reported per message"
	b.Support(mq.FeaturePublisherConfirm, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic, Note: confirmNote})
	b.Support(mq.FeatureDurableConfirm, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic, Note: confirmNote})
	b.Support(mq.FeatureReceipts, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "the server-assigned message identifier, carried in PublishReceipt.MessageID. There is no partition, no offset and no sequence on this broker, so those stay zero rather than being invented",
	})

	// Fan-out and groups. This is the shape Pub/Sub is built around: a topic
	// has many subscriptions, each subscription gets every message, and the
	// consumers of ONE subscription compete for its messages.
	b.Support(mq.FeatureFanOut, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "a topic has many subscriptions and each of them receives every message independently, with its own acknowledgement state; a subscription created AFTER a publication never sees it, which is what makes Replay conditional rather than native",
	})
	b.Support(mq.FeatureConsumerGroups, mq.Support{
		Level: mq.Native, Scope: mq.ScopeGroup,
		Note: "a subscription IS the group: every consumer of one subscription competes for its messages, and the acknowledgement deadline is what keeps two of them from processing the same one. SourceConfig.Subscription is the subscription ID",
	})

	// Batching. The client library bundles publications itself, so a batch
	// publish is the library's bundler rather than a batch API, and a batch
	// settlement is the library coalescing acknowledgement identifiers.
	b.Support(mq.FeatureBatchPublish, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "the client library's own bundler: at most " + itoa(MaxBatchMessages) + " messages and " + itoa(MaxMessageBytes) + " bytes per publish request, flushed by Config.PublishCountThreshold, PublishByteThreshold or PublishDelayThreshold. PublishBatch hands every message to the bundler and waits for all of their results, so a partial failure is reported per index",
	})
	b.Support(mq.FeatureBatchSettle, mq.Support{
		Level: mq.Native, Scope: mq.ScopeGroup,
		Note: "the client library coalesces acknowledgement identifiers into batched Acknowledge and ModifyAckDeadline requests on its own schedule; AckBatch settles every delivery and reports each outcome at its own index, and there is no flush policy to choose because the batching is not this driver's",
	})

	// Settlement.
	b.Support(mq.FeatureNack, mq.Support{
		Level: mq.Native, Scope: mq.ScopeGroup,
		Note: "Message.Nack sends a ModifyAckDeadline of zero, which makes the message deliverable again at once and increments the delivery attempt when the subscription has a dead-letter policy",
	})
	b.EmulatedVolatile(mq.FeatureDelayedRedelivery, mq.ScopeGroup,
		"an IN-PROCESS HOLD, because Pub/Sub has no nack delay: the driver keeps the delivery unacknowledged for the delay while the client library goes on extending its acknowledgement deadline, and nacks it when the delay elapses. It is volatile by construction — a process that dies during the hold loses the delay, and the message becomes deliverable when its deadline expires instead — so it never satisfies a durability requirement. The delay is bounded by what is left of Config.MaxExtension, because a hold longer than the extension budget would lose the lease half-way through")
	b.EmulatedVolatile(mq.FeatureExtendLease, mq.ScopeGroup,
		"the client library extends a held message's acknowledgement deadline AUTOMATICALLY, up to Config.MaxExtension in total, so a handler that needs longer than the deadline gets it without asking. Extend(by) checks that `by` fits inside what is left of that budget and moves the driver's own view of the lease; it sends no ModifyAckDeadline of its own, because the acknowledgement identifier that request needs is private to the client library's message type. An extension beyond the budget is refused with ErrUnsupported rather than granted and then lost")

	// The dead-letter route, ordering, seeking and the filter are
	// CONDITIONAL, and every one of them is advertised UNCONDITIONALLY with
	// its condition in the note.
	//
	// That is the lesson and `drivers/sqs` cites it for the
	// same reason: the conformance kit runs two scenarios for one claim and
	// shapes the topology differently for each, so a table that answered
	// "unsupported" to the scenario whose Need does not configure the
	// feature proves the claim in NEITHER direction — the positive probe is
	// gated off and the negative one is asked to refute a denial the driver
	// does not make. The specification also says capabilities are constant per client,
	// and a table computed from Config is not.
	b.EmulatedDurable(mq.FeatureDeadLetter, mq.ScopeGroup,
		"two routes, both needing Config.DeadLetterTopic. The subscription's own dead-letter policy is NATIVE and needs no settlement at all: the SERVICE moves a message it has delivered Config.MaxDeliveryAttempts times (at least five, which is the service's own minimum) to the dead-letter topic. An explicit Reject cannot wait for five deliveries, so it PUBLISHES the message to the dead-letter topic and acknowledges the original only once that publication is confirmed — durable, because the destination is a Pub/Sub topic, and at-least-once, because a failure between the publication and the acknowledgement leaves the original to be redelivered. A Reject on a client with no DeadLetterTopic configured is refused with ErrUnsupported rather than destroying the message. The dead-letter policy is also what makes DeliveryAttempt present at all: without one, DeliveryInfo.Attempt is zero because the service counts nothing")
	b.Conditional(mq.FeatureOrderedByKey, mq.ScopeKey,
		"with Config.Ordering, which sets the client library's EnableMessageOrdering on the publisher and enableMessageOrdering on subscriptions this driver creates. Message.Key becomes the ordering key -- verbatim when it is valid UTF-8 of at most 1024 bytes and base64 otherwise, which can only MERGE two ordering groups and never split one -- and the service then delivers one key's messages in publication order to one subscriber. WITHOUT Config.Ordering the key is still carried as metadata and still round-trips, and publications are not ordered by it. The driver resumes a key the library paused after a failure: a publisher that is not resumed silently stops publishing that key for the rest of its life")
	b.Conditional(mq.FeatureSeekByTime, mq.ScopeGroup,
		"with Config.RetainAcked and Config.Retention: a seek moves the subscription's acknowledgement state to a timestamp inside the retention window, which is what makes ConsumerConfig.Start = StartAt(t) a Seek at OpenSource. Without retained acknowledged messages a seek backwards has nothing to replay and StartAt is refused before any I/O. Seeking FORWARD needs no retention and is what Admin.Purge is")
	b.Conditional(mq.FeatureReplay, mq.ScopeGroup,
		"with Config.RetainAcked, and only for a subscription that ALREADY EXISTED when the messages were published: a Pub/Sub subscription's backlog begins at its own creation, so a new subscription can never read a topic's history however long the retention is. Replaying means seeking an existing subscription back over messages it has acknowledged")
	b.Conditional(mq.FeatureServerFilter, mq.ScopeGroup,
		"with Config.Filter, which becomes the subscription's filter expression: the SERVICE evaluates it against a message's attributes and acknowledges what it does not match, so a filtered message is never delivered and never counted against the backlog. The grammar is the service's and this driver does not parse it")

	// The claims this driver deliberately does not make, each with the
	// reason. CapabilityBuilder.Missing is what turns "the driver decided it
	// does not have this" into something a test can tell apart from "nobody
	// looked".
	b.Support(mq.FeatureExactlyOnceDelivery, mq.Support{
		Level: mq.Unsupported,
		Note:  "Pub/Sub's \"exactly-once delivery\" is a guarantee about a SETTLEMENT and not about delivery multiplicity: an acknowledged message is never redelivered and AckWithResult reports whether the acknowledgement took, which is what Config.ExactlyOnce buys. An UNACKNOWLEDGED message whose deadline expires is still redelivered, so the feature — a message delivered exactly once — is not something the service promises and this driver does not claim it",
	})
	b.Support(mq.FeatureBinaryHeaders, mq.Support{
		Level: mq.Unsupported,
		Note:  "a Pub/Sub attribute value is a string. A binary header value has no native representation, so MetadataAdaptive envelopes the message, MetadataNativeStrict refuses it before any I/O, and MetadataLenientDrop drops it with a metric",
	})
	b.Support(mq.FeatureDuplicateHeaders, mq.Support{
		Level: mq.Unsupported,
		Note:  "an attribute set is a map, so a repeated header key has no native representation and takes the same three answers as a binary value",
	})
	b.Support(mq.FeatureScheduledPublish, mq.Support{
		Level: mq.Unsupported,
		Note:  "Pub/Sub has no publication timer: a message is deliverable as soon as the service has stored it. Delay and DeliverAt are refused before any I/O rather than silently ignored",
	})
	b.Support(mq.FeatureMessageTTL, mq.Support{
		Level: mq.Unsupported,
		Note:  "a message expires with the SUBSCRIPTION's retention period (Config.Retention), which is not a per-message time to live",
	})
	b.Support(mq.FeatureDeduplication, mq.Support{
		Level: mq.Unsupported,
		Note:  "the service does not deduplicate publications. A DedupKey is refused rather than accepted and ignored",
	})
	b.Support(mq.FeatureIdempotentPublish, mq.Support{
		Level: mq.Unsupported,
		Note:  "a retried publication is a second message. An ordering key serialises publications and does not deduplicate them",
	})
	b.Support(mq.FeatureAtomicBatchPublish, mq.Support{
		Level: mq.Unsupported,
		Note:  "one publish request carries many messages and each of them succeeds or fails on its own, which is why a partial result has to be correlated per index",
	})
	b.Support(mq.FeatureSeekBySequence, mq.Support{
		Level: mq.Unsupported,
		Note:  "Pub/Sub has no per-message sequence number; a seek names a timestamp or a snapshot",
	})
	b.Support(mq.FeatureOrderedByPartition, mq.Support{
		Level: mq.Unsupported,
		Note:  "Pub/Sub has no partitions. The ordering key is the ordering unit and is reported as FeatureOrderedByKey",
	})
	b.Support(mq.FeatureOrderedGlobally, mq.Support{
		Level: mq.Unsupported,
		Note:  "a topic has no global order: messages with different ordering keys, or none, are delivered in whatever order the service finds them in",
	})
	b.Support(mq.FeatureWildcardSubscribe, mq.Support{
		Level: mq.Unsupported,
		Note:  "a subscription names exactly one topic and there are no topic patterns. Config.Filter selects on a message's ATTRIBUTES, which is a different thing and is reported as FeatureServerFilter",
	})
	b.Support(mq.FeatureNativeTracing, mq.Support{
		Level: mq.Unsupported,
		Note:  "the client library can emit OpenTelemetry spans and this driver does not turn them on: integrations/otel owns the semantic conventions and the span names. Docs/drivers/gcppubsub.md says so",
	})
	for _, f := range [...]mq.Feature{
		// At-most-once would mean acknowledging before the handler runs,
		// which this driver does not offer: a caller who wants it
		// acknowledges on receipt itself.
		mq.FeatureAtMostOnce,
		// No priority, no transactions, no request/reply verb.
		mq.FeaturePriority,
		mq.FeatureTransactions,
		mq.FeatureRequestReply,
	} {
		b.Unsupported(f)
	}

	b.Limits(limitsFor(resolved))

	if missing := b.Missing(); len(missing) > 0 {
		return mq.Capabilities{}, errIncompleteTable
	}
	return b.Build()
}

// limitsFor is the numeric half of the table.
//
// What is NOT known is left unknown: the specification forbids reading an unadvertised
// limit as zero, and a limit the service does not publish is one this driver
// does not know. MaxDelay is the interesting case — the in-process hold's
// bound is what is left of the extension budget, which is a real number and is
// advertised as such, because a caller asking "how long may I delay a
// redelivery?" gets a worse answer from "unknown" than from the truth.
func limitsFor(cfg Config) mq.Limits {
	return mq.Limits{
		MaxMessageBytes:  mq.Limit[int64]{Value: MaxMessageBytes, Known: true},
		MaxBatchMessages: mq.Limit[int]{Value: MaxBatchMessages, Known: true},
		MaxBatchBytes:    mq.Limit[int64]{Value: MaxMessageBytes, Known: true},
		// A hundred attributes, less the ten this driver reserves for the
		// key, the identifier, the envelope marker and the six typed
		// properties. Advertising a hundred would tell a caller it may send
		// a hundred headers, and the ninety-first would then push the
		// message into the envelope or into a refusal.
		MaxHeaderCount: mq.Limit[int]{Value: MaxAttributes - len(reservedNames), Known: true},
		MaxHeaderBytes: mq.Limit[int64]{Value: MaxAttributeValueBytes, Known: true},
		MaxDelay:       mq.Limit[time.Duration]{Value: cfg.MaxExtension, Known: true},
		MaxLease:       mq.Limit[time.Duration]{Value: MaxAckDeadline, Known: true},
	}
}

// itoa is strconv.Itoa, named locally so that the capability notes read as
// prose rather than as string building.
func itoa(n int) string { return strconv.Itoa(n) }
