package snssqs

import (
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// capabilities builds the driver table once, at open, from the
// configuration.
//
// It is the SQS standard profile's table with four differences, and every one
// of them is the fan-out:
//
//   - FeatureFanOut is NATIVE rather than absent. That is the whole driver: one
//     publication reaches every subscribed queue, and each queue's consumer
//     settles its own copy.
//   - FeatureScheduledPublish is ABSENT rather than native. SQS has a
//     per-message DelaySeconds and SNS has no per-message timer at all, so the
//     option is refused before any I/O rather than honoured by the queue.
//   - FeatureServerFilter is CONDITIONAL rather than absent, on a filter policy
//     being configured for the subscription. The specification requires the condition to be
//     stated and driverkit's builder refuses a conditional support with no
//     note, which is what keeps the claim actionable.
//   - the message limit is a QUARTER of the queue's: SNS takes 256 KiB.
//
// Everything else is the same because it is the same code: the consume side of
// this driver is `drivers/sqs`.
func capabilities(cfg Config) (mq.Capabilities, error) {
	b := driverkit.NewCapabilities()

	// The fan-out itself. FeatureFanOut is "independent delivery to
	// several subscriptions", and the word that matters is independent: the
	// copies are separate MESSAGES in separate queues, with separate receipt
	// handles, separate visibility windows and separate settlements. Nothing
	// about SNS relates them, and neither does anything in this driver.
	b.Support(mq.FeatureFanOut, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "one SNS Publish reaches every subscribed queue, and each subscription's consumer settles ITS OWN copy: the copies are separate SQS messages with separate receipt handles, separate visibility windows and separate ApproximateReceiveCounts, so one subscriber's acknowledgement, rejection or failure is invisible to the others and a message dead-lettered for one subscription is still in flight for the rest",
	})

	// The queue and the topic. A message is stored redundantly across
	// availability zones by SNS before Publish answers, and again by each
	// queue it is delivered into.
	b.Native(mq.FeatureDurableStorage, mq.ScopeTopic)
	b.Native(mq.FeatureAtLeastOnce, mq.ScopeTopic)
	b.Native(mq.FeatureExplicitAck, mq.ScopeTopic)
	b.Native(mq.FeatureDurableSubscriptions, mq.ScopeTopic)
	b.Native(mq.FeatureNativeHeaders, mq.ScopeTopic)
	b.Native(mq.FeatureBatchPublish, mq.ScopeTopic)
	b.Native(mq.FeatureBatchReceive, mq.ScopeTopic)
	b.Native(mq.FeatureAdmin, mq.ScopeTopic)
	b.Native(mq.FeatureReceipts, mq.ScopeTopic)

	// The confirmation boundary is SNS's, and the note says what that does
	// and does not mean. ConfirmDurable is "the broker has durably
	// stored the message", which SNS has; what it has NOT done when Publish
	// answers is put a copy in any queue. A caller who read ConfirmDurable
	// as "a consumer can see it now" would be reading a promise nobody made.
	const confirmNote = "the SNS Publish or PublishBatch response: the service has stored the message redundantly across availability zones before it answers, so ConfirmAccepted and ConfirmDurable are the same boundary and neither costs an extra round trip. What the response does NOT mean is that a copy is in any queue yet — the fan-out happens after it, so a consumer may see the message a moment later, and a subscription created after the publication never sees it at all"
	b.Support(mq.FeaturePublisherConfirm, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic, Note: confirmNote})
	b.Support(mq.FeatureDurableConfirm, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic, Note: confirmNote})

	// A subscription's queue is a competing-consumer group by construction:
	// every consumer of one subscription takes from the same queue, and the
	// visibility timeout is what keeps two of them from taking the same
	// message. Two DIFFERENT subscriptions are the fan-out, which is the
	// distinction this driver exists to make.
	b.Support(mq.FeatureConsumerGroups, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "SourceConfig.Subscription names the QUEUE: consumers that share a subscription compete for its messages, and consumers of two subscriptions each receive every message. It is the opposite of drivers/sqs, where the subscription is ignored because a queue has exactly one set of messages",
	})

	// Binary header values travel in SNS's Binary attribute type and arrive
	// in SQS's, which raw message delivery maps one to one. Without raw
	// delivery they would arrive inside a JSON envelope as base64 under a
	// "Type" field, which is why a non-raw subscription is refused rather
	// than decoded.
	b.Support(mq.FeatureBinaryHeaders, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "an SNS message attribute of the Binary type carries an arbitrary byte value and raw message delivery maps it to an SQS Binary attribute unchanged; the attribute NAME still has to be a legal attribute name, and a header whose key is not takes the envelope under MetadataAdaptive",
	})

	// Settlement is the queue's, and it is PER QUEUE: see FeatureFanOut.
	b.Support(mq.FeatureBatchSettle, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "DeleteMessageBatch and ChangeMessageVisibilityBatch take ten entries each, every entry with its own outcome, against ONE subscription's queue; Config.Settle is the flush policy and decides when a coalesced batch leaves",
	})
	b.Support(mq.FeatureNack, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "ChangeMessageVisibility with a timeout of zero makes this subscription's copy immediately visible again, which is a native negative acknowledgement and increments that copy's ApproximateReceiveCount; the other subscriptions' copies are untouched",
	})
	b.Support(mq.FeatureDelayedRedelivery, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "ChangeMessageVisibility with the delay, bounded by the queue's twelve-hour visibility timeout; the delay survives a process death, because it is the queue that holds it. It is a REDELIVERY delay and not a publication delay — SNS has no per-message timer, so FeatureScheduledPublish is absent",
	})
	b.Support(mq.FeatureExtendLease, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "ChangeMessageVisibility moves this copy's window, bounded by the queue's twelve hours measured from the call and not from the delivery",
	})

	// The dead-letter route is the QUEUE's, which is the only place a
	// fan-out can put one: a message N subscribers failed to handle failed N
	// times independently, so there is no such thing as dead-lettering "the
	// publication".
	b.EmulatedDurable(mq.FeatureDeadLetter, mq.ScopeTopic,
		"two routes, both per subscription. The subscription's queue carries its own redrive policy, which moves a copy the service has delivered Config.MaxReceiveCount times with no settlement from the consumer at all; an explicit Reject sends that copy to Config.DeadLetterQueue with its provenance and deletes the original only once the send is confirmed. Neither touches another subscription's copy, and a Reject on a client with no DeadLetterQueue configured is refused with ErrUnsupported rather than destroying the message. SNS's own subscription RedrivePolicy — the dead-letter queue for deliveries SNS itself could not make — is not wired by this driver and is the operator's, through TypedAdmin")

	// The conditional claim, and the one rule that made it conditional
	// rather than native: a capability that needs an option has to name the
	// option. driverkit.NewCapabilities refuses a Conditional with an empty
	// note, so the condition cannot be forgotten.
	b.Conditional(mq.FeatureServerFilter, mq.ScopeGroup,
		"with an SNS subscription filter policy, which is Config.FilterPolicies keyed by subscription name, applied by this driver when it creates the subscription and settable afterwards through TypedAdmin.SetFilterPolicy. Config.FilterPolicyScope decides what the policy is evaluated against: \"attributes\" (the service's default) filters on message attributes, which on this driver are the message's HEADERS — so a policy written against header names works, unless the message took the metadata envelope, in which case every header is in the body and the only attribute left is the envelope marker; \"body\" filters on the body parsed as JSON, which is useless for a payload that is not JSON and for an enveloped message. Without an entry for a subscription there is no server-side filter at all and every message reaches its queue")

	// What SNS does not have, stated rather than omitted.
	//
	// FeatureScheduledPublish is the one to read twice. drivers/sqs claims
	// it natively, because a queue has DelaySeconds; SNS has no per-message
	// timer of any kind, so mq.Delay and mq.DeliverAt are refused with
	// ErrUnsupported before any I/O. A caller porting a publisher from the
	// queue driver to this one loses it, and the specification makes silently dropping
	// it a disqualifier.
	b.Unsupported(mq.FeatureScheduledPublish)

	for _, f := range [...]mq.Feature{
		// At-most-once would mean deleting before the handler runs, which
		// this driver does not offer.
		mq.FeatureAtMostOnce,
		// Nothing here makes a delivery exactly-once, and the fan-out adds
		// a second reason: a publication becomes N messages.
		mq.FeatureExactlyOnceDelivery,
		// A standard SNS topic has no deduplication and no publication
		// idempotence; a FIFO topic does, and this driver refuses a .fifo
		// name rather than pretending to a profile it does not implement.
		mq.FeatureDeduplication,
		mq.FeatureIdempotentPublish,
		// A queue has retention and no per-message expiry, and no
		// priority.
		mq.FeatureMessageTTL,
		mq.FeaturePriority,
		// A settled message is deleted and a subscription created after a
		// publication never receives it, so there is no history to replay
		// and nothing to seek in. The kit's replay probe cannot refute
		// this on a fan-out broker — see conformanceSkips.
		mq.FeatureReplay,
		mq.FeatureSeekByTime,
		mq.FeatureSeekBySequence,
		// A standard topic delivering into standard queues orders nothing:
		// not a key's messages, not a partition's, not the topic's. An SNS
		// FIFO topic is where ordering lives and is not this driver.
		mq.FeatureOrderedByKey,
		mq.FeatureOrderedByPartition,
		mq.FeatureOrderedGlobally,
		// An attribute set is a map, so a repeated header key has no
		// native representation. MetadataAdaptive carries it in the
		// envelope, which is an envelope decision rather than a
		// capability: the claim here is about what the SERVICES can hold.
		mq.FeatureDuplicateHeaders,
		// Each entry of a PublishBatch succeeds or fails on its own, which
		// is the opposite of atomic — and is exactly why a partial result
		// has to be correlated accurately.
		mq.FeatureAtomicBatchPublish,
		// No transactions, no pattern subscription — an SNS subscription
		// names one topic ARN — and no request/reply verb.
		mq.FeatureTransactions,
		mq.FeatureWildcardSubscribe,
		mq.FeatureRequestReply,
		// SQS carries an AWSTraceHeader system attribute for X-Ray and SNS
		// forwards it; this driver wires neither. integrations/otel is
		// where tracing lives and the semantic conventions are its (plan
		// ).
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
// Every number was read out of the pinned SDKs' generated documentation and is
// recorded in this driver rather than in the portable API. What is NOT
// known is left unknown: the specification forbids reading an unadvertised limit as zero.
//
// MaxDelay is deliberately absent rather than zero. A zero Limit with Known
// true would say "the longest delay is nothing", which is a different claim
// from "this broker has no per-message delay"; the capability table says the
// second through FeatureScheduledPublish, and the limit says nothing.
func limitsFor(cfg Config) mq.Limits {
	resolved := cfg.resolve()
	return mq.Limits{
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
}

// MaxVisibility is the longest visibility timeout a subscribing queue grants,
// which is the lease ceiling this driver advertises. It is the queue's limit
// and not the topic's: a lease exists only after the fan-out.
const MaxVisibility = 12 * time.Hour

var errIncompleteTable = errcode.New(errcode.InvalidConfig,
	"iqmq/snssqs: the capability table does not mention every feature of the inventory")
