package jetstream

import (
	"strconv"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// streamFacts is what the connected server and the bound stream really said.
//
// The capability table is built from this and from nothing else, which is the
// whole design for this driver: `Nats-TTL` needs a 2.11+ server and a
// stream created with AllowMsgTTL, message schedules need a 2.12+ server and
// AllowMsgSchedules, and durability needs file storage. Every one of those is a
// fact about the deployment rather than about the code, so a claim read off a
// constant would be wrong on two of the three broker versions this driver's
// matrix runs against.
type streamFacts struct {
	// maxPayload is the server's own max_payload from its INFO.
	maxPayload int64
	// maxMsgSize is the stream's own per-message ceiling, or zero.
	maxMsgSize int64
	// memory reports that the stream is memory-backed, so a restart loses
	// everything it held.
	memory bool
	// allowMsgTTL and allowMsgSchedules are the stream's own flags, read back
	// from the server after the stream was created or looked up.
	allowMsgTTL       bool
	allowMsgSchedules bool
	// schedulesConfigured reports that this client is willing to write
	// schedule messages at all ([Config.DisableSchedules] off).
	schedulesConfigured bool
	// duplicates is the stream's Nats-Msg-Id de-duplication window.
	duplicates time.Duration
	// deadLetter is the subject Reject routes to, or empty.
	deadLetter string
	// doubleAck reports that a positive settlement is a round trip.
	doubleAck bool
}

// ttlUsable reports whether a per-message TTL can actually be honoured.
func (f streamFacts) ttlUsable() bool { return f.allowMsgTTL }

// schedulesUsable reports whether a scheduled publication can actually be
// honoured: the stream has to allow schedules and this client has to be willing
// to write them.
func (f streamFacts) schedulesUsable() bool {
	return f.allowMsgSchedules && f.schedulesConfigured
}

// capabilities builds the driver table once, at open, from what the
// connected server and the bound stream said.
//
// The specification makes the absent half of a table as load-bearing as the present half — a
// caller reads it to decide whether it may rely on a behaviour, and the
// conformance kit proves both directions — so everything this profile does not
// have is stated rather than omitted, and every conditional claim carries the
// condition that decides it.
func capabilities(f streamFacts) (mq.Capabilities, error) {
	b := driverkit.NewCapabilities()

	// What the broker does itself.
	b.Native(mq.FeatureAtLeastOnce, mq.ScopeTopic)
	b.Native(mq.FeatureExplicitAck, mq.ScopeTopic)
	b.Native(mq.FeatureNack, mq.ScopeTopic)
	b.Native(mq.FeatureExtendLease, mq.ScopeTopic)
	b.Native(mq.FeatureConsumerGroups, mq.ScopeGroup)
	b.Native(mq.FeatureDurableSubscriptions, mq.ScopeGroup)
	b.Native(mq.FeatureFanOut, mq.ScopeTopic)
	b.Native(mq.FeatureReplay, mq.ScopeTopic)
	b.Native(mq.FeatureSeekByTime, mq.ScopeTopic)
	b.Native(mq.FeatureSeekBySequence, mq.ScopeTopic)
	b.Native(mq.FeatureNativeHeaders, mq.ScopeTopic)
	b.Native(mq.FeatureDuplicateHeaders, mq.ScopeTopic)
	b.Native(mq.FeatureBatchReceive, mq.ScopeTopic)
	b.Native(mq.FeatureWildcardSubscribe, mq.ScopeTopic)
	b.Native(mq.FeatureReceipts, mq.ScopeTopic)

	// A nack with a delay is `-NAK {"delay":n}`, which the server honours
	// itself: the message becomes deliverable again when the delay is up and
	// nothing is held in this process meanwhile.
	b.Native(mq.FeatureDelayedRedelivery, mq.ScopeTopic)

	// The confirmation boundary. On this broker "accepted" and "durable" are
	// the same round trip: the PubAck is sent after the message is committed
	// to the stream's storage, so a publisher that asked for ConfirmDurable
	// gets exactly what one that asked for ConfirmAccepted got.
	b.Support(mq.FeaturePublisherConfirm, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "the stream's PubAck, which the server sends after the message is committed to storage",
	})
	if f.memory {
		// A memory-backed stream acknowledges a publication it will lose on
		// the next restart. The specification forbids reading a durability claim off
		// anything weaker, so the claim is withdrawn rather than qualified.
		b.Unsupported(mq.FeatureDurableStorage)
		b.Unsupported(mq.FeatureDurableConfirm)
	} else {
		b.Native(mq.FeatureDurableStorage, mq.ScopeTopic)
		b.Support(mq.FeatureDurableConfirm, mq.Support{
			Level: mq.Native, Scope: mq.ScopeTopic,
			Note: "a PubAck from a file-backed stream; ConfirmAccepted and ConfirmDurable are the same boundary here",
		})
	}

	// Server-side filtering. The durable consumer's FilterSubjects is the
	// mapped subject, so a subscription's filtering happens at the server and
	// never in the client — which is what makes a wildcard subscription cost
	// the same as an exact one.
	b.Support(mq.FeatureServerFilter, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "the durable consumer's FilterSubjects is the subject the topic maps to; the only filter the portable API can express is a subject pattern",
	})

	// Ordering. JetStream stores one subject's messages in the order the
	// stream accepted them and a single consumer sees them in that order.
	// Two publisher connections are not ordered against each other, and a
	// subscription with more than one consumer interleaves across them. The
	// three ordering features differ only in scope, and on a broker with no
	// partitions and no keys they all reduce to "per subject".
	const orderNote = "one publisher's messages on one subject delivered to a single consumer of a subscription; two publishers are not ordered against each other, a subscription with two consumers interleaves, and a nack puts a message behind the messages that followed it"
	b.Conditional(mq.FeatureOrderedGlobally, mq.ScopeTopic, orderNote)
	b.Conditional(mq.FeatureOrderedByKey, mq.ScopeKey,
		"JetStream has no message key, so a key orders only in so far as its messages share a subject: "+orderNote)
	b.Conditional(mq.FeatureOrderedByPartition, mq.ScopePartition,
		"JetStream has no partitions; a subject is the only partition there is, so: "+orderNote)

	// De-duplication is the stream's Nats-Msg-Id window and nothing more: it
	// is publication de-duplication, so the same key published twice inside
	// the window is stored once, and the same message DELIVERED twice after a
	// lease expiry is still two deliveries.
	dedupNote := "PublishOption DedupKey travels as Nats-Msg-Id and the stream discards a repeat inside its duplicate window, which is " +
		f.duplicates.String() + " on this stream; it de-duplicates PUBLICATIONS and never deliveries"
	b.Conditional(mq.FeatureDeduplication, mq.ScopeTopic, dedupNote)
	b.Conditional(mq.FeatureIdempotentPublish, mq.ScopeTopic, dedupNote)

	// Per-message TTL and scheduled publication are the two claims the
	// broker-version matrix flips. Each is conditional on a server version
	// AND on a stream flag, and each is withdrawn outright when the stream
	// this client is bound to does not carry the flag — because a claim whose
	// condition cannot be met on this deployment is not a conditional claim,
	// it is a false one.
	if f.ttlUsable() {
		b.Conditional(mq.FeatureMessageTTL, mq.ScopeTopic,
			"PublishOption TTL travels as Nats-TTL, which needs a 2.11+ server and a stream created with AllowMsgTTL; this stream has it")
	} else {
		b.Unsupported(mq.FeatureMessageTTL)
	}
	if f.schedulesUsable() {
		b.Conditional(mq.FeatureScheduledPublish, mq.ScopeTopic,
			"PublishOption Delay and DeliverAt travel as a Nats-Schedule control message on Config.ScheduleSubject with the topic as its target, which needs a 2.12+ server and a stream created with AllowMsgSchedules; this stream has it")
	} else {
		b.Unsupported(mq.FeatureScheduledPublish)
	}

	// Dead-lettering is this driver's own route and it is durable: a core
	// publication to the dead-letter subject, its PubAck, and only then
	// +TERM. The condition is the destination, so the claim is conditional
	// whether or not this client has one — and the note says which.
	dlqNote := "Reject publishes the message to Config.DeadLetterTopic, waits for that publication's PubAck and only then sends +TERM; a Term before the publication was durable would destroy the message. "
	if f.deadLetter == "" {
		dlqNote += "No destination is configured on this client, so Reject reports ErrUnsupported and settles nothing"
	} else {
		dlqNote += "This client routes to " + f.deadLetter
	}
	b.Conditional(mq.FeatureDeadLetter, mq.ScopeTopic, dlqNote)

	// A batch is the driver's own: every message is published
	// asynchronously, bounded by Config.PublishAsyncMaxPending, and every
	// PubAck is awaited, so each message of the batch is as durable as a
	// single publication and the result is index-aligned. It is not atomic —
	// 2.14's atomic batch is a stream flag this driver does not set — which
	// is why the claim is an emulation rather than a native feature.
	b.EmulatedDurable(mq.FeatureBatchPublish, mq.ScopeTopic,
		"the driver publishes every message of the batch asynchronously and waits for every PubAck, so each is durable; JetStream's atomic batch is not used, so a batch is not all-or-nothing")

	// Everything else, stated rather than omitted. CapabilityBuilder.Missing
	// is what turns "the driver decided it does not have this" into something
	// a test can tell apart from "nobody looked".
	for _, feature := range [...]mq.Feature{
		// At-most-once would be an AckNone consumer, which this driver does
		// not open: the profile exists for the settlement.
		mq.FeatureAtMostOnce,
		// Exactly-once DELIVERY is not what JetStream offers. Publication
		// de-duplication is advertised above; a delivery whose lease
		// expires is still redelivered, so a handler may run twice.
		mq.FeatureExactlyOnceDelivery,
		mq.FeaturePriority,
		mq.FeatureBinaryHeaders,
		mq.FeatureAtomicBatchPublish,
		// Settlement is one message per delivery on its own reply subject.
		// There is no batch ack verb, and a driver that looped would be
		// advertising a saving it does not make.
		mq.FeatureBatchSettle,
		mq.FeatureTransactions,
		// No AdminBackend: the stream and consumer API is reachable
		// through Client.Native(), and a topology surface is not this
		// increment's.
		mq.FeatureAdmin,
		// Request/reply is a core NATS pattern over an ephemeral inbox,
		// not a stream one. drivers/nats has it.
		mq.FeatureRequestReply,
		mq.FeatureNativeTracing,
	} {
		b.Unsupported(feature)
	}

	var lim mq.Limits
	if n := messageCeiling(f); n > 0 {
		lim.MaxMessageBytes = mq.Limit[int64]{Value: n, Known: true}
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

// messageCeiling is the largest message this deployment accepts: the smaller of
// the server's max_payload and the stream's own MaxMsgSize.
//
// Taking the smaller is the only answer that cannot lie in the dangerous
// direction. The specification forbids reading an unadvertised limit as zero, so a deployment
// where neither is known leaves the limit unknown rather than guessing 1 MiB.
func messageCeiling(f streamFacts) int64 {
	switch {
	case f.maxPayload > 0 && f.maxMsgSize > 0:
		return min(f.maxPayload, f.maxMsgSize)
	case f.maxPayload > 0:
		return f.maxPayload
	default:
		return f.maxMsgSize
	}
}

// describeFacts renders the stream facts for a log line or a test failure, so
// that a capability answer can be read against what the broker said.
func describeFacts(f streamFacts) string {
	return "storage=" + storageName(f.memory) +
		" allowmsgttl=" + strconv.FormatBool(f.allowMsgTTL) +
		" allowmsgschedules=" + strconv.FormatBool(f.allowMsgSchedules) +
		" schedulesconfigured=" + strconv.FormatBool(f.schedulesConfigured) +
		" duplicates=" + f.duplicates.String() +
		" maxpayload=" + strconv.FormatInt(f.maxPayload, 10) +
		" maxmsgsize=" + strconv.FormatInt(f.maxMsgSize, 10) +
		" doubleack=" + strconv.FormatBool(f.doubleAck)
}

// storageName names the storage a fact set describes.
func storageName(memory bool) string {
	if memory {
		return StorageMemory.String()
	}
	return StorageFile.String()
}
