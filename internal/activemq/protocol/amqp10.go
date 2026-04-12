package protocol

import (
	"strings"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
)

// amqp10 is the AMQP 1.0 arm, over `github.com/Azure/go-amqp`,
// and the reference kind.
//
// It is the default because criterion is read on feature
// completeness FIRST and on perf second, and the completeness half is settled
// before any measurement: on Artemis this arm supports nineteen of the specification's
// thirty-nine features natively against STOMP's fifteen, and the four rows it
// wins are not decoration. It can NACK (a `modified{delivery-failed}`
// disposition returns the delivery and raises `delivery-count`, measured on
// both brokers); it can report a real attempt count; it carries a binary
// header value; and its receive window is the link's credit rather than a
// broker-specific subscribe header.
//
// # What Carry and Restore are for on this arm
//
// The AMQP 1.0 driver's real mapping is `drivers/amqp10`'s `Mapper`, which
// fills `amqp.Message`'s four sections directly. These two methods exist for
// the family's MEASUREMENT and for its support table: they map one
// `mq.Message` into the same slots over the same fields, so the benchmark
// compares like with like against the STOMP arm — whose `Carry` IS its
// driver's mapping — and so the property decisions (which iqmq field goes in
// which section, on which broker) live in one place that both the driver and
// the bake-off read. `internal/rabbit/protocol`'s AMQP 1.0 arm says the same
// thing one broker along.
//
// # The sections, measured against apache/activemq-artemis:2.44.0-alpine and
// apache/activemq-classic:6.2.0
//
//   - the HEADER section carries `durable`, `priority`, `ttl` (a relative
//     duration) and `delivery-count` (the number of PREVIOUS attempts).
//   - the PROPERTIES section carries `message-id`, `correlation-id`,
//     `content-type`, `content-encoding`, `reply-to`, `group-id`,
//     `absolute-expiry-time` and `creation-time`.
//   - the APPLICATION-PROPERTIES section carries the caller's headers, the
//     reserved entries neither protocol has a slot for, Artemis's
//     `_AMQ_DUPL_ID`, and — on CLASSIC ONLY — `AMQ_SCHEDULED_DELAY`.
//   - the MESSAGE-ANNOTATIONS section carries — on ARTEMIS ONLY —
//     `x-opt-delivery-delay` and `x-opt-delivery-time`.
//
// The last two are the same feature in two different sections, and each broker
// IGNORES the other's: measured, an `AMQ_SCHEDULED_DELAY` annotation on
// Artemis and an `x-opt-delivery-delay` annotation on Classic both delivered
// immediately. [Fields.Slot] is what carries that decision to the driver.
type amqp10 struct{}

var _ Protocol = amqp10{}

// Kind returns AMQP10.
func (amqp10) Kind() Kind { return AMQP10 }

// MinBroker is the oldest release of each product whose AMQP 1.0 transport
// this driver claims.
//
// On Artemis it is 2.7, and the reason is the driver's own addressing rather
// than the protocol: a shared durable subscription is addressed by FULLY
// QUALIFIED QUEUE NAME (`address::queue`, `activemq.FQQN`), and 2.7.0 is the
// release whose acceptors accept one. An older Artemis speaks AMQP 1.0
// perfectly well and would detach the receiver with amqp:not-found.
//
// On Classic it is 5.9, the release in which the AMQP transport stopped being
// experimental. Note that a Classic broker sends no product and no version in
// its AMQP 1.0 open performative at all, so this floor is unenforceable over
// this protocol and `activemq.CheckAtLeast` accepts silence deliberately; what
// guards the wrong-broker case instead is the product check, which reads the
// STOMP `server` header or Artemis's `product` property.
func (amqp10) MinBroker(b activemq.Broker) activemq.Version {
	switch b {
	case activemq.Artemis:
		return activemq.MinArtemisFQQN
	case activemq.Classic:
		return activemq.MinClassicAMQP
	default:
		return activemq.Version{}
	}
}

// MaxMessageBytes is zero: an AMQP 1.0 message is framed in as many transfer
// frames as the negotiated frame size needs, so the protocol imposes no
// ceiling and the real one is the broker's own (Artemis's
// `max-size-bytes`/`min-large-message-size`, Classic's
// `wireFormat.maxFrameSize`), which a driver reads from the connection rather
// than guessing at.
func (amqp10) MaxMessageBytes() int64 { return 0 }

// stompPathPrefixes are STOMP's own destination forms, which an AMQP 1.0
// address must not be in.
var stompPathPrefixes = [...]string{"/topic/", "/queue/", "/temp-queue/"}

// Destination is the address an AMQP 1.0 link is attached to.
//
// On Artemis it is a bare address for a publication and an `address::queue`
// fully qualified queue name for a consumer; on Classic it is a
// `topic://`/`queue://` scheme form, which is what `activemq.Config.Scheme`
// asks `activemq.Derive` for. A name set derived for STOMP instead is REFUSED
// rather than sent: see [ErrWrongDestinationForm] for what accepting one costs.
func (amqp10) Destination(n activemq.Names, role Role) (string, error) {
	return destination(n, role, func(name string) bool {
		for _, p := range stompPathPrefixes {
			if strings.HasPrefix(name, p) {
				return true
			}
		}
		return false
	})
}

// Carry maps the message into the four sections.
func (amqp10) Carry(dst *Fields, m *mq.Message, o Options) error {
	return carry(dst, m, o, AMQP10)
}

// Restore is the receive direction.
func (amqp10) Restore(dst *mq.Message, f *Fields, o Options) error {
	return restore(dst, f, o, AMQP10)
}

// Support is this protocol's half of the capability table, for one
// broker.
//
// Both halves of the row matter. Where a feature is the BROKER's the two
// protocols agree and the answer still differs between Artemis and Classic —
// de-duplication, the fan-out mechanism, priority enforcement, topology
// management — and where a feature is the PROTOCOL's the answer differs from
// the STOMP arm on both brokers. `TestFeatureCoverageDiffers` is what keeps
// either difference from being quietly equalised.
func (p amqp10) Support(b activemq.Broker, f mq.Feature) mq.Support {
	if absentEverywhere(f) {
		return unsupported()
	}
	switch f {
	// --- Delivery and settlement, which is this protocol's strongest ground
	// and the half the STOMP arm cannot match.
	case mq.FeatureDurableStorage:
		return native(mq.ScopeTopic)
	case mq.FeatureAtLeastOnce:
		return native(mq.ScopeGroup)
	case mq.FeatureAtMostOnce:
		// A pre-settled transfer (`settled=true`) is at-most-once by
		// construction: the broker owes no disposition and the sender has
		// nothing to wait for.
		return native(mq.ScopeGroup)
	case mq.FeatureExplicitAck:
		return native(mq.ScopeGroup)
	case mq.FeatureNack:
		// Measured on BOTH brokers: a `modified{delivery-failed}`
		// disposition returns the delivery to the queue and raises the
		// header's `delivery-count`, which came back as 0, 1 and 2 across
		// three rounds. `released` returns it without raising the count.
		// This is the row the STOMP arm loses outright, and losing it is
		// what "feature completeness on Artemis" decides the family on.
		return native(mq.ScopeGroup)
	case mq.FeatureDelayedRedelivery:
		// Measured on Artemis: a `modified` disposition CARRYING
		// `x-opt-delivery-delay` is redelivered immediately, at 0 s, with
		// `delivery-count` 1. The annotation is honoured on a PUBLICATION
		// and ignored on a DISPOSITION, and Artemis's own redelivery delay
		// is an address-setting (`redelivery-delay`) rather than a per-nack
		// value — so there is no per-delivery delay to advertise, on either
		// broker, over either protocol.
		return unsupportedBecause(
			"measured on Artemis: a `modified` disposition carrying x-opt-delivery-delay is redelivered at once with delivery-count 1; the annotation is honoured on a publication and ignored on a disposition, and the broker's redelivery delay is an address-setting rather than a per-nack value",
		)
	case mq.FeatureDeadLetter:
		return conditional(mq.ScopeGroup, deadLetterNote(b))
	case mq.FeatureConsumerGroups:
		// Several receivers attached to one queue compete, which IS a
		// consumer group: on Artemis the queue is the shared durable
		// subscription named by the FQQN, on Classic it is the virtual
		// topic's consumer queue.
		return native(mq.ScopeGroup)
	case mq.FeatureDurableSubscriptions:
		return native(mq.ScopeGroup)
	case mq.FeatureFanOut:
		return fanOut(b)

	// --- Timing. This is own list: delay, groups, priority,
	// TTL.
	case mq.FeatureScheduledPublish:
		return nativeNote(mq.ScopeTopic, scheduleNote(b))
	case mq.FeatureMessageTTL:
		// The header section's `ttl` is a RELATIVE duration, so a per-message
		// TTL needs no clock at all on this arm. That is the difference
		// against STOMP, whose `expires` is an instant.
		return nativeNote(mq.ScopeTopic,
			"the AMQP 1.0 header section's ttl is a relative duration, so a per-message TTL needs no clock; `absolute-expiry-time` is set as well when the driver has resolved one")
	case mq.FeaturePriority:
		return priority(b)

	// --- Ordering. A queue is FIFO and a message group is how a key is kept
	// on one consumer.
	case mq.FeatureOrderedByKey:
		// Measured on Artemis: two competing consumers on one queue received
		// the `k0` messages exclusively on one and the `k1` messages
		// exclusively on the other, with the key carried as the `group-id`
		// property. That is the mechanism, and it is native.
		return nativeNote(mq.ScopeKey,
			"the key is carried as the AMQP 1.0 group-id property; measured on Artemis, two competing consumers on one queue received one key's messages exclusively on one of them and the other key's exclusively on the other")
	case mq.FeatureOrderedByPartition:
		return native(mq.ScopeGroup)
	case mq.FeatureOrderedGlobally:
		return unsupportedBecause(
			"one queue is FIFO and nothing above one queue is ordered: a multicast address fans a publication into several queues, each of which is consumed independently",
		)

	// --- Headers. Application properties are TYPED, which is the second row
	// the STOMP arm cannot match.
	case mq.FeatureNativeHeaders:
		return native(mq.ScopeTopic)
	case mq.FeatureBinaryHeaders:
		return native(mq.ScopeTopic)
	case mq.FeatureDuplicateHeaders:
		return unsupportedBecause(
			"the application-properties section is a map, so a second value under one key replaces the first",
		)

	// --- Batching.
	case mq.FeatureBatchPublish:
		return unsupportedBecause(
			"there is no batch frame: a batch is N transfers on one link, which the link's credit makes cheap and does not make one operation",
		)
	case mq.FeatureBatchReceive:
		// The link's CREDIT is the receive window: a receiver that issued
		// credit for N gets up to N transfers before it settles any, which
		// is what a batched receive reads. It is a protocol mechanism, and
		// that is the third row the STOMP arm cannot match.
		return native(mq.ScopeGroup)

	// --- De-duplication, which is Artemis's and not Classic's.
	case mq.FeatureIdempotentPublish:
		return unsupportedBecause(
			"neither broker has a producer epoch or sequence number, so a retried publication is a second message",
		)
	case mq.FeatureDeduplication:
		return deduplication(b)

	// --- Filtering. A subscription-time filter exists in AMQP 1.0 as a link
	// SOURCE filter, and `go-amqp` does not expose one.
	case mq.FeatureServerFilter:
		return unsupportedBecause(
			"an AMQP 1.0 subscription filter is a filter-set on the link's source, which github.com/Azure/go-amqp does not expose; the STOMP arm's `selector` header is what this driver pair has instead, and it works on Classic only",
		)

	// --- The rest.
	case mq.FeatureAdmin:
		return admin(b)
	case mq.FeatureRequestReply:
		// The `reply-to` property is native and a reply destination can be a
		// dynamic node the client asks the broker to create, but neither is
		// a broker-side request/reply MECHANISM in the sense the specification means:
		// there is no equivalent of RabbitMQ's direct reply-to, so the
		// correlation is the core's portable emulation and the specification forbids
		// advertising one as the other.
		return unsupportedBecause(
			"the reply-to property is native and the correlation is the core's portable emulation; neither broker has a request/reply mechanism of its own over this protocol",
		)
	case mq.FeaturePublisherConfirm:
		// A transfer on an unsettled link is resolved by its own
		// disposition, which is exactly the guarantee FeaturePublisherConfirm
		// names: the broker said it took this message.
		return native(mq.ScopeTopic)
	case mq.FeatureDurableConfirm:
		return conditional(mq.ScopeTopic,
			"the disposition says the broker took the message; it implies PERSISTENCE only for a durable message (header durable=true) on a durable queue whose address is not configured non-persistent")
	case mq.FeatureReceipts:
		// The disposition identifies the transfer it resolves, so what comes
		// back is the publication's own receipt. What does NOT come back is
		// a broker-assigned message identifier: neither broker sends one.
		return nativeNote(mq.ScopeTopic,
			"the disposition resolves the transfer it names, so the receipt is the publication's; neither broker returns a message identifier of its own")

	default:
		// A feature nobody answered. See [notConsidered]: it is a failure in
		// TestEveryFeatureIsConsidered rather than a silent absence in four
		// capability tables.
		return unsupportedBecause(notConsidered)
	}
}

// Features counts the natively supported features on b.
func (p amqp10) Features(b activemq.Broker) int { return countNative(p, b) }

// --- The rows that are facts about the BROKER rather than about the protocol.
//
// They are shared by the two arms because they really are the same answer
// twice: which product fans a publication out how, which one de-duplicates,
// which one enforces a priority, which one can be managed over the wire. Both
// arms call them, so a change to ActiveMQ's own behaviour is edited once —
// which is the drift `internal/rabbit/protocol` records as its reason for
// deriving one table from the other, arrived at from the other direction.

// fanOut is how each product delivers one publication to several
// subscriptions.
func fanOut(b activemq.Broker) mq.Support {
	if b == activemq.Artemis {
		return native(mq.ScopeTopic)
	}
	// Classic has no multicast address. Its fan-out is the VIRTUAL TOPIC
	// naming convention, under which the broker copies a publication into
	// every queue called `Consumer.<name>.VirtualTopic.<topic>` -- every
	// queue that ALREADY EXISTS, which is the condition: a subscription
	// created after a publication does not receive it, because there was no
	// queue to copy into.
	return conditional(mq.ScopeTopic,
		"Classic's fan-out is the virtual-topic convention: a publication is copied only into the Consumer.<name>.VirtualTopic.<topic> queues that already exist, so a subscription created after a publication does not receive it and a durable subscription has to be declared before the first publish")
}

// deduplication is Artemis's `_AMQ_DUPL_ID` and Classic's nothing.
func deduplication(b activemq.Broker) mq.Support {
	if b == activemq.Artemis {
		return nativeNote(mq.ScopeTopic,
			"Artemis de-duplicates a publication whose _AMQ_DUPL_ID it has already seen, within the broker's id-cache size; it is a publication-side promise and says nothing about a REDELIVERY")
	}
	return unsupportedBecause(
		"ActiveMQ Classic has no broker-side de-duplication: only Artemis reads _AMQ_DUPL_ID, so a de-duplication key is refused here rather than sent and ignored",
	)
}

// priority is the message priority, which both products carry and only one
// reorders without being configured to.
//
// Artemis honours a message's priority within an address by default. Classic
// carries the header and reorders a destination's messages only when its
// policy entry enables `prioritizedMessages`, which is the broker's own XML
// and not something a client can ask for — so the row is Conditional there
// rather than Native, and the family's coverage score on Classic is one lower
// for both protocols because of it. That is a documented fact about the
// products rather than something this increment measured.
func priority(b activemq.Broker) mq.Support {
	if b == activemq.Artemis {
		return native(mq.ScopeGroup)
	}
	return conditional(mq.ScopeGroup,
		"Classic carries the priority and reorders a destination's messages only when its policy entry sets prioritizedMessages, which is the broker's own configuration; without it the header travels and the order does not change")
}

// admin is topology management, which only Artemis offers over these two
// protocols.
func admin(b activemq.Broker) mq.Support {
	if b == activemq.Artemis {
		return conditional(mq.ScopeTopic,
			"Artemis manages topology through its `activemq.management` address, which a client addresses by sending a management message with a reply-to and the broker's management permissions; these drivers otherwise rely on the broker's auto-creation, which is also what makes a wrong-broker connection dangerous rather than merely broken")
	}
	return unsupportedBecause(
		"Classic has no management destination either protocol can address: a destination comes into being on first use and its policy lives in the broker's own XML",
	)
}

// deadLetterNote is each product's dead-letter destination, which is the
// broker's own policy rather than anything a client asks for.
func deadLetterNote(b activemq.Broker) string {
	if b == activemq.Artemis {
		return "the destination is the broker's own dead-letter address, set per address in Artemis's address-settings (`dead-letter-address`, with `max-delivery-attempts`); a driver that needs its own dead-letter topic publishes to it instead, which is the emulation and not this row"
	}
	return "the destination is Classic's own `ActiveMQ.DLQ`, or whatever the broker's `individualDeadLetterStrategy` names; the client does not choose it and a driver that needs its own dead-letter topic publishes to it instead"
}

// scheduleNote is the measured scheduled-delivery slot, which differs by
// protocol AND by broker and is the reason [DelaySlot] exists.
func scheduleNote(b activemq.Broker) string {
	if b == activemq.Artemis {
		return "measured: the delay is the message ANNOTATION x-opt-delivery-delay in whole milliseconds, or x-opt-delivery-time as absolute epoch milliseconds; both work with a plain string key and with an amqp.Symbol key, and AMQ_SCHEDULED_DELAY sent as an annotation is ignored"
	}
	return "measured: the delay is the APPLICATION PROPERTY AMQ_SCHEDULED_DELAY in whole milliseconds, or AMQ_SCHEDULED_TIME as absolute epoch milliseconds; the x-opt-delivery-delay annotation form Artemis reads is ignored here and the message is delivered immediately"
}
