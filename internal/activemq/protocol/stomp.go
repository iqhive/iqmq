package protocol

import (
	"strings"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/activemq"
)

// stomp is the STOMP 1.2 arm, over `github.com/go-stomp/stomp/v3`.
//
// It is not the default and the specification keeps it anyway: "STOMP: virtual topics,
// `client-individual` ack, selectors via typed config". The reason it is kept
// is the one row it WINS — a `selector` header really does filter on Classic,
// measured, and no AMQP 1.0 filter is reachable through `go-amqp` — and the
// reason it is not the default is the four rows it loses, one of which is
// dangerous rather than merely absent.
//
// # Carry and Restore ARE this driver's mapping
//
// Unlike the AMQP 1.0 arm, whose real mapping is `drivers/amqp10`'s `Mapper`,
// these two methods are what the STOMP driver runs on every publication and
// every delivery. So this is the half of the family that must allocate nothing
// per message, and `TestAllocs` holds it to zero in both directions.
//
// # Everything is a frame header, and every frame header is TEXT
//
// That one sentence is most of this arm's feature table. A STOMP frame header
// is a name and a value separated by ":" and terminated by a newline, both
// text, so:
//
//   - a header value that is not valid UTF-8 CANNOT be carried. It is refused
//     with [ErrTextOnly] rather than mangled, and `mq.FeatureBinaryHeaders` is
//     Unsupported. The same goes for a message key, which travels as a header
//     because neither protocol has a key.
//   - a header whose name collides with a frame header or with one of the
//     broker's own is refused with [ErrProtocolHeader] rather than shadowed,
//     and the same list filters a delivery's own headers back out. A Classic
//     delivery was measured carrying eleven of them.
//   - there is no `content-encoding` header at all, so that property travels
//     as the reserved entry [EncodingHeader].
//   - `expires` is an ABSOLUTE epoch-millisecond instant, so a relative TTL is
//     the driver's arithmetic against the injected clock and [Options.Expiry]
//     is where the result arrives.
//
// # The measured facts, against apache/activemq-artemis:2.44.0-alpine and
// apache/activemq-classic:6.2.0
//
//   - DELAY: `AMQ_SCHEDULED_DELAY`, whole milliseconds, works on BOTH brokers.
//     Measured at 2.5 s on Artemis, which echoes it on delivery as
//     `_AMQ_SCHED_DELIVERY` (an absolute epoch time), and at 2.6 s on Classic,
//     which echoes it as `scheduledJobId`.
//   - GROUPS: `JMSXGroupID` is accepted by both brokers. Artemis echoes it on
//     delivery as `_AMQ_GROUP_ID` and Classic does not echo it at all, so
//     [EchoedGroup] reads both spellings and promises neither.
//   - ATTEMPT: `redelivered` is a BOOLEAN, and neither broker sends a count —
//     measured, no `delivery-count`, no `JMSXDeliveryCount` and no
//     `_AMQ_DELIVERY_COUNT` on either. So the attempt is 1, or at least 2, and
//     the protocol cannot say more ([Attempt]).
//   - SELECTOR: the `selector` header works on Classic (a `colour = 'red'`
//     selector filtered a blue message out) and is IGNORED on Artemis when the
//     subscription is a fully qualified queue name (the blue message was
//     delivered) — which is exactly how these drivers address an Artemis
//     shared durable subscription, so it is ignored in the only shape this
//     driver uses.
//   - RECEIPTS: a `receipt` header on a SEND frame works on both. A `receipt`
//     header on a SUBSCRIBE frame is a DEFECT in go-stomp v3.1.5 — the library
//     registers the subscription's own frame channel under the receipt
//     identifier, the RECEIPT frame closes that channel, and the library then
//     panics with "send on closed channel" from its own `processLoop`. That is
//     the driver's problem and not this package's; what belongs here is that
//     `mq.FeatureReceipts` is about the PUBLISH path.
//   - NACK: a STOMP NACK frame returns NOTHING on either broker. See the
//     FeatureNack row below; it is the finding this increment exists for.
type stomp struct{}

var _ Protocol = stomp{}

// Kind returns STOMP.
func (stomp) Kind() Kind { return STOMP }

// MinBroker is the oldest release of each product whose STOMP transport this
// driver claims.
//
// On Artemis it is 2.7 for the same reason the AMQP 1.0 arm's floor is: the
// subscription is a FULLY QUALIFIED QUEUE NAME (`address::queue`), and 2.7.0
// is the release whose acceptors — the STOMP one included — accept one. On
// Classic it is 5.6, which added STOMP 1.2 and with it the
// `client-individual` acknowledgement mode and the `ack` header's message
// identifier that asks this driver to use.
func (stomp) MinBroker(b activemq.Broker) activemq.Version {
	switch b {
	case activemq.Artemis:
		return activemq.MinArtemisFQQN
	case activemq.Classic:
		return activemq.MinClassicSTOMP
	default:
		return activemq.Version{}
	}
}

// MaxMessageBytes is zero: a STOMP frame's body is bounded by the broker's own
// configuration (Artemis's `stompMinLargeMessageSize` and the acceptor's frame
// limits, Classic's `wireFormat.maxFrameSize`) and not by the protocol, so a
// driver reads it from the deployment rather than advertising a guess.
func (stomp) MaxMessageBytes() int64 { return 0 }

// Destination is the destination a STOMP frame names.
//
// On Artemis it is a bare address for a SEND and an `address::queue` fully
// qualified queue name for a SUBSCRIBE; on Classic it is the `/topic/` and
// `/queue/` PATH form, which is what `activemq.Config.STOMP` asks
// `activemq.Derive` for. A name set derived for AMQP 1.0 instead carries
// `topic://` or `queue://` and is REFUSED: see [ErrWrongDestinationForm] for
// what accepting one costs on Classic.
func (stomp) Destination(n activemq.Names, role Role) (string, error) {
	return destination(n, role, func(name string) bool {
		return strings.Contains(name, "://")
	})
}

// Carry maps the message into the frame's headers.
func (stomp) Carry(dst *Fields, m *mq.Message, o Options) error {
	return carry(dst, m, o, STOMP)
}

// Restore is the receive direction.
func (stomp) Restore(dst *mq.Message, f *Fields, o Options) error {
	return restore(dst, f, o, STOMP)
}

// Support is this protocol's half of the capability table, for one
// broker.
//
// Four rows differ from the AMQP 1.0 arm on BOTH brokers — Nack,
// BinaryHeaders, OrderedByKey and BatchReceive — and one differs between the
// two brokers in the opposite direction from every other row, because it is
// the only row STOMP wins: ServerFilter is Native on Classic and Unsupported
// on Artemis. That asymmetry is why ends "STOMP kept for
// Classic".
func (p stomp) Support(b activemq.Broker, f mq.Feature) mq.Support {
	if absentEverywhere(f) {
		return unsupported()
	}
	switch f {
	// --- Delivery and settlement. The positive half is ordinary; the
	// negative half is this increment's most important finding.
	case mq.FeatureDurableStorage:
		return native(mq.ScopeTopic)
	case mq.FeatureAtLeastOnce:
		return native(mq.ScopeGroup)
	case mq.FeatureAtMostOnce:
		// `ack: auto` settles on the broker as the frame leaves it.
		return native(mq.ScopeGroup)
	case mq.FeatureExplicitAck:
		// `ack: client-individual` plus an ACK frame naming the delivery's
		// own `ack` header, which is STOMP 1.2's message identifier.
		return native(mq.ScopeGroup)
	case mq.FeatureNack:
		// THE row. A STOMP NACK frame does not return a message on either
		// broker, and a driver that mapped `Nack` to one would SILENTLY
		// DESTROY messages while reporting a requeue.
		//
		// Measured on Artemis: one NACK and one ACK took
		// `getMessagesAcknowledged` from 0 to 2, the queue emptied, the
		// dead-letter queue did not grow and a re-subscribe saw nothing --
		// the NACK was treated exactly as an ACK. A transacted ACK followed
		// by ABORT is acknowledged anyway (`getMessagesAcknowledged` 1,
		// queue count 0), while a transacted SEND that is aborted IS
		// correctly discarded, so go-stomp's transactions do reach the
		// broker and Artemis's transacted acknowledgement does not roll
		// back. Measured on Classic: a NACK is a POISON_ACK and cannot be
		// transacted at all -- "Poison ack cannot be transacted" arrives as
		// an ERROR frame and closes the connection.
		//
		// So there is no negative settlement to advertise. The driver's
		// answer to `Nack` is to not acknowledge and let the subscription's
		// own redelivery return the message, which is a different promise
		// and is the driver's to describe.
		return unsupportedBecause(
			"measured on both brokers: a STOMP NACK frame returns no message. On Artemis it is treated as an ACK (one NACK and one ACK took getMessagesAcknowledged from 0 to 2, the queue emptied, the DLQ did not grow and a re-subscribe saw nothing), so a driver that mapped Nack to it would destroy messages while reporting a requeue; on Classic a NACK is a POISON_ACK and cannot be transacted at all",
		)
	case mq.FeatureDelayedRedelivery:
		// Unsupported for a simpler reason than the AMQP arm's: there is no
		// negative settlement at all, so there is nothing to delay.
		return unsupportedBecause(
			"there is no negative settlement over STOMP to attach a delay to (see the FeatureNack row), and both brokers' redelivery delay is a broker-side policy rather than a per-nack value",
		)
	case mq.FeatureDeadLetter:
		return conditional(mq.ScopeGroup, deadLetterNote(b))
	case mq.FeatureConsumerGroups:
		return native(mq.ScopeGroup)
	case mq.FeatureDurableSubscriptions:
		return native(mq.ScopeGroup)
	case mq.FeatureFanOut:
		return fanOut(b)

	// --- Timing: own list of delay, groups, priority, TTL.
	case mq.FeatureScheduledPublish:
		return nativeNote(mq.ScopeTopic,
			"measured on both brokers: the AMQ_SCHEDULED_DELAY frame header in whole milliseconds held a publication back 2.5 s on Artemis (echoed on delivery as _AMQ_SCHED_DELIVERY, an absolute epoch time) and 2.6 s on Classic (echoed as scheduledJobId); STOMP has no absolute form, so a caller's DeliverAt becomes a duration the driver resolved")
	case mq.FeatureMessageTTL:
		// `expires` is an ABSOLUTE epoch-millisecond instant and STOMP has
		// no relative form, so a per-message TTL is arithmetic somebody has
		// to do against a clock. It is the driver's, because this mapping is
		// pure and `internal/*` forbids reading the clock in it; a
		// publication with a TTL and no resolved expiry is refused with
		// ErrNeedsAbsoluteExpiry rather than published without one.
		return nativeNote(mq.ScopeTopic,
			"STOMP's `expires` header is an absolute epoch-millisecond instant and the protocol has no relative TTL, so a per-message TTL is the driver's arithmetic against the injected clock; a publication with a TTL and no resolved expiry is refused before any I/O rather than sent without it")
	case mq.FeaturePriority:
		return priority(b)

	// --- Ordering.
	case mq.FeatureOrderedByKey:
		// The mechanism is the same `JMSXGroupID` the AMQP arm carries as a
		// group-id property, and it is accepted by both brokers. What is
		// weaker here is the EVIDENCE and the feedback: the exclusive
		// assignment of a group to one consumer was measured over AMQP 1.0
		// on Artemis, and over STOMP Artemis echoes the group as
		// `_AMQ_GROUP_ID` while Classic echoes nothing -- so a consumer
		// cannot confirm which group a delivery belonged to on Classic at
		// all. A key that is not valid UTF-8 cannot be a group either.
		return conditional(mq.ScopeKey,
			"the key is carried as the JMSXGroupID frame header, which both brokers accept; the exclusive assignment of a group to one consumer was measured over AMQP 1.0, and over STOMP Artemis echoes the group as _AMQ_GROUP_ID while Classic echoes nothing, so a consumer cannot confirm the group it received. A key that is not valid UTF-8 cannot be carried as a frame header at all")
	case mq.FeatureOrderedByPartition:
		return native(mq.ScopeGroup)
	case mq.FeatureOrderedGlobally:
		return unsupportedBecause(
			"one queue is FIFO and nothing above one queue is ordered",
		)

	// --- Headers, which is where this protocol pays for being text.
	case mq.FeatureNativeHeaders:
		return native(mq.ScopeTopic)
	case mq.FeatureBinaryHeaders:
		return unsupportedBecause(
			"a STOMP frame header is a text name and a text value, so a header value that is not valid UTF-8 cannot be carried; it is refused before any I/O rather than mangled, and MetadataPortableEnvelope or the AMQP 1.0 arm is the route that carries one",
		)
	case mq.FeatureDuplicateHeaders:
		// Worse than the AMQP arm's map, and worth stating as such: the
		// frame CAN carry a repeated header name, so the second value goes
		// on the wire, and STOMP 1.2 then says the FIRST occurrence wins --
		// so the broker discards it and nothing reports that it did.
		return unsupportedBecause(
			"a STOMP frame can carry a repeated header name, so a duplicate is put on the wire and then discarded: STOMP 1.2 says the first occurrence wins, and nothing reports the loss. The promise is kept by MetadataPortableEnvelope instead",
		)

	// --- Batching.
	case mq.FeatureBatchPublish:
		return unsupportedBecause(
			"there is no batch frame: a batch is N SEND frames on one connection",
		)
	case mq.FeatureBatchReceive:
		// STOMP has no prefetch frame of its own. The receive window is a
		// broker-specific SUBSCRIBE header, which is a condition rather than
		// a protocol feature -- and it is the fourth row the AMQP 1.0 arm
		// wins, where the window is the link's own credit.
		return conditional(mq.ScopeGroup,
			"STOMP has no flow-control frame: the receive window is a broker-specific SUBSCRIBE header (Artemis's consumer-window-size, Classic's activemq.prefetchSize) rather than a protocol mechanism, so a portable driver cannot set it from the protocol alone")

	// --- De-duplication.
	case mq.FeatureIdempotentPublish:
		return unsupportedBecause(
			"neither broker has a producer epoch or sequence number, so a retried publication is a second message",
		)
	case mq.FeatureDeduplication:
		return deduplication(b)

	// --- Filtering: the one row this arm wins, and it wins it on one broker.
	case mq.FeatureServerFilter:
		if b == activemq.Classic {
			return nativeNote(mq.ScopeGroup,
				"measured on Classic: a `selector` header on the SUBSCRIBE frame filters broker-side -- a `colour = 'red'` selector kept a blue message out of the subscription. This is the row that makes end \"STOMP kept for Classic\"")
		}
		return unsupportedBecause(
			"measured on Artemis: a `selector` header is IGNORED when the subscription is a fully qualified queue name -- the blue message a `colour = 'red'` selector should have excluded was delivered -- and an FQQN subscription is the only shape this driver uses on Artemis",
		)

	// --- The rest.
	case mq.FeatureAdmin:
		return admin(b)
	case mq.FeatureRequestReply:
		return unsupportedBecause(
			"the `reply-to` header is native and a temporary reply destination can be created (`/temp-queue/<name>`), but the correlation is the core's portable emulation rather than a broker mechanism",
		)
	case mq.FeaturePublisherConfirm:
		// The `receipt` header on a SEND frame, answered by a RECEIPT frame.
		// Measured working on both brokers, which is the guarantee
		// FeaturePublisherConfirm names: the broker said it took this
		// message.
		return native(mq.ScopeTopic)
	case mq.FeatureDurableConfirm:
		return conditional(mq.ScopeTopic,
			"the RECEIPT frame says the broker took the message; it implies PERSISTENCE only for a publication sent with `persistent: true` to a destination the broker stores, so the durable boundary is the destination's own configuration")
	case mq.FeatureReceipts:
		// It is the PUBLICATION's receipt and not the broker's identifier
		// for the message: the `receipt` id is the client's own, and what
		// comes back is a RECEIPT frame naming it. (A `receipt` on a
		// SUBSCRIBE frame is a go-stomp v3.1.5 defect -- see this file's own
		// doc comment -- and is the driver's problem, not this row's.)
		return nativeNote(mq.ScopeTopic,
			"measured on both brokers: a `receipt` header on a SEND frame is answered with a RECEIPT frame naming it, so what is returned is the publication's own receipt and not a broker-assigned message identifier")

	default:
		// A feature nobody answered. See [notConsidered].
		return unsupportedBecause(notConsidered)
	}
}

// Features counts the natively supported features on b.
func (p stomp) Features(b activemq.Broker) int { return countNative(p, b) }
