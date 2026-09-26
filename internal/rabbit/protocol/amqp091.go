package protocol

import (
	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/rabbit"
)

// amqp091 is AMQP 0-9-1, the protocol every RabbitMQ speaks and the reference
// kind.
//
// It is the default for one reason that settles the family before any
// measurement: it is the only arm that works on every broker in the version
// matrix. The expected outcome is "0-9-1 default; 1.0 conditional profile",
// and the specification adds that the 1.0 profile is "never silently substituted" — so a
// caller who asked for RabbitMQ gets this, and the other arm is a module they
// import deliberately.
//
// Its message model is fourteen fixed `basic.properties` fields and one field
// table. The fixed fields cover most of the typed properties exactly, which
// is why this protocol needs no envelope for an ordinary message; what the
// table cannot do is carry a repeated key, because it is a map.
type amqp091 struct{}

var _ Protocol = amqp091{}

// Kind returns AMQP091.
func (amqp091) Kind() Kind { return AMQP091 }

// MinBroker is the oldest RabbitMQ this driver claims to work against, which
// is the oldest image the version matrix runs rather than the oldest broker
// that speaks the protocol.
//
// The distinction is deliberate: AMQP 0-9-1 has not changed since 2008 and
// this driver would very likely work against RabbitMQ 3.8, but nothing in the
// repository RUNS 3.8, so claiming it would be a claim nobody checked. The specification's
// evidence rule applies to a version gate as much as to a capability.
func (amqp091) MinBroker() rabbit.Version { return rabbit.MinBrokerSupported }

// MaxMessageBytes is zero: AMQP 0-9-1 frames a body in as many content frames
// as it takes, so the protocol imposes no ceiling of its own and the real one
// is the broker's `max_message_size`, which the driver reads from the
// connection rather than guessing at.
func (amqp091) MaxMessageBytes() int64 { return 0 }

// Address is `basic.publish`'s exchange and routing key, or `basic.consume`'s
// queue.
func (amqp091) Address(n rabbit.Names, role Role) (Address, error) {
	switch role {
	case Consume:
		if n.Queue == "" {
			return Address{}, ErrNoAddress
		}
		return Address{Queue: n.Queue, Exchange: n.Exchange, RoutingKey: n.RoutingKey}, nil
	case Publish:
		if n.Exchange == "" {
			return Address{}, ErrNoAddress
		}
		return Address{Exchange: n.Exchange, RoutingKey: n.RoutingKey}, nil
	default:
		return Address{}, ErrNoAddress
	}
}

// Carry maps the message into the fourteen fixed properties and the field
// table.
func (amqp091) Carry(dst *Fields, m *mq.Message, o Options) error {
	return carry(dst, m, o, true)
}

// Restore is the receive direction.
func (amqp091) Restore(dst *mq.Message, f *Fields, o Options) error {
	return restore(dst, f, o)
}

// Support is this protocol's half of the capability table.
//
// Every feature of the inventory is answered, and the ones that differ from
// the AMQP 1.0 profile are commented where they differ: that list IS the specification's
// "feature subset" criterion, and the two tables are held apart by
// TestFeatureSubsetsDiffer.
func (p amqp091) Support(f mq.Feature) mq.Support {
	switch f {
	// --- The settlement model, which is this protocol's strongest ground.
	case mq.FeatureDurableStorage:
		return native(mq.ScopeTopic)
	case mq.FeatureAtLeastOnce:
		return native(mq.ScopeGroup)
	case mq.FeatureAtMostOnce:
		return native(mq.ScopeGroup)
	case mq.FeatureExplicitAck:
		return native(mq.ScopeGroup)
	case mq.FeatureNack:
		return native(mq.ScopeGroup)
	case mq.FeatureDeadLetter:
		return native(mq.ScopeGroup)
	case mq.FeatureConsumerGroups:
		// A queue with several consumers IS a consumer group: the broker
		// hands each message to one of them.
		return native(mq.ScopeGroup)
	case mq.FeatureDurableSubscriptions:
		return native(mq.ScopeGroup)
	case mq.FeatureFanOut:
		// One queue per subscription bound to one exchange.
		return native(mq.ScopeTopic)

	// --- Publisher confirms. This is the sharpest difference between the two
	// arms: AMQP 0-9-1 has a confirm MODE with its own frames, three
	// strategies and a cumulative form, and AMQP 1.0 has a
	// per-delivery disposition instead.
	case mq.FeaturePublisherConfirm:
		return native(mq.ScopeTopic)
	case mq.FeatureDurableConfirm:
		return conditional(mq.ScopeTopic,
			"a publisher confirm on a quorum queue is returned after the message is written to a majority of the queue's replicas; on a classic non-durable queue it is returned once the broker holds it in memory, so the durable boundary needs a durable quorum queue")
	case mq.FeatureReceipts:
		return native(mq.ScopeTopic)

	// --- Request/reply. Direct reply-to (`amq.rabbitmq.reply-to`) is a
	// RabbitMQ extension of AMQP 0-9-1 and has no AMQP 1.0 equivalent, so
	// this is the second sharp difference.
	case mq.FeatureRequestReply:
		return native(mq.ScopeTopic)

	// --- Headers. A field table carries a byte array natively, so a binary
	// header value survives; what it cannot do is carry a repeated key,
	// because it is a map.
	case mq.FeatureNativeHeaders:
		return native(mq.ScopeTopic)
	case mq.FeatureBinaryHeaders:
		return native(mq.ScopeTopic)
	case mq.FeatureDuplicateHeaders:
		return unsupported()

	// --- Timing.
	case mq.FeatureMessageTTL:
		return native(mq.ScopeTopic)
	case mq.FeatureDelayedRedelivery:
		return conditional(mq.ScopeGroup,
			"a delayed redelivery is built from a TTL queue that dead-letters back, or from the rabbitmq_delayed_message_exchange plugin when it is installed; neither is a protocol feature")
	case mq.FeatureScheduledPublish:
		return conditional(mq.ScopeTopic,
			"a scheduled publication needs the rabbitmq_delayed_message_exchange plugin, which is not part of the official image")
	case mq.FeatureExtendLease:
		// There is no such frame. A delivery's lease is the channel's
		// lifetime, so there is nothing to extend and nothing to expire.
		return unsupported()

	// --- Ordering. A queue is FIFO, so one queue's messages are ordered;
	// nothing above one queue is.
	case mq.FeatureOrderedByPartition:
		return native(mq.ScopeGroup)
	case mq.FeatureOrderedByKey:
		return conditional(mq.ScopeTopic,
			"messages under one key are ordered when the key is the routing key and one queue receives them (WithKeyAsRoutingKey), or through a consistent-hash exchange; with the key in a header the broker does not read it")
	case mq.FeatureOrderedGlobally:
		return unsupported()

	// --- Priority, which is a queue argument rather than a protocol feature.
	case mq.FeaturePriority:
		return conditional(mq.ScopeGroup,
			"the queue must be declared with x-max-priority; a priority on a message to a queue that was not is ignored by the broker")

	// --- History. A queue has none: a delivered and acknowledged message is
	// gone, so there is nothing to replay and no position to seek to.
	case mq.FeatureReplay, mq.FeatureSeekByTime, mq.FeatureSeekBySequence:
		return unsupported()

	// --- Batching. AMQP 0-9-1 has no batch frame: a batch is N publishes on
	// one channel, which the confirm window makes cheap but not atomic.
	case mq.FeatureBatchPublish:
		return unsupported()
	case mq.FeatureAtomicBatchPublish:
		return unsupported()
	case mq.FeatureBatchReceive:
		// `basic.qos` prefetch lets the broker push several deliveries
		// before any is acknowledged, which is what a batched receive reads.
		return native(mq.ScopeGroup)
	case mq.FeatureBatchSettle:
		// `basic.ack{multiple=true}` settles every delivery up to a tag.
		return conditional(mq.ScopeGroup,
			"basic.ack carries a multiple flag that settles every delivery up to a tag, which is safe only when the deliveries are settled in the order they arrived; this driver settles individually unless cumulative settlement is proven safe for the subscription")

	// --- Exactly-once and de-duplication, neither of which this broker has.
	case mq.FeatureExactlyOnceDelivery, mq.FeatureIdempotentPublish, mq.FeatureDeduplication:
		return unsupported()
	case mq.FeatureTransactions:
		// `tx.select` exists in the protocol and is not implemented by this
		// driver: it serialises the channel and its guarantees are weaker
		// than publisher confirms, which is why the RabbitMQ team
		// discourages it.
		return unsupported()

	// --- Filtering and patterns.
	case mq.FeatureWildcardSubscribe:
		return native(mq.ScopeGroup)
	case mq.FeatureServerFilter:
		// A headers exchange filters on the header table, but a filter is
		// then a BINDING rather than a subscription-time expression, which
		// is not the shape FeatureServerFilter describes.
		return unsupported()

	// --- The rest.
	case mq.FeatureAdmin:
		return native(mq.ScopeTopic)
	case mq.FeatureNativeTracing:
		return unsupported()
	default:
		return unsupported()
	}
}

// Features counts the natively supported features, which is the specification's "feature
// subset" as one number.
func (p amqp091) Features() int { return countNative(p) }
