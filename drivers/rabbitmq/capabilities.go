package rabbitmq

import (
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/rabbit"
	"bitbucket.org/iqhive/iqmq/internal/rabbit/delay"
	"bitbucket.org/iqhive/iqmq/internal/rabbit/protocol"
)

// facts are what the connected broker and this client's own configuration say,
// and they are the only inputs to the capability table.
//
// They are a value and not a set of reads against the Config because the specification
// makes a capability table constant per client: it is built once, at open,
// from facts that are already known, and nothing afterwards can change it. The
// two facts that come from the BROKER rather than from the configuration —
// the version and whether the delayed-message plugin is installed — are read
// once at open and are why [Backend.Facts] exists.
type facts struct {
	// version is what the broker reported in `connection.start`. The zero
	// value is "it did not say", which satisfies no version gate.
	version rabbit.Version
	// quorum reports that this client declares quorum queues, which is where
	// `x-delivery-count` comes from and therefore whether Attempt is
	// available at all.
	quorum bool
	// declare reports that this client may create its own topology, which
	// several emulations need: a delayed redelivery needs a delay queue, and
	// a dead-letter route needs an exchange and a queue.
	declare bool
	// deadLetter reports that the subscriptions' queues are pointed at a
	// dead-letter exchange.
	deadLetter bool
	// deadLetterTopic is the TOPIC dead-lettered messages go to, or empty
	// for the derived `<queue>.dlq`. It changes what the claim's note says,
	// because the two destinations differ in something a caller acts on: one
	// is consumable through the portable API and the other is not.
	deadLetterTopic string
	// delayPlugin reports that the broker has the delayed-message exchange,
	// which is the better arm and is detected rather than
	// assumed.
	delayPlugin bool
	// delayKind is the strategy this client uses for a delayed redelivery.
	delayKind delay.Kind
	// maxPriority is `x-max-priority` on the subscriptions' queues. Zero
	// means the queues were not declared with one, and a priority on a
	// message to such a queue is ignored by the broker.
	maxPriority uint8
	// keyAsRoutingKey reports that the message key is the routing key, which
	// is what makes ordering by key expressible.
	keyAsRoutingKey bool
	// mandatory reports that publications carry the `mandatory` flag, so an
	// unroutable one is Rejected rather than silently discarded.
	mandatory bool
	// maxMessageBytes is the broker's own `max_message_size` when it
	// reported one, and zero otherwise.
	maxMessageBytes int64
	// frameMax is the negotiated frame size, which bounds nothing a caller
	// can act on but is worth reporting.
	frameMax int
}

// capabilities builds the table (ADR-007).
//
// It starts from `internal/rabbit/protocol`'s answer for AMQP 0-9-1 — the
// PROTOCOL's feature subset, which is the criterion and is
// shared with the AMQP 1.0 profile so that the two cannot drift — and narrows
// it for what this client's own configuration and this broker's own version
// cannot do. It never widens one: a driver that claimed more than its protocol
// offers would be claiming something no code implements, and
// TestDriverNarrowsAndNeverWidensTheProtocol is what holds it to that.
//
// Narrowing rather than a table of its own is the design decision worth
// naming. The obvious alternative is one flat table per driver, and it drifts:
// two drivers over one broker would answer differently about the broker's own
// behaviour, and the half that drifted would be the one with fewer users.
func capabilities(f facts) (mq.Capabilities, error) {
	proto, err := protocol.New(protocol.AMQP091)
	if err != nil {
		return mq.Capabilities{}, err
	}
	b := driverkit.NewCapabilities()
	for i := range mq.FeatureCount {
		feature := mq.Feature(i)
		b.Support(feature, narrow(feature, proto.Support(feature), f))
	}
	b.Limits(limitsOf(f))
	return b.Build()
}

// narrow applies this client's own configuration to one of the protocol's
// answers.
//
// Every case is a fact about THIS client rather than about the protocol, and
// every one of them can only reduce the claim. The default is the protocol's
// own answer unchanged.
func narrow(feature mq.Feature, s mq.Support, f facts) mq.Support {
	switch feature {
	// --- The features that need topology this client may not create.
	case mq.FeatureDeadLetter:
		if !f.deadLetter {
			return mq.Support{Level: mq.Unsupported}
		}
		if f.deadLetterTopic != "" {
			return mq.Support{
				Level: mq.Native, Scope: mq.ScopeGroup,
				Note: "a rejected or exhausted message is routed to the exchange of Config.DeadLetterTopic, so every subscription of that topic receives it",
			}
		}
		return mq.Support{
			Level: mq.Native, Scope: mq.ScopeGroup,
			Note: "a rejected or exhausted message is routed to `<queue>.dlx` and held in `<queue>.dlq`, which is a queue and not a topic: it is read with `rabbitmqctl` or by a consumer of its own, not through mq.Client. Set Config.DeadLetterTopic for a destination the portable API can consume",
		}
	case mq.FeatureDelayedRedelivery:
		return narrowDelay(f)
	case mq.FeatureScheduledPublish:
		if !f.delayPlugin {
			// Without the plugin there is no way to schedule a PUBLICATION
			// at all: the TTL-and-dead-letter mechanism can delay a
			// redelivery to one queue, and a scheduled publication has to
			// reach every subscription of the topic, which would need a
			// delay queue per subscription and would deliver at different
			// moments to each.
			return mq.Support{Level: mq.Unsupported}
		}
		return mq.Support{
			Level: mq.Conditional, Scope: mq.ScopeTopic,
			Note: "the broker has the rabbitmq_delayed_message_exchange plugin and the publication goes through `<exchange>.delayed`; the plugin holds a delayed message in Mnesia and delivers it when the node comes back rather than at the moment it was due if the node was down",
		}

	// --- The features that need a queue argument.
	case mq.FeaturePriority:
		if f.maxPriority == 0 {
			// The broker IGNORES a priority on a message to a queue that
			// was not declared with `x-max-priority`, which is the worst
			// kind of unsupported: the publication succeeds and the
			// ordering is not what the caller asked for. Advertising it
			// Unsupported is what makes the core refuse the publication
			// instead.
			return mq.Support{Level: mq.Unsupported}
		}
		return mq.Support{
			Level: mq.Conditional, Scope: mq.ScopeGroup,
			Note: "the subscription's queue was declared with x-max-priority, so a priority above it is clamped by the broker",
		}

	// --- The features that need a configuration choice.
	case mq.FeatureOrderedByKey:
		if !f.keyAsRoutingKey {
			// With the key in a header the broker cannot read it, so two
			// messages under one key can land in different queues of a
			// consistent-hash exchange or be interleaved by two publishers.
			// The specification makes that not ordered, and saying so is what stops a
			// caller planning on it.
			return mq.Support{Level: mq.Unsupported}
		}
		return mq.Support{
			Level: mq.Conditional, Scope: mq.ScopeKey,
			Note: "the message key is the routing key (Config.KeyAsRoutingKey), so one key's messages reach one queue and are delivered in order while a single consumer settles them in order",
		}

	case mq.FeatureAtMostOnce:
		// The PROTOCOL has it: `basic.consume` takes a `no-ack` flag, and a
		// no-ack consumer is at-most-once. This driver never sets it --
		// `autoAck` is false on every consume, because the explicit
		// settlement is what a Settler is for -- so the claim would be about
		// a code path that does not exist.
		//
		// It is the one row this driver withdraws for no reason but its own
		// implementation, which is why it is stated here rather than in the
		// protocol's table: the AMQP 1.0 profile withdraws it too, and both
		// for the same reason.
		return mq.Support{
			Level: mq.Unsupported,
			Note:  "`basic.consume` has a no-ack mode and this driver never uses it: every consumer settles explicitly, so a delivery is at-least-once and never at-most-once",
		}

	// --- The features whose availability is a version gate.
	case mq.FeatureAtLeastOnce, mq.FeatureExplicitAck, mq.FeatureNack:
		if !f.quorum {
			// A classic queue has at-least-once delivery and explicit
			// settlement too; what it does NOT have is the redelivery
			// counter, so the note changes rather than the level.
			return mq.Support{
				Level: s.Level, Scope: s.Scope,
				Note: "this client declares classic queues, which do not carry x-delivery-count, so a delivery's Attempt is zero unless it is a delayed redelivery this driver parked and re-published, whose attempt the driver carries itself; the message contract's MaxAttempts therefore cannot be enforced from the broker's own count",
			}
		}
		return s

	// --- The surfaces this driver does not implement, whatever the protocol
	// offers.
	case mq.FeatureBatchSettle:
		// `basic.ack{multiple=true}` exists and this driver does not use it:
		// it settles every delivery up to a tag, which is safe only when
		// deliveries are settled in the order they arrived, and the core
		// makes no such promise. The specification says "individual acks unless
		// cumulative proven safe" and it is not proven.
		return mq.Support{Level: mq.Unsupported}
	case mq.FeatureAtomicBatchPublish:
		// There is no batch frame in AMQP 0-9-1 at all, so there is nothing
		// that could be atomic. `tx.select` would be the only candidate and
		// this driver does not use it (see FeatureTransactions).
		return mq.Support{Level: mq.Unsupported}
	case mq.FeatureBatchPublish:
		// There is no batch frame either, and the batch still WORKS: the core
		// loops, which is the answer for a driver with no native batch,
		// and this driver confirms every publication of the loop. So the
		// claim is an EMULATION and not an absence -- a driver that said
		// Unsupported here would be refuting something a caller can do, which
		// the conformance kit catches ("the driver advertises no BatchPublish
		// and the feature worked anyway").
		//
		// The LEVEL follows the durability of the confirm, which is the same
		// distinction FeatureDurableConfirm turns on: on a quorum queue a
		// confirm means a majority of replicas has the message on disk, and
		// on a classic queue it means one node accepted it.
		level := mq.EmulatedVolatile
		if f.quorum {
			level = mq.EmulatedDurable
		}
		return mq.Support{
			Level: level, Scope: mq.ScopeTopic,
			Note: "AMQP 0-9-1 has no batch frame: the core publishes every message of the batch and this driver confirms each, so a batch is NOT atomic -- a failure part-way through leaves the messages before it published. A windowed or asynchronous confirm strategy (the specification) is what makes the loop cost one round trip instead of N",
			// See for why this is an emulation rather than
			// an absence, and TestDriverNarrowsAndNeverWidensTheProtocol for
			// the one widening the narrowing rule allows.
		}
	case mq.FeatureWildcardSubscribe:
		// The PROTOCOL has wildcards -- a topic exchange's binding key is a
		// pattern, which is why `internal/rabbit/protocol` answers native --
		// and this driver's TOPOLOGY cannot use them for the thing the specification's
		// FeatureWildcardSubscribe names. A topic maps to its own durable
		// exchange, so a subscription to `orders.*` would have to bind one
		// queue to every exchange whose name matches, including exchanges
		// created later, and a binding names one exchange.
		//
		// Mapping every topic onto one shared exchange with the topic as the
		// routing key WOULD make it work, and it was rejected: one exchange
		// is one authorisation boundary, so every publisher would need write
		// on every topic, and `rabbitmqctl list_exchanges` would no longer
		// show what exists. The pattern power is still reachable inside one
		// topic through Config.KeyAsRoutingKey and a binding of the
		// operator's own, which the driver page shows.
		//
		// The refusal is `errNoWildcards` and it is ErrUnsupported, which is
		// what makes this negative claim provable in the conformance kit.
		return mq.Support{Level: mq.Unsupported}

	// --- Admin, which this driver implements.
	case mq.FeatureAdmin:
		if !f.declare {
			return mq.Support{
				Level: mq.Conditional, Scope: mq.ScopeTopic,
				Note: "Client.Admin can inspect, purge and delete; it refuses to CREATE unless Config.Declare is set, because the API forbids creating production topology behind the caller's back",
			}
		}
		return mq.Support{Level: mq.Native, Scope: mq.ScopeTopic}

	default:
		return s
	}
}

// narrowDelay is the delayed-redelivery claim, which has three answers rather
// than two and is the most conditional row in the table.
//
// It is separate because the whole subject is this row: the
// mechanism is emulated rather than native, the emulation differs, and the specification
// requires an emulation to say what it is.
func narrowDelay(f facts) mq.Support {
	switch {
	case !f.declare:
		// Both mechanisms need topology this client may not create.
		return mq.Support{Level: mq.Unsupported}
	case f.delayKind.Resolve() == delay.Plugin && f.delayPlugin:
		return mq.Support{
			Level: mq.EmulatedDurable, Scope: mq.ScopeGroup,
			Note: "the message is acknowledged and re-published to `<exchange>.delayed` with an x-delay header, which the rabbitmq_delayed_message_exchange plugin holds for exactly that long (kind plugin)",
		}
	case f.delayKind.Resolve() == delay.Plugin:
		// The configuration asked for the plugin and the broker does not
		// have it. Claiming the fallback would be claiming a mechanism this
		// client will not use.
		return mq.Support{Level: mq.Unsupported}
	default:
		return mq.Support{
			Level: mq.EmulatedDurable, Scope: mq.ScopeGroup,
			Note: "the message is acknowledged and parked in a durable queue whose x-message-ttl is the delay and whose dead-letter route is this subscription's own queue, so the delay is rounded UP to the next bucket of the configured ladder (kind ttldlx)",
		}
	}
}

// limitsOf is the numeric half of the table.
//
// Only what the broker really said is advertised. RabbitMQ's
// `max_message_size` is not in the connection's own properties — it is a
// server configuration readable through the management API — so a driver that
// advertised a number here would be advertising a guess, and the specification makes an
// unadvertised limit distinct from one advertised as zero.
func limitsOf(f facts) mq.Limits {
	var l mq.Limits
	if f.maxMessageBytes > 0 {
		l.MaxMessageBytes = mq.Limit[int64]{Value: f.maxMessageBytes, Known: true}
	}
	// The header ceiling IS known, and it is the protocol's rather than the
	// broker's: a field-table key is a shortstr, so 255 bytes is the most one
	// can carry however the broker is configured.
	l.MaxHeaderBytes = mq.Limit[int64]{Value: rabbit.MaxNameLen, Known: true}
	if f.declare {
		// The delay ceiling is the strategy's, and it is only meaningful
		// when a delay is available at all.
		if st, err := delay.New(f.delayKind, delay.Config{}); err == nil {
			l.MaxDelay = mq.Limit[time.Duration]{Value: st.MaxDelay(), Known: true}
		}
	}
	return l
}
