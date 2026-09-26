package pulsar

import (
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The ordering notes, written once because each of them is a claim this driver
// has to be able to defend and because the driver page quotes them verbatim.
const (
	// keySharedOrderNote is what Key_Shared with AllowOutOfOrderDelivery
	// false actually promises.
	keySharedOrderNote = "a Key_Shared subscription sends every message of one key to one consumer and delivers them in publication order, and AllowOutOfOrderDelivery is false, so Pulsar BLOCKS a reassigned hash range rather than handing it over early. Two things still end the promise and neither is under the driver's control: a consumer joining or leaving moves hash ranges, and adding partitions to the topic rehashes every key onto a different partition, after which a key's older and newer messages are in two partitions with no order between them (TestCriticalPartitionExpansion)"
	// exclusiveOrderNote is what Exclusive and Failover promise.
	exclusiveOrderNote = "one active consumer receives each partition's messages in publication order. On a non-partitioned topic that is the whole topic's order; on a partitioned one it is per partition, and adding partitions rehashes every key. A negative settlement reorders by construction — the message comes back after the ones behind it "
	// globalOrderNote is the global claim's condition.
	globalOrderNote = "only a NON-PARTITIONED topic has a global order, and only an Exclusive or Failover subscription has one consumer to deliver it to. A partitioned topic has no order between partitions, and this driver cannot tell the two apart when the capability table is built — so the claim is conditional on the topology the caller creates"
)

// capabilities builds the driver table once, at open.
//
// Every claim is either something Pulsar does itself or something stated as
// unsupported; the two emulations are declared as emulations and each says what
// it is. The table depends on the configuration in five places, and each of
// them is a real difference in what the broker will do:
//
//   - [Config.SubscriptionType] decides EVERY ordering claim, and also whether
//     a second consumer of one subscription is a competitor, a standby or a
//     refusal. It is the whole subject of this increment.
//   - [Config.Domain] decides durability: a non-persistent topic is
//     acknowledged and never stored, so neither DurableStorage nor
//     DurableConfirm can be claimed for it.
//   - [Config.DeadLetterTopic] decides DeadLetter. Without a destination there
//     is nowhere for a rejected message to go, and a driver that invented one
//     would destroy a message the caller believed was routed.
//   - [Config.Metadata] decides the two header claims the envelope carries,
//     which is the emulation for a carrier that cannot hold them —
//     advertising them as unsupported while the envelope carried them would be
//     the mirror image of the silent emulation the specification forbids.
//   - [Config.RetryEnable] decides both DelayedRedelivery's LEVEL and whether
//     a seek is possible at all: a consumer with the retry topic enabled is a
//     MULTI-TOPIC consumer, and pulsar-client-go refuses Seek and SeekByTime on
//     one.
//
// cfg is the RESOLVED configuration.
func capabilities(cfg Config) (mq.Capabilities, error) {
	b := driverkit.NewCapabilities()
	enveloped := cfg.Metadata == mq.MetadataPortableEnvelope || cfg.Metadata == mq.MetadataAdaptive
	persistent := cfg.Domain != DomainNonPersistent
	sub := cfg.SubscriptionType

	// Delivery and settlement.
	b.Native(mq.FeatureAtLeastOnce, mq.ScopeTopic)
	b.Native(mq.FeatureExplicitAck, mq.ScopeTopic)
	b.Native(mq.FeatureNack, mq.ScopeTopic)
	b.Native(mq.FeatureDurableSubscriptions, mq.ScopeGroup)
	b.Native(mq.FeatureFanOut, mq.ScopeTopic)
	b.Native(mq.FeatureBatchPublish, mq.ScopeTopic)
	b.Native(mq.FeatureBatchReceive, mq.ScopeTopic)
	b.Native(mq.FeatureBatchSettle, mq.ScopeTopic)
	b.Native(mq.FeatureWildcardSubscribe, mq.ScopeTopic)
	b.Native(mq.FeatureReceipts, mq.ScopeTopic)
	b.Support(mq.FeaturePublisherConfirm, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "Producer.Send returns the broker's send receipt, which for a persistent topic is given after the entry has been written to the configured write quorum of bookies",
	})
	b.Support(mq.FeatureNativeHeaders, mq.Support{
		Level: mq.Native, Scope: mq.ScopeTopic,
		Note: "a header is a Pulsar property. Properties are a map of strings, so a header's VALUE and KEY travel and the insertion ORDER of two different keys does not; MetadataPortableEnvelope is what keeps the order",
	})

	// Durability, which is the domain's answer and not a choice.
	if persistent {
		b.Native(mq.FeatureDurableStorage, mq.ScopeTopic)
		b.Support(mq.FeatureDurableConfirm, mq.Support{
			Level: mq.Native, Scope: mq.ScopeTopic,
			Note: "a persistent topic's send receipt is given after the entry is written to the write quorum, so ConfirmDurable and ConfirmAccepted are the same round trip here",
		})
	} else {
		b.Unsupported(mq.FeatureDurableStorage)
		b.Unsupported(mq.FeatureDurableConfirm)
	}

	// Scheduling. Pulsar's delayed delivery is the broker DISPATCHER's, and
	// the dispatcher that has it is the one a Shared or Key_Shared
	// subscription uses: an Exclusive or Failover subscription receives a
	// delayed message as soon as it reaches the topic. So the claim — and
	// the refusal of a delayed publication — follow the subscription type.
	if sub.Exclusive() {
		b.Unsupported(mq.FeatureScheduledPublish)
	} else {
		b.Support(mq.FeatureScheduledPublish, mq.Support{
			Level: mq.Native, Scope: mq.ScopeTopic,
			Note: "ProducerMessage.DeliverAfter and DeliverAt, held by the broker's delayed-delivery tracker. Pulsar's tracker belongs to the Shared and Key_Shared dispatchers only, so this driver refuses a delayed publication when Config.SubscriptionType is Exclusive or Failover rather than publishing one that would arrive at once",
		})
	}

	// Delayed redelivery: Pulsar's retry topic when it is enabled, and an
	// in-process hold when it is not.
	switch {
	case cfg.RetryEnable:
		b.Support(mq.FeatureDelayedRedelivery, mq.Support{
			Level: mq.Native, Scope: mq.ScopeTopic,
			Note: "Consumer.ReconsumeLater republishes the message to the subscription's retry topic with the delay as a DeliverAfter and acknowledges the original, and the consumer is subscribed to that topic as well. The attempt count survives the hop in Pulsar's own RECONSUMETIMES property, which this driver adds to the broker's redelivery count",
		})
	case sub.Exclusive():
		// With no retry topic the delay has to be held somewhere, and
		// the only place left is this process — which is an emulation
		// this driver is willing to declare. On an Exclusive or Failover
		// subscription even that does not work, because the hold ends in
		// a negative acknowledgement and Pulsar's own
		// NackRedeliveryDelay is the only delay such a subscription
		// honours.
		b.Support(mq.FeatureDelayedRedelivery, mq.Support{
			Level: mq.EmulatedVolatile, Scope: mq.ScopeTopic,
			Note: "the driver holds the unsettled delivery in this process for the delay and then negatively acknowledges it. The hold is lost with the process, after which the message is redelivered as soon as the consumer disconnects rather than at the time asked for. Config.RetryEnable replaces it with Pulsar's own retry topic",
		})
	default:
		b.Support(mq.FeatureDelayedRedelivery, mq.Support{
			Level: mq.EmulatedVolatile, Scope: mq.ScopeTopic,
			Note: "the driver holds the unsettled delivery in this process for the delay and then negatively acknowledges it, which costs nothing at the broker and is lost with the process. Config.RetryEnable replaces it with Pulsar's own retry topic, which is durable",
		})
	}

	// The dead-letter route is the client library's, and it needs a
	// destination.
	if cfg.HasDeadLetter() {
		b.Support(mq.FeatureDeadLetter, mq.Support{
			Level: mq.Native, Scope: mq.ScopeTopic,
			Note: "pulsar-client-go's DLQPolicy: the client publishes the message to Config.DeadLetterTopic and acknowledges the original only once that publication succeeded, so a failed dead-letter publish never acknowledges. It fires on its own once a message has had Config.MaxAttempts deliveries, and an explicit Reject reaches it at once",
		})
	} else {
		// CONDITIONAL and not unsupported, and the difference is what
		// the conditional level is for: the route exists in the
		// client library and this client has not been given a
		// destination, which is an OPTION rather than a missing
		// feature. A caller reading the table sees the condition and a
		// caller that ignores it gets ErrUnsupported from Reject, which
		// is the same answer either way.
		//
		// It also buys two pieces of evidence the unsupported reading
		// threw away. The specification's `dlq/failed-publish-never-acks` is gated
		// on the CLAIM and asks a driver that claims the feature to
		// refuse a Reject on a topic with no destination — which is
		// exactly this configuration — and `capability/DeadLetter/claim`
		// is gated the same way. Advertised as unsupported, both were
		// skipped and the claim stayed unproven for ever.
		b.Conditional(mq.FeatureDeadLetter, mq.ScopeTopic,
			"the route is pulsar-client-go's DLQPolicy and this client has no destination: set Config.DeadLetterTopic, and Config.MaxAttempts for the attempt bound. Without one, Reject is refused with ErrUnsupported rather than destroying the message, and mq.WithDeadLetter — the core's publish-then-acknowledge emulation — needs nothing from this driver")
	}

	// Consumer groups: what a SECOND consumer of one subscription is.
	switch sub {
	case SubscriptionShared, SubscriptionKeyShared:
		b.Native(mq.FeatureConsumerGroups, mq.ScopeGroup)
	case SubscriptionFailover:
		b.Conditional(mq.FeatureConsumerGroups, mq.ScopeGroup,
			"a Failover subscription admits several consumers and delivers to ONE of them; the rest are standbys that receive nothing until it goes away. Exactly one consumer sees each message, which is what the specification asks of a group, but the others are not competing for work")
	case SubscriptionExclusive:
		b.Unsupported(mq.FeatureConsumerGroups)
	default:
		// Config.Validate refuses a kind outside the four, so this is
		// unreachable; the safe answer is the one that claims least.
		b.Unsupported(mq.FeatureConsumerGroups)
	}

	// Replay and the start positions.
	b.Conditional(mq.FeatureReplay, mq.ScopeTopic,
		"a subscription created with StartEarliest reads from the oldest message the NAMESPACE RETENTION POLICY still keeps. Pulsar's default retention is zero, under which an entry every subscription has acknowledged may already have been deleted, so replay is a property of the namespace rather than of the topic or this driver")
	if cfg.RetryEnable {
		// A consumer with the retry topic enabled is subscribed to two
		// topics, and pulsar-client-go refuses Seek and SeekByTime on a
		// multi-topic consumer outright. It is not a limit this driver
		// can work around: the two are one handle.
		b.Unsupported(mq.FeatureSeekByTime)
	} else {
		b.Support(mq.FeatureSeekByTime, mq.Support{
			Level: mq.Native, Scope: mq.ScopeGroup,
			Note: "Consumer.SeekByTime, applied to every partition. It moves the SUBSCRIPTION's cursor, so a StartAt on a subscription that already has a position rewinds it rather than being ignored; Config.RetryEnable makes the consumer multi-topic and pulsar-client-go then refuses a seek altogether",
		})
	}
	// A Pulsar position is a (ledger, entry, batch index) triple and a
	// StartPosition's sequence is one uint64, so there is no honest
	// encoding: a driver that packed them would hand back a number that is
	// not a position on any other broker and would break the moment a
	// ledger identifier grew.
	b.Unsupported(mq.FeatureSeekBySequence)

	// ORDERING — the increment's subject. Each of the four subscription
	// types gets exactly what it can defend and nothing more.
	switch {
	case sub == SubscriptionKeyShared && !cfg.AllowOutOfOrderDelivery:
		b.Conditional(mq.FeatureOrderedByKey, mq.ScopeKey, keySharedOrderNote)
		b.Unsupported(mq.FeatureOrderedByPartition)
		b.Unsupported(mq.FeatureOrderedGlobally)
	case sub == SubscriptionKeyShared:
		// AllowOutOfOrderDelivery was asked for, which is the caller
		// telling Pulsar to hand a reassigned hash range over before the
		// previous owner has finished with it. There is then no per-key
		// order to claim.
		b.Unsupported(mq.FeatureOrderedByKey)
		b.Unsupported(mq.FeatureOrderedByPartition)
		b.Unsupported(mq.FeatureOrderedGlobally)
	case sub.Exclusive():
		b.Conditional(mq.FeatureOrderedByKey, mq.ScopeKey, exclusiveOrderNote)
		b.Native(mq.FeatureOrderedByPartition, mq.ScopePartition)
		b.Conditional(mq.FeatureOrderedGlobally, mq.ScopeTopic, globalOrderNote)
	default:
		// Shared. Pulsar hands a Shared subscription's messages to
		// whichever consumer has permits, so there is no order of any
		// kind — not per key, not per partition, not even with one
		// consumer, because a negative acknowledgement puts a message
		// back behind the ones that followed it.
		b.Unsupported(mq.FeatureOrderedByKey)
		b.Unsupported(mq.FeatureOrderedByPartition)
		b.Unsupported(mq.FeatureOrderedGlobally)
	}

	// The header claims the envelope carries.
	const envelopeNote = "carried in iqmq's portable binary envelope, which Config.Metadata switched on: the emulation for a carrier that cannot hold it. Pulsar's properties are a map of protobuf strings, so a binary value and a repeated key have nowhere native to go. The headers are as durable as the message and the cost is interoperability — a non-iqmq consumer reads the envelope rather than the body"
	for _, f := range [...]mq.Feature{mq.FeatureBinaryHeaders, mq.FeatureDuplicateHeaders} {
		if enveloped {
			b.EmulatedDurable(f, mq.ScopeTopic, envelopeNote)
		} else {
			b.Unsupported(f)
		}
	}

	// The emulated lease.
	if cfg.HasLease() {
		b.Support(mq.FeatureExtendLease, mq.Support{
			Level: mq.EmulatedVolatile, Scope: mq.ScopeTopic,
			Note: "Pulsar's broker redelivers an unsettled message when the consumer disconnects and on no timer, and pulsar-client-go v0.21 has no client-side acknowledgement timeout at all. Config.AckTimeout is therefore the DRIVER's: it tracks each delivery's deadline in this process, hands the message back with a negative acknowledgement one LeaseGuard before it, and refuses a settlement that arrives after that. The deadline is lost with the process, which is what makes it volatile",
		})
	} else {
		b.Unsupported(mq.FeatureExtendLease)
	}

	// Administration.
	if cfg.AdminURL != "" {
		b.Native(mq.FeatureAdmin, mq.ScopeTopic)
	} else {
		b.Unsupported(mq.FeatureAdmin)
	}

	// Transactions are reachable, and not through the portable API.
	if cfg.EnableTransactions {
		b.Conditional(mq.FeatureTransactions, mq.ScopeTopic,
			"the client was opened with transactions enabled and the broker must have them enabled too. There is no portable transaction verb; a transaction is driven through github.com/iqhive/iqmq/x/pulsartx, which takes this client's native handle")
	} else {
		b.Unsupported(mq.FeatureTransactions)
	}

	// Everything else, stated rather than omitted. CapabilityBuilder.Missing
	// is what turns "the driver decided it does not have this" into
	// something a test can tell apart from "nobody looked".
	for _, f := range [...]mq.Feature{
		mq.FeatureAtMostOnce,
		mq.FeatureExactlyOnceDelivery,
		mq.FeatureMessageTTL,
		mq.FeaturePriority,
		mq.FeatureAtomicBatchPublish,
		mq.FeatureIdempotentPublish,
		mq.FeatureDeduplication,
		mq.FeatureServerFilter,
		mq.FeatureRequestReply,
		mq.FeatureNativeTracing,
	} {
		b.Unsupported(f)
	}

	b.Limits(limitsOf(cfg))

	if missing := b.Missing(); len(missing) > 0 {
		// A feature nobody mentioned renders as unsupported, which is the
		// safe answer — but it is also indistinguishable from a feature
		// the driver forgot, and TestCapabilitiesConsidersEveryFeature is
		// what keeps that from happening quietly as the inventory grows.
		return mq.Capabilities{}, errIncompleteTable
	}
	return b.Build()
}

// limitsOf is the limit set.
//
// MaxMessageBytes is the one limit this driver can actually learn from the
// broker: [Backend] reads `maxMessageSize` from the administrative API at open
// when one is configured, and falls back to the broker's documented default. It
// is negotiated per connection in the Pulsar protocol and pulsar-client-go does
// not surface the negotiated figure, so an administrative lookup is the only
// route.
func limitsOf(cfg Config) mq.Limits {
	var lim mq.Limits
	if cfg.MaxMessageBytes > 0 {
		lim.MaxMessageBytes = mq.Limit[int64]{Value: cfg.MaxMessageBytes, Known: true}
	}
	lim.MaxDelay = mq.Limit[time.Duration]{Value: MaxDelay, Known: true}
	if cfg.BatchingMaxMessages > 0 {
		lim.MaxBatchMessages = mq.Limit[int]{Value: cfg.BatchingMaxMessages, Known: true}
	}
	if cfg.BatchingMaxSize > 0 {
		lim.MaxBatchBytes = mq.Limit[int64]{Value: int64(cfg.BatchingMaxSize), Known: true}
	}
	// MaxLease is deliberately NOT advertised. The lease is this driver's
	// own and has no maximum — an extension is a store to a deadline in
	// this process — so Config.AckTimeout is a DEFAULT rather than a bound,
	// and the specification forbids reading an advertised limit as anything but a bound.
	return lim
}

var errIncompleteTable = errcode.New(errcode.InvalidConfig,
	"iqmq/pulsar: the capability table does not mention every feature of the inventory")
