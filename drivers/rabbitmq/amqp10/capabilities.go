package amqp10

import (
	mq "github.com/iqhive/iqmq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
)

// claims is this profile capability table, as the difference from the
// shared core's own.
//
// # Why a difference rather than a table
//
// The core builds a base table of what AMQP 1.0 and its implementation really
// do, and a profile narrows or widens it. Returning only the rows this broker
// answers differently is what keeps the two halves from drifting: a row the
// core gets right for every AMQP 1.0 broker — that a disposition assigns no
// identifier, that there is no lease to extend, that application-properties is
// a map and cannot carry a repeated key — is stated once, in the core, and a
// copy here would be a second thing to keep in step.
//
// What is NOT left to the core is any row where RabbitMQ's answer differs from
// "a conforming AMQP 1.0 broker's", and there are two kinds of those. The
// widenings are facts about RabbitMQ the core cannot know: that a queue is
// durable, FIFO and shared, and that a rejected message has somewhere to go.
// The narrowings are the more important half, because each is a feature the
// AMQP 0-9-1 driver has and this profile does not — and the specification's "never silently
// substituted" is about exactly that list.
//
// # The four features this profile does not have and the other one does
//
//   - **Request/reply.** Direct reply-to (`amq.rabbitmq.reply-to`) is a
//     RabbitMQ extension of AMQP 0-9-1 with no AMQP 1.0 form.
//   - **A delayed redelivery, and a scheduled publication.** Both are variant
//     the mechanisms on the other profile — a TTL queue that
//     dead-letters back, or the delayed-message plugin — and neither is
//     reachable here: the first needs topology this profile cannot create, and
//     nothing in this repository has verified how a delay reaches the plugin
//     over AMQP 1.0. A claim that cannot be proved is not made. The other
//     profile withdraws the scheduled publication too, because its delayed
//     exchange is bound to each subscription's queue by name, for a
//     redelivery, so the publication is in this list for its mechanism and
//     not as a difference between the two.
//   - **Ordering by key.** The key can be the routing key on AMQP 0-9-1
//     because the routing key is a field of every `basic.publish`; here it is
//     part of the link's address (see [Config.KeyAsRoutingKey]).
//   - **A wildcard subscription.** A wildcard is a BINDING pattern on a topic
//     exchange. A consuming link addresses a QUEUE, so the pattern is not
//     expressible in the portable Topic at all, and the binding that would
//     carry it is topology this profile cannot create.
//
// # And what it gains
//
// Nothing in the capability table, which is worth saying plainly: what AMQP
// 1.0 gains over AMQP 0-9-1 here is a typed message model, a disposition that
// maps onto the four actions directly, and `delivery-count` as a protocol
// field rather than a header extension. All three are better MECHANISMS for
// features both profiles claim, so they change the notes and not the levels.
//
// Every claim below is a fact about this DEPLOYMENT — the queue type, the
// dead-letter route, the priority ceiling, the durable flag — rather than
// about the broker version, because [checkBroker] has already refused
// everything below RabbitMQ 4.0. Above that gate this table does not move with
// the version, which is why the peer's facts are read for nothing here.
func (c Config) claims(f core.Facts) []core.Claim {
	_ = f
	out := make([]core.Claim, 0, 24)
	add := func(claims ...core.Claim) { out = append(out, claims...) }

	// --- Storage and delivery semantics.
	if c.NonDurable {
		add(core.Conditional(mq.FeatureDurableStorage, mq.ScopeTopic,
			"this client publishes with the AMQP header's `durable` flag clear (Config.NonDurable), so the broker is free to lose the message when the node restarts however the queue was declared"))
	} else {
		add(claimed(mq.FeatureDurableStorage, mq.ScopeTopic,
			"the message is published with the AMQP header's `durable` flag set, and a durable queue writes it to disk; what the queue is was decided when it was declared, which on this profile is not this driver's doing"))
	}
	add(
		withdrawn(mq.FeatureAtMostOnce,
			"this driver attaches every receiving link unsettled and settles each delivery explicitly, so a delivery is redelivered rather than lost; a pre-settled receiving link would be at-most-once and there is no configuration for one"),
		claimed(mq.FeatureAtLeastOnce, mq.ScopeGroup,
			"a transfer is unsettled until this consumer's disposition, and detaching the link returns everything unsettled on it to the queue, where RabbitMQ hands it to the next consumer; DeliveryInfo.Attempt is the AMQP header's `delivery-count` plus one, which is a protocol field here rather than RabbitMQ's x-delivery-count header extension"+attemptCaveat(c)),
		claimed(mq.FeatureExplicitAck, mq.ScopeGroup,
			"Ack is the `accepted` disposition for the delivery, which is final when the frame is written under the default receiver settle mode `first` and confirmed by the broker under `second`"),
		claimed(mq.FeatureNack, mq.ScopeGroup,
			"a requeue is `released`, which does NOT count the attempt; a drop and a dead-letter are both `rejected`, which on a queue with a dead-letter exchange routes there rather than destroying the message -- AMQP 1.0 has no destroy outcome, and that is the safe direction stated rather than hidden"),
	)

	// --- Timing. Both rows are withdrawals and both are the specification's
	// subject: see this function's own documentation.
	add(
		withdrawn(mq.FeatureDelayedRedelivery,
			"a `modified` disposition returns the delivery to the queue at once, so there is nothing to hold it back. The other profile builds a delay from a TTL queue that dead-letters back or from the rabbitmq_delayed_message_exchange plugin; the first needs topology this profile cannot create, and how a delay reaches the plugin over AMQP 1.0 is not verified here, so NackOptions.Delay is refused with ErrUnsupported and settles nothing"),
		withdrawn(mq.FeatureScheduledPublish,
			"RabbitMQ has no AMQP 1.0 spelling of a delivery delay. The rabbitmq_delayed_message_exchange plugin reads an `x-delay` HEADER on AMQP 0-9-1, and which AMQP 1.0 section and key that becomes -- a message annotation, an application property, neither -- is not verified here, so PublishOptions.Delay and DeliverAt are refused before any I/O rather than written where they may be ignored"),
		claimed(mq.FeatureMessageTTL, mq.ScopeTopic,
			"PublishOptions.TTL travels as the AMQP message header's `ttl` in milliseconds, which RabbitMQ reads as the message's own expiry; whether the expiry survives a dead-letter hop into `<queue>.dlq`, and whether the AMQP 1.0 view carries the `x-death` record of one, is not verified here"),
	)
	if c.MaxPriority == 0 {
		// The broker ACCEPTS a priority on a message to a queue that was not
		// declared with `x-max-priority` and ignores it, which is the worst
		// kind of unsupported: the publication succeeds and the ordering is
		// not what the caller asked for. Advertising it Unsupported is what
		// makes the core refuse the publication instead.
		add(withdrawn(mq.FeaturePriority,
			"the subscription's queue was not declared with x-max-priority (Config.MaxPriority is zero), and RabbitMQ accepts the AMQP header's `priority` on such a queue and ignores it"))
	} else {
		add(core.Conditional(mq.FeaturePriority, mq.ScopeGroup,
			"the subscription's queue was declared with x-max-priority, so a priority above the ceiling is clamped by the broker; RabbitMQ refuses x-max-priority on a quorum queue, so a prioritised subscription is a classic queue and the delivery attempt count goes with it"))
	}

	// --- The dead-letter route, which is the QUEUE's policy and not a
	// publication this driver makes.
	if c.DeadLetter {
		add(claimed(mq.FeatureDeadLetter, mq.ScopeGroup,
			"a `rejected` disposition routes the message through the queue's own dead-letter exchange to `<queue>.dlx` and `<queue>.dlq`, which whoever declared the queue had to set: this profile declares nothing and publishes no copy of its own"))
	} else {
		add(withdrawn(mq.FeatureDeadLetter,
			"Config.DeadLetter is not set, so this client does not claim the subscriptions' queues were declared with a dead-letter exchange; a Nack asking for a dead-letter route is refused with ErrUnsupported and settles nothing rather than rejecting the message into nowhere"))
	}

	// --- The shapes a queue and an exchange give a subscription. All three
	// are facts about RabbitMQ that the core cannot know, and all three need
	// the topology to exist, which on this profile is somebody else's job
	// (Config.Declare).
	add(
		claimed(mq.FeatureConsumerGroups, mq.ScopeGroup,
			"several receiving links on one queue compete: the broker hands each message to one of them, bounded by each link's own credit"),
		claimed(mq.FeatureDurableSubscriptions, mq.ScopeGroup,
			"a subscription is a durable queue, which keeps accumulating while no link is attached; detaching a link is not unsubscribing"),
		claimed(mq.FeatureFanOut, mq.ScopeTopic,
			"one queue per subscription, each bound to the topic's exchange, so two subscriptions each receive every message; the bindings are whoever declared the topology's, because this profile creates none"),
		claimed(mq.FeatureOrderedByPartition, mq.ScopeGroup,
			"a queue is FIFO, so one queue's messages are delivered in the order the exchange routed them while a single consumer settles them in order; a requeue puts a message back at the head and a second consumer breaks it, which is the specification's own caveat and not this driver's"),
		withdrawn(mq.FeatureOrderedByKey,
			"a sending link's address carries the routing key (`/exchanges/<exchange>/<routing-key>`), so the key cannot be the routing key per message the way WithKeyAsRoutingKey makes it on AMQP 0-9-1; with the key in an application property the broker does not read it"),
		withdrawn(mq.FeatureWildcardSubscribe,
			"a wildcard is a binding pattern on a topic exchange. A receiving link addresses a queue (`/queues/<queue>`), so a pattern is not expressible in the portable Topic, and the binding that would carry it is topology this profile does not create"),
		withdrawn(mq.FeatureServerFilter,
			"a receiving link's source address names a queue, so the only filtering the portable API can express is which queue is read. RabbitMQ 4.1 added AMQP filter expressions for streams; this driver does not set one and nothing here has verified them, so the claim is not made"),
		withdrawn(mq.FeatureAdmin,
			"AMQP 1.0 has no topology API. RabbitMQ 4 has an AMQP 1.0 management extension over a `/management` node and this profile does not implement it, so Client.Admin answers ErrUnsupported; declare topology with drivers/rabbitmq, the HTTP management API or rabbitmqctl"),
		withdrawn(mq.FeatureRequestReply,
			"direct reply-to (`amq.rabbitmq.reply-to`) is a RabbitMQ extension of AMQP 0-9-1 and has no AMQP 1.0 form. A reply address still travels in the `reply-to` property, which is the portable emulation the core provides rather than a native mechanism"),
	)

	// --- Headers. The level is the same on both profiles and the reason is
	// not, which is why the note is here rather than left to the core.
	add(
		claimed(mq.FeatureNativeHeaders, mq.ScopeTopic,
			"a portable header is an AMQP application property, which is the section RabbitMQ converts to and from the AMQP 0-9-1 field table -- so a header published through one RabbitMQ profile reads back through the other. A key longer than 255 bytes is legal here and cannot become a field-table key, which is an interoperability ceiling rather than a limit this driver enforces"),
		claimed(mq.FeatureBinaryHeaders, mq.ScopeTopic,
			"an application-property value is typed, so a header value that is not valid UTF-8 travels as an AMQP binary and comes back byte for byte rather than as a byte array inside a field table"),
		claimed(mq.FeatureBatchReceive, mq.ScopeGroup,
			"the link's credit lets the broker send several transfers before any is settled, which is what a batched fetch reads; credit is the AMQP 1.0 answer to `basic.qos` and is set by Config.Credit"),
	)

	// --- The confirmation boundary, which is this profile's sharpest
	// mechanical difference from the other one.
	add(claimed(mq.FeaturePublisherConfirm, mq.ScopeTopic,
		"the peer's `accepted` disposition for the transfer, which ConfirmAccepted waits for. There is no confirm MODE to enable and no cumulative form: every transfer on an unsettled link gets its own disposition, so the per-message, windowed and asynchronous strategies have no equivalent and what a caller sizes is the credit window"))
	if c.QueueType == QueueQuorum {
		add(core.Conditional(mq.FeatureDurableConfirm, mq.ScopeTopic, durableNote))
	}
	return out
}

// durableNote is the exact durable confirmation boundary the specification requires every
// driver to state, and it is the same sentence in the capability table and in
// [Config.Profile]'s durable rule so that the two cannot disagree.
const durableNote = "the peer's `accepted` disposition for a transfer on an unsettled link, which RabbitMQ returns for a durable quorum queue once the message has been written to a majority of the queue's replicas. There is no confirm MODE to select and no cumulative form: every transfer gets its own disposition, so the three strategies have no equivalent here"

// durable is the profile's answer to [github.com/iqhive/iqmq.ConfirmDurable].
//
// It is supported on a quorum queue and REFUSED on a classic one, and the
// refusal is the point. The specification requires a driver either to state the exact
// durable boundary or to fail at construction, and on a classic queue the
// `accepted` disposition means only that the broker holds the message in
// memory — a boundary that reads like durability and is not one. So a
// publisher that asks for ConfirmDurable against a classic-queue configuration
// fails when its sink is opened, which is what the specification asks for, rather than
// succeeding against a weaker promise.
func (c Config) durable() core.DurableRule {
	if c.QueueType != QueueQuorum {
		return core.DurableRule{}
	}
	return core.DurableRule{Supported: true, Note: durableNote}
}

// attemptCaveat is what the attempt count depends on, appended to the
// at-least-once note.
//
// RabbitMQ's quorum queues count redeliveries and reach an AMQP 1.0 consumer
// through the header's `delivery-count`. Whether a CLASSIC queue populates the
// same field is not something this repository can check without a broker, so a
// classic-queue configuration says so rather than claiming a count that may
// always be zero — which the specification makes a different answer from "this is the first
// delivery".
func attemptCaveat(c Config) string {
	if c.QueueType == QueueQuorum {
		return ""
	}
	return ". This client is configured for CLASSIC queues, and whether a classic queue's redeliveries reach the AMQP 1.0 header's `delivery-count` is not verified here, so Attempt may be 1 on every delivery and the message contract's MaxAttempts cannot be enforced from the broker's own count"
}

// claimed is a feature this broker has, with the note that says HOW.
//
// The core's [github.com/iqhive/iqmq/drivers/amqp10.Native] carries no note,
// and most of this profile's native rows need one: the LEVEL is the same as the
// AMQP 0-9-1 driver's and the MECHANISM is not, so a note is the only place a
// caller reads that an ack is a disposition, that a confirm is not a mode, and
// that a header is an application property because that is the section RabbitMQ
// converts between its two protocols through.
func claimed(f mq.Feature, scope mq.Scope, note string) core.Claim {
	return core.Claim{
		Feature: f,
		Support: mq.Support{Level: mq.Native, Scope: scope, Note: note},
	}
}

// withdrawn is a claim that this broker does not have a feature, WITH the
// reason.
//
// The core's [github.com/iqhive/iqmq/drivers/amqp10.Withdraw] carries no note,
// and every withdrawal in this profile needs one: eight of them are features
// the AMQP 0-9-1 driver really has, so "unsupported" with no reason would read
// as an omission by whoever wrote this table rather than as the difference
// The specification says the two profiles have.
func withdrawn(f mq.Feature, note string) core.Claim {
	return core.Claim{
		Feature: f,
		Support: mq.Support{Level: mq.Unsupported, Scope: mq.ScopeNone, Note: note},
	}
}
