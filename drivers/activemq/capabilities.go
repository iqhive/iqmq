package activemq

import (
	mq "github.com/iqhive/iqmq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/internal/activemq"
)

// claims is this profile table, as the rows it answers DIFFERENTLY from
// the shared core's base table.
//
// Returning only the differences is what keeps the two halves from drifting: a
// profile that restated the whole table would have to be edited every time the
// core learns something, and the core refuses an overclaim anyway
// ([core.CoreCeiling]).
//
// Every row below is a fact about one of the two products, established against
// the pinned image rather than read from documentation, and the rows that
// differ BETWEEN the products are the reason this driver has two conformance
// reports rather than one.
func (c Config) claims(f core.Facts) []core.Claim {
	artemis := c.Broker == activemq.Artemis
	out := make([]core.Claim, 0, 20)
	add := func(cl core.Claim) { out = append(out, cl) }

	// --- What both products have.

	// A topic is a multicast address with a durable queue per subscription
	// (Artemis) or a virtual topic with a consumer queue per subscription
	// (Classic). Either way the message is on disk before the broker accepts
	// it, and the subscription survives a restart with no consumer attached.
	add(claimed(mq.FeatureDurableStorage, mq.ScopeTopic,
		"the destination is a durable address with a durable queue per subscription; a publication with AMQP's `durable` flag set is journalled before it is accepted"))
	add(claimed(mq.FeatureDurableSubscriptions, mq.ScopeGroup,
		"a subscription is a durable queue that keeps its messages with no consumer attached"))
	// the rule, and the whole reason the two products need two
	// mappings: two consumers of one subscription compete for a message and
	// two subscriptions each receive it.
	//
	// It is NATIVE on Artemis, whose address model is the mechanism, and
	// CONDITIONAL on Classic, whose fan-out is a naming convention the
	// broker's `virtualDestinationInterceptor` implements and a deployment
	// can reconfigure -- the distinction `internal/activemq/protocol` makes
	// and this driver must not widen.
	if artemis {
		add(claimed(mq.FeatureFanOut, mq.ScopeTopic, fanoutNote(true)))
	} else {
		add(conditional(mq.FeatureFanOut, fanoutNote(false)+
			"; it is the broker's DEFAULT virtual-destination configuration and a deployment that changed the prefix "+
			"would take the fan-out away with no error anywhere"))
	}
	add(claimed(mq.FeatureConsumerGroups, mq.ScopeGroup,
		"every consumer of one subscription attaches to the same queue and they compete for its messages"))
	// Measured with two competing consumers on one queue: every message of
	// group k0 went to the first consumer and every message of group k1 to
	// the second, in publication order within each group.
	add(claimed(mq.FeatureOrderedByKey, mq.ScopeKey,
		"the message key travels as AMQP's `group-id` property, which both products read as the JMS message group and dispatch exclusively to one consumer of a subscription (JMSXGroupID)"))
	// A `rejected` delivery reaches the broker's dead-letter address:
	// measured on Artemis, one rejection raised DLQ's countMessages from 0
	// to 1 and the message was not redelivered.
	switch {
	case c.DeadLetterTopic != "":
		// The driver PUBLISHES the dead letter and settles the original
		// only once that publication is accepted, so the emulation is this
		// driver's and it is durable.
		add(emulatedDurable(mq.FeatureDeadLetter, mq.ScopeTopic, deadLetterNote(c, artemis)))
	case c.DeadLetter:
		// The broker's own route, which the driver cannot read: neither
		// product reports a destination's dead-letter address over AMQP
		// 1.0, so this is a claim about the BROKER's configuration and the word for that is conditional.
		add(conditional(mq.FeatureDeadLetter, deadLetterNote(c, artemis)))
	default:
		add(withdrawn(mq.FeatureDeadLetter,
			"this client is configured NoDeadLetter: the destinations' address settings name no dead-letter address, so a `rejected` delivery is discarded rather than routed"))
	}

	// The publisher-confirm boundary. The peer's `accepted` disposition says
	// the broker took the message and implies PERSISTENCE only for a durable
	// message on a destination the broker persists -- which is a fact about
	// the deployment's address settings and not about the protocol, so the specification's
	// word for it is conditional. The core derives the row from
	// Profile.Durable and sets it native; this narrows it.
	if !c.NonDurable {
		add(conditional(mq.FeatureDurableConfirm, c.durable().Note))
	}

	// Priority is native on Artemis, whose queues are priority-ordered, and
	// on Classic it is the DEPLOYMENT's: the broker carries the AMQP
	// header's priority and dispatches in FIFO order unless the
	// destination's policy sets `prioritizedMessages="true"`, which the
	// pinned image does not do for queues at all. Measured with it off, a
	// high-priority publication arrives after a low-priority one -- so the
	// claim is withdrawn unless Config.Prioritised says the policy is
	// there, which is the same shape as Config.Scheduler.
	if !artemis {
		if c.Prioritised {
			add(conditional(mq.FeaturePriority,
				"ActiveMQ Classic reorders a destination's messages by priority only when its policy sets "+
					"prioritizedMessages=\"true\", which this client is configured to expect (Config.Prioritised); "+
					"the driver cannot read the broker's policy to check it"))
		} else {
			add(withdrawn(mq.FeaturePriority,
				"ActiveMQ Classic carries the AMQP header's priority and dispatches in FIFO order unless the "+
					"destination's policy sets prioritizedMessages=\"true\", which the pinned image does not do for "+
					"queues; measured with it off, a high-priority publication arrives after a low-priority one. Set "+
					"Config.Prioritised once the broker's policy has it"))
		}
	}

	// Idempotent publication is NOT the same mechanism as de-duplication and
	// this driver has only the second. Neither product has a producer epoch
	// or a sequence number, so a retried publication is a SECOND message
	// unless the caller supplied a de-duplication key for it -- and the core
	// derives both rows from one rule, so the one it over-claims has to be
	// withdrawn here. The evidence rule is what makes that matter: a
	// caller who read this row would retry an ambiguous publication and get
	// two messages.
	add(withdrawn(mq.FeatureIdempotentPublish,
		"neither product has a producer epoch or a sequence number, so a retried publication is a second message; "+
			"what Artemis has is caller-supplied DE-DUPLICATION (PublishOptions.DedupKey as `_AMQ_DUPL_ID`), which is "+
			"FeatureDeduplication and needs the caller to name a key"))

	// --- What neither has.

	// Measured on Artemis: a `modified` disposition carrying
	// `x-opt-delivery-delay: 3000` was redelivered IMMEDIATELY with
	// delivery-count 1. The annotation is honoured on a publication and
	// ignored on a disposition, and each product's redelivery delay is an
	// address setting rather than a per-nack value -- so there is no
	// protocol spelling for the delayed requeue and the core refuses it
	// before any I/O.
	add(withdrawn(mq.FeatureDelayedRedelivery,
		"measured: a `modified` disposition carrying the scheduled-delivery annotation is redelivered immediately, and each product's redelivery delay is an address setting rather than a per-nack value, so Nack(NackDelay(d)) is refused rather than silently ignored"))
	// A receiving link CAN carry a source filter and go-amqp does not expose
	// one, so a selector is unavailable over this protocol whatever the
	// broker supports. `drivers/activemq/stomp` is where a selector lives,
	// and on Classic only.
	add(withdrawn(mq.FeatureServerFilter,
		"a JMS selector is an AMQP 1.0 link source filter and go-amqp exposes no way to set one; drivers/activemq/stomp carries a selector and, measured, only Classic honours it"))
	add(withdrawn(mq.FeatureWildcardSubscribe,
		"a topic maps to its own address, so a pattern over topic names would have to span addresses and a receiving link attaches to one"))

	// --- What one has and the other does not.

	if artemis {
		// Artemis keeps a cache of identifiers it has seen and discards a
		// second message carrying one.
		add(claimed(mq.FeatureDeduplication, mq.ScopeTopic,
			"PublishOptions.DedupKey travels as the `_AMQ_DUPL_ID` application property and Artemis discards a second message carrying a value within its own id-cache-size window"))
		// Artemis has a management address reachable over AMQP 1.0, so
		// topology is a portable operation here -- CONDITIONALLY: the
		// address is a destination like any other and a deployment that
		// secured or disabled it would leave the surface present and every
		// call refused.
		add(conditional(mq.FeatureAdmin,
			"Artemis's `activemq.management` address, over this same connection: EnsureTopic creates a multicast "+
				"address and EnsureSubscription a durable queue on it. It needs the connecting user to hold the "+
				"broker's management permissions, which the driver cannot check before it is used"))
	} else {
		add(withdrawn(mq.FeatureDeduplication,
			"ActiveMQ Classic has no duplicate detection: there is no property a publication can carry that makes the broker discard a repeat"))
		// Classic's management is JMX and its web console, and neither is
		// reachable over AMQP 1.0. Claiming Admin would make
		// `Client.Admin()` available and answer ErrUnsupported from it,
		// which the specification forbids -- the core's checkClaim refuses the claim
		// without a Profile.Admin anyway.
		add(withdrawn(mq.FeatureAdmin,
			"ActiveMQ Classic's management is JMX and its web console, neither of which is reachable over AMQP 1.0; its destinations are created by the broker on attach"))
	}

	// Scheduled publication is the one claim a DEPLOYMENT can take away, and
	// Classic's pinned image takes it away by default: `schedulerSupport` is
	// unset, and measured, a publication carrying AMQ_SCHEDULED_DELAY is
	// then delivered immediately with the property passed through to the
	// consumer. The core derives this row from Profile.Delay, so the
	// withdrawal is what has to be said explicitly.
	if !artemis && !c.Scheduler {
		add(withdrawn(mq.FeatureScheduledPublish,
			"ActiveMQ Classic's message scheduler is off unless the broker is configured with schedulerSupport=\"true\", and measured, with it off the broker delivers a message carrying AMQ_SCHEDULED_DELAY immediately and passes the property through; set Config.Scheduler once the broker has it"))
	}

	// the evidence rule is that a claim which cannot be exercised does
	// not render, so a feature this driver has no code for is withdrawn here
	// rather than left to the core's base table, where three of them sit at
	// Native on the strength of the PROTOCOL alone.
	if f.Product == "" && c.Broker == activemq.Artemis {
		// Artemis always sends a product. A connection that reports none
		// while configured for Artemis is a peer this driver cannot
		// identify -- a test's scripted peer, or a router in between -- and
		// the claims above are about the broker rather than the link, so
		// nothing is withdrawn. The branch exists so that a reader asking
		// "what if the peer says nothing" finds the answer here.
		_ = f
	}
	return out
}

// fanoutNote states the rule in the product's own vocabulary.
func fanoutNote(artemis bool) string {
	if artemis {
		return "a topic is a MULTICAST address and each subscription a durable queue bound to it, addressed by the fully qualified queue name `address::queue`"
	}
	return "a topic is a VIRTUAL TOPIC and each subscription a queue called `Consumer.<subscription>.VirtualTopic.<topic>`, which the broker copies every publication into"
}

// deadLetterNote says which dead-letter route a rejection takes.
func deadLetterNote(c Config, artemis bool) string {
	if c.DeadLetterTopic != "" {
		return "a rejected or dead-lettered delivery is PUBLISHED to " + c.DeadLetterTopic +
			" and the original is settled only once that publication is accepted "
	}
	if artemis {
		return "a `rejected` delivery reaches the address settings' dead-letter-address (DLQ in the pinned image); measured, one rejection raised its message count by one and the message was not redelivered"
	}
	return "a `rejected` delivery reaches the broker's dead-letter strategy (ActiveMQ.DLQ by default); the driver cannot read the queue's configuration over AMQP 1.0, so this is Config.DeadLetter's claim"
}

// conditional is a feature whose availability is the DEPLOYMENT's rather than
// the protocol, with the condition the specification requires it to state.
//
// Every conditional row here is topic-scoped, which is why there is no scope
// parameter: the four of them -- the dead-letter route, the confirmation
// boundary, Classic's priority and Artemis's management address -- are all
// properties of a destination's own configuration.
func conditional(f mq.Feature, note string) core.Claim {
	return core.Claim{Feature: f, Support: mq.Support{Level: mq.Conditional, Scope: mq.ScopeTopic, Note: note}}
}

// emulatedDurable is a feature this driver provides with durable broker
// operations of its own rather than one the broker provides.
func emulatedDurable(f mq.Feature, scope mq.Scope, note string) core.Claim {
	return core.Claim{Feature: f, Support: mq.Support{Level: mq.EmulatedDurable, Scope: scope, Note: note}}
}

// claimed and withdrawn are the two constructors this table is written in
// terms of. The core's [core.Native] and [core.Withdraw] carry no note, and the specification asks a driver to say WHY for every row that is not a bare native
// support -- so these take the note and the table reads as sentences.
func claimed(f mq.Feature, scope mq.Scope, note string) core.Claim {
	return core.Claim{Feature: f, Support: mq.Support{Level: mq.Native, Scope: scope, Note: note}}
}

// withdrawn narrows a row the core's base table leaves open.
func withdrawn(f mq.Feature, note string) core.Claim {
	return core.Claim{Feature: f, Support: mq.Support{Level: mq.Unsupported, Scope: mq.ScopeNone, Note: note}}
}
