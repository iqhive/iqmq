// Package amqp10 is the shared AMQP 1.0 core of the iqmq driver family: the
// protocol, the links, the credit, the dispositions and the dial loop, with
// every broker-specific decision moved behind one seam.
//
// It is NOT a driver. Opening it needs a [Profile], because AMQP 1.0 settles on
// almost nothing a message queue needs: two conforming brokers agree about
// transfers, dispositions, credit and the message format, and agree about
// nothing else. `drivers/rabbitmq/amqp10` is RabbitMQ 4's profile over this
// core; `drivers/activemq` is Artemis's and ActiveMQ Classic's; a future Azure
// Service Bus driver is another (the specification's "Azure Service Bus (shared amqp10
// internals)" and "generic AMQP 1.0").
//
// # What a Profile is for
//
// [Profile] is the most important type in this module and its documentation is
// the one to read first. What it carries is the list of things the protocol does
// not decide and a core must therefore not guess at:
//
//   - What an ADDRESS is. Apache Artemis names a shared durable subscription by
//     the fully qualified queue name `address::queue`; RabbitMQ 4 names a queue
//     `/queues/<name>` and an exchange `/exchanges/<name>/<key>`; ActiveMQ
//     Classic uses virtual topics, `Consumer.<sub>.VirtualTopic.<topic>`. There
//     is no intersection of the three, so [Profile.SinkAddress] and
//     [Profile.SourceAddress] are the profile's and the rule — one
//     subscription's consumers compete, two subscriptions each receive
//     everything — is delivered by them.
//   - What the broker can DO. The core supplies a base capability table of what
//     this protocol and this implementation really provide, and the profile
//     narrows or widens it; [CoreCeiling] is the enforcement, so a profile
//     cannot claim a feature the core has no code for.
//   - How a delayed publication is expressed (an `x-opt-delivery-delay`
//     annotation on Artemis, `AMQ_SCHEDULED_DELAY` on Classic, refused on
//     RabbitMQ 4), how a delayed NACK is expressed, where the attempt count
//     lives, how a de-duplication key travels, and what a durable confirmation
//     means.
//   - Which brokers work at all. RabbitMQ's AMQP 1.0 endpoint exists from 4.0
//     and its 3.13 plugin is a different protocol, so "refuse below 4.0" is
//     [Profile.CheckBroker]'s and not this core's.
//   - Whether there is a topology API. AMQP 1.0 has none, so [Profile.Admin] is
//     the seam and a profile without one answers
//     [bitbucket.org/iqhive/iqmq.Client.Admin] with ErrUnsupported (admin.go).
//
// # Semantics
//
// AMQP 1.0 is at-least-once here. A transfer is unsettled until the receiver
// says what it did with it, and a delivery is the receiver's until it settles
// it or its link goes away. That single fact decides everything below.
//
//   - **Confirmation boundary.** [bitbucket.org/iqhive/iqmq.ConfirmNone] sends the
//     transfer PRE-SETTLED: the frame leaves, no disposition is coming, and the
//     call costs no round trip — so the boundary is local admission, which the specification
//     says may be lossy. [bitbucket.org/iqhive/iqmq.ConfirmAccepted], the default,
//     waits for the peer's `accepted` disposition, which is the strongest
//     ordinary acknowledgement the protocol has.
//     [bitbucket.org/iqhive/iqmq.ConfirmDurable] is the PROFILE's: the protocol
//     has no durable disposition, so what "durable" means is a broker-specific
//     boundary ([Profile.Durable]) — on Artemis and on RabbitMQ 4 the accepted
//     disposition from a durable queue is one, because both write before they
//     accept — and a publisher that asks for it on a profile that defines none
//     fails when its sink is opened, which is the rule.
//   - **Acknowledgement boundary.** Ack is the `accepted` outcome, and
//     [Config.ReceiverSettle] decides when it is final. Under the default
//     `first` the delivery is settled the moment the disposition frame is
//     written, so Ack costs no round trip and a link that dies immediately
//     afterwards leaves the message redeliverable. Under `second` the peer
//     confirms the settlement, at the cost of a round trip per
//     acknowledgement — and the classification of a FAILED settlement changes
//     with it: `first` means the peer never got it (a lost lease, so the
//     message is coming back), `second` means it may have (an unknown outcome,
//     sticky for ever). errors.go carries the reasoning.
//   - **Nack.** Requeue is `released`, which does NOT count the attempt;
//     requeue with a delay is `modified` carrying the profile's annotation, or
//     ErrUnsupported when the profile has none; drop and dead-letter are both
//     `rejected`, which on a broker with a dead-letter address routes there.
//     AMQP 1.0 has no "destroy" outcome, so Nack(NackDrop()) on a broker with
//     a dead-letter policy puts the message in the dead-letter queue rather
//     than destroying it — the safe direction, stated rather than hidden.
//   - **At-least-once window.** From the first transfer until the disposition
//     the peer acted on. A handler whose process dies, a link that drops before
//     the disposition and a connection that dies mid-handler all produce a
//     redelivery, which is a duplicate the application has to be able to
//     absorb. `DeliveryInfo.Attempt` is the AMQP header's `delivery-count` plus
//     one, so the second delivery of a message says so.
//   - **No lease.** AMQP 1.0 grants none: an unsettled delivery is this
//     receiver's for as long as the link lives, so `DeliveryInfo.LeaseExpires`
//     is the zero time, FeatureExtendLease is unsupported and there is no
//     [bitbucket.org/iqhive/iqmq.DeadlineExtender] anywhere in this module. What
//     bounds the deliveries a consumer holds is the link CREDIT, which is the
//     prefetch (credit.go).
//
// # A stale disposition must not settle the delivery that took its place
//
// This is the hazard worth reading the source for, and the specification makes the
// unguarded behaviour a disqualifier. Detaching an AMQP 1.0 link returns
// everything unsettled on it to the node, where another consumer may pick it up
// at once. A disposition written after that either fails — the new link does not
// know the delivery id — or, on a peer that reuses delivery ids across an
// attach, settles a delivery this process never saw.
//
// This core refuses such a disposition before it reaches the wire. Every
// delivery records the CONNECTION GENERATION of the link it arrived on and takes
// a slot in a [bitbucket.org/iqhive/iqmq/driverkit.TokenTable]; a lost connection
// bumps the generation and invalidates every table before anything else happens,
// and a settlement whose delivery names an older generation reports
// [bitbucket.org/iqhive/iqmq.ErrLeaseLost]. links.go says why both halves are
// needed and settler.go carries the check.
//
// # The Ambiguous publication
//
// The other hazard the specification names is a publication whose outcome is reported as a
// definite failure when nobody knows. A transfer that has left the socket and
// whose disposition never arrived may have been accepted, so the caller that
// retries publishes twice. Every failure on the publish path therefore goes
// through [bitbucket.org/iqhive/iqmq/driverkit.ErrorMapper.MapNet] with a
// sentAlready the call site knows: a peer's own refusal is definite, and a link
// or connection that died is not. sink.go carries the two halves.
//
// # Maturity and tier
//
// Tier 3 as a core (the broker profiles's "activemq (community welcome)"), and the
// tier a caller sees is the profile's: `drivers/rabbitmq/amqp10` is tier 2 and
// `drivers/activemq` is tier 3. This module's own tests need NO broker — every
// one of them runs against the scripted peer in internal/client — and the
// conformance suite is the profile, against the profile's own pinned
// container.
//
// # Opening
//
//	client, err := amqp10.Open(ctx, profile, amqp10.Config{
//		Addrs:    []string{"amqp://127.0.0.1:5672"},
//		Username: "artemis",
//		Password: dsn.Secret("artemis"),
//	})
//
// or from a DSN, once a profile is registered with a
// [bitbucket.org/iqhive/iqmq/dsn.Registry]:
//
//	amqp10://user:password@host:5672?sasl=plain&tls=true&receiversettle=second
//
// [OpenWithClient] takes an *amqp.Conn the caller already has, with explicit
// [bitbucket.org/iqhive/iqmq/driverkit.Ownership] so that iqmq never closes a
// connection it was not asked to close. [BackendWithDialer] takes a dialler,
// which is how a broker-specific module tests its own profile against this core
// with no container.
//
// # Metadata
//
// All four of the modes behave differently here, because there really is
// metadata this protocol cannot carry: application-properties is a MAP, so one
// header key cannot hold two values. Strict refuses such a message before any
// I/O; MetadataPortableEnvelope always carries the whole message in the
// envelope; MetadataAdaptive envelopes only the messages that need it; and
// MetadataLenientDrop leaves the later values out and counts them. A header
// value that is not valid UTF-8 needs none of that — it travels as an AMQP
// binary and comes back byte for byte. mapping.go carries the whole table,
// including the reserved header names that make a receive-then-publish round
// trip lossless.
package amqp10
