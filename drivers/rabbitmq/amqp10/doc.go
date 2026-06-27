// Package amqp10 is the AMQP 1.0 profile for RabbitMQ 4.0 and newer
// (the driver documentation, the second arm).
//
// It is not a driver of its own and it is not a second RabbitMQ driver. It is
// one value — an
// [bitbucket.org/iqhive/iqmq/drivers/amqp10.Profile] — handed to the shared AMQP
// 1.0 core, which owns the links, the credit, the dispositions, the message
// mapping and the dial loop. What this module owns is everything AMQP 1.0
// leaves open: what an address means on RabbitMQ, which versions work at all,
// what the capability table may claim, and the one error rule this broker
// needs. [Config.Profile] and [Config.CoreConfig] are that answer, and they
// are this module's real API.
//
// # The version gate is the headline
//
// [Open] refuses a broker older than RabbitMQ 4.0 with
// [bitbucket.org/iqhive/iqmq.ErrUnsupported], naming both the version the broker
// reported and the version this profile needs, and it does NOT fall back to
// AMQP 0-9-1. The specification says this profile is "never silently substituted" and that
// sentence is the whole reason the gate exists: RabbitMQ 4.0 is the first
// release that speaks AMQP 1.0 natively, on 3.13 it is a plugin whose feature
// set, address syntax and settlement behaviour differ, and a driver that
// accepted either would be claiming this profile's behaviour for something
// else. A peer that did not say what version it is satisfies the gate no more
// than an old one does, which is what keeps a version gate from being open by
// omission.
//
// A peer that names a PRODUCT which is not RabbitMQ is refused too, before the
// version is looked at, and for the same reason read backwards. Apache Artemis
// reports version 2.44, which passes no RabbitMQ gate; but it also
// auto-creates an address from an attach, so a client pointed at the wrong
// broker would not fail — it would create an address literally called
// `/exchanges/orders/orders` and publish into it for ever.
//
// # Topology
//
// The same topology the AMQP 0-9-1 driver uses, derived by the same code. A
// topic is a durable exchange, `topic`-typed by convention, and a subscription
// is a durable quorum queue named `<topic>.<subscription>` bound to it with the
// topic as the routing key; the dead-letter route is `<queue>.dlx` and
// `<queue>.dlq`. Every name comes from `internal/rabbit`, which both RabbitMQ
// modules share so that a caller who moves between the profiles finds the same
// exchange, the same queue and the same dead-letter route rather than two
// topologies wearing one name.
//
// What differs is how a name becomes an ADDRESS. AMQP 1.0 gives a link one
// address string and no second field for a routing key, so RabbitMQ 4's own
// path form carries both: a publication attaches its target to
// `/exchanges/<exchange>/<routing-key>` and a consumer attaches its source to
// `/queues/<queue>`, each segment percent-encoded so that a routing key
// containing a "/" addresses the node it names rather than a different one.
// `internal/rabbit/protocol` owns that form for both arms.
//
// **Nothing is declared, and nothing can be.** RabbitMQ 4's AMQP 1.0 endpoint
// creates no exchange, queue or binding: an attach to a queue that does not
// exist closes the link with `amqp:not-found`, which this driver reports as
// CodeNotFound. So the specification's "no hidden creation" is a property of the protocol
// here rather than a choice, [Config.Declare] is refused outright, and
// FeatureAdmin is not claimed. The topology is `drivers/rabbitmq`'s to declare,
// or an operator's.
//
// # What differs from drivers/rabbitmq
//
// The specification says the two are "never silently substituted", and this is the list
// that sentence is about. They are not one broker seen through two windows.
//
//   - **There is no `mandatory` flag and no `basic.return`.** the specification's
//     "Mandatory + NotifyReturn = Rejected" therefore has no equivalent: an
//     unroutable message on RabbitMQ 4's AMQP 1.0 is RELEASED by the broker, so
//     it reaches this driver as a disposition rather than as a returned
//     message. The outcome a caller sees is the same — CodeNotFound, not
//     retryable, because retrying routes it nowhere again — and that agreement
//     is a rule this profile adds to the core's error table, not something the
//     protocol gives for free (profile.go). A target address whose EXCHANGE
//     does not exist is different again: the broker closes the link with
//     `amqp:not-found`.
//   - **There is no confirm MODE, and therefore no at all.**
//     Every transfer on an unsettled link gets its own disposition, so there is
//     nothing to select: the per-message, windowed and asynchronous
//     strategies collapse into the link's credit window, and what a caller
//     chooses here is a CREDIT WINDOW ([Config.Credit]) rather than a confirm
//     strategy. It is simpler and it is not free — there is no cumulative
//     `basic.ack{multiple}` to amortise a batch across, which is why this
//     module's conformance fidelity figure is lowered.
//   - **There is no direct reply-to.** `amq.rabbitmq.reply-to` is a RabbitMQ
//     extension of AMQP 0-9-1 with no AMQP 1.0 form, so native request/reply is
//     gone and FeatureRequestReply is not claimed. A reply address still
//     travels in the `reply-to` property, which is the portable emulation the
//     core provides rather than a native mechanism.
//   - **`delivery-count` is a protocol field of the header section** rather
//     than RabbitMQ's `x-delivery-count` header extension. Attempt is
//     read from it by the core — the header counts REDELIVERIES, so Attempt is
//     `delivery-count` plus one and is 1 on a first delivery — which means the
//     count needs no queue argument and no header the publisher must not
//     overwrite.
//   - **Settlement is a DISPOSITION**: accepted, rejected, released, or
//     modified carrying `delivery-failed` and `undeliverable-here` as separate
//     flags. That maps onto the four actions more directly than
//     `basic.ack`/`basic.nack`/`basic.reject` do — a requeue that does not
//     count the attempt is `released` rather than `basic.nack{requeue=true}`,
//     and the two flags say what a nack means instead of leaving it to a
//     broker's reading of a requeue.
//
// And four capability claims the other driver makes are absent here: a delayed
// redelivery, a scheduled publication, ordering by key and a wildcard
// subscription. capabilities.go says why each one, and none of them is a
// limitation of this module's code that a later version might lift: two are
// unverified mappings that forbids claiming, and two are consequences of
// an AMQP 1.0 link carrying one address.
//
// # The publication path and the confirmation boundary
//
// A publication is one transfer on a sending link attached to the topic's
// exchange path. The boundary each of the modes means is:
//
//	ConfirmNone      the transfer is sent PRE-SETTLED: the frame leaves, no
//	                 disposition is coming, and the call costs no round trip.
//	                 The boundary is local admission, which the specification says may be
//	                 lossy.
//	ConfirmAccepted  the peer's `accepted` disposition for this transfer. It
//	                 is the AMQP 1.0 equivalent of `basic.ack` on a confirm
//	                 channel, per transfer and with no mode to enable.
//	ConfirmDurable   the same `accepted` disposition, on a durable QUORUM
//	                 queue, where RabbitMQ returns it once the message has
//	                 been written to a majority of the queue's replicas. A
//	                 classic-queue configuration ([Config.QueueType]) defines
//	                 no durable boundary, so a publisher that asks for
//	                 ConfirmDurable against one fails when its sink is opened
//
// A publication whose transfer has left and whose disposition never arrived is
// AMBIGUOUS and is reported as such: the peer may hold the message, so the specification
// forbids reporting it as a definite failure. A peer's own `rejected` or
// `released` disposition is definite, which is the half a driver that marked
// everything ambiguous would destroy.
//
// # The consumption path and settlement
//
// A source is a receiving link attached to `/queues/<topic>.<subscription>`
// with a credit window. Credit is the AMQP 1.0 answer to `basic.qos`: it is
// what bounds the deliveries a consumer holds unsettled, and it is the only
// window this protocol has because it grants no lease at all —
// `DeliveryInfo.LeaseExpires` is the zero time, FeatureExtendLease is
// unsupported, and an unsettled delivery is this receiver's for as long as the
// link lives.
//
// Settlement is one disposition each: Ack is `accepted`; an immediate Nack is
// `released`, which does not count the attempt; a Nack asking for a
// dead-letter route or a drop is `rejected`, which on a queue with a
// dead-letter exchange routes to `<queue>.dlx` rather than destroying the
// message — the safe direction, stated rather than hidden. A Nack with a delay
// is refused with ErrUnsupported and settles nothing: RabbitMQ has no AMQP 1.0
// spelling for "hold this back", and the specification makes settling nothing the right
// answer for an action the driver cannot honour.
//
// # The at-least-once window
//
// From the transfer that delivered the message until the disposition the
// broker acted on. A process that stops inside it — a crash, a link that
// detached, a connection that died — leaves the delivery unsettled, and
// detaching an AMQP 1.0 link returns everything unsettled on it to the queue,
// where RabbitMQ hands it to whichever consumer of that queue is next. So a
// handler may run twice for one message, and `DeliveryInfo.Attempt` is the
// header's `delivery-count` plus one, so the second delivery says so.
//
// A settlement whose link belongs to an older connection is refused with
// ErrLeaseLost and NEVER written to the wire. That is the disqualifier in
// AMQP 1.0's clothing — a peer that reuses delivery ids across an attach would
// otherwise apply the disposition to whatever took the delivery's place — and
// the guard is the shared core's: every delivery records the connection
// generation of the link it arrived on, a lost connection bumps it before
// anything else happens, and a disposition naming an older one is refused
// before it reaches the peer. This profile adds nothing that could defeat it,
// which critical_test.go is what holds.
//
// # Maturity and tier
//
// Tier 2, as the conditional profile the specification describes, and behind the
// AMQP 0-9-1 driver in evidence rather than in surface: nothing in this
// increment has run against a RabbitMQ broker, so the claims marked
// **(verify)** and the four this profile records as unverified are
// exactly that. docs/drivers/rabbitmq-amqp10.md lists them under "Known
// gotchas", and none of them is claimed as a capability.
//
// # Opening
//
//	client, err := rmq10.Open(ctx, rmq10.Config{
//		Addrs:    []string{"amqp://127.0.0.1:5672"},
//		Username: "iqmq",
//		Password: dsn.Secret("iqmq"),
//		Vhost:    "acme",
//	})
//
// or from a DSN, once the profile is registered with a
// [bitbucket.org/iqhive/iqmq/dsn.Registry]:
//
//	rabbitmq+amqp10://user:password@host:5672?vhost=acme&queuetype=quorum&deadletter=true
//
// The scheme is neither "rabbitmq", which the AMQP 0-9-1 driver claims, nor
// "amqp10", which the core's generic profile claims; [Scheme] says why.
// [OpenWithConn] takes an *amqp.Conn the caller already has, with explicit
// [bitbucket.org/iqhive/iqmq/driverkit.Ownership] so that iqmq never closes a
// connection it was not asked to close — and it is the seam that makes this
// module's own tests need no broker.
package amqp10
