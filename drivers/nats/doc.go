// Package nats is the iqmq driver for NATS **Core**: subject-based publish and
// subscribe with no persistence and no per-message settlement.
//
// JetStream — streams, durable consumers, acknowledgement and replay — is a
// different driver (bitbucket.org/iqhive/iqmq/drivers/jetstream) over the same
// server. Nothing here reaches for it, and nothing here pretends to it.
//
// # Semantics
//
// NATS Core is at-most-once. The server holds nothing: a message is routed to
// whatever is subscribed at the instant it arrives and is then gone. A
// subscriber that is not connected, or that cannot keep up, does not get it.
// That single fact decides almost everything below.
//
//   - **Confirmation boundary.** [bitbucket.org/iqhive/iqmq.ConfirmNone] returns
//     once the publication is in the client's write buffer. ConfirmAccepted —
//     the default — additionally round-trips PING/PONG (`FlushWithContext`), so
//     it returns once the server has *parsed* every byte written before it.
//     That is a socket-level boundary and nothing more: the server has seen the
//     message and has already forgotten it.
//     [bitbucket.org/iqhive/iqmq.ConfirmDurable] is refused when the publisher is
//     constructed, because this profile has no durable boundary to define and the specification requires the refusal to happen there rather than at the first
//     message. A driver that answered "durable" here would be lying about the
//     one property a caller asks that question to find out.
//   - **Acknowledgement boundary: there is none.** [Source.Settler] returns
//     nil, which is AckNone model: the core answers every Ack, Nack and
//     Reject with [bitbucket.org/iqhive/iqmq.ErrNotSettleable] and never calls the
//     driver. A delivery is complete the moment the handler has it.
//   - **At-least-once window: none.** There is no redelivery, so there is no
//     window. The failure mode of this driver is loss, not duplication: a
//     handler that panics loses the message, and so does a subscriber that
//     falls behind its pending limits (see "Slow consumers" below).
//
// # Maturity and tier
//
// Tier 1 (core, tested against a pinned container in `make integration`). The
// driver is small because the broker is: everything iqmq layers on top —
// retries, dead-lettering, ordering lanes, worker pools — is the core's, and a
// driver that added a second one would break the specification's "exactly one layer owns
// redelivery accounting".
//
// # Opening
//
//	client, err := nats.Open(ctx, nats.Config{Addrs: []string{"nats://127.0.0.1:4222"}})
//
// or from a DSN, once the driver is registered with a [bitbucket.org/iqhive/iqmq/dsn.Registry]:
//
//	nats://user:password@host:4222/prefix?prefetch=256&pendingmessages=8192
//
// [OpenWithClient] takes a *nats.Conn the caller already has, with explicit
// [bitbucket.org/iqhive/iqmq/driverkit.Ownership] so that iqmq never closes a
// connection it was not asked to close.
//
// # Topics, subjects and queue groups
//
// A portable topic maps to a subject by replacing "/" with "." and prefixing
// [Config.SubjectPrefix]; a subscription name maps to a queue group, which is
// what makes two consumers of one subscription compete and two consumers of
// different subscriptions both receive. The mapping is not injective —
// "a/b" and "a.b" are one subject — which is stated here rather than hidden,
// and is.
//
// A consumer's Topic may carry NATS wildcards ("orders.*", "orders.>"), which
// is what [bitbucket.org/iqhive/iqmq.FeatureWildcardSubscribe] means here. A
// publication's topic may not: a publisher that named a wildcard would be
// publishing to a pattern, which NATS does not do and which iqmq refuses before
// any I/O.
//
// # Slow consumers
//
// Every subscription is opened with [Config.PendingMessages] and
// [Config.PendingBytes] as its `SetPendingLimits`, and the driver's fetch ring
// is bounded by the consumer's prefetch. When a subscriber cannot keep up the
// server-side client buffer overflows, nats.go reports ErrSlowConsumer through
// the asynchronous error handler, and the driver's health becomes
// [bitbucket.org/iqhive/iqmq.HealthDegraded] with that error — which is the specification's
// rule that a driver's background failures surface through health and hooks
// rather than only in a log. Messages dropped that way are lost; on an
// at-most-once broker that is the contract, not a defect.
//
// # Documentation
//
// docs/drivers/nats.md carries the fixed per-driver sections the specification requires,
// including the ordering statement delegates to each driver page and the
// full table of what maps to what.
package nats
