// Package jetstream is the iqmq driver for NATS **JetStream**: a replicated,
// persistent stream with durable consumers, per-message acknowledgement and
// redelivery.
//
// NATS Core — subject routing with no store and no settlement — is a different
// driver (bitbucket.org/iqhive/iqmq/drivers/nats) over the same server and the
// same client library. ADR-010 the specification chose separate modules so that a caller
// cannot mistake one profile's capabilities for the other's, and the two
// drivers share no code: everything below is about the stream.
//
// # Semantics
//
// JetStream is at-least-once. A publication is stored before it is confirmed, a
// delivery is leased, and a lease that runs out is redelivered. That single fact
// decides almost everything below, and it is the opposite of the Core driver's.
//
//   - **Confirmation boundary.** [bitbucket.org/iqhive/iqmq.ConfirmNone] publishes
//     with core NATS and asks for no reply: the message reaches a stream-bound
//     subject and is stored, but nothing waits and nothing is reported.
//     ConfirmAccepted — the default — and
//     [bitbucket.org/iqhive/iqmq.ConfirmDurable] both wait for the stream's
//     **PubAck**, which the server sends after the message is committed to the
//     stream's storage. On this broker "accepted" and "durable" are therefore
//     the same round trip and the same guarantee, which is why
//     [bitbucket.org/iqhive/iqmq.FeatureDurableConfirm] is advertised natively and
//     ConfirmDurable is not refused.
//   - **Acknowledgement boundary.** Ack is a message on the delivery's own reply
//     subject. [Config.DoubleAck] turns it into a request/reply round trip, so
//     that Ack returns only once the server has recorded it; without it the ack
//     is a publish and a link that dies immediately afterwards leaves the
//     message redeliverable. Nack is `-NAK` (immediate) or `-NAK {"delay":n}`
//     (delayed), Reject is a core publish to the dead-letter subject followed by
//     `+TERM`, and an in-progress extension is `+WPI`.
//   - **At-least-once window.** From the first delivery until the ack the server
//     recorded. A handler that takes longer than the consumer's AckWait, a
//     process that dies mid-handler and a link that drops before the ack all
//     produce a redelivery, which is a duplicate the application has to be able
//     to absorb. `DeliveryInfo.Attempt` is the server's `NumDelivered`, so the
//     second delivery of a record says so.
//
// # A stale ack must not settle a redelivery
//
// This is the one hazard worth reading the source for. JetStream acknowledges by
// **reply subject**, and the server's `AckExplicit` path deletes its pending
// entry for the message's *stream sequence* without looking at which delivery
// the reply subject came from. So the reply subject of a delivery whose lease
// expired still works: acking it acknowledges the record that a *later* delivery
// now owns, and the holder of that later delivery never sees a redelivery. The
// message is gone and nothing in the client or the server reports it.
//
// This driver refuses that ack. Every delivery takes a slot in a
// [bitbucket.org/iqhive/iqmq/driverkit.TokenTable] and the source keeps an index
// from stream sequence to token: when a record is delivered again, the previous
// delivery's slot is released, which bumps its generation, and a settlement
// carrying the old token reports
// [bitbucket.org/iqhive/iqmq.ErrLeaseLost] rather than reaching the wire.
// FuzzNATSStaleAck and TestStaleAckIsRefused are what hold it, and
// the performance budgets makes the unguarded behaviour a disqualifier.
//
// # Dead-lettering is publish, then Term
//
// [bitbucket.org/iqhive/iqmq.Delivery.Reject] publishes the message to
// [Config.DeadLetterTopic] and waits for that publication's PubAck, and only
// then sends `+TERM`. The order is the whole of it: a Term that ran first would
// destroy the message before its copy was durable, so a failure in between would
// lose it. A backend with no dead-letter subject configured answers Reject with
// [bitbucket.org/iqhive/iqmq.ErrUnsupported] and settles nothing, which is the specification's
// rule and leaves the message redeliverable.
//
// # Maturity and tier
//
// Tier 1 (core, tested against a pinned container in `make integration`). The
// module's own tests run against an embedded nats-server with JetStream enabled
// and need no container.
//
// # Opening
//
//	client, err := jetstream.Open(ctx, jetstream.Config{
//		Addrs:  []string{"nats://127.0.0.1:4222"},
//		Stream: "ORDERS",
//	})
//
// or from a DSN, once the driver is registered with a
// [bitbucket.org/iqhive/iqmq/dsn.Registry]:
//
//	natsjs://user:password@host:4222/acme?stream=ORDERS&ackwait=30s&doubleack=true
//
// [OpenWithClient] takes a *nats.Conn the caller already has, with explicit
// [bitbucket.org/iqhive/iqmq/driverkit.Ownership] so that iqmq never closes a
// connection it was not asked to close.
//
// # Topics, subjects, streams and consumers
//
// A portable topic maps to a subject exactly as it does on the Core driver: "/"
// becomes ".", [Config.SubjectPrefix] is prepended as a token, and the mapping
// is not injective. The subject has to be captured by [Config.Stream], which is
// the one piece of topology this broker really has; a publication to a subject no
// stream captures is refused by the server with
// [bitbucket.org/iqhive/iqmq.CodeNotFound] rather than silently dropped.
//
// A subscription maps to a **durable pull consumer** whose `FilterSubjects` is
// the mapped subject and whose `MaxAckPending` is the consumer's prefetch. Two
// consumers of one subscription share one durable and therefore compete; two
// subscriptions are two durables and each receives everything. The
// durable's name is [Config.ConsumerPrefix], the sanitised subscription name and
// a short hash of the filter, because one subscription name may be used on two
// topics and a durable is per filter.
//
// # Documentation
//
// docs/drivers/jetstream.md carries the fixed per-driver sections the specification requires,
// including the version matrix: `Nats-TTL` needs a 2.11+ server and message
// schedules a 2.12+ one, so [bitbucket.org/iqhive/iqmq.FeatureMessageTTL] and
// [bitbucket.org/iqhive/iqmq.FeatureScheduledPublish] are conditional claims whose
// condition is read off the stream this client is bound to.
package jetstream
