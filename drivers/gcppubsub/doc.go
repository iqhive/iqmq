// Package gcppubsub is the iqmq driver for Google Cloud Pub/Sub.
//
// It is built on cloud.google.com/go/pubsub/v2, the current client generation.
// The deprecated v1 API is the other arm and lives in
// [github.com/iqhive/iqmq/drivers/gcppubsub/v1compat], which reuses every line
// of this package's semantics behind one client interface — so the two arms
// differ by their client library and by nothing else.
//
// # Semantics
//
// Pub/Sub is a topic with named subscriptions and an acknowledgement deadline.
// A publication is stored in at least two zones before the service answers
// with a message identifier; every subscription of the topic receives every
// message independently; a delivery is invisible to other consumers of that
// subscription for the deadline; an acknowledgement removes it from that
// subscription's backlog; and a message nobody acknowledges is delivered again
// when the deadline passes. That mechanism decides almost everything below.
//
//   - **Confirmation boundary.** [github.com/iqhive/iqmq.ConfirmAccepted] and
//     [github.com/iqhive/iqmq.ConfirmDurable] are the SAME boundary and
//     neither costs an extra round trip: PublishResult.Get answers once the
//     service has stored the message. ConfirmNone is accepted and is also that
//     boundary — there is no fire-and-forget publication in the client library
//     — and the receipt says which boundary the publication actually reached.
//   - **Acknowledgement boundary.** An Ack is the client library's own
//     batched Acknowledge request. On an ordinary subscription the library
//     readies the call immediately and the Ack returns without an outcome,
//     which is what Pub/Sub's acknowledgements are: best effort. With
//     [Config.ExactlyOnce] the settlement has a real outcome and the Ack
//     returns it. A refusal is final and not retryable: the library sends one
//     settlement per message, so the delivery's lease is lost and the message
//     comes back once its deadline passes.
//   - **At-least-once window.** From the delivery to the acknowledgement. A
//     handler that takes longer than the acknowledgement deadline would have
//     its message delivered to somebody else — which the client library
//     prevents by extending the deadline automatically, up to
//     [Config.MaxExtension]. Past that, the lease really does end, and this
//     driver refuses a settlement whose local lease view has run out BEFORE it
//     calls the service (see "The local lease check").
//
// # Push to pull
//
// Subscriber.Receive delivers by CALLBACK and the SPI asks the driver to be
// called instead. [github.com/iqhive/iqmq/driverkit.PushAdapter] is what makes
// the two meet: the callback pushes into a bounded ring and blocks while the
// ring is full, and because the library releases a message's flow-control slot
// when the callback RETURNS, a blocked callback is a stream that stops asking
// for more. That is the only backpressure a streaming pull has.
// MaxOutstandingMessages is the prefetch and NumGoroutines is one.
//
// # The local lease check
//
// The service accepts an acknowledgement for an identifier whose message
// another consumer now holds. There is no API that asks "do I still hold
// this?", the acknowledgement does not fail, and the message is gone from the
// other consumer's backlog. So the refusal happens in the driver, before the
// call: every settlement compares the delivery's lease — as this driver
// recorded it at the delivery, on the injected clock — against now, and
// answers [github.com/iqhive/iqmq.ErrLeaseLost] when the lease has run out.
//
// The lease starts at the earliest moment the message can have reached the
// client library, not when the callback is handed it. The library starts its
// extension budget when the streaming pull returns a message, and a message
// can wait in the library while a callback is blocked on the full ring, so a
// lease started at the callback could outlast the library's extension by the
// length of that wait.
//
// A second, sharper case is the same message delivered TWICE to one source,
// because its deadline expired. Both acknowledgement identifiers are valid at
// the service. The source marks the earlier delivery superseded the moment the
// later one arrives, so a settlement for the earlier one reports ErrLeaseLost
// rather than acknowledging the message the later delivery owns. It also nacks
// the earlier delivery's message in the client library, which otherwise goes
// on extending that identifier and holds Close for it until MaxExtension.
//
// # What this driver does NOT claim
//
// Two claims the service's own vocabulary would suggest are deliberately not
// made, and each carries its reason in the capability table:
//
//   - Exactly-once DELIVERY. Pub/Sub's "exactly-once delivery" is a guarantee
//     about a SETTLEMENT: an acknowledged message is never redelivered, and
//     AckWithResult reports whether the acknowledgement took. An
//     UNACKNOWLEDGED message whose deadline expires is redelivered on such a
//     subscription exactly as on any other, so the feature — a message
//     delivered exactly once — is not something the service promises.
//     [Config.ExactlyOnce] is still worth setting, for the settlement outcome.
//   - Scheduled publication. There is no publication timer at all, so Delay
//     and DeliverAt are refused before any I/O rather than silently ignored.
//
// # Ordering keys, and the publisher that stops
//
// Ordering is conditional on [Config.Ordering], because the client library's
// EnableMessageOrdering is a flag on the PUBLISHER and not on a publication: a
// publisher built without it refuses every message that carries an ordering
// key. With it, Message.Key becomes the ordering key and the service delivers
// one key's messages in publication order.
//
// The part that needs care is the failure path. When an ordered publication
// fails, the library puts that key into a permanently failed state, and every
// LATER publication of the same key is refused for the life of the publisher —
// silently, as far as a caller reading only the first failure is concerned.
// That is the message loss in slow motion. This driver therefore calls
// ResumePublish on every failure of an ordered publication, on both the
// synchronous path and the completion pool's.
//
// # Delayed negative settlement is an emulation
//
// Pub/Sub has no nack delay: a nack makes the message deliverable at once. A
// delayed negative settlement is therefore an IN-PROCESS HOLD — the driver
// keeps the delivery unacknowledged while the library goes on extending its
// deadline, and nacks it when the delay elapses — which is advertised as
// [github.com/iqhive/iqmq.EmulatedVolatile] and never satisfies a durability
// requirement. A process that dies during a hold loses the delay.
//
// # A hundred attributes, and the envelope
//
// A Pub/Sub attribute value is a STRING. The question for this driver is
// therefore what happens to a binary header value, a repeated header key, a
// key the service's attribute-name grammar refuses, and the hundred-and-first
// attribute — and the answer is the metadata mode:
//
//   - MetadataNativeStrict, the default, refuses it before any I/O.
//   - MetadataAdaptive encodes the whole message into the data as an iqmq
//     envelope.
//   - MetadataPortableEnvelope always envelopes.
//   - MetadataLenientDrop drops headers, with a metric.
//
// # Maturity and tier
//
// Tier 2 (supported, tested against the pinned Pub/Sub emulator in
// `make integration` and against an in-process fake in `make test-race`).
// Numbers taken against the emulator describe the client and the container and
// are never evidence for the managed service (layer 4), and the emulator
// does not implement everything: the capability matrix marks what it could not
// exercise.
//
// # Opening
//
//	client, err := gcppubsub.Open(ctx, gcppubsub.Config{
//	    ProjectID: "my-project",
//	    Ordering:  true,
//	})
//
// or from a DSN, once the driver is registered with a
// [github.com/iqhive/iqmq/dsn.Registry]:
//
//	gcppubsub://my-project?ordering=true&ackdeadline=30s
//	gcppubsub://127.0.0.1:8085/my-project?insecuretransport=true&autocreate=true
//
// [OpenWithClient] takes a *pubsub.Client the caller already has, with explicit
// [github.com/iqhive/iqmq/driverkit.Ownership]. It is the way in for a
// credential scheme a typed Config cannot carry.
//
// # Topics and subscriptions
//
// A topic maps to a topic ID by [Config.TopicID] — the prefix, then the name
// with "/" replaced by "~" — and a subscription maps to a subscription ID by
// [Config.SubscriptionID]. The mapping is injective, because "~" is in the
// service's identifier grammar and not in
// [github.com/iqhive/iqmq.ValidateTopic]'s.
//
// # Documentation
//
// docs/drivers/gcppubsub.md carries the fixed per-driver sections the specification
// requires, including the ordering statement delegates to each driver
// page, the full table of what maps to what, and the service limits this
// driver read out of the pinned client.
package gcppubsub
