// It is a PROFILE of `drivers/amqp10`, which owns the protocol — the session
// per link, the manual credit window, the settlement token table, the
// reconnect loop and the condition table — and knows nothing about any
// broker. What this module adds is the two things AMQP 1.0 does not settle and
// the two ActiveMQ products disagree about: how a topic and a subscription are
// addressed, and which of the features the combination really has.
//
// # Two brokers, one module
//
// "ActiveMQ" is two products with one name, and their topic mappings have
// nothing in common.
//
// Apache ActiveMQ **Artemis** has an address model: a topic is a MULTICAST
// address, every subscription is a durable queue bound to it, and a consumer
// names one of those queues by the fully qualified queue name
// `address::queue`. That is the rule exactly — two consumers of one
// subscription attach to the same queue and compete, two consumers of
// different subscriptions attach to different queues and each receives the
// message.
//
// Apache ActiveMQ **Classic** has no address model. A topic is a topic and a
// queue is a queue, and the way to get a durable shared subscription is a
// VIRTUAL TOPIC: a publication to `topic://VirtualTopic.orders` is copied by
// the broker into every queue named `Consumer.<name>.VirtualTopic.orders`, and
// a subscription is therefore a queue whose NAME encodes the topic. The
// convention is the broker's default and this driver does not rename it: a
// driver that used a prefix of its own would publish into a destination
// nothing is subscribed to and report success. [Config.Prefix] goes INSIDE
// that convention for the same reason, and the ordering was settled by the
// broker rather than by reasoning -- see `internal/activemq.Config.Prefix`.
//
// [Config.Broker] selects between them and has no default. Nothing on the wire
// says which product a caller MEANT, and getting it wrong is not an error on
// either broker — Artemis auto-creates an address literally called
// `topic://VirtualTopic.orders` unless the deployment turned auto-creation off,
// and publishes into it for ever. The specification's "never silently substituted" is why
// the field is required and why [CheckBroker] refuses a broker that identifies
// itself as the other one.
//
// # This driver creates no topology
//
// A sink or a source ATTACHES to a destination and never creates one. The specification
// makes implicit topology creation a defect, and on Artemis it is a defect
// with teeth: a typo in a topic name becomes an address, the publications go
// into it, and nothing anywhere reports a problem. So topology is
// [mq.Admin]'s, which on Artemis is the broker's own management address
// (admin.go) and on Classic does not exist over AMQP 1.0 at all.
//
// The consequence a deployment has to know is that this driver's guarantees
// are only as good as the broker's configuration. Run Artemis with auto-create
// DISABLED (`artemis create --no-autocreate`, or `<auto-create-queues>false`
// and `<auto-create-addresses>false` in the address settings). Measured
// against the pinned image with auto-creation off, an attach to a destination
// that does not exist is refused with `amqp:not-found` — "AMQ119010: source
// address does not exist" for a receiver, "AMQ119002: target address <name>
// does not exist" for a sender — which the core's condition table turns into
// [mq.CodeNotFound]. With auto-creation ON, which is the image's default, the
// same attach silently creates the address and this driver cannot tell.
// Classic creates a destination on attach and cannot be told not to without an
// authorization plugin, so on Classic the claim is the driver's own and not
// provable from the outside.
//
// # Settlement
//
// The four AMQP dispositions carry the verbs, and the mapping is the
// core's:
//
//	Ack               accepted
//	Nack(requeue)     modified{delivery-failed}
//	Nack(drop)        rejected
//	Nack(deadLetter)  rejected, which reaches the broker's dead-letter address
//	Reject            rejected
//
// `released` is deliberately NOT a requeue, and the reason is measured rather
// than argued: on both brokers a `released` delivery comes back with
// `delivery-count` UNCHANGED — three rounds at 0, 0, 0 — while
// `modified{delivery-failed}` comes back at 0, 1, 2. Attempt has to
// rise across a nack or `MaxAttempts` is compared against a number that never
// moves and a poison message is retried for ever. The core makes that choice
// and this module's measurement confirms it on two more brokers.
//
// `modified`'s `undeliverable-here` flag is NOT set, and that too is measured:
// on Artemis a delivery modified with it is never redelivered to the consumer
// that modified it, so a subscription with one consumer loses the message
// entirely.
//
// # What this combination cannot do
//
//   - **No delayed redelivery.** A publication's delay works (below), and the
//     same annotation on a `modified` DISPOSITION does not: measured on
//     Artemis, a `modified` carrying `x-opt-delivery-delay: 3000` was
//     redelivered immediately with `delivery-count` 1. Artemis's redelivery
//     delay is an address-setting rather than a per-nack value, so
//     `Nack(NackDelay(d))` is refused before any I/O.
//   - **No lease.** AMQP 1.0 grants none; an unsettled delivery is the
//     receiver's for as long as the link lives, so `DeliveryInfo.LeaseExpires`
//     is the zero time and there is no extender. What bounds a consumer's
//     unsettled deliveries is the link credit, which is the prefetch.
//   - **No replay, seek or wildcard subscription.** A receiving link attaches
//     to a destination and reads what is there.
//   - **`ReceiverSettleSecond` on Artemis.** Measured: Artemis answers an
//     attach requesting receiver-settle-mode `second` with `first`, and
//     go-amqp refuses the negotiation outright ("amqp: receiver settlement
//     mode \"second\" requested, received \"first\" from server"). So
//     [Config.Validate] refuses the combination rather than letting every
//     source fail at attach.
//
// # Delayed publication
//
// Both brokers schedule a publication and they spell it differently, which is
// why it is a profile decision and not the core's:
//
//	Artemis   message annotation   x-opt-delivery-delay   milliseconds
//	          message annotation   x-opt-delivery-time    absolute epoch ms
//	Classic   application property AMQ_SCHEDULED_DELAY    milliseconds
//	          application property AMQ_SCHEDULED_TIME     absolute epoch ms
//
// Measured: on Artemis the annotation form delays and `AMQ_SCHEDULED_DELAY` as
// an annotation does not; on Classic the application-property form delays and
// the annotation form does not. Classic also needs `schedulerSupport="true"`
// in its broker configuration, which is NOT the default in the pinned image —
// with the scheduler off the header travels through to the consumer and the
// message is delivered immediately, which is a silent loss of the guarantee.
// [Config.Scheduler] is how a deployment says the scheduler is on, and the
// capability table withdraws [mq.FeatureScheduledPublish] when it is not.
//
// # Key ordering
//
// the specification's `JMSXGroupID` is AMQP 1.0's `group-id` message property, and both
// brokers dispatch one group exclusively to one consumer of a subscription.
// Measured on Artemis with two competing consumers on one queue: every `k0`
// message went to the first and every `k1` message to the second, in order.
// That is [mq.FeatureOrderedByKey] at [mq.ScopeKey], and it is the only
// ordering claim this driver makes — a subscription's queue is ordered, but a
// nack returns a message to the queue and the core makes no promise about
// where.
//
// # Maturity
//
// Tier 3 (the specification's "activemq (community welcome)"). The evidence is four
// conformance reports — two brokers times two protocols — under
// `conformance/reports/latest/`, and the differences between them are the
// point: `conformance/suites.json` names all four.
package activemq
