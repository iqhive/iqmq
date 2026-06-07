// It is a separate module from `drivers/activemq` and shares no code with it
// above `internal/activemq`, because there is nothing to share: STOMP is a
// text framing protocol with one flat set of headers and AMQP 1.0 is a binary
// one with three sections and typed values, so the only thing the two arms
// have in common is the DECISION about which iqmq field goes where — which is
// exactly what `internal/activemq/protocol` holds, and why the specification is a family rather than two unrelated drivers.
//
// the specification's own summary of this arm is short and the reason it is short is the
// point: "STOMP: virtual topics, `client-individual` ack, selectors via typed
// config. Tier 3." The AMQP 1.0 arm is the default and this one is kept for
// ActiveMQ Classic, where a STOMP selector is the only server-side filter
// either protocol offers.
//
// # The finding a reader should see first
//
// **A STOMP `NACK` frame does not return a message on either broker**, and
// nothing in either product's documentation says so. Measured against the
// pinned images:
//
//   - On Apache ActiveMQ Artemis a NACK is treated exactly as an ACK. One NACK
//     and one ACK took `getMessagesAcknowledged` from 0 to 2, the queue
//     emptied, the dead-letter queue did not grow, and a fresh subscription
//     saw nothing. A transacted ACK followed by ABORT is acknowledged anyway
//     — while a transacted SEND that is aborted is correctly discarded, so
//     go-stomp's transactions do reach the broker and Artemis's transacted
//     ACKNOWLEDGEMENT does not roll back.
//   - On ActiveMQ Classic a NACK is a POISON_ACK: the message is not returned
//     to the queue, it goes to the broker's dead-letter strategy. It cannot be
//     transacted at all — "Poison ack cannot be transacted", which arrives as
//     an ERROR frame and closes the connection.
//
// So a driver that mapped the specification's `Nack` onto the protocol's NACK frame would
// silently DESTROY messages on Artemis while reporting a requeue. This one
// does not. What it does instead is the protocol's own recovery mechanism,
// which is measured to work on both brokers: an unacknowledged
// `client-individual` delivery is redelivered when its SUBSCRIPTION goes
// away, with `redelivered: true` on it. So `Nack(NackRequeue())` unsubscribes
// and re-subscribes, and the broker gives the message back.
//
// The cost of that is stated rather than hidden, and it is the reason the
// claim is [mq.EmulatedDurable] rather than native: a subscription reset
// returns EVERY unacknowledged delivery of that subscription, not just the one
// that was nacked. Every other delivery the caller was holding becomes stale
// and its settlement reports [mq.ErrLeaseLost], which is the honest answer
// for a delivery that is no longer this consumer's. A caller that nacks often
// and holds a deep prefetch wants the AMQP 1.0 arm.
//
// # Settlement
//
//	Ack                 ACK frame, `ack: client-individual`
//	Nack(requeue)       UNSUBSCRIBE + SUBSCRIBE; the broker redelivers
//	Nack(requeue,delay) refused: no header either broker reads on a NACK
//	Nack(drop)          NACK frame
//	Nack(deadLetter)    a publication to Config.DeadLetterTopic, then ACK;
//	                    or the NACK frame on Classic, whose poison-ack
//	                    reaches the broker's dead-letter strategy
//	Reject              as Nack(drop)
//
// The at-least-once window runs from the SEND frame until the ACK frame is
// WRITTEN, and there is no answer to wait for: go-stomp's `Conn.Ack` and
// `Conn.Nack` take no options, so a settlement frame cannot carry a `receipt`
// header at all and a driver cannot know that the broker applied it. That is
// the asymmetry with the publish path, which does have a receipt, and it is
// why an acknowledgement whose connection died is reported as a LOST LEASE
// rather than as an unknown outcome: the broker still holds the delivery and
// hands it to the next subscription. What DOES produce an unanswered receipt
// on the settlement path is the requeue, because a requeue is an UNSUBSCRIBE
// and that frame does carry one.
//
// # What a STOMP delivery cannot say
//
// **There is no attempt count.** Measured on both brokers, a STOMP delivery
// carries no `delivery-count`, no `JMSXDeliveryCount` and no
// `_AMQ_DELIVERY_COUNT`; the whole of what it says about its history is the
// boolean `redelivered` header -- and that boolean is not reliable either:
// measured on Artemis, the redelivery a requeue produces arrives with
// `redelivered: false`. So Attempt is ZERO, which is its documented
// value for "the broker does not count attempts", `DeliveryInfo.Redelivered`
// is the honest field, and a `MaxAttempts` policy above this driver has
// nothing to count with. The AMQP 1.0 arm has a real
// `delivery-count` and that asymmetry is one of the most important
// measured differences.
//
// **There is no binary header.** A frame header is text, so a header value or
// a message key that is not valid UTF-8 is refused before any I/O rather than
// mangled. A caller with binary headers wants `MetadataPortableEnvelope`.
//
// **There is no lease.** A delivery is this subscription's until the
// subscription goes away, so `DeliveryInfo.LeaseExpires` is the zero time and
// there is no extender. What bounds the deliveries a consumer holds is the
// driver's own bounded buffer and the broker's flow-control window.
//
// # Delayed publication works, and on Classic only if the broker allows it
//
// Both brokers read the `AMQ_SCHEDULED_DELAY` frame header, in whole
// milliseconds: measured 2.5 s on Artemis (echoed on delivery as
// `_AMQ_SCHED_DELIVERY`, an absolute time) and 2.6 s on Classic (echoed as
// `scheduledJobId`). Classic's scheduler is OFF unless the broker is
// configured with `schedulerSupport="true"`, and with it off the header
// travels through to the consumer and the message is delivered at once — so
// [Config.Scheduler] is how a deployment says the scheduler is on, and the
// capability table withdraws [mq.FeatureScheduledPublish] when it is not.
//
// # Selectors
//
// A `selector` header on SUBSCRIBE is a JMS message selector, and it is the
// one thing this arm has that the AMQP 1.0 arm does not — go-amqp exposes no
// way to set a link source filter. Measured: it filters on Classic (a
// `colour = 'red'` selector kept a blue message out) and is IGNORED on
// Artemis when the subscription is a fully qualified queue name, which is the
// only form this driver's durable shared subscription takes. So
// [mq.FeatureServerFilter] is native on Classic and unsupported on Artemis,
// and [Config.Selector] is refused there rather than accepted and dropped.
//
// # Topology
//
// This driver creates none, which is. On Classic a consumer queue comes
// into existence when a subscription attaches to it, and it has to exist
// BEFORE the first publication or the virtual topic copies nothing into it; on
// Artemis, with the broker's auto-creation disabled, a SUBSCRIBE to a
// destination that does not exist is refused with `AMQ339001 Destination does
// not exist`. Neither is reachable over STOMP as an operation, so
// `Client.Admin()` reports ErrUnsupported and `drivers/activemq`'s Admin (or
// the broker's own console) is what creates it.
//
// # Maturity
//
// Tier 3. The evidence is two conformance reports, one per broker,
// under `conformance/reports/latest/`.
package stomp
