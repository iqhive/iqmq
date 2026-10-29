// Package rabbitmq is the iqmq driver for RabbitMQ over AMQP 0-9-1
// (the driver documentation, variant families, and).
//
// It speaks AMQP 0-9-1 through github.com/rabbitmq/amqp091-go, which is the
// client the RabbitMQ team maintains. The AMQP 1.0 profile for RabbitMQ 4 and
// newer is a separate module, `drivers/rabbitmq/amqp10`, and the two are never
// substituted for one another: the specification says so outright, and the reason is that
// they are not one broker seen through two windows — the settlement model, the
// unroutable-message outcome and the request/reply mechanism all differ, so a
// caller silently moved between them would find different behaviour under one
// name.
//
// # Topology
//
// A topic is a durable exchange, `topic`-typed by default, and a subscription
// is a durable quorum queue named `<topic>.<subscription>` bound to it with the
// topic as the routing key. The dead-letter route is a second exchange and
// queue derived from the queue's own name, `<queue>.dlx` and `<queue>.dlq`.
// Every name is derived by `internal/rabbit`, which both RabbitMQ drivers
// share so that a caller who moves between the protocols finds the same
// topology.
//
// Nothing is declared unless [Config.Declare] says so. That is the rule and
// it is not a convenience: a driver that created an exchange behind a caller's
// back would make a typo in a topic name into a new, empty, permanent
// exchange, and the symptom is a publisher that succeeds and a consumer that
// receives nothing for ever. With declaration off, a publication to an
// exchange that does not exist is refused by the broker with a channel
// exception, which this driver maps to CodeNotFound.
//
// # The publication path and the confirmation boundary
//
// Publisher confirms are on. A publication is a `basic.publish` on a
// confirm-mode channel, and the boundary each of the modes means is:
//
//	ConfirmNone      the frame has been written to the socket. Nothing has
//	                 confirmed anything; a broker that died a microsecond
//	                 later never had the message.
//	ConfirmAccepted  the broker sent `basic.ack` for this publication's
//	                 delivery tag. On a quorum queue that means the message
//	                 was written to a majority of the queue's replicas; on a
//	                 classic non-durable queue it means the broker holds it in
//	                 memory.
//	ConfirmDurable   the same `basic.ack`, and the sink refuses to open unless
//	                 the subscription's queues are durable quorum queues, so
//	                 that the boundary really is a durable one.
//
// HOW the publisher waits for that ack is and is
// configurable: per-message (the default), windowed, or asynchronous.
// [Config.ConfirmKind] selects it and `internal/rabbit/confirm` is where the
// three live. The strategy decides how far ahead `PublishAsync` can run. A
// synchronous `Publish` waits for its own publication's `basic.ack` under
// every strategy, because it has nowhere else to report a `basic.nack` or a
// `basic.return`.
//
// Two publication outcomes deserve naming because a caller acts on them
// differently from an ordinary failure:
//
//   - **Rejected.** With [Config.Mandatory] — the default — a publication that
//     no queue is bound for comes back as `basic.return` and is then acked.
//     The broker took the message and threw it away, which the specification makes a
//     rejection: CodeNotFound, not retryable, because retrying will throw it
//     away again. It almost always means the topology is wrong.
//   - **Ambiguous.** A channel that closes with confirms outstanding leaves
//     every one of those publications unresolved. The broker may have taken
//     them all; it may have taken none. The specification forbids reporting that as a
//     definite failure and the specification names doing so a settlement disqualifier, so
//     this driver reports CodeAmbiguous with Ambiguous set. amqp091-go's own
//     answer is to NACK them, which is a definite negative and the wrong one;
//     the driver does not use that path (see `internal/client`).
//
// # The consumption path and settlement
//
// A source is a `basic.consume` on its subscription's queue with
// `basic.qos(prefetch)`. Deliveries reach a bounded ring through
// `driverkit.PushAdapter`, so a broker that pushes faster than the application
// consumes is answered by backpressure rather than by unbounded memory.
//
// Settlement is one frame each: Ack is `basic.ack`, an immediate Nack is
// `basic.nack{requeue=true}`, a Nack with a dead-letter action is
// `basic.nack{requeue=false}` onto the queue's dead-letter exchange, and a
// Nack with a delay is — a TTL queue that dead-letters back, or the
// delayed-message exchange when the plugin is installed. Settlement is
// serialised per channel and individual: `basic.ack{multiple=true}` would
// settle every delivery up to a tag, which is safe only when deliveries are
// settled in the order they arrived and is not an assumption this driver
// makes.
//
// `x-delivery-count` is Attempt. It is a quorum-queue field: a classic
// queue does not carry it, and a driver reading it off one would report every
// delivery as the first. The capability table says so conditionally and the
// version matrix is what proves it.
//
// # The at-least-once window
//
// The window is from the broker's `basic.deliver` to this driver's
// `basic.ack`. A process that stops inside it — a crash, a channel close, a
// connection loss — leaves the delivery unacknowledged, and RabbitMQ redelivers
// it to whichever consumer of that queue is next. So a handler may run twice
// for one message and the accounting is the core, not this driver's.
//
// A settlement whose channel has gone is refused with ErrLeaseLost and never
// applied: delivery tags restart at 1 on a new channel, so an ack carrying an
// old one would settle whatever delivery took that tag afterwards. That is the disqualifier in AMQP's clothing, `FuzzRabbitTagSettlement` is the
// evidence, and the generation `driverkit.TokenTable` bumps on every
// reconnection is the mechanism.
//
// # Reconnection
//
// This driver owns its dial loop, which the specification requires and which the client
// library — since v1.14 — can also do itself. The driver's own loop is used
// because a reconnection is not only a new socket: it invalidates every
// delivery tag handed out, it has to resolve the publications the old channel
// was holding as ambiguous rather than failed, and it has to restore the
// topology and the consumers before the first fetch returns. See
// `reconnect.go` for the order those happen in and why.
//
// # Maturity and tier
//
// Tier 2. Everything the specification asks of this driver is implemented; what
// keeps it below tier 1 is the evidence rather than the surface — the
// conformance suite runs against three broker versions and one delayed-message
// image, and the managed-service deployments the tier-1 drivers have been run
// against do not exist for this one.
package rabbitmq
