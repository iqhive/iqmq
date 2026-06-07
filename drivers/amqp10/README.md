# iqmq/drivers/amqp10 — the shared AMQP 1.0 core

This module is the AMQP 1.0 **core** of the iqmq driver family: the protocol, the
links, the credit, the dispositions and the dial loop, with every
broker-specific decision moved behind one seam.

**It is not a driver.** Opening it needs a `Profile`, because AMQP 1.0 settles on
almost nothing a message queue needs. Two conforming brokers agree about
transfers, dispositions, credit and the message format, and agree about nothing
else — so this module owns the first list and refuses to guess at the second.

| Built on it | What the profile supplies |
|---|---|
| `drivers/rabbitmq/amqp10` | `/exchanges/<name>/<key>` and `/queues/<name>` addressing, a `>= 4.0` version gate, no delayed publication |
| `drivers/activemq` | Artemis's `address::queue` and Classic's virtual topics, `x-opt-delivery-delay` / `AMQ_SCHEDULED_DELAY`, `_AMQ_DUPL_ID` |
| a future Azure Service Bus driver | the specification's "Azure Service Bus (shared `amqp10` internals)" |
| the generic profile | plain addresses and nothing broker-specific claimed |

## The seam

`Profile` is the most important type here and its doc comment is the one to read
first. Everything it carries is something AMQP 1.0 does not decide:

- **What an address is.** Artemis names a shared durable subscription by the
  fully qualified queue name `address::queue`; RabbitMQ 4 names a queue
  `/queues/<name>`; ActiveMQ Classic uses `Consumer.<sub>.VirtualTopic.<topic>`.
  There is no intersection, so `SinkAddress` and `SourceAddress` are the
  profile's — and the rule (one subscription's consumers compete, two
  subscriptions each receive everything) is delivered by them.
- **What the broker can do.** The core supplies a base capability table of what
  this protocol and this implementation really provide; the profile narrows or
  widens it, and `CoreCeiling` refuses a claim the core has no code for.
- **How the per-message controls are expressed**: a delayed publication, a
  delayed nack, the attempt count, a de-duplication key, a durable confirmation.
- **Which brokers work at all** (`CheckBroker`), and **whether there is a
  topology API** (`Admin`).

A profile is a struct of functions rather than an interface, for the reason
`mq.Hooks` is: most of what distinguishes two AMQP 1.0 brokers is data, and an
interface would make every profile implement a dozen methods in order to answer
"this broker cannot do that" a dozen times. The price is that a new obligation
is not a compile error for an existing profile, which is why `Profile.Validate`
refuses an incomplete one before the dial and why every optional rule's zero
value means "refused" rather than "guess".

## Semantics

At-least-once. A transfer is unsettled until the receiver says what it did with
it, and a delivery is the receiver's until it settles it or its link goes away.

| Boundary | What it is here |
|---|---|
| `ConfirmNone` | a **pre-settled** transfer: the frame leaves, no disposition is coming, and the call costs no round trip. Local admission, which the specification says may be lossy. |
| `ConfirmAccepted` (default) | the peer's `accepted` disposition, which is the strongest ordinary acknowledgement this protocol has. |
| `ConfirmDurable` | the **profile's** (`Profile.Durable`). The protocol has no durable disposition, so a publisher that asks for it on a profile that defines none fails when its sink is opened. |
| Ack | the `accepted` outcome. `Config.ReceiverSettle` decides when it is final: `first` (default) settles on the write and costs no round trip; `second` waits for the peer's confirmation. |
| Nack(requeue) | `released`, which does **not** count the attempt. |
| Nack(requeue, delay) | `modified` with the profile's annotation, or `ErrUnsupported` when the profile has none. |
| Nack(drop) and Reject | `rejected`. AMQP 1.0 has no "destroy" outcome, so on a broker with a dead-letter address a dropped message reaches it rather than being destroyed. |
| at-least-once window | from the first transfer until the disposition the peer acted on. |
| lease | **none.** An unsettled delivery is this receiver's for as long as the link lives, so `DeliveryInfo.LeaseExpires` is the zero time, `FeatureExtendLease` is unsupported, and there is no `DeadlineExtender` anywhere in the module. What bounds a consumer's unsettled deliveries is the link **credit**, which is the prefetch. |

## The two hazards the specification names

**A stale disposition must not settle the delivery that took its place.**
Detaching an AMQP 1.0 link returns everything unsettled on it to the node, where
another consumer may pick it up at once. Every delivery therefore records the
connection generation of the link it arrived on and takes a slot in a
`driverkit.TokenTable`; a lost connection bumps the generation and invalidates
every table **before anything else happens**, and a settlement whose delivery
names an older generation reports `ErrLeaseLost` without a frame reaching the
wire. `links.go` says why both halves are needed.

**An ambiguous publication must not be reported as a definite failure.** A
transfer that has left the socket and whose disposition never arrived may have
been accepted, so a caller that retries publishes twice. Every failure on the
publish path carries the ambiguity: `sink.go` distinguishes a peer's own answer
(definite) from a link or connection that died (ambiguous), and `errors.go`
splits the condition table into the two halves so that a
`amqp:connection:forced` on a publish is ambiguous and the same condition on a
fetch is a plain retry.

## Testing needs no broker

Every test in this module runs against the scripted peer in `internal/client`:
programmable dispositions, link detach, connection loss, credit behaviour and
out-of-order dispositions are all values a test sets. Three of the four critical
scenarios are unreachable from a real broker in a unit test, and against the
peer each is three lines of script.

    go test ./...                    # no container, no network
    go test -race ./...
    go test -run '^Fuzz' ./...       # the committed seed corpus
    IQMQ_AMQP10_ADDR=amqp://localhost:5672 go test -run TestConformance ./...

`BackendWithDialer` is the same seam a broker-specific module uses to test **its**
profile against this core, which is the point of the core being a module of its
own.

## Tuning knobs and their measured effect (rule 10)

| Knob | Effect |
|---|---|
| `Config.ReceiverSettle` | `first` costs no round trip per settlement and reports a failed settlement as a lost lease; `second` costs one round trip and reports it as an unknown outcome. The choice changes the error table, not only the latency. |
| `SourceConfig.Prefetch` | the link credit, and therefore the most unsettled deliveries one source holds. A fetch of a batch costs one blocking wait and a non-blocking drain, so a larger prefetch is fewer waits rather than more memory per wait. |
| `Config.MaxCredit` | caps the prefetch for a broker with a per-link ceiling of its own. |
| `Config.MaxPending` | the asynchronous publications one sink may have outstanding. A full window is `CodeBackpressure` rather than a buffer that grows. |
| `Config.MaxFrameSize`, `Config.IdleTimeout` | the connection's own negotiation. A shorter idle timeout notices a dead TCP connection sooner and costs an empty frame per interval. |
| `Config.Backoff`, `ReconnectBase`, `ReconnectMax`, `ReconnectGrowth` | the dial loop. The default is full jitter, named explicitly rather than left to the provisional `Auto` winner, which is no delay at all. |
| metadata mode | `MetadataNativeStrict` is free and refuses what the protocol cannot carry; the envelope modes cost an encode and carry a repeated header key. `BenchmarkAMQP10/Encode` measures both. |

`BenchmarkAMQP10` measures the pure paths and needs no broker;
`BenchmarkDriver` is env-gated on `IQMQ_AMQP10_ADDR`.

## Allocations

| Path | Allocations |
|---|---|
| encode a bare payload into a reused `Buf` | 0 |
| encode one header | 2 (the boxed `any` an application property is, and the string copy of the header's bytes) |
| encode a message key | 1 (the `group-id` string) |
| decode a bare payload into reused storage | 0 |
| decode one string-valued application property | 1 |
| a publication or a fetch on a closed handle | 0 (the preallocated `*OpError` per handle) |

None of the remaining allocations can be removed without `unsafe`, which 
rule 8 confines to two packages that are not this one. `allocs_test.go` asserts
each count exactly, so a change to the mapping is a diff in that file.
