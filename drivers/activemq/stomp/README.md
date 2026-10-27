# iqmq/drivers/activemq/stomp — Apache ActiveMQ over STOMP 1.2

It is a separate module from `drivers/activemq` and shares no code with it
above `internal/activemq`, because there is nothing to share: STOMP is a text
framing protocol with one flat set of headers and AMQP 1.0 is a binary one with
three sections and typed values. The only thing the two arms have in common is
the **decision** about which iqmq field goes where, which is exactly what
`internal/activemq/protocol` holds.

`drivers/activemq` (AMQP 1.0) is the default. This arm is kept for the one row
it wins — a `selector` header really does filter on Classic, measured, and no
AMQP 1.0 link filter is reachable through `go-amqp` — and the DSN scheme is
`activemq+stomp` rather than `activemq` so that a connection string can never
be ambiguous about which capability table the caller gets.

## The finding to read first

**A STOMP `NACK` frame does not return a message on either broker**, and
nothing in either product's documentation says so. Measured against
`apache/activemq-artemis:2.44.0-alpine` and `apache/activemq-classic:6.2.0`:

- On **Artemis** a NACK is treated exactly as an ACK. One NACK and one ACK took
  `getMessagesAcknowledged` from 0 to 2, the queue emptied, the dead-letter
  queue did not grow, and a fresh subscription saw nothing.
- On **Classic** a NACK is a `POISON_ACK`: the message is not returned to the
  queue, it goes to the broker's dead-letter strategy. It cannot be transacted
  at all — "Poison ack cannot be transacted" arrives as an ERROR frame and
  closes the connection.

So a driver that mapped the specification's `Nack` onto the protocol's NACK frame would
silently **destroy** messages on Artemis while reporting a requeue. This one
does not. What it does instead is the protocol's own recovery mechanism, which
is measured to work on both brokers: an unacknowledged `client-individual`
delivery is redelivered when its **subscription** goes away, with
`redelivered: true` on it. So `Nack(NackRequeue())` unsubscribes and
re-subscribes, and the broker gives the message back.

The cost is stated rather than hidden, and it is why the claim is
`EmulatedDurable` rather than native: a subscription reset returns **every**
unacknowledged delivery of that subscription, not just the one that was nacked.
Every other delivery the caller was holding becomes stale and its settlement
reports `mq.ErrLeaseLost`. A caller that nacks often and holds a deep prefetch
wants the AMQP 1.0 arm.

`TestCriticalRequeueSendsNoNackFrame` asserts this positively: after a requeue
the frames the broker received are an UNSUBSCRIBE and a SUBSCRIBE, and **no
NACK frame**.

## Settlement

| verb | What reaches the broker |
|---|---|
| `Ack` | an ACK frame naming the delivery's own `ack` header (`ack: client-individual`) |
| `Nack(requeue)` | an UNSUBSCRIBE and a SUBSCRIBE. The broker redelivers, marked `redelivered` |
| `Nack(requeue, delay)` | **refused** (`ErrUnsupported`): neither broker reads a delay on a negative settlement, and a reset is answered at once |
| `Nack(drop)` | a NACK frame — the one verb that frame really serves, because neither broker returns the message |
| `Nack(deadLetter)` | a publication to `Config.DeadLetterTopic` and **then** an ACK, in that order. With no route: the NACK frame on Classic, whose poison-ack reaches the broker's own dead-letter strategy; **refused** on Artemis, where a NACK acknowledges |
| `Reject` | as `Nack(drop)` |
| lease | **none.** A delivery is this subscription's until the subscription goes away, so `DeliveryInfo.LeaseExpires` is the zero time, `FeatureExtendLease` is unsupported, and there is no `DeadlineExtender` anywhere in the module |
| at-least-once window | from the SEND frame until its RECEIPT. A settlement frame carries **no** receipt: go-stomp's `Conn.Ack` and `Conn.Nack` take no options, so there is no way to ask for one |

## What a STOMP delivery cannot say

- **There is no attempt count, so the specification's `Attempt` is always 0** — its own
  documented answer for "the broker does not count attempts". Measured on both
  brokers: no `delivery-count`, no `JMSXDeliveryCount`, no
  `_AMQ_DELIVERY_COUNT`. The boolean `redelivered` header is not a substitute
  either: measured on Artemis, the redelivery a requeue produces is *not*
  marked redelivered, and only one after a reconnection is — so a count derived
  from it read "attempt 1 after a first delivery at 1". A zero makes a
  `MaxAttempts` policy above this driver refuse to act rather than act on a
  wrong number. The AMQP 1.0 arm has a real `delivery-count`, and that
  asymmetry is one of the most important measured differences.
- **There is no binary header.** A frame header is a text name and a text
  value, so a header value or a message key that is not valid UTF-8 is refused
  before any I/O rather than mangled. A caller with binary headers wants
  `MetadataPortableEnvelope`.
- **There is no topology operation.** No frame creates a destination, deletes
  one, purges one or reports its depth, in any version of the specification. So
  `*Backend` deliberately does not satisfy `mq.AdminBackend` and
  `Client.Admin()` reports `ErrUnsupported` rather than returning a surface
  that refuses every call. `drivers/activemq`'s Admin, or the broker's own
  console, is what creates the topology.

## Testing needs no broker

    GOWORK=off go test ./...                                  # no container, no network
    GOWORK=off go test -race ./...
    GOWORK=off go test -run '^Fuzz' ./...                      # the committed seed corpus
    IQMQ_ARTEMIS_ADDR=localhost:61613 go test -run TestConformance ./...

Every test in this module runs against `internal/stompsrv`, a **scriptable
in-process STOMP 1.2 server** on a loopback listener. The seam is the
**socket** and not a client interface, which is the opposite of
`drivers/rabbitmq/internal/rabbitfake`'s choice and for the same reason:
amqp091-go interposes behaviour of its own, and go-stomp does not — it is a
frame codec plus one process loop. Every decision this driver has to get right
is a decision about **frames**, so the server records every frame it was sent
and a test asserts on the log.

Five of the seven critical scenarios cannot be provoked from a container at all:

| Scenario | What the server does |
|---|---|
| the ambiguous publication | answers a SEND frame's `receipt` header with **nothing** |
| Artemis's `AMQ339001` | answers a SUBSCRIBE with an ERROR frame and then closes the socket |
| a connection that simply went away | closes the socket with no DISCONNECT, at a chosen moment |
| a redelivery | hands over a delivery whose `redelivered` and `ack` headers the test chose |
| bad credentials | refuses the CONNECT |

The server has a fuzz target of its own (`FuzzSTOMPServer`), and it earns its
keep for the reason `drivers/amqp10`'s peer target states: **a defect in the
peer makes every critical test in the module above it vacuous, in the last
place anybody looks.** It scripts client frames — including the malformed ones
no encoder can produce: an empty command, a header with no colon, a
`content-length` that disagrees with the body, a NUL inside a header value, a
frame with no terminator — and holds the server to a dense ordered frame log, a
settlement accessor that agrees with the log frame for frame, a receipt
answered exactly once, an `ack` identifier never reused, and no goroutine left
behind.

The server has to answer three receipts a broker strictly need not, and each is
a 30-second hang if it does not: go-stomp tags an UNSUBSCRIBE with a `receipt`
header **itself**, on its own I/O goroutine, and then waits on a condition
variable; a DISCONNECT carries one this driver does not shorten; and the
CONNECTED frame's `version` header is what makes the library speak 1.2 at all —
without it `Conn.Nack` is refused outright and the headline test above would
pass for entirely the wrong reason.

## Tuning knobs and their measured effect (rule 10)

| Knob | Effect |
|---|---|
| `Config.Receipts` / `NoReceipts` | receipts are **on** by default, which is the opposite of a performance default on purpose: without one a STOMP publication is fire-and-forget and the specification's `ConfirmAccepted` has no boundary at all, so a sink would have to refuse the confirmation mode a caller gets by not choosing |
| `Config.Prefetch` | bounds the deliveries one source buffers for `Fetch`. On Classic it is also the broker's window (`activemq.prefetchSize`, a message count), which is the backpressure there: the broker stops sending until an `ACK`. A full buffer does not stop the source reading its socket, so heart-beats keep flowing while the caller is not fetching |
| `Config.ConsumerWindow` | Artemis's `consumer-window-size`, in **bytes**. Classic's `activemq.prefetchSize` is a **message count**, so the portable prefetch can be passed to one and not the other — which is why the count is enforced on this side as well |
| `Config.Selector` | a JMS message selector per subscription. Native on Classic, **refused** on Artemis, where it is ignored on a fully qualified queue name |
| `Config.Scheduler` | says the Classic broker's scheduler is on. With it off the broker delivers a message carrying `AMQ_SCHEDULED_DELAY` immediately and passes the header through to the consumer, so `FeatureScheduledPublish` is withdrawn until a deployment says otherwise |
| `Config.DeadLetterTopic` | a dead-letter route this driver owns, published to and then acknowledged. It is the only route on Artemis |
| metadata mode | `MetadataNativeStrict` is free and refuses what a text header cannot carry; the envelope modes cost an encode and carry a binary value and a repeated header key |

## Allocations

| Path | Allocations |
|---|---|
| `protocol.Carry` into a reused `Fields` | 0 |
| `protocol.Restore` into reused header storage | 0 |
| `Sink.frameFor` (the mapping plus the frame options) | 16 for a message with nine headers |
| `Source.subscribeOptions` | 4 |

The two that are not zero are the client library's rather than this driver's:
`gostomp.SendOpt.Header(k, v)` returns a **closure** over the key and the
value, and go-stomp offers no other way to set a header on a frame it is about
to build. Reaching zero would mean re-implementing `Conn.Send`'s
content-length handling, its receipt bookkeeping and its write channel — a fork
of the library's send path to save a closure. So `allocs_test.go` pins the
numbers as ceilings and holds the shared mapping's own half to zero, which is
the part a regression would actually appear in.
