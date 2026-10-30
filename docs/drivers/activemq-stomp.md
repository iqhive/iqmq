# activemq (STOMP 1.2, Artemis and Classic)

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 3 · **Driver:** `github.com/iqhive/iqmq/drivers/activemq/stomp`

**A STOMP `NACK` frame does not return a message on either broker.** That is
this driver's headline behaviour, nothing in either product's documentation says
so, and the obvious mapping — the specification's `Nack` onto the protocol's `NACK` frame —
**silently destroys messages while reporting a requeue**. Measured against the
pinned images:

- On **Apache ActiveMQ Artemis** a `NACK` is treated exactly as an `ACK`. One
  `NACK` and one `ACK` took `getMessagesAcknowledged` from **0 to 2**, the queue
  **emptied**, the dead-letter queue **did not grow**, and a fresh subscription
  **saw nothing**. A transacted `ACK` followed by `ABORT` is acknowledged
  **anyway** — while a transacted `SEND` that is aborted **is** correctly
  discarded, so go-stomp's transactions do reach the broker and it is Artemis's
  transacted *acknowledgement* that does not roll back.
- On **ActiveMQ Classic** a `NACK` is a `POISON_ACK`: the message is not
  returned to the queue, it goes to the broker's dead-letter strategy. It cannot
  be transacted at all — *"Poison ack cannot be transacted"*, which arrives as an
  `ERROR` frame and **closes the connection**.

So this driver's requeue is a **subscription reset**: unsubscribe, re-subscribe,
and the broker redelivers every unacknowledged delivery marked `redelivered` —
the protocol's own recovery mechanism, measured working on both brokers. **Its
cost is stated rather than hidden: nacking one delivery returns them ALL, and
every other delivery the caller was holding then reports `ErrLeaseLost` when it
is settled.** A caller who nacks often with a deep prefetch wants
[the AMQP 1.0 driver](activemq.md).

This page is the Apache ActiveMQ driver over **STOMP 1.2**, for both products:
Apache ActiveMQ **Artemis** and Apache ActiveMQ **Classic**. The same two
brokers over AMQP 1.0 are a separate module with a page of its own,
[activemq.md](activemq.md) — that one is built on
[the shared AMQP 1.0 core](amqp10.md) — and the pair is variant family ** **
("ActiveMQ protocol"). The specification makes the protocol choice **explicit**: the
AMQP 1.0 arm is the default and this one is kept for ActiveMQ Classic, where a
STOMP selector is the only server-side filter either protocol offers.

The two modules share no code above `internal/activemq` and there is nothing to
share: STOMP is a text framing protocol with one flat set of headers and AMQP
1.0 is a binary one with three sections and typed values. What they *do* share is
the **decision** about which iqmq field goes where, which is exactly what
`internal/activemq/protocol` holds — and why the specification is a family rather than
two unrelated drivers.

## Client & version policy

| Client | Pinned version | Confirmation boundary |
|---|---|---|
| `github.com/go-stomp/stomp/v3` | **v3.2.0** | the `RECEIPT` frame answering a `SEND` frame's `receipt` header |

There is no native baseline for this client: `benchmarks/native-baselines/`
measures `github.com/Azure/go-amqp` against the harness's `artemis` image, and a
number taken from it is not comparable with this arm's, because the
confirmation boundary is reached by a different mechanism. The specification's own
benchmark measures what *is* honestly comparable without a broker — the cost of
the property mapping each protocol's message model demands, which is on the hot
path of every publication and every delivery.

The images the evidence is taken against are
**`apache/activemq-artemis:2.44.0-alpine`** and
**`apache/activemq-classic:6.2.0`**.

**The version floors are per product**, and `Open` refuses a broker older than
the floor with `mq.ErrUnsupported` naming both versions:

| | Artemis | Classic |
|---|---|---|
| floor this driver enforces | **2.7** | **5.6** |
| why | a shared durable subscription is addressed by fully qualified queue name (`address::queue`), and 2.7.0 is the release whose acceptors — the STOMP one included — accept one | 5.6.0 added **STOMP 1.2**, and with it the `client-individual` acknowledgement mode and the `ack` header's message identifier that asks this driver to use |
| what the broker calls itself | the `CONNECTED` frame's `server` header: `ActiveMQ-Artemis/2.44.0` | the same header: `ActiveMQ/6.2.0` |

Unlike over AMQP 1.0, **both** products identify themselves over STOMP, so the
product check has something to read on either. The order of the two tests is
load-bearing: Classic's `server` header is a **prefix** of nothing and Artemis's
is `ActiveMQ-Artemis/…`, so a Classic test written as a prefix match on
`ActiveMQ` would match Artemis as well.

The driver connects with `accept-version: 1.2` explicitly. That matters more than
it looks: a `1.2` `ACK` frame names the delivery by its own `id` header while a
`1.0` one names it by `subscription` plus `message-id`, and in 1.0 the library's
`Nack` is refused outright.

### Verified facts (the API contract)

Every row below was established against one of the two pinned images, not read
from documentation. Re-verify when a pin moves.

- **`NACK` returns nothing.** The numbers are at the top of this page: on
  Artemis a `NACK` is an acknowledgement, and on Classic it is a `POISON_ACK`
  that dead-letters and cannot be transacted.
  _Verified against: both pinned images;
  `internal/activemq/protocol/stomp.go`'s `FeatureNack` row._
- **a subscription reset does return everything.** An unacknowledged
  `client-individual` delivery is redelivered when its **subscription** goes
  away, with `redelivered: true` on it. Measured on both brokers, and it is
  what this driver's requeue is.
  _Verified against: both pinned images; `drivers/activemq/stomp/source.go`'s
  `reset`._
- **there is no attempt count, and the boolean is not reliable either.** A
  STOMP delivery carries no `delivery-count`, no `JMSXDeliveryCount` and no
  `_AMQ_DELIVERY_COUNT` on **either** broker. The whole of what it says about
  its history is the boolean `redelivered` header — and measured on Artemis,
  the redelivery a **requeue** produces arrives with `redelivered: false`;
  only one after a reconnection arrives with `true`.
  _Verified against: both pinned images; `protocol.Attempt`;
  `drivers/activemq/stomp/source.go`'s `deliveryFor`._
- **a selector filters on Classic and is ignored on Artemis.** A
  `colour = 'red'` selector on the `SUBSCRIBE` frame kept a blue message out of
  the subscription on Classic; on Artemis, with the subscription named by a fully
  qualified queue name, the blue message **was delivered**.
  _Verified against: both pinned images;
  `internal/activemq/protocol/stomp.go`'s `FeatureServerFilter` row._
- **`AMQ_SCHEDULED_DELAY` works on both.** In whole milliseconds: measured at
  **2.5 s** on Artemis, which echoes it on delivery as `_AMQ_SCHED_DELIVERY` (an
  absolute epoch time), and at **2.6 s** on Classic, which echoes it as
  `scheduledJobId`. Classic's scheduler is **off** unless the broker is
  configured with `schedulerSupport="true"`.
  _Verified against: both pinned images._
- **a `receipt` on a `SEND` frame works on both; a `receipt` on a `SUBSCRIBE`
  frame is a defect in go-stomp v3.1.5.** The library registers the
  subscription's own frame channel under the receipt identifier, the `RECEIPT`
  frame closes that channel, and the library then **panics** with "send on
  closed channel" from inside its own `processLoop`. Observed against Artemis.
  _Verified against: go-stomp v3.1.5 and
  `apache/activemq-artemis:2.44.0-alpine`; `source.go`'s `subscribe`
  ._
- **`JMSXGroupID` is accepted by both brokers, and only one echoes it.** Artemis
  echoes the group on delivery as `_AMQ_GROUP_ID`; Classic echoes nothing at
  all, so a consumer there cannot confirm which group a delivery belonged to.
  _Verified against: both pinned images; `protocol.EchoedGroup`, which reads
  both spellings and promises neither._
- **an `ERROR` frame is fatal to the connection.** Artemis **closes the socket**
  after the `ERROR` frame it refuses a `SEND` with. Classic keeps it open after
  refusing a `SEND` it could read, and closes it after its codec's refusal, but
  go-stomp closes the connection itself on any `ERROR` frame, so on both brokers
  one refused frame costs every other publisher and consumer on that connection.
  _Verified against: both pinned images, raw frames; go-stomp v3.2.0's
  `processLoop`; `drivers/activemq/stomp/errors.go`._
- **an `ERROR` frame names the refused `SEND` by its receipt, if it has one.**
  Both brokers copy a refused `SEND`'s `receipt` into the `ERROR` frame's
  `receipt-id`: Classic for an authorisation refusal and a converter one
  (`Invalid transaction id`), Artemis for an authorisation refusal (`AMQ229032`)
  and a malformed `SEND` (`AMQ339031`). The refusal of a `SEND` that asked for no
  receipt carries none. The one receipted frame refused without a `receipt-id` is
  a frame Classic's codec cannot read, such as `The maximum frame size was
  exceeded` for a frame over the connector's `wireFormat.maxFrameSize`. The codec
  has read the frame's headers by then, the `receipt` among them, but the error
  it hands the converter keeps none of them, so there is no receipt to copy.
  Both brokers also send a receipt-less `ERROR` about the connection itself
  (Artemis's connection TTL, Classic's broker stopping), and Classic refuses a
  receipt-less `SEND` from an asynchronous dispatch that a later `RECEIPT` can
  overtake.
  _Verified against: both pinned images, raw frames for every case but the
  connection-level ones, which are from the sources: activemq-stomp 6.2.0's
  `ProtocolConverter.handleException` and `StompWireFormat`, activemq-broker
  6.2.0's `TransportConnection.service` and `serviceException`, and
  artemis-stomp-protocol 2.44.0's `VersionedStompFrameHandler.handleFrame` and
  `StompConnection.fail`; `drivers/activemq/stomp/sink.go`'s `answers`._
- **a refused login is a sentence with no code, on both brokers.** Classic
  answers a `CONNECT` whose credentials it refuses with
  `User name [iqmq] or password is invalid.` and Artemis with
  `Security Error occurred: User name [artemis] or password is invalid`. Both
  say `[null]` for a `CONNECT` with no login, and both then close the socket. Artemis's `AMQ229031` goes to its own
  log and never reaches a STOMP client. The driver matches the sentence, so
  `Open` reports `CodeUnauthorized` from the first attempt; before it did, both
  refusals were `CodeUnknown` and `Open` retried them until its context ended.
  _Verified against: both pinned images, raw frames and `OpenBackend` with a
  wrong passcode; artemis-stomp-protocol 2.44.0's
  `getFailedAuthenticationResponse`, activemq-broker 6.2.0's
  `SimpleAuthenticationBroker` and `JaasAuthenticationBroker`;
  `drivers/activemq/stomp/errors.go`._
- **Artemis refuses a `SUBSCRIBE` to a destination that does not exist**, with
  auto-creation disabled, as `AMQ339001 Destination does not exist <name>` in an
  `ERROR` frame — and then closes the connection.
  _Verified against: `apache/activemq-artemis:2.44.0-alpine` with
  auto-creation off._
- **a Classic delivery carries eleven protocol or broker headers of its own**,
  which the mapping reads through its own named fields and filters out of the
  caller's headers: carrying them through would put `subscription: 1` and
  `ack: 1082,1088` in a message.
  _Verified against: `apache/activemq-classic:6.2.0`;
  `protocol.ReservedFrameHeader`._

## Topic / subscription mapping

`Config.Broker` is **required and has no default**, for the reason
[the AMQP 1.0 page](activemq.md) gives and for one more of this protocol's own:
on Classic a destination's own **shape** is the only thing that says whether a
name is a queue or a topic, so a wrong guess publishes into a queue every
consumer is subscribed to and none of them receives anything from — with no
error anywhere.

| | Artemis | Classic |
|---|---|---|
| topic → `SEND` to | `<prefix><topic>` — the address name, **bare** | `/topic/VirtualTopic.<prefix><topic>` |
| subscription → `SUBSCRIBE` to | `<address>::<address>.<subscription>` — a fully qualified queue name | `/queue/Consumer.<subscription>.VirtualTopic.<prefix><topic>` |
| extra `SUBSCRIBE` headers | `subscription-type: MULTICAST`, `consumer-window-size: <bytes>` | `activemq.prefetchSize: <count>` |
| who creates the destination | `drivers/activemq`'s `Admin` over AMQP 1.0, or the broker's console | the `SUBSCRIBE` itself |

**On Artemis a STOMP destination is the address name itself.** A `SEND` names a
bare address and a `SUBSCRIBE` names an `address::queue` fully qualified queue
name; there is no `/topic/` or `/queue/` decoration, and a name set derived for
AMQP 1.0 — which carries `topic://` or `queue://` — is **refused** outright
rather than sent. On Classic the decoration is the `/topic/` and `/queue/`
**path** form, and getting it wrong there does not fail: it publishes to a
**queue** called `VirtualTopic.orders`, which every consumer queue is subscribed
to and none of them receives anything from. Refusing the other protocol's form is
the only place that defect can be caught before it becomes a topology.

**`subscription-type: MULTICAST` is required on Artemis and was measured.**
Without it a `SUBSCRIBE` to a fully qualified queue name is read as an **ANYCAST**
consumer, which is a different routing type on the same address.

**`Config.Prefix` goes INSIDE the `VirtualTopic.` prefix on Classic**
(`VirtualTopic.<prefix><topic>`), and the ordering was settled by the broker
rather than by reasoning: Classic's default virtual-destination interceptor
matches `VirtualTopic.>`, so a destination called `iqmq.VirtualTopic.orders` is
**not a virtual topic at all** — every publication to it is accepted and no
consumer queue receives anything, silently.
[activemq.md](activemq.md) carries the three-line experiment that settled it.

The name rules are `internal/activemq`'s and are shared with the other arm: 255
bytes, no `::`, no `/`, no `,`, no `:`, no whitespace, no control characters, no
wildcard (`#`, `*`, `>`) in a topic or a subscription, and no `.` in a
subscription. Two of them matter more over this protocol than over the other: a
**`:`** separates a STOMP header's name from its value, and a **control
character** terminates a frame or a header — so a name carrying either is a
malformed frame the broker answers with an `ERROR` that closes the connection,
taking every other publication in flight with it.

### Reading a JMS queue by name

The mapping above names its own queues, so it cannot reach a queue that some
other application created and named. Set `Config.Addressing` to
`stomp.AddressQueue` (or `addressing=queue` in the DSN) and the topic becomes
the queue's name, for publishing and for consuming:

| | Artemis | Classic |
|---|---|---|
| `SEND` and `SUBSCRIBE` destination | `<prefix><topic>` | `/queue/<prefix><topic>` |
| routing headers | `destination-type: ANYCAST` on a `SEND` and `subscription-type: ANYCAST` on a `SUBSCRIBE`, so that an address carrying both routing types resolves to its ANYCAST queue | none: the `/queue/` path says it |

`ConsumerConfig` still requires a subscription, and the driver ignores it.
Every consumer of the queue competes for its messages, so `FeatureFanOut` is
withdrawn from the capability table. `Prefix` goes in front of the queue's
name, so leave it empty to read a queue that another application named. Over
this protocol nothing creates the queue on Artemis: `drivers/activemq`'s
`Admin` in the same mode creates it as an ANYCAST address and queue.
`Config.Selector` is still refused on Artemis, because nobody has measured
whether Artemis honours a selector on a bare ANYCAST subscription.

## Publish path & confirmation boundary

**A STOMP `SEND` frame has no answer of its own.** What the protocol offers
instead is the `receipt` header: a client that sets one is answered with a
`RECEIPT` frame once the broker has processed the frame, and a client that does
not is answered with nothing at all. So the boundaries are exactly those two
cases:

| Mode | Boundary |
|---|---|
| `ConfirmNone` | no receipt: the frame is written and the call returns. Local admission, which the specification says may be lossy |
| `ConfirmAccepted` | a `RECEIPT` frame: the broker has the message |
| `ConfirmDurable` | a `RECEIPT` frame for a `persistent: true` publication, which both brokers write to their store before they answer. **Conditional**: the durable half is the destination's own configuration, which the driver cannot read over STOMP |

**`Config.Receipts` is ON by default, and that is the opposite of a performance
default on purpose.** Without receipts a STOMP publication is fire-and-forget and
the specification's `ConfirmAccepted` — which is what a caller gets by *not* choosing — cannot
be honoured at all. A client configured `NoReceipts` therefore has its sink
**refused at open** for any mode but `ConfirmNone`, rather than publishing and
reporting a success the wire never promised, and its capability table says the
same thing up front: `FeaturePublisherConfirm` and `FeatureDurableConfirm` are
withdrawn along with `FeatureReceipts`, because both boundaries are the receipt. Both refusals happen at `OpenSink`
and not at the first publication, because validates before it starts work and
because a STOMP broker answers a frame it refuses with an `ERROR` frame and then
closes the connection.

**A receipt that never arrives is AMBIGUOUS and nothing else.** The frame left
the socket, so the broker may have the message, and a publisher told "failed"
republishes it — after which the broker holds two and nothing anywhere reports a
duplicate. The error table carries that (`CodeSettlementUnknown`, retryable,
ambiguous) rather than the call site, because go-stomp's errors are neither a
`net.Error` nor an EOF and the generic network classifier cannot see them.
`Backend.ambiguous` counts them, so a deployment can read the window as a
number.

**The caller's context ends the wait.** go-stomp's `Conn.Send` takes no
context, so a publication that waits for its receipt runs on a goroutine of its
own and returns when the caller's context ends: `CodeDeadlineExceeded` or
`CodeCanceled`, and **ambiguous**, because the frame may have left. The send
finishes in the background and does the same bookkeeping, so a receipt that
never comes is still counted in `Backend.ambiguous`, and a body the caller owns
is copied for it, so the caller may reuse its message once `Publish` returns. A
publication used to wait out the queue and then the receipt, each bounded by
`Config.ReceiptTimeout`, whatever its deadline, and report
`CodeSettlementUnknown`. The dead-letter copy publishes through the same sink
and ends with the settlement's context the same way.

**An `ERROR` frame is a refusal only of the publication it names.** go-stomp
hands an `ERROR` frame to every receipt it is waiting on, whatever frame the
broker refused, so the driver reads the frame's `receipt-id`. When the id is the
publication's own receipt the error is a definite refusal with the broker's
sentence. Any other `ERROR` frame, carrying another frame's id or none, ends the
publication as `CodeUnavailable`, retryable and **ambiguous**: the connection is
gone and nothing answered the publication. The broker's sentence stays reachable
with `errors.As` as an `*ErrorFrameError`. Reading it the old way was wrong in
both directions, measured on both pinned images: a refused `ConfirmNone`
publication followed by a `ConfirmAccepted` one reported the second as a
definite failure naming the first one's destination, while Classic had stored
the message and Artemis had never read it. The one exception is Classic's codec
refusal, which carries no `receipt-id` for any frame. It is the publication's
when every other frame on a connection the driver dialled has been answered, and
ambiguous while a receipt-less `SEND`, a timed-out receipt or a concurrent
publication is outstanding, or on a connection passed to `OpenWithClient`.
A refusal the publication can claim takes its code from the sentence. An
authorisation refusal is `CodeForbidden` on both brokers (Artemis's `AMQ229032`,
Classic's `User reader is not authorized to write to: queue://denied`), and
Classic's codec refusing a frame over one of its size limits, such as
`The maximum frame size was exceeded`, is `CodeMessageTooLarge`.

**The `RECEIPT` frame carries the receipt id and nothing else** — no broker
message identifier, no offset, no timestamp. So the receipt this driver fills is
the *fact* of acceptance and the time it was reached, and `MessageID` is left
**empty** rather than filled with the identifier the publisher itself sent:
reporting a caller's own value back as the broker's would be inventing evidence.
The identifier is the driver's own (`iqmq-<n>`) rather than the library's, so it
is visible to a test and to a log line — a receipt nobody can name is a round
trip nobody can debug.

**A batch is a loop.** One `SEND` frame carries one message and the library has
no batched write, so `PublishBatch` is a loop over `Publish`:
`FeatureBatchPublish` is **EmulatedVolatile**, and a failure part-way through
leaves what it had already sent published. The error is an index-aligned
`*mq.BatchError` (so `errors.Is(err, mq.ErrPartial)` holds): `nil` for each
message that got through, the failure itself for the message that met it, and a
**definite** not-sent error for every message after it, because a caller told
nothing happened would republish the ones that did get through, and a caller
told the unsent ones "may be in the broker" would never send them.
`PartialBatchError` is no longer returned.

## Consume path

**STOMP pushes, so the broker's window is the backpressure.** A receiving link in
AMQP 1.0 has credit and this does not: a `SUBSCRIBE` frame tells the broker to
start sending and the broker sends. What stops it is the broker's own
flow-control window, which counts what the subscription has not acknowledged.
Classic stops at `activemq.prefetchSize` messages (the prefetch) and Artemis at
`consumer-window-size` bytes of `MESSAGE` frames (`Config.ConsumerWindow`), and an
`ACK` reopens the window by one delivery. A fetch reads from a bounded buffer
whose count and byte budget come from the prefetch.

**A full buffer does not stop the connection.** go-stomp runs each connection on
one loop, and that loop both hands a `MESSAGE` to the subscription and writes the
client's heart-beats. The driver reads the subscription with a goroutine that
never waits on your code and queues what it reads behind the buffer, so the loop
keeps writing heart-beats, receipts and acknowledgements however long you leave
the buffer unfetched. That queue is bounded by the broker's window, in the
broker's own unit: a source holds at most what the broker sends it before an
acknowledgement, the prefetch in messages on Classic and `Config.ConsumerWindow`
in frame bytes on Artemis. So `Config.PrefetchBytes` bounds the buffer a fetch
reads and not what a source holds, and on Classic a byte bound on a source comes
from the prefetch and the size of your largest message. The queue stops reading
only for a peer that sends past the window the driver asked for, which neither
pinned broker does.

Measured against both pinned images, with a consumer that held its deliveries for
eight seconds against a one-second `HeartbeatSend`: when the buffer's wait
stopped the loop, the broker dropped the connection and redelivered what was
unacknowledged. With the default configuration on Artemis that was 64 duplicates
in 120 messages, and acknowledgements written during the pause were lost with the
socket. Classic's window is the prefetch, which the buffer holds by count, so it
stalled only once a byte budget left the buffer short of the prefetch by more
than go-stomp's 16-deep channel holds (prefetch 64 with a one-byte
`PrefetchBytes`: 28 duplicates). Reading the subscription on its own goroutine,
the same runs deliver every message once, on one connection, including a full
1 MiB Artemis window of about 4,000 frames.

**`OpenSource` sends the `SUBSCRIBE` itself, and that is the opposite of what
`drivers/amqp10` does with a link.** On ActiveMQ Classic the `SUBSCRIBE` is the
only topology operation this protocol has — a consumer queue comes into existence
when a subscription attaches to it and nothing else creates one over STOMP — so a
lazy subscribe makes `OpenSource` create nothing, and a deployment that opened its
consumers and then published loses everything published before the first fetch.
The lazy version was written, made the priority and TTL probes pass, and was
reverted anyway, because it buys those two claims by making a Classic queue's
existence depend on somebody calling `Fetch`. What eagerness
costs is measured and is in the capability table: from the moment the
subscription exists the broker sends and this driver buffers, so a destination's
priority ordering and the expiry of a message whose TTL has passed — both decided
by the broker **at dispatch** — stop being observable through it.

Only the RE-subscribe is lazy: a subscription whose connection died is
re-established by the next `Fetch`, which is the [Reconnect](#reconnect) section's
"the correctness half is eager and the re-subscribe is lazy".
`TestTheOpenSendsOneSubscribeAndTheFirstFetchSendsNoMore` pins both halves — one
`SUBSCRIBE` taken by the open, and none added by the fetch that reads it.

**The prefetch asymmetry is real and it is why the count is enforced here.**
`SourceConfig.Prefetch` is a message **COUNT**; Artemis's
`consumer-window-size` is **BYTES** and Classic's `activemq.prefetchSize` is a
**count**. A driver can honour one or the other and not both, so the count is
enforced in this driver's own buffer, where it means what the specification says it means, and
**each broker is told what it understands** :

| | Artemis | Classic |
|---|---|---|
| the broker's own window | `consumer-window-size`, in bytes — `Config.ConsumerWindow`, default 1 MiB, the default of Artemis's core protocol. Its STOMP acceptor would choose 10 KiB (`stompConsumerWindowSize`) for a `SUBSCRIBE` without the header, and this driver always sends it | `activemq.prefetchSize`, in messages — the resolved prefetch |
| the count a caller asked for | enforced by the driver's bounded buffer | the same buffer, and the broker agrees |
| the byte ceiling on the buffer | `Config.PrefetchBytes`; zero leaves the buffer bounded by the count alone | the same |
| the most a source holds | `Config.ConsumerWindow` bytes of frames, plus the frame that crossed it | the prefetch, in messages |

`ack: client-individual` is the acknowledgement mode, which is the specification's own word
for this arm, and an `ACK` frame names one delivery by the `ack` header the
broker sent with it.

**There is no lease.** A delivery is this subscription's until the subscription
goes away, so `DeliveryInfo.LeaseExpires` is the zero time,
`FeatureExtendLease` is unsupported and there is no extender.

**There is no attempt count, and this is the limit a `MaxAttempts` policy runs
into.** Measured on both brokers, a STOMP delivery carries no `delivery-count`,
no `JMSXDeliveryCount` and no `_AMQ_DELIVERY_COUNT`; the whole of what it says
about its history is the boolean `redelivered` header — and that boolean is not
reliable: measured on Artemis, the redelivery a requeue produces arrives with
`redelivered: false`. So the specification's `Attempt` is **zero**, which is its documented
value for "the broker does not count attempts", `DeliveryInfo.Redelivered` is
the honest field, and **a `MaxAttempts` policy above this driver has nothing to
count with**. Reporting 1 and 2 from the boolean was this driver's first answer
and it was worse than reporting nothing: the specification's `nack/immediate-redelivery`
read "the redelivery reports attempt 1 after a first delivery at 1", and a
caller cannot tell that 1 from a first delivery's. The AMQP 1.0 arm has a real
`delivery-count`, and that asymmetry is one of the most important
measured differences.

**A delivery this driver cannot decode is dead-lettered where the
configuration has a route, and is never acknowledged in place.** Acknowledging
it would destroy it, and leaving it unacknowledged held one message of the
broker's flow-control window per delivery until the consumer stopped. With
`DeadLetterTopic` set, the driver publishes a copy there and acknowledges the
original once the copy's receipt arrives. On Classic with no topic it sends a
`NACK`, which Classic poison-acks onto its own dead-letter strategy
(`ActiveMQ.DLQ` by default). That `ACK` or `NACK` passes the same staleness
guard as a caller's settlement, so a requeue that resets the subscription
during the copy's round trip has it refused and counted in
`Source.StaleSettlements()`: the reset brings the delivery back and it is
dead-lettered a second time. It used to go out after the requeue's
`UNSUBSCRIBE`, naming a delivery the broker had taken back. Artemis with no
topic has no frame that gives a delivery back without destroying it, so there
the delivery stays unacknowledged until the subscription goes away; set
`DeadLetterTopic` on Artemis if your producers can write bodies this consumer
cannot read. `Source.Undecodable()` counts them all.

**On Classic the consumer queue must exist BEFORE the first publication.** A
virtual topic copies a publication into the consumer queues that exist *at that
moment*, so a queue created afterwards gets nothing. `FeatureFanOut` is
therefore conditional on Classic and native on Artemis.

## Ack / nack / extend mapping

The specification has four verbs and STOMP has two frames, and **the obvious pairing is a
silent data defect on both brokers**. The measurement is at the top of this
page; this is what the driver does instead:

| action | What goes on the wire | Notes |
|---|---|---|
| `Ack` | an `ACK` frame, `ack: client-individual` | final when the frame is **written**: go-stomp's `Conn.Ack` takes no options, so a settlement frame cannot carry a `receipt` header at all, whatever `Config.Receipts` says. That is why an acknowledgement whose connection died is reported as a **lost lease** and not as an unknown outcome — the broker still holds the delivery |
| `Nack(NackRequeue())` | **`UNSUBSCRIBE` + `SUBSCRIBE`** — a subscription reset | the broker redelivers every unacknowledged delivery of that subscription, marked `redelivered`. **No `NACK` frame and no `ACK` frame is written at all** |
| `Nack(NackRequeue(), NackDelay(d))` | — | **`ErrUnsupported`, and settles nothing.** No header either broker reads on a negative settlement, and a reset is answered at once |
| `Nack(NackDrop())` | a `NACK` frame | whose *meaning* differs by broker: on Artemis it acknowledges, on Classic it poison-acks. That difference is what the capability note states |
| `Nack(NackDeadLetter())` | a publication to `Config.DeadLetterTopic`, **then** an `ACK` — or, on Classic with no topic set, the `NACK` frame, whose poison-ack reaches the broker's own dead-letter strategy | on **Artemis** with no `DeadLetterTopic` it is **refused**: measured, a `NACK` there acknowledges the delivery rather than dead-lettering it, so there is no frame that reaches a dead-letter address |
| `Reject` | as `Nack(NackDrop())` | |
| `Extend` | — | STOMP grants no lease, so there is no deadline to extend |

Both refusals happen **before** the token table is touched, so a delivery this
driver will not settle **stays settleable**: a caller told "unsupported" has to
be able to do something else with it.

### What a requeue costs

`FeatureNack` is **`EmulatedDurable` at subscription scope**, and every word of
that is load-bearing. It is *emulated* because the protocol's own negative
settlement returns nothing. It is *durable* because nothing is held in this
process — the broker gives the message back from its own storage. And the scope
is the **subscription and not the delivery**:

- nacking **one** delivery returns **all** of the subscription's unacknowledged
  deliveries;
- every token issued before the reset is invalidated, so every other delivery
  the caller was holding reports **`ErrLeaseLost`** when it is settled, which is
  the honest answer for a delivery that is no longer this consumer;
- the invalidation happens **before** the `UNSUBSCRIBE`, because from the moment
  the broker is told to take the subscription back every delivery it held is its
  own again and a settlement in flight must be refused rather than written to a
  subscription that is going;
- requeues of several deliveries of one subscription at once reset it **once**:
  the first resets, and every other one, whose delivery that reset has already
  returned, reports **`ErrLeaseLost`**. Each used to run a reset of its own and
  report success, and every reset after the first retired the subscription the
  one before had just made, so a delivery handed out in between was processed
  twice;
- `Source.Resets()` counts the resets and `Source.StaleSettlements()` counts the
  refusals, so a deployment can see what its nack rate is buying.

### The staleness guard

A settlement is refused when the delivery's **subscription** is no longer the
source, which is the rule in this protocol's terms: a STOMP 1.2 `ACK`
names a delivery by the `ack` header the broker sent, and that identifier belongs
to the subscription it came from. A reset, a reconnection or a close makes every
one of them somebody else's — so the token table's subscription generation and
connection generation are what decide, and the answer is `mq.ErrLeaseLost`. On a
broker that reuses identifiers, sending one anyway would settle whoever now holds
it.

The check is made twice: when the delivery's slot is claimed, and again as the
frame is written, under a lock a reset and a close take before they move the
subscription on. A settlement checked before a reset has its frame queued ahead
of the `UNSUBSCRIBE`, and one checked after is refused. It used to be made once,
at the claim, with only the connection compared at the write, which a reset
keeps. A requeue of another delivery in between, most often during the
dead-letter route's round trip for its copy, let the `ACK` go out after the
reset and report success while the broker redelivered the message. On the
dead-letter route that refusal comes after the copy was published, so the
redelivered message can be dead-lettered a second time.

A settlement that **fails** lands on one of three sides, and the difference
matters: a receipt that never came is an **unknown** outcome and the specification makes it
permanent (the slot is kept, so the delivery can never be settled again by
accident); a connection that died is a **lost lease** (the broker still holds the
delivery and will hand it to the next subscription, so it is not permanent); and
a definite refusal leaves the delivery **settleable** so the caller can try
again.

## Ordering

The key travels as the **`JMSXGroupID`** frame header, which both brokers accept
as the JMS message group and dispatch exclusively to one consumer of a
subscription. `FeatureOrderedByKey` is claimed at key scope but as
**Conditional**, not Native, and the specification requires the condition to be stated — here
it is the **evidence and the feedback** rather than the mechanism:

- the exclusive assignment of a group to one consumer was measured over **AMQP
  1.0** on Artemis (two competing consumers on one queue, every `k0` message to
  the first and every `k1` to the second), not over STOMP;
- Artemis echoes the group back on delivery as `_AMQ_GROUP_ID` and **Classic
  echoes nothing**, so a consumer there cannot confirm the group it received;
- a key that is not valid UTF-8 cannot be carried as a frame header at all, so
  it cannot be a group either.

An enveloped publication carries the header too, under `MetadataPortableEnvelope`
and whenever `MetadataAdaptive` envelopes. The broker dispatches a group by the
header and cannot read the envelope, so the key goes in the envelope for the
consumer and in `JMSXGroupID` for the broker. A key that is not valid UTF-8
travels in the envelope alone and has no group.

Above that: one queue is FIFO, so `FeatureOrderedByPartition` is claimed at group
scope, and **`FeatureOrderedGlobally` is not claimed** — nothing above one queue
is ordered. A requeue is a subscription reset, so a nacked message comes back
with the rest of the subscription's unacknowledged deliveries and the core makes
no promise about the order they return in.

## Delays

**A scheduled publication works on both brokers, through one header.**

That is simpler than the AMQP 1.0 arm, where the same feature lives in a
different **section** on each product and each ignores the other's spelling. It
is also one thing weaker: **STOMP has no absolute form**, so a caller's
`PublishOptions.DeliverAt` becomes a **duration the driver resolved** against the
injected clock, and a `DeliverAt` already in the past is published **at once**:
the sink clears the instant as well as the duration, because the shared mapping
reads "`DeliverAt` set, delay zero" as "the driver did not resolve it" and would
refuse the publication. No delay header is written at all, which matters —
`AMQ_SCHEDULED_DELAY: 0` is accepted by both brokers and turns an ordinary
publication into a scheduled one.

**Classic's scheduler is off by default and its absence is silent.** With
`schedulerSupport` unset the broker accepts the header, delivers the message at
once, and passes the header through to the consumer. So `Config.Scheduler` is how
a deployment says the scheduler is on, and `FeatureScheduledPublish` is
**withdrawn** on Classic without it, with `ErrUnsupported` before any I/O rather
than a delay reported as successful and never applied. Artemis's scheduler is
always available and the field is ignored there.

`Limits.MaxDelay` is left **unknown**: neither broker documents a ceiling on a
scheduled delivery and neither refused one in testing, and the rule is that an
unknown limit is never an overloaded zero.

**A delayed redelivery does not exist**, for a simpler reason than the AMQP 1.0
arm's: there is no negative settlement over STOMP at all, so there is nothing to
attach a delay to, and both brokers' redelivery delay is a broker-side policy
rather than a per-nack value. `Nack(NackRequeue(), NackDelay(d))` is refused
before any I/O and settles nothing.

**A per-message TTL needs a clock, and that is this protocol's doing.** STOMP's
`expires` header is an **absolute** epoch-millisecond instant and the protocol
has no relative form, while TTL is a duration — so the arithmetic is the
driver's, against `Config.Clock`. The shared mapping is pure and **refuses** a
publication whose expiry was not resolved for it, rather than sending one
without its expiry.

## Dead-letter queue

There are two routes and which one a caller has depends on the **broker**, which
is unusual enough to be worth a table:

| `Config` | Artemis | Classic |
|---|---|---|
| `DeadLetterTopic: "orders.dlq"` | **EmulatedDurable**: the driver publishes the message there and acknowledges the original only once that publication's **receipt** has arrived | the same |
| neither set | **Unsupported**, and `Nack(NackDeadLetter())` is **refused**: measured, a `NACK` frame on Artemis acknowledges the delivery rather than dead-lettering it, so there is no frame that reaches a dead-letter address | **Conditional**: a `NACK` is a `POISON_ACK` and reaches the broker's own dead-letter strategy (`ActiveMQ.DLQ` by default), which the driver cannot read over STOMP |

The dead-letter publication is opened with `ConfirmDurable`, not with the
caller's mode: the specification says the original may not be acknowledged until the
dead-letter publication is durable, so the boundary is the strongest one this
protocol has. That needs receipts, so `Config.Validate` refuses a
`DeadLetterTopic` together with `NoReceipts`; before it did, every
`Nack(NackDeadLetter())` on such a client failed behind a capability table that
advertised the route.

The copy is the message the caller received, decoded: its body, key, ID,
headers and properties, published through the dead-letter sink, which encodes
it once in the configured `Metadata` mode. A consumer of the dead-letter topic
reads the same message the original consumer did, plus two headers:
`iqmq-dead-letter-reason` (the specification's `NackOptions.Reason`) and
`iqmq-dead-letter-attempt`. They replace any copies of themselves the message
already carried. They are ordinary application headers rather than names under
the reserved `x-iqmq-` prefix, because the shared mapping refuses a caller's
header under that prefix and cannot tell the driver's from a caller's.

A delivery the driver could not decode has no decoded message, so its copy is
the raw frame body with the same two headers. Under
`MetadataPortableEnvelope` the sink wraps those bytes in an envelope of their
own, so a consumer of the dead-letter topic can read the bytes that failed.
That copy is published by the source's pump, which no caller's context bounds,
so it ends when the source closes: `Close` waits for the pump, and a broker that
never answered the copy used to hold `Close` for the whole receipt timeout. A
copy cut short leaves the delivery unacknowledged, and the broker redelivers it
once the subscription goes.

Under `stomp.AddressQueue` on Artemis the dead-letter `SEND` carries
`destination-type: ANYCAST` like any other publication, so the copy reaches the
dead-letter queue on an address that carries both routing types.

The sink is opened **lazily**, on first use: a source that never rejects anything
should not pay for a handle it does not use, and opening it at the source's own
open would make a misconfigured dead-letter topic a failure of the *consumer*,
which is not where a caller would look for it.

## Headers & limits

**Everything is a frame header, and every frame header is TEXT.** That one
sentence is most of this arm's feature table:

- **`FeatureBinaryHeaders` is Unsupported.** A header value — or a **message
  key**, which travels as a header because neither protocol has a slot for a key
  — that is not valid UTF-8 **cannot be carried**. It is refused **before any
  I/O** rather than mangled, and `MetadataPortableEnvelope` (or the AMQP 1.0
  arm, whose application properties are typed) is the route that carries one.
- **`FeatureDuplicateHeaders` is Unsupported, and worse than a map.** A STOMP
  frame *can* carry a repeated header name, so the second value goes on the wire
  — and STOMP 1.2 then says the **first** occurrence wins, so the broker discards
  it and nothing reports that it did. The promise is kept by
  `MetadataPortableEnvelope` instead.
- **A header whose name collides with a frame header or with one of the brokers'
  own is refused rather than shadowed**, and the same list filters a delivery's
  own headers back out. A Classic delivery was measured carrying eleven of them,
  and carrying those through would put `subscription: 1` and `ack: 1082,1088` in
  a caller's message.
- **There is no `content-encoding` header at all**, so that property travels as
  the reserved entry `x-iqmq-content-encoding`. A caller's own header under the
  reserved `x-iqmq-` prefix is refused rather than shadowed: the alternative is a
  header that silently means something to the driver.
- **An empty header name is refused on both arms.** A STOMP frame header with an
  empty name is a malformed frame the broker answers with an `ERROR` that closes
  the connection — taking every other publication in flight with it.

**The limits, and why only one is advertised:**

| Limit | Advertised | Why |
|---|---|---|
| `MaxHeaderBytes` | **255** | the longest header name or value either broker carries, enforced by the mapping **before any I/O** so an oversized header is refused rather than turned into a frame error. 255 is the smallest ceiling any broker in this repository's harness imposes, so it is the one a portable driver can advertise without claiming a limit it cannot check |
| `MaxMessageBytes` | **unknown** | a STOMP frame's body is bounded by the broker's own configuration — Artemis's `stompMinLargeMessageSize` and its acceptor's frame limits, Classic's `wireFormat.maxFrameSize` — and not by the protocol, so a driver reads it from the deployment rather than advertising a guess |
| `MaxDelay` | **unknown** | neither broker documents a ceiling and neither refused one in testing |
| `MaxLease` | **unknown** | STOMP grants no lease; advertising zero would read as "every lease expires immediately", which the specification makes a different answer |
| the batch limits | **unknown** | a batch here is a loop with no protocol-level bound |

## Reconnect

**The socket is watched.** STOMP has no liveness frame a client may send
unprompted (a `SEND` publishes, a `SUBSCRIBE` changes the broker's state, and
`Session`, `Server`, `Version` and `Stats` are answered from memory), so the
death of a connection is observed where it actually happens: go-stomp's reader
is the only reader of the socket, and a read error in a wrapper around it *is*
the death. Relying on the library to notice is not enough, measured against
v3.1.5: when the broker drops the socket, `processLoop` answers every **pending**
receipt with a synthetic `connection closed` error on an *unbuffered* channel,
and a publisher that has already given up on its receipt — the window, which
`Config.ReceiptTimeout` bounds — is not reading it. That send parks for ever,
the library's own `MustDisconnect` never runs, the connection is never marked
closed, and every later publication waits out `MsgSendTimeout` against a write
channel nobody reads. Before the socket was watched, a sink whose broker had
dropped the connection reported `msg receipt timeout` for ever while the
client's health still said **Ready**, and an IDLE connection's death was
noticed by nothing at all.

What a death notice is allowed to do is bounded above, and both bounds were
found the hard way. It runs on go-stomp's reader goroutine, so the backend's
notice only **wakes** the dial loop and the loop does the invalidation — a
notice that invalidated for itself raced the operation that met the same
death, and the loser found the connection already re-dialled and disconnected
one that was working. And its one-shot is an atomic flag rather than a
`sync.Once`: a source's notice closes the socket, and a `sync.Once` holds its
own mutex while it runs, so the notice re-entered the `Once` through `Close`
and parked the reader for ever.

**A close is a death too.** go-stomp closes a connection itself when a
heart-beat it was owed does not arrive and when the broker sends an `ERROR`
frame, so the watcher reports a close as well as a failed read or write. The
backend acts on it only while the closed connection is still the one installed,
which no close of the driver's own leaves it: a replaced connection, a torn-down
one and the client's own `Close` are all uninstalled first. The watcher used to
treat every close as the driver's and stay silent, so a connection the library
had timed out left the client Ready, nothing re-dialled, and the next
publication met `connection already closed`. That error is now a definite,
retryable `Unavailable` on the publish path rather than an ambiguous one,
because `Conn.Send` returns it before it builds the frame.

**go-stomp does not reconnect**: a connection whose socket died is a `*Conn`
whose every method reports it, and nothing re-establishes one. So this driver
owns the loop, and **the order is the contract**:

1. the connection **generation is bumped first**, so that from the instant the
   connection died every delivery a source handed out is stale and a settlement
   carrying one is refused rather than applied to whatever takes its place
   ;
2. every source's tokens are invalidated and its buffer swapped, which is what
   wakes a parked fetch — the bounded buffer has no other signal, so a driver
   that merely recorded the error would leave a consumer parked on a buffer
   nothing will ever push to ;
3. only **then** is the socket re-dialled;
4. each source re-sends its **own** `SUBSCRIBE` on its next fetch, because a
   subscription belongs to a source and the backend does not know what any of
   them asked for. The correctness half is eager and the re-subscribe is lazy.

A driver that re-dialled first would have a live subscription and a pile of `ack`
identifiers belonging to a subscription the broker has forgotten, and every one
of them would settle whatever now holds it.

"Dial" here means **the client can work again**, not "a socket is open": the
`CONNECT` frame is sent and the `CONNECTED` frame read before the dial returns,
so a client whose health is Ready really can publish. The backoff policy is named
**explicitly** (full jitter) rather than left to the variant family's `Auto`,
whose zero value resolves to no delay at all — which the specification forbids. This is the
third driver in this repository to work around that default.

`Config.Addrs` is a failover set tried round-robin across attempts. Each
dialler walks it on its own: the client's dial loop for its connection, and each
source for the connection it owns, starting at the address the client's
connection reached. They used to share one counter, so with a live node and a
dead one every second `OpenSource` drew the dead node and failed. `DialTimeout`
bounds every dial, under a caller's deadline as well (the sooner of the two
ends it), and a dial it ends while the caller's deadline still has time is a
retryable `Unavailable` (it was `DeadlineExceeded`, which blamed the caller's
deadline); `ReconnectAttempts` bounds the dials made for **one outage**, which is
how the helper defines it, so the count starts again after every connection
that succeeds; `NoReconnect` makes a lost connection terminal; and a
**borrowed** connection (`OpenWithClient` with `ClientBorrowed`) is never redialled, because
dialling would replace a connection the caller still holds, so such a client
goes Degraded and stays there.

While the dial loop runs, a publication or fetch that finds no connection gets a
retryable `Unavailable` ("the connection is not established"), because the next
attempt can succeed. Once the loss is terminal (`NoReconnect`, a spent
`ReconnectAttempts` budget, a borrowed connection, or the caller's connection
with no `Config.Addrs`) nothing will dial again, so every later publication,
fetch and `Ping` gets a **non-retryable** `Unavailable` that names which of the
four it was. The retryable answer used to stay for good, and a caller retried a
client nothing would repair. Health stays Degraded, and when `NoReconnect` or
the missing `Config.Addrs` is what stopped the client, its error is that same
refusal. It used to be `ErrClosed`, which told a caller watching Health that a
client nobody had closed was closed.

A source holds the same promises for the connection it owns. With `NoReconnect`
a source whose connection died does not dial another: the next fetch reports
what the pump read when it died (once), and every fetch after that a
non-retryable `Unavailable`. The socket's death is noticed before the pump reads
the broker's `ERROR` frame, so that fetch waits, under its own context, for the
dead subscription's pump to read it. It used to report the refusal to re-dial in
place of the broker's sentence, in 28 runs of 100 against the in-process server
and 79 under the race detector. That includes a source whose connection went with
the client's, because the client's death takes every source's connection with
it.

A source also drops a connection that still works, when an `UNSUBSCRIBE` goes
unanswered: on a requeue, and when it retires its subscription after the
client's connection has moved. Dialling the replacement is not a reconnection,
so `NoReconnect` does not refuse it. The replacement goes back to the node the
source just left, and a replacement dial that fails is not counted against
`ReconnectAttempts`; the next fetch dials the next node, and that dial is
counted. When the caller's context cuts the replacement short, the next fetch
goes back to that node once more, as an ordinary dial, and the walk moves on if
that one is cut short too. The replacement used to take the next node of the
walk, so with a live node and a standby one slow receipt failed the requeue on
the standby and, with `ReconnectAttempts` at 1, left the source refusing every
fetch while the live node was up. Going back costs something on a node that
takes the socket and never answers `CONNECT`: with a working standby and
deadlines longer than `DialTimeout`, the requeue's `Nack` now fails after
`DialTimeout` and the next fetch reaches the standby, where it used to succeed
on the standby in 251 ms.

With `ReconnectAttempts`, a source that has failed that many dials in a row
stops and says so, also non-retryable. A source re-dials inside the fetch that
needs the connection, so a dial that the fetch's own deadline or cancellation
ends is not counted and not remembered: the next fetch dials again, and only
the fetch whose context ended reports it.

A connection handed to `OpenWithClient` is held to the same product and version
gate as a dial. It carries the client's publications; a source dials a
connection of its own from `Config.Addrs`, so with no `Config.Addrs` a source is
**refused** at open rather than dialled at the resolved default (localhost,
which is not where the caller's connection went), and an owned connection with
no `Config.Addrs` is not redialled either.

`Close` leaves nothing behind that was being made while it ran. A dial of the
reconnect loop that had its `CONNECTED` frame before `Close` cancelled it used to
install its connection after `Close` had taken the old one, and an `OpenSource`
that was dialling the source's own connection used to register the source after
`Close` had closed every source it held. The connection is now disconnected,
and the source closed and refused with `ErrClosed`.

**Heart-beats are off when both are zero**, which is what a test wants and what a
deployment should not have: without them a connection that died silently is
discovered by the next write.

**There is no topology to restore**, because this driver creates none.

One library detail a deployment will meet in a packet capture: go-stomp tags its
own `UNSUBSCRIBE` and `DISCONNECT` frames with a `receipt` header and waits for
the answer — and it takes **four** receipt timeouts, not one.
`RcvReceiptTimeout` and `MsgSendTimeout` bound the publish path;
`UnsubscribeReceiptTimeout` and `DisconnectReceiptTimeout` are separate, are
not derived from the first two, and default to **thirty seconds**. This driver
sets all four from `Config.ReceiptTimeout`, because its requeue *is* an
`UNSUBSCRIBE`: with the defaults left alone a `Nack(NackRequeue())` cost half a
minute and reported `SettlementUnknown`.

**The broker is checked at dial.** The `CONNECTED` frame's `server` header
answers two questions with one string, and both are refused before any
destination is named: the **product** decides the whole destination mapping — a
client configured for Classic and pointed at Artemis publishes to
`VirtualTopic.orders` on a broker with no such convention, which auto-creates it
and reports nothing — and the **version** decides whether the mapping exists at
all (Artemis's FQQN arrived in 2.7, STOMP 1.2 with `client-individual` in
Classic 5.6). A broker that sends no header is accepted: absence is not
disagreement. `Open` returns these refusals, and a refused credential, from the
first attempt rather than retrying them: every attempt would get the same
answer, so with a context that never ends and no attempt limit, `Open` used to
never return. Once a client is open, the dial loop does keep retrying them,
because an operator may be fixing the broker.

## At-least-once window

From the `SEND` frame until the `ACK`'s own `RECEIPT` — or until the `ACK` frame
is written, when receipts are off.

A process that stops inside it leaves the delivery unacknowledged, and **an
unacknowledged `client-individual` delivery comes back when its subscription goes
away**: measured on both brokers, marked `redelivered: true`. That is what makes
at-least-once work here and it is also what makes the requeue work; the two are
the same mechanism seen from different ends. So a handler may run twice for one
message and the application has to absorb the duplicate — and **the second
delivery can say only that it is a redelivery, not that it is the third
attempt**, because the protocol carries no count.

Three things widen or narrow the window and each is explicit:

- **Receipts.** With them on, an `Ack` is final when the broker says so and a
  failure is an *unknown* outcome; with them off, an `Ack` is final when the
  frame is written and a failure is a *lost lease*. The specification makes the first
  permanent and the second not.
- **A subscription reset.** Every unacknowledged delivery comes back, so the
  window reopens for all of them at once. That is the requeue's cost, above.
- **`Close`.** The order is the contract: the buffer is closed **first**, so a
  parked fetch wakes with `ErrClosed` rather than with whatever the unsubscribe
  did to the channel; the token table is invalidated **second**, so a pump
  waiting for a free slot wakes and can be waited for; and the
  subscription is dropped **last**, which is what makes the broker redeliver
  everything this consumer never acknowledged (a close that only dropped the
  socket would leave them in the broker's delivering set until it timed out).
  The table used to go after the unsubscribe, and a caller holding every token
  then hung `Close` for good: the parked pump read nothing, go-stomp's channel
  filled, the `UNSUBSCRIBE`'s `RECEIPT` was never read, and the library's own
  timeout blocked on that full channel. A settlement racing the close is refused
  either way, because the source is marked closed before any of it, and a
  settlement checked before that has its frame queued ahead of the
  `UNSUBSCRIBE`. Whatever the
  buffer still held was never handed to a caller and stays unacknowledged, which
  is what makes the broker redeliver it.

## Config & DSN

| Field | What it is |
|---|---|
| `Broker` | **required, no default**: `stomp.Artemis` or `stomp.Classic` |
| `Addrs` | `host[:port]`, or with a TLS scheme: `stomp+ssl://…`, `ssl://…`, `stomps://…` or `tls://…`; a failover set each dialler (the client's dial loop, each source) tries round-robin across its own reconnect attempts. Default port **61613**, TLS **61614** |
| `Login`, `Passcode` | the `CONNECT` frame's credentials; both empty connects anonymously, which Classic's pinned image allows and Artemis's does not. `Passcode` is a `dsn.Secret` |
| `Host` | the `CONNECT` frame's `host` header, which STOMP 1.1 made mandatory and a broker behind a router uses for virtual hosting. Empty sends the address's own host |
| `TLS`, `InsecureSkipVerify`, `InsecurePlainLogin` | the security surface. A passcode over a plaintext connection is **refused** unless `InsecurePlainLogin` says so deliberately — and the check reads **every** address, because a failover list that mixes a TLS node and a plaintext one sends the passcode in clear as soon as it fails over, which is the one moment nobody is watching |
| `Prefix` | prepended to every destination. **Inside** `VirtualTopic.` on Classic |
| `Addressing` | `stomp.AddressFanOut` (the default) or `stomp.AddressQueue`, which reads from and publishes to a JMS queue by its own name; see "Reading a JMS queue by name" |
| `Selector` | a JMS message selector applied to every subscription this client opens (the specification's "selectors via typed config"). **Refused on Artemis**; see "Known gotchas" |
| `Scheduler` | reports that the broker's message scheduler is enabled. Ignored on Artemis; **required** on Classic for `FeatureScheduledPublish` |
| `Prioritised` | reports that the broker's destination policy reorders by priority. Ignored on Artemis, whose queues are priority-ordered; **required** on Classic for `FeaturePriority`, for the same reason and with the same silence as `Scheduler` |
| `DeadLetterTopic` | a dead-letter route this driver publishes to; see "Dead-letter queue". Needs receipts, so it is refused with `NoReceipts` |
| `Receipts`, `NoReceipts`, `ReceiptTimeout` | the confirmation boundary. `Receipts` is **on** by default; `NoReceipts` turns it off for a publisher that has chosen `ConfirmNone` everywhere and wants the round trip back |
| `Prefetch`, `PrefetchBytes`, `ConsumerWindow` | the three halves of the prefetch asymmetry; see "Consume path" |
| `HeartbeatSend`, `HeartbeatReceive` | the `CONNECT` frame's heart-beat pair |
| `DialTimeout`, `Reconnect*`, `NoReconnect` | the dial loop |
| `Metadata` | the mode for metadata the protocol has no place for — which over this protocol is more than over most |
| `Clock` | the clock the driver reads. It is on the **publish path** and that is not incidental: `expires` is absolute and TTL is relative, so every publication with a TTL is arithmetic against this clock |
| `Hooks` | the callbacks |

The DSN scheme is **`activemq+stomp`**:

    activemq+stomp://user:pass@localhost:61613?broker=classic&selector=colour%3D%27red%27

It is **not** `activemq`, which `drivers/activemq` claims, and the reason is
the specification's "never silently substituted": a registry in which one scheme could open
either protocol would make a connection string ambiguous about which capability
table the caller gets, **and the two tables really differ** — there is no native
nack over STOMP and no selector over AMQP 1.0. The `+` is RFC 3986's own
sub-scheme separator and the shared parser accepts it, so `activemq+stomp` reads
as what it is. Registering both modules with one registry is the supported
arrangement, and it is how a deployment moves one application from one protocol
to the other by editing a connection string.

The parameters, all optional but `broker`: `addressing` (`fanout` or `queue`),
`host`, `prefix`, `selector`,
`scheduler`, `prioritised`, `deadlettertopic`, `receipts`, `prefetch`,
`prefetchbytes`, `consumerwindow`, `receipttimeout`, `heartbeatsend`,
`heartbeatreceive`, `dialtimeout`, `reconnectbase`, `reconnectmax`,
`reconnectgrowth`, `reconnectattempts`, `noreconnect`, `tls`,
`insecureskipverify`, `insecureplainlogin` and `metadata`. Anything else is a
construction error naming it. **A DSN with a path is refused** rather than
ignored: neither ActiveMQ has virtual hosts, and STOMP's `host` header is a
parameter here because it is not a path either. `Config.DSN()` renders the
configuration with the passcode replaced by the redaction marker and everything
else intact, and the result parses back. An address with a TLS scheme is written
bare with `tls=true`, which is what the scheme meant; a failover list that mixes
TLS and plaintext nodes has no connection-string form, and its addresses are
written as given.

## Known gotchas

### Selectors are the reason this arm exists, and they work on one broker

A `selector` header on `SUBSCRIBE` is a JMS message selector, and it is the one
thing this arm has that the AMQP 1.0 arm does not — an AMQP 1.0 subscription
filter is a filter-set on the link's **source**, which go-amqp exposes no way to
set.

| | Artemis | Classic |
|---|---|---|
| measured | **ignored**: with the subscription named by a fully qualified queue name, the blue message a `colour = 'red'` selector should have excluded **was delivered** | **filters**: the same selector kept the blue message out |
| `FeatureServerFilter` | Unsupported | Native |
| `Config.Selector` | **refused at construction** | accepted |

The refusal matters more than the absence. An FQQN subscription is the **only**
shape this driver's durable shared subscription takes on Artemis, so a caller who
set a selector there would be told nothing and would filter nothing. It is
refused with `ErrUnsupported` naming the two alternatives (`drivers/activemq`
with a queue whose filter `Admin` set, or ActiveMQ Classic) rather than accepted
and dropped. The driver's own conformance harness documents the consequence
honestly, marking `capability/ServerFilter/works` inapplicable on Artemis with
the reason _"measured: Artemis ignores a STOMP `selector` header when the
subscription is a fully qualified queue name, which is the only form this
driver's durable shared subscription takes, so Config.Validate refuses a
selector here and there is nothing to exercise"_ — and a skip is never a pass,
so the claim stays unverified rather than being taken on trust.

### A subscription cannot be confirmed, because of a defect in go-stomp v3.1.5

A `receipt` header on a `SUBSCRIBE` frame cannot be used at all: the library
registers the subscription's own frame channel under the receipt identifier, the
`RECEIPT` closes that channel, and the library then **panics** with "send on
closed channel" from inside its own `processLoop`. Observed against Artemis.

So a subscription cannot be confirmed synchronously, and a destination that does
not exist surfaces on the **first `Fetch`**: the broker sends an `ERROR` frame,
the pump reads it, and the fetch reports `mq.CodeNotFound` **carrying the
broker's own sentence**. On Artemis with auto-creation disabled that sentence is
`AMQ339001 Destination does not exist <name>`, and keeping it is the whole
diagnosis: an `ERROR` frame's `message` header is the only one STOMP offers, so
the alternative — `Unavailable` with the sentence dropped — leaves an operator
with nothing. `TestCriticalRefusedSubscribeReportsNotFound` pins it, and pins it
at **two different moments** (the refusal arriving as the `SUBSCRIBE` is
processed, and at a moment the test picks) because the two orderings take
different paths through the driver and a test that could not choose would be
green about whichever one it happened to get.

### There is no topology frame, in any version of the specification

STOMP can `SEND` to a destination and `SUBSCRIBE` to one. It cannot create one,
delete one, purge one or report its depth — there is no frame for any of it. Both
brokers do those things over interfaces of their own (Artemis's management
address, Classic's JMX and its web console) and **neither is reachable from a
STOMP connection**.

So this backend deliberately does **not** satisfy `mq.AdminBackend`, and
`Client.Admin()` reports `ErrUnsupported` rather than returning a surface that
refuses every call — the core asserts the interface once when the client is built
and records the answer, so a backend that satisfied it and then refused would make
`FeatureAdmin` look available to a caller who asked the client rather than the
table. A compile-time assertion in `admin.go` is what keeps it that way.

What creates topology for these brokers, in the order a deployment should prefer:

1. **`drivers/activemq`'s `Admin`, against Artemis.** It is the broker's own
   management address over AMQP 1.0, and it creates the same names this driver
   derives, because both derive them from `internal/activemq`. The
   `activemq-artemis-stomp` conformance suite provisions exactly this way, which
   is what makes it a test of the addressing rather than of a coincidence.
2. **On ActiveMQ Classic, a `SUBSCRIBE`.** A consumer queue comes into existence
   when a subscription attaches to it and the broker cannot be told not to do
   that without an authorization plugin. It has to exist **before** the first
   publication. `ConformanceHarness` does it explicitly, which is what the specification asks
   of a driver that will not do it implicitly.
3. **The broker's own configuration**, which is what a production deployment
   should be doing anyway.

On Artemis with auto-creation disabled a `SUBSCRIBE` to a destination that does
not exist is refused — which is the honest failure and is what makes the "creates
no topology" claim provable.

The specification marks the two delay mechanisms `(verify)` and says nothing else about this
arm beyond "virtual topics, `client-individual` ack, selectors via typed config".
Both delay claims were verified against containers, and this arm's answer is
simpler than the AMQP 1.0 arm's:

Every "measured" fact on this page was measured against one of the two pinned
images. One thing on it is **not** measured and is called out so nobody reads it
as container evidence: **the go-stomp defect's blast radius** beyond the panic
itself, which was observed once against Artemis and not characterised further.

### Smaller ones

- **`FeatureRequestReply` is not claimed.** The `reply-to` header is native and a
  temporary reply destination can be created (`/temp-queue/<name>`), but the
  correlation is the core's portable emulation rather than a broker mechanism,
  and the specification forbids advertising one as the other.
- **`Config.Metadata` matters more here than elsewhere.** Over this protocol a
  binary header value, a binary key, a repeated header name and a content
  encoding all have no native place, so the mode is the difference between a
  refusal and an envelope. `MetadataPortableEnvelope` is the answer for a caller
  with any of them. Under it EVERY message is enveloped, and under `MetadataAdaptive` a
  message is also enveloped when its body begins with the envelope's own magic
  (`IQMQ` and a version byte), because this driver's source decides from the
  body. Before both went out natively and the driver's own
  source refused them.
- **Classic does not reorder by priority unless its policy says so, and the
  absence is silent — exactly like the scheduler's.** Classic carries the
  priority header and dispatches in FIFO order unless the destination's policy
  sets `prioritizedMessages="true"`, which the pinned image does not do for
  queues at all. Measured with it off, a high-priority publication arrives
  **after** a low-priority one — so `FeaturePriority` is **withdrawn** on Classic
  unless `Config.Prioritised` says the policy is there. Artemis's queues are
  priority-ordered and the field is ignored there.
- **A de-duplication key is refused on Classic** and carried on Artemis as the
  `_AMQ_DUPL_ID` header. `FeatureIdempotentPublish` is withdrawn on **both**:
  neither broker has a producer epoch or a sequence number, so a retried
  publication is a **second message**.
- **`Backend.Native()` is the `*gostomp.Conn`**, which is the escape hatch. A
  caller that reaches for it owns what it does with it.
- **Two scenarios must not share a topic on Artemis**, because the derived queue
  name is `<topic>.<subscription>` and an Artemis queue name is unique across the
  whole broker. The harness's topic names carry a nonce for that reason.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

None of these can be provoked from a container, which is why there is an
in-process STOMP 1.2 server (`internal/stompsrv`) and why its seam is the
**socket** rather than the library: go-stomp interposes almost nothing — it is a
frame codec plus one process loop — and every decision this driver has to get
right is a decision about **frames**. A seam above the codec would observe the
driver's own calls, which is the same thing as asserting that the code does what
it does. The server does not route, store, expire, schedule, filter or implement
a virtual topic: modelling the brokers' behaviour would be a second opinion about
the very facts this page records as measured, and the two would drift. One frame
it does refuse: an `ACK` or `NACK` naming a delivery of a subscription it has
unsubscribed. The driver must never send one, so the server records it as a
protocol violation that every test in the module reports, and answers with the
harshest reaction a broker could have, an `ERROR` frame and the socket. Neither
broker's real answer has been measured: Classic may drop the frame silently, and
Artemis may acknowledge the redelivered copy that now holds the identifier. The
server used to accept a settlement naming any identifier, which hid an `ACK`
written after a requeue's `UNSUBSCRIBE`.

- **`TestCriticalRequeueSendsNoNackFrame`** — this page's headline as a test, and
  the assertion is **positive** rather than an absence nobody looked for: after a
  requeue the server must have seen exactly `[UNSUBSCRIBE SUBSCRIBE]` and **no
  `NACK` frame and no `ACK` frame at all**. It would catch the obvious mapping,
  which destroys the message on Artemis while reporting a requeue. It then checks
  the other half of the mechanism — the redelivery arrives marked `redelivered`
  and reports attempt **0**, which is the specification's own value for "the broker does not
  count attempts" — and that `Source.Resets()` is 1, because a deployment that
  cannot see the number cannot see what its nack rate is buying.
- **`TestCriticalSettlementAfterAResetIsRefused`** —. An `ACK` identifier
  belongs to the **subscription** that sent it, so after a reset every one of them
  is the broker's again. It holds two deliveries, requeues one and settles the
  other: it would catch a driver that wrote the second `ACK` to a subscription the
  broker has taken back, which on a broker that reuses identifiers settles whoever
  now holds it. It is also what makes the requeue's **cost** a tested property
  rather than a claim.
- **`TestCriticalCancelUnblocksAParkedFetch`** —. A push-protocol driver
  buffers, and a fetch that found the buffer empty parks on it, so the **only**
  thing that can wake it is the buffer being closed. It would catch a driver that
  recorded the close and left the buffer alone, which leaves a consumer parked for
  ever and shows the caller a hang rather than an error.
- **`TestCriticalPublicationWithNoReceiptIsAmbiguous`** —. A `RECEIPT` is
  the whole of `ConfirmAccepted`, so a receipt that never arrives is the one case
  The specification names ambiguous. It would catch a driver that reported it as a definite
  failure, after which a publisher republishes and the broker holds two with
  nothing reporting a duplicate. No broker will hold a receipt on request.
- **`TestCriticalFailedDeadLetterPublishDoesNotAcknowledge`**, and the
  order is the whole contract. The injected failure is the **ambiguous** half of
  "failed or ambiguous": the dead-letter publication's `RECEIPT` never arrives, so
  the driver does not know whether the dead-letter topic has the message and must
  therefore not destroy the only other copy. It would catch an `ACK` that went
  first or went anyway — which on Artemis loses the message, because a `NACK`
  there acknowledges.
- **`TestCriticalRefusedSubscribeReportsNotFound`** — where a broker's refusal of
  a `SUBSCRIBE` surfaces, at two different moments. It would catch a driver that
  reported `Unavailable` and dropped the broker's own sentence, which is the only
  diagnosis STOMP offers.
- **`TestCriticalConnectionDeathIsRecoveredAndStaleSettlementsRefused`** — the specification's
  reconnection and the staleness as one test, because the second is what makes
  the first safe. It would catch a driver that re-dialled **before** invalidating,
  which leaves a live subscription and a pile of `ack` identifiers belonging to a
  subscription the broker has forgotten.
- **`TestCriticalSinkRecoversAfterTheConnectionDies`** — the **backend's** dial
  loop, which no source exercises any more now that each one owns its socket. A
  publisher never stops reading, so there is nothing for it to deadlock against,
  and this is what holds the backend's connection to the specification's "reconnect with
  backoff". It also pins what the specification allows the publication that met the death to
  report: retryable, or ambiguous, and never a failure a caller can neither
  retry nor reconcile.
- **`TestCriticalAnIdleConnectionsDeathIsNoticed`** — the death **nobody meets**.
  A broker drops a connection nothing is using, so there is no operation to
  report it, no error for the rule table to classify and no event from go-stomp.
  It would catch — and did catch — a client that went on calling itself Ready
  with a dead socket until the next publication, which then failed for a reason
  the library could not even name.

## Choosing between the four combinations

[activemq.md](activemq.md#choosing-between-the-four-combinations) carries the
whole table and the verdict. The short version, from this side:

| Combination | Native features (of the 39) |
|---|---|
| AMQP 1.0 / Artemis | **19** |
| AMQP 1.0 / Classic | **16** |
| **STOMP / Artemis** | **15** |
| **STOMP / Classic** | **13** |

expected answer is **"feature completeness on Artemis, then perf →
AMQP 1.0 default; STOMP kept for Classic"**, and the measurements bear it out.
Four rows separate this arm from the default on **both** brokers and each of
them is on this page: no negative settlement, no attempt count, no binary header
value, and no protocol-level flow control. **One** row goes the other way, and it
goes the other way on one broker only — a working server-side **selector**, which
is measured to filter on Classic and to be ignored on Artemis, and which is the
only thing in the whole family that the default cannot do.

So choose this driver when you are on **ActiveMQ Classic and need a broker-side
filter**, or when STOMP is the only port you are allowed through. Choose
[the AMQP 1.0 driver](activemq.md) otherwise — and in particular whenever you
nack, count attempts, or carry a header that is not text.
