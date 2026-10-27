# jetstream

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 1 · **Module:** `github.com/iqhive/iqmq/drivers/jetstream` · **Profile:** JetStream

## Client & version policy

The native baselines for this driver live in `benchmarks/native-baselines/` and measure the clients below against the harness's `nats` image (`make bench-native CLIENTS="jetstream"`). A driver number is only comparable with a baseline taken from the same client version, broker version and confirmation boundary, so the pins move together; each baseline's package comment records the settlement boundary it measures as well.

| Baseline | Client | Pinned version | Confirmation boundary |
|---|---|---|---|
| `jetstream` | github.com/nats-io/nats.go | `github.com/nats-io/nats.go@v1.54.0` | JetStream PubAck from a file-backed stream (R1) |

### Verified facts (the API contract)

Checked while building the native baselines; re-verify when a pin moves.

- **per-message TTL and schedules.** The client exposes per-message TTL as the Nats-TTL header (jetstream.MsgTTLHeader) and message schedules as a schedule header taking an @at timestamp, an @every duration or a cron expression, gated by StreamConfig.AllowMsgSchedules; TTL needs a 2.11+ server and schedules a 2.12+ server, and the harness pins nats 2.14.6.
  _Verified against: nats.go v1.53.1 jetstream/message.go:210,220-232 and jetstream/stream_config.go:211; conformance/harness/images.go._
- **deduplication and progress.** Publish deduplication uses the Nats-Msg-Id header (jetstream.MsgIDHeader) against the stream's duplicate window, and a consumer extends its ack wait with Msg.InProgress rather than a lease API.
  _Verified against: nats.go v1.53.1 jetstream/message.go:169,378._

## Topic / subscription mapping

| iqmq | JetStream | Notes |
|---|---|---|
| `Topic` | subject inside `Config.Stream` | `/` becomes `.`; `Config.SubjectPrefix` is prepended as a token |
| `ConsumerConfig.Subscription` | durable pull consumer | two consumers of one subscription compete; two subscriptions each receive everything |
| `ConsumerConfig.Topic` (pattern) | the consumer's `FilterSubjects` | `*` matches one token, a trailing `>` matches the rest |
| `ConsumerConfig.Prefetch` | `MaxAckPending` **and** the client's `PullMaxMessages` | the same number, which is what makes the prefetch a real bound |
| `ConsumerConfig.AckDeadline` | `AckWait` | `Config.AckWait` (30s) when the consumer names none |
| `Message.Key` | `iqmq-key` header | base64, because a NATS header value is a text line |
| `Properties.ReplyTo` | `iqmq-reply-to` header | not `Msg.Reply`: a JetStream publish takes that subject for its own ack, and an async publish refuses one |
| `DeliveryInfo.Attempt` | `NumDelivered` | the server's own count, which starts at 1 |
| `DeliveryInfo.Sequence` | the stream sequence | always set; it is also what the stale-ack guard is keyed on |
| `DeliveryInfo.Source` | the stream name | |

The subject alphabet this driver will build or accept is letters, digits, `_`,
`-` and `$`, with `.` as the separator — the image of the portable topic
grammar under the separator swap, plus the `$` that NATS system subjects use.
Anything else is refused **before any I/O**, because a malformed subject does not
produce a rejected publication on this protocol: it produces a malformed frame
on a connection every other publisher and subscriber in the process is sharing.

**The mapping is not injective.** `orders/created` and `orders.created` are one
subject, so `DeliveryInfo.Topic` reports the subject the message really arrived
on with the prefix stripped. A deployment that needs the two to stay distinct
uses one spelling.

A publication's topic may never be a pattern. A consumer's topic may be one, and
that is what `FeatureWildcardSubscribe` means here.

### The stream, and who creates it

`Config.Stream` is the one piece of topology JetStream really has, and
**`Config.ManageStream` is off by default**: the specification forbids a driver from
provisioning topology behind the caller's back, so a client whose stream does not
exist fails at `Open` with `CodeNotFound`. A deployment that wants the driver to
own the stream says so, and then gets the stream the configuration describes
(`StreamSubjects`, `Storage`, `Replicas`, `MaxAge`, `MaxMsgs`, `MaxBytes`,
`MaxMsgSize`, `DuplicateWindow`).

`ManageStream` needs either a `SubjectPrefix` or an explicit `StreamSubjects`.
The guess that suggests itself — `>`, every subject — is not merely broad:
JetStream **refuses** a stream that captures every subject unless it is also
created with `NoAck`, which would discard every acknowledgement this profile
exists for.

A publication to a subject the bound stream does not capture is refused with
`ErrTopicNotFound` when the publisher is opened. That is not fussiness: under
`ConfirmNone` the publication is a core publish with no reply subject, so the
server drops it and tells nobody. The subject list is read at
`Open`; a stream reconfigured underneath a running client is not noticed until
the next one.

### Durable consumer names

A subscription maps to a durable named `Config.ConsumerPrefix` + the sanitised
subscription + a short hash of the filter — `iqmq-billing-1f3k9z`. The hash is
not decoration: a JetStream durable is per `(stream, filter)`, so the same
subscription name used on two topics has to be two durables, or the second would
inherit the first's ack floor and skip messages. Every byte
JetStream refuses in a name is mapped to `-`, and an over-long name is cut. When
either changes the name, the original subscription is folded into the hash too, so
`billing.v1` and `billing-v1` stay two durables; a name that needed neither keeps
the durable it always had.

An existing durable's own configuration wins. `CreateOrUpdateConsumer` cannot
change a consumer's `DeliverPolicy`, and a second consumer of one subscription
asking for a different start position must not be the reason the subscription
stops working, so an update the server refuses becomes a lookup — which is what
a durable subscription means.

## Publish path & confirmation boundary

| Mode | What it waits for |
|---|---|
| `ConfirmNone` | nothing: a **core** publish to a stream-bound subject, with no reply subject asked for |
| `ConfirmDefault` / `ConfirmAccepted` | the stream's **PubAck** |
| `ConfirmDurable` | the stream's **PubAck** — the same round trip |

On this broker "accepted" and "durable" are the same boundary, because the
PubAck is sent after the message is committed to the stream's storage. That is
why `ConfirmDurable` is **not** refused here — the Core driver refuses it at
construction — and why `FeatureDurableConfirm` is a native claim.
It is withdrawn, with `FeatureDurableStorage`, when the
bound stream is **memory-backed**: a stream that loses everything on the next
restart cannot support a durability claim, and a program that requires one fails
at construction instead.

`ConfirmNone` costs no round trip and reports nothing. The message still reaches
the stream — the subject is bound to it — but a failure is invisible, which is
what the mode means.

Per-call controls:

| Control | Answer |
|---|---|
| `DedupKey` | `Nats-Msg-Id`, de-duplicated inside the stream's `Duplicates` window |
| `TTL` | `Nats-TTL` — **conditional**: a 2.11+ server and a stream created with `AllowMsgTTL` |
| `Delay` / `PublishOptions.DeliverAt` | a `Nats-Schedule` control message — **conditional**: a 2.12+ server and `AllowMsgSchedules` |
| `Priority` | `ErrUnsupported` before any I/O |
| `Partition` | `ErrUnsupported` — a subject is the only partition there is |

The controls are written as the server's own headers rather than through the
client library's `PublishOpt` functions, so that all three publication paths
carry them identically: a `ConfirmNone` publication is a core publish and never
sees a `PublishOpt`, and a caller who asked for de-duplication has no way to tell
which path its message took.

`PublishBatch` publishes every message asynchronously — bounded by
`Config.PublishAsyncMaxPending` — and waits for every PubAck, so each message of
a batch is exactly as durable as a single publication and the result is
index-aligned with the batch. It is **not atomic**: JetStream's atomic batch is a
stream flag this driver does not set, which is why `FeatureBatchPublish` is
`EmulatedDurable` rather than `Native`.

`PublishAsync` costs **no goroutine per message**, which the specification forbids outright.
The JetStream client reports every asynchronous publication through two
connection-wide callbacks that carry the `*nats.Msg` the publication was made
with, so the backend keeps a map from that pointer to the publication and
resolves each one from the client's own reply goroutine. A
publication still outstanding when the publisher closes reports
`CodeSettlementUnknown`, which is the truth: the message had left and its PubAck
had not arrived.

A PubAck gets `Config.PublishTimeout` to arrive. One that does not, with the
link still up (a reply lost across a cluster leader change, say), is reported
to the callback as an ambiguous `CodeDeadlineExceeded` and its place in
`PublishAsyncMaxPending` is given back. Without the bound the callback waited
for the publisher's close. The client library reports that timeout while it
holds its own publisher lock, and a callback that published again from there
would wait for that lock for ever. So those callbacks, and only those, run on a
goroutine of the backend's that exists only while one of them is waiting.

**Every asynchronous path copies the payload.** `nats.go` appends the payload to
its own write buffer inside `PublishMsg`, so the first attempt never aliases the
caller's slice — but the JetStream client *retains* the `*nats.Msg` and
republishes it on a "no responders" retry and after a reconnection, and a retry
that read the caller's slice would put whatever the caller wrote next on the
wire. `Backend.AsyncBodyCopies` counts them. A synchronous
publication retains nothing and copies nothing.

`FeatureReceipts` is native: a receipt carries the stream sequence, the
confirmation boundary (always `ConfirmDurable`, because that is what a PubAck
*is* here), the confirmation time and `<stream>:<sequence>` as the identifier.

## Consume path

`OpenSource` creates or binds the subscription's durable **pull** consumer and
starts the client library's `Consume` loop, whose callback pushes into a
`driverkit.PushAdapter` sized at the consumer's prefetch.

The prefetch is a real bound on this broker rather than a client-side buffer,
because it is three numbers at once: the ring's capacity, the client's
`PullMaxMessages`, and the consumer's `MaxAckPending`. Blocking in the callback
therefore stops the **server**, not just the client.

`Fetch` is one blocking wait, then a non-blocking take of everything the ring
holds — which is what makes `FeatureBatchReceive` true — and then one filter.
The filter is not optional. `MaxAckPending` counts unacknowledged **messages**,
so a record whose lease keeps expiring while nobody settles it is delivered again
and again and every copy lands in the ring behind the first. Handing the caller
the oldest would be handing it a delivery the driver already knows is dead: its
settlement is refused with `ErrLeaseLost` and the handler's work is thrown away.
A delivery whose token went stale while it waited is dropped here, and
`Backend.SupersededDeliveries` counts them. A deployment
that sees that counter climb raises `AckWait`.

All four start positions exist: `StartLatest` is `DeliverNew`, `StartEarliest` is
`DeliverAll`, `StartAt` is `DeliverByStartTime` and `StartAtSequence` is
`DeliverByStartSequence`. A start sequence of zero is refused — JetStream stream
sequences begin at 1. The policy only decides where a consumer with **no
committed position** begins; a durable that already has an ack floor resumes from
it.

A consumer whose pattern also matches `Config.ScheduleSubject`'s tree is
refused. A JetStream consumer has no negative filter, so the alternative is
handing the caller the driver's own schedule control messages as though the
application had published them.

`Close` does four things in an order where each depends on the one before: it
sets the closing flag, closes the ring (which wakes a parked callback), **drains**
the client library's own buffer so that what it had already fetched reaches the
callback and is naked, and then naks every delivery the token table still calls
active — which is the ring's leftovers *and* everything the source handed the
core that was never settled. Abandoning instead would lose nothing, because the
server redelivers after `AckWait`, but on a thirty-second default it is the
difference between a restart that resumes and one that appears to hang.

**One window a `Close` cannot reach.** A pull consumer's request is outstanding
for a whole batch, and `nats.go` drops what arrives after it unsubscribes, so
messages the server had already put on the wire towards the dying subscription
are never seen by the driver and cannot be naked. They wait out their lease: at
most one batch per close, stalled for at most one `AckWait`, never lost —
at-least-once is the server's. `nats-server` then advances the consumer's ack
floor over such messages once its pending set empties, so a handful can be
accounted for at the broker without this client settling them at all.
A deployment that closes consumers often and cannot afford
the stall lowers `AckWait`.

## Ack / nack / extend mapping

| iqmq | JetStream |
|---|---|
| `Delivery.Ack` | `+ACK` — a publish to the delivery's reply subject, or a request/reply round trip under `Config.DoubleAck` |
| `Delivery.Nack()` / `Nack(NackRequeue())` | `-NAK` |
| `Nack(NackDelay(d))` | `-NAK {"delay":<d>}` — the server holds the message; nothing is held in this process |
| `Nack(NackDrop())` | `+TERM` — destructive, which is why the specification makes it something a caller has to write |
| `Delivery.Reject()` / `Nack(NackDeadLetter())` | a durable publication to `Config.DeadLetterTopic`, **then** `+TERM` |
| `Delivery.Extend(by)` | `+WPI` — the server resets the redelivery timer to `AckWait` from now. A `by` longer than `AckWait` is refused with `InvalidArgument`, because `+WPI` cannot grant it and the message would come back at `AckWait` |

`Config.DoubleAck` decides the positive boundary. Without it the ack is a publish
and the call returns as soon as it is in the write buffer, so a link that dies
immediately afterwards leaves the message redeliverable — which is at-least-once
behaving as advertised. With it the ack is a request and the call returns only
once the server has recorded it, at the cost of one round trip per message.

### A stale ack must not settle a redelivery

This is the part worth reading the source for, and the specification makes the unguarded
behaviour a **disqualifier**.

JetStream acknowledges by **reply subject**, and `nats-server`'s `AckExplicit`
path deletes its pending entry for the message's *stream sequence* without
looking at which delivery the reply subject came from. So the reply subject of a
delivery whose lease expired still works: acking it acknowledges the record that a
*later* delivery now owns, the holder of that later delivery never sees a
redelivery, and nothing in the client or the server reports the loss.

This driver refuses that ack. Every delivery takes a slot in a
`driverkit.TokenTable`, and the source keeps an index from stream sequence to
token: when a record is delivered again, the previous delivery's slot is released
— which bumps its generation — and a settlement carrying the old token reports
`ErrLeaseLost` before anything reaches the wire.
`Backend.StaleSettlements` counts the refusals.

`TestStaleAckWouldOtherwiseSettleTheRedelivery` reaches past iqmq through
`Delivery.Native()` and makes the ack the guard refuses, so that the guard has
something to be measured against; `FuzzNATSStaleAck` holds the invariant over
every interleaving with a model beside it; and the broker-version matrix checks
the guard on 2.10, 2.11, 2.12 and 2.14, because it is the *server's* behaviour
that makes the guard necessary.

## Ordering

JetStream stores one subject's messages in the order the stream accepted them,
and a single consumer of a subscription sees them in that order. That is the
whole guarantee, and the three ordering features are three names for it:

| Feature | Level | Condition |
|---|---|---|
| `OrderedGlobally` | conditional | one publisher, one subject, one consumer of the subscription |
| `OrderedByKey` | conditional | there is no message key; a key orders only in so far as its messages share a subject |
| `OrderedByPartition` | conditional | there are no partitions; a subject is the only partition there is |

Three things break it and all three are ordinary: **two publishers** are not
ordered against each other, a **subscription with two consumers** interleaves
across them, and a **nack** puts a message behind the messages that followed it.
A deployment that needs order keeps one publisher per subject and one consumer
per subscription — the same trade every broker makes, stated here rather than
implied.

## Delays

Two different things, and only one of them is version-gated.

**Delayed redelivery** is native and unconditional: `Nack(NackDelay(d))` is
`-NAK {"delay":<d>}`, the server makes the message deliverable again when the
delay is up, and nothing is held in this process. This is the one a retry policy
uses.

**Scheduled publication** is conditional. `Delay` and
`PublishOptions.DeliverAt` are written as a `Nats-Schedule: @at <timestamp>`
control message on `Config.ScheduleSubject`'s tree, with the topic's subject as
the `Nats-Schedule-Target`; the server generates the application's message on
that subject when the schedule fires. It needs a **2.12+** server and a stream
created with `AllowMsgSchedules`, and the claim is read off the stream this
client is bound to rather than off a version string. The timestamp keeps its
fraction of a second, which RFC 3339 allows and the server parses, so a
publication is not delivered before the time it names; in whole seconds it
fired up to a second early.

The schedule tree is a **sibling** of the topic tree and not a child of it —
`iqmqsched.<stream>.>` — so a wildcard subscription under `SubjectPrefix` cannot
receive the control messages, and the stream's name is part of the root because
JetStream refuses two streams in one account whose subjects overlap.
When the driver manages the stream it adds the tree itself;
when the caller supplies `StreamSubjects`, a schedule root the stream does not
capture withdraws the claim, because a control message the stream does not hold
is a schedule that never fires.

A schedule in the past is refused with `CodeInvalidArgument`: a caller who asked
for a delay and got none has been told something untrue about when its message
becomes visible.

## Dead-letter queue

`Reject()` publishes the message to `Config.DeadLetterTopic` and **then**
terminates the original. The order is the whole of it:

1. `js.PublishMsg` to the dead-letter subject — a **durable** publication that
   returns after the stream has committed the copy, not a core publish that
   returns from the write buffer;
2. only once that PubAck is in hand, `+TERM` on the original.

A backend with **no** dead-letter subject configured answers `Reject` with
`ErrUnsupported` and settles nothing, which is the rule. `FeatureDeadLetter`
is therefore **conditional** on every client, and the note says which side of the
condition this one is on.

The copy carries the original's payload and caller headers unchanged, minus the
server's own `Nats-` headers, plus three reserved ones: `iqmq-dead-letter-from`
(the subject it was rejected from), `iqmq-dead-letter-attempt` (the delivery
count) and `iqmq-dead-letter-reason` (`NackOptions.Reason`). The specification makes `Reason`
local diagnostics "unless the driver documents that it transports it"; this
driver documents that it does, on this route only, because a dead-letter queue
with no reason in it is the one place the diagnosis is worth the bytes.

**A message the consumer cannot decode is the one message the driver
dead-letters on its own.** That is a plain body arriving at a
`MetadataPortableEnvelope` consumer, or an envelope a `MetadataAdaptive`
consumer refuses, and the application never sees it. With
`Config.DeadLetterTopic` set, the driver publishes a raw copy there in
`Reject`'s order (the PubAck first, then `+TERM`), with
`iqmq-dead-letter-reason` set to `undecodable: ` and the decode error, so the
original does not come back and the messages behind it keep arriving. Health
stays as it was and `Backend.DeadLettered` counts it. With no dead-letter topic,
or when the copy cannot be stored, the driver sends `-NAK` with a delay of one
lease (the consumer's `AckDeadline`, or `Config.AckWait`) and reports the
failure to health and `Backend.ConsumeErrors`. `MaxDeliver` is unlimited by
default, so the message then comes back once per lease for ever unless you set
`Config.MaxDeliver`. JetStream keeps a naked message in the consumer's pending
set while it waits, so it also holds one slot of the prefetch: under
`Prefetch: 1` nothing behind it is delivered. Set `DeadLetterTopic` if your
producers can write messages this consumer cannot read.

## Headers & limits

NATS headers are an HTTP-like block, `Name: value` per CRLF-terminated line. That
decides what can be carried, and the rules are about framing rather than taste:

| Rule | Why |
|---|---|
| a name may not be empty, carry a colon, a space, a control byte or a byte above ASCII | the reader truncates at the colon and a CR or LF forges a second header |
| a name may not be in the **`Nats-`** namespace | the server owns it: `Nats-Msg-Id` is de-duplication, `Nats-TTL` is expiry, `Nats-Schedule` is a schedule, so a caller header there would change the broker's behaviour rather than travel with the message |
| a value must be valid UTF-8 | the block is a text line and the reader is `net/textproto`'s |
| a value may carry no control byte | CR and LF forge a header; the rest do not survive |
| a value may not begin with a space or a tab | the reader eats those after the colon, so the value comes back shorter |

The `Nats-` rule is this profile's own and the Core driver does not need it.
It runs in both directions: a caller header there is refused
under `MetadataNativeStrict` and dropped under `MetadataLenientDrop`, and a
header the **server** wrote is never decoded back as a caller header — a message
the scheduler generated carries `Nats-Scheduler` and `Nats-Schedule-Next`, and
handing those to the application would be inventing metadata it never published.

A header that breaks one of the other rules is refused with `ErrUnsupported`
under `MetadataNativeStrict`, dropped under `MetadataLenientDrop` (and counted by
`Backend.DroppedMetadata`), and carried in the portable envelope under
`MetadataAdaptive` and `MetadataPortableEnvelope`. `FeatureBinaryHeaders` is
therefore unsupported and `FeatureNativeHeaders` and `FeatureDuplicateHeaders`
are native.

**Header order is not preserved.** `nats.Header` is a Go map, so the publisher's
ordering is gone by the time the client hands the message over; this driver
decodes the names **sorted**, which is deterministic where a bare map iteration
would give one message a different order on every delivery. Duplicate values of
one name keep their order.

Everything the specification names that has no native field travels under the reserved
`iqmq-` prefix, and the names are identical to the Core driver's so that a
message published by one profile is readable by the other: `iqmq-key`,
`iqmq-id`, `iqmq-content-type`, `iqmq-content-encoding`,
`iqmq-correlation-id`, `iqmq-causation-id`, `iqmq-created-at`,
`iqmq-empty-body`, `iqmq-env`, and the three dead-letter headers above.

!!! warning "Upgrade producers before consumers"
    A message enveloped by a version of this driver that predates the marker
    carries none, so a `MetadataAdaptive` consumer on the NEW code delivers it
    natively — the envelope's own bytes as the payload — rather than decoding
    it. The pairing that breaks is narrow: a producer that enveloped (either
    `MetadataPortableEnvelope`, or `MetadataAdaptive` with metadata NATS cannot
    carry) read by a `MetadataAdaptive` consumer, for messages published before
    the producers are upgraded. A `MetadataPortableEnvelope` consumer is
    unaffected in either direction, and an upgraded producer is read correctly
    by an OLD consumer, because the old code sniffs the body and the extra
    header is invisible to it. So: producers first, then consumers.

`iqmq-empty-body` is the driver's own: `nats.go` builds every payload with
`make([]byte, n)`, so a zero-length payload always arrives as an empty non-nil
slice and the nil-versus-empty distinction would be lost in one direction.

**Limits.** `Limits.MaxMessageBytes` is the **smaller** of the server's
`max_payload` and the stream's own `MaxMsgSize`, which is the only answer that
cannot lie in the dangerous direction; a publication past it is refused with
`ErrMessageTooLarge` before any I/O. Every other limit is left unadvertised —
The specification forbids reading an unadvertised limit as zero, so "we do not know" is a
`Limit` whose `Known` is false. The driver's own bounds are a 1 KiB subject, 64
subject tokens, a 255-byte stream or consumer name, a 256-byte header name and an
8 KiB header value.

## Reconnect

The client library owns the dial loop, and this driver owns what it means:

- a lost link makes health `Degraded` with the reason (`DisconnectErrHandler`);
- a re-established link makes it `Ready` again and fires `Hooks.OnReconnect`
  with the attempt number (`ReconnectHandler`);
- a connection the server closed for good makes it `Closed`, which is terminal
  in `driverkit.Health`, so a late callback cannot report `Ready` afterwards;
- a **slow consumer** makes it `Degraded` and increments
  `Backend.SlowConsumers`;
- the JetStream **consume loop** has an error channel of its own — a deleted
  consumer, a leadership change, a missing heartbeat, a refused pull request —
  and every one of those reaches health and `Backend.ConsumeErrors`. Nothing on
  the fetch path reports them, and the symptom is a consumer that simply stops
  receiving.

**A reconnection does not invalidate the delivery generations**, and that is a
decision rather than an omission. An acknowledgement here is a message to a
delivery's own `$JS.ACK.…` reply subject, and that subject survives a
reconnection: the consumer, its pending state and its ack floor are the server's,
not the connection's. A driver that bumped the generation on reconnect would
answer `ErrLeaseLost` to a caller whose ack would have worked. What *does* make a
token stale on this broker is a redelivery of the same record.

Nothing is buffered across a reconnection beyond the client's own reconnect
buffer, whose overflow is reported as `CodeBackpressure`. Asynchronous
publications outstanding across a reconnection are reported as **ambiguous**: the
message had left and the PubAck had not arrived. nats.go v1.54.0 drops them at
the disconnection and holds their error callbacks until the connection closes,
so the driver does not wait for the library. When the link comes back, every
`PublishAsync` publication the library dropped gets its callback with
`CodeSettlementUnknown`, including one that was still stalled on a full
`PublishAsyncMaxPending` window when the link went, and `Flush` stops waiting
for it. The library's own callback still arrives at the close and finds nothing
left to report, so the callback runs once. A publication made while the link
was down is buffered and kept by the library, and its PubAck resolves it.
`PublishBatch` and `Flush` are told at the reconnection too.

On a connection passed to `OpenWithClient` the driver installs no connection
handlers, so there a dropped publication's callback still waits for the
publisher's close or the connection's.

## At-least-once window

**From the first delivery until the ack the server recorded.** Three ordinary
things widen it and each produces a redelivery the application has to absorb:

- a handler that takes longer than `AckWait` (use `Delivery.Extend`, or raise the
  deadline);
- a process that dies mid-handler;
- a link that drops after `Ack` returned but before the ack reached the server —
  which is what `Config.DoubleAck` closes, at one round trip per message.

`DeliveryInfo.Attempt` is the server's `NumDelivered`, so the second delivery of
a record says so, and `DeliveryInfo.Redelivered` is `Attempt > 1`.

Publication has an ambiguous window too: a PubAck that times out, a link that
drops with a publication outstanding, or a publisher closed before its PubAck
arrived, are all reported with `OpError.Ambiguous` or
`CodeSettlementUnknown`, because the stream may hold the message. A retry may
duplicate — which is what `DedupKey` and the stream's `Duplicates` window exist
for.

What this driver does **not** have is exactly-once *delivery*. Publication
de-duplication is real and advertised; a delivery whose lease expires is still
redelivered, so a handler may run twice, and `FeatureExactlyOnceDelivery` is
unsupported.

## Config & DSN

```go
client, err := jetstream.Open(ctx, jetstream.Config{
        Addrs:           []string{"nats://broker-1:4222", "nats://broker-2:4222"},
        Stream:          "ORDERS",
        SubjectPrefix:   "acme",
        ManageStream:    true,
        AckWait:         30 * time.Second,
        DeadLetterTopic: "orders/dlq",
        DoubleAck:       true,
})
```

or from a DSN, once the driver is registered with a `dsn.Registry`:

```
natsjs://user:password@host-1:4222,host-2:4222/acme?stream=ORDERS&managestream=true&ackwait=30s&doubleack=true&deadlettertopic=orders%2Fdlq
```

| Parameter | Meaning |
|---|---|
| *path* | `SubjectPrefix` |
| `stream`, `streamsubjects`, `managestream` | the stream and whether the driver owns it (`;` separates subjects, because `,` separates the endpoints) |
| `storage`, `replicas`, `maxage`, `maxmsgs`, `maxbytes`, `maxmsgsize`, `duplicatewindow` | the stream's own settings |
| `consumerprefix`, `ackwait`, `maxdeliver`, `inactivethreshold` | the durable consumer's |
| `deadlettertopic` | where `Reject` routes before it terminates |
| `schedulesubject`, `disableschedules` | the schedule tree, or no schedules at all |
| `doubleack` | make `Ack` a request/reply round trip |
| `publishasyncmaxpending`, `publishtimeout` | the asynchronous bound and the round-trip bound |
| `apiprefix`, `domain` | a JetStream API prefix or a leaf-node domain (exclusive) |
| `name`, `creds`, `token`, *userinfo*, `tls`, `insecureskipverify` | the connection and its credential |
| `pendingmessages`, `pendingbytes`, `connecttimeout`, `reconnectwait`, `maxreconnects`, `pinginterval`, `maxpingsout`, `draintimeout` | the client library's own bounds |

An unknown parameter is a construction error naming it, and
every credential is a `dsn.Secret`: `Config.String()`, `Config.LogValue()` and
`Config.DSN()` all render the redaction. `Config.Validate()` is pure — no I/O,
no dial — so a typo is an immediate refusal rather than a five-second timeout.

Authentication that needs a signing callback (NKey, a custom JWT handler), a
custom dialler or a websocket URL is reached through `OpenWithClient`: build the
`*nats.Conn` however the deployment needs and hand it over with an explicit
`driverkit.Ownership`.

## Known gotchas

1. **`Config.ManageStream` is off.** Without it, `Open` fails with
   `CodeNotFound` unless somebody has already created the stream. That is,
   not an oversight.
2. **A stream that captures `>` needs `NoAck`**, which would discard every
   acknowledgement, so `ManageStream` requires a `SubjectPrefix` or an explicit
   `StreamSubjects`.
3. **A short `AckWait` wastes handler work.** A record whose lease keeps expiring
   is delivered again and again; the driver drops the superseded copies rather
   than handing them out, but each one was a message the broker sent for nothing.
   Watch `Backend.SupersededDeliveries` and `Backend.StaleSettlements`.
4. **`Ack` is not a round trip unless you ask.** Set `Config.DoubleAck` when a
   redelivery after a lost link is unacceptable.
5. **`Reject` is `ErrUnsupported` without `Config.DeadLetterTopic`**, and the
   delivery stays settleable. That is deliberate: the alternative destroys the
   message.
6. **`Nack(NackDrop())` destroys the message.** It is `+TERM`, and nothing
   brings it back.
7. **A per-message TTL and a scheduled publication depend on the STREAM, not on
   the driver.** An older server ignores the flags, the stream comes back without
   them, and the claim is withdrawn — so check
   `Capabilities().Has(FeatureMessageTTL)` rather than the broker's version
   string, and remember that a stream created before an upgrade still lacks the
   flag.
8. **A consumer whose pattern reaches the schedule tree is refused.** Narrow the
   pattern, or set `Config.DisableSchedules`.
9. **`a/b` and `a.b` are the same subject.** Pick one spelling per deployment.
10. **Header order is sorted, not published order**, and a binary header value is
    refused outright unless you choose an enveloping metadata mode. A caller
    header in the `Nats-` namespace is refused too.
11. **A memory-backed stream withdraws the durability claims.** That is the
    point — but a program that did not require them will run happily on one and
    lose everything on the next restart.
12. **Durable consumers are not cleaned up** unless
    `Config.ConsumerInactiveThreshold` is set. A test suite that opens a
    subscription per run leaves a durable per run behind.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

| Test | What it holds |
|---|---|
| `TestStaleAckIsRefused` | the disqualifier: acking an expired delivery reports `ErrLeaseLost` and the redelivery is still settleable |
| `TestStaleAckWouldOtherwiseSettleTheRedelivery` | the same ack made through `Delivery.Native()` destroys the record — the hazard is the server's, not a hypothesis |
| `TestSupersededDeliveriesAreNotHandedOut` | a delivery the driver knows is dead never reaches a handler |
| `TestExtendAfterTheLeaseIsLostIsRefused` | the lease guard: no handler keeps working on a delivery someone else owns |
| `TestExtendLongerThanAckWaitIsRefused` | an extension `+WPI` cannot grant is refused, and the amount the lease scheduler asks for is not |
| `TestDeadLetterPublishFailureNeverTerminates` | the ORDER: a failed dead-letter publication settles nothing and the message is still redeliverable |
| `TestRejectPublishesThenTerminates` | the successful route, its three reserved headers, and an original that does not come back |
| `TestRejectWithoutADeadLetterDestinationIsRefused` | the refusal, with the delivery still settleable |
| `TestAnUndecodableMessageIsDeadLettered` | under `Prefetch: 1` an undecodable message lands on the dead-letter subject as a raw copy with its reason, the original does not come back, the message behind it arrives and health stays `Ready` |
| `TestAnUndecodableMessageIsNotRedeliveredInAHotLoop` | with no dead-letter topic an undecodable message comes back once per lease instead of thousands of times a second, and the rest of the prefetch carries the messages behind it |
| `TestCloseNaksWhatItHeld` / `TestSettlementsAfterSourceCloseAreRefused` | a restart resumes instead of waiting out a lease |
| `TestCloseRacesDeliveries` | 1,500 rounds of Close against arriving deliveries under `-race`, which is the playbook's recurring defect class applied to a driver |
| `TestDoubleNakIsNotABackgroundFailure` | a clean Close does not degrade the client's health, and a nak that failed for another reason still does |
| `TestConsumerShapeIsThePlansShape` | the server's own view of the durable: pull, explicit, `MaxAckPending` = prefetch, one filter subject |
| `TestCapabilityClaimsFollowTheStream` | the version matrix from one server: the claims flip with the stream's flags, in both directions |
| `TestMessageTTLIsRefusedWithoutTheStreamFlag` / `TestScheduledPublicationIsRefusedWithoutTheStreamFlag` | a withdrawn claim refuses the control before any I/O |
| `TestScheduledPublicationIsNotDeliveredEarly` | a schedule keeps its fraction of a second, so it does not fire early |
| `TestPublishToASubjectNoStreamCapturesIsNotFound` | under `ConfirmNone`, which is the only case that can notice |
| `TestPatternReachingTheScheduleTreeIsRefused` | a consumer never receives the driver's own control messages |
| `TestAsyncCallbackRunsExactlyOnce` | clause by clause, including the goroutine count |
| `TestAsyncPublicationCopiesTheBody` | on the one path where the library keeps the message |
| `TestAsyncPublicationWhosePubAckNeverComesIsAmbiguous` | a PubAck that never comes is ambiguous after `PublishTimeout` and gives its place back, and a callback that publishes again does not deadlock |
| `TestAsyncPublicationsAReconnectionDroppedAreUnknown` / `TestAsyncPublicationStalledAcrossAReconnectionIsUnknown` | a publication a reconnection dropped, one stalled on the window included, is `CodeSettlementUnknown` at the reconnection and no longer holds `Flush` |
| `TestAsyncPublicationTheLibraryKeptIsLeftToItsPubAck` | the reconnection answers only for what the library dropped, so a publication made after the drop keeps its own outcome |
| `TestAsyncPublicationAReconnectionDroppedResolvesOnce` / `TestAsyncCallbackResolvedAtAReconnectionMayPublishAgain` | exactly once against the library's own callback at the close and the sink's, and a callback that publishes again from the reconnection does not deadlock |
| `TestBorrowedConnectionSurvivesClose` | explicit ownership, both ways |
| `TestDecodeDropsTheServersOwnHeaders` | the scheduler's headers are not the application's metadata |
| `FuzzNATSStaleAck` | the guard's invariant over every interleaving, with a two-map model |
| `FuzzJetStreamMetadata` | everything `Representable` accepts survives nats.go's own header decoder, and nothing reaches the `Nats-` namespace |
| `FuzzJetStreamSubjectMapping` | no input produces a subject that breaks the protocol, a pattern that publishes, or a durable JetStream refuses |
| `FuzzJetStreamDSN` | a DSN the parser accepted is a configuration `Validate` accepts and renders without its credential, and its rendering parses back to the same resolved settings, field by field (a rendering past the 8 KiB bound excepted) |
