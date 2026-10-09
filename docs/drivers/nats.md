# nats

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 1 · **Module:** `github.com/iqhive/iqmq/drivers/nats` · **Profile:** `core`

## Client & version policy

The native baselines for this driver live in `benchmarks/native-baselines/` and measure the clients below against the harness's `nats` image (`make bench-native CLIENTS="nats"`). A driver number is only comparable with a baseline taken from the same client version, broker version and confirmation boundary, so the pins move together; each baseline's package comment records the settlement boundary it measures as well.

| Baseline | Client | Pinned version | Confirmation boundary |
|---|---|---|---|
| `nats` | github.com/nats-io/nats.go | `github.com/nats-io/nats.go@v1.54.0` | core NATS at-most-once: server round-trip via Flush, no store |

### Verified facts (the API contract)

Checked while building the native baselines; re-verify when a pin moves.

- **delivery guarantee.** Core NATS is at-most-once with no store and no per-message acknowledgement: a publish is only known to have reached the server after a round trip (Conn.Flush), and a subscription that is not connected when a message is published never sees it.
  _Verified against: nats.go v1.53.1 nats.go Conn.Flush; NATS core documentation._
- **max payload.** The server advertises its max_payload in the INFO it sends on connect and the client rejects larger publishes locally with ErrMaxPayload; the default is 1 MiB, so payload limits are a server setting, not a client constant.
  _Verified against: nats.go v1.53.1 nats.go (Conn.MaxPayload, ErrMaxPayload); nats-server default._

## Topic / subscription mapping

| iqmq | NATS Core | Notes |
|---|---|---|
| `Topic` | subject | `/` becomes `.`; `Config.SubjectPrefix` is prepended as a token |
| `ConsumerConfig.Subscription` | queue group | two consumers of one subscription compete; two subscriptions each receive everything |
| `ConsumerConfig.Topic` (pattern) | subject wildcard | `*` matches one token, a trailing `>` matches the rest |
| `Message.Key` | `iqmq-key` header | base64, because a NATS header value is a text line |
| `Properties.ReplyTo` | `Msg.Reply` | the native reply subject, which is what makes request/reply work |

The subject alphabet this driver will build or accept is letters, digits, `_`,
`-` and `$`, with `.` as the separator. That is the image of the portable
topic grammar under the separator swap, plus the `$` that NATS system subjects
use. Anything else — a space, a control byte, a quote, a byte above ASCII — is
refused **before any I/O**, because a malformed subject does not produce a
rejected publication on this protocol: it produces a malformed frame on a
connection every other publisher and subscriber in the process is sharing.

**The mapping is not injective.** `orders/created` and `orders.created` are one
subject, and no reverse can tell which one was published, so
`DeliveryInfo.Topic` reports the subject the message really arrived on with the
prefix stripped and the separator left as `.`. A deployment
that needs the two to stay distinct uses one spelling.

A publication's topic may never be a pattern: `orders.*` and `orders.>` are
refused when the publisher is constructed. A consumer's topic may be one, and
that is what `FeatureWildcardSubscribe` means here — `ConsumerConfig.Topic`
documents that a driver supporting it accepts its pattern syntax there.

There is no topology to create and none to delete. A subject exists because
somebody published to it, which is why this driver implements no
`AdminBackend` and `Client.Admin` answers `ErrUnsupported`, and why
`topology/no-hidden-creation` is reported as inapplicable rather than passed.

## Publish path & confirmation boundary

`Publish` maps the message, writes it into the connection's write buffer with
`PublishMsg`, and then applies the confirmation boundary:

| Mode | What it waits for |
|---|---|
| `ConfirmNone` | the publication is in the client's write buffer; nothing has left the process |
| `ConfirmDefault` / `ConfirmAccepted` | `FlushWithContext` — a PING/PONG round trip, so the server has parsed every byte written before it |
| `ConfirmDurable` | **refused when the publisher is constructed** |

That is the whole of the confirmation this profile can offer: the server has
seen the message and has already forgotten it. `ConfirmDurable` fails at
construction rather than at the first message, which is the rule, because a
publisher that discovered it later would already have told the application it
was safe to forget the data.

The default therefore costs **one round trip per publication**. A caller who
wants throughput asks for `ConfirmNone`, or publishes a batch: `PublishBatch`
writes every message and confirms all of them with a single flush, so a batch of
a hundred costs one round trip instead of a hundred. That batch is the driver's
own and is not atomic — NATS has no batch verb — which is why
`FeatureBatchPublish` is advertised as `EmulatedVolatile` and not `Native`.
Its result is index-aligned with the batch.

`FlushWithContext` **requires** a deadline. When the caller's context has none,
`Config.FlushTimeout` (10s by default) supplies one; when the caller has a
deadline, the caller's is used unchanged.

Per-call publication controls this broker cannot honour are refused before any
I/O with `ErrUnsupported`: `Delay`, `DeliverAt`, `TTL`, `Priority`, `DedupKey`
and `Partition`. Ignoring them is the dangerous alternative — a caller who asked
for a delay and got none has no way to find out, and the message is already
delivered.

`Message` is not retained past `Publish`: header names and values are copied
into the `nats.Header` and the payload is serialised into the write buffer by
the call, so a caller may reuse its buffers the instant it returns.

## Consume path

`OpenSource` opens one `QueueSubscribe` on the mapped subject with the
subscription name as the queue group, and wires the callback into a
`driverkit.PushAdapter` whose capacity is the consumer's prefetch.

NATS delivers by calling the driver and the SPI asks the driver to be called,
so the bounded ring between them is the whole adapter. `Fetch` is one blocking
wait and then a non-blocking drain of everything the ring holds, which is what
makes `FeatureBatchReceive` true; it allocates nothing per fetch.

Blocking in the callback is the backpressure, and on this broker it has a
consequence worth stating plainly: while the callback is parked, nats.go keeps
delivering into the subscription's pending queue, and once that queue passes
`SetPendingLimits` the client **drops** what it cannot deliver and reports a
slow consumer. On an at-most-once broker that is the contract — there is nowhere
to push back to — and it is why the health transition below matters more here
than it would on a broker with flow control.

`Config.PendingMessages` (64Ki) and `Config.PendingBytes` (64Mi) are the limits
every subscription is opened with.

Only `StartLatest` exists. The broker retains nothing, so `StartEarliest`,
`StartAt` and `StartAtSequence` are refused with `ErrUnsupported` when the
consumer is constructed rather than silently answered with `StartLatest`.

`Close` unsubscribes, wakes a parked `Fetch` with `ErrClosed` and drops whatever
the ring still held. There is nothing to requeue: a message this source holds is
one the server has already forgotten, so the specification's "requeue or abandon" can only be
abandon.

## Ack / nack / extend mapping

**There is none.** `Source.Settler()` returns nil, which is how AckNone
model is declared: the core answers every `Ack`, `Nack`, `Reject` and `Extend`
with `ErrNotSettleable` and never calls the driver.

| iqmq | NATS Core |
|---|---|
| `Delivery.Ack` | `ErrNotSettleable` |
| `Delivery.Nack` | `ErrNotSettleable` |
| `Delivery.Reject` | `ErrNotSettleable` |
| `Delivery.Extend` | `ErrNotSettleable` |
| `DeliveryInfo.Attempt` | `0` — the specification's "the broker does not count attempts" |
| `DeliveryInfo.LeaseExpires` | the zero time — the specification's "the broker grants no lease" |

A delivery is complete the moment the handler has it. A settler whose `Ack`
silently succeeded would be the worse lie of the two available: the caller would
believe a message was safely processed by a broker that had already forgotten
it, and nothing anywhere could tell the difference.

## Ordering

NATS Core delivers one subject's messages **to one subscriber** in the order one
publisher connection wrote them. That is the entire guarantee, and the three
ordering features are three names for it :

| Feature | Level | Condition |
|---|---|---|
| `OrderedGlobally` | conditional | one publisher connection to one subject, delivered to a single subscriber |
| `OrderedByKey` | conditional | there is no key; a key orders only in so far as its messages share a subject |
| `OrderedByPartition` | conditional | there are no partitions; a subject is the only partition there is |

Two things break it and both are ordinary: a **queue group with more than one
member** interleaves across its members, and **two publisher connections** are
not ordered against each other. A deployment that needs order keeps one
publisher connection per subject and one member per queue group — which is the
same trade every broker makes, stated here rather than implied.

## Delays

None. `Delay`, `DeliverAt` and `TTL` are refused with `ErrUnsupported` before
any I/O, and `FeatureScheduledPublish`, `FeatureMessageTTL` and
`FeatureDelayedRedelivery` are all advertised as unsupported.

Delayed redelivery would need a redelivery, and there is none. A caller who
needs any of the three uses `drivers/jetstream`.

## Dead-letter queue

None. There is no settlement, so there is no `Reject` to route and no attempt
count to exhaust; `FeatureDeadLetter` is unsupported.

The core's own dead-letter route (`WithDeadLetter` on a runner) works, and it
needs one thing said about it. It is a publish to another topic followed by a
settlement, and the settlement is a no-op here — so a dead-lettered message is
published to the destination and the original is neither redelivered nor
acknowledged, because it was never redeliverable in the first place.

What is not obvious is how the route is **reached**. `Nack` answers
`mq.ErrNotSettleable` and this broker keeps no attempt counter, so under the
default `OrderingRelaxed` the runner requeues by nack, the nack fails, and the
message is never declared exhausted: nothing is ever dead-lettered. Ask for
`mq.WithOrderingByKey(mq.OrderingStrict)`, which holds the retry in process and
makes the attempt count exact whatever the broker does.

Until the route did not work here at all: this driver's
`Representable` refused every header under `mq.ReservedHeaderPrefix`, which is
all five of the provenance headers the route stamps, and `Encode` dropped
them.

## Headers & limits

NATS headers are an HTTP-like block: `Name: value` per CRLF-terminated line.
That decides what can be carried, and the rules are about framing rather than
taste.

| Rule | Why |
|---|---|
| a name may not be empty, carry a colon, a space, a control byte or a byte above ASCII | the reader truncates at the colon and a CR or LF forges a second header |
| a value must be valid UTF-8 | the block is a text line and the reader is `net/textproto`'s |
| a value may carry no control byte | CR and LF forge a header; the rest do not survive |
| a value may not begin with a space or a tab | the reader eats those after the colon, so the value comes back shorter |

A header that breaks one of them is refused with `ErrUnsupported` under
`MetadataNativeStrict`, dropped under `MetadataLenientDrop` (and counted by
`Backend.DroppedMetadata`), and carried in the portable envelope under
`MetadataAdaptive` and `MetadataPortableEnvelope`. `FeatureBinaryHeaders` is
therefore unsupported and `FeatureNativeHeaders` and `FeatureDuplicateHeaders`
are native.

**Header order is not preserved.** `nats.Header` is a Go map, so the publisher's
ordering is gone by the time the client hands the message over; this driver
decodes the names **sorted**, which is deterministic where a bare map iteration
would give the same message a different order on every delivery.
Duplicate values of one name keep their order.

Everything the specification names that has no native field travels under the reserved
`iqmq-` prefix, which `Headers.Validate` keeps a caller out of and this driver
refuses independently :

`iqmq-key`, `iqmq-id`, `iqmq-content-type`, `iqmq-content-encoding`,
`iqmq-correlation-id`, `iqmq-causation-id`, `iqmq-created-at`,
`iqmq-empty-body`, and `iqmq-env`.

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

The last is the driver's own: nats.go builds every payload with
`make([]byte, n)`, so a zero-length payload always arrives as an empty non-nil
slice and the nil-versus-empty distinction would be lost in one direction.
Marking the rarer half keeps both, and means a message from a non-iqmq publisher
with no payload decodes as a nil body.

**Limits.** The only limit this driver knows is the server's own `max_payload`,
taken from the INFO on connect, and it is advertised as
`Limits.MaxMessageBytes`; a publication past it is refused with
`ErrMessageTooLarge` before any I/O. Every other limit is left unadvertised —
The specification forbids reading an unadvertised limit as zero, so "we do not know" is a
`Limit` whose `Known` is false. The driver's own bounds are a 1 KiB subject, 64
subject tokens, a 256-byte header name and an 8 KiB header value.

## Reconnect

The client library owns the dial loop, and this driver owns what it means:

- a lost link makes health `Degraded` with the reason (`DisconnectErrHandler`);
- a re-established link makes it `Ready` again and fires `Hooks.OnReconnect`
  with the attempt number (`ReconnectHandler`);
- a connection the server closed for good makes it `Closed`, which is terminal
  in `driverkit.Health`, so a late callback cannot report `Ready` afterwards;
- a **slow consumer** makes it `Degraded` with the subscription's subject and
  queue group in the error (`ErrorHandler`), and increments
  `Backend.SlowConsumers`.

That last one is the only place a dropped message surfaces at all: a slow
consumer is invisible to the consuming path, which simply stops seeing messages.
the specification's "background failures surface via hooks and health, not only logs" is not
a formality on this driver.

`Config.ReconnectWait`, `Config.MaxReconnects`, `Config.PingInterval` and
`Config.MaxPingsOut` configure the loop. Nothing is buffered across a
reconnection beyond the client's own reconnect buffer, whose overflow is
reported as `CodeBackpressure`.

## At-least-once window

**There is none, because there is no at-least-once.** NATS Core is at-most-once:
a message is routed to whatever is subscribed at the instant it arrives and is
then gone.

The failure mode of this driver is **loss**, not duplication:

- a subscriber that is not connected when a message is published never sees it;
- a subscriber that falls behind its pending limits has messages dropped by its
  own client;
- a handler that panics loses the message it was holding;
- a publication under `ConfirmNone` that was in the write buffer when the link
  went away was never sent.

A publication under `ConfirmAccepted` whose flush fails is **ambiguous** — the
bytes had already left, so the server may have taken them — and is reported that
way (`OpError.Ambiguous`), which is the rule and is the one place this
driver can duplicate: a retry of an ambiguous publication may deliver twice.

Anything that needs at-least-once uses `drivers/jetstream`.

## Config & DSN

```go
client, err := nats.Open(ctx, nats.Config{
        Addrs:           []string{"nats://broker-1:4222", "nats://broker-2:4222"},
        Name:            "orders-service",
        SubjectPrefix:   "acme",
        PendingMessages: 64 << 10,
        FlushTimeout:    10 * time.Second,
})
```

or from a DSN, once the driver is registered with a `dsn.Registry`:

```
nats://user:password@host-1:4222,host-2:4222/acme?creds=/etc/nats.creds&tls=true&pendingmessages=8192
```

| Parameter | Meaning |
|---|---|
| *path* | `SubjectPrefix` |
| `name` | connection name the server reports |
| `creds` | NATS credentials file |
| `token`, *userinfo* | token or user/password authentication |
| `tls`, `insecureskipverify` | TLS with the platform roots; skipping verification needs `tls=true` |
| `pendingmessages`, `pendingbytes` | per-subscription `SetPendingLimits` |
| `connecttimeout`, `flushtimeout`, `reconnectwait`, `maxreconnects` | the dial and confirmation bounds |
| `pinginterval`, `maxpingsout`, `draintimeout` | the connection's own liveness and drain |
| `noecho` | do not deliver a connection its own publications |

An unknown parameter is a construction error naming it, and
every credential is a `dsn.Secret`: `Config.String()`, `Config.LogValue()` and
`Config.DSN()` all render the redaction. `Config.Validate()` is pure — it
performs no I/O and never dials — so a typo is an immediate refusal rather than
a five-second timeout.

Authentication that needs a signing callback (NKey, a custom JWT handler), a
custom dialler or a websocket URL is reached through `OpenWithClient`: build the
`*nats.Conn` however the deployment needs and hand it over with an explicit
`driverkit.Ownership`, which says who closes it.

## Known gotchas

1. **`Settler()` is nil.** Every `Ack` returns `ErrNotSettleable`. Code written
   against an at-least-once driver compiles and runs and silently stops
   acknowledging. Check `Capabilities().Has(FeatureExplicitAck)`, or require it:
   `Requirements: []mq.Requirement{mq.RequireAtLeast(mq.FeatureExplicitAck, mq.Native)}`
   fails at construction here, which is the point.
2. **`MaxInFlight` stalls a runner.** Nothing is ever settled, so the in-flight
   count only rises. Leave it unset on this driver until the core's accounting
   treats a non-settleable delivery as terminal.
3. **The default confirmation costs a round trip per message.** `ConfirmNone` or
   `PublishBatch` is what a throughput path wants.
4. **`a/b` and `a.b` are the same subject.** Pick one spelling per deployment.
5. **Header order is sorted, not published order**, and a binary header value is
   refused outright unless you choose an enveloping metadata mode.
6. **A slow consumer loses messages and nothing on the consuming path says so.**
   Watch `Client.Health()` and `Backend.SlowConsumers()`; size
   `PendingMessages`/`PendingBytes` and the consumer prefetch together.
7. **A publisher bound to a wildcard is refused**, and a consumer subscribed to
   `>` receives the whole prefix. Both are deliberate.
8. **The reply subject of a `Request` is the request's own inbox**, so a
   `Properties.ReplyTo` set on a request message is ignored rather than
   honoured.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

| Test | What it holds |
|---|---|
| `TestAckNoneSettlementIsRefused` | every settlement verb reports `ErrNotSettleable`, and `Settled()` stays false |
| `TestSourceHasNoSettler` | the SPI half: the source declares it, rather than the core inferring it |
| `TestQueueGroupsCompete` / `TestFanOutAcrossSubscriptions` | one subscription competes, two subscriptions each receive |
| `TestWildcardSubscription` / `TestWildcardPublishRefused` | a pattern subscribes and reports the resolved subject; a pattern never publishes |
| `TestRequestReply` / `TestRequestWithNoResponderIsNotFound` | native correlation, and the immediate `CodeNotFound` that is its whole advantage |
| `TestRequestOverridesTheCallersReplyTo` | an answer never goes where the requester is not listening |
| `TestSlowConsumerDegradesHealth` | the only place a dropped message surfaces |
| `TestSourceFetchIsBoundedByPrefetch` | the ring is a bound, checked at the driver's own seam rather than through the core's |
| `TestBackendCloseUnblocksAFetchNobodyClosed` | the specification's "a backend closes what it opened" |
| `TestKeyAndBodyRoundTrip` / `TestNilVersusEmptyBody` | the observable distinctions over a protocol that has neither |
| `TestUnsupportedPublishControlsRefusedBeforeIO` | five controls refused, and nothing published |
| `TestBorrowedConnectionSurvivesClose` | explicit ownership, both ways |
| `TestDecodeIsDeterministic` | one message decodes to one header order, every time |
| `FuzzNATSSubjectMapping` | no input produces a subject that breaks the protocol or becomes a wildcard |
| `FuzzNATSHeaders` | everything `Representable` accepts survives nats.go's own header decoder byte for byte |
| `FuzzNATSDSN` | a DSN the parser accepted is a configuration `Validate` accepts and renders without its credential |
