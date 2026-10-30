# gcppubsub

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 2 · **Owner:** iqmq maintainers

## Client & version policy

The native baselines for this driver live in `benchmarks/native-baselines/` and measure the clients below against the harness's `pubsub` image (`make bench-native CLIENTS="pubsub"`). A driver number is only comparable with a baseline taken from the same client version, broker version and confirmation boundary, so the pins move together; each baseline's package comment records the settlement boundary it measures as well.

| Baseline | Client | Pinned version | Confirmation boundary |
|---|---|---|---|
| `pubsub` | cloud.google.com/go/pubsub/v2 | `cloud.google.com/go/pubsub/v2@v2.8.0` | PublishResult.Get: server-assigned message ID (emulator, not evidence for managed Pub/Sub) |

The driver itself is built on the same pin. `drivers/gcppubsub/v1compat` is the second arm, over the **deprecated** `cloud.google.com/go/pubsub@v1.51.1`; `DriverInfo.Profile` is the client generation (`v2` or `v1compat`) so a report or a log line always names the library that answered, and the two arms share one capability table (`TestBothArmsAnswerOneCapabilityTable`).

| Family | Arms | Where it is measured | Decision |
|---|---|---|---|
| | `v2` · `v1compat` | `drivers/gcppubsub/v1compat`, both arms against one in-process `pstest` service over one gRPC connection | **Fixed on `v2`.** The row's criterion is correctness (a gate, not a metric), maintenance (the v1 module is deprecated upstream) and a native comparison (the driver against the raw client, which is `benchmarks/native-baselines/pubsub` against the container). The measurement reports what each generation costs and does not choose. |

### Verified facts (the API contract)

Checked while building the native baselines; re-verify when a pin moves.

- **client generation.** cloud.google.com/go/pubsub/v2 replaces the v1 Topic and Subscription types with Publisher and Subscriber handles obtained from the client, and exposes the generated admin surface as client.TopicAdminClient and client.SubscriptionAdminClient.
  _Verified against: pubsub/v2 v2.8.0 doc.go, publisher.go:167, subscriber.go:58, pubsub.go:51-59._
- **settlement.** Ack and Nack must be called inside the Receive callback or flow control is not respected, and the exactly-once path is AckWithResult/NackWithResult, which return an AckResult carrying an AcknowledgeStatus instead of failing silently.
  _Verified against: pubsub/v2 v2.8.0 doc.go and message.go:29-44,102-120._
- **delivery attempts and ordering.** Message.DeliveryAttempt is only populated when the subscription has a dead-letter policy, and publishing with an ordering key fails unless Publisher.EnableMessageOrdering is set.
  _Verified against: pubsub/v2 v2.8.0 message.go:84-95 and publisher.go:80-81,215,239._
- **emulator evidence.** The emulator is not evidence for managed Pub/Sub: it does not implement exactly-once delivery, and its latency and quota behaviour are unrelated to the service, so emulator-based claims are marked as such and managed numbers are measured separately.
  _Verified against: conformance/harness/brokers.go PubSub; the performance budgets layer 4._

## Topic / subscription mapping

A topic maps to a topic **ID** by a pure function, and a (topic, subscription)
pair maps to a subscription **ID** by another. Both mappings are injective,
which the SQS driver's is not and documents as a gotcha: `~` is in the
service's identifier grammar and is not in `mq.ValidateTopic`'s, so no two
portable names can collide.

| Portable | Pub/Sub |
|---|---|
| topic `orders` | topic `<TopicPrefix>orders` |
| topic `orders/eu` | topic `<TopicPrefix>orders~eu` |
| topic that is already a resource name (`projects/p/topics/t`) | used verbatim |
| (topic `orders`, subscription `workers`) | subscription `<SubscriptionPrefix>orders~workers` |
| (topic, subscription) named in `Config.SubscriptionIDs` | that identifier, verbatim |

The subscription identifier carries **both** halves and it has to. A Pub/Sub
subscription names exactly one topic and has its own acknowledgement state, so
two topics sharing one subscription identifier would be one resource attached
to whichever topic created it first — and a consumer of the second topic would
silently read the first topic's messages. `Config.SubscriptionIDs`, keyed by
`"<topic>/<subscription>"`, is the escape hatch for a deployment whose
subscriptions are named by Terraform or by a naming policy.

An identifier the service's grammar would refuse, or one over its 255
characters, is a **construction** failure and not a surprise at the first
message. A topic that is already a fully qualified resource name is passed
through, which is reachable only through the SPI: `mq.ValidateTopic` refuses a
name with a `/` and a scheme in it, so a portable caller uses `Config.TopicPrefix`.
Both arms address such a name in the project it names. The v1 library can only
create a topic or subscription in the client's own project, so `v1compat`
refuses to create one qualified with another project.

Nothing is created unless `Config.AutoCreate` is set. The specification makes provisioning
opt-in, and a driver that created a subscription for a typo would leave a
backlog nobody reads and a bill nobody expects. A consumer of a subscription
that does not exist is refused at open with `CodeNotFound`, which
`mq.IsSubscriptionNotFound` recognises, the same way the SQS and SNS+SQS
drivers refuse it. Earlier versions opened it, and the consumer received
nothing for ever while its streaming pull failed behind a `Degraded` health.

## Publish path & confirmation boundary

`Publisher.Publish` hands the message to the client library's **bundler** and
returns a pending result; `PublishResult.Get` is the confirmation.

| Mode | What it means here | Extra round trips |
|---|---|---|
| `ConfirmNone` | the same boundary as the two below — there is no fire-and-forget publication in the client library, so there is nothing weaker to offer | none |
| `ConfirmAccepted` | `PublishResult.Get` returned a message identifier | none |
| `ConfirmDurable` | the same call: the service stores a message in at least two zones before it answers | none |

The receipt says which boundary the publication actually reached, and it is
always `ConfirmDurable` — the specification's "a driver that had to fall back says so here",
read in the direction where the driver did better than it was asked.
`PublishReceipt.MessageID` is the server-assigned identifier; `Partition`,
`Offset` and `Sequence` stay zero, because this broker has none and an invented
value is worse than an absent one.

**`Publish` waits on the calling goroutine. `PublishAsync` waits in a fixed
completion-worker pool.** The obvious implementation of the asynchronous path
on this library is `go func() { res.Get(ctx); done(err) }()`, which is a
goroutine per message — rule 3's prohibition, verbatim. So completions
nobody is waiting on go through `Config.CompletionWorkers` goroutines with a
queue bounded by `Config.CompletionQueue`, and a submission blocks when the
queue is full. A synchronous publication is not routed through the pool,
because the caller is already blocked: adding a hand-off, a channel and a
wake-up to a path that has a goroutine of its own by definition buys nothing.

`PublishBatch` hands every message to the bundler first and waits for every
result afterwards, so a batch is as few publish requests as the bundler decides
rather than `len(ms)` of them. A message the driver refuses before any I/O
fails at **its own index** and the rest of the batch is still published, which
is why the encoding happens in a first pass.

`Flush` waits for what the publisher holds. The library's `Flush` takes no
context, so the wait happens on one goroutine per **flush** — not per message —
and the caller's context bounds the call rather than the flush.

## Consume path

`Subscriber.Receive` delivers by **callback** and the SPI asks the driver to be
called instead. `driverkit.PushAdapter` is what makes the two meet:

- the callback pushes each delivery into a bounded ring and **blocks** while the
  ring is full;
- the library releases a message's flow-control slot when the callback
  **returns**, so a blocked callback is a stream that stops asking for more.
  That is the only backpressure a streaming pull has;
- `MaxOutstandingMessages` is the prefetch and so is the ring's capacity;
- `NumGoroutines` is **one**, always. A second stream is a second source of
  out-of-order delivery for an ordering key, and the ring behind the callback is
  what bounds concurrency instead.

Both bounds being the prefetch means the driver holds at most **twice** it:
`prefetch` deliveries in the ring and up to `prefetch` callbacks parked on the
push. That is stated rather than hidden, because it is the number an operator
sizing memory needs. The alternative — making the callback wait for the
*settlement* — would hold one library goroutine per unsettled message for as
long as a handler runs.

`Fetch` **drops** a delivery the ring still holds that a later delivery of the
same message has superseded, or whose lease has run out while it waited. It is
not an optimisation: handing out a delivery whose settlement is certain to be
refused gives the caller work it cannot complete, while the message itself is
either one slot along in the ring or back at the service with a full deadline.
The callback drops a message whose lease has already run out by the time it is
handed over (see "The local lease check"), rather than pushing it.

A message the driver cannot decode is **nacked**, not acknowledged, and the
failure is reported through `Health` because there is no delivery to attach it
to. On a subscription with a dead-letter policy the nack is sent at once: the
service counts the deliveries and takes the corrupt record to the dead-letter
topic after `MaxDeliveryAttempts`. Without a policy nothing would ever end it,
and a nack at once had the service redeliver it straight back, over and over,
as fast as the stream could carry it. So the driver holds it for one
acknowledgement deadline first, on the same in-process hold a delayed `Nack`
uses, and it comes back once per deadline for as long as it exists. A held
message is unacknowledged at the service, so it takes one slot of the prefetch
while it waits: under `Prefetch: 1` nothing behind it is delivered in that
time. Give the subscription a dead-letter policy if your producers can write
messages this consumer cannot read.

`Config.Filter` becomes the subscription's server-side filter. The service
evaluates it and acknowledges what it does not match, so a filtered message is
never delivered and never counted against the backlog. The grammar is the
service's and this driver does not parse it.

## Ack / nack / extend mapping

| Portable | Pub/Sub | Notes |
|---|---|---|
| `Ack` | `Message.Ack` | fire-and-forget: the library queues the acknowledgement identifier into a batched `Acknowledge` |
| `Ack` with `Config.ExactlyOnce` | `Message.AckWithResult` | the settlement has an **outcome** and `Ack` returns it. A refusal is final and not retryable: the library sends one settlement per message, so the delivery reports `ErrLeaseLost` from then on and the message comes back once its deadline passes |
| `AckBatch` | a loop | the library coalesces the identifiers itself, so there is no flush policy to choose (belongs to a driver whose settlement is its own round trip) |
| `Nack` | `Message.Nack` (a `ModifyAckDeadline` of zero) | the message is deliverable again at once |
| `Nack` with a delay | an **in-process hold** | emulated; see below |
| `Reject` (`NackActionDeadLetter`) | publish to `Config.DeadLetterTopic`, then acknowledge the original | see "Dead-letter queue" |
| `NackActionDrop` | refused with `ErrUnsupported` | a negative settlement cannot destroy a message on this broker |
| `Reject` **with a delay** | refused with `InvalidArgument` | a delay asks for a redelivery held back and a dead-letter settlement produces none: the message goes to another topic and the original is acknowledged. The specification refuses an option a driver cannot honour rather than ignoring it |
| `Extend` | nothing is sent | emulated; see below |
| `DeliveryAttempt` | `DeliveryInfo.Attempt` | **zero** without a dead-letter policy, because the service counts nothing and the specification forbids reading an unknown as a value |

### Why `Extend` sends nothing

The client library extends a held message's acknowledgement deadline
**automatically**, so a handler that needs longer than the deadline gets it
without asking. The driver tells the library to extend for the delivery's whole
lease: the longer of the acknowledgement deadline and `Config.MaxExtension`,
capped at 60 minutes. It has to, because streaming pull sets its own deadline
on each stream, which overrides the subscription's, and the library lets a
message go once its extension budget has passed. `Extend(by)` checks that `by`
fits inside what is **left** of that lease and moves the driver's own view of
it.

It sends no `ModifyAckDeadline` of its own, and it cannot: the acknowledgement
identifier that request needs is private to the client library's message type
and there is no accessor for it. The specification says "Extend via ModifyAckDeadline" for
this driver and that is not reachable through `Subscriber.Receive`; the
capability is therefore advertised as `EmulatedVolatile` with the emulation
named, rather than as a native call that does nothing. An extension beyond the
budget is **refused** with `ErrUnsupported`: a handler told its lease runs for
another minute, whose message is redelivered in ten seconds, is worse off than
one told the extension is not available.

### The local lease check

The service accepts an acknowledgement for an identifier whose message another
consumer now holds. There is no API that asks "do I still hold this?", the
acknowledgement does not fail, and the message is gone from the other
consumer's backlog. So the refusal happens in the driver, **before the call**:
every settlement compares the delivery's lease — as the driver recorded it at
the delivery, on the injected clock — against now, and answers `ErrLeaseLost`
when the lease has run out.

It is a **local** view and the driver says so. It cannot see a deadline the
service moved for its own reasons, and it deliberately refuses on a lease this
process believes has passed even when the service would still have accepted the
acknowledgement. Both directions of that trade are the safe one: the cost is a
redelivery, and the alternative is acknowledging somebody else's work.

The lease starts at the earliest moment the message can have reached the client
library, and `DeliveryInfo.ReceivedAt` reports that moment. The library starts
its extension budget when the streaming pull returns a message, and hands the
message to the callback only when its flow control has room, which a callback
blocked on a full ring holds. So a message can wait in the library with its
budget running. Earlier versions started the lease at the callback, so it
outlasted the library's extension by the length of that wait: the library let
go, the service gave the message to another consumer, and this delivery's
acknowledgement was accepted for that consumer's work. The driver cannot see
when the library pulled a message, so when a callback returned while the
library's flow control may have been holding a message back, it charges the
leases of the messages handed over next with the gaps between the callbacks of
the last two prefetches, which covers what can be ahead of a message in its
pull. Room is counted the way the library counts it: a slot of the prefetch,
and a message's own data against `Config.MaxOutstandingBytes`, held until the
callback returns, with a blocked callback with an ordering key counting as a
full flow control because the later messages of its key wait behind it.
"May have been holding a message back" means the flow control had no room for
a message as large as any of the last two prefetches within 50 ms before the
handover, or for one as large as the service takes within 1 ms. The answer is
the same for every message one return lets through, whatever its own size:
the library hands those over in order, but each callback runs on a goroutine
of its own and they reach the driver in any order. A message the library hands
over when nothing can have held it back gets its whole lease. A delivery
dropped for a lease that ran out goes back to the library with a nack while the
library can still be extending it, so that it holds neither the message nor
`Close`.

Earlier versions asked whether the largest message the service takes would fit
for the whole 50 ms. With `Config.MaxOutstandingBytes` at or below that, 10 MB,
any message in flight made the flow control look full, so a steady stream of
small messages was charged every gap between them. Once those gaps added up to
the lease, every message was dropped as expired and nacked as soon as it
arrived, and on a subscription with a dead-letter policy each nack counted as a
delivery attempt.

What the driver still cannot tell apart, and charges:

- **Messages too large to share `Config.MaxOutstandingBytes`.** When two
  messages of the recent largest size do not fit in it together, a callback
  returning leaves no room for the next one, so a stream of them arriving less
  than 50 ms apart is charged every gap. Under a large prefetch those gaps can
  add up to the lease, and such messages are then dropped and nacked as they
  arrive. Set `Config.MaxOutstandingBytes` to at least twice the largest
  message, or leave it at the library's default.
- **A stream faster than one message a millisecond** with
  `Config.MaxOutstandingBytes` at or below 10 MB is charged its gaps, and so is
  a message that arrives within 50 ms of one of them. A lease is shortened by
  the sum of the gaps charged over the last two to four prefetches of messages.

And one case the driver charges less than the wait: a message larger than any
of the last two prefetches, held back by the flow control, when the library
takes longer than 1 ms to hand it over and a smaller message handed over with
it reaches the driver first.

A second, sharper case is the same message delivered **twice** to one source,
because its deadline expired. Both acknowledgement identifiers are valid at the
service. The source marks the earlier delivery superseded the moment the later
one arrives, so a settlement for the earlier one reports `ErrLeaseLost` rather
than acknowledging the message the later delivery owns. It also **nacks** the
earlier delivery's message in the client library, which otherwise goes on
extending that identifier and holds `Close` for it (see below). A nack is the
one safe settlement there: the service has already made the message deliverable
again, which is how the later delivery exists. A settlement already in flight on
the earlier delivery is left to finish, and one that ends without settling the
message nacks it instead of handing the delivery back to the caller, even when
it ends at the moment the redelivery arrives. A delayed `Nack` that the
redelivery reaches before its hold starts nacks at once and reports
`ErrLeaseLost`, because the message has already come back.

### Close settles before it waits

`Close` ends every hold, wakes every parked callback, nacks what the ring still
holds and nacks what was handed out and never settled — and only **then**
cancels the receiver and waits for it. The order is not a preference: the client
library's shutdown waits for every message it has handed to the callback to be
settled (`messageIterator.stop` blocks on exactly that), so a `Close` that
waited for `Receive` first would deadlock for the whole automatic-extension
budget. Ten minutes on the default configuration.

A delivery a redelivery superseded is not among them, because its message went
back to the library when it was superseded. Before it did, nothing ever settled
that message, and `Close` after a redelivery took the whole budget too.

A delivery a settlement is working on when `Close` runs is left to that
settlement, and a settlement that then ends without settling the message, such
as a `Reject` whose dead-letter publication fails, nacks it instead of handing
the delivery back. Earlier versions handed it back after `Close` had passed it
over, nobody nacked its message, and `Close` waited for it for the whole
budget.

## Ordering

**Conditional on `Config.Ordering`**, and the condition is the client library's:
`EnableMessageOrdering` is a flag on the **publisher**, not on a publication, so
a publisher built without it refuses every message that carries an ordering key.

With it, `Message.Key` becomes the ordering key — verbatim when it is valid
UTF-8 of at most 1024 bytes, and **base64** otherwise — and the service
delivers one key's messages in publication order to one subscriber. A
subscription this driver creates gets `enableMessageOrdering` too, which the
service needs before it will deliver an ordered key in order.

Base64 rather than a refusal, because a Pub/Sub ordering key is a *string* and a
binary key cannot be one verbatim. The three answers were refusing the
publication, publishing it unordered, and encoding the key: refusing costs a
core round-trip scenario that is not about ordering at all, publishing it
unordered is the silent downgrade the specification forbids, and encoding keeps the
guarantee because base64 is injective. What encoding costs is that a *text* key
which happens to spell the base64 of some binary key shares that key's ordering
group, which can only **merge** two groups and never split one — the same
argument the SQS driver makes for hashing a key into a FIFO message group.

**Without `Config.Ordering` the key is still carried** (in the `iqmq-k`
attribute, so it round-trips) and publications are simply not ordered by it.

Nothing else is ordered. There are no partitions, so `OrderedByPartition` is
not a claim, and a topic has no global order: messages with different ordering
keys, or none, arrive in whatever order the service finds them in.

### The publisher that silently stops

When an ordered publication fails, the client library puts that key into a
**permanently failed** state and refuses every later publication of the same
key for the life of the publisher — reporting the pause only to those later
publications, never to the one that caused it. A driver that does not call
`ResumePublish` therefore has a publisher that has silently stopped carrying one
key while every other key still works, which is the message loss in slow
motion rather than an error anybody sees.

This driver resumes the key on **every** failure of an ordered publication, on
the synchronous path and in the completion pool, and it does so unconditionally
rather than only on the library's paused-key error — because the first failure
is a plain service error and by the time a paused-key error arrives, every
publication of that key since has already been refused.

## Delays

**There are none, in either direction, natively.**

- **No scheduled publication.** A message is deliverable as soon as the service
  has stored it. `Delay` and `DeliverAt` are refused with `ErrUnsupported`
  before any I/O rather than silently ignored, and `FeatureScheduledPublish` is
  not claimed.
- **No nack delay.** A nack is a `ModifyAckDeadline` of zero and the message
  becomes deliverable at once. A delayed negative settlement is therefore an
  **in-process hold**: the driver keeps the delivery unacknowledged while the
  library goes on extending its deadline, and nacks it when the delay elapses.

The hold is advertised as `EmulatedVolatile` and three things follow, which is
why it must be declared an emulation rather than claimed as native:

1. it does not survive a process death — a hold interrupted by a crash becomes a
   redelivery when the acknowledgement deadline expires, which is sooner than
   the delay asked for;
2. it is bounded by what is left of the delivery's lease, and a longer delay is
   refused before anything is held, because a hold that outlives the lease is a
   redelivery at a time nobody chose. That bound is what `Limits.MaxDelay`
   advertises;
3. it costs a timer and a goroutine per held delivery — bounded, because a
   source holds at most its prefetch, and the price of a delay on a broker that
   has none.

`Close` ends every hold at once. A hold that outlived its source would be a
goroutine and a timer nobody can stop, and a message held unacknowledged by a
source that no longer exists until its deadline expires — a
three-hundred-millisecond delay silently becoming the acknowledgement deadline.

## Dead-letter queue

**Two routes, both needing `Config.DeadLetterTopic`.**

The subscription's own dead-letter policy is **native** and needs no settlement
at all: the service moves a message it has delivered
`Config.MaxDeliveryAttempts` times to the dead-letter topic. The service's own
minimum for that number is **five**, which is why `Config.Validate` refuses a
policy below it.

An explicit `Reject` cannot wait for five deliveries, so it **publishes** the
message to the dead-letter topic and acknowledges the original only once that
publication is confirmed. The order is what makes it safe: a failure anywhere in
between leaves the original unacknowledged and the message is redelivered, which
is at-least-once with a possible duplicate in the dead-letter topic. A route
that acknowledged first and then failed to publish would destroy the message.

The copy is a publication, so its failure is classified the way a failed
`Publish` is. A wait that ended (the settlement's context, or the library's own
publish timeout) may still have left a stored copy. So may a transient answer
from the publish RPC: `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`,
`ABORTED`, `INTERNAL` or `UNKNOWN`. A deadline can always come after the
service stored the copy, a stream that broke mid-RPC can too, and the client
library retries the RPC on every one of those codes, so an earlier attempt may
have been stored. In each case `Reject` reports an **ambiguous** error, and a
second settlement of the delivery is refused with `ErrSettlementUnknown`
instead of publishing a second copy. The original stays unacknowledged, and
`Close` nacks it like any other delivery it holds. A refusal the service
answered, such as a dead-letter topic that does not exist or one the
credential may not publish to, is definite, and the delivery stays settleable.

A `Reject` on a client with **no** dead-letter topic configured is refused with
`ErrUnsupported` rather than destroying the message or requeuing it for ever,
and the delivery stays settleable.

The message is republished **unchanged**. The five provenance headers are the
*runner's* route (`mq.WithDeadLetter`) and not the topology's, so a driver that
stamped its own would put two different provenance schemes on one message.

The dead-letter policy is also what makes `DeliveryAttempt` present at all:
without one, `DeliveryInfo.Attempt` is zero and `Redelivered` is false, because
the service counts nothing.

## Headers & limits

A Pub/Sub attribute value is a **string**. The question for this driver is
what happens to a binary header value, a repeated header key, a key the
service's attribute-name grammar refuses, and the hundred-and-first attribute.

| Portable | Pub/Sub |
|---|---|
| `Message.Body` | `Message.Data`, verbatim — there is no binary-body policy on this driver |
| a nil or empty body | `Data` empty plus `iqmq-enc` = `nil` / `empty`; protobuf cannot tell an absent `bytes` field from a present empty one, and the specification can |
| `Message.Key` | `iqmq-k` (and `iqmq-kenc` = `b64` when the key is not text, `empty` when it is empty) |
| `Message.ID` | `iqmq-id` |
| `Properties.ContentType` … `CreatedAt` | `iqmq-ct`, `iqmq-ce`, `iqmq-cid`, `iqmq-cause`, `iqmq-reply`, `iqmq-ts` |
| one header | one attribute of the same name |
| an enveloped message | the whole message in `Data`, plus `iqmq-env` = `1` |

The specification reserves the `iqmq-` prefix and the service reserves `goog` for itself, so
a caller's header may be in neither. A collision is refused in **every**
metadata mode, because an envelope would carry it and the decoder would then
hand it back as a header the driver wrote.

### What each mode does

| Trigger | `NativeStrict` | `Adaptive` | `PortableEnvelope` | `LenientDrop` |
|---|---|---|---|---|
| a binary header value | refused | enveloped | enveloped | dropped |
| a repeated header key | refused | enveloped | enveloped | dropped |
| a header value over 1024 bytes | refused | enveloped | enveloped | dropped |
| more than 100 attributes | refused | enveloped | enveloped | dropped |
| a header key over **256** bytes | refused | **refused** | **refused** | dropped |
| a header key that is not valid UTF-8 | refused | **refused** | **refused** | dropped |
| a non-UTF-8 identifier or property | refused | **refused** | **refused** | **refused** |

The three bold rows are the interesting ones, and they are why the admission
bound asks `driverkit.Envelope.Check` rather than reproducing the decoder's
numbers. The service's attribute-name limit and the envelope decoder's
`MaxHeaderKey` are the **same** 256 bytes, so a longer key has no
representation in either half — and a driver that enveloped it anyway would
publish a record its own `Unwrap` refuses for ever: undeliverable, therefore
unsettleable, which is the disqualifier reached through a record the driver
made out of a publication it accepted. `FuzzPubSubAttributes` found the
identifier case in a third of a second.

### The limits this driver advertises

Read from the pinned client library and from the service's published quotas at
that version, and recorded here rather than in the portable API — which is what
The specification asks. Re-verified when the client pin moves.

| Limit | Value | Source |
|---|---|---|
| `MaxMessageBytes` | 10,000,000 | `pubsub.MaxPublishRequestBytes` |
| `MaxBatchMessages` | 1,000 | `pubsub.MaxPublishRequestCount` |
| `MaxBatchBytes` | 10,000,000 | the same publish-request limit |
| `MaxHeaderCount` | 100 − 11 reserved = 89 | the service's attribute count, less what this driver spends |
| `MaxHeaderBytes` | 1,024 | the service's attribute-value limit |
| `MaxDelay` | `Config.MaxExtension` (10 minutes by default) | what the in-process hold can keep the lease alive for |
| `MaxLease` | 600 s | the service's `ackDeadlineSeconds` ceiling |

What the mapping costs, measured by `TestAllocsMappingIsBoundedPerPublication`
and logged on every run: **1** allocation for a body-only publication (the
attribute map), **4** with a key and an identifier, and **10** with eight
sixteen-byte headers — an attribute value IS a string and the SPI's header
value is a `[]byte`, so the per-header conversion is the boundary's and not
this driver's to remove. A refused publication allocates **nothing** beyond
that map: the refusals are preallocated `errcode` values rather than
formatted strings.

Not advertised, because the driver does not know them: nothing else in the specification's
table. Two more numbers bound resource names rather than messages — an
identifier is 3 to 255 characters and an ordering key is 1024 bytes — and both
are checked before any I/O.

## Reconnect

**The client library owns it.** Pub/Sub is a gRPC service and the library
re-establishes its streaming pull and its publish channel on its own, with its
own backoff, so there is no dial loop here and no `driverkit.Reconnector`.

What the driver adds is the part the library does not do:

- a streaming pull that returns a non-retryable error is **restarted** after
  `receiveBackoff` (500 ms), so a subscription that has been deleted or a
  credential that has expired costs one attempt per interval rather than a spin,
  and a stream the service tears down by design is simply reopened;
- every such failure moves `Health` to `Degraded` and the next success moves it
  back to `Ready`. The specification makes that the only place a background failure can
  surface, because there is no call to return it from;
- the ordering-key resume, without which a reconnection that failed one ordered
  publication would silently stop that key for ever.

`Health` is deliberately weaker than a driver that owns its connection, and it
says so: a client that has never been asked to do anything reports `Ready`
because nothing has failed, not because anything has been proved. `Ping` is the
call that proves it — one `ListTopics` bounded to a single result, which
consumes no message and creates no topology.

## At-least-once window

**From the delivery to the acknowledgement.**

A delivery is invisible to other consumers of its subscription for its
acknowledgement deadline, and the client library extends that automatically
while a handler holds the message. The driver asks the library to extend for
the longer of the deadline and `Config.MaxExtension`, capped at 60 minutes, and
that window is what `DeliveryInfo.LeaseExpires` reports. Up to v1.0.8 the
driver reported a deadline longer than `Config.MaxExtension` without asking the
library for it, so the library let the message go at `Config.MaxExtension`
while the delivery still claimed it.

Past it, the service delivers the message again to somebody else — and the
first delivery's acknowledgement would then acknowledge a message another
consumer is working on, which is why the local lease check refuses it before the
call.

Nothing here makes a delivery exactly-once. `Config.ExactlyOnce` creates a
subscription with the service's `enableExactlyOnceDelivery` and settles through
`AckWithResult`, which means:

- an **acknowledged** message is never redelivered, and the acknowledgement has
  an outcome the driver reports rather than swallows;
- an **unacknowledged** message whose deadline expires is redelivered exactly as
  on any other subscription.

the specification's `ExactlyOnceDelivery` is a guarantee about delivery *multiplicity*, which
is the second thing, so this driver does not claim it however the subscription
is configured. Setting `Config.ExactlyOnce` is still worth it, for the
settlement outcome.

## Config & DSN

```go
client, err := gcppubsub.Open(ctx, gcppubsub.Config{
    ProjectID:   "my-project",
    Ordering:    true,
    AckDeadline: 30 * time.Second,
})
```

```text
gcppubsub://my-project?ordering=true&ackdeadline=30s
gcppubsub://127.0.0.1:8085/my-project?insecuretransport=true&autocreate=true
```

The authority is the **endpoint** when the DSN names one and the **project**
when it does not, which is the one shape that makes both forms readable: a DSN
with an endpoint puts the project in the path (how the emulator is reached), and
a DSN with no endpoint puts the project in the host, because a client with no
endpoint talks to the service's own and the only thing left to name is the
project.

The authority names **one** thing. A second host is refused, a project has no
port (`gcppubsub://my-project:8085` is refused; an endpoint names its project
in the path), and a project is read as written: `gcppubsub://[example.com:my-project]`
and `gcppubsub://example.com%3Amy-project` are both the domain-scoped project
`example.com:my-project`. An endpoint host that decodes to `@`, `/`, `?`, `#`,
a backslash, a space or a control character is refused, because gRPC would
dial somewhere else (`a%3Fb:8085` becomes `dns:///a`). The `project` and
`endpoint` parameters are the other spelling of those two settings, and a DSN
that names one in both places is refused rather than resolved:
`gcppubsub://emulator-host?project=p` used to keep the parameter, drop the
emulator and dial the managed service.

`Config.DSN()` renders a configuration so that it parses back to itself, and
uses two more shapes when the two above cannot carry a value: a project the
authority would have to escape goes in the path of an empty authority, and an
endpoint that is not `host[:port]` goes in the `endpoint` parameter.

```text
gcppubsub:///example.com:my-project
gcppubsub://my-project?endpoint=https://pubsub.example:443
```

**The credential is not in the DSN.** A Pub/Sub credential is a service-account
key — a JSON document with a private key in it — and a DSN is a string that ends
up in logs, environment listings and process tables. `credentialsfile` names a
path, which is what a deployment has; `Config.CredentialsJSON` is reachable only
from Go, and `OpenWithClient` is the way in for everything else — workload
identity federation, an impersonated service account, a token source of the
deployment's own.

Both credential fields are **service-account keys and only that**. The client
library's untyped options (`option.WithCredentialsJSON`,
`option.WithCredentialsFile`) accept any credential configuration, including an
externally-sourced one whose URLs decide where a token is fetched from, and are
deprecated for exactly that reason; this driver uses the typed form with the
type pinned to `ServiceAccount`, so a configuration from an untrusted source is
refused rather than dialled.

| Parameter | Field |
|---|---|
| `project`, `endpoint`, `credentialsfile` | the same |
| `insecuretransport` | `InsecureTransport` — plaintext gRPC and no credentials, which is what an emulator needs and a real endpoint must never be given |
| `ordering`, `exactlyonce`, `retainacked`, `autocreate`, `strictackdeadline`, `compression` | the same flags |
| `ackdeadline`, `maxextension`, `retention`, `apitimeout`, `publishdelay`, `publishtimeout` | the same durations |
| `prefetch`, `maxoutstandingbytes`, `publishcount`, `publishbytes`, `completionworkers`, `completionqueue`, `maxdeliveryattempts` | the same counts |
| `topicprefix`, `subscriptionprefix`, `filter`, `deadlettertopic` | the same |

Every parameter is read exactly once and anything left over is an **error**, so
a typo is a refusal rather than a setting that silently did nothing.
`Config.Validate` is pure — no clock, no environment, no network — and
`Config.String`, `GoString`, `LogValue` and `DSN` all redact.

## Known gotchas

**The managed service's acknowledgement deadline is 10 to 600 seconds; the
emulator accepts one.** `Config.StrictAckDeadline` refuses a deadline outside
the managed range before any I/O, and it is **off** by default: several 
scenarios ask for five hundred milliseconds and the emulator grants it, so a
driver that refused locally would report "unsupported" for a topology the broker
under test builds happily. On the managed service the range is enforced by the
service, with an `InvalidArgument` this driver maps and reports. Turn the flag on
to find out at construction instead. **A conformance report made against the
emulator therefore proves the lease scenarios in a configuration the managed
service would refuse**, which is what `EvidenceEmulator` means.

**A handler may hold a delivery for the longer of the acknowledgement deadline
and `Config.MaxExtension`, whose default is ten minutes.** The library keeps
extending for that long, so a handler that hangs holds its message for the
whole of it and no other consumer can take it. Lower both for a consumer whose
handlers should be fast.

**A subscription created after a publication never sees that publication.** A
Pub/Sub backlog begins at the subscription's own creation, however long the
retention is. That is why the driver does not claim `Replay`, and why the
conformance harness provisions every subscription a scenario names *before*
the scenario publishes: several scenarios publish before they open a consumer,
and on this broker every one of them would otherwise look like a driver that
drops publications.

**`StartEarliest` on a new subscription is refused.** A consumer that asks for
the earliest position on a subscription that does not exist yet is asking for
the topic's history, which that subscription can never read, so `OpenSource`
answers `ErrUnsupported` before `Config.AutoCreate` creates it. Earlier
versions created it and read from its creation, which is `StartLatest` under
another name. On a subscription that exists, `StartEarliest` leaves its backlog
where it is, as any start position other than `StartAt` does. To re-read what
an existing subscription has acknowledged, set `Config.RetainAcked` and open it
with `StartAt`, which is a seek. Reading a topic's history from a new
subscription needs topic message retention and a seek to a time, which this
driver does not configure.

**Deleting a topic leaves its subscriptions.** They are detached, with the
backlog they already had. `Admin.DeleteTopic` does not delete subscriptions it
did not create, and the specification's "everything in it" is therefore not quite what
happens.

**`Admin.Stats` returns `ErrUnsupported`.** Pub/Sub does not answer the question
through its own API: the undelivered-message count and the
oldest-unacknowledged-message age are Cloud Monitoring metrics, which is a
different service, a different credential scope and a sampling delay of minutes.
the rule that an unknown is never an overloaded zero makes the refusal the
honest answer, because a caller cannot tell an empty subscription from one
nobody can measure.

**`Admin.Purge` is a seek forward.** It needs no retained messages — seeking
forward is always available — which is why it is native while seeking *backwards*
is conditional on `Config.RetainAcked`.

**A topic that is already a resource name reaches the driver only through the
SPI.** `mq.ValidateTopic` refuses a name with a `/` and a scheme in it, so
`Client.Publisher(… Topic: "projects/p/topics/t")` fails in the core.
`Config.TopicPrefix` and `Config.SubscriptionIDs` are the reachable forms. This
is the recorded finding shared with `drivers/sqs` and `drivers/snssqs`.

**The client library's stream keep-alive handler outlives a closed source by up
to ten seconds.** It exits on its own tickers rather than on the iterator's stop
signal. It is a shutdown lag rather than a leak — one goroutine per closed
source, bounded, and it does exit — and this repository's goleak configurations
ignore it by name.

**Tracing is not wired.** The client library can emit OpenTelemetry spans and
this driver turns them off (`option.WithTelemetryDisabled` on an insecure
endpoint, and no tracer provider otherwise): `integrations/otel` owns the
semantic conventions and the span names.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

`drivers/gcppubsub/critical_test.go`, each watched failing against a deliberate
mutation of the production code before it was trusted:

| Test | The failure mode |
|---|---|
| `TestCriticalOrderingKeyIsResumedAfterAFailedPublication` | the real client library over `pstest`, with a `PermissionDenied` injected into the publish-response queue: a publisher that is not resumed silently stops carrying one key |
| `TestCriticalOrderingKeyPauseIsRealInTheFake` | the scripted fake's own pause contract, so it is usable as an oracle |
| `TestCriticalDelayedNackIsAnInProcessHold` | both directions of the hold: not deliverable during it, deliverable after it |
| `TestCriticalDelayedNackHoldEndsOnClose` | the hold racing `Close` for three thousand rounds under `-race` |
| `TestCriticalCloseWithUnsettledDeliveriesDoesNotHang` | the library's shutdown waits for settlement, so `Close` must settle before it waits |
| `TestCriticalASupersededDeliveryDoesNotHoldClose` | the same wait reached through a redelivery, with `pstest`'s clock moved past the deadline: the superseded delivery's message has to go back to the library |
| `TestCriticalStaleSettlementAfterRedeliveryIsRefused` | the local lease check |
| `TestCriticalCompletionsDoNotCostAGoroutinePerMessage` | the fixed completion pool against a goroutine per message, with the fake's results gated so a thousand publications are outstanding at once |
| `TestCriticalRejectReachesTheDeadLetterTopicBeforeAcknowledging` | republish before acknowledge, proved by making the republication fail |
| `TestCriticalRejectWithNoDestinationIsRefused` | the refusal with nowhere to send the message |
| `TestCriticalRejectRefusesADelay` | on the one combination whose halves are both supported and whose combination means nothing |

`drivers/gcppubsub/ambiguity_test.go` covers the and the boundary: a
publication that fails with a transient code, a `Reject`'s copy to the
dead-letter topic included, is **ambiguous** (the service answers after it has
read the request), a settlement that fails is **not** (the
message is still unacknowledged, so it comes back as a redelivery; the refused
settlement was the last one the client library sends for that delivery, which
reports `ErrLeaseLost` afterwards), and a settlement past the lease is refused
**before the call**, which the message still being outstanding at the service
afterwards proves.

`TestConformance` in the driver's own module runs the suite against an
endpoint named by `IQMQ_PUBSUB_ADDR`, for somebody who has one and does not
want the conformance module, its testcontainers dependency or Docker. Set
`IQMQ_PUBSUB_MANAGED` for a run against a real project: it reads the evidence
as `managed`, which drops the emulator-only skips and makes
`limits/advertised` strict.

`drivers/gcppubsub/v1compat` adds the specification's `TestModel` (one scripted
sequence through both arms), `TestBothArmsAnswerOneCapabilityTable` and
`TestTheArmsAreWireCompatible` (publish through each generation, consume through
the other).

`FuzzPubSubAttributes` and `FuzzPubSubErrorMapping` carry committed regression
inputs; `testdata/fuzz/FuzzPubSubAttributes.README` says what each one reaches.
