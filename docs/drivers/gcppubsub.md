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

### Verified facts

Checked while building the native baselines; re-verify when a pin moves.

- **client generation.** cloud.google.com/go/pubsub/v2 replaces the v1 Topic and Subscription types with Publisher and Subscriber handles obtained from the client, and exposes the generated admin surface as client.TopicAdminClient and client.SubscriptionAdminClient.
  _Verified against: pubsub/v2 v2.8.0 doc.go, publisher.go:167, subscriber.go:58, pubsub.go:51-59._
- **settlement.** Ack and Nack must be called inside the Receive callback or flow control is not respected, and the exactly-once path is AckWithResult/NackWithResult, which return an AckResult carrying an AcknowledgeStatus instead of failing silently.
  _Verified against: pubsub/v2 v2.8.0 doc.go and message.go:29-44,102-120._
- **delivery attempts and ordering.** Message.DeliveryAttempt is only populated when the subscription has a dead-letter policy, and publishing with an ordering key fails unless Publisher.EnableMessageOrdering is set.
  _Verified against: pubsub/v2 v2.8.0 message.go:84-95 and publisher.go:80-81,215,239._
- **emulator evidence.** The emulator is not evidence for managed Pub/Sub: it does not implement exactly-once delivery, and its latency and quota behaviour are unrelated to the service, so emulator-based claims are marked as such and managed numbers are measured separately.
  _Verified against: conformance/harness/brokers.go PubSub; the performance budgets layer 4._
- **topic message retention and a seek before a subscription existed.** A topic's `messageRetentionDuration` (10 minutes to 31 days) keeps every message for that long whatever its subscriptions acknowledge, lets any attached subscription seek to a time up to that far back, and is what lets a subscription "replay messages that were published before you created the subscription". A seek to a time marks retained messages published before it acknowledged and those after it unacknowledged; a time before the retention window, or before the subscription's creation, marks only what is still retained. A seek is eventually consistent (up to a minute to take full effect), redelivers only what the subscription's filter matches, and on an exactly-once subscription fails the acknowledgements of deliveries made before it.
  _Verified against: [Topic.messageRetentionDuration](https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics), [SeekRequest.time](https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/seek), [Enable topic message retention](https://cloud.google.com/pubsub/docs/create-topic#enable_message_retention), [Replay and purge messages with seek](https://cloud.google.com/pubsub/docs/replay-overview), all read 2026-10-31._
- **what the managed service and the emulator did with it.** Measured on 2026-10-31 against project `iqhive-test` (message storage in australia-southeast1) and against the pinned emulator image. On both, a topic created with ten minutes of retention reports it from GetTopic, five minutes is refused with InvalidArgument, and a subscription created after three publications and sought to ten minutes ago (or to 40 days ago) receives all three, plus one published between its creation and the seek; a seek straight after CreateSubscription succeeded on the managed service three times out of three; a filtered subscription receives only what its filter matches; and on a topic with no retention the same seek receives nothing. On the emulator, a subscription with `retainAckedMessages` sought to before two messages it had acknowledged received both again. They differ in three ways. The managed service gives a new subscription on a retained topic nothing until it seeks, and the emulator gives it the retained messages without one. The managed service seeks an ordered subscription, and the emulator answers `Unimplemented` (Google lists it as a [known emulator limitation](https://cloud.google.com/pubsub/docs/emulator#known_limitations)). The managed service reports the topic's retention on the subscription too (`topicMessageRetentionDuration`), and the emulator does not, which is why the driver reads it from the topic.
  _Verified against: a probe built on pubsub/v2 v2.8.0; every resource it created was deleted afterwards._

## Topic / subscription mapping

A topic maps to a topic **ID** by a pure function, and a (topic, subscription)
pair maps to a subscription **ID** by another. The topic mapping is injective,
which the SQS driver's is not and documents as a gotcha: `~` is in the
service's identifier grammar and is not in `mq.ValidateTopic`'s, so no two
portable topics can collide.

The subscription mapping is not, across topics. A `/` in either half is
written `~` and the halves are joined with `~`, so (`orders/eu`, `billing`)
and (`orders`, `eu/billing`) are both `orders~eu~billing`. The identifiers
stay as they always were, because changing them would rename every
deployment's subscriptions. The service records the topic a subscription is
attached to, and the driver asks it instead: `EnsureSubscription`, a consumer's
open, `Purge` and `DeleteSubscription` refuse, with `CodeConflict`, a
subscription attached to a topic other than the one the call names. Of two such
pairs, the first to create the subscription owns it, and the other is refused
until one of the two takes another subscription name. Earlier versions asked
nothing: the second pair's consumer read the first topic's messages under its
own topic's name, and its `Purge` and `DeleteSubscription` acted on the first
pair's subscription. A detached subscription is not refused by `Purge` or
`DeleteSubscription`, because the topic that would tell two such pairs apart
is gone: once (`orders/eu`, `billing`)'s topic is deleted, a `Purge` or a
`DeleteSubscription` of (`orders`, `eu/billing`) acts on its subscription.

A `~` in a subscription name stands as it is, so `eu~billing` and `eu/billing`
of one topic name one subscription, `orders~eu~billing`, and consumers of the
two share its messages. That has been so since the first release, and the
service cannot tell the two apart either, so spell each subscription one way.

The topic check reads the subscription, which takes the
`pubsub.subscriptions.get` permission (in `roles/pubsub.viewer`,
`roles/pubsub.editor` and `roles/pubsub.admin`, not in
`roles/pubsub.subscriber`). `EnsureSubscription` of a subscription that
already exists, `Purge` and `DeleteSubscription` read it first; a credential
that may not read it is not refused for that, and the call goes ahead without
the check, as it did before there was one. A consumer's open has read the
subscription since the first release (to learn its exactly-once and
dead-letter settings), so it needs the permission as it always did, and its
topic check needs nothing more.

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
subscriptions are named by Terraform or by a naming policy. The key has the
same ambiguity (`orders/eu/billing` names both pairs above), and the same
topic check applies to an identifier it names.

`Admin().ListTopics` reports each topic under `Config.TopicPrefix` as the
portable topic that addresses it, with the prefix taken off and each `~`
written `/` again, and leaves out a topic this configuration did not name and
one whose ID reads back to no portable topic (`a~~b`, `a+b`, made by hand).
Earlier versions reported the topic ID as it stands, so under a prefix
`DeleteTopic` of a listed name prefixed it again, found nothing and answered
nil, and `iqmq topology apply` reported every topic it had ensured as not
visible.

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
is why the encoding happens in a first pass. With `Config.Ordering` the
exception is the rest of the refused message's ordering key: each later
message of the key gets a retryable not-attempted error and is not published,
so you can retry the key in order. The library pauses a key only when a
publication of it fails, and a refused message never reaches it; earlier
versions published the rest of the key, so the refused message, once fixed and
sent again, landed behind them.

**A publication whose context ends during the wait is still the library's.**
`Publish`, `PublishBatch` and `PublishOwned` return the ambiguous answer, and
the library's bundler still holds the message and may send it later. It holds
the body by reference, not as a copy, and the API lets a caller reuse its
buffer the moment `Publish` returns. So `Publish`, `PublishBatch` and
`PublishOwned` copy a body the library could still read after the call: a body
that is not an envelope, under a context that can end. That is one allocation
and one copy per message under most real contexts. A context that can never
end needs no copy, because the call then returns only once the library has
answered. `PublishOwned` still releases the message before it returns, on the
caller's goroutine, because a release may publish again and must never wait
on the completion pool. `PublishAsync` copies nothing, because the message is
borrowed until its callback runs. Earlier versions sent whatever the caller's
buffer held by the time the bundler read it, and `PublishOwned` handed the
storage back to its owner while the library still read it.

Under `Config.Ordering`, a `Publish`, `PublishBatch` or `PublishOwned` with an
ordering key and a context that can end takes one place in the completion
queue before the library sees it. A publication whose wait ends is still
counted against its key until the library answers it (see "The publisher that
silently stops" below), and that count is left to the completion pool. The
wait for the place is bounded by the context, as `PublishAsync`'s is: with the
queue full, the call returns its context's error, a definite answer for a
publication that was never sent. A `PublishBatch` takes one place for the
whole call; refused one, it answers each ordered message as never attempted
(the context's code, retryable) and still publishes the messages without a
key. A place that is not needed goes back when the call returns. Size
`Config.CompletionQueue` with these publications in mind: they compete for
places with `PublishAsync` and with each other.

`Flush` waits for what the publisher holds. The library's `Flush` takes no
context, so the wait happens on one goroutine per **flush**, not per message,
and the caller's context bounds the call rather than the flush. That goroutine
is the publisher's: `Close` waits for it before it stops the library's
publisher, because the library's `Flush` reads the publisher's stopped flag
without the lock `Stop` writes it under, and the two side by side are a data
race inside the library. `Stop` flushes anyway, so the wait costs `Close`
nothing. Nor does `Flush` call the library's before the publisher's first
publication has been handed over, which is the other race of the same kind.
A context that had ended before the call gets the context's error every time,
and the library's flush is still started, so an ended `Flush` still sends.

**A refusal after a retried attempt is ambiguous.** The client library repeats
a publish RPC on `DEADLINE_EXCEEDED`, `UNAVAILABLE`, `INTERNAL`,
`RESOURCE_EXHAUSTED`, `ABORTED`, `CANCELLED` and `UNKNOWN`, and the result
carries the last attempt's status alone. The service can store an attempt
whose answer is lost and then refuse the retry, because the topic was deleted,
an IAM binding was removed or a credential lapsed in between. On a client this
driver dials itself (`Open`, `OpenDSN`, `OpenBackend`, and `v1compat`'s own),
an interceptor on the client's connections recognises the attempts of one
call by their request, and a failure after a failed attempt is reported as
**ambiguous**, with the refusal's own code. A refusal of a first attempt stays
definite. Earlier versions reported the refusal after a stored attempt as
definite, and a caller that repeated it published the message twice.

A client you pass to `OpenWithClient` is yours to configure, and the driver
does not add the interceptor to it, so there a definite answer after a retried
attempt is still reported as definite. The driver cannot see the library's
attempts through its API. If you must reconcile that case, add the same kind
of interceptor to your client's connections, or watch the library's retries
in its debug logging.

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

A delivery whose settlement has finished keeps that settlement's answer after
its lease runs out: a second settlement reports `ErrAlreadySettled` once it was
settled and `ErrSettlementUnknown` once an exactly-once settlement was
ambiguous. An acknowledgement that did not land brings the message back once
its deadline passes, so that is the usual order. Earlier versions answered
`ErrLeaseLost` there and, once the client library had let the message go,
recorded the delivery as lost, over the sticky unknown answer.

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

This driver resumes the key after **every** failure of an ordered publication,
on the synchronous path and in the completion pool, and it does so
unconditionally rather than only on the library's paused-key error, because
the first failure is a plain service error and by the time a paused-key error
arrives, every publication of that key since has already been refused.

It resumes the key once the library has answered **every publication of the
key it held**, not when it reads the failure. The library refuses the
publications of a paused key that it holds only as each comes up to be sent,
and a key resumed before the next one comes up lets that one through while the
failed one is not stored: the caller's retry of the failure then lands behind
it. Earlier versions did that. Now each publication behind the failure gets
the paused-key error, which is retryable, so the caller retries the key in
order, and a publication whose caller stopped waiting counts until the library
has answered it. The cost falls on a publisher that keeps a failed key busy:
while some publication of the key is always outstanding, every publication of
it gets the paused-key error, until the publisher lets the key go quiet. A
caller that stops to retry the failure in order is that pause. A producer that
keeps one key saturated, such as a bulk replay, gets every publication of it
refused until it pauses, so it should stop that key at its first failure.

The dead-letter copy a `Reject` publishes is the exception. Its key is resumed
as soon as the failure is read, so a copy of the same key from a concurrent
rejection can still reach the dead-letter topic ahead of a retry of the failed
one.

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
   refused with `ErrUnsupported` before anything is held, because a hold that
   outlives the lease is a redelivery at a time nobody chose. The refusal
   settles nothing, so an immediate `Nack` still requeues the message, and
   `Run` holds the delivery for as much of the delay as its lease allows and
   then makes that requeue (see
   [A delay the driver cannot make](../reliability/poison-messages.md#a-delay-the-driver-cannot-make)). A handler that keeps
   asking for such a delay, or ran so long that little of the budget is left,
   sees the message again sooner than it asked. That bound is what
   `Limits.MaxDelay` advertises;
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

The publication can take as long as `Config.PublishTimeout`, and longer while
the client library retries it, so the lease is checked again once the copy is
confirmed. When the lease has run out meanwhile, or a redelivery has
superseded the delivery, the original is not acknowledged: the service accepts
an acknowledgement for an identifier whose message another delivery now
holds. `Reject` reports `ErrLeaseLost`, the message goes back to the client
library while the library can still be extending it (nothing is sent past its
budget), and the copy on the dead-letter topic is a duplicate. A `Reject`
whose copy is confirmed while `Close` runs still acknowledges the original.
Earlier versions acknowledged the original whatever had happened during the
publication and reported success.

Two narrow windows remain. A redelivery that supersedes the delivery and a
`Close` that then runs, both while the copy is published, look like `Close`
alone, so the original is acknowledged while its lease lasts. A redelivery that
arrives between the check and the acknowledgement is not seen either, as for a
plain `Ack` between its claim and its acknowledgement. Either way the message
is on the dead-letter topic, so nothing is lost.

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
credential may not publish to, is definite, and the delivery stays settleable,
unless it answered a retry after an attempt that failed, on a client the
driver dials itself: then the copy may be stored, and the `Reject` is
ambiguous (see [Publish path](#publish-path-confirmation-boundary)).

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

Whether a body is an envelope is the `iqmq-env` attribute's answer and never
the body's. A `MetadataAdaptive` consumer unwraps a marked body and delivers
any other as it was published, whatever it looks like; a
`MetadataPortableEnvelope` consumer unwraps every body; `MetadataNativeStrict`
and `MetadataLenientDrop` unwrap nothing, so a marked body arrives with its
envelope as its body. A `Reject` of such a delivery marks the dead-letter copy
again. Earlier versions of the `Adaptive` consumer decided from the body, so a
native publication whose payload was a whole envelope (a relay forwarding what
a `NativeStrict` consumer received, a replayed record, an upload published as
it came) was delivered as the message inside it, body, key and headers, or for
the layout the consumer does not decode was refused as undecodable and never
delivered.

The five dead-letter provenance headers (`iqmq-origin-topic`,
`iqmq-origin-subscription`, `iqmq-attempts`, `iqmq-failure`,
`iqmq-first-delivered-at`) are the exception: `mq.WithDeadLetter` writes them
on the copy it publishes, so they travel as attributes of the same name and
come back as headers. Earlier versions refused them like a caller's, so the
route's publication failed before any I/O and the runner requeued the message:
with in-process retries it looped for ever, and with a dead-letter policy it
reached the policy's topic after `Config.MaxDeliveryAttempts` deliveries with
no provenance. The route needs attempts it can count: a subscription with a
dead-letter policy, or in-process retries (`mq.WithOrderingByKey()`), since
without either the service reports no attempt and the runner never finds the
message exhausted. From v1.1.3 the core caps the failure text it writes in
`iqmq-failure`: valid UTF-8, at most 1,024 bytes cut at a character boundary,
and marked as cut. So a long or non-UTF-8 handler error fits an attribute
value.

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
the specification asks. Re-verified when the client pin moves.

| Limit | Value | Source |
|---|---|---|
| `MaxMessageBytes` | 10,000,000 | `pubsub.MaxPublishRequestBytes` |
| `MaxBatchMessages` | 1,000 | `pubsub.MaxPublishRequestCount` |
| `MaxBatchBytes` | 10,000,000 | the same publish-request limit |
| `MaxHeaderCount` | 100 − 11 reserved = 89 | the service's attribute count, less what this driver spends |
| `MaxHeaderBytes` | **unknown** | the service bounds each attribute (a 256-byte key, a 1,024-byte value) and the message, not the header set; the mapping applies the per-attribute limits in each mode. Earlier versions advertised the 1,024-byte value limit here, so a header set over 1,024 bytes in total, which the service takes, read as over the limit |
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

The specification's `ExactlyOnceDelivery` is a guarantee about delivery *multiplicity*, which
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
| `ackdeadline`, `maxextension`, `retention`, `topicretention`, `apitimeout`, `publishdelay`, `publishtimeout` | the same durations |
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

**A subscription created after a publication does not see it on its own.** A
Pub/Sub backlog begins at the subscription's own creation, whatever the topic
retains, and only a seek into the topic's message retention reaches further
back. That is why `Replay` is conditional rather than native, and why the
conformance harness provisions every subscription a scenario names *before*
the scenario publishes: several scenarios publish before they open a consumer,
and on this broker every one of them would otherwise look like a driver that
drops publications.

**`StartEarliest` on a new subscription seeks into the topic's retention, and
is refused without it.** A consumer that asks for the earliest position on a
subscription that does not exist yet is asking for the topic's history.
`OpenSource` reads the topic's `messageRetentionDuration` from the service, so
a topic provisioned elsewhere with retention works, and `Config.TopicRetention`
(`topicretention` in the DSN, ten minutes to 31 days) gives retention to the
topics this driver creates. With retention the open creates the subscription
(`Config.AutoCreate`) and seeks it straight away to 32 days before the wall
clock, earlier than any retention window the service allows. Google documents
a seek before the window as marking only what is still retained, and a seek to
40 days ago was measured giving the same result as one to ten minutes ago, so
no clock, the driver's own `Config.Clock` included, decides what the
subscription reads. The consumer then reads everything the topic still
retains, filtered by `Config.Filter`, including what was published before the
subscription existed. Without retention the open answers `ErrUnsupported`
before `Config.AutoCreate` creates anything, and the error names the setting.
Earlier versions created the subscription and read from its creation, which is
`StartLatest` under another name. On a subscription that exists,
`StartEarliest` leaves its backlog where it is, as any start position other
than `StartAt` does.

**`StartAt` needs something retained, and the topic's retention is read, not
assumed.** With `Config.RetainAcked` the subscription keeps its own
acknowledged messages and `StartAt` seeks over them, as before. Without it the
open reads the topic's own retention, as `StartEarliest` does: a topic with
retention takes the seek, and a topic without it is refused with
`ErrUnsupported`, whatever `Config.TopicRetention` says, because that setting
applies only to topics this driver creates.

**From the create to a successful seek, every failure is undone or reported
as a conflict.** A subscription created for `StartEarliest` and never sought
reads from its creation, and the next open at `StartEarliest` would find it and
accept it as an existing subscription. So the seek comes straight after the
create, before the open reads the subscription back or does anything else that
can fail. When the create fails in a way that may have landed all the same (a
transient code, a cancellation or a deadline that expired with the answer on
its way) or the seek fails, the open deletes the subscription on a context
that outlives a cancelled caller and returns the failure, and a retry starts
from nothing. A create the service refused, such as an `InvalidArgument` for a
filter it cannot parse, made nothing, and the open returns that refusal as it
is without deleting anything. When the deletion fails, or the
create answered `AlreadyExists` (the client library retries a create whose
first attempt may have landed, and another consumer may have made it at the
same moment, so the driver seeks it but will not delete it), or the
`AlreadyExists` was followed by a read of the subscription for the topic check
that failed, the open returns
`CodeConflict`, not retryable, for that subscription: it may exist without its
seek, so delete it, or open it with `StartAt`, before opening it at
`StartEarliest` again. A failure after the seek leaves a subscription whose
backlog is the history, which a retry reads. One path is not covered: a
process that dies between the create and the seek leaves a subscription that
reads from its creation.

**While a seek settles, a consumer can receive a message twice.** Google
describes delivery as eventually consistent with a seek, for up to a minute,
and nothing the seek marks unacknowledged is lost; but in that minute a
message can be delivered more than once, on an exactly-once subscription too.
The receiver starts only after the seek returns, so no delivery of the open
that sought predates it.

**Two consumers opening the same new subscription at once can re-seek it, or
delete it under each other.** Both find it missing. One creates it and the
other's create answers `AlreadyExists`, and both seek. The second seek marks
unacknowledged again whatever the first consumer acknowledged in between, so
that consumer receives those messages again: duplicates, not loss. If the
creator's seek fails, it deletes the subscription, which the other consumer may
already be reading. That consumer's streaming pull then fails with NotFound,
its health reports `Degraded` and it receives nothing until the subscription
exists again; a retry of the failed open re-creates and re-seeks it, and the
other consumer's pull then picks it up and receives the history again. If the
other consumer's own seek fails instead, its `CodeConflict` asks the caller to
delete a subscription the creator may be reading. Making either safe would
need the two opens to agree on who owns the subscription, which Pub/Sub has no
primitive for, so it is documented rather than handled. Open a new
subscription at `StartEarliest` from one process, or create it before the
consumers start. With `Config.AutoCreate` off the same race takes another
shape: a subscription another process creates between the open's first check
and its read of the subscription is sought back too, as one whose create
answered `AlreadyExists` is, so a consumer that provisioned it for
`StartLatest` receives the topic's history, and a failed seek there is a
`CodeConflict` about a subscription the open did not create.

**Topic retention costs storage and is not reconfigured.** The service bills
the retained messages to the topic's project for the whole window, whether
anything reads them. `Config.TopicRetention` applies only when this driver
creates the topic: a topic that already exists keeps the retention it has,
because `Admin.EnsureTopic` creates what is missing and reconfigures nothing.

**Topic retention needs a client arm that can read it.** The v2 arm can, and
so can `drivers/gcppubsub/v1compat` from the release that ships with this
driver. `v1compat` up to v1.1.1 cannot: it still compiles and works against
this driver, because the retention methods are a separate interface the
driver asserts rather than part of the one every arm implements, and over it
`Config.TopicRetention` is refused at open, and `StartEarliest` on a new
subscription and `StartAt` without `Config.RetainAcked` are refused at
`OpenSource`, each with `ErrUnsupported` naming the `v1compat` upgrade.

**The emulator hands a new subscription a retained topic's history without a
seek, and refuses a seek on an ordered subscription.** Google lists
configurable message retention among the emulator's known limitations, and the
first difference is the one that matters here. A Replay pass on the emulator
would also be a pass for a driver that created the subscription and never
sought, and giving its topics retention would make `start/positions` (which
checks that a `StartLatest` subscription created after a publication does not
receive it) fail for every driver. So the emulator run gives no topic
retention, keeps `start/positions`, and skips `capability/Replay/works` and
`capability/Replay/claim` with that reason; a run against a real project
gives the retained-message scenarios ten minutes of topic retention and drops
the skip. The seek itself is pinned by the scripted fake, which begins a
subscription at its creation as the managed service does
(`TestStartEarliestOnANewSubscriptionReadsWhatTheTopicRetains`, watched failing
with the seek target moved to now), and the behaviour it models was measured
against the managed service. The second difference is why the conformance
harness turns ordering off for the scenarios that ask for retained messages.
With ordering off, the emulator seeks an unordered subscription back over its
own acknowledged messages (measured: two acknowledged messages came back after
a seek to before their publication), so `capability/SeekByTime/works` runs on
the emulator. It used to be skipped as "the emulator does not implement Seek",
which was true only because every scenario ran ordered.

**Deleting a topic leaves its subscriptions.** They are detached, with the
backlog they already had. `Admin.DeleteTopic` does not delete subscriptions it
did not create, and the specification's "everything in it" is therefore not quite what
happens. A topic created again under the same name is not attached to them,
so nothing published to it reaches them. `EnsureSubscription` and a
consumer's open refuse a detached subscription with `CodeConflict`, saying it
is detached, and `Admin.DeleteSubscription` removes it, after which
`EnsureSubscription` creates it again on the new topic. Earlier versions took
the create's `AlreadyExists` as success: a deployment that deleted a topic and
provisioned it again got nil from `EnsureSubscription`, and its consumer opened
and received nothing, for ever. On the `v1compat` arm, reading a detached
subscription back panicked inside the v1 library.

**`Admin.Stats` returns `ErrUnsupported`.** Pub/Sub does not answer the question
through its own API: the undelivered-message count and the
oldest-unacknowledged-message age are Cloud Monitoring metrics, which is a
different service, a different credential scope and a sampling delay of minutes.
the rule that an unknown is never an overloaded zero makes the refusal the
honest answer, because a caller cannot tell an empty subscription from one
nobody can measure.

**`Admin.Purge` is a seek forward.** It needs no retained messages — seeking
forward is always available — which is why it is native while seeking *backwards*
is conditional on `Config.RetainAcked` or on topic message retention.

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
