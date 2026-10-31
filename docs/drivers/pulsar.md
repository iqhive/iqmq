# pulsar

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 2 · **Owner:** iqmq maintainers

## Client & version policy

`drivers/pulsar` is over [`github.com/apache/pulsar-client-go`](https://github.com/apache/pulsar-client-go),
pinned at `v0.21.0`. The transaction helper is a separate module,
[`x/pulsartx`](https://github.com/iqhive/iqmq/tree/main/x/pulsartx), so a
program that does not use transactions does not carry the coordinator client.
A commit whose reply was lost is `ErrSettlementUnknown`, and ambiguous. A
commit the client refuses without sending, because an enrolled send or
acknowledgement failed or the transaction timed out, is a definite
`CodeConflict` that is not retryable: the transaction can no longer end, and
the coordinator aborts it once its timeout passes.

The native baselines for this driver live in `benchmarks/native-baselines/` and
measure the clients below against the harness's `pulsar` image
(`make bench-native CLIENTS="pulsar"`). A driver number is only comparable with
a baseline taken from the same client version, broker version and confirmation
boundary, so the pins move together; each baseline's package comment records the
settlement boundary it measures as well.

| Baseline | Client | Pinned version | Confirmation boundary |
|---|---|---|---|
| `pulsar` | github.com/apache/pulsar-client-go | `github.com/apache/pulsar-client-go@v0.21.0` | broker send receipt (Producer.Send), persistent topic |

### Verified facts (the API contract)

Checked while building the driver and the native baselines; re-verify when a
pin moves. The first four are the facts `benchmarks/native-baselines`
records, quoted verbatim — `nativebench.TestFactsRecorded` holds this page to
them — and the rest are this increment's.

- **producer batching.** The Go client batches by default — 10 ms BatchingMaxPublishDelay, 1000 messages, 128 KiB — so a synchronous Send measures the batch delay unless batching is disabled; the baseline disables it for the per-message benchmarks and keeps it for the batched ones.
  _Verified against: pulsar-client-go v0.21.0 pulsar/producer.go:141-165._
  One broker **entry** therefore holds several messages, which is what makes the
  batch-index acknowledgement rules below matter.
- **message size.** The broker advertises its maximum message size during the connection handshake and the client falls back to the 5 MiB default when it does not, so payload limits are negotiated per connection rather than fixed by the client.
  _Verified against: pulsar-client-go v0.21.0 pulsar/internal/connection.go:337-342._
  The client surfaces the negotiated figure nowhere, so this driver reads
  `maxMessageSize` from the administrative API at open instead, and the
  broker's version for `DriverInfo.BrokerVersion` from
  `/admin/v2/brokers/version`, since the runtime configuration does not carry
  it.
- **retry topics and transactions.** Dead-letter and retry topics are a client-side policy (ConsumerOptions.DLQ, DLQPolicy), not a broker feature, and transactions are supported by the Go client but must be enabled on both the client and the broker.
  _Verified against: pulsar-client-go v0.21.0 pulsar/consumer.go:69-101,183-185; pulsar/client.go ClientOptions.EnableTransaction._
  A consumer with `RetryEnable` subscribes to **two** topics, and
  pulsar-client-go refuses `Seek` and `SeekByTime` on a multi-topic consumer.
  _Verified against: pulsar-client-go v0.21.0 pulsar/consumer_impl.go:139-199,745-786._
- **ordering.** Key_Shared subscriptions keep per-key order only while the consumer set is stable: a new consumer takes over hash ranges, and messages already delivered to the previous owner may be redelivered out of order relative to the new owner's, so ordering claims must be scoped to a stable subscription.
  _Verified against: Pulsar Key_Shared documentation._
  `AllowOutOfOrderDelivery=false` makes Pulsar BLOCK a reassigned range instead,
  which is what this driver sets. Adding partitions rehashes every key onto a
  different partition, which ends the promise whatever the subscription type.
  _Verified against: TestCriticalPartitionExpansion._
- **no client acknowledgement timeout.** v0.21.0 has **no** `AckTimeout`
  option — the field the Java client still has was removed — and Pulsar's broker
  redelivers an unacknowledged message only when the consumer disconnects. An
  unsettled delivery therefore never comes back while the consumer holds its
  connection. It is why `Config.AckTimeout` is this driver's own deadline and
  why `FeatureExtendLease` is `EmulatedVolatile`.
  _Verified against: `grep -r AckTimeout` over pulsar-client-go v0.21.0 finds nothing._
- **batch-index acknowledgement.** `ConsumerOptions.EnableBatchIndexAcknowledgment`
  makes the client put an acknowledgement **set** in the frame; the broker
  honours one only when its own `acknowledgmentAtBatchIndexLevelEnabled` is
  true, and Apache Pulsar's default is **false**. Without the option, the client
  holds an individual acknowledgement back until every index of the entry has
  been acknowledged.
  _Verified against: pulsar-client-go v0.21.0 pulsar/consumer_partition.go:549-595, pulsar/ack_grouping_tracker.go:148-183._
- **nil and empty bodies are distinguishable.** pulsar-client-go sets the
  message metadata's null-value flag for a nil payload and the consumer reports
  it, so the distinction survives without an envelope. It is the only broker
  in this repository where that is true.
  _Verified against: pulsar-client-go v0.21.0 pulsar/producer_partition.go:727-729,778-780; pulsar/consumer_partition.go:1373-1395._
- **delayed delivery is the dispatcher's.** Pulsar's delayed-delivery tracker
  belongs to the Shared and Key_Shared dispatchers; an Exclusive or Failover
  subscription receives a delayed message as soon as it reaches the topic.
  _Verified against: Apache Pulsar's delayed-message documentation._
- **`partitionProducer.failTimeoutMessages` outlives a closed producer.** It is
  started when `SendTimeout > 0` and exits only when its one-shot timer next
  fires AND the producer is closing, so a closed producer leaks one goroutine
  for up to one `SendTimeout`. goleak in this repository ignores it by name.
  _Verified against: pulsar-client-go v0.21.0 pulsar/producer_partition.go:238-240,940-952._
- **a dead-letter copy keeps the properties it was moved with.** Both of the
  client's dead-letter routes (a reconsume past `MaxDeliveries`, and the
  redelivery bound in the partition consumer's dispatcher) publish the
  message's properties as they find them, so a copy carries the source's
  `RECONSUMETIMES` and `REAL_TOPIC`. The redelivery bound also records
  `REAL_TOPIC` and `ORIGIN_MESSAGE_ID` under the message's own properties, so
  its copy of a message that never went round the retry topic names the topic
  it was moved from and carries no count. `RETRY_TOPIC` is defined and never
  written, which is what lets this driver write it to tell a copy from a retry
  hop. Re-verify all three when the pin moves.
  _Verified against: pulsar-client-go v0.21.0 pulsar/retry_router.go:38; pulsar/consumer_impl.go:617-680; pulsar/consumer_partition.go:1828-1851; pulsar/dlq_router.go:112-119._

## Topic / subscription mapping

A portable topic is the **local name** of a Pulsar topic:

```
persistent://<Config.Tenant>/<Config.Namespace>/<Config.TopicPrefix><topic>
```

`Config.Domain` chooses `persistent` (the default) or `non-persistent`.

Two forms are accepted and the difference is deliberate:

* a topic carrying `://` is taken as a **fully qualified** Pulsar topic and
  validated as one. It is how a caller reaches another tenant or another
  namespace, and the only way to name a non-persistent topic from a persistent
  client;
* anything else is the local name inside the configured namespace.

A short name containing a slash is **refused** rather than read as
`tenant/namespace/topic`, which Pulsar itself would accept. A deployment that
configured a tenant and a namespace has said where its topics live, and a name
that silently escaped that would put one tenant's messages in another's.
An empty topic is refused too: with a prefix configured it
composes to the prefix alone, which is a legal topic every caller who forgot to
name one would share (found by `FuzzPulsarNaming`).

There is no escaping and no rewriting. A name segment holds letters, digits and
`-`, `_`, `=`, `:`, `.`; anything else is refused, because a mapper that
transformed an illegal character would fold two portable topics onto one Pulsar
topic and one subscription would receive another's messages. Pulsar's
`-partition-N` suffix is refused for the same class of reason: a handle opened
on it sees a fraction of the topic with no indication that it has.

The `Admin` surface follows the same line for a partitioned topic, the kind
`EnsureTopic` creates when `Config.Partitions` is positive. `ListTopics` names
it once, by its own name: Pulsar lists each of its partitions as a topic of the
namespace as well, and those are skipped, while a `-partition-N` name with no
partitioned topic over it is still reported as the broker gave it. `Stats` reads
Pulsar's `partitioned-stats`, which sums the subscription over the partitions,
because Pulsar answers a partitioned topic's plain `stats` with 404.

A portable **subscription** is a Pulsar subscription: a named, durable cursor
every consumer of that name shares. It takes the topic grammar, because Pulsar
builds the retry and dead-letter topic names out of it
(`<topic>-<subscription>-RETRY`).

A topic carrying a regular-expression metacharacter — `* ? [ ] ( ) | ^ $ + \ { }
# >` — is a **pattern** and opens a regex subscription; `.` is deliberately not
one of them, because it is an ordinary character in a Pulsar topic name and
`orders.eu` is a name. A pattern is qualified with the
configured namespace before it reaches the client, because Pulsar matches the
pattern against the whole topic name.

## Publish path & confirmation boundary

One producer per topic, **cached** on the backend and reference-counted, so two
publishers on one topic share it and the last one to close closes it.

The client closes a producer for good when the broker refuses its reconnection
with `TopicNotFound`, `TopicTerminated` or `ProducerFenced`, and every later
send on it fails with `ProducerClosed`. The first publication that fails with
one of those four marks the cached producer gone, and the next publication on
any sink for that topic (or a new sink opening on it) creates a fresh producer
and uses that. The sinks waiting share one creation, and each waits no longer
than its own context, because `CreateProducer` takes none. A creation the
broker refuses is that publication's answer and is definite, because nothing
was sent: a terminated topic is a `CodeConflict`, a missing one with automatic
creation off is a `CodeNotFound`, and the next publication asks again. The
producer it replaces is closed once the sends already in flight on it have
finished, by their receipt or `SendTimeout`: on a partitioned topic the client
may have closed only one of its partitions, and closing the rest at once would
fail sends that were on their way to healthy partitions. The backend's `Close`
closes a replaced producer with the cached ones, which fails the sends still in
flight on it as ambiguous, and then waits for a replacement under way, whose
creation `OperationTimeout` bounds. So it never returns with a replacement still
running on the client.

**Closing does not wait for receipts that never come, with two exceptions.**
The backend's `Close` stops every sink from admitting publications, waits for
each asynchronous publication it admitted to be handed to the client (to return
from pulsar-client-go's `SendAsync`), flushes every producer, closes them, and
only then waits for the publications' callbacks. A sink's `Close` does the same
for its own publications, and flushes and closes the producer when it held the
producer's last reference. The flush hands the broker what the client still
holds, in its send queue and in a batch the batching delay has not sent yet, and
waits for the answers for at most `OperationTimeout` in all. The order matters
because pulsar-client-go v0.21's own `Close` fails only what is waiting for a
receipt. It drops a batched or still-queued message with its callback, and a
publication that reaches the producer after it has closed panics the process.

What a publication in flight at `Close` gets:

* A publication the broker answers before the flush ends gets that answer. One
  still waiting when the flush ends, because the broker is silent or slow, is
  failed by the close as ambiguous, `PublishAsync`, `PublishOwned`,
  `PublishBatch` and `Publish` alike. It used to wait for its receipt, up to
  `SendTimeout` or, with `SendTimeout` disabled, for ever.
* `Close` then returns after the flush and each partition producer's own close,
  which pulsar-client-go bounds by `OperationTimeout` per partition.
* pulsar-client-go's flush of a partitioned producer stops at the first
  partition that fails, and the backend's flush shares one `OperationTimeout`
  across every producer. A producer the flush did not reach can still drop the
  callback of a message batched within the last `BatchingMaxPublishDelay`.

Two waits have no bound of their own:

* Under `Config.BlockIfQueueFull`, a publication can be inside `SendAsync`
  waiting for room in a full queue. `Close` waits for it, because closing the
  producer under it would panic the process, and only that publication's
  context, a receipt or `SendTimeout` ends the wait. With `SendTimeout`
  disabled, a context with no deadline and a silent broker, `Close` waits until
  the broker answers.
* A sink closed while other sinks share its producer leaves the producer open
  and waits for its own asynchronous publications' receipts. With `SendTimeout`
  disabled and a silent broker, that `Close` waits until the broker answers or
  the backend closes.

| mode | What it waits for |
|---|---|
| `ConfirmNone` | the client has admitted the publication (`SendAsync` returned). The outcome still reaches the driver's health state and counters, because a publication whose failure nobody can see is a lost message with no evidence |
| `ConfirmAccepted` | `Producer.Send` has returned the broker's send receipt |
| `ConfirmDurable` | the same round trip. On a persistent topic the send receipt is given after the entry has been written to the configured write quorum of bookies, so the two modes are the **same boundary** here and both are durable. On a **non-persistent** topic there is no durable boundary at all and `ConfirmDurable` fails at construction |

`PublishAsync` is `Producer.SendAsync`, and its native callback *is* the specification's
`done`: there is no goroutine per message and no completion pool of this
driver's. `PublishBatch` is one `SendAsync` per message and one wait for every
callback, which is what lets the producer's own batching put them in one
entry — Pulsar has no "publish these together" verb, and the `*BatchError` is
index-aligned with the input. `Flush` is `Producer.FlushWithCtx`.

The client reads a batched message's payload after `SendAsync` or `Send` has
returned, when its event loop adds the message to a batch. `PublishAsync`,
`PublishOwned` and `PublishBatch` hand it your body as it is, because you get
the body back only once the native callback has run. `Publish` under
`ConfirmNone` copies the body first, since you may reuse it as soon as
`Publish` returns. A confirmed `Publish` copies it as well when its context can
end, because a `Send` that gives up on its context leaves the request in the
client's queue.

**`ProducerQueueIsFull` is backpressure, not a failure.** The driver sets
`DisableBlockIfQueueFull`, so a full producer queue is answered rather than
waited on, and the answer is `CodeBackpressure`: the message was never admitted,
so the outcome is certain and the specification gives the core a mode for exactly this.
Mapping it to `CodeUnavailable` would make a caller's retry policy hammer a
broker that is keeping up. `ClientMemoryBufferIsFull` is the same thing measured
in bytes.

**A publication the producer fails as it goes away is ambiguous.** When the
producer closes, or a reconnection is refused with `TopicTerminated`,
`ProducerFenced` or `ProducerBlockedQuotaExceededException`, the client fails
every message still waiting for its receipt with that code, and those messages
are already on the wire. `ProducerClosed` is also the answer for a message sent
to a producer that had closed before it, which never left, and the code cannot
tell the two apart. So all four are reported ambiguous, and you de-duplicate
rather than assume the message was lost.

Refused before any I/O, naming the reason: a per-message TTL (Pulsar's is a
namespace policy), a priority (Pulsar's `PriorityLevel` is a *consumer's*), a
de-duplication key (Pulsar de-duplicates by `(producer name, sequence number)`),
an explicit partition (a Pulsar partition is the producer router's choice), a
delay longer than `pulsar.MaxDelay`, and a message larger than the advertised
maximum.

## Consume path

`ConsumerOptions.MessageChannel` is the fetch seam, and its capacity is the
resolved prefetch. There is **no `driverkit.PushAdapter`**: the kit's adapter
turns a native *callback* into a bounded ring, and pulsar-client-go does not
call back — it writes into a channel the caller supplies, which already is the
bounded hand-off, and behind it sit the per-partition receiver queue and the
protocol's own flow-control permits. A ring in front of that would be a second
buffer of the same messages: twice the memory a prefetch asks for, and a
delivery whose lease is running down in a queue the broker cannot see.

`Fetch` is one blocking wait for the first delivery and then a non-blocking
drain, which is what makes `FeatureBatchReceive` a batch. `Close` selects on the
source's own channel, because pulsar-client-go's `Close` does **not** close the
message channel the caller supplied. Nor does it drain it, so a `Fetch` after
`Close` checks the source's closed flag before it hands anything out and
returns `ErrClosed`, rather than a prefetched delivery nothing could settle.

`DeliveryInfo` carries the concrete topic (never the pattern a wildcard
subscription named, and never the retry topic a redelivered message came back
through; a dead-letter copy reports the dead-letter topic, see *Dead-letter
queue*), the partition with its `Has` flag, the received time, the emulated
lease's expiry, `Redelivered`, and the attempt count.

**The attempt count adds two of Pulsar's counters.** `RedeliveryCount` is the
broker's and counts how many times *this* message was pushed again;
`RECONSUMETIMES` is pulsar-client-go's and counts hops through the **retry
topic**, which are republications, so the broker's counter starts again at zero
for each of them. A driver that read only the broker's counter would report
attempt 1 on every delivery of a message that had been round the retry topic
five times, and a `MaxAttempts` policy above it would never fire. The hop would
still drop the broker's count of the message it republishes, so a delayed
requeue hands the retry router the attempt so far rather than the hop count.
The client's dead-letter route reads the same number, so it counts every
delivery towards `MaxAttempts` and not only the hops. The sum saturates at
`math.MaxUint32` rather than wrapping.
pulsar-client-go's own system properties are stripped on decode, so a message
that came back through the retry topic carries the headers its publisher set and
no others.

**A dead-letter copy starts again at attempt 1**, as it does on the memory and
Redis drivers: its count is the broker's count on the dead-letter topic and
nothing else. pulsar-client-go's dead-letter router republishes the properties
it finds, `RECONSUMETIMES` included, and the driver used to add that count in,
so a rejected message's copy arrived at attempt 18 under the default
`MaxAttempts` of 16, and a runner on the dead-letter topic with
`mq.WithMaxAttempts(17)` or less took every copy as exhausted at its first
failure. A copy that carries a count and a retry hop carry the same system
properties otherwise. The one thing that differs is the topic each was
published to, which the client writes nowhere, so the driver writes it: every
delayed requeue sets `RETRY_TOPIC` (a property pulsar-client-go defines and
never sets) to the retry topic, an explicit `Reject` sets it empty, and a
delivery whose `RETRY_TOPIC` names a topic other than the one it arrived on is a
copy. That covers the three ways a copy is made: an explicit `Reject`, a delayed
requeue past `MaxAttempts`, and the client's own redelivery bound, which copies
the properties of the retry hop it moves. A delivery with no `RETRY_TOPIC` was
moved by another client or by an earlier version of this driver, and is counted
as it always was. The one exception is a delivery with `REAL_TOPIC` and no
count, which only the redelivery bound writes, for a message that never went
round the retry topic: it is a copy too, though with no count to leave out the
attempt is the same either way. A dead-letter subscription with a retry
topic of its own requeues a copy from the copy's count too, so its own
`MaxAttempts` counts the copy's deliveries and not the source's.

A delivery that cannot be decoded is negatively acknowledged rather than
acknowledged — there is no settlement that means "this is not a message I can
read", and acknowledging it would discard somebody's message — and the failure
surfaces as `Backend.Undecodable()` and a health transition.

## Ack / nack / extend mapping

| Portable | Pulsar |
|---|---|
| `Ack` | `Consumer.AckID`, grouped by the client's acknowledgement-grouping tracker |
| `AckBatch` | a **loop** over `Consumer.AckID`. pulsar-client-go v0.21's `AckIDList` discards every per-partition error that is not an `AckError`, so a failed batch would read as settled; the grouping tracker still coalesces the frames |
| `Nack` (requeue, no delay) | `Consumer.NackID`; the client asks the broker to redeliver after `Config.NackRedeliveryDelay` |
| `Nack` (requeue, delay) | `Consumer.ReconsumeLater` when `Config.RetryEnable` is on — a republication to the retry topic with the delay as a `DeliverAfter` — and an **in-process hold** when it is not. The retry topic's hand-off can return `ErrSettlementUnknown` (below) |
| `Nack` (drop) | `AckID`. The specification calls it destructive and it is: Pulsar's only way to make a message go away without a destination is to mark it consumed |
| `Reject` (dead letter) | the client's dead-letter router, reached by pushing the message's reconsume count past `Config.MaxAttempts` — the same route a message exhausted by redelivery takes. Refused with `ErrUnsupported` when no destination is configured |
| `Extend` | a store to the **driver's** deadline. There is no frame: see the lease below |
| `NackBatch` | a **loop**. Pulsar has no batched negative acknowledgement, and the driver says so rather than presenting a loop as a round trip |

**The settlement boundary** depends on `Config.AckWithResponse`. With it off —
the default, and pulsar-client-go's — an acknowledgement is handed to the
client's grouping tracker and sent within `AckGroupingMaxTime`, and `Ack`
returns once the client has taken it. With it on, every acknowledgement is a
round trip the broker answers and `Ack` returns when the answer arrives.

**A delayed requeue through the retry topic and a dead-letter `Reject` wait
under their context and `Close`.** Both hand the message to one of
pulsar-client-go's routers over an unbuffered channel. A router that cannot
create its producer retries without end and takes nothing meanwhile, and one
that `Close` has stopped takes nothing at all, so the hand-off can block for
ever. The driver gives up when the settlement's context ends or the source
closes, returns `ErrSettlementUnknown`, and leaves the delivery unknown, because
the router may still take the message later. The call it gave up on stays
blocked until the router takes it, since nothing can call back a blocked send.

**Every local check happens before anything is written.** Pulsar redelivers a
message under the *same* `(ledger, entry, batch index)`, and the broker accepts
an acknowledgement for it from any consumer of the subscription that holds it —
so an acknowledgement written for a delivery a later one has superseded would
settle the work that replaced it, which is the specification's "lets a stale ack settle a
later redelivery". There is no verb that asks "do I still hold this?", so the
driver refuses: a delivery a redelivery superseded, or one past its lease guard,
answers `ErrLeaseLost` and writes nothing. `Backend.Stale()` counts the
refusals, and a driver whose `Stale` is zero under a workload that overruns its
deadlines is not checking the lease.

A non-persistent topic is the exception. Its broker stores nothing, so it has
nothing to redeliver, and it dispatches every message as entry `0:0`, so two
different messages share a position. The driver tracks each of that topic's
deliveries on its own rather than by position, or every delivery would
supersede the one before it.

### Batched entries

One Pulsar entry usually holds several messages and an acknowledgement names an
entry. `AckRules` is the truth table, and two of its five rows lose messages:

| identifier carries the client's bitmap | client sends an acknowledgement set | broker honours one | what an acknowledgement settles |
|---|---|---|---|
| — (entry holds one message) | — | — | that message |
| yes | no | — | nothing, until every index is acknowledged; then the entry |
| yes | yes | yes | the named index |
| yes | yes | **no** | **the whole entry, siblings included** |
| **no** | — | — | **the whole entry, siblings included** |

Both bad rows are guarded. The driver keeps the identifier the consumer handed
it for the delivery's whole life and never rebuilds one — an identifier put
through `Serialize` and `DeserializeMessageID` is indistinguishable through the
`MessageID` interface and settles the entry  — and
`Config.EnableBatchIndexAck` requires `Config.AdminURL`, with `Open` refusing it
against a broker whose `acknowledgmentAtBatchIndexLevelEnabled` is false.
A settlement the rules cannot make safely is answered
`ErrNotSettleable`, which leaves the message to be redelivered rather than
destroying its siblings.

## Ordering

**`Config.SubscriptionType` decides every claim, and nothing else does.**
It is why the subscription type is the conformance report's *profile*: two
subscription types are two capability tables.

| | `OrderedByKey` | `OrderedByPartition` | `OrderedGlobally` | `ConsumerGroups` | `ScheduledPublish` |
|---|---|---|---|---|---|
| **Shared** (default) | unsupported | unsupported | unsupported | native | native |
| **Key_Shared** (`AllowOutOfOrderDelivery=false`) | **conditional** | unsupported | unsupported | native | native |
| **Exclusive** | conditional | **native** | conditional | unsupported | unsupported |
| **Failover** | conditional | **native** | conditional | conditional | unsupported |

* **Shared** promises nothing. Pulsar hands a Shared subscription's messages to
  whichever consumer has permits, so there is no order of any kind — not per
  key, not per partition, and not even with one consumer, because a negative
  acknowledgement puts a message back behind the ones that followed it.
* **Key_Shared** sends every message of one key to one consumer and delivers
  them in publication order, and `AllowOutOfOrderDelivery` is false so Pulsar
  *blocks* a reassigned hash range rather than handing it over early. Two things
  still end the promise and neither is under the driver's control: a consumer
  joining or leaving moves hash ranges, and adding partitions rehashes every key
  onto a different partition, after which a key's older and newer messages are
  in two partitions with no order between them.
  A client configured for Key_Shared also gives its producer the **key-based
  batcher**, and that is a correctness requirement rather than tuning: Pulsar
  dispatches an *entry* to one consumer, and with the default batcher one entry
  holds several keys — so the subscription would have to send one consumer an
  entry holding another's keys and the per-key claim would be false for every
  key that shared an entry.
* **Exclusive** and **Failover** deliver each partition's entries to the single
  active consumer in the order they were written. On a non-partitioned topic
  that is the whole topic's order, which is why `OrderedGlobally` is conditional
  rather than unsupported; on a partitioned one it is per partition, and adding
  partitions rehashes every key. Failover's second consumer is a **standby**
  that receives nothing until the active one goes away, which is why its
  `ConsumerGroups` claim is conditional rather than native; Exclusive refuses a
  second consumer outright with `ConsumerBusy`.

A negative settlement reorders by construction on every type: the message comes
back after the ones behind it.

## Delays

Two different delays, and they are not the same mechanism.

* **A delayed publication** (`mq.Delay`, `mq.PublishOptions.DeliverAt`) is
  `ProducerMessage.DeliverAfter` / `DeliverAt`, held by the broker's
  delayed-delivery tracker. `FeatureScheduledPublish` is native — **except** on
  a client configured for Exclusive or Failover, where Pulsar's tracker does not
  apply and the message would arrive at once. The driver refuses the schedule
  there rather than making one that does not happen , and the
  capability table says unsupported.
* **A delayed negative settlement** (`mq.NackDelay`) is either Pulsar's retry
  topic or an in-process hold:
  * with `Config.RetryEnable`, `Consumer.ReconsumeLater` republishes the message
    to the retry topic with the delay as a `DeliverAfter` and acknowledges the
    original only once the republication is stored, so a failed republication
    never loses the message. `FeatureDelayedRedelivery` is **native**, and the
    attempt count survives the hop in `RECONSUMETIMES`. The cost is that the
    consumer is then **multi-topic**, and pulsar-client-go refuses a seek on one:
    `FeatureSeekByTime` becomes unsupported. This applies to Shared and
    Key_Shared subscriptions only. The retry topic's delay is a `DeliverAfter`,
    which an Exclusive or Failover subscription ignores, so a client configured
    for one of those takes the in-process hold below even with the retry topic
    on.
  * without it, the driver holds the unsettled delivery in this process for the
    delay and then negatively acknowledges it. `FeatureDelayedRedelivery` is
    **`EmulatedVolatile`**: the hold is lost with the process, after which the
    message comes back at the next disconnection rather than at the time asked
    for. `Backend.HeldDelayedNacks()` is how many are waiting. A hold whose
    message comes back first, because the consumer reconnected, writes nothing
    when it comes due: the redelivery carries the same identifier and answers
    for the message, and a negative acknowledgement would give back its work.

The hold is the source's one scheduler — one timer per source over a list
bounded by the prefetch, never a timer per delivery — and it is the same
scheduler that sweeps the lease.

`pulsar.MaxDelay` (30 days) bounds both. Pulsar's delayed-delivery tracker holds
an in-memory index entry per delayed message, so a very distant delivery time is
a broker-side leak rather than a schedule.

## Dead-letter queue

`Config.DeadLetterTopic` and `Config.MaxAttempts` are pulsar-client-go's
`DLQPolicy`. The client publishes the message to the dead-letter topic and
acknowledges the original **only once that publication succeeded**, so the specification's
"a failed dead-letter publish never acknowledges" is the library's own rule
here.

It fires on its own once a message has had `MaxAttempts` deliveries — that path
happens inside the client and never reaches this driver — and an explicit
`Reject` reaches the same router at once, by pushing the message's reconsume
count past `MaxAttempts`. `Backend.DeadLettered()` counts
only the explicit ones, because the other path is below this driver's view.

Whichever route moved it, the copy's first delivery on the dead-letter topic is
attempt 1 (see the attempt count above). The source's count is still on the
copy, in the `RECONSUMETIMES` property of the `pulsar.Message` that
`Delivery.Native()` returns. It is the reconsume count the copy was made with,
which is the attempt that was requeued for a delayed requeue past the bound, the
forced `MaxAttempts` plus one for a `Reject`, and for the client's redelivery
bound the attempt last requeued with a delay (or no property at all, for a
message that never went round the retry topic). If you need the attempt that
failed, use `mq.WithDeadLetter`: the core publishes a new message with no system
properties, which starts at attempt 1 anyway, and records the failed attempt in
`mq.HeaderAttempts`.

**A copy reports the dead-letter topic** as `DeliveryInfo.Topic` and
`Message.Topic`, whichever route made it, as a copy does on the memory and
RabbitMQ drivers and as the core's `mq.WithDeadLetter` publication does. The
driver reads `REAL_TOPIC` so that a retry hop reports the topic its message was
published to, both dead-letter routes keep that property, and the redelivery
bound writes it where there is none. A copy of a
message that had been round the retry topic, and every copy the redelivery
bound made, used to report the source's topic, while a `Reject` of a message
that never hopped reported the dead-letter topic, so a consumer of the
dead-letter topic saw one or the other depending on the route. The topic is now
decided by the same rule as the attempt count. The source's topic is still in
the native message's `REAL_TOPIC` wherever the copy has one: a copy of a message
that went round the retry topic, and any copy the redelivery bound made. A
`Reject` of a message that never hopped carries none, and `mq.WithDeadLetter`
records it on every copy in `mq.HeaderOriginTopic`. A dead-letter subscription
with a retry topic of its own starts the copy's record afresh when it requeues
it, so the copy's hop reports the dead-letter topic too, and its `REAL_TOPIC`
names the dead-letter topic from then on.

**Without a destination there is no claim and no route.** `FeatureDeadLetter` is
advertised only when `Config.DeadLetterTopic` is set, and `Reject` is otherwise
refused with `ErrUnsupported`: a driver that invented a destination would
destroy a message the caller believed was routed. That holds with
`Config.RetryEnable` too. pulsar-client-go invents `<topic>-<subscription>-DLQ`
for a retry-enabled consumer given no policy and routes to it after 16
deliveries, so the driver gives it a policy whose bound no count reaches.
`mq.WithDeadLetter` — the
core's publish-then-acknowledge emulation — needs one thing from this driver,
and did not get it until the route's five provenance
headers are under `mq.ReservedHeaderPrefix`, which `Representable` refused
outright, so every dead-letter publication failed before any I/O. Those five
keys are now admitted and nothing else under the prefix is.

`Config.MaxAttempts` without a destination is refused at construction, because
Pulsar would route the message to a topic this driver never named and the caller
would find neither the message nor the reason.

## Headers & limits

A header is a Pulsar **property**. Properties are a `map[string]string`, which
imposes five losses, and each of them is something iqmq's portable envelope can
carry:

| | why |
|---|---|
| a header value that is not valid UTF-8 | a property is a protobuf string |
| a header key that is not valid UTF-8 | the same |
| a repeated header key | a map holds one value per key |
| a message key that is not valid UTF-8 | `ProducerMessage.Key` is a string |
| a message key that is **empty but not nil** | pulsar-client-go writes the partition key only when it is non-empty, so an empty key arrives as no key at all — and the specification makes nil and empty observably different |

`MetadataNativeStrict` refuses all five before any I/O, naming the field.
`MetadataAdaptive` wraps exactly the messages that need it, and
`MetadataPortableEnvelope` wraps every message. The two key cases are the ones
`driverkit.Envelope.Needed` cannot express — the inventory has no word for a
message key at all — so the driver asks its own `Representable` and takes a
second envelope in the portable mode for them.

An enveloped message still carries its textual key as the partition key,
because the broker picks the partition, dispatches a `Key_Shared`
subscription and compacts a topic by that key and cannot read the envelope. A
key with no native form (empty, or not valid UTF-8) travels in the envelope
alone, so such a message has no partition key.

Header **order** is the sixth loss and the one without a flag: a map has none,
so two different keys come back in an arbitrary order. `FeatureNativeHeaders` is
native with a note saying so, and `MetadataPortableEnvelope` is what keeps the
order.

A **nil body** and an **empty** one survive natively: Pulsar has a null-value
flag in its message metadata and pulsar-client-go sets it for a nil payload.
It is the only broker in this repository where the distinction needs no
envelope.

`Message.ID` and the typed properties travel as reserved properties under
the specification's `iqmq-` prefix (`iqmq-id`, `iqmq-content-type`, …). A caller's own header
under that prefix is refused rather than renamed. `Properties.CreatedAt` is
Pulsar's `EventTime` plus an `iqmq-created` marker: a Pulsar message that
carries no event time arrives with the **Unix epoch** rather than the zero time,
so a decoder without the marker would invent a creation time of 1970 for every
message that carried none. The time round-trips as an **instant** and not with
its location — Pulsar's event time is milliseconds since the epoch and the
envelope's is nanoseconds, so a `CreatedAt` comes back in UTC.

| Limit | Where it comes from |
|---|---|
| `MaxMessageBytes` | the broker's own `maxMessageSize`, read from the administrative API at open, falling back to Pulsar's documented 5 MiB default. The figure is negotiated per connection in the binary protocol and pulsar-client-go surfaces the negotiated value nowhere, so an administrative lookup is the only route  |
| `MaxDelay` | `pulsar.MaxDelay`, this driver's own bound |
| `MaxBatchMessages`, `MaxBatchBytes` | `Config.BatchingMaxMessages` / `BatchingMaxSize`, when set |
| `MaxLease` | **not advertised.** The lease is this driver's own and has no maximum — an extension is a store to a deadline in this process — so `Config.AckTimeout` is a default rather than a bound, and the specification forbids reading an advertised limit as anything but a bound |

## Reconnect

pulsar-client-go owns the dial loop: the client reconnects inside its own
connection pool, with its own backoff, and re-creates its producers and
consumers. This driver therefore does **not** use `driverkit.Reconnector` — a
second dial loop over a client that already has one would be two policies for
one connection. The one producer the client does not re-create is one it
closed for good, and the driver replaces that one itself, as the publish path
above describes.

`Ping` itself is the cheap check and creates no topology. With
`Config.AdminURL` it is the broker's own health endpoint, which touches no
topic. Without one it is a partitioned-topic **metadata lookup** for a topic
this backend already has a handle on, or `Config.PingTopic`; with neither it
reports `ErrUnsupported`, because on a namespace with automatic creation a
lookup for a topic nobody created is exactly the hidden topology creation 
forbids.

Since the client offers no reconnection callback, a successful `Ping` is the
only reconnection the driver can see, and it counts one (`Reconnects`,
`Config.Hooks.OnReconnect`) only after the broker failed a `Ping` or a
publication failed as `CodeUnavailable`. A `Ping` that had nothing to probe, or
whose caller cancelled it, leaves health alone, and a successful `Ping` after
some other degradation, such as an undecodable delivery, restores health
without counting a reconnection.

## At-least-once window

Four windows, and each of them is a duplicate rather than a loss.

1. **Between a delivery and its acknowledgement.** A consumer that disconnects
   makes every unacknowledged message of its subscription available again at
   once, which is also how this driver's `Close` gives work back: the specification's "requeues
   or abandons everything it has handed out" is the broker's here and not a loop
   of this driver's.
2. **Between an acknowledgement being grouped and being sent**, when
   `Config.AckWithResponse` is off. A process that dies inside
   `AckGroupingMaxTime` loses the acknowledgement and not the message.
3. **A delayed negative settlement held in this process**, when
   `Config.RetryEnable` is off. The hold is lost with the process.
4. **The emulated lease.** `Config.AckTimeout` is the driver's deadline: it
   tracks each delivery's, hands the message back with a negative
   acknowledgement one `LeaseGuard` before it, and refuses a settlement that
   arrives afterwards. `DeliveryInfo.LeaseExpires` is that earlier moment,
   `AckTimeout` less `LeaseGuard` after the delivery, and `Extend` by `d` moves
   it to `d` from now, so a lease scheduler's lead is measured from the
   deadline the driver acts on. `Config.DisableAckTimeout` removes it and gives Pulsar's
   own behaviour — an unsettled delivery that never comes back while the
   consumer holds its connection.

## Config & DSN

```
pulsar://broker:6650/tenant/namespace/prefix?admin=http://broker:8080&subscriptiontype=keyshared
pulsar+ssl://broker:6651/acme/billing?trustcerts=/etc/ssl/ca.pem
```

With `pulsar+ssl://` the broker is verified against `trustcerts` when it is
given and the system roots when not, `insecureskipverify=true` turns
verification off, and `authname=tls` presents the provider's client
certificate. A DSN sets no `Config.TLS`: pulsar-client-go uses a `*tls.Config`
exactly as given and reads none of those beside it, which is why a
programmatic `Config.TLS` is refused together with `TLSTrustCertsFilePath`,
`AuthName` "tls" or an `InsecureSkipVerify` it does not carry itself.

`authname=tls` needs the `pulsar+ssl://` scheme and is refused without it. A
client certificate is presented during a TLS handshake, pulsar-client-go picks
the transport from the scheme alone, and on `pulsar://` there is no handshake at
all: measured against a TLS port that requires a certificate, the port saw no
handshake and zero certificates, so the authentication silently did nothing. The
mechanisms that travel in the CONNECT frame instead — `Token`, and `authname`
"oauth2", "basic" or "athenz" — are accepted on either scheme.

Both schemes are registered: `pulsar+ssl://` is what Pulsar's own tooling and
service URLs use, so a driver that accepted only `pulsar://?tls=true` would
refuse the connection string an operator already has. The **path** is the tenant
and namespace, with an optional third segment for the topic prefix — Pulsar's
own service URLs put the namespace in the topic, so there is no established
spelling to copy and this is the natural place.

Every parameter is listed on `ParseDSN`. A parameter this driver does not have
is a construction error naming it, not a setting nobody notices was ignored.
Prefetch is deliberately absent: it is the core's, resolved from the consumer
configuration, and a DSN that set it would be a second owner of one number.
`receiverqueuesize` is a different number — the per-partition buffer behind the
prefetch, which defaults to the prefetch.

Credentials (`Config.Token`, `Config.AuthParams`) are `dsn.Secret`, so
`String()`, `LogValue()` and `DSN()` render the redaction and never the
plaintext. `Config.DSN()` deliberately does **not** round-trip: a rendering that
round-tripped would be a rendering that carried the credential.

`Config.Logger` is where pulsar-client-go's own logging goes, and **nil
discards it**. The library's default is `logrus.StandardLogger()`, which writes
every reconnection and every lookup to the standard error of a process that
never asked for it; a library does not own its host's log stream.

## Known gotchas

* **Batching means an entry holds several messages**, and an acknowledgement
  names an entry. If you reach past this driver to `Consumer.AckID` with an
  identifier you serialised, you will acknowledge every sibling in the batch.
  See `AckRules`.
* **`Config.EnableBatchIndexAck` needs the broker to agree.** Apache Pulsar's
  `acknowledgmentAtBatchIndexLevelEnabled` is false by default, and a client
  that sends an acknowledgement set to such a broker acknowledges the whole
  entry with no error anywhere. The driver refuses the option at open unless the
  administrative API says the broker honours it.
* **`Config.RetryEnable` costs you `Seek`.** The consumer is then subscribed to
  two topics, and pulsar-client-go refuses a seek on a multi-topic consumer.
* **A delayed publication does nothing on an Exclusive or Failover
  subscription.** The driver refuses it, which is the honest answer and may
  surprise a caller whose client is configured for those types but only
  publishes.
* **`FeatureReplay` is the namespace's, not the topic's.** Pulsar's default
  retention is zero, under which an entry every subscription has acknowledged
  may be deleted at once. A deployment that wants to replay a topic sets a
  retention policy; `Backend.SetNamespaceRetention` is the typed extension that
  does it, and the conformance harness calls it.
* **`StartAt` is a seek, and a seek moves an existing subscription's cursor.**
  The specification describes a start position as where a subscription with *no* committed
  position begins, and this driver cannot tell the two apart without the
  administrative API — so a `StartAt` on a subscription that already has a
  position rewinds it.
* **pulsar-client-go leaks one goroutine per closed producer for up to one
  `SendTimeout`.** `partitionProducer.failTimeoutMessages` exits only when its
  one-shot timer next fires *and* the producer is closing, and the library
  offers no way to wake it. goleak in this repository ignores it by name.
* **A non-persistent topic has no durable boundary.** `ConfirmDurable` fails at
  construction, and `FeatureDurableStorage` is not advertised.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

All four are in `drivers/pulsar/critical_test.go`, against the in-process model,
and each was watched failing under a deliberate mutation before it was made to
pass.

| Test | What it holds |
|---|---|
| `TestCriticalKeySharedOrdering` | one key goes to one consumer and arrives in publication order, **and** a Key_Shared client's producer asks for the key-based batcher — without which an entry holds several keys and the per-key claim is false for all of them |
| `TestCriticalPartitionExpansion` | after the partition count grows, the producer discovers it and publishes the same key to a different partition, the consumer receives from the new partitions, every delivery says which partition it came from, and the per-key ordering claim is **not** native |
| `TestCriticalSubscriptionModes` | the four types' ordering, consumer-group and scheduled-publish claims, what a second consumer of one subscription *is*, and the refusal of a delayed publication on the two types that would deliver it at once |
| `TestCriticalBatchIndexAckDoesNotSettleASibling` | the disqualifier in Pulsar's clothing, in four parts: the client holds a partial batch's frame back; an identifier with no batch bookkeeping is refused; a broker that ignores an acknowledgement set is refused at open; and a broker that honours one settles the named index alone |

Beside them, `race_test.go` covers this repository's recurring defect class in
the two routes this driver adds: a delayed negative settlement placed while the
source is closing (three thousand rounds), and an asynchronous publication
admitted while the sink is closing — which is a race the suite actually found.
