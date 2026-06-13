# `drivers/pulsar`

iqmq's Apache Pulsar driver, over
[pulsar-client-go](https://github.com/apache/pulsar-client-go).

The reference is [`docs/drivers/pulsar.md`](../../docs/drivers/pulsar.md), which
carries the fixed sections: the mapping, the confirmation boundary, the
settlement verbs, the four subscription types and what each of them lets this
driver claim about ordering, the limits, the reconnect behaviour, the
at-least-once window, the gotchas and the driver-specific critical tests.

## What you get

| | |
|---|---|
| Topic | `persistent://<tenant>/<namespace>/<prefix><topic>`, from `Config` |
| Subscription | a Pulsar subscription: a named, durable cursor every consumer of that name shares |
| Publish | `Producer.Send`, which returns the broker's send receipt — for a persistent topic, after the write quorum |
| Async publish | `Producer.SendAsync`, whose native callback is the specification's `done` |
| Batch publish | one `SendAsync` per message and one wait, so the producer's own batching puts them in one entry |
| Scheduled publish | `DeliverAfter` / `DeliverAt`, held by the broker |
| Ack / Nack | `AckID` (grouped by the client) / `NackID`, and `AckIDList` for a batch |
| Delayed nack | `ReconsumeLater` through the retry topic when `RetryEnable` is on, and an in-process hold when it is not |
| Dead letter | the client's `DLQPolicy`, reached by an explicit `Reject` as well as by attempt exhaustion |
| Attempt | `RedeliveryCount` **plus** Pulsar's `RECONSUMETIMES`, so a retry-topic hop still counts |
| Seek | `SeekByTime` across every partition |
| Wildcards | a regex subscription, when the topic carries a pattern metacharacter |
| Admin | Pulsar's HTTP administrative API (`Config.AdminURL`) |
| Transactions | `x/pulsartx` |

## The one setting that changes everything

`Config.SubscriptionType`. The ordering claims are a property of it and of
nothing else, so the capability table follows it:

| | per key | per partition | global | a second consumer is |
|---|---|---|---|---|
| `SubscriptionShared` (default) | — | — | — | a competitor |
| `SubscriptionKeyShared` | conditional | — | — | a competitor |
| `SubscriptionExclusive` | conditional | **native** | conditional | refused |
| `SubscriptionFailover` | conditional | **native** | conditional | a standby |

The conditions are stated in the table's own notes and quoted on the driver
page. In short: Key_Shared's per-key order ends when a consumer joins or leaves
or when the partition count changes, and Exclusive's and Failover's per-key
order ends when the partition count changes.

A client configured for Key_Shared also gives its producer the **key-based
batcher**, because Pulsar dispatches an *entry* to one consumer and an entry
holding two keys cannot be routed by key.

## What you do not get

* **Per-message TTL** — Pulsar's is a namespace policy.
* **Message priority** — Pulsar's `PriorityLevel` is a *consumer's* priority.
* **De-duplication by a caller's key** — Pulsar de-duplicates by
  `(producer name, sequence number)`.
* **A start position by sequence** — a Pulsar position is a
  `(ledger, entry, batch index)` triple.
* **A delayed publication from a client configured for Exclusive or Failover** —
  Pulsar's delayed-delivery tracker belongs to the Shared and Key_Shared
  dispatchers, so such a message would arrive at once. The driver refuses rather
  than making a schedule that does not happen.

## Two things are emulated, and both say so

* **The delivery lease.** pulsar-client-go v0.21 has no client-side
  acknowledgement timeout and Pulsar's broker redelivers only when a consumer
  disconnects, so `Config.AckTimeout` is the *driver's* deadline:
  `FeatureExtendLease` is `EmulatedVolatile`. `Config.DisableAckTimeout` gives
  Pulsar's own behaviour.
* **A delayed negative settlement**, when `RetryEnable` is off: the driver holds
  the unsettled delivery in this process for the delay.
  `FeatureDelayedRedelivery` is then `EmulatedVolatile`, and `RetryEnable`
  replaces it with Pulsar's own durable retry topic.

## Batched entries and 

Pulsar's producer batches by default, so one broker **entry** usually holds
several messages and an acknowledgement names an entry. Two things then lose
messages silently, and both are guarded:

* an identifier that lost the client's per-entry bitmap acknowledges the whole
  entry. This driver keeps the identifier the consumer gave it for the
  delivery's whole life and refuses a settlement it cannot vouch for;
* an acknowledgement **set** sent to a broker whose
  `acknowledgmentAtBatchIndexLevelEnabled` is false is ignored, and the broker
  acknowledges the entry. `Config.EnableBatchIndexAck` therefore needs
  `Config.AdminURL`, and `Open` refuses it against a broker that would ignore
  it.

See `AckRules` and `TestCriticalBatchIndexAckDoesNotSettleASibling`.

## Testing

`pulsarfake` is an in-process model of the broker **and** of
pulsar-client-go's own bookkeeping, because the sharpest of this driver's
hazards lives in the client library rather than at the socket. It carries the
four subscription types' dispatch rules, Key_Shared hash-range blocking,
partitioned logs and partition expansion, batched entries with the client's
per-entry bitmap, redelivery under the same identifier, both kinds of delay, and
the retry and dead-letter routers — and it is an `mq.Clock`, so a lease expires
when a test says so rather than when a test waits.
