# memory

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 1 · **Owner:** iqmq maintainers

the driver documentation delegates this page, and the specification describes the engine.
What follows is the driver *over* that engine: what a caller of the public API
sees, and where this driver's answer differs from a network broker's.

**A `memory://` DSN names a broker inside one process, and two processes that
open the same DSN do not meet.** The scheme looks like every other driver's,
so it is worth saying plainly: there is no socket, no port and no address, and
the string after `memory://` selects a broker within this program rather than
a place to reach. Two goroutines share one; two processes -- a producer and a
consumer started separately, or a test binary and the program under test --
each get their own and neither sees the other's messages.

## Client & version policy

There is no client library and no broker. The driver is a
`github.com/iqhive/iqmq.Backend` over `internal/engine`, in the same module as
the root package, so it is always available and always the same version as the
core. `Client.Info()` reports `Name: "memory"`, a `ClientName` naming the
engine it wraps, and an empty `BrokerVersion` — the specification makes an empty string
"the driver did not say" rather than a claim, and there is no broker here to
report one.

Nothing is persistent. The specification forbids this driver from advertising durable
storage and it does not: `FeatureDurableStorage` and `FeatureDurableConfirm`
are unsupported, and the two guarantees that *are* real but die with the
process — a subscription outliving its consumers, and deduplication inside a
window — are advertised as `EmulatedVolatile`, the one level the specification forbids from
satisfying a durability requirement.

## Topic / subscription mapping

A topic is a topic and a subscription is a subscription; nothing is mapped
onto anything.

| iqmq | memory |
|---|---|
| topic | `engine.Topic`, one per name, created through the admin surface or by `AutoCreate` |
| subscription | `engine.Subscription`, one independent consumer group per name |
| consumer | `engine.Consumer`, attached to a subscription; consumers of one subscription compete |
| partition | one lane per subscription-partition, `TopologyConfig.Partitions` of them |

`AutoCreate` is **false** by default, which is what every network driver does
and what the specification requires of `Open`: a publisher or a consumer naming an unknown
topic fails with `ErrTopicNotFound`. `NewForTest` turns it on, and `Admin.EnsureTopic` is the explicit route.

Topic names are held to the portable grammar `mq.ValidateTopic` defines. The
driver adds no restriction of its own.

## Publish path & confirmation boundary

`Publish` validates, copies the caller's storage once, hashes the key to a
lane, reserves capacity in every target subscription in a stable order and
enqueues. It returns when the record is in every lane the fan-out policy made
it wait for.

That is the **whole** of the confirmation this driver can offer, and it is
what each mode means here:

| Mode | Boundary |
|---|---|
| `ConfirmNone` | the same as `ConfirmAccepted`: there is no local admission step separate from the lane |
| `ConfirmAccepted` (default) | the record is in every target lane and a consumer can claim it |
| `ConfirmDurable` | **fails at construction** with `ErrUnsupported` |

`ConfirmDurable` fails when the publisher is *opened*, not when it publishes:
there is no durable boundary to define, and the specification requires the refusal to be at
construction.

`PublishBatch` reaches the engine's own batch call. The result is index-aligned
with the caller's slice: the engine's batch stops at the first element it
refuses and names its index, and the driver records that element's outcome and
offers the rest, so a refusal is never reported against the messages behind it.

Three ownership modes ship:

| Surface | Contract |
|---|---|
| `Sink.Publish` (default) | safe copy: the message is cloned on admission and the caller may reuse its `Key`, `Body` and header storage the instant `Publish` returns |
| `OwnedSink.PublishOwned` | the storage transfers to the engine and `Release` runs exactly once — on settlement, on rejection, on a refused option and on close |
| `AsyncSink.PublishAsync` | the storage is borrowed: it stays the caller's and must stay valid and unmutated until the completion callback runs, which is when the last subscription that was admitted settles the record |

`FlushSink.Flush` is implemented and has nothing to do: the engine admits
synchronously, so a flush is a barrier that has already passed. It is
implemented rather than omitted so that the boundary is stated here instead of
inferred from a missing method.

## Consume path

`Source.Fetch` fills the caller's slice from the consumer's assigned lanes,
rotating the start index so that no lane starves. It blocks only when nothing
is available, and it allocates nothing per fetch and nothing per message: the
claims come out of a buffer allocated once when the source opens, and the
engine handle a settlement needs later is a pointer in `DriverDelivery.Ref`'s
interface word rather than a boxed value.

`Source.Close` unblocks a parked `Fetch` (the obligation), which is why the
core never has to merge the caller's context with a cancellation of its own —
a merge costs two or three allocations on the one path that may not have them.

`DeliveryInfo` is filled as follows. The `Has` flags matter: the specification forbids "the
broker did not say" from being the same answer as zero.

| Field | Memory |
|---|---|
| `Topic` | the resolved topic; on a wildcard topic the publication's subject rather than the pattern |
| `Source` | always empty: there is no broker-side origin behind a memory topic |
| `Subscription` | the subscription the source was opened on |
| `ReceivedAt` | the broker clock at the fetch |
| `LeaseExpires` | `ReceivedAt` plus the subscription's lease (the deadline it was created with, whatever a later source asks for); the **zero time** when that lease is negative, which is "leases never expire" |
| `Attempt` | the delivery count, 1 on a first delivery |
| `Redelivered` | `Attempt > 1` |
| `Partition` / `HasPartition` | the lane; always set |
| `Sequence` / `HasSequence` | the record's publication sequence within its topic; always set |
| `Offset` / `HasOffset` | **never set**: the engine has no per-partition offset distinct from the sequence |

`Delivery.Native()` is nil. The engine's envelope is opaque by contract and a
caller could not name its type, so surfacing it would be an escape hatch that
escapes to nothing. `Client.Native()` returns the `*memory.Broker`, which is
the escape hatch that is worth having.

A received `Body`, `Key` and header value alias the engine's own storage and
are valid until the delivery is settled, which is the safe receive mode.
Nothing is recycled unless `ConsumerConfig.BufferReuse` is set. With
`ZeroOnRelease` the engine overwrites the payload as it releases the record,
so a delivery read *after* it was settled sees zeros rather than its data.

The source implements `DeadlineExtender` and deliberately **not**
`BatchSettler`: the engine settles one delivery at a time under one lane
mutex, so a batch method would be the loop the core already writes, and a
driver that offers one makes the core skip its own adapter for no saving.

## Ack / nack / extend mapping

| Verb | Native action |
|---|---|
| `Ack` | terminal: the record leaves the subscription and its payload is released once every subscription has settled it |
| `Nack` (requeue) | the record returns to its lane, immediately or through the delayed structure when a delay is named; the attempt count survives and the next delivery increments it |
| `Nack(NackDrop())` | the engine's terminal settlement — the same native action an acknowledgement takes |
| `Reject` / `Nack(NackDeadLetter())` | publish to the topology's `DeadLetterTopic`, then acknowledge |
| `Extend` | the lease deadline moves to at least `by` from now, through the broker's shared lease scheduler |

Two of those need a sentence each.

**Drop is an acknowledgement to the engine.** The engine has one terminal
settlement, so a dropped record is counted in `Snapshot.Acked` rather than in
`Snapshot.Dropped`, and `Snapshot.Dropped` keeps its own meaning: a record
discarded *without* a settlement. Most brokers are the same — a delete or an
offset commit serves both verbs — and the distinction iqmq keeps is the
consumer's own: `ConsumerHandle.Stats()` counts a drop as a negative
settlement.

A settlement carries the generation the delivery was claimed under, so a stale
one is answered rather than applied to the redelivery that took its place: a
lease that expired or was rebalanced away reports `ErrLeaseLost`, and a
consumer that closed reports `ErrClosed`.

## Ordering

This is the sentence delegates to each driver page.

**The memory driver delivers one ordered lane per subscription-partition, in
publication sequence.** With the default `Partitions: 1` that is one ordered
lane per subscription, which is the guarantee without the caller asking for
it. A topic with more than one partition orders each lane
and not the topic; a key always hashes to the same lane, so one key's records
are ordered with respect to each other.

What may overtake is a **redelivery**. Under the default `OrderingRelaxed` a
record that comes back from a negative acknowledgement, a lease expiry or a
consumer close is placed by the retry-lane kind (`pushfront` by
default), which serves it before the records still waiting — the
redelivery-first order real brokers use. `OrderingStrict` stops it: the
redelivery blocks its lane, and nothing published behind it is delivered until
it has been settled or routed to a dead-letter topic.

Three things carry a caveat of their own, and all three are control operations
rather than delivery:

`Reorder` (the fault) never fires on a lane whose topic asked for strict
ordering, nor on an engine whose kinds order what they hold.

## Delays

Native, both forms. `Delay` and `DeliverAt` hold a publication back; a
negative acknowledgement with `NackDelay` holds a redelivery back; `TTL`
discards a record that has still not been delivered that long after it became
due.

All three are driven by **one scheduler per broker**, which sleeps until the
next non-empty slot: there is no timer and no goroutine per message.
The granularity is the selected `EngineConfig.Timer` kind's:
`Broker.SchedulerResolution()` reports it, the wheels quantise to a
millisecond and the heaps fire at the exact due time. A
`Config.SchedulerResolution` finer than the selected kind provides is refused
at construction rather than silently approximated.

Time moves by itself on the wall clock and only on demand under a
`TestClock`: `Broker.Advance` moves the clock **and** runs the sweeps the move
implies, which is why `TestClock` has no `Advance` of its own. Inside a
`testing/synctest` bubble the wall clock follows the bubble clock, so a test
needs neither.

## Dead-letter queue

There is no implicit dead-letter topic: `TopologyConfig.DeadLetterTopic` names
one and it must already exist, because the engine never creates a topic behind
the operator's back. A record whose attempts run out with no usable
destination is discarded and counted in `Snapshot.Dropped`.

Two routes reach it, and they are accounted differently:

- **The attempt budget.** A failed delivery past `MaxAttempts` is published to
  the dead-letter topic by the engine and counted in
  `Snapshot.DeadLettered`. A publication that fails returns the record to its
  lane and is counted in `Snapshot.DeadLetterFailures` — it never
  acknowledges.
- **An explicit `Reject`.** The driver publishes and then acknowledges, so the
  record is counted in `Snapshot.Acked` on the source subscription. See
  "Ack / nack / extend mapping".

Under `FanoutIndependent` a dead-letter topic with several subscriptions can
take the copy into some and not others. Both routes count that partial fan-out
as published and settle the record: the subscriptions with room hold the copy,
and the full one counts the refusal in `Snapshot.Rejected`. A `Reject` that
failed on it instead would be retried and add a second copy to every
subscription that took the first.

A rebalance never dead-letters: a revoked delivery is not a failed one, so it
does not consume an attempt.

## Headers & limits

Every piece of metadata is native. The engine stores the `*mq.Message` itself,
so binary header values, duplicate keys, insertion order, the identifier and
every typed property travel unchanged, and **nothing is ever
unsupported metadata**. Three of the four modes therefore come out
identical here: `MetadataNativeStrict` has nothing to reject,
`MetadataAdaptive` never needs the envelope, and `MetadataLenientDrop` never
drops anything.

`MetadataPortableEnvelope` is honoured as written, because the whole point
is that the envelope is never applied by accident and never skipped when it
was asked for. A sink in that mode wraps the message in the `iqmq/wire`
envelope and publishes the envelope as the whole body; a source in the same
mode decodes it. **A source in native mode on the same topic sees the envelope
bytes**, exactly as a consumer written against a broker's own client would,
which is the body-interoperability change the specification requires the mode to be
explicit about. A source in envelope mode reading a body that is not an
envelope reports `CodeCorruptMessage` rather than handing over nonsense.

A sink in that mode refuses, before anything is stored, a message its own
source could not decode: an identifier or a header key over 256 bytes, or any
other field over the `iqmq/wire` decode limits, fails with
`CodeMessageTooLarge`, and an identifier, header key or string property that
is not valid UTF-8 fails with `CodeCorruptMessage`. Those are the codes the
decode would have failed with, and the same ones `driverkit.Envelope` gives.
Admitted, such a record would fail every fetch and go back to its lane each
time, holding up a strictly ordered lane for good.

Limits: only what the broker really knows is advertised, because the specification forbids
an unadvertised limit from being read as a limit of zero.

| Limit | Memory |
|---|---|
| `MaxMessageBytes` | the default topology's `MaxMessageSize`, when it has one |
| `MaxLease` | **not advertised.** This broker has no longest lease: `Extend` sets the deadline to `now + d` for any `d`, and the lane's scheduler is a list move rather than a timer per delivery. The topology's `AckDeadline` is the lease a delivery STARTS with, which the specification's `Limits` has no field for |
| `MaxBatchMessages`, `MaxBatchBytes`, `MaxHeaderCount`, `MaxHeaderBytes`, `MaxDelay` | not advertised: there is no limit |

`Capabilities` is constant per backend and cannot describe one topic, so a
topic created with a different `MaxMessageSize` through the admin surface is
enforced by the engine rather than by the core's pre-flight check.

**Byte accounting counts the key and the body**, not the header storage:
counting headers means walking the header set on every publication, and
`mq.Headers` exposes no allocation-free size accessor. `Snapshot.Bytes` and
the capacity bounds are therefore key-plus-body figures.

## Reconnect

There is nothing to reconnect to, so there is no reconnect loop, no backoff
and no `Hooks.OnReconnect` event from this driver. `Client.Health()` is
`Ready` until the broker closes and then `Closed`; it is never `Degraded`.

`Faults.Reconnect()` simulates what a reconnect does to a subscription: the
assignment generation increments and every lease outstanding under the old one
is revoked, so a settlement holding one reports `ErrLeaseLost`. The
deliveries it revokes are not failures and do not consume an attempt.

## At-least-once window

At-least-once is the default and the only delivery model this driver ships.
The window in which a message can be delivered twice is the ordinary one: a
delivery that was processed and whose acknowledgement did not reach the engine
— because the lease expired first, because a rebalance revoked it, or because
the consumer closed — is redelivered. `Faults.DuplicateDelivery` reproduces it
deterministically (and is a documented no-op under a shared partition log,
which hands each slot to each cursor exactly once by construction;
`Faults.Duplicates()` reports which happened).

`FeatureExactlyOnceDelivery` is **not advertised**, and it used to be. The
root API defines it as "de-duplicated at-least-once delivery" — a consumer
never sees one publication twice — and the paragraph above says plainly that
this broker redelivers a delivery whose lease expired, so a consumer can. What
the claim described was publication de-duplication: a publication carrying a
dedup key is discarded if the topic has seen that key inside `DedupWindow`,
**in this process**, and a restart forgets the window. That is
`FeatureDeduplication`, which is still advertised as `Conditional` on
`TopologyConfig.DedupWindow`.

`ConfirmAccepted` means the record is in every target lane. A process that
dies after a publication returned loses the record: there is no durable
boundary, and `ConfirmDurable` says so by failing.

## Config & DSN

`memory.Config` is the struct. The zero value is usable and is the bounded,
ordered configuration: one partition per subscription, 65,536
records per lane, a 30-second lease, the blocking overflow policy, the real
clock, and the configured variant-family defaults.

Two fields are deliberately **asymmetric**, so that "I did not set it" and "I
want no limit" cannot be the same value:

| Field | Zero | Negative |
|---|---|---|
| `TopologyConfig.CapacityMessages` | 65,536 records per lane | unbounded, which selects the chunked ready queue |
| `TopologyConfig.Partitions` | 1 lane, and therefore strict order | `GOMAXPROCS` lanes |
| `TopologyConfig.AckDeadline` | 30 seconds | leases never expire |

`TopologyConfig.MaxInFlight` and `MaxDelayed` are fields the engine does
not have: it bounds the ready, in-flight and delayed records of a lane
*together* through `CapacityMessages`. A non-zero value is refused at
construction rather than ignored.

`EngineConfig` pins one kind of every variant family: the ready queue,
the fan-out architecture, the wait strategy, the scheduler, the key hash, the
pools, the topic registry, the counters and the retry placement. The zero value
is each family's `Auto`; `Safe` forces the mutex-backed reference kind of every
family at once, which is the configuration that contains no lock-free data
structure at all. The single-producer and single-consumer ready queues are
refused: a lane is fed by any number of publishers and drained by any number
of consumers.

DSN, under `memory://` and the alias `mem://`. A memory DSN names
no host, user, path or fragment — there is nothing to connect to — and an
unknown parameter is an error rather than a warning.

```
memory://?partitions=8&capacity=100000&overflow=block&wildcards=true&autocreate=true
```

| Parameter | Values |
|---|---|
| `autocreate` | `true` \| `false` |
| `partitions` | a positive integer up to 1024, or `auto` for `GOMAXPROCS` |
| `capacity` | a positive integer, or `unbounded` |
| `capacitybytes`, `maxmessagesize`, `maxtotalmessages`, `maxtotalbytes` | positive integers |
| `kind` | `queue` \| `topic` \| `stream` |
| `overflow` | `block` \| `reject` \| `dropnew` \| `dropold` |
| `fanout` | `atomic` \| `independent` \| `besteffort` \| `dropslow` |
| `ordering` | `relaxed` \| `strict` |
| `ackdeadline` | a Go duration, or `never` |
| `maxattempts` | a positive integer |
| `deadletter` | a topic name |
| `dedupwindow`, `retentionage` | Go durations |
| `retentionmessages`, `retentionbytes` | positive integers |
| `priority`, `wildcards`, `zeroonrelease` | `true` \| `false`; `priority=true` is refused beside `ordering=strict`, because priorities reorder the lane strict ordering keeps |
| `engine` | `safe` |

`memory.Register(reg)` registers both schemes with a DSN registry. A
driver exposes `Register` and never reaches for a global registry of its own.

## Known gotchas

- **`Admin.DeleteTopic` answers `ErrUnsupported`.** The engine's registry has
  no removal: a topic exists for the life of the broker. A caller that wants a
  fresh one builds a fresh broker, which in process costs nothing. Closing
  every subscription and leaving the topic in the registry would be neither a
  delete nor a no-op, so it is not done.
- **`Admin.Purge` is an emulation and does not reach everything.** A temporary
  consumer claims and settles every record the subscription has *ready*. It
  does not discard a record another consumer holds in flight — that record is
  not the subscription's to throw away while a lease is outstanding — nor one
  still waiting for its due time. What it discards is counted in
  `Snapshot.Acked`, for the same reason a drop is.
- **`FeatureWildcardSubscribe` is unsupported even though the engine has
  subject routing.** `SourceConfig` carries no filter, so a wildcard
  subscription cannot be opened through the SPI; a topic may still be created
  with wildcards, and a delivery from one reports the resolved subject as its
  topic.
- **A delivery is borrowed until it is settled.** Reading `d.Body` after
  `d.Ack()` is reading storage the engine may have released — and, with
  `ZeroOnRelease`, storage it has overwritten. `d.Message.Clone()` is the
  independent copy.
- **Publishing to a topic with no subscription is not an error.** The record is
  discarded and released, which is an admission of nothing to nobody;
  `Broker.Totals()` reports it as discarded.
- **The default overflow policy blocks.** A publisher that finds a full lane
  parks until room appears or its context ends. That is the default and it
  is the right one for a test double, but it means a publication into a lane
  nobody is draining hangs rather than failing. `OverflowReject` is the
  fail-fast policy.
- **`Broker.Advance` needs a clock the broker owns.** It answers
  `ErrUnsupported` on a broker built with no `Config.Clock` or with a clock of
  the caller's own, because a fault that silently did nothing would make a test
  that relies on it pass for the wrong reason. `NewTestClock` is the clock it
  needs and `NewForTest` installs one.
- **`Faults` is broker-wide.** A fault armed for "the next n" is armed for the
  next n of anything, on any topic.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

| Test | What it holds |
|---|---|
| `FuzzMemoryModel` | the invariants after every step of a fuzz-chosen script, against a fuzz-chosen kind of every variant family, inside a `synctest` bubble |
| `TestEveryKindSetPublishesAndReceives` | a message goes through every kind of every family — 54 of them — through the public API |
| `TestEngineConfigMirrorsTheFamilies` | the public kind enumerations still name the engine's kinds, so a family that gains or reorders a kind cannot silently rebind a caller's choice |
| `TestAllocs` | the exact allocation counts: one per message on the receive path in safe mode, none under `BufferReuse`, none at all in `Fetch`, and none end-to-end for an owned publication with a recycled record |
| `TestSourceCloseUnblocksAParkedFetch` | the close obligation, for every wait kind |
| `TestOwnedPublicationReleasesExactlyOnce` | the release on admission, refusal, fault and close |
| `TestEveryMetadataModeCarriesEveryField` | the four modes, none of which loses a field |
| `TestTheEnvelopeModeRefusesWhatItCouldNotReadBack` | a message the envelope source could not decode is refused at publish and never reaches the lane |
| `FuzzParseDSN` | a DSN the parser accepted is a configuration `NewBroker` can build |
| `BenchmarkMemory/PubSubAck/{api,engine}/…` | what the public API costs over the engine it wraps |
