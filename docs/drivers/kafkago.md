# kafka (kafka-go)

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 1 · **Owner:** iqmq maintainers

This driver uses `github.com/segmentio/kafka-go`;
[`drivers/kafka`](kafka.md) uses franz-go. Both are available. Choose according
to the capabilities and client APIs your application needs.

No generated conformance reports are tracked here. Run the suites for both
profiles on the intended source revision and broker when comparing behaviour.
The cross-driver tests in `drivers/kafka/kafkago/crossdriver_test.go` check
reserved headers, topic validation, partition hashing and portable error codes.

## Client & version policy

The scheme is separate deliberately. The two drivers have different producer
guarantees, different rebalance behaviour and different capability tables, so
one connection string must not be able to mean either — a caller reading a DSN
could not tell which client it got. A program that wants both registers both,
which is possible precisely because the schemes differ.

The native baselines for both clients live in `benchmarks/native-baselines/`
(`make bench-native CLIENTS="franzgo kafkago sarama"`) and are the denominator
for any overhead claim; the shared pin table and the verified facts
are on the [franz-go page](kafka.md#client-version-policy), which is the
`kafka` driver's page for both profiles.

### Six defaults this driver sets that kafka-go does not

Each one is a correctness requirement rather than tuning, and each is listed
because a caller building its own `kafka.Writer` for `OpenWithWriter` has to
set them too.

| Field | kafka-go's default | This driver | Why |
|---|---|---|---|
| `Writer.RequiredAcks` | `RequireNone` (acks=0, fire and forget) | `RequireAll` | every confirmation mode would otherwise be a lie |
| `Writer.BatchSize` | 100 | 1 | kafka-go has no immediate flush, so a batch above one makes a synchronous publish wait out the timer |
| `Writer.BatchTimeout` | 1 s | 10 ms | at kafka-go's default a synchronous publisher manages one message a second |
| `Transport.MetadataTTL` | 6 s | 1 s | kafka-go refreshes metadata on a timer and on nothing else, so this is the window in which a leader election fails publications |
| `Writer.WriteBackoffMin`/`Max` | 100 ms / 1 s | 250 ms / 1 s | so that `MaxAttempts` attempts SPAN the metadata TTL and the retry can find the new leader |
| `ConsumerGroupConfig.JoinGroupBackoff` | 5 s | 1 s | a transient coordinator error would otherwise be a five-second outage for the subscription |
| `ReaderConfig.MaxWait` | 10 s | 500 ms | a cancelled `Fetch` cannot return until the read in flight does |

## Topic / subscription mapping

| iqmq | Kafka |
|---|---|
| topic | topic, prefixed with `Config.TopicPrefix` verbatim |
| subscription | consumer group |
| `Message.Key` | record key (murmur2, the same formula the franz-go driver and the Java client use) |
| `Message.Headers` | record headers, binary and duplicate-preserving, in order |
| `Message.Body` | record value — **see the gotcha below: this client cannot READ BACK the difference between a null value and an empty one** |
| `Message.ID` | `iqmq-id` header |
| `Properties.CreatedAt` | record timestamp, with an `iqmq-created` marker so that a record the publisher did not stamp does not acquire a creation time |
| lane | partition |
| `DeliveryInfo.Offset` | record offset (Kafka's sequence IS the offset, so `Sequence` stays unset) |

The topic mapping is the identity plus the prefix. Kafka's grammar —
`[a-zA-Z0-9._-]`, at most 249 characters, never `.` or `..` — is a subset of
what the specification allows, so a name the broker would refuse is refused here rather
than transformed: a Kafka topic name is something operators type into
`kafka-acls` and Grafana queries, and a driver that silently renamed it would
point the deployment's own tooling at a topic nobody configured.

A consumer group name is required. kafka-go refuses an empty
`ConsumerGroupConfig.ID`, Kafka has no anonymous group, and a driver that
invented one would give two processes the same durable position by accident.

## Publish path & confirmation boundary

`acks=all`. A record's reply arrives when every in-sync replica has it in the
log, so **`ConfirmAccepted` and `ConfirmDurable` are the same boundary** and
neither costs an extra round trip or is refused. `ConfirmNone` returns as soon
as the record is in the writer's batch, and the message's storage is copied
first (lets a caller reuse its buffers the instant `Publish` returns).

**There is no idempotent producer.** kafka-go's writer sends no producer id and
no sequence numbers, so the broker cannot de-duplicate the writer's own
retries: a retried produce whose predecessor was written and whose response
was lost leaves TWO copies in the log, and `Config.MaxAttempts` (3 by default)
bounds how many. `FeatureIdempotentPublish` is therefore **unsupported** here
and native on the franz-go driver, and `Backend.DuplicablePublications()`
counts the publications that may have duplicated.

The driver owns **two** writers because `Async` is a field of a
`kafka.Writer` and not of a call: one with `Async` false for the publications
that wait, one with `Async` true for `PublishAsync`, `PublishOwned` and a sink
configured `ConfirmNone`. They share one transport, so the second costs no
connections.

**Batching.** kafka-go produces a batch when it is FULL or when its timer
fires; there is no "the caller has stopped publishing, send what you have".
So `Config.BatchMaxRecords` defaults to **one** — a synchronous publisher pays
no linger — and a caller that publishes in batches raises it and pays
`Config.BatchTimeout` on any partially filled batch. With a
batch size of one a `PublishBatch` of a hundred costs a hundred produce
requests; raise the batch size to batch.

A message larger than `Config.BatchMaxBytes` makes kafka-go refuse the whole
`WriteMessages` call before it writes anything. In a `PublishBatch` the driver
refuses only that message, with `ErrMessageTooLarge` at its own index, and
writes the others without it, so the result is a `*BatchError` that blames the
one message.

**`Flush` is the driver's own.** `kafka.Writer` has no flush: its only flush is
`Close`, which is terminal and would end every other sink on the backend. So
the driver counts the publications it has admitted and not yet had a reply
for, and `Flush` waits for that count to reach zero — which is exact rather
than a timer.

**`Close` ends what the writer is still retrying.** kafka-go's writer tries a
failed batch up to `Config.MaxAttempts` times, each attempt bounded by
`Config.WriteTimeout` and each retry behind a backoff sleep that nothing
interrupts, and `Writer.Close` waits for every batch. Closing a publisher ends
the wait of a `Publish` or `PublishBatch` on it at once. The client's `Close`
then closes the transport's connections before it closes the writers: the
attempt under way fails where it is, a new connection is refused, and the
writer does not retry either failure. So `Close` returns within the backoff a
batch is already sleeping, at most `Config.WriteBackoffMax` (1 s), rather than
after every attempt. A publication ended this way is ambiguous, because an
earlier attempt may have been appended. The driver closes connections rather
than cancelling the round trips because kafka-go's connection goroutine goes
on encoding a request whose round trip has been cancelled, and the writer
would hand the record's storage back while it was still being read. `Close`
does not flush: a batch the writer has not sent yet fails with the rest, which
is the core's prompt `Close`, and `Shutdown` flushes first. A writer passed to
`OpenWithWriter` dials through its own transport, which the driver cannot
close, so the client's `Close` still waits for that writer's attempts.

### Ambiguity

Everything that fails after the record entered the writer is **ambiguous**, not
failed: `ErrSettlementUnknown` (`CodeSettlementUnknown`) with `IsAmbiguous`
true and the native cause underneath. The window is **wider** here than on
franz-go and kafka-go says so itself: when `WriteMessages` returns early
because the caller's context is done, "there are no guarantees made on whether
messages were written to kafka, they might also still be written after this
method has already returned". The caller's context ends the WAIT, not the
WRITE.

| Outcome | Portable code | Ambiguous |
|---|---|---|
| `NOT_ENOUGH_REPLICAS_AFTER_APPEND` | `CodeSettlementUnknown` | yes — by its own name, an append that happened |
| `REQUEST_TIMED_OUT` | `CodeSettlementUnknown` | yes — the append may have happened |
| the caller's deadline or cancellation, after submission | `CodeSettlementUnknown` | yes — the write continues |
| `NOT_ENOUGH_REPLICAS` | `CodeUnavailable`, retryable | no — refused before appending |
| `MESSAGE_TOO_LARGE`, `RECORD_LIST_TOO_LARGE` | `CodeMessageTooLarge` | no |
| a protocol code this driver's table does not name | `CodeUnavailable` + retryable if Kafka calls it retriable, else `CodeUnknown` | **no** |

The table classifies one attempt. kafka-go's writer tries a batch again on a
retriable answer, up to `Config.MaxAttempts` times, and returns the last
attempt's answer alone, so a `REQUEST_TIMED_OUT` that was appended and then a
`NOT_ENOUGH_REPLICAS` on the retry used to come back as the definite refusal
while the record sat in the log. So with `MaxAttempts` above one, a retriable
answer the writer gave up on is `CodeSettlementUnknown` and ambiguous whatever
its code, and the native code stays underneath. A `NOT_ENOUGH_REPLICAS` is
reported as the table says only with `MaxAttempts` 1. An answer the writer
does not retry still reads as its own row, even after attempts it did retry,
because nothing kafka-go returns tells the two cases apart. A writer handed to
`OpenWithWriter` makes its own `Writer.MaxAttempts` attempts (ten when that is
zero), so the driver counts those and ignores `Config.MaxAttempts`.

That last row is a defect this increment fixed twice, and both times the cause
was the same: `kafka.Error` is an integer type with `Error`, `Timeout` AND
`Temporary` methods, so it satisfies `net.Error`, and driverkit's transport
classification reads a `net.Error` on a send path as ambiguous. First, every
one of the 55 protocol codes the table did not name came out of a publish as
"the broker may have written it" for answers the broker gave INSTEAD of writing
anything. Then the same thing happened to the codes the
table DOES name, when `MapNet` began OR-ing the transport classification's
ambiguity into a matching rule's : the driver now routes a
protocol code through `Map`, which honours the rule table alone, and only a
cancellation, a deadline, an EOF or a genuine `net.Error` through `MapNet`.
driverkit cannot know that this client's protocol error
type is not a transport error; the driver can.

## Consume path

`kafka.ConsumerGroup` for membership and `kafka.Client.Fetch` for reading —
**neither** of kafka-go's two convenient consumer surfaces whole, and both
choices are requirements rather than preferences.

**Membership** is `ConsumerGroup` and not `Reader` because a `Reader`
owns group membership and never tells its caller that a rebalance happened, so
a driver built on it cannot refuse a settlement for a partition it no longer
owns — it would acknowledge a record another member is processing.

**Reading** is `Client.Fetch` and not a per-partition `Reader` because of
The specification: a Kafka record value is a nullable byte string, and a `Reader` reads
records through kafka-go's legacy `Conn`/`Batch` path, whose `readNewBytes`
returns nil for any length at or below zero — so it collapses an empty body
and an absent one into the same nil `Body`, and the same for an empty key. The
modern protocol path keeps them apart. This driver failed the specification's
`publish/roundtrip-bodies` until the read path moved.
Fetching directly also puts the consume path on the same `Transport` as the
producer and the administrative client, so the connection pool, the metadata
cache and the credential are one thing rather than two — and it replaces the
Reader's buffering with the source's own bounded record queue
(`Config.QueueCapacity`), its read backoff with one fetch wait, and its opaque
offset-out-of-range behaviour with an explicit re-resolve to the log's earliest
offset. When that offset is below the lost position, because the topic was
recreated or the log truncated, the partition starts again the way a new
assignment does. The new log's records are delivered from its earliest offset,
and a delivery handed out from the old log reports `ErrLeaseLost` when it is
settled, because its offset now names a record of the new log.

A generation is the assignment. The driver starts **one** function per
generation through `Generation.Start` — not one per partition — because
kafka-go ends a generation as soon as any started function returns and waits
for all of them before it rejoins, so one function is the only place the specification's
sequence can run. That function spawns a reader goroutine
per assigned partition, waits for the generation's context, and then performs
the sequence below.

`Fetch` is one blocking receive from the fetch loops' shared bounded channel
followed by a non-blocking drain, which is the specification's "one blocking wait then a
non-blocking drain". The drain is what makes a batch a batch: one fetch returns
a record batch and the loops hand its records to the channel one at a time, so
a driver that returned after its first receive would fill one slot of a
caller's batch whatever the backlog.

**Pausing.** kafka-go offers no per-partition pause on either consumer
surface. A partition whose tracking window is full is held back by the
source's own backlog and by the bounded channel the fetch loops write into,
which is back-pressure by construction rather than a client-side pause.

### The rebalance sequence 

1. **Stop dispatch.** Nothing more is handed out and the redeliveries the
   partitions were holding are dropped: they belong to whoever takes the
   partition next.
2. **Stop the readers and discard what they had read.** Every queued record
   carries the generation it was read under, so a record left over from an
   ending generation is dropped rather than re-basing the next generation's
   tracker.
3. **Wait, bounded** (`Config.RevokeWait`, 5 s), for the in-flight deliveries
   to settle. Bounded because kafka-go will not rejoin until this returns, and
   a member that overruns the group's rebalance timeout is removed from the
   group — which is strictly worse, because a removed member cannot commit.
4. **Commit the safe boundary synchronously**, through the ENDING generation.
   The safe boundary is each tracker's watermark and never the highest offset
   seen. It is read in the same step that stops accepting the wait's
   settlements, so a settlement that arrives after the wait reports
   `ErrLeaseLost` instead of completing an offset the commit does not carry.
5. **Invalidate the generations**, so a settlement that arrives afterwards
   reports `ErrLeaseLost`.

**Every rebalance is eager.** kafka-go's assignors are range and round-robin
and there is no cooperative one, so a generation revokes the WHOLE assignment
and hands it back: a member that gains one partition reports `ErrLeaseLost` for
every delivery it was holding, where the franz-go driver keeps the ones whose
partitions did not move. That is the difference
`TestCriticalRebalanceWithDeliveriesInFlight` asserts in both drivers, with
opposite expected outcomes.

**There is no revoked-versus-lost distinction.** franz-go calls
`OnPartitionsRevoked` for a cooperative handover and `OnPartitionsLost` when
the coordinator has already taken the partitions away, and commits only in the
first case. kafka-go ends a generation the same way whichever it was and does
not report why, so this driver ALWAYS attempts the safe-boundary commit and
reads the answer: a commit the coordinator refuses IS the lost case, and
`Backend.LostPartitions()` counts it. Attempting it is safe
because a refused commit changes nothing; not attempting it would turn every
cooperative handover into redelivery.

## Ack / nack / extend mapping

| Portable | Kafka |
|---|---|
| `Ack` | the offset completes in `driverkit/offset`'s tracker; the watermark is committed by the ticker, by `Backend.Commit` or when the generation ends |
| `AckBatch` | one cumulative `OffsetCommit` whatever the batch's size |
| `Nack` | `Config.Nack`'s strategy |
| `Reject` | the dead-letter route, or `ErrUnsupported` when none is configured |
| `Extend` | **not implemented**, and that is a decision: Kafka grants no per-record lease, so there is nothing an `Extend` could move |

A settlement is resolved through a token (the partition's slot, stable for the
life of the source) and a generation (the tracker's, which rises on every
rebalance). A settlement whose generation has been superseded reports
`ErrLeaseLost` — in **both** directions, positive and negative, because
neither may reach a partition somebody else owns. The generation is checked
again when the settlement takes effect, so one that began before the end of a
generation and lands after it is refused too: a nack that retries in place
holds nothing into the next generation.

## Ordering

A partition is the unit and a key maps to a partition by **murmur2**, which is
what makes per-key order hold across a mixed-client fleet: a Go producer that
hashed differently would split one key's order across two partitions, silently
and only in a mixed-language deployment. The implementation is the specification's
`internal/hash.KafkaPartition`, the same one the franz-go driver uses, and
`TestCrossDriverPartitioning` plus `FuzzCrossDriverPartitioning` hold the two
drivers to the same answer.

A **keyless** record rotates every `Config.BatchMaxRecords` records rather than
sticking to one partition per batch. franz-go's partitioner is told when a
batch ends (`TopicPartitionerOnNewBatch`) and re-rolls then; kafka-go's
`Balancer` interface has no such notification, so the run length is the only
thing stickiness can be keyed on here and the writer's own batch size is the
honest choice. At the default batch size of one this is
plain round-robin, which is kafka-go's own default.

Global order holds only for a single-partition topic, which is a topology the
deployment chooses, so `FeatureOrderedGlobally` is **conditional** with the
condition stated.

## Delays

Kafka has none. A publication cannot be delayed or scheduled and
`PublishOptions.Delay`/`DeliverAt` are refused with `ErrUnsupported` before
anything is written. A *redelivery* delay is the consumer's business and
reaches it through the nack strategy: block-partition holds the record in this
process and redelivers it after the delay, so the delay is volatile — it is
lost if the process dies, and the record is redelivered immediately by whoever
takes the partition. `FeatureDelayedRedelivery` is advertised only when
`Config.Nack` resolves to block-partition or custom.

## Dead-letter queue

`Config.NackConfig.DeadLetterTopic`, and the route is iqmq's own: the driver
publishes the record there with the provenance headers
(`iqmq-origin-topic`, `iqmq-origin-subscription`, `iqmq-origin-partition`,
`iqmq-origin-offset`, `iqmq-attempts`, `iqmq-failure`,
`iqmq-first-delivered-at`) and commits the original offset **only after that
publication is confirmed**. A failed or ambiguous move never acknowledges the
original, which is the rule and what
`TestCriticalDeadLetterPublicationFails` and
`TestAmbiguousRepublishNeverAcksTheOriginal` prove.

A `Reject` on a client with no destination configured is refused with
`ErrUnsupported` rather than destroying the message.

Note what happens after an **ambiguous** move: the core refuses a second
settlement of that delivery with `ErrSettlementUnknown`, because iqmq cannot
tell whether the record reached the destination. What the driver owes is that
the original offset was not committed, so the record returns to whoever
reopens the subscription.

## Headers & limits

Kafka's record headers are ordered, binary and duplicate-preserving, and its
key and value are nullable byte strings, so **three of the four metadata
modes behave identically**: strict refuses nothing, adaptive envelopes nothing
and lenient drops nothing. `Backend.DroppedMetadata()` is always zero and
exists to say so. Only `MetadataPortableEnvelope` changes the bytes on the
wire. The record key is still the message key under it, because the balancer
and the broker partition, order and compact by the key without reading the
value; the envelope carries a second copy for the consumer.

A caller's header under the `iqmq-` prefix is refused, not renamed.

!!! warning "Upgrade producers before consumers"
    A record enveloped by a version of this driver that predates the marker
    carries none, so a `MetadataAdaptive` consumer on the NEW code delivers it
    natively — the envelope's own bytes as the payload — rather than decoding
    it. The pairing that breaks is narrow: a `MetadataPortableEnvelope`
    producer read by a `MetadataAdaptive` consumer, for records already on the
    topic when the consumers are upgraded. A `MetadataPortableEnvelope`
    consumer is unaffected in either direction (the mode says every value is an
    envelope and it never consults the marker), and an upgraded producer is
    read correctly by an OLD consumer, because the old code sniffs the body and
    the extra header is invisible to it. So: producers first, then consumers,
    or drain the topic across the change.

| Limit | Value |
|---|---|
| `MaxMessageBytes` | the cluster's own `message.max.bytes`, read at open through `DescribeConfigs` |
| `MaxBatchBytes` | the same figure: the broker bounds a batch by it too |
| `MaxHeaderCount`, `MaxHeaderBytes`, `MaxBatchMessages`, `MaxDelay`, `MaxLease` | **unknown** — Kafka bounds none of them on its own, and the specification forbids reading an unadvertised limit as zero |

Reading `message.max.bytes` takes two round trips: kafka-go's
`DescribeConfigs` takes a resource NAME, so a broker's configuration can only
be asked for by broker id, and the id comes from the metadata. franz-go's
`kadm` can be called with no id at all — and that is exactly what made the specification's
driver advertise no limit, because the resource an absent id names is the
cluster-level one, which `apache/kafka:4.3.1` answers with the DYNAMICALLY set
broker configuration and nothing else. `message.max.bytes` has source
`DEFAULT_CONFIG`, so it is not in that answer; it is in a named broker's
effective configuration. Both drivers now name the broker.

## Reconnect

kafka-go offers **no connection hook of any kind**: neither `Writer` nor
`Transport` nor `Dialer` reports a dial, a dial failure or a disconnection, and
`driverkit.Reconnector` cannot be used either because the client owns its own
dial loop. So the events this driver reports are the ones it drives itself
:

- a generation **established** is `Ready`, and every generation after the first
  is counted by `Backend.Reconnects()` and fires `Hooks.OnReconnect`;
- a generation **ended** is `Degraded`, because the subscription is not
  consuming until the next one is established;
- a fetch error, a commit failure or an undecodable record is `Degraded` with
  the mapped reason.

`Reconnects()` is therefore **not a count of sockets**: a group rejoins for a
rebalance as well as for a broken link. A caller that needs socket-level
health polls `Ping`, which is a metadata round trip.

## At-least-once window

A delivery is acknowledged when its offset has completed in the tracker AND
the resulting watermark has been committed. The two are not simultaneous, so
the window is "everything completed since the last commit":

| Closed by | When |
|---|---|
| the commit ticker | every `Config.CommitInterval` (250 ms) |
| the end of a generation | synchronously, through the ending generation, before kafka-go rejoins |
| `Close` | by closing the consumer group, which ends the generation and runs the sequence above while the coordinator connection is still open |
| `Backend.Commit(ctx)` | at a point of the caller's own choosing, one round trip per source |

A source with **no live generation commits nothing**, and that is not a
shortcoming to work around: kafka-go's `OffsetCommit` carries the generation
and the member id, so an offset committed between generations would be refused
by the coordinator — or, worse, accepted for a generation this member no longer
holds.

The window opens **wider than `CommitInterval`** when a generation ends before
the boundary can be committed, which `Backend.LostPartitions()` counts. A
non-zero value means duplicates reached the application.

## Config & DSN

```
kafkago://user:password@broker1:9092,broker2:9092/prefix?sasl=scram-sha-512&tls=true
```

The path is `Config.TopicPrefix`, used verbatim, separator included. Every
parameter is in `ParseDSN`'s doc comment; `dsn.Params.Strict` turns a parameter
this driver does not have into a construction error naming it, rather than a
setting nobody notices was ignored.

**Three of the franz-go driver's parameters are absent rather than ignored**,
and asking for one is an error naming it: `disableidempotency` (there is no
idempotent producer to disable), `transactionalid` (there is no transactional
producer) and `instanceid` (kafka-go has no static group membership).
`linger` is `batchtimeout` here and `unboundeddelivery` has no analogue.

`Config.Validate` is pure: no I/O, no dial. It refuses one thing kafka-go
would otherwise PANIC on — `FetchMinBytes` above `FetchMaxBytes`, which
`kafka.NewReader` panics on, from a goroutine this driver started, long after
the configuration was accepted.

Every credential is a `dsn.Secret`, so neither `String`, nor `LogValue`, nor an
error built from a configuration can carry it.

## Known gotchas

1. **Do not build your own reader with `kafka.NewReader` and expect an empty
   body back.** A `Reader` collapses a null record value and a zero-length one
   into the same nil `Body` (`readNewBytes` returns nil for any length at or
   below zero), and the same for the key. This driver does not use one for
   exactly that reason ; a caller reaching past the driver
   through `Backend.Client()` should fetch, not read.
2. **A publication may leave two copies in the log.** There is no producer id,
   so `Config.MaxAttempts` is the number of duplicates one publication may
   produce. Set it to 1 and reconcile the ambiguous outcome yourself if you
   cannot tolerate that.
3. **Every rebalance costs the whole assignment.** Eager assignors only. A
   scale-up is a burst of `ErrLeaseLost` for every in-flight delivery, not
   just the moved partitions'.
4. **A batch size above one makes a synchronous publish wait.** There is no
   immediate-flush path, so `Publish` pays `BatchTimeout` per message when
   `BatchMaxRecords > 1`. The default is 1 for that reason.
5. **A `PublishBatch` at the default batch size is one request per message.**
   Raise `BatchMaxRecords` to the batch you publish.
6. **`Purge` does not destroy records.** It commits the end offsets for the
   group, so the subscription resumes at the end and the backlog is skipped.
   The records stay in the log for whoever else is reading it.
7. **`Reconnects()` counts generations, not sockets.**
8. **A commit's context does not bound it.** `Generation.CommitOffsets` takes
   no context: the round trip is bounded by the coordinator connection's own
   timeout, which is `Config.RequestTimeout`.
9. **`WatchPartitionChanges` is off by default.** A rebalance is eager here, so
   a poll that notices a new partition stops every member's consumption.
10. **Every delivery copies its key, value and headers once.** A fetched
    record points into the fetch response's page buffers, which kafka-go
    documents as valid only until the record set is closed, and a delivery
    outlives the fetch it came from. `MessageFrom` is where that copy is made,
    and it is what preserves the nil-versus-empty distinction as well.
11. **A cancelled `Publish` does not cancel the produce**, and neither does
    closing its publisher, so the record's storage is still being read after
    `Publish` has returned. The driver
    reference-counts the pooled buffer for that reason ;
    a caller reusing its own key or body buffer immediately after an
    AMBIGUOUS `Publish` is writing storage kafka-go may still be
    serialising, so a caller that pools its own payloads should treat an
    ambiguous outcome as "still in flight" rather than "done".
12. **There is no pattern subscription at all**, where franz-go has one this
    repository does not wire. `CheckTopic` refuses a name carrying a
    regular-expression metacharacter with `ErrUnsupported` rather than
    "invalid topic", so the difference is visible.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

Every one of these needs control over the broker that the portable API does not
have, so they run against `kfake` — franz-go's in-process Kafka broker, which
is a real protocol server on a real socket and a **test-only** dependency of
this module. `GOWORK=off go build ./...` links neither kfake nor franz-go.

| Test | What it would catch |
|---|---|
| `TestCriticalRebalanceWithDeliveriesInFlight` | a settlement landing on a partition another member now owns. Its outcome is the OPPOSITE of the franz-go driver's, because the assignors are eager |
| `TestCriticalRevokedDeliveryReportsLeaseLost` | a fenced member acknowledging a record it no longer owns, and a record not coming back when its boundary could not be committed |
| `TestCriticalOutOfOrderCompletion` | a commit passing an offset that has not completed, which loses every record still in flight below it |
| `TestCriticalLeaderChange` | a leader election reported as a definite publication failure. This one FOUND the metadata-staleness defect above |
| `TestCriticalRetryTopicPublicationFails` | a record acknowledged on its source topic and written nowhere |
| `TestCriticalDeadLetterPublicationFails` | the same on the dead-letter route, which the specification names |
| `TestCloseRacesSettlement`, `TestCloseRacesPublication` | 2,000 crossings each under `-race`: a settlement or a pooled buffer landing after `Close` has drained |
| `TestCloseEndsARetryingPublish` | a client `Close` that waits out kafka-go's retries, or an attempt the broker holds unanswered, before it returns |
| `TestASettlementAfterTheRevokeWaitIsRefused` | a settlement accepted while the ending generation's commit, read without it, is on the wire |
| `TestCancelledPublishDoesNotRecycleALiveBuffer` | a pooled buffer recycled while the writer is still serialising the record it holds. This one FOUND the buffer-lifetime defect: `-race` reports a write in `Encode` against a read in kafka-go's `RecordSet.writeToVersion2` |
| `TestCrossDriverNullAndEmptyBodiesSurvive` | the nil-versus-empty body, over all four crossings between the two Kafka drivers. This one FOUND the read-surface defect: a `kafka.Reader` collapses them and `Client.Fetch` does not |
| `TestCrossDriver*`, `FuzzCrossDriver*` | the two Kafka drivers drifting apart on the reserved headers, the topic grammar, the partition formula or the portable error codes |
