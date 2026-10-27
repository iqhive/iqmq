# kafka (franz-go)

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 1 · **Owner:** iqmq maintainers

## Client & version policy

The native baselines for this driver live in `benchmarks/native-baselines/` and measure the clients below against the harness's `redpanda` image (`make bench-native CLIENTS="franzgo kafkago sarama"`). A driver number is only comparable with a baseline taken from the same client version, broker version and confirmation boundary, so the pins move together; each baseline's package comment records the settlement boundary it measures as well.

| Baseline | Client | Pinned version | Confirmation boundary |
|---|---|---|---|
| `franzgo` | github.com/twmb/franz-go | `github.com/twmb/franz-go@v1.22.2` | acks=all on an idempotent producer (client default), ProduceSync |
| `kafkago` | github.com/segmentio/kafka-go | `github.com/segmentio/kafka-go@v0.4.51` | RequiredAcks=RequireAll set explicitly, synchronous WriteMessages |
| `sarama` | github.com/IBM/sarama | `github.com/IBM/sarama@v1.61.1` | Producer.RequiredAcks=WaitForAll with Idempotent=true, SyncProducer |

### Verified facts (the API contract)

Checked while building the native baselines; re-verify when a pin moves.

- **franz-go defaults.** franz-go produces with acks=all and an idempotent producer by default; idempotency is only turned off by DisableIdempotentWrite, and its default partitioner (UniformBytesPartitioner) hashes keys with murmur2, so partition assignment matches the Java client.
  _Verified against: franz-go v1.21.6 pkg/kgo/config.go:663,672 and pkg/kgo/partitioner.go:254,278._
- **Sarama defaults.** Sarama's NewConfig defaults are weaker than franz-go's: Producer.RequiredAcks is WaitForLocal (acks=1) and Producer.MaxMessageBytes is 1 MiB; an idempotent producer additionally requires RequiredAcks=WaitForAll, Net.MaxOpenRequests=1 and Retry.Max>=1.
  _Verified against: Sarama v1.60.2 config.go:569-570,811-824._
- **Sarama partitioner compatibility.** Sarama's default hash partitioner uses FNV-1a, not Kafka's murmur2, and NewReferenceHashPartitioner only fixes the sign handling; matching the Java client exactly needs NewCustomPartitioner with a murmur2 hash function.
  _Verified against: Sarama v1.60.2 partitioner.go:170-195,250-264; franz-go pkg/kgo/partitioner.go:540-560._
- **kafka-go maintenance and acks.** segmentio/kafka-go carries no maintenance-mode or deprecation notice for the project, but kafka.NewWriter and kafka.WriterConfig are deprecated in favour of the Writer struct — and a zero-valued Writer.RequiredAcks means RequireNone, because only the deprecated constructor rewrites 0 to RequireAll.
  _Verified against: kafka-go v0.4.51 README.md and writer.go:499-511._
- **message size.** The broker's default message.max.bytes is 1 MiB, so a 100-message batch of 64 KiB payloads must be split; Sarama splits a produce set when a partition's buffer would reach Producer.MaxMessageBytes, and franz-go splits at ProducerBatchMaxBytes.
  _Verified against: Sarama v1.60.2 produce_set.go:316-336; Kafka broker default._

## Topic / subscription mapping

| iqmq | Kafka |
|---|---|
| topic | topic, prefixed with `Config.TopicPrefix` verbatim |
| subscription | consumer group |
| `Message.Key` | record key (murmur2 partitioner, Kafka-compatible) |
| `Message.Headers` | record headers, binary and duplicate-preserving, in order |
| `Message.Body` | record value (`nil` is a tombstone and stays `nil`) |
| `DeliveryInfo.Partition` / `.Offset` | partition and offset, always populated |
| ordering lane | partition |

The mapping is the **identity** plus the prefix, and a topic Kafka cannot carry
is refused rather than transformed. Kafka's grammar — `[a-zA-Z0-9._-]`, at most
249 bytes, never `.` or `..` — is a subset of what the specification allows, so:

- a name outside it is `CodeInvalidArgument` before any I/O;
- a name carrying a regular-expression metacharacter is `ErrUnsupported`, on
  the assumption that somebody is asking for a pattern subscription (see
  **Known gotchas**);
- `Config.TopicPrefix` is used **verbatim**, separator included. `acme.` gives
  `acme.orders`. The driver does not insert a separator of its own, because
  Kafka has none and inventing one would make the deployment's own tooling —
  `kafka-acls`, a Grafana query — point at a topic nobody configured.

A subscription is **required**: Kafka has no anonymous group, and a driver that
invented one would give two processes the same durable position by accident.

Everything the specification names that Kafka has no native field for travels as a header
under the reserved `iqmq-` prefix, and a caller's own header under that prefix
is refused: `iqmq-id`, `iqmq-content-type`, `iqmq-content-encoding`,
`iqmq-correlation-id`, `iqmq-causation-id`, `iqmq-reply-to`. `Properties.CreatedAt`
uses the record's **native timestamp**, marked by an empty `iqmq-created`
header — because Kafka stamps every record whether the publisher set a time or
not, so a decoder that read the timestamp unconditionally would invent a
creation time for every message that carried none. The creation time therefore
round-trips at **millisecond** resolution, which is Kafka's.

## Publish path & confirmation boundary

`acks=all` with an **idempotent producer**, which is franz-go's own default and
is not turned off unless `Config.DisableIdempotency` says so. A record's
promise fires when every in-sync replica has the record in its log.

| mode | boundary |
|---|---|
| `ConfirmNone` | the record is in the producer's buffer. `Flush` is what waits for it. |
| `ConfirmAccepted` | every in-sync replica has the record in its log. |
| `ConfirmDurable` | **the same boundary.** Kafka's strongest ordinary acknowledgement is already a durable one, so `ConfirmDurable` costs no extra round trip and is never refused. |

All four optional publication surfaces are implemented:

- **`BatchSink`** — every record is handed to the producer and the call waits
  for all of their promises. The result is index-aligned with the input, which
  is the whole contract: franz-go's promises fire in whatever order the
  partitions resolve, so a driver that collected results in completion order
  would report message seven's failure against message two.
- **`AsyncSink`** — franz-go's produce *is* asynchronous, so the promise is the
  callback. No goroutine per message.
- **`OwnedSink`** — the release happens in the promise, which is the earliest
  moment it can: nothing is copied.
- **`FlushSink`** — for what `ConfirmNone` left in the buffer.

`ConfirmNone` **copies** the key, body and header values into a pooled buffer,
because lets the caller reuse its buffers the instant `Publish` returns
and franz-go is still reading them. The pool means the steady state allocates
nothing; the other modes wait for the promise and copy nothing.

### The ambiguity, which is the part to read

Everything that goes wrong **after the record has entered the producer's
buffer** is reported as the specification's `ErrSettlementUnknown` with
`OpError.Ambiguous` set, and the native cause is still reachable through
`errors.Is`/`errors.As`:

| condition | why it is unknown |
|---|---|
| `REQUEST_TIMED_OUT` | the broker ran out of time; the append may have happened |
| `NOT_ENOUGH_REPLICAS_AFTER_APPEND` | by its own name, the record **was** appended |
| `kgo.ErrRecordTimeout`, `kgo.ErrRecordRetries` | franz-go gave up after several attempts; one may have landed |
| `OUT_OF_ORDER_SEQUENCE_NUMBER`, `UNKNOWN_PRODUCER_ID` | the idempotent sequence is broken; the client no longer knows what the broker has |
| a context cancellation or deadline after buffering | the client has the record and the connection is open |

Reporting any of them as a *definite* failure is how a caller retries and
publishes twice while believing it published once. The other direction matters
as much: `MESSAGE_TOO_LARGE`, `TOPIC_AUTHORIZATION_FAILED`,
`INVALID_REQUIRED_ACKS` and `INVALID_RECORD` are definite refusals, and marking
those ambiguous would make every caller reconcile a publication that certainly
did not happen.

`NOT_ENOUGH_REPLICAS` is a definite refusal *as an error* and reaches a caller
as an ambiguous one *as an outcome*, because franz-go retries it: by the time
the record is failed there have been several attempts and the client cannot
know whether one of them landed. The rule table says the first and the
behaviour says the second; both are right.

**`Config.UnboundedDelivery`** is the knob behind this. franz-go refuses by
default to let a delivery timeout or the caller's own cancellation fail an
in-flight *idempotent* record, so a `Publish` against a cluster answering
`REQUEST_TIMED_OUT` **never returns** — which the specification forbids. The driver
therefore sets `AllowIdempotentProduceCancellation` and bounds the record with
`Config.DeliveryTimeout` (2 minutes, Kafka's own `delivery.timeout.ms`
default), reporting the outcome as unknown. Setting `UnboundedDelivery`
restores franz-go's behaviour for a caller who would rather wait for ever than
reconcile. It has no effect on a transactional producer, where franz-go refuses
the option.

Closing a publisher ends a `Publish` or `PublishBatch` that franz-go is still
retrying. The sink's `Close` cancels the context the records were produced
under, franz-go fails them when it next comes to retry them, and the outcome is
ambiguous. The client's `Close` used to wait out the whole
`Config.DeliveryTimeout` first. Under `UnboundedDelivery` franz-go does not let
a cancellation fail a record it has already sent, so `Close` waits for the
broker's answer as the `Publish` does.

`Config.Linger` is **zero** by default, which is Kafka's own `linger.ms`
default. A non-zero default looks attractive and is wrong for a synchronous
publisher: `Publish` waits for the promise, so every publication pays the
linger before its record is even sent.

## Consume path

One `*kgo.Client` per source, with a consumer group, because a franz-go client
consumes at most one group. `Fetch` is the poll:

That order is what keeps a partition's records in order — a fetch that went to
the broker first would hand out a later offset before the earlier one it is
already holding.

- **`cooperative-sticky`** balancer, so a rebalance does not stop the world for
  partitions nobody is taking away.
- **auto-commit off.** The commit is the acknowledgement; a client committing on
  a timer would acknowledge records the application has not finished.
- **`BlockRebalanceOnPoll`**, so the rebalance callbacks never run while a
  fetch is filling the driver's tables. `AllowRebalance` is deferred, because a
  member that forgot it would hold the group's rebalance until the
  coordinator's timeout and then be removed from the group.
- **`PollFetches`, not `PollRecords`.** franz-go buffers each fetch *response*
  separately and `PollRecords` serves one at a time; a Kafka fetch returns as
  soon as one byte is available, so the first response of a burst routinely
  holds a single record. A driver built on `PollRecords` fills one slot of a
  caller's batch and looks as though it cannot batch at all.

**Start positions.** All four work, and they are made **durable** at
`OpenSource`: the resolved offset is committed for every partition the group
has no position for, so a subscription that was opened and closed without
consuming anything still remembers where it was asked to start. Every position
is **clamped** into the log's own range — a sequence beyond the end resolves to
the end, one below the start to the start, and a time no record is at or after
to the end — because an out-of-range fetch offset is an error the caller cannot
act on, and Kafka's own `auto.offset.reset` exists for the same reason.
`StartAtSequence(n)` is the Kafka **offset** `n`, and a Kafka offset is
zero-based.

## Ack / nack / extend mapping

| iqmq | Kafka |
|---|---|
| `Ack` | the offset completes in `driverkit/offset`'s tracker; the watermark is committed on the ticker, at a revoke and at `Close` |
| `AckBatch` | the same, and the commit is one round trip whatever the batch size — the acknowledgement is cumulative |
| `Nack` | `Config.Nack`'s strategy; see below |
| `Reject` | `NackActionDeadLetter`: published to `Config.NackConfig.DeadLetterTopic`, then the offset completes |
| `Extend` | **`ErrUnsupported`.** Kafka grants no per-record lease |

An `Ack` means *this offset has completed*, not *this offset is committed*. The
tracker computes the offset after the highest record whose completion can no
longer lose work, and a failed or unresolved offset below it blocks the commit —
which is what makes the committed offset **safe** rather than merely recent.
Acknowledging therefore costs no round trip, and the price is the
at-least-once window (below).

`Backend.Commit(ctx)` commits every open source's watermark synchronously, for
a caller that wants the window closed at a point of its own choosing.

### Nack strategies 

Kafka has no per-record negative acknowledgement, so what a `Nack` does is a
**policy**, and the specification requires all four to exist and none to be selected
silently — because two of them reorder a partition and one halts a
subscription.

| `Config.Nack` | what it does | ordering | throughput |
|---|---|---|---|
| `nack.BlockPartition` (**default**) | holds the record in process, pauses the partition and hands out nothing later of it until the record is redelivered after the delay; the offset stays uncommitted | preserved, in the order records are handed out (below) | the partition stops while a record is retried |
| `nack.RetryTopic` | publishes to `<topic>.retry.<n>` with the attempt in a header, then commits the original offset | **broken** for the moved record | unaffected |
| `nack.Stop` | stops the consumer; nothing after the refused record is consumed and the record is not acknowledged | preserved | zero until somebody restarts it |
| `nack.Custom` | the caller's own function, whose plan is validated | its own | its own |

While block-partition holds a record, the topic's other partitions keep
flowing. The block ends when the record is redelivered, not when it is
acknowledged, so the records behind it can arrive in the same `Receive` as the
redelivery. A record already handed out is not called back: one that was out
before the nack, such as the rest of the same batch, can be handled before the
redelivery, and one that arrived with the redelivery can be handled before the
next attempt if the redelivery fails again. A custom plan that sets `Pause`
gets the same block.

`Auto` resolves to `BlockPartition` **by specification** and not by
measurement: the specification fixes the default independently of any number, so a
campaign that finds retry topics faster records the margin a caller pays for
ordering rather than moving the default.

When a record's attempts are exhausted (`NackConfig.MaxAttempts`) or its retry
levels run out (`NackConfig.Retries`), the configured failure policy applies: the
dead-letter topic when one is configured, and a **halt** when one is not. The
halt surfaces from `Receive` as `CodePoisonMessage`.

## Ordering

A **partition** is the ordering lane, and the core serialises a lane, so
per-key ordering holds end to end.

- `FeatureOrderedByKey` — **native**. The key is hashed with Kafka's own
  murmur2 partitioner (`(murmur2(key) & 0x7fffffff) % partitions`), so one
  key's records share a partition *with the records the rest of the fleet
  produced*. A Go producer that hashed differently would split one key's order
  across two partitions — silently, and only in a mixed-language deployment.
- `FeatureOrderedByPartition` — **native**.
- `FeatureOrderedGlobally` — **conditional**: only on a topic with a single
  partition. Kafka orders within a partition and never across partitions.

A keyless record is **sticky**: it goes to one partition until the producer
starts a new batch. Round-robin across sixteen partitions would make sixteen
batches of a sixteenth the size, which is sixteen produce requests instead of
one.

`Publish(..., mq.Partition(n))` routes explicitly. The request travels in the
record's *enrichment context* rather than in `Record.Partition`, because
franz-go writes the partition it picked into that field before buffering — so
the field cannot carry what the caller asked for.

## Delays

Kafka has none, in either direction:

- a **scheduled publication** (`mq.Delay`, `mq.PublishOptions.DeliverAt`) is
  `ErrUnsupported` before any I/O, in every metadata mode. A delay is not
  metadata an envelope could carry, it is behaviour the broker does not have.
- a **delayed redelivery** (`d.Nack(mq.NackDelay(d))`) is emulated: the
  block-partition strategy holds the record **in this process** with its offset
  uncommitted and redelivers it after the delay. The claim is therefore
  `EmulatedVolatile`: if the process dies, the delay is lost and the record is
  redelivered immediately by whoever takes the partition. Under
  `nack.Stop` there is no delayed redelivery at all and the claim is withdrawn.

`mq.TTL` is `ErrUnsupported`: Kafka has topic **retention**, not a per-message
time to live. `mq.Priority` is `ErrUnsupported`: a partition is a queue, not a
heap. `mq.DedupKey` is `ErrUnsupported`: the idempotent producer de-duplicates
a producer's own retries and nothing an application would call a key.

## Dead-letter queue

`EmulatedDurable`, advertised **unconditionally**, with the condition in its
note:

- with `Config.NackConfig.DeadLetterTopic` set, `Reject` publishes the record
  there with its provenance and commits the original offset only after that
  publication is confirmed. The destination is a durable Kafka topic, so the
  route survives a restart.
- with none set, `Reject` is **refused with `ErrUnsupported`** rather than
  destroying the message.

Both are the same behaviour described from two sides, which is why the claim is
constant: the specification makes a capability table constant for the life of a client, and
a table that answered differently depending on a field left the claim provable
in neither direction.

The dead-letter topic is **not created** by the driver. The specification forbids a driver
provisioning behind a caller's back, and a dead-letter destination created by a
produce would have the cluster's default partition count and retention.

## Headers & limits

Kafka's record headers are ordered, duplicate-preserving and binary, so the specification's
three promises are all native and no envelope is needed. `MetadataNativeStrict`
refuses nothing here except a caller's header under the reserved prefix, and
`MetadataAdaptive` and `MetadataLenientDrop` therefore behave identically to
it; only `MetadataPortableEnvelope` changes the bytes on the wire, and a caller
asks for it when a non-iqmq consumer must not have to understand the header
mapping.

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

| limit | value |
|---|---|
| `MaxMessageBytes` | the cluster's own `message.max.bytes`, read once at open with `DescribeConfigs`; **unknown** when the cluster will not say |
| `MaxBatchBytes` | the same number: the broker bounds a batch by it too |
| everything else | **unknown.** Kafka bounds headers and batch counts only by the message size, and it has no delay and no lease, so the specification forbids advertising a number for any of them |

The size limit is enforced by the driver **before any I/O**, which is what
makes the advertised number worth reading.

## Reconnect

- a lost connection is `HealthDegraded` with the reason;
- a re-established one is `HealthReady`, and fires `Config.Hooks.OnReconnect`
  — the first connection of the client's life is not reported as a
  reconnection;
- `Close` is terminal, so a late callback cannot report a closed client ready.

The counters a deployment samples are `Backend.Reconnects`,
`DegradedTransitions`, `Rebalances`, `LostPartitions`, `Commits` and
`AmbiguousPublications`. The last two are the ones to alert on.

## At-least-once window

**"Everything completed since the last commit."**

A delivery is acknowledged when its offset has completed in the tracker *and*
the resulting watermark has been committed. The two are not simultaneous,
because a commit is a round trip:

- on the ticker, every `Config.CommitInterval` (250 ms by default);
- synchronously in `OnPartitionsRevoked`;
- synchronously on `Close`;
- whenever `Backend.Commit` is called.

A process that dies between a completion and the commit that would have carried
it redelivers those records. That is a configuration choice — a commit per
record would cost a round trip per record — and `Backend.Commit` narrows it to
zero at a point of the caller's choosing.

**A rebalance closes the window; a loss opens it.** `OnPartitionsRevoked` stops
dispatch for the revoked partitions, waits a bounded `Config.RevokeWait` for
their in-flight deliveries to settle, reads the safe boundary and stops
accepting their settlements in one step, commits that boundary synchronously,
and *then* invalidates the generation. A settlement arriving after the wait
therefore reports `ErrLeaseLost` instead of acknowledging a record the commit
does not carry and another member is about to process. Commits go out one at a
time, and a boundary read before a later commit overtook it is dropped rather
than sent, so the group's position never moves back. `OnPartitionsLost` does the same **without the commit**, because a
lost partition is one the coordinator has already taken away and a commit for
it would overwrite the position of the member that now owns it. Those records
are redelivered, and `Backend.LostPartitions` is what says it happened.

The bounded wait is not optional. A callback that overran the group's rebalance
timeout would take the member out of the group, and its partitions would be
taken rather than handed over — which is the `OnPartitionsLost` path, where the
boundary cannot be committed at all. Overrunning the wait is therefore strictly
worse than giving up on it.

**A log that loses records starts the partition again.** When a log no longer
holds the consumer's position, because the topic was recreated on a broker
without topic IDs or an unclean leader election truncated it, franz-go resets
the partition below offsets the consumer has already read and reports
`ErrDataLoss`. The driver then re-bases the partition the way a new assignment
does. The records from the reset point on are delivered, and a delivery handed
out before the loss reports `ErrLeaseLost` when it is settled, because its
offset may now name a different record.

`Attempt` is 1 on a first delivery, rises on an in-place retry, and comes from
the `iqmq-attempts` header on a retry topic. A record redelivered because its
offset was never committed arrives looking exactly like a first delivery, which
is honest: Kafka does not count attempts.

## Config & DSN

    kafka://user:password@broker1:9092,broker2:9092/prefix?sasl=scram-sha-512&tls=true

The typed `Config` is what a production program writes; the DSN parses into
exactly it. Every credential is a `dsn.Secret`, so neither `Config.String` nor a
slog record nor an error built from one can carry it. A parameter nothing read
is a **construction error naming it**.

| parameter | field |
|---|---|
| `clientid` | `ClientID` |
| `sasl=plain\|scram-sha-256\|scram-sha-512` | `SASL` |
| `tls`, `insecureskipverify` | `TLS`, `InsecureSkipVerify` |
| `disableidempotency` | `DisableIdempotency` |
| `unboundeddelivery` | `UnboundedDelivery` |
| `autocreatetopics` | `AutoCreateTopics` |
| `readcommitted` | `ReadCommitted` |
| `instanceid`, `transactionalid` | static membership, transactions |
| `linger`, `batchmaxbytes`, `maxbufferedrecords` | the producer |
| `fetchmaxwait`, `fetchmaxbytes` | the consumer |
| `sessiontimeout`, `rebalancetimeout`, `heartbeatinterval` | the group |
| `commitinterval`, `revokewait` | the commit and the revoke |
| `requesttimeout`, `deliverytimeout` | the two bounds |
| `partitions`, `replicationfactor` | what the typed `Admin` creates with |
| `tracker=<kind>` | the offset tracker |
| `nack=<kind>`, `deadlettertopic`, `maxattempts`, `retries`, `nackdelay` | |

Everything a typed `Config` cannot carry — an OAUTHBEARER token source, GSSAPI,
a custom dialler, a compression codec, franz-go's own metrics hooks — is
reached through `OpenWithClient`, which takes a `*kgo.Client` the caller built
and an explicit `driverkit.Ownership`. That client is the **producer**: every
source opens a client of its own.

## Known gotchas

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

| test | what it proves |
|---|---|
| `TestCriticalRebalanceWithDeliveriesInFlight` | one member holds an unsettled delivery from each of two partitions; a second joins, so cooperative-sticky must hand one over. The delivery from the partition that **moved** is refused with `ErrLeaseLost`; the one from the partition that **stayed** is acknowledged. |
| `TestCriticalRevokedDeliveryReportsLeaseLost` | a partition **lost** rather than revoked, forced by fencing the member's heartbeat with `UNKNOWN_MEMBER_ID`. `Ack` and `Nack` both report `ErrLeaseLost`, and the record is redelivered because its boundary could not be committed. |
| `TestCriticalOutOfOrderCompletion` | twelve records settled from the top down; the committed offset does not move until the lowest completes, and then it passes all twelve. |
| `TestCriticalLeaderChange` | every partition re-elected mid-stream; nothing lost. |
| `TestCriticalRetryTopicPublicationFails` | the republication to `<topic>.retry.1` is refused; the settlement fails, the original offset is **not** committed, and exactly one copy reaches the retry topic once the fault is removed. |
| `TestCriticalDeadLetterPublicationFails` | the same on the dead-letter route, which is the one the specification names. |

And the ambiguous publication, which the conformance kit skips because the
portable API cannot reach it: `TestAmbiguousPublishAfterRetries`,
`TestAmbiguousPublishOnBrokenIdempotence`,
`TestAmbiguousPublishOnCancellationAfterBuffering`,
`TestAmbiguousBatchIsIndexAligned`, `TestAmbiguousRepublicationNeverAcks` — and
`TestDefinitePublishFailures`, which proves the other direction, because a
driver that marked everything ambiguous would pass the first six.
