# iqmq Kafka driver (kafka-go)

```go
import kgq "github.com/iqhive/iqmq/drivers/kafka/kafkago"

client, err := kgq.Open(ctx, kgq.Config{Brokers: []string{"kafka-1:9092"}})
```

The **second** Kafka driver in this repository. `drivers/kafka` is franz-go;
this one is [`github.com/segmentio/kafka-go`](https://github.com/segmentio/kafka-go),
and (`the API contract` the specification) is the bake-off between them. Until
ADR-013 is ratified neither is the canonical one, and ends "loser
demoted to community".

At-least-once with cumulative acknowledgement. A topic is a Kafka topic, a
subscription is a consumer group, a partition is an ordering lane. `acks=all`
(kafka-go's own default is `acks=0`); auto-commit off.

The driver uses **neither** of kafka-go's convenient consumer surfaces whole:
`kafka.ConsumerGroup` for membership, because a `kafka.Reader` never reports a
rebalance and the specification's `ErrLeaseLost` is unreachable without one; and
`kafka.Client.Fetch` for reading, because a Reader collapses a null record
value and a zero-length one into the same nil `Body` and the specification makes that
distinction observable.

- **There is no idempotent producer.** kafka-go's writer sends no producer id,
  so the broker de-duplicates nothing — not an application's key and not the
  writer's own retries. `FeatureIdempotentPublish` is unsupported here and
  native there, and `Config.MaxAttempts` is the number of copies one
  publication may leave in the log.
- **There is no cooperative assignor.** Range and round-robin are both eager,
  so every rebalance revokes the WHOLE assignment: every in-flight delivery
  reports `ErrLeaseLost`, where franz-go keeps the ones whose partitions did
  not move.
- **There is no transactional producer**, so `x/kafkatx` drives the franz-go
  driver and not this one.
- **There is no pattern subscription** at all, where franz-go has one this
  repository does not wire.
- **A produce is not flushed when the caller stops publishing.** kafka-go
  batches per partition and flushes on a full batch or a timer, so the default
  batch size is ONE record — a synchronous publisher pays no linger — and a
  batching caller raises `Config.BatchMaxRecords` and pays the timer.

Two defaults this driver sets that kafka-go does not, both for correctness:
`RequiredAcks=RequireAll`, and `MetadataTTL` at one second with a write
backoff that spans it — kafka-go refreshes its cached cluster layout on a
timer and on nothing else, so at its own six-second default a leader election
fails publications for up to six seconds.

The Docker-free test suite is built on `kfake`, franz-go's in-process broker,
which is a real Kafka protocol server on a real socket; it is a test
dependency only and the driver links neither kfake nor franz-go. The
conformance evidence is `the conformance suite` against the
pinned `apache/kafka` image, and the interesting artefact is its **diff
against `kafka-franz-go.json`**: one scenario by outcome and three by reason,
all of them the intended `IdempotentPublish` withdrawal. A fourth,
`publish/oversized-payload`, differed until this increment fixed the franz-go
driver's `message.max.bytes` lookup.
