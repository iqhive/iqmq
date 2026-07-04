# kafka-orders — a partitioned order pipeline

Keyed publishing, a consumer group with concurrency, settlement the handler
does itself, retry into a dead-letter topic, and a shutdown that leaves nothing
in flight — over `drivers/kafka` (franz-go).

    go run .            # against a Kafka broker on 127.0.0.1:9092
    go test ./...       # runs the same program against an in-process cluster

## What it shows

```
topology: orders with 3 partition(s), dead letters to orders.dlq
key ord-1001: created,paid,shipped in one partition, in order
key ord-1002: created,paid,shipped in one partition, in order
key ord-1003: created,paid,shipped in one partition, in order
key ord-bad: 3 handler attempts of created, every one failed (the broker's own attempt counter stayed at 1: the retry never left this process)
the handler acked 9 deliveries itself; the runner overruled none
dead-lettered ord-bad/created, with origin topic orders, subscription fulfilment, partition and offset
...and with neither iqmq-attempts nor iqmq-failure: Nack(NackDeadLetter()) carries no attempt count and no reason 
commit: offsets committed explicitly, so the at-least-once window is zero here (1 commit(s) in this run)
shutdown: run drained cleanly; received 10, acked 9, dead-lettered 1, settle failures 0, in flight 0
```

### Per-key order is two decisions, not one

Kafka orders a **partition** and nothing else, so "every event of one order
arrives in order" needs both halves:

- the **producer** puts every event of an order in one partition, because it
  publishes them under one `Message.Key`. Drop the key and Kafka spreads the
  three events over three logs; no consumer can put them back.
- the **consumer** serialises a partition. The ordering machinery reads
  `DeliveryInfo.Partition` and routes one partition to one lane, which is the
  runner's default — so `WithConcurrency(4)` processes four partitions at once
  and never two events of one order at once.

`report` in `main.go` checks both, and `main_test.go` breaks each of them
against fabricated observations so that the check is one somebody has watched
fail (`TestReportRefusesALostKey`, `TestReportRefusesReorderedEvents`).

### Explicit settlement beside the retry route

The nine good deliveries are acked by the handler with `Delivery.Ack`; the
runner does not overrule a delivery the handler settled, so returning
`nil` afterwards is not a second acknowledgement. `mq.WithSettlement(
mq.SettlementManual)` is the real alternative — it makes the runner settle
nothing at all, and with it the retry and dead-letter routes never run.

### The attempt count a handler cannot see

`ord-bad`'s handler is called three times and `DeliveryInfo.Attempt` says `1`
every time. That is correct: Kafka's default nack strategy
(`nack.BlockPartition`) holds the record in this process and hands it back
without re-fetching it, so the **broker's** attempt counter never moves, and
nothing in the frozen API carries the **runner's** own attempt number to a
handler. A handler that needs it counts for itself, as this one does.

### Committing on purpose

An ack completes an offset; the commit that makes it durable happens on
`Config.CommitInterval`, at a rebalance and at `Close`. The at-least-once
window is "everything completed since the last commit". `Backend.Commit`
closes it to zero at a point of your choosing, and this program calls it once
the batch is done. It is a driver method, not a portable one: nothing 
asks a broker to checkpoint a group.

## Why the dead-letter route is the driver's and not the core's

The program uses `mq.WithNativeDeadLetter()`, not `mq.WithDeadLetter(topic)`.
Two findings, both from writing this example:

1. **`mq.WithDeadLetter` could not work on this driver — fixed.** the route
   publishes a copy of the message stamped with five provenance headers, all
   under the reserved `iqmq-` prefix, through the ordinary publish path. The
   driver's `Representable` refused **any** header under that prefix — the specification's
   rule about a caller's headers, applied to the core's own — so the
   publication was refused before any I/O:

       iqmq/kafka: a header key uses the reserved iqmq- prefix

   The runner then requeued the message and counted a `DeadLetterFailure`, and
   the message was redelivered for ever. `drivers/nats`, `drivers/jetstream`,
   `drivers/kafka/kafkago`, `drivers/amqp10` and `drivers/pulsar` carried the
   same check.

   `driverkit.ReservedHeaderAllowed` now admits exactly those five keys and
   nothing else, in every driver, so the core route works on all twelve.
   This program keeps the native route: it is one
   settlement with no publish-then-ack window, and finding 2 below is about
   the native route and is what the example prints.
   `TestTheCoreDeadLetterRouteWorksOnThisDriver` is the evidence and fails the
   day the refusal comes back.

2. **The native route carries no reason and no attempt count.** It settles with
   `Nack(NackDeadLetter())`, which names neither, so the driver stamps
   `iqmq-origin-topic`, `iqmq-origin-subscription`, `iqmq-origin-partition` and
   `iqmq-origin-offset` and neither `iqmq-attempts` nor `iqmq-failure`. The
   dead-lettered message says where it came from and not why it died.

## Running it against a broker

`go test ./...` always runs the example against **kfake**, franz-go's
in-process Kafka broker: the real protocol, real record batches, real consumer
groups, no Docker. To run it against a container as well, start one and name
it — the variable is `conformance/harness`'s own convention,
`IQMQ_<BROKER>_ADDR`:

```sh
docker run -d --name kafka-example -p 9092:9092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837
IQMQ_KAFKA_ADDR=127.0.0.1:9092 go test -count=1 -v -run TestRunAgainstABroker ./...
docker rm -f kafka-example
```

(The digest is the one pinned in `conformance/harness/images.lock`; the
environment is `conformance/harness`'s own Kafka recipe. Redpanda works too:
`IQMQ_KAFKA_ADDR` takes a comma-separated seed list.)

With `IQMQ_KAFKA_ADDR` unset that test **skips** and says so; with
`IQMQ_REQUIRE_BROKERS=1` an unset variable is a **failure** instead.
`make examples` prints which examples reached a broker and which skipped, by
name, so a run in which nothing did says so rather than reading as green.

Re-running against the same broker is safe: the group's committed offsets mean
a second run consumes only what it publishes, and `EnsureTopic` is idempotent.
The dead-letter reader uses its own group, so it reads the whole topic — a
third run against a broker that has kept earlier dead letters reads the
earliest one, which is still `ord-bad`.

## Files

| File | What is in it |
|---|---|
| `main.go` | the program: `open`, `publish`, the handler, `report`, `readDeadLetter`, `shutdown` |
| `main_test.go` | both arms, the ordering checks watched failing, and the dead-letter-route finding |
| `report_test.go` | the `IQMQ_EXAMPLE_REPORT` contract `make examples` reads |
