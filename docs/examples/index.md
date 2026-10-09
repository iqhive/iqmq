# Examples

Every example is a **module of its own** with its own `go.mod`, replacing only
`github.com/iqhive/iqmq` and the driver it needs. That is the point of them: an
example that compiled because it shared the repository's `go.mod` would not
prove that somebody outside this repository can build it.

Each one also carries a **test that runs the program and reads what it
printed**, and `make examples` — which is part of `make ci` — builds, vets and
runs all of them. An example that compiles and deadlocks is the first
documentation defect anybody meets, and `go build` cannot see it.

## The modules

| Example | Driver | What it is for |
|---|---|---|
| [`quickstart-memory`](https://github.com/iqhive/iqmq/tree/main/examples/quickstart-memory) | `memory` | the quick start, built from outside the repository against the published API and nothing else |
| [`typed-json`](https://github.com/iqhive/iqmq/tree/main/examples/typed-json) | `memory` | the generic layer over `encoding/json/v2`, and the decode-failure path — the one place a typed layer can quietly lose a message |
| [`testing-with-synctest`](https://github.com/iqhive/iqmq/tree/main/examples/testing-with-synctest) | `memory` | the deterministic-test story: a retrying consumer whose backoff is hours long, tested in milliseconds, with the schedule asserted |
| [`custom-driver`](https://github.com/iqhive/iqmq/tree/main/examples/custom-driver) | its own | a complete iqmq backend written from outside, against the published SPI, held to the conformance suite |
| [`kafka-orders`](kafka-orders.md) | `kafka` | a partitioned order pipeline: keyed publishing, a consumer group with concurrency, explicit settlement, retry into a dead-letter topic, graceful shutdown |
| [`nats-reqreply`](nats-reqreply.md) | `nats` | the request/reply, native and emulated over the same broker, and the four answers a caller can get |
| [`sqs-fifo`](sqs-fifo.md) | `sqs` | the FIFO profile: message groups, deduplication, the visibility timeout, and what a blocked group looks like from the API |
| [`pubsub-ordering`](pubsub-ordering.md) | `memory`, `gcppubsub` | per-key ordering through the portable API, run unchanged on two drivers, with identical output on both |
| [`rabbitmq-dlq`](rabbitmq-dlq.md) | `rabbitmq`, `memory` | the two dead-letter routes side by side, and what each one leaves behind |
| [`otel-tracing`](otel-tracing.md) | `memory`, `redis` | the span tree printed: a caller's span, the publish span under it, the process span under that |
| [`outbox-postgres`](outbox-postgres.md) | `memory` | the transactional outbox and the idempotent consumer that copes with the duplicates a relay is guaranteed to produce |
| [`otel-tracing`](otel-tracing.md) | `memory` | the span tree, printed: a caller's span, the `publish` span under it, and the `receive`/`process`/`settle` spans on the other side |
| [`pubsub-ordering`](pubsub-ordering.md) | `gcppubsub` + `memory` | per-key ordering through the portable API, run unchanged on two drivers |
| [`rabbitmq-dlq`](rabbitmq-dlq.md) | `rabbitmq` | the two dead-letter routes, which are not equivalent |
| [`outbox-postgres`](outbox-postgres.md) | `x/outbox` | the transactional outbox: a business write and a message made atomic by a database transaction |
| [`bridge`](https://github.com/iqhive/iqmq/tree/main/examples/bridge) | `jetstream` + `memory` | moving messages between two brokers, and the duplicate the seam allows — produced rather than described |

The container-backed ones talk to a broker, and `make ci` runs on machines that have
none. Each of them therefore has **two arms**:

- an **in-process arm** that always runs. `nats-server` is a Go library,
  `kfake` is franz-go's own in-process Kafka broker, and
  `drivers/sqs/sqsfake` is the SQS driver's own in-process server — so each
  example is held to its printed output, byte for byte, with no Docker at all.
  `bridge`'s in-process arm is an embedded `nats-server` with **JetStream
  enabled**, so its source has a real ack deadline and a real redelivery.
  This is what makes these tests able to fail.
- a **container arm** gated on `IQMQ_<BROKER>_ADDR`, which is
  `conformance/harness`'s own convention for an externally managed broker:
  `IQMQ_KAFKA_ADDR`, `IQMQ_NATS_ADDR` (JetStream's too), `IQMQ_ELASTICMQ_ADDR`. Unset, it
  **skips**; with `IQMQ_REQUIRE_BROKERS=1`, an unset variable is a
  **failure** instead. Each example's `README.md` carries the `docker run`
  line, with the digest pinned in `conformance/harness/images.lock`.

The skips are **named**. Each example appends one line per arm to the file
`IQMQ_EXAMPLE_REPORT` points at, and `make examples` prints them:

```
==> examples: 7 module(s)
==> examples: broker arms: 1 of 3 ran (gate: IQMQ_<BROKER>_ADDR; IQMQ_REQUIRE_BROKERS=1 makes an unset one a failure)
      kafka-orders     broker      skipped  IQMQ_KAFKA_ADDR unset
      kafka-orders     in-process  ran      kfake in-process cluster
      nats-reqreply    broker      skipped  IQMQ_NATS_ADDR unset
      nats-reqreply    in-process  ran      embedded nats-server 2.14.6
      sqs-fifo         broker      ran      IQMQ_ELASTICMQ_ADDR=127.0.0.1:19324
      sqs-fifo         in-process  ran      sqsfake in-process server on its own clock
```

A module whose sources read `IQMQ_EXAMPLE_REPORT` and then report no arm
**fails** the target. A test that always skips is a check that cannot fail,
and a run in which nothing reached a broker should say so rather than read as
green.

## Checking broker behaviour

Use the broker-backed examples to exercise topology, settlement and
retry behaviour against your intended broker and source revision.
In-process fixtures cannot establish the behaviour of a deployed broker.
