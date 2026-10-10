# rabbitmq-dlq — the dead-letter destination, both ways

The specification has **two** dead-letter routes and they are not equivalent. This example
runs both, over RabbitMQ ([`drivers/rabbitmq`](../../drivers/rabbitmq)) and
over the in-process memory broker, and prints what each one leaves behind.

    go run .          # against a RabbitMQ broker on 127.0.0.1:5672
    go test ./...     # the same program against the memory broker, and against
                      # RabbitMQ when IQMQ_RABBITMQ_ADDR names one

## What it shows

```
claims: dead letter true
core route: 3 handler attempt(s), then acked 1 nacked 2 dead-lettered 0 — the copy was published and the original ACKED, so ConsumerStats.DeadLettered counts none of it 
native route: 3 handler attempt(s), then acked 0 nacked 3 dead-lettered 1 — the last nack asked the broker to move the message, which is the only shape ConsumerStats.DeadLettered counts
core route: the copy carries origin topic orders-core-test, its subscription, an attempt count and the handler's reason "the order references a product that does not exist"
native route: the broker moved the message, and it carries neither iqmq-attempts nor iqmq-failure — Nack(NackDeadLetter()) names neither 
```

## The two routes

| | core route | native route |
|---|---|---|
| option | `mq.WithDeadLetter(topic)` | `mq.WithNativeDeadLetter()` |
| mechanism | the runner **publishes a copy** to the topic, then acks the original | the runner settles `Nack(NackDeadLetter())` and the **broker moves** the message |
| carries | `iqmq-origin-topic`, `iqmq-origin-subscription`, `iqmq-attempts`, `iqmq-failure`, `iqmq-first-delivered-at` | the broker's own provenance (`x-death` on RabbitMQ) and none of iqmq's |
| says why it died | **yes** — the handler's error is in `iqmq-failure` | **no**  |
| `ConsumerStats.DeadLettered` | **0** | 1 |
| works on | every driver, since the specification closed | every driver whose capability table claims `FeatureDeadLetter` |

### Which route this example uses, and why the sentence matters

Both, because both work — and until that was a property of
these two drivers rather than of the library. The specification recorded that
`mq.WithDeadLetter` could not work on `drivers/kafka`,
`drivers/kafka/kafkago`, `drivers/nats`, `drivers/jetstream`,
`drivers/amqp10` and `drivers/pulsar`: their `Representable` refused **any**
header under `mq.ReservedHeaderPrefix`, which is every one of the five the core
route stamps. The publication was refused before any I/O, the runner requeued
the message and counted a `DeadLetterFailures`, and the message was redelivered
for ever.

`driverkit.ReservedHeaderAllowed` now admits exactly those five and nothing
else, in every driver, so the core route is available on all twelve. The
paragraph stays because the two routes still differ in what the dead-lettered
message **carries** — a reason and an attempt count, or the broker's own
provenance and neither — so this program prints which route it took rather than
letting a reader guess what arrived.
`TestTheCoreRouteWorksOnEveryDriver` pins the mechanism in both directions: all
five headers are under the reserved prefix, `Headers.Validate` still refuses
each of them as a *caller's*, and `driverkit.ReservedHeaderAllowed` admits each
of them as the *core's*. It fails the day any of the three changes.

### `ConsumerStats.DeadLettered` counts only the native route

This is the example's own finding. The counter is
documented as "how many negative settlements asked for the dead-letter
destination", and the core route makes **no** negative settlement on the final
attempt: it publishes the copy and **acks** the original. So a service using
the specification's own route and alerting on `DeadLettered` sees zero however many messages
are being dead-lettered, and the only trace left in the counters is `Acked`
rising for a message whose handler failed. Measured identically on both
drivers; the numbers in the output above are asserted, per route.

## The configuration RabbitMQ needs

Three settings, all off by default, and each refused without the one below it:

- `Declare` — the specification forbids a driver from creating topology behind a caller's
  back, so nothing is declared unless asked.
- `DeadLetter` — declares `<queue>.dlx` and `<queue>.dlq` and points each
  subscription's queue at them. Without it a `Nack` asking for the
  dead-letter route is refused with `ErrUnsupported` rather than dropping the
  message.
- `DeadLetterTopic` — sends dead letters to **another topic** instead of the
  derived `<queue>.dlq`, which is a *queue* and not a topic: nothing can read
  it through `mq.Client`. The destination is a place a caller reads from,
  and this is how a deployment says which place.

The named topic has to exist **before** a message is rejected: RabbitMQ drops a
dead-lettered message whose dead-letter exchange is missing, silently.
That is why `run` opens the dead-letter consumer first, on
both arms.

## The two arms

| arm | driver | gate |
|---|---|---|
| in-process | `memory`, `DeadLetterTopic` set | never skips |
| broker | `drivers/rabbitmq` | `IQMQ_RABBITMQ_ADDR` |

`drivers/rabbitmq` has no exported in-process fake, so the in-process arm
exercises the two **routes** rather than RabbitMQ's own
`x-dead-letter-exchange`. The broker arm asserts exactly
the same output, which is what says RabbitMQ agrees — including that its
`Representable` accepts the five reserved headers. To run it:

    docker run -d -p 127.0.0.1:5672:5672 \
      rabbitmq@sha256:ac1201d1dc227779d93c607f976e7f9156fd5b551ee5ff7946cab6c242091bf3
    IQMQ_RABBITMQ_ADDR=127.0.0.1:5672 go test ./...

`IQMQ_REQUIRE_BROKERS=1` turns the unset variable from a skip into a failure.

## Topics per run

A durable exchange and a quorum queue outlive the process, so every topic here
carries a per-run suffix. A second run of a shared name would inherit the first
run's dead letters — which is exactly what this program counts. It is
the lesson in a different broker.
