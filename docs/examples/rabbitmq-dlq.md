# rabbitmq-dlq

[`examples/rabbitmq-dlq`](https://github.com/iqhive/iqmq/tree/main/examples/rabbitmq-dlq)
— the specification has **two** dead-letter routes and they are not equivalent. This runs
both, over RabbitMQ (`drivers/rabbitmq`) and over the in-process memory broker,
and prints what each one leaves behind.

## The two routes

| | core route | native route |
|---|---|---|
| option | `mq.WithDeadLetter(topic)` | `mq.WithNativeDeadLetter()` |
| mechanism | the runner **publishes a copy** to the topic, then acks the original | the runner settles `Nack(NackDeadLetter())` and the **broker moves** the message |
| carries | `iqmq-origin-topic`, `iqmq-origin-subscription`, `iqmq-attempts`, `iqmq-failure`, `iqmq-first-delivered-at` | the broker's own provenance (`x-death`) and none of iqmq's |
| says **why** it died | yes — the handler's error is in `iqmq-failure` | **no**  |
| `ConsumerStats.DeadLettered` | **0** | 1 |
| availability | every driver, since the specification closed | any driver claiming `FeatureDeadLetter` |

### The portable route is not portable

`mq.WithDeadLetter` could not work on `drivers/kafka`,
`drivers/kafka/kafkago`, `drivers/nats`, `drivers/jetstream`,
`drivers/amqp10` or `drivers/pulsar`: their `Representable` refused **any**
header under `mq.ReservedHeaderPrefix`, which is every one of the five the core
route stamps. The publication was refused before any I/O, the runner requeued
the message and counted a `DeadLetterFailures`, and the message was redelivered
for ever.

`driverkit.ReservedHeaderAllowed` now admits exactly those five keys and
nothing else, in every driver, so the core route is available on all twelve.
The example still prints which route it took, because
the two routes differ in what the dead-lettered message **carries** — a reason
and an attempt count, or the broker's own provenance and neither.

### `ConsumerStats.DeadLettered` counts only the native route

The field counts "negative settlements that asked for the dead-letter
destination", and the core route makes none on the final attempt: it publishes
the copy and **acks** the original. Measured identically on both drivers, for
one poison message over three attempts:

| route | acked | nacked | dead-lettered |
|---|---|---|---|
| core | 1 | 2 | **0** |
| native | 0 | 3 | 1 |

A service on the specification's own route that alerts on `DeadLettered` therefore sees zero
however many messages it dead-letters, and the only trace left in the counters
is `Acked` rising for a message whose handler failed.

## What RabbitMQ needs

Three settings, all off by default and each refused without the one above it:

- `Declare` — the specification forbids a driver from creating topology behind a caller's
  back.
- `DeadLetter` — declares `<queue>.dlx` and `<queue>.dlq`. Without it a `Nack`
  asking for the dead-letter route is refused with `ErrUnsupported` rather than
  dropping the message.
- `DeadLetterTopic` — sends dead letters to another **topic** instead of the
  derived `<queue>.dlq`, which is a *queue* and cannot be read through
  `mq.Client`.

The named topic has to exist **before** a message is rejected: RabbitMQ drops a
dead-lettered message whose dead-letter exchange is missing, silently.
The example opens the dead-letter consumer first for
exactly that reason.

## The two arms

| arm | driver | gate |
|---|---|---|
| in-process | `memory`, `DeadLetterTopic` set | never skips |
| broker | `drivers/rabbitmq` | `IQMQ_RABBITMQ_ADDR` |

`drivers/rabbitmq` exports no in-process fake, so the in-process arm exercises
the two **routes** rather than `x-dead-letter-exchange`; the broker arm asserts
the same output and is what says RabbitMQ agrees, including that its
`Representable` accepts the five reserved headers.
