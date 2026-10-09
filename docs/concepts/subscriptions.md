# Groups and subscriptions

Who gets which message. Specification the specification maps three words onto eleven
brokers, and this page is that map plus the two places it leaks.

## The vocabulary

| iqmq | means | Kafka | JetStream | SQS | Pub/Sub | RabbitMQ | NSQ | Redis Streams | Pulsar |
|---|---|---|---|---|---|---|---|---|---|
| **topic** | where a message is published | topic | subject in a stream | queue name/URL | topic | exchange (or queue) | topic | stream key | `persistent://t/ns/topic` |
| **subscription** | a named, independent consuming position | consumer group | durable pull consumer | *(empty — SQS has no fan-out)* | subscription | durable quorum queue bound to the exchange | channel | consumer group | subscription |
| **lane** | one unit of ordered work inside a subscription | partition | — | — | — | — | — | shard | partition |

`mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}` is therefore
"consume the orders topic under the billing group's own position". Two
processes with the same `Subscription` compete; two with different ones each
get everything.

## Where it leaks: SQS has no fan-out

`drivers/sqs` maps a topic to one queue, and a queue has exactly one set of
competing consumers. There is no second independent position to give a second
`Subscription`, so the driver leaves `Subscription` empty and **does not
advertise `FeatureFanOut`**. Asking two SQS consumers for two subscriptions of
one topic does not give you two copies; it gives you a config error, before any
I/O.

`drivers/snssqs` is the composite that fixes it: the topic is an SNS topic, the
subscription is an SQS queue subscribed to it, and fan-out is the broker's.
That is a different driver rather than a flag, because it is a different
topology with different failure modes and a different cost.

## Where it leaks: not every subscription is durable

`FeatureDurableSubscriptions` means a subscription's position survives every
consumer disconnecting. On the `memory` driver it does not — the engine is
never persistent, so the claim renders as *emulated-volatile* rather than as
support. On NATS Core there is no position at all. The matrix's
`DurableSubscriptions` row is the authority, and `?` in it means nothing
established the answer either way rather than "no".

## Where a subscription starts

`mq.ConsumerConfig.Start` decides what a subscription with **no committed
position** does. It has four kinds, and three of them need a capability:

| `mq.StartPosition` | Needs | Meaning |
|---|---|---|
| `mq.StartLatest` | — | only messages published after the subscription exists. The safe default. |
| `mq.StartEarliest` | `FeatureReplay` | the oldest message the broker still retains |
| `mq.StartAt(t)` | `FeatureSeekByTime` | the first message at or after `t` |
| `mq.StartAtSequence(n)` | `FeatureSeekBySequence` | the broker sequence `n` |

A start position the driver cannot honour fails when the consumer is
constructed, not on the first fetch. It is also ignored once a position exists:
a restart resumes, it does not re-read, and a `Start` that surprises you on a
*second* run is almost always a subscription that already had a position.

## Fan-out inside the memory engine

The memory driver has three topology kinds — `memory.TopologyQueue`,
`memory.TopologyTopic` and `memory.TopologyStream` — and they are genuinely
different data structures rather than one with flags, because the specification requires the
queue fast path not to pay for stream bookkeeping. Which you get decides
whether a new subscription sees history, whether a slow subscription blocks a
fast one, and whether the log is retained at all. See
[the memory driver page](../drivers/memory.md).
