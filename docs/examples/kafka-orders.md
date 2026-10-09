# kafka-orders

[`examples/kafka-orders`](https://github.com/iqhive/iqmq/tree/main/examples/kafka-orders)
— a partitioned order pipeline over `drivers/kafka`: keyed publishing, a
consumer group at concurrency four, settlement the handler does itself, retry
into a dead-letter topic, an explicit offset checkpoint, and the specification's
Drain/Run/Shutdown trio with nothing left in flight.

The module's `README.md` is the full walk-through. This page records what it
established about the driver and the core, because three of the four things
are limitations a caller has to plan around.

## Per-key order is two decisions, not one

Kafka orders a **partition** and nothing else, so "every event of one order
arrives in order" holds only if both halves hold:

- the **producer** publishes every event of an order under one
  `Message.Key`, so the driver hashes them to one partition;
- the **consumer** serialises a partition. The ordering machinery reads
  `DeliveryInfo.Partition` and routes one partition to one lane, which is the
  runner's default — so `WithConcurrency(4)` processes four partitions at once
  and never two events of one order at once.

The example checks both and its test breaks each of them, so the check is one
somebody has watched fail.

## The dead-letter route that could not work

the specification's own dead-letter route — `mq.WithDeadLetter(topic)` — publishes a copy of
the message stamped with five provenance headers, all under the reserved
`iqmq-` prefix, through the ordinary publish path. Six drivers refused **any**
header under that prefix on publish, which is the rule about a *caller's*
headers applied to the core's own:

    iqmq/kafka: a header key uses the reserved iqmq- prefix

The runner then requeued the message and counted a `DeadLetterFailure`, and the
message was redelivered for ever. The drivers that carried the check were
`drivers/kafka`, `drivers/kafka/kafkago`, `drivers/nats`,
`drivers/jetstream`, `drivers/amqp10` and `drivers/pulsar`, and no option
helped: the runner opens its dead-letter publisher with a `PublisherConfig`
carrying only `Topic` and `Confirmation`, so a caller could not put it into
`MetadataAdaptive` — the mode that would envelope the message instead of
refusing it.

`driverkit.ReservedHeaderAllowed` closed it: the core's own five provenance
keys are admitted by every driver and nothing else under the prefix is, so
the route works on all twelve. The example still takes
`mq.WithNativeDeadLetter()`, which is one settlement with no publish-then-ack
window, and the section below is why that choice is worth reading.

## What the native route does and does not carry

`mq.WithNativeDeadLetter()` settles with `Nack(NackDeadLetter())` and the
driver moves the record to `Config.NackConfig.DeadLetterTopic`. For Kafka the
dead-lettered record arrives with

- `iqmq-origin-topic`, `iqmq-origin-subscription`, `iqmq-origin-partition`,
  `iqmq-origin-offset`

and **without** `iqmq-attempts` or `iqmq-failure`, because the nack option
names neither an attempt count nor a reason. The dead letter says where the
message came from and not why it died.

## A handler cannot see which attempt it is on

Kafka's default nack strategy (`nack.BlockPartition`) holds the record in
process and hands it back to the handler without re-fetching it, so the
**broker's** attempt counter never moves: `DeliveryInfo.Attempt` reads `1`
however many times the specification retries. The **runner's** own attempt number is
`runItem.attempts` and nothing in the frozen API carries it to a handler. A
handler that needs it counts for itself.

## Drain's answer is a bare cancellation

`ConsumerHandle.Drain` cancels the runner's internal fetch context, and
`Run` then returns that context's `context.Canceled` — with the handle neither
closed nor cancelled by the caller. `Shutdown` has a named answer
(`ErrClosed`); a drain does not, so a caller cannot tell "I drained you" from
"my own context died".
