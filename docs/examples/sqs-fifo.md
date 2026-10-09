# sqs-fifo

[`examples/sqs-fifo`](https://github.com/iqhive/iqmq/tree/main/examples/sqs-fifo)
— Amazon SQS's **FIFO** profile through `drivers/sqs`: ordering inside a
message group, deduplication by id, the visibility timeout, and what a blocked
group looks like from the API.

## The mapping

| Portable API | SQS | What it buys |
|---|---|---|
| `Message.Key` | `MessageGroupId` | order **within a group**, none across groups |
| `mq.DedupKey(s)` | `MessageDeduplicationId` | the service discards a second publication of the same id for five minutes (the window is the service's and is not configurable) |
| — | one in flight per group | what makes the order hold under concurrency |
| `ConsumerConfig.AckDeadline` | the visibility timeout | the lease |

A publication with **no** `DedupKey` is given a unique identifier by the
driver, so it is never deduplicated by accident — which also means a retried
publication without one publishes a second message. A publication with **no
key** goes to `sqs.DefaultGroupID`, which puts every keyless message in one
group, in order, one in flight at a time: a throughput cliff rather than an
error.

The two profiles are two **clients**, not one: the specification makes a capability table
constant per client and seven claims differ between them, `Config.FIFO`
selects the profile, and `DriverInfo.Profile` reports it.

## A blocked group is indistinguishable from an empty queue

With one message of a group in flight, a receive produces another group's
message; with every group blocked, a receive produces **nothing at all**,
though the queue is not empty. There is no error, no code and no signal for
that state through the portable API.

Do not read an empty poll from a FIFO queue as an empty queue. `Admin.Stats`
is where a depth comes from.

## Two things the lease API will not tell you

1. **`Extend` does not report what it bought.** `DeliveryInfo` is the snapshot
   the receive handed over, so `LeaseExpires` still reports the *original*
   deadline after a successful extension, and the SPI's `Extend` returns only
   an error. A caller that needs the new deadline has to assume the documented
   minimum.
2. **On this driver `Extend(by)` can make the lease shorter.** The specification and
   `Delivery.Extend` both say an extension moves the deadline "at least `by`
   further into the future". The driver sends one `ChangeMessageVisibility`
   with `VisibilityTimeout=by`, and the service measures that **from now** —
   so a `by` shorter than what is left moves the deadline backwards and the
   message becomes visible to another consumer early. `drivers/nsq` guards the
   analogous case; this one does not.

## Why the driver refuses a settlement it could have sent

The service accepts a `DeleteMessage` for a receipt handle whose message
another consumer now holds: there is no API that asks "do I still hold this?",
the delete does not fail, and the message is gone. So the refusal can only
happen in the driver, and it does — before the call, comparing the visibility
deadline it recorded at the receive against now on the injected clock, and
answering `mq.ErrLeaseLost`.

It is a **local** view and deliberately pessimistic in both directions. The
cost is a redelivery; the alternative is deleting somebody else's work.

## What a FIFO queue does to a program that runs twice

Three things the in-process server does not model, all met against a real
broker :

- **one visibility window is not enough.** A proof that a group is blocked
  holds a message unsettled while it polls, so its window must outlast the
  polling; a proof that a lease is lost has to wait the window out, so a long
  one is a long test. An acknowledgement deadline is per **consumer**, so use
  two.
- **two consumers of one queue compete.** `SourceConfig.Subscription` is
  ignored by this driver — a queue has exactly one set of messages. Two
  independent subscriptions need `drivers/snssqs`, where the topic is an SNS
  topic and the subscription is a queue.
- **a shared queue name is expensive.** A message an earlier run left behind
  is delivered to this one; and the deduplication window is five minutes, so a
  second run inside it has its first publication discarded as a duplicate of
  the earlier run's. Give each run a topic of its own.

## A message nobody settles accumulates attempts on a timer

The driver's pollers keep fetching while the consumer is open, so an unsettled
message is received, expired and received again once per window: its attempt
count is a function of wall time. That is the mechanism a redrive policy's
`maxReceiveCount` counts — a message nobody settles reaches the dead-letter
queue on a timer, not on a decision.
