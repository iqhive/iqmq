# Rebalance

A rebalance is the broker taking a partition away from this process and giving
it to another one, usually because a consumer joined or left. Everything
in-flight on that partition is suddenly somebody else's problem, and the
question is whether it is processed twice, processed once, or lost.

This page is mostly about Kafka, because Kafka is where the portable API has to
do the most work.

## The offset tracker

Kafka commits a single offset per partition and means "everything before this
is done". A consumer that processes messages **concurrently** completes them
out of order, so the naive commit — the highest offset seen — would skip the
ones still in flight.

`driverkit/offset` is the tracker that makes this correct:

* it marks delivered offsets as in-flight;
* it commits only the **highest contiguous completed offset plus one**;
* a failed or unresolved offset therefore **blocks** later commits rather than
  being skipped over;
* it pauses the partition when the gap or the in-flight count exceeds its
  bounds, so an unbounded gap cannot become unbounded memory.

The offset-tracker implementations and their benchmarks are under
`driverkit/offset`. Compare them on your workload before changing the binding.

The consequence to plan around: **one stuck message stops the commit for its
whole partition.** The messages behind it are still processed — they are marked
in-flight and completed — but the committed position does not advance past the
gap, so a rebalance re-delivers them. That is the trade the specification asks
for: duplicates rather than a silent skip.

## What the driver does on revocation

In `OnPartitionsRevoked`, in this order:

1. **stop dispatching** for the revoked partitions. No new handler starts on
   work that is about to belong to someone else.
2. **wait, bounded.** Handlers already running get a chance to finish.
3. **commit the safe boundary synchronously.** Still inside the callback,
   because after it returns the partition is gone and a commit is refused.
4. **invalidate the old generations.** Every outstanding delivery for those
   partitions has its generation bumped, so a late settlement from a handler
   that finished after step 2 returns `mq.ErrLeaseLost` instead of settling an
   offset that now belongs to another consumer.

Step 4 is what the generation token is for, and it is why a stale `*mq.Delivery`
is safe to hold: it cannot settle the new owner's work.

## What you see

| Signal | Meaning |
|---|---|
| `mq.ErrLeaseLost` from a settlement | the partition moved while your handler ran. Expected during a rebalance; the message will be redelivered. |
| `ConsumerStats.Redelivered` jumping | the new owner started from the last committed position |
| the handler's context cancelled | the bounded wait in step 2 expired |

None of the three is an error to alert on individually. A *rate* of them that
does not settle down after a deploy is: that is a consumer group that cannot
stop rebalancing, usually because a handler is slower than the session timeout.

## Ordering across a rebalance

Order within a key survives only if the key's partition does not move, and a
rebalance moves partitions. The cooperative-sticky balancer minimises the
movement — it is the driver's default for exactly this reason — but it does not
eliminate it.

If your requirement is that a key is never processed out of order *even across
a rebalance*, the ordering has to come from the broker rather than from the
consumer: a FIFO group (SQS FIFO), a `Key_Shared` subscription (Pulsar), or a
partition you never move.

## The other brokers

* **SQS** has no rebalance: every consumer takes from the same queue, and a
  visibility timeout is the only thing that transfers a message.
* **JetStream** and **Pub/Sub** redistribute by lease expiry rather than by a
  group protocol, so the "rebalance" is just the redelivery path and
  `mq.WithLeaseExtension` is the lever.
* **Redis Streams** reassigns through the pending-entries list: the mandatory
  `XAUTOCLAIM` reclaim loop takes over entries idle past the ack deadline, and
  the delivery count it returns becomes `Delivery.Attempt`.
* **RabbitMQ** moves a consumer rather than a partition; the driver owns
  bounded reconnection and topology restoration.

Each driver page's *Reconnect* and *At-least-once window* sections state its
own version.

## What is not tested yet

What *is* tested: `TestCriticalReconnectRestoresTheTopology` and the drivers'
own critical tests, in-process, in every `make ci`. One of them is
rare-flaky under host load and is recorded as such rather than retried until
green.
