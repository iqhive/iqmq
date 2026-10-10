# Ordering

Specification. **No global ordering is implied by anything.** Order is a
capability with a scope, and the scope is part of the claim.

## The four conditions

Order holds for a set of messages only when all four of these are true. Three
of them are yours.

1. **the profile supports the requested scope.** `FeatureOrderedByKey`,
   `FeatureOrderedByPartition` and `FeatureOrderedGlobally` are separate
   features and a driver advertises each one separately;
   the matrix has a row for each.
2. **the messages share a non-empty `Key`** — or the scope you asked for is
   partition or global.
3. **the runner is configured to order.** `mq.WithOrderingByKey()` is the
   request. The routing it implies — every delivery of one key to one lane,
   one lane at a time — is the runner's default with or without the option, so
   raising the concurrency does not by itself reorder a key.
4. **exactly one active handler per lane.** The runner guarantees this; a
   handler that starts a goroutine of its own and returns breaks it, and the
   library cannot tell.

## What ordering does to a failure

This is the part that surprises people, because it is a semantic choice rather
than an implementation detail, and the three answers have very different
throughput.

Two options, and they are different questions. `mq.WithOrdering(o)` takes an
`mq.Ordering` and decides **what shares a lane** —
`mq.OrderingByKey` (the zero value), `mq.OrderingUnordered`,
`mq.OrderingByPartition`. `mq.WithOrderingByKey(mode...)` asks for order *and*
carries the `mq.OrderingMode` that decides what a failure does to the lane:

**`mq.OrderingStrict`** — the default once ordering is enabled. A failed
delivery blocks its lane until it is settled or terminally routed. The runner
holds the message **in the process**, waits out the backoff, and calls the
handler again. Nothing behind it on that lane moves.

Two consequences, one good and one expensive. The good one: because the message
never left the process, the attempt count is exact even on a broker that keeps
none. The expensive one: a worker is parked for the length of the backoff, and
at the default `mq.WithConcurrency(1)` that is *the whole runner* stopped for up
to the configured 30-second cap. If you ask for order, raise the concurrency.

**`mq.OrderingRelaxed`** — what a runner that was never asked for order keeps.
The
failed message goes back to the broker with the delay, and the deliveries
behind it carry on. Order is lost for that key from that moment.

**`mq.OrderingStopOnFailure`** — stop the consumer and let the broker's own
redelivery decide what happens next. The right answer when your ordering
requirement is stronger than your availability requirement.

## The attempt bound can be unreachable

`mq.WithMaxAttempts(5)` is a bound on a count, and the count has to come from
somewhere.

`Delivery.Attempt` is **the broker's** delivery count, and it is `0` on a
broker that does not keep one. A message the runner gave back under relaxed
ordering carries nothing else — so on such a broker the runner can never
conclude that the attempts are exhausted, the failure policy is never reached,
and the message is retried for as long as it keeps failing.

Two things make the bound real: strict ordering, which counts in-process
because the message never leaves; or a broker with a native count. The matrix
says which drivers have one, and the driver pages state it under
*Attempt counting*. It is recorded as a known limitation rather than a
surprise — the runner cannot invent a count that no layer kept.

## What else reorders

Concurrency, retries, rebalances, dead-lettering and delayed delivery all can,
and each driver page says how. Three worth naming:

* **a rebalance** moves a partition to another process, which starts from the
  last committed position. Kafka's offset tracker commits only the highest
  *contiguous* completed offset, so a gap blocks the commit rather than
  skipping it — see [rebalance](../reliability/rebalance.md).
* **dead-lettering** removes a message from the middle of a key's sequence.
  Every message behind it then proceeds, in order, without it.
* **a delayed nack** moves a message to the future. Under strict ordering it
  takes its lane with it; under relaxed it does not, and the key is reordered.

## Batches

`RunBatch` groups deliveries before it calls you, and the grouping respects the
ordering mode: under `mq.OrderingByKey` a batch holds the deliveries of one key
hash, and under an unordered mode a whole fetch is one batch. So a batch
handler under ordering sees fewer, smaller batches — the grouping is a
correctness constraint, not a tuning knob.
