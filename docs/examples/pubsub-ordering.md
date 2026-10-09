# pubsub-ordering

[`examples/pubsub-ordering`](https://github.com/iqhive/iqmq/tree/main/examples/pubsub-ordering)
— per-key ordering through the portable API, run **unchanged** on two drivers:
the in-process memory broker and Google Cloud Pub/Sub's ordering keys through
`drivers/gcppubsub`.

Every line of its output is identical on both, which is the claim:

```
claims: ordering by key true at scope key
order: ord-1001 delivered created,paid,shipped
order: ord-1002 delivered created,paid,shipped
order: ord-1003 delivered created,paid,shipped
concurrency: 4 handlers over 3 key(s), and no key ever had two deliveries in flight at once
shutdown: received 9, acked 9, in flight 0, settle failures 0
```

## Ordering by key is three decisions

| Who | What it must do | If it does not |
|---|---|---|
| the **broker** | claim `FeatureOrderedByKey` at `ScopeKey` | nothing is ordered; the capability table is the only honest place to look |
| the **publisher** | put every event of one entity under one `Message.Key` | the service orders nothing and no consumer can put the events back |
| the **runner** | serialise a key — the specification routes by `hash(key) % lanes` | `WithConcurrency(n)` processes two events of one key at once |

Pub/Sub makes the claim only with `Config.Ordering`, which sets the client
library's `EnableMessageOrdering` and the subscription's own flag. A publisher
built **without** it **refuses** every publication that carries a key, so this
is not a setting to discover in production.

The example publishes *interleaved* — `created` for all three keys, then `paid`
for all three — on purpose. A program that published one key at a time would
print the right answer with no ordering at all.

## `mq.WithOrdering` is not a request the broker answers

It names how the runner **routes**, and nothing refuses it for a scope the
profile does not have. Pub/Sub has no partitions and its capability table says
so; `mq.WithOrdering(mq.OrderingByPartition)` is accepted anyway, every
delivery then carries partition 0, and the whole subscription collapses onto
one lane. A throughput cliff rather than an error, with nothing in the API to
report it. `TestOrderingByPartitionCollapsesEveryKeyOntoOneLane` shows it
deterministically on a single-partition memory topic, where four configured
handlers never run two at once and the order is still perfectly kept.

## What the example deliberately does not assert

Cross-key parallelism. Lanes are `hash(key) % lanes`, so two of three keys may
share one whatever the concurrency is, and whether two keys overlap in *time*
is the scheduler's business — breaking the collapse test above measures two
deliveries at once, not three. The example asserts the half that is a
guarantee, per key and per delivery, rather than printing a number that would
fail at random on a loaded machine.

## The two arms

| arm | driver | gate |
|---|---|---|
| in-process | `memory`, 8 partitions | never skips |
| broker | `drivers/gcppubsub` | `IQMQ_PUBSUB_ADDR` |

The in-process arm is the memory broker and not a Pub/Sub fake, because
`drivers/gcppubsub`'s fake is `internal/psfake` and cannot be imported from
outside that module. So it checks the portable contract rather than Pub/Sub's
own mapping of it, and the broker arm — which asserts exactly the same
output — is what says Pub/Sub keeps the contract.
