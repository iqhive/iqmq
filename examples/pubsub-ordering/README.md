# pubsub-ordering — per-key order, on two drivers, unchanged

Keyed publishing, four handlers, and every entity's events still arriving in
publication order — over the in-process memory broker and over Google Cloud
Pub/Sub's ordering keys through
`drivers/gcppubsub`.

    go run .          # against the Pub/Sub emulator on 127.0.0.1:8085
    go test ./...     # the same program against the memory broker, and against
                      # the emulator when IQMQ_PUBSUB_ADDR names one

## What it shows

```
claims: ordering by key true at scope key
order: ord-1001 delivered created,paid,shipped
order: ord-1002 delivered created,paid,shipped
order: ord-1003 delivered created,paid,shipped
concurrency: 4 handlers over 3 key(s), and no key ever had two deliveries in flight at once
shutdown: received 9, acked 9, in flight 0, settle failures 0
```

Every line is identical on both drivers, and that is the point: the first line
is a **capability**, not a promise this library makes, and the rest is what the
portable API does with it.

## Ordering by key is three decisions

- **The broker** has to keep an order, at a scope its capability table names.
  Pub/Sub makes the claim only with `Config.Ordering`, which turns on the
  client library's `EnableMessageOrdering` and the subscription's own flag; the
  memory broker makes it natively. Without `Config.Ordering` a Pub/Sub
  publisher **refuses** every publication that carries a key, so this is not a
  setting to discover in production.
- **The publisher** has to put every event of one entity under one
  `Message.Key`, which becomes Pub/Sub's ordering key. This program publishes
  *interleaved* — `created` for all three keys, then `paid` for all three — on
  purpose: a program that published one key at a time would print the right
  answer with no ordering at all.
- **The runner** has to serialise a key. The specification routes by `hash(key) % lanes` and
  one lane is one handler at a time, so `mq.WithConcurrency(4)` runs four
  *lanes* and never two events of one key.

## What `mq.WithOrdering` does not do

It names how the runner **routes**. It is not a request the broker answers, and
nothing refuses it for a scope the profile does not have: Pub/Sub has no
partitions and its capability table says so, and `mq.WithOrdering(
mq.OrderingByPartition)` is accepted anyway. Every delivery then carries
partition 0 and the whole subscription collapses onto one lane — a throughput
cliff rather than an error, with nothing in the API to report it.

`TestOrderingByPartitionCollapsesEveryKeyOntoOneLane` is that, deterministically:
on a single-partition memory topic under `OrderingByPartition`, four configured
handlers never run two at once, and the order is still perfectly kept.

## Why the parallelism is not in the output

The tempting assertion — "four handlers and three keys, so three keys were in
flight at once" — cannot be made deterministic. Lanes are `hash(key) % lanes`,
so two of three keys may share a lane whatever the concurrency is, and whether
two keys overlap in *time* is the scheduler's business. Breaking the collapse
test above (eight partitions instead of one) shows two deliveries at once, not
three. So the example asserts the half that is a guarantee — per key, per
delivery — and says the rest in prose rather than printing a number that would
fail at random on a loaded machine.

## The two arms

| arm | driver | gate |
|---|---|---|
| in-process | `memory`, 8 partitions | never skips |
| broker | `drivers/gcppubsub` | `IQMQ_PUBSUB_ADDR` |

The in-process arm is the **memory broker** and not a Pub/Sub fake, because
`drivers/gcppubsub`'s in-process fake is `internal/psfake` and cannot be
imported from outside that module. So it checks the portable contract rather
than Pub/Sub's own mapping of it, and the broker arm — which asserts exactly
the same output — is what says Pub/Sub keeps the same contract. Run against the pinned emulator:

    docker run -d -p 127.0.0.1:8085:8085 \
      gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:07e4b8c3075ca793552fcfaf4808f104ef155d7805d87ade8e01b440463be262 \
      /bin/sh -c "gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=iqmq-example"
    IQMQ_PUBSUB_ADDR=127.0.0.1:8085 go test ./...

`IQMQ_REQUIRE_BROKERS=1` turns the unset variable from a skip into a failure.

## Reading the counters after `Run` has returned

The last line is `ConsumerStats`, and it is read **after** `Shutdown` and after
`Run` has returned. The runner acks a delivery once the handler returns, so a
program that reads the counters when its last handler signals completion sees
"acked 8, want 9" — the defect, which cost
`examples/kafka-orders` six failures in ten under `-race`. The ledger here
closes its `done` channel only after its own accounting for the final delivery
is finished, and the main goroutine then drains before it counts.
