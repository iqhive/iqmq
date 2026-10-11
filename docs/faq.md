# FAQ

## Is this ready to use?

No. It is pre-alpha and under construction against
[the specification](https://github.com/iqhive/iqmq/blob/main/the API contract).
Status is the honest list of what works, what is measured and what
is open, including the parts of the specification the implementation does not
yet meet.

## Does it give me exactly-once processing?

No, and nothing can give you that at the messaging layer alone. The specification recognises
*broker-scoped exactly-once **delivery*** — Pub/Sub exactly-once subscriptions,
Kafka transactions with `read_committed`, Pulsar transactions, JetStream dedup
— and each of those deduplicates the **broker's own record**, within its own
scope and window.

Your side effects are a different problem, and the answer is an idempotent
handler or an inbox table. See
[duplicates and idempotency](concepts/idempotency.md) and
[inbox and outbox](reliability/inbox-outbox.md).

## Why one Go module per driver?

So that depending on this library costs you **one** broker SDK — the one you
chose — rather than eleven. The root module and the `memory` driver depend on
the standard library only, and `make stdlib-check` holds them to it.

It is a security property as much as a build-time one: your program's attack
surface is the client you actually use.

## Why is the API so large?

Because a portable messaging API has two honest designs and both are bad. The
**intersection** of what every broker does is small and useless. The **union**
is large and requires lying about the brokers that cannot do it.

This library takes the union and pays for it with the capability model:
everything is guarded by a level the driver either advertises or refuses, and
asking for something a broker cannot do fails at construction or before any
I/O. [Capability negotiation](concepts/capabilities.md) is the mechanism.

## Why does `Publish` block?

Because `mq.BackpressureBlock` is the default, and blocking is the reliable
answer: the caller is either served or told. If you would rather shed load,
put a deadline on the context or ask for `mq.BackpressureReject`. The two drop
modes exist and are never defaults, because they lose a publication without
telling the caller. [Backpressure](concepts/backpressure.md).

## Why won't it retry my ambiguous publish?

Because a library that silently retries an ambiguous publication silently
duplicates whatever that publication caused. The specification forbids the core from
auto-retrying unless native idempotent publication makes it safe within its
documented scope, or you asked for it explicitly.
[Ambiguous publishes](concepts/ambiguous-publishes.md).

## The same message keeps arriving and never gets dead-lettered. Why?

Almost certainly because your broker does not keep a delivery count, so
`Delivery.Attempt` is `0`, and under the default `mq.OrderingRelaxed` the
runner has no count to compare with `mq.WithMaxAttempts`. Use
`mq.OrderingStrict` — which holds the message in the process and counts
exactly — or a broker with a native count. The matrix says which have one.
[Ordering](concepts/ordering.md#the-attempt-bound-can-be-unreachable).

## Why did throughput collapse after one failure?

`mq.OrderingStrict` at the default `mq.WithConcurrency(1)`. A strict retry
holds the lane in the process for the whole backoff, so one worker is parked
for up to the configured 30-second cap — and at concurrency 1 that is the whole
runner. Raise the concurrency, or lower `mq.ExponentialBackoff.Max`.

## What does `(N)` mean in the capability matrix?

The driver *advertises* that level and **no probe verified it**. A bare `N`
means a probe verified it in both directions. There are also `—` (not offered,
and the refusal was observed), `·` (a denied guarantee no run contradicted),
`?` (nothing established it either way), `!` (a scenario FAILED), and `◐` (the
evidence is an emulator's). [Capability negotiation](concepts/capabilities.md)
has the whole table.

## Can I use the memory driver as a mock?

Use it as a *driver*. It implements the same `mq.Backend` as Kafka, so a test
against it exercises the real runner and the real settlement state machine.
`memory.NewForTest(t)` adds a fake clock, `AutoCreate`, and a leak check.

It is never persistent and never advertises durable storage, so it cannot
stand in for a durability test. [The memory driver](testing/memory-driver.md).

## How do I test a 30-second backoff?

`broker.Advance(30 * time.Second)` — never `time.Sleep`. Inside a
`testing/synctest` bubble you can go further and assert the **schedule**, which
is what catches a runner that stopped waiting between attempts.
[synctest](testing/synctest.md).

## How do I assess performance?

Run the benchmarks against the revision, broker and workload you plan
to deploy. See the [performance guide](performance/index.md) and
[methodology](performance/methodology.md). Budget thresholds describe
targets; they are not measurements or evidence that a target was met.

## Which driver should I use?

The one your broker is. [The selection guide](drivers/index.md) compares them,
and where there are two clients for one broker — Kafka, Redis — the choice is
which module you import, with the bake-off's recommendation on the driver
page.

## How do I write a driver for a broker you do not support?

[The driver author guide](drivers/author-guide.md). The short version:
implement `Backend`, `Sink` and `Source`; add the optional interfaces your
broker can support; build errors with `driverkit.ErrorMapper` or an
`mq.OpError`; and run `testkit.RunConformance` against it. The kit uses only
the portable API, so it works from outside this repository —
`examples/custom-driver` is a worked example, with a test proving it imports no
internal package.

## Where do I report a bug?

A GitHub issue, with the `iqmq doctor` dump for a driver bug — that is what the
issue form asks for. A **security** issue goes through
[private vulnerability reporting](security/index.md) and never a public issue.
