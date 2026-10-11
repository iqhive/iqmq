# Tuning

The knobs, in the order they matter, with what each one costs. The specification rule 10
requires every driver to document its own with the measured effect; this page
is the portable layer.

## The four that matter most

**`ConsumerConfig.Prefetch`.** How many unsettled deliveries the runner will
hold. Raising it buys throughput by hiding the fetch round trip, and costs
memory (`Prefetch × message size`) and duplicates after a crash (every
unsettled delivery comes back). Start at your concurrency and raise it while
watching `ConsumerStats.InFlight`.

**`mq.WithConcurrency(n)`.** How many handlers run at once. The default is one
serial worker per assigned lane, which
preserves order. If you have asked for `mq.OrderingStrict`, **raise this**: a
strict retry parks a worker for the whole backoff, so at concurrency 1 the
first failure stops the runner for up to 30 seconds.

**`ConsumerConfig.BufferReuse`.** Returns a delivery's storage to the consumer
the moment it is settled, which makes the receive path allocation-free. The
price is `Delivery.Body` and the headers are **borrowed** until
settlement, so a handler that keeps them must `CloneMessage` first. Off by
default for that reason.

**The publisher's batching.** `PublisherConfig.Batching` groups asynchronous
publications. It is the single largest throughput lever on every broker — and
it is also where the batched overhead budget is breached by 7 % to 46 %, so
it is the path where this library currently costs you the most against the raw
client. Driver overhead has the figures.

## The ones that change semantics, not speed

Worth separating, because they look like tuning and are not:

| Knob | Looks like | Actually is |
|---|---|---|
| `mq.ConfirmNone` | "faster publishes" | a different durability contract: may be lossy |
| `mq.BackpressureDropNewest` / `DropOldest` | "never block" | a publication lost without the caller being told |
| `mq.FailureSkip` | "stop the queue filling up" | an ack for a message nobody handled |
| `mq.OrderingRelaxed` | "higher throughput on failure" | order abandoned for that key from the failure onwards |
| `mq.MetadataPortableEnvelope` | "portable headers" | a wire-format change every other reader of that topic sees |

Each of those is the right answer sometimes. None is a tuning knob.

## Per-driver knobs

Every driver page has a *Tuning* section with its own — linger, prefetch,
poller count, pipelining depth, batch size, credit window — and rule 10
requires the measured effect rather than an assertion. The shapes that recur:

* **linger versus latency.** Every batching producer trades one for the other.
  The knob is the client library's and the driver exposes it in typed config.
* **poller count.** SQS's long poll is 20 seconds × 10 messages per poller, so
  throughput is `pollers × 10 / poll duration` and the only way up is more
  pollers.
* **pipelining depth.** Redis: how many commands are in flight. Both Redis
  drivers pipeline batches, and it is the lever the batched overhead breach
  lives on.
* **credit window.** AMQP 1.0: `ReceiverOptions.Credit`, which is the prefetch
  under another name.
* **the ack batching strategy.** SQS coalesces `DeleteMessageBatch`, and which
  strategy — immediate, count, time, hybrid — was a bake-off of its own.
  `Ack` waits for the configured delete boundary, so the
  strategy is visible in your ack latency.

## The memory engine

`memory.EngineConfig` selects the variant implementations, and every field's
`Auto` value is the bake-off winner recorded in an ADR:

```go
memory.EngineConfig{
    Queue:     memory.QueueAuto,   // lane queue
    Wait:      memory.WaitAuto,    // empty-lane strategy
    Timer:     memory.TimerAuto,   // scheduler
    // …Arch, Hash, Pool, Registry, RetryLane
}
```

**Leave them on `Auto`.** The whole point of the bake-off is that `Auto` moves
when the evidence moves and your code does not. Naming a kind explicitly is for
a differential test, or for a workload you have measured yourself and found the
default wrong for — and if you have, that is a benchmark worth contributing,
because it is a row the campaign is missing.

`memory.SafeEngine()` forces the mutex reference implementations. It is for
differential testing, not for production.

## How to tell whether a change helped

```sh
make bench-quick                         # smoke: seconds, never a decision
make gate BASE=main GATE=pr              # the regression gate, two interleaved runs
make allocs                              # allocation counts; load-insensitive, always trustworthy
```

Three rules from this repository's own history:

**Allocation tests are always trustworthy; timing on a shared host is not.**
`testing.AllocsPerRun` is deterministic. Run `make allocs` freely; treat a
timing delta on a loaded machine as noise until the guard says otherwise.

**A verdict of `clean` is not a verdict of `quiet`.** `clean` means no foreign
*benchmark* ran. Read `foreign_runnable_p95` beside it.

**Suspect the instrument.** A number that is implausibly bad is as likely to be
the harness as the code — and in this repository it has more often been the
harness.
