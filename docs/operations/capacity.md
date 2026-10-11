# Capacity

How much memory a client holds, and which four numbers decide it.

## The consumer side

**`ConsumerConfig.Prefetch`** is the ceiling on unsettled deliveries the runner
will hold. It is the dominant term in a consumer's footprint and in its
duplicate rate after a crash: every unsettled delivery is a message that comes
back.

**`mq.WithMaxInFlight(n)`** bounds the same thing from the runner's side, and
`mq.WithConcurrency(n)` bounds how many handlers run at once. A prefetch far
above the concurrency buys latency at the cost of duplicates; a prefetch equal
to the concurrency is the conservative setting and costs a round trip per
batch of work.

**`ConsumerConfig.BufferReuse`** is the allocation lever: with it on, a
delivery's storage returns to the consumer the moment it is settled. That makes
the receive path allocation-free and makes `Delivery.Body` **borrowed** — see
The specification and `Delivery.CloneMessage`. A handler that keeps the body past the
settlement without cloning it is a data race, and the reason the mode is off by
default.

A rough figure, and it is rough on purpose: `Prefetch × (envelope + body)`.
The envelope is small — the memory engine's is about 96 bytes — and the body is
yours. If your messages are 64 KiB and your prefetch is 1024, you have asked
for 64 MiB per consumer before anything else.

## The publisher side

**`PublisherConfig.MaxPendingMessages`** and **`MaxPendingBytes`** bound the
publisher's own buffer: the asynchronous publications outstanding at the driver
plus the messages waiting in the batching buffer. Zero for the first selects
`mq.DefaultMaxPendingMessages` (1024) rather than "unbounded" — the specification requires
the buffer to be bounded by default, so there is no value that turns the bound
off.

`PublisherConfig.Backpressure` is what happens at the bound, and the two drop
modes are the only way a publication is lost without the caller being told; see
[backpressure](../concepts/backpressure.md).

Then there is the client library's own buffer, which is not this library's and
which the driver page states.

## The memory engine

Different question, because here the engine *is* the broker and the messages
are yours to hold.

| Bound | What it limits |
|---|---|
| `Config.MaxTotalMessages`, `MaxTotalBytes` | the whole broker |
| `TopologyConfig.CapacityMessages` | per subscription-partition; default 65,536 |
| `TopologyConfig.CapacityBytes` | the same, by size |
| `TopologyConfig.MaxInFlight`, `MaxDelayed` | unsettled and scheduled records per lane |
| `TopologyConfig.Retention` | the retained log, for `TopologyStream` |

the last invariant is the one to hold onto: **fixed limits imply a memory
plateau.** A broker configured with bounds does not grow without limit under
any load — the model fuzzer asserts it after every step, and a run that grew
would be a failure rather than a tuning note.

`CapacityMessages: 0` selects an unbounded chunked queue, and it is explicit
for that reason.

## Sizing, in the order the questions actually arrive

1. **How large is a message?** `caps.Limit(mq.LimitMaxMessageBytes)` is what
   the broker will take, and `Known == false` means the driver has not said.
2. **How many can be in flight?** Prefetch × concurrency, and then ask whether
   you can afford to reprocess all of them after a crash.
3. **How long can a handler take?** If it is close to the lease, you need
   `mq.WithLeaseExtension` — and a driver that has a `DeadlineExtender`. The
   matrix's `ExtendLease` row says which do.
4. **What happens when the broker slows down?** That is the backpressure mode,
   and it is the question most people answer last and should answer first.

## Measured numbers

Per-operation costs, allocation budgets and the throughput figures live in
[the performance section](../performance/index.md). Two things to carry over
here:

* the allocation budgets in `benchmarks/budgets.yaml` are **gated** — a change
  that allocates on a path the specification requires to be allocation-free fails
  `make gate`;
* the **overhead** budgets against the raw clients are **breached** on 10
  of the 12 batched rows measured, by up to 46 %.
  Driver overhead has the figures.
