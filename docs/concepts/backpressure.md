# Backpressure

Specification. There are two independent bounded buffers in a publishing
path and they fail differently, so this page separates them before it says
anything else.

## The two places a publication can be held

**The broker's**, or rather the client library's. This is the one you cannot
see: franz-go's producer buffer, rueidis's pipeline, the Pub/Sub publisher's
batch. Every one of them has a bound and every one of them has its own policy,
and the driver page states it.

**The publisher's own**, which is this library's and which you configure.
`mq.PublisherConfig.MaxPendingMessages` and `MaxPendingBytes` bound the
*asynchronous* path — `PublishAsync` and the batching publisher — and
`PublisherConfig.Backpressure`, an `mq.BackpressureMode`, says what happens
when a bound is reached:

Zero for `MaxPendingMessages` selects `mq.DefaultMaxPendingMessages` rather
than "unbounded": the specification requires the buffer to be bounded by default, so there
is no value of the field that turns the bound off.

| Mode | When the buffer is full | Reliable? |
|---|---|---|
| `mq.BackpressureBlock` | the caller waits until there is room or the context ends | yes — the default |
| `mq.BackpressureReject` | the call returns `ErrBufferFull` immediately | yes |
| `mq.BackpressureDropNewest` | this publication is discarded | **no** |
| `mq.BackpressureDropOldest` | an earlier, unsent publication is discarded | **no** |

`BackpressureMode.Reliable()` is that last column, in code. Blocking and
rejecting are reliable because the caller is either served or told; the two drop
modes lose a publication the caller was not told about, which is why they are
never a default and why `iqmq_dropped_total{policy}` exists.

## Blocking is the default, and blocking means blocking

A `Publish` on a full buffer with `mq.BackpressureBlock` waits. It waits until
there is room, or until the context you passed is done — which is the whole
reason every call takes a context. A service with no deadline on its publish
context and a broker that has stopped accepting will stop serving; that is not
a defect, it is the reliable choice propagating. Put a timeout on the context
if you would rather shed load, or ask for `mq.BackpressureReject` and decide
what to do with the error.

## The consumer side is a bound, not a policy

A consumer does not need a drop policy, because the broker is already holding
the messages: `mq.ConsumerConfig.Prefetch` bounds how many unsettled deliveries
the runner will hold, and when it is reached the runner simply stops fetching.
That is backpressure in the direction that costs nothing — the messages stay at
the broker, redeliverable, rather than in your heap.

`mq.WithMaxInFlight(n)` bounds the same thing from the runner's side. A
prefetch far above your concurrency buys latency and costs duplicates after a
crash, because every unsettled delivery is a delivery that comes back.

## Inside the memory engine

`memory.TopologyConfig.Overflow` is the same question for the in-process
engine, where the publication really does have nowhere else to go:
`memory.OverflowBlock` (the default), `OverflowReject`, `OverflowDropNewest`,
`OverflowDropOldest`. And `memory.TopologyConfig.Fanout` decides what a *slow
subscription* does to a fast one: `Atomic` reserves capacity in every target
before it enqueues to any, so a full subscription blocks the publication rather
than silently getting fewer messages than its siblings.

That atomic default is why the memory engine is a faithful stand-in rather than
a fast approximation: the failure mode you would meet on a real broker with an
overfull consumer is the failure mode you meet in a test.

## What the numbers say

The publisher's backpressure path is measured and gated —
`benchmarks/budgets.yaml` holds its allocation and latency budgets — but
the **overhead** budgets against the raw clients are breached on the
batched path on every broker family measured, by 7 % to 46 %.
Driver overhead has the figures and the
methodology. Nothing here is widened to make that look better.
