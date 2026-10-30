# Metrics

`integrations/prometheus` exposes iqmq's observation points as Prometheus
metrics under the canonical names the API fixes.

```go
import (
    mq "github.com/iqhive/iqmq"
    "github.com/iqhive/iqmq/middleware"
    iqmqprom "github.com/iqhive/iqmq/integrations/prometheus"
    "github.com/prometheus/client_golang/prometheus"
)

reg := prometheus.NewRegistry()
m, err := iqmqprom.New(iqmqprom.Config{Registerer: reg})
if err != nil {
    return err
}

client := mq.NewClient(backend, mq.WithHooks(m.Hooks()))
```

Several families come from somewhere other than the hooks, because the core
has no hook for them:

```go
// The handler families, through the middleware chain.
consumer.Run(ctx, handler, mq.WithMiddleware(middleware.Metrics(m.Recorder(), nil)))

// The gauges and the counters the core keeps in its stats snapshots.
m.AddConsumer("orders", "billing", consumer.Stats)
m.AddPublisher("orders", mq.BackpressureBlock, publisher.Stats)

// The broker's own view, polled on your schedule rather than on a scrape.
if st, err := admin.Stats(ctx, "orders", "billing"); err == nil {
    m.ObserveQueueStats("orders", "billing", st)
}
// When you stop polling a queue, so its last answer leaves the series.
m.RemoveQueueStats("orders", "billing")

// The memory engine's per-lane gauges.
m.AddMemory("orders", "billing", broker)
```

## The names are the API

A dashboard, an alert rule and a recording rule are all written against a
metric name and its label names, and renaming either breaks every one of them
with nothing at compile time to notice. So there is **no namespace or
subsystem option**: the names are the specification, and the whole exposed surface —
name, type, label names and help text — is pinned by a golden file
(`integrations/prometheus/testdata/metrics.golden`) that `go test` compares
byte for byte.

| Metric | Type | Labels | Source |
|---|---|---|---|
| `iqmq_publish_messages_total` | counter | topic, outcome | `Hooks.OnPublishDone` |
| `iqmq_publish_bytes_total` | counter | topic | `Hooks.OnPublishDone` |
| `iqmq_publish_duration_seconds` | histogram | topic, outcome | `Hooks.OnPublishDone` |
| `iqmq_publish_batch_size` | histogram | topic | `Hooks.OnPublishDone` |
| `iqmq_consume_messages_total` | counter | topic, subscription | `Hooks.OnReceive` |
| `iqmq_handler_duration_seconds` | histogram | topic, subscription, outcome | `Metrics.Recorder` |
| `iqmq_settle_duration_seconds` | histogram | topic, subscription, kind | `Hooks.OnSettle` |
| `iqmq_settle_failures_total` | counter | topic, subscription, kind | `Hooks.OnSettle` |
| `iqmq_lease_extensions_total` | counter | topic, subscription, outcome | `Hooks.OnSettle` |
| `iqmq_reconnects_total` | counter | driver, outcome | `Hooks.OnReconnect` (see below) |
| `iqmq_redeliveries_total` | counter | topic, subscription | `ConsumerStats.Redelivered` |
| `iqmq_in_flight` | gauge | topic, subscription | `ConsumerStats.InFlight` |
| `iqmq_pending_publishes` | gauge | topic | `PublisherStats.Pending` |
| `iqmq_backpressure_events_total` | counter | topic | `PublisherStats.BackpressureEvents` |
| `iqmq_dropped_total` | counter | topic, policy | `PublisherStats.Drops` |
| `iqmq_queue_depth` | gauge | topic, subscription | `Admin.Stats` |
| `iqmq_lag` | gauge | topic, subscription | `Admin.Stats` |
| `iqmq_memory_ready` | gauge | topic, subscription | `memory.Broker.Inspect` |
| `iqmq_memory_delayed` | gauge | topic, subscription | `memory.Broker.Inspect` |
| `iqmq_memory_inflight` | gauge | topic, subscription | `memory.Broker.Inspect` |
| `iqmq_memory_retained_bytes` | gauge | topic, subscription | `memory.Broker.Inspect` |

Each quantity has exactly one source. Where a hook and a stats snapshot both
carry a number — deliveries received, settlements failed, lease extensions —
the hook wins and the snapshot is not read, because a quantity with two
sources is a quantity that double-counts.

`iqmq_settle_failures_total` has no `kind="extend"` series: an extension is
not a settlement, and its outcome is `iqmq_lease_extensions_total`.

`iqmq_handler_duration_seconds` observes handler calls. Through
`middleware.Metrics`, a delivery that a middleware postponed (`mq.Postpone`,
as a rate limit or an open circuit does) never reached the handler and adds
no observation; the offer that reaches it is observed as usual.

## What is not exposed, and why

A metric that is always zero is worse than none: "no option was ignored" and
"nobody wired up ignored options" look identical on a graph. `Unsourced()`
returns the list in code, and the test that checks it against the registry
runs in `make ci`.

- **`iqmq_option_ignored_total` is not registered.** An option a driver cannot honour is
  a *construction error* naming the
  option, not a silently ignored setting, so nothing in the core or in any
  driver has an ignored option to count.
- **`iqmq_reconnects_total` is registered and the core never fires it.** A
  `Backend` is built before the `Client` that holds `mq.Hooks`, and no part of
  the SPI carries them, so at v0 the reconnect event reaches a **driver's own
  `Config`**. Pass `m.Hooks()` to the driver that offers it. Passing it to
  both the driver and `mq.WithHooks` counts every reconnection twice.
- **`iqmq_dropped_total`'s `policy` label has no source.** `PublisherStats`
  does not carry the backpressure mode and `PublisherHandle` has no accessor
  for it, so `AddPublisher` takes the `mq.BackpressureMode` you configured.
- **`iqmq_queue_depth` and `iqmq_lag` are pushed, not pulled.** The answer is
  a broker round trip, and a collector that called `Admin.Stats` on every
  scrape would put an RPC — with its own timeout, failure and rate limit —
  inside `/metrics`. A value the driver reports as unknown adds nothing, and
  when no queue behind a series reports a value there is **no series at all**,
  so a broker that cannot measure a queue is never graphed as an empty one.
  Each queue keeps its last answer until you observe it again or call
  `RemoveQueueStats`.
- **`ConsumerStats.Lag` is always unknown at v0**, because no SPI carries lag.
  `iqmq_lag` therefore only has series for the subscriptions whose
  `Admin.Stats` you polled. `iqmq_lag` is not in the canonical list, and
  it is the name `integrations/otel` uses for the lag gauge that sentence does
  ask for; one quantity is better named once than twice.
- **`iqmq_memory_retained_bytes` is `memory.Snapshot.Bytes`**, the accounted
  payload bytes of every record the lane holds. The engine has no separate
  retention-log byte count.

## Cardinality

`topic` and `subscription` are label values, and both normally come from your
own configuration — except that a wildcard subscription resolves to whatever
concrete topic the broker delivered, which your program did not choose.

```go
iqmqprom.Config{
    Registerer: reg,
    Cardinality: iqmqprom.Cardinality{
        MaxSeries: 256,      // 0 = 128; negative = unbounded, explicitly
        Overflow:  "other",  // everything past the limit folds onto this value
        OmitTopic: false,    // true replaces the value with "", which PromQL reads as absent
    },
}
```

The fold is a relabelling and not a discard: the observation is still counted,
on the overflow series. A message identifier is never a label.

`OmitTopic` and `OmitSubscription` put several handles or queues on one
series, and the families `AddConsumer`, `AddPublisher`, `AddMemory` and
`ObserveQueueStats` feed report their **sum**: two consumers' in-flight
counts, or the last depth each of two queues reported, rather than whichever
one reported last. `MaxSeries` counts series for these families, not handles
or queues, so one that joins a series already fed is always admitted, and one
that would add a series past the limit is refused with `false` instead of
being folded. A tenant's queue that goes away stays inside its series' sum,
where nothing shows it is stale, until you call `RemoveQueueStats`.

A topic name is also **not necessarily yours**: the specification makes a delivery's topic
the concrete one the broker reported, so a wildcard subscription, a NATS
subject or a queue somebody else created decides what goes into the label.
`client_golang` *panics* on a label value that is not valid UTF-8 — on the
goroutine that published or settled, inside the hook — so invalid bytes are
replaced with U+FFFD before the label is built, one replacement per run, and
the readable part of the name survives. `FuzzMetricLabels` is what found that
and what holds it.

## What it costs

With no hooks installed, nothing: the core builds no event and reads no clock
(the specification's "disabled hooks/observer path: 0 allocs, ≤ 1 predictable branch").

With the hooks installed, **zero allocations per event**. The per-series
children are resolved once, on the first event for a label combination, and
found afterwards through one atomic load and one map lookup. `TestAllocs`
pins both halves, and `make allocs` runs it.
