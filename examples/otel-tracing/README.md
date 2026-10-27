# otel-tracing — the span tree, printed

A caller's span, the `publish` span under it, and — after the message has
crossed a broker — the `process` span under **that**, in one trace. Over the
in-process memory broker and over Redis Streams
([`drivers/redis`](../../drivers/redis)), through
[`integrations/otel`](../../integrations/otel).

    go run .          # against a Redis on 127.0.0.1:6379
    go test ./...     # the same program against the memory broker, and against
                      # Redis when IQMQ_REDIS_ADDR names one

## What it shows

```
instrumented: driver memory as messaging.system=memory
trace: checkout -> publish orders-dlh3j -> process orders-dlh3j, one trace id across the broker
beside it: "receive" and "ack" are ROOT spans, because Run starts outside any span and a settlement has no context of its own, and the settle span is named for the settlement rather than "settle" 
attributes: messaging.system=memory, messaging.destination.name=<topic>, semantic conventions pinned at 1.43.0
instruments: iqmq_publish_duration, iqmq_handler_duration, iqmq_end_to_end_duration recorded a point each, with no unit suffix — the exporter appends it 
```

## The four spans, and why only two are in the tree

| span | source | parent |
|---|---|---|
| `publish <topic>` | `mq.PublishMiddleware` | the **caller's** span; writes the `traceparent` onto the message |
| `process <topic>` | `mq.Middleware` | the **publisher's** span, read back out of the delivery's headers and marked remote |
| `receive <topic>` | `mq.Hooks.OnReceive` | the span of the context the fetch ran in: a root here, because `Run` starts outside any span |
| `ack <topic>` | `mq.Hooks.OnSettle` | **none**, a root |

The first two are proper children, and that is what makes one trace span the
broker. The last two are roots here. A `receive` span is a child of the
context `Receive` was called with, and the runner fetches under `Run`'s
context, which this program starts outside any span. A settle span is always
a root: `OnSettle` is given `context.Background()`, because a settlement has
no context of its own (`Delivery.Ack` takes none), so there is nothing to
parent it to. Both are recorded with explicit start and end timestamps and a
reader sees them *beside* the trace. This program asserts it, so it fails the
day either changes.

Two smaller surprises the output names:

- the settle span is called **`ack <topic>`**, not `settle <topic>`. Its
  `messaging.operation.name` is the settlement's own — ack, nack or extend —
  under the pinned operation *type* `settle`. Searching a trace viewer for
  "settle" finds nothing.
- there is usually **more than one** `receive` span, because a fetch loop makes
  as many fetches as it makes. The program asserts that every one of them is a
  root and counts none of them.

## Two things that silently produce nothing

**A bound publisher needs `PublishMiddlewareFor`.** The specification makes a bound handle
ignore `Message.Topic`, so a publication through one reaches the publish chain
with an empty topic and `PublishMiddleware` builds a span called `publish` with
no `messaging.destination.name` at all — present, plausible, and impossible to
group by. Watched: the mutation produces a span literally named `publish`
beside the correctly-named `process orders-…`.

**`iqmq_end_to_end_duration` needs `PublisherConfig.Timestamp`.** The histogram
is measured from `Properties.CreatedAt` to the handler being called, and the specification's
rule is that "Publish never generates IDs or timestamps on the hot path unless
configured" — so a publisher without it stamps nothing, the consumer has no
origin, and the series is simply **absent from the export**. No error, no log
line, and indistinguishable from a consumer that is not running.
`TestWithoutTimestampTheEndToEndHistogramIsSilent` is that, watched failing.

## What it does not do

It exports nothing. Spans go to an in-memory `tracetest.SpanRecorder` and
metrics to a `metric.ManualReader`, because the subject is the **shape** of the
telemetry and an OTLP exporter would make the example about a collector.
Swapping the two providers in `newTelemetry` for `otlptracegrpc` and a periodic
reader is the whole change a service makes.

It also asserts no gauge. `iqmq_in_flight` is observed at collection time and
reads zero once the consumer has drained, and the lag gauge needs an
`Admin.Stats` round trip this program does not make.

## The two arms

| arm | driver | gate |
|---|---|---|
| in-process | `memory` | never skips |
| broker | `drivers/redis` | `IQMQ_REDIS_ADDR` |

Nothing about tracing is broker-specific — what carries the trace is a header
on the message — so the memory broker exercises the whole tree. What the broker
arm adds is that a real broker has to carry a header it knows nothing about
through whatever its native metadata is, or the `process` span loses its parent
and the trace breaks in two. Redis Streams carries it as a field of the entry.

    docker run -d -P \
      redis@sha256:ff02b58f971e7d7d156a1267e283fcbbeee91773b6aa36c49dac28ecfe28eadf
    IQMQ_REDIS_ADDR=127.0.0.1:<mapped port> go test ./...

`-P` and not a fixed `-p 6379:6379`: on a shared Docker host the port you name
may already be held by somebody else's container, and a `docker run` that fails
while your test still passes means your test measured **their** broker. That
happened while this example was being written.

`IQMQ_REQUIRE_BROKERS=1` turns the unset variable from a skip into a failure.
