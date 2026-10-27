# otel-tracing

[`examples/otel-tracing`](https://github.com/iqhive/iqmq/tree/main/examples/otel-tracing)
— the span tree, printed: a caller's span, the `publish` span under it,
and — after the message has crossed a broker — the `process` span under
**that**, in one trace. Over the memory broker and over Redis Streams, through
`integrations/otel`.

## The four spans, and why only two are in the tree

| span | source | parent |
|---|---|---|
| `publish <topic>` | `mq.PublishMiddleware` | the **caller's** span; writes the `traceparent` onto the message |
| `process <topic>` | `mq.Middleware` | the **publisher's** span, read out of the delivery's headers and marked remote |
| `receive <topic>` | `mq.Hooks.OnReceive` | the span of the context the fetch ran in: a root here, because `Run` starts outside any span |
| `ack <topic>` | `mq.Hooks.OnSettle` | **none**, a root |

The first two are proper children, and that is what makes one trace span the
broker. The last two are roots here. A `receive` span is a child of the
context `Receive` was called with, and the runner fetches under `Run`'s
context, which this example starts outside any span. A settle span is always
a root: `OnSettle` is given `context.Background()`, because a settlement has
no context of its own (`Delivery.Ack` takes none), so there is nothing to
parent it to. Both are recorded with explicit timestamps and a reader sees
them *beside* the trace. The example asserts it, so it fails the day either
changes.

Two smaller surprises:

- the settle span is called **`ack <topic>`**. Its `messaging.operation.name`
  is the settlement's own — ack, nack or extend — under the pinned operation
  *type* `settle`. There is no `settle <topic>` span to search a trace viewer
  for.
- there is usually **more than one** `receive` span, because a fetch loop makes
  as many fetches as it makes.

## Two things that silently produce nothing

**A bound publisher needs `PublishMiddlewareFor`.** The specification makes a bound handle
ignore `Message.Topic`, so a publication through one reaches the chain with an
empty topic and `PublishMiddleware` builds a span named just `publish`, with no
`messaging.destination.name` — present, plausible, impossible to group by.

**`iqmq_end_to_end_duration` needs `PublisherConfig.Timestamp`.** The histogram
is measured from `Properties.CreatedAt`, and the rule is that "Publish never
generates IDs or timestamps on the hot path unless configured". Without the
flag nothing stamps, the consumer has no origin, and the series is simply
**absent from the export** — no error, no log line, and indistinguishable from
a consumer that is not running.

## What it does not do

It exports nothing: spans go to a `tracetest.SpanRecorder` and metrics to a
`metric.ManualReader`, because the subject is the **shape** of the telemetry.
Swapping the two providers for `otlptracegrpc` and a periodic reader is the
whole change a service makes.

It asserts no gauge either. `iqmq_in_flight` is observed at collection time and
reads zero once the consumer has drained, and the lag gauge needs an
`Admin.Stats` round trip the program does not make.

## The two arms

| arm | driver | gate |
|---|---|---|
| in-process | `memory` | never skips |
| broker | `drivers/redis` | `IQMQ_REDIS_ADDR` |

Nothing about tracing is broker-specific — what carries the trace is a header
on the message — so the memory broker exercises the whole tree. What the broker
arm adds is that a real broker has to carry a header it knows nothing about
through its own native metadata, or the `process` span loses its parent and the
trace breaks in two.
