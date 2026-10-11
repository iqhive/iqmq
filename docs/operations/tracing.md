# Tracing

`integrations/otel` instruments iqmq with OpenTelemetry: the API's four spans, its histograms and gauges, and W3C trace context carried
through a message's headers.

```go
import (
    mq "github.com/iqhive/iqmq"
    "github.com/iqhive/iqmq/middleware"
    otelmq "github.com/iqhive/iqmq/integrations/otel"
)

i, err := otelmq.New(otelmq.Config{
    TracerProvider: tp,          // nil disables every span
    MeterProvider:  mp,          // nil disables every instrument
    System:         "kafka",     // messaging.system; required
})
if err != nil {
    return err
}

client := mq.NewClient(backend, mq.WithHooks(i.Hooks()))

// Publishing: the span and the injected trace context.
pub, _ := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
out := middleware.WrapPublisher(pub, i.PublishMiddlewareFor("orders"))

// Consuming: the process span, parented to the publisher's.
consumer.Run(ctx, handler, mq.WithMiddleware(i.Middleware()))

// The gauges.
i.AddConsumer("orders", "billing", consumer.Stats)
if st, err := admin.Stats(ctx, "orders", "billing"); err == nil {
    i.ObserveQueueStats("orders", "billing", st)
}
```

Use `PublishMiddlewareFor("orders")` for a publisher bound to a topic.
`mq.PublishFunc`'s topic argument is read from `Message.Topic`, and the specification says
a bound publisher *ignores* `Message.Topic` — so the chain sees an empty one,
and a span built from it would have no `messaging.destination.name`.

## The four spans

| Span | Comes from | Parent |
|---|---|---|
| `publish <topic>` | `PublishMiddleware` | the caller's span |
| `process <topic>` | `Middleware` | the **publisher's** span, read out of the delivery's headers |
| `receive <topic>` | `Hooks.OnReceive` | none |
| `ack`/`nack`/`extend` `<topic>` | `Hooks.OnSettle` | none |

The first two are proper children, and together they are what makes one trace
span the broker. The last two **cannot** be: `mq.Hooks` carries no
`context.Context`, so there is nothing to parent them to and nothing to put
them in. They are recorded with explicit start and end timestamps derived from
the event's own duration, so their position in time is right even though their
position in the trace is nowhere.

If you would rather have no span than an unparented one — or if one settle
span per message is more than you will pay for — turn them off:

```go
otelmq.Config{Spans: otelmq.Spans{NoReceive: true, NoSettle: true}}
```

A suppressed span's hook is not installed at all, so the core does not build
its event either.

## Pinned semantic conventions

The attribute names come from **one pinned convention version**,
`otelmq.SemconvVersion`, and nothing derives an attribute name from the OTel
module's own release. Changing the semantic-convention version changes the exported attribute names.

The messaging conventions have renamed attributes between releases —
`messaging.destination` became `messaging.destination.name`,
`messaging.operation` became `messaging.operation.type` with a narrowed value
set, `messaging.kafka.consumer.group` became the system-independent
`messaging.consumer.group.name`. A dashboard built on the old name shows an
empty graph after the upgrade that renamed it, and nothing at compile time
notices. So the whole span surface — every name, kind, attribute key and
attribute value — is pinned by a golden file
(`integrations/otel/testdata/spans.golden`).

The specification names the four spans `publish`, `receive`, `process` and `settle`, and
those are the `messaging.operation.name` values. `messaging.operation.type`
carries the convention's own enumeration, which has no "publish" and calls it
`send`; a settlement's operation *name* is `ack`, `nack` or `extend` under
operation *type* `settle`.

A message identifier is never an attribute: one attribute value per
message is how a tracing backend's index falls over.

## W3C trace context, and the envelope

`Inject` and `Extract` render and parse through `middleware.TraceContext`,
which is the root module's own W3C implementation — the one the correlation
middleware uses and `FuzzTraceContext` fuzzes. There is deliberately no second
W3C parser in the repository.

A malformed `traceparent` never fails a delivery and never becomes a parent:
The specification makes broker-delivered metadata untrusted, and a consumer a publisher can
stop by writing nonsense into a header is a denial of service. `tracestate` is
bounded at 512 bytes and dropped if it carries control characters.

There is nothing special for a broker with no headers, and that is the design:
a backend whose body is an opaque blob carries the whole header table in
iqmq's own envelope (the specification's `MetadataPortableEnvelope`, the `wire` package),
so a `traceparent` written into `Headers` travels inside the envelope on
exactly the brokers that have no header field and as a native header
everywhere else. `FuzzTraceContext` proves it through `wire` itself.

For baggage as well as trace context, hand in an OpenTelemetry propagator:

```go
otelmq.Config{Propagator: otelmq.W3CPropagator()}
```

That replaces `Inject`/`Extract` on every path with upstream's implementation,
including its own W3C parser, which is why it is not the default.

## The instruments

 OpenTelemetry sentence asks for three histograms and two gauges. The
instrument names carry no unit suffix, because an OpenTelemetry instrument
names its unit in a field and a Prometheus exporter appends the suffix from
it — so what reaches a Prometheus server is the canonical name.

| Instrument | Unit | Exported as |
|---|---|---|
| `iqmq_publish_duration` | `s` | `iqmq_publish_duration_seconds` |
| `iqmq_handler_duration` | `s` | `iqmq_handler_duration_seconds` |
| `iqmq_end_to_end_duration` | `s` | `iqmq_end_to_end_duration_seconds` |
| `iqmq_in_flight` | `{message}` | `iqmq_in_flight` |
| `iqmq_lag` | `{message}` | `iqmq_lag` |

Two of them are honest about what they cannot know:

- **End-to-end latency needs a creation timestamp.** The specification says the publish
  path never reads the clock on its own, so a message carries
  `Properties.CreatedAt` only when the publisher was configured to stamp one
  (`middleware.Timestamp`). A message without one contributes **no
  observation** rather than a latency measured from the zero time.
- **Lag has no source in the SPI.** `ConsumerStats.Lag` is always unknown at
  v0, so the gauge is observable and reports a series only for a subscription
  whose `Admin.Stats` you polled through `ObserveQueueStats`. A subscription
  nobody has measured has no data point, rather than a zero that reads as
  "caught up".

## Cardinality and cost

`Cardinality` bounds how many `(operation, topic, subscription)` attribute
sets are cached and folds the rest onto one overflow destination, keeping the
operation so that a publication never appears in a span called
`process other`.

A destination name is not necessarily yours either: the specification makes a delivery's
topic the concrete one the broker reported. Bytes that are not valid UTF-8 are
replaced with U+FFFD before they become a span name or an attribute value,
because an OTLP exporter encodes both into protobuf `string` fields and proto3
requires valid UTF-8 — one odd topic would otherwise fail the export of a
whole batch and lose every span in it.

With neither provider set, the middlewares are the identity and `Hooks()` is
the zero `mq.Hooks`: **zero allocations, and no code on the message path at
all**. With a non-recording tracer — the state a sampled-out span is in — a
publish or process span costs three allocations and an unparented receive or
settle span five, every one of them the OpenTelemetry API's own (a span's
context value, an injected header's bytes, the boxed timestamp options). Not
one of them is an attribute slice or a formatted span name: those are built
once per destination. A recording SDK adds whatever it charges for a span.
`TestAllocs` states all four columns.
