# From gocloud.dev/pubsub

`gocloud.dev/pubsub` and iqmq answer the same question — one API over several
brokers — and disagree about what to do when the brokers differ. gocloud
normalises: one `Message` shape, one `Send`, one `Receive`, and where a backend
cannot represent something, gocloud *transforms* it so that it can. iqmq
declares: the capability table says what a driver cannot do, and the portable
API refuses rather than transforms unless you configure otherwise.

Almost everything on this page follows from that.

The measured comparison at equivalent semantics is in
the competitor benchmark, with its exclusions.

## What maps directly

| gocloud.dev/pubsub | iqmq | Note |
|---|---|---|
| `pubsub.OpenTopic(ctx, "nats://orders")` | `dsn.Open(ctx, "nats://host/orders")` or `natsdriver.Open(ctx, cfg)` | iqmq prefers the typed `Config` in production and `String()`/`LogValue()` redact credentials |
| URL openers, `blank import` per backend | `dsn.NewRegistry()` + `driver.Register(reg)`, or `dsn/global` for a CLI | iqmq's registry is an instance by default; the global facade is opt-in, so a library cannot mutate a program's driver set |
| `Topic.Send(ctx, m)` | `pub.Publish(ctx, m)` | |
| *(no batch API)* | `pub.PublishBatch(ctx, ms)` | gocloud's batcher merges *concurrent* sends; it has no caller-visible batch |
| `Subscription.Receive(ctx)` | `con.Receive(ctx, dst []mq.Delivery) (int, error)` | iqmq fills a caller-owned slice; one call can return many |
| `msg.Ack()` | `d.Ack() error` | gocloud's **panics** if called twice |
| `msg.Nack()`, `msg.Nackable()` | `d.Nack(opts...) error`, `client.Capabilities().Has(mq.FeatureNack)` | gocloud's `Nack` **panics** on a driver that cannot nack; iqmq returns `mq.ErrUnsupported` |
| `pubsub.Message.Body` | `mq.Message.Body` | |
| `pubsub.Message.Metadata` (`map[string]string`) | `mq.Message.Headers` | not the same thing — see below |
| `Topic.Shutdown(ctx)` / `Subscription.Shutdown(ctx)` | `client.Shutdown(ctx)` | one call for the client: publishers flush, then consumers drain, then the backend |
| `topic.As(&native)`, `msg.As(&native)` | `client.Native()`, `d.Native()`, `mq.NativeAs[T](d)` | same escape hatch, typed |
| `ErrorAs`, `gcerrors.Code(err)` | `errors.As`, `mq.CodeOf(err)` and the sentinels in `errors.go` (`mq.ErrUnsupported`, `mq.ErrLeaseLost`, `mq.ErrSettlementUnknown`, …) | iqmq's codes are the and include the settlement-specific ones gocloud has no room for |
| `Message.BeforeSend` / `AfterSend` | `mq.PublishMiddleware`, hooks | |

## Metadata: gocloud rewrites it, iqmq refuses it

This is the single biggest behavioural difference and it is easy to miss,
because gocloud's transformation is invisible and lossless *within* gocloud.

`gocloud.dev/pubsub/awssnssqs` hex-escapes every metadata **key** that is not
ASCII-alphanumeric and URL-escapes every **value** on the way out, and
un-escapes them on the way in. A key of `x-trace-id` does not arrive at SQS as
`x-trace-id`. A non-gocloud consumer reading the same queue sees the escaped
form; a gocloud consumer sees the original. The same pattern appears in other
gocloud drivers for the same reason: the backend cannot carry the value, so
gocloud encodes it.

iqmq does not do this. In the default `mq.MetadataNativeStrict` mode a header
the driver cannot carry natively is `mq.ErrUnsupported` at publish. Your
choices are explicit:

| Mode | What happens to a header the broker cannot carry |
|---|---|
| `mq.MetadataNativeStrict` (default) | the publication fails with `mq.ErrUnsupported` |
| `mq.MetadataPortableEnvelope` | an envelope is always used; portable binary and duplicate headers work, and the **body on the wire changes**, which is documented and breaks non-iqmq consumers |
| `mq.MetadataAdaptive` | native where possible, envelope where required (SQS over ten attributes, say) |
| `mq.MetadataLenientDrop` | dropped, with a hook and a metric; cannot satisfy a metadata requirement |

**What you lose:** gocloud's escaping means a gocloud-written message is
readable by gocloud and awkward for anything else, and a port to iqmq's strict
mode will *fail* on headers gocloud used to accept. That is the point, and it
is still work. If you want the old behaviour, `mq.MetadataPortableEnvelope` is
the mode that says "the body is not the payload any more" out loud, and it is
not compatible with gocloud's escaping either — an iqmq envelope and a gocloud
escape are different wire formats. **A mixed fleet of gocloud and iqmq readers
on one topic needs a decision before the port, not during it.**

## Panics become errors

Two gocloud behaviours are panics and their iqmq equivalents are not:

```go
// gocloud: panics if the driver is at-most-once.
if m.Nackable() { m.Nack() }
// gocloud: panics on the second call.
m.Ack()
```

```go
// iqmq: both are errors with portable codes.
if err := d.Nack(); errors.Is(err, mq.ErrUnsupported) { /* at-most-once transport */ }
if err := d.Ack();  errors.Is(err, mq.ErrAlreadySettled) { /* settled twice */ }
```

**What you gain:** a settlement bug is a log line instead of a crashed process,
and `mq.ErrSettlementUnknown` names the case gocloud has no way to report at
all — an ack whose result is ambiguous. **What you lose:** nothing, except that
code relying on the panic to find double-acks in tests needs the error checked
instead.

## Delivery guarantees are declared rather than documented

gocloud documents at-most-once and at-least-once per driver in prose and makes
`Nackable()` the only programmatic signal. iqmq's capability table is generated
from conformance runs (`client.Capabilities()`, `capabilities.json`), and every
driver scenario results are published. `mq.Requirement` lets a consumer
refuse to open at all:

```go
con, err := client.Consumer(ctx, mq.ConsumerConfig{
	Topic: "orders", Subscription: "billing",
	Requirements: []mq.Requirement{
		mq.RequireAtLeast(mq.FeatureExplicitAck, mq.Native),
		mq.RequireAtLeast(mq.FeatureOrderedByKey, mq.Native),
	},
})
// err is mq.ErrUnsupported here on a transport that cannot do both.
```

There is no gocloud equivalent; the nearest thing is reading the driver's
documentation and hoping.

## Receive: the shapes are different on purpose

`Subscription.Receive(ctx)` returns one message and hides its prefetch inside
a batcher whose parameters are the driver's. That is a genuine convenience and
it is also why the competitor benchmark **excludes every `Receive` row**: four
libraries have four prefetch models and comparing them compares the
configurations rather than the libraries.

iqmq's `Receive(ctx, dst)` fills a caller-owned `[]mq.Delivery` and returns how
many it filled, with `Prefetch` and `MaxInFlight` on the consumer config. The
port is mechanical:

```go
// gocloud
for {
	m, err := sub.Receive(ctx)
	if err != nil { return err }
	handle(m); m.Ack()
}
```

```go
// iqmq
dst := make([]mq.Delivery, 64)   // allocated once, reused
for {
	n, err := con.Receive(ctx, dst)
	for i := range n {
		handle(&dst[i])
		if err := dst[i].Ack(); err != nil { log(err) }
	}
	if err != nil { return err }
}
```

Note the shape of the error handling: `Receive` may return a non-nil error
*together* with deliveries, and those deliveries are valid and must be settled
before the error is returned. gocloud's one-message signature cannot express
that case and so never has to.

Or skip the loop entirely and use `con.Run(ctx, handler, opts...)`, which is
what most gocloud receive loops are reimplementing: concurrency, lane routing
by key, retry, dead-lettering, lease extension and a graceful drain.

## What does not map at all

* **gocloud's `blob`, `docstore`, `runtimevar`, `secrets` and `mysql`
  packages.** iqmq is messaging only. If your use of gocloud is really "the Go
  Cloud Development Kit", only the `pubsub` part has an iqmq counterpart.
* **URL-string configuration as the primary path.** iqmq parses DSNs (a pure,
  fuzzed function) but the typed `Config` is the production path, and unknown
  DSN parameters are an error rather than ignored.
* **`pubsub.Message.LoggableID`.** iqmq logs no body and no header by default;
  there is no redacted-id field because there is nothing logging it for you.
* **gocloud's implicit topic and subscription creation.** iqmq's auto-provision
  is off by default; `client.Admin()` creates topology explicitly, or
  returns `mq.ErrUnsupported` on a backend with no admin surface.
* **The `driver` package as an extension point with five methods.** iqmq's
  driver SPI (the specification) is larger, because a driver also declares its capabilities,
  its settlement semantics and its limits. `examples/custom-driver` is a
  complete one written from outside the repository.
* **gocloud's ordering-key support via `BeforeSend`.** iqmq's `Message.Key` is
  a first-class field and `mq.WithOrderingByKey()` is a runner option, but the
  *driver* must advertise `mq.FeatureOrderedByKey` — there is no per-send hook that
  makes an unordered transport ordered.

## Traps

**A gocloud `Topic` is one topic; an iqmq `Client` is a connection.** Porting
`OpenTopic` per topic to `client.Publisher` per topic is right; porting it to
`Open` per topic gives you a connection per topic.

**`Send` on gocloud's NATS driver does not flush.** `natspubsub`'s `SendBatch`
says so in its own source. If your port sets `Confirmation` to its default,
iqmq's `mq.ConfirmAccepted` adds a `PING`/`PONG` round trip per publication and
your throughput drops — not because iqmq is slower, but because you asked for
a stronger boundary. Set `mq.ConfirmNone` to keep the old semantics, and know
what you are keeping.

**gocloud's `Ack` is asynchronous and returns before the ack is sent.** iqmq's
returns when the settlement has been attempted and tells you the outcome. A
port that measures handler latency will see it move.

**`mq.ErrPartial` on a batch.** `PublishBatch` reports per-item failures with
index-aligned results. gocloud has no batch and therefore no partial-failure
case; code ported from a `for … Send` loop needs to decide what a partially
failed batch means for it.

## A worked port

Before:

```go
topic, err := pubsub.OpenTopic(ctx, "awssqs://"+queueURL)
defer topic.Shutdown(ctx)
sub, err := pubsub.OpenSubscription(ctx, "awssqs://"+queueURL)
defer sub.Shutdown(ctx)

if err := topic.Send(ctx, &pubsub.Message{
	Body:     body,
	Metadata: map[string]string{"x-trace-id": traceID},
}); err != nil { return err }

m, err := sub.Receive(ctx)
if err != nil { return err }
process(m.Body, m.Metadata["x-trace-id"])
m.Ack()
```

After:

```go
client, err := sqs.Open(ctx, sqs.Config{QueueURL: queueURL})
defer client.Shutdown(ctx)

pub, _ := client.Publisher(ctx, mq.PublisherConfig{Topic: queueURL})
con, _ := client.Consumer(ctx, mq.ConsumerConfig{Topic: queueURL, Subscription: "workers"})

m := &mq.Message{Body: body}
m.Headers.Set("x-trace-id", []byte(traceID))
if err := pub.Publish(ctx, m); err != nil { return err }

dst := make([]mq.Delivery, 1)
n, err := con.Receive(ctx, dst)
for i := range n {
	v, _ := dst[i].Headers.Get("x-trace-id") // Delivery embeds Message by value
	process(dst[i].Body, string(v))
	if aerr := dst[i].Ack(); aerr != nil { log(aerr) }
}
if err != nil { return err }
```

The header survives this port because `x-trace-id` is ASCII-alphanumeric plus
hyphens and SQS carries it natively. **A key gocloud used to escape will not:**
it becomes `mq.ErrUnsupported` in the default metadata mode, and the queue
you are reading may still contain gocloud-escaped keys from before the port.
Deciding what to do about those is the migration, and it is the part a
mechanical rewrite will not do for you.
