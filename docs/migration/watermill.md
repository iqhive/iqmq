# From Watermill

Watermill is the closest thing to iqmq in the Go ecosystem, and the two make
opposite bets in one place: Watermill's `Publisher`/`Subscriber` pair is
deliberately tiny and every backend fits behind it, while iqmq's surface is
larger and every backend has to *declare* what it cannot do. Most of this page
is the consequence of that one difference.

The comparison of the two at equivalent semantics is in
the competitor benchmark, with its exclusions.

## What maps directly

| Watermill | iqmq | Note |
|---|---|---|
| `message.Publisher.Publish(topic, msgs...)` | `client.Publisher(ctx, mq.PublisherConfig{Topic: …})` then `pub.Publish(ctx, m)` / `pub.PublishBatch(ctx, ms)` | iqmq binds the topic at the publisher, not per call; `client.Publish(ctx, topic, m)` is the ad-hoc form |
| `message.Subscriber.Subscribe(ctx, topic)` returning `<-chan *Message` | `client.Consumer(ctx, mq.ConsumerConfig{Topic: …, Subscription: …})` then `con.Receive(ctx, dst)` or `con.Run(ctx, handler)` | a channel adapter is not in the primary API; see "What does not map" |
| `message.Message.Payload` | `mq.Message.Body` | both `[]byte` |
| `message.Message.UUID` | `mq.Message.ID` | Watermill's is for debugging and may be empty; iqmq's is an application identifier and is empty unless you set it or configure `AutoID` |
| `message.Message.Metadata` (`map[string]string`) | `mq.Message.Headers` | **not** the same thing — see below |
| `msg.Ack()` / `msg.Nack()` | `d.Ack()` / `d.Nack()` | Watermill's return `bool` (false when the other was already sent); iqmq's return `error` and tell you *why* |
| `message.Router` + `AddHandler` | `con.Run(ctx, handler, opts...)` | the runner is per consumer rather than a process-wide router |
| `message.HandlerMiddleware` | `mq.Middleware` (`iqmq/middleware`) | same shape: `func(Handler) Handler` |
| Watermill's `Retry` middleware | `mq.WithMaxAttempts`, `mq.WithRetryBackoff` | a runner option rather than a middleware, because the retry decision interacts with settlement |
| Watermill's `CorrelationID` middleware | `middleware.CorrelationID` | |
| Watermill's `Poison` middleware | `mq.WithDeadLetter(topic)` / `mq.WithNativeDeadLetter()` | iqmq refuses to ack a delivery whose dead-letter publish failed or was ambiguous |
| Watermill's `Recoverer` middleware | on by default; `mq.WithoutRecovery()` turns it off | |
| `gochannel.GoChannel` | `memory.Open` | both in-process; the memory engine has topology profiles, retention, delay and leases |

## Metadata is the first real difference

Watermill's `Metadata` is `map[string]string`. iqmq's `Headers` are **ordered,
duplicate-preserving, binary-capable and deterministically encoded**, and
a Watermill-shaped `map[string]string` goes in with a two-line loop over
`Headers.Set`, whose value is a `[]byte`.

Going the other way is where you lose something:

* **Order.** A `map[string]string` has none. If a consumer of yours depends on
  header order — a trace context stack, a signature over the header block —
  it cannot depend on it in Watermill and can in iqmq.
* **Duplicates.** Watermill cannot represent two values for one key. iqmq can,
  behind `mq.FeatureDuplicateHeaders`, and **rejects the publication at the
  client** on a driver that cannot carry them rather than silently keeping one.
* **Binary values.** Watermill's values are `string`, so a binary value has to
  be encoded by you. iqmq carries `[]byte` where the driver advertises
  `mq.FeatureBinaryHeaders`, and refuses or envelopes it (your choice of
  metadata mode) where it does not.
* **Limits.** iqmq enforces `MaxHeaderCount` and `MaxHeaderBytes` *before* the
  publication reaches the socket, and reports them in the capability table.
  Watermill's backends fail at the broker, later and less specifically.

**What you lose:** every Watermill subscriber in the wild reads
`msg.Metadata["key"]`. Nothing in iqmq gives you a `map[string]string` view for
free, and there is no `map[string]string` view to read back. A port of a large
Watermill codebase touches every metadata read.

## Acknowledgement is the second

Watermill's `Ack()` returns `bool`: `false` means "a nack was already sent".
Nothing tells you whether the backend *can* nack, whether your lease has
expired, or whether the ack reached the broker. iqmq's `Ack() error` answers
all three, with the portable codes:

```go
if err := d.Ack(); err != nil {
	switch {
	case errors.Is(err, mq.ErrAlreadySettled):   // a second terminal settlement
	case errors.Is(err, mq.ErrLeaseLost):        // the lease expired; somebody else has it
	case errors.Is(err, mq.ErrNotSettleable):    // this transport has no ack (NATS Core)
	case errors.Is(err, mq.ErrSettlementUnknown): // ambiguous: it may or may not have landed
	}
}
```

`mq.ErrNotSettleable` is the one that changes a port. On NATS Core, Watermill's
`msg.Ack()` returns `true` — it is a client-side signal that releases the
subscriber's slot — and iqmq's `d.Ack()` returns `mq.ErrNotSettleable`, because
NATS Core has no acknowledgement to send. Neither is wrong; iqmq is telling you
something Watermill does not, and code that ignores the error will look like it
broke.

## Capabilities: what you gain and what it costs you

Watermill has no capability model. Every `Publisher` satisfies the same
interface, so a feature a backend cannot provide is either silently absent or a
runtime surprise in the backend package. iqmq publishes a capability table per
driver and profile (`client.Capabilities()`), generated from conformance runs,
and the portable API **refuses** rather than emulates unless you ask:

```go
if !client.Capabilities().Has(mq.FeatureScheduledPublish) {
	// mq.Delay(d) would be mq.ErrUnsupported here.
}
```

**What you gain:** a publication that would have quietly lost its delay, its
ordering key or its duplicate header fails at the call site, in the strict
metadata mode that is the default.

**What you lose:** code that worked against three Watermill backends may now
need a branch, an explicit `mq.MetadataLenientDrop` or an emulation policy, for
each capability one of them lacks. iqmq makes you decide; Watermill let you not
notice. If what you want is "it works everywhere and I accept the loss",
`mq.MetadataLenientDrop` with the drop hook wired to a metric is the mode that
says so out loud.

## Confirmation boundaries

Watermill's `Publish` returns when the backend's publish call returns, and what
that means is the backend's business. iqmq makes it a configured value:

```go
pub, _ := client.Publisher(ctx, mq.PublisherConfig{
	Topic:        "orders",
	Confirmation: mq.ConfirmDurable, // None | Accepted | Durable
})
```

This is not cosmetic. On NATS Core, watermill-nats' `Publish` calls
`PublishMsg` and returns — the message is in the client's write buffer and
nothing has acknowledged it. `mq.ConfirmNone` is that same boundary named;
`mq.ConfirmAccepted` costs a `PING`/`PONG` round trip; `mq.ConfirmDurable` is
`mq.ErrUnsupported`, because NATS Core stores nothing. A port that leaves
`Confirmation` at its default gets `ConfirmAccepted`, which is **slower than
the Watermill code it replaced and stronger**, and the comparison in
the competitor benchmark is measured at
`ConfirmNone` for exactly that reason.

## What does not map at all

* **`message.Router`'s process-wide graph.** Watermill's router owns handlers
  across topics, with `AddNoPublisherHandler`, plugins and a single `Run`.
  iqmq's runner is per consumer: `con.Run(ctx, h, opts...)`. A router with
  twelve handlers becomes twelve consumers and your own supervision, or one
  `errgroup`. Nothing in iqmq replaces the router's plugin surface.
* **A `<-chan *Message` subscription.** iqmq's primary receive surfaces are
  `Receive(ctx, dst []Delivery) (int, error)`, `Messages(ctx)` (an
  `iter.Seq2`) and `Run`. A channel adapter is deliberately outside the
  primary API: a bounded channel per subscription is a goroutine and a buffer
  per subscription, and the specification forbids that on the receive path. If you want
  one, build it over `Receive` — it is ten lines and it is then *your* buffer
  policy.
* **`msg.Acked()` / `msg.Nacked()` channels.** iqmq settlement is synchronous
  and returns an error. There is no channel to select on.
* **`msg.Context()` and `msg.SetContext()`.** A Watermill message carries a
  context. An iqmq `Message` is data; the context is the handler's argument.
* **Watermill's `SQL`, `io`, `firestore` and other backends.** iqmq has no
  equivalent for several of them. Check the driver matrix before planning a
  port.
* **Watermill's `Subscriber` as a plugin point for your own backend.** iqmq's
  extension point is the driver SPI, which is a larger interface because
  it also has to declare capabilities and settlement semantics. Writing one is
  `examples/custom-driver`; it is not two methods.

## Traps

**Watermill's `Ack()` is idempotent-ish and iqmq's is not.** A second `Ack()`
in Watermill returns `true` and does nothing. In iqmq it is
`mq.ErrAlreadySettled`. Handler code that acks defensively at two exit points
will start reporting errors.

**A Watermill handler that returns an error nacks; an iqmq handler that returns
an error nacks *and counts an attempt*.** iqmq's runner owns the retry policy
(`WithMaxAttempts`, `WithRetryBackoff`, `WithDeadLetter`), so a handler that
retried internally and then returned an error will now be retried twice over.
Port the retry out of the handler.

**`mq.WithSettlement(mq.SettlementManual)` is what a Watermill handler that
acks by hand actually wants.** The default is `Auto`: nil error acks, non-nil
nacks. If your handler calls `Ack()` itself and returns nil, the runner's ack
is the second one.

**Watermill's gochannel `Publish` can block for ever on a full buffer, and so
can iqmq's memory engine.** Both are correct; the configuration is in different
places (`OutputChannelBuffer` against `memory.Config`'s capacity and overflow
policy, which also offers `reject`, `dropnew` and `dropold`).

**The `iqmq-` header prefix is reserved.** A Watermill codebase that puts its
own keys in metadata is fine, unless one of them starts with `iqmq-`, which is
rejected at publish.

## A worked port

Before:

```go
pub, _ := nats.NewPublisher(nats.PublisherConfig{URL: url, Marshaler: &nats.NATSMarshaler{}}, logger)
sub, _ := nats.NewSubscriber(nats.SubscriberConfig{URL: url, Unmarshaler: &nats.NATSMarshaler{}}, logger)

router.AddHandler("orders", "orders", sub, "billed", pub,
	func(m *message.Message) ([]*message.Message, error) {
		defer m.Ack()
		return []*message.Message{message.NewMessage(watermill.NewUUID(), out)}, nil
	})
```

After:

```go
client, _ := natsdriver.Open(ctx, natsdriver.Config{Addrs: []string{url}})
defer client.Shutdown(ctx)

billed, _ := client.Publisher(ctx, mq.PublisherConfig{Topic: "billed"})
orders, _ := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})

err := orders.Run(ctx, mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
	// No Ack(): the runner acks on a nil return (mq.WithSettlement(mq.SettlementAuto)).
	return billed.Publish(ctx, &mq.Message{Body: out})
}), mq.WithConcurrency(8), mq.WithMaxAttempts(3))
```

Three things changed that are worth naming. The `defer m.Ack()` is gone,
because acking before the downstream publish is what turns a broker outage
into lost messages and the runner's automatic settlement waits for the handler
to return. The retry is a runner option instead of a middleware. And on NATS
Core the `Run` above will report `mq.ErrNotSettleable` from its settlement step
unless the driver is one that acks — which is the capability model telling you
that the `defer m.Ack()` you just deleted never did anything either.
