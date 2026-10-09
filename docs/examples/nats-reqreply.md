# nats-reqreply

[`examples/nats-reqreply`](https://github.com/iqhive/iqmq/tree/main/examples/nats-reqreply)
— the request/reply over `drivers/nats`: one responder, a native requester
and an emulated one over the same broker, and the four answers a caller can
get back.

## The two paths are not equivalent

`reqreply` has a native path and an emulated one, and the difference changes
how a caller writes code:

| | native | emulated |
|---|---|---|
| topology | none — the requester subscribes to a private inbox | a reply topic and a subscription on it |
| correlation | the server's | a correlation id and a table in this process |
| nobody is listening | the specification's `NotFound`, **at once** | indistinguishable from slow: the caller's deadline |
| available on | a broker that has it (NATS's inbox, RabbitMQ's direct reply-to) | every broker, opt-in |

The example runs both against the same server and prints what each produces,
so the last row is a line of output rather than a claim. Emulation is opt-in
(`reqreply.WithEmulatedRequestReply()`) because it **creates topology**, and
The specification will not provision a reply topic nobody asked for.

## One responder serves both

`reqreply.Serve` answers `Properties.ReplyTo` and stamps the request's
correlation id on the reply. On the native path the reply subject is the
requester's private inbox and the id is ignored; on the emulated path it is
the shared reply topic and the id is the only thing that matches the answer to
its waiter. A responder written once is correct on both.

## The adapter a caller has to write today

`reqreply.New` takes the native path when `mq.Client.Native()` satisfies
`reqreply.NativeRequester`. The NATS driver **has** the method —
`Backend.Request` is `RequestNative`'s signature exactly — but `Native()`
answers the underlying `*nats.Conn`, so nothing in the repository satisfies
that interface and `reqreply.New` would return `ErrNotEnabled` rather than
provision a reply topic. The whole adapter is the rename:

```go
type nativeRequester struct{ b *nq.Backend }

func (n nativeRequester) RequestNative(ctx context.Context, topic string, m *mq.Message) (*mq.Message, error) {
	return n.b.Request(ctx, topic, m)
}

rr, err := reqreply.New(ctx, client, reqreply.Config{Timeout: 3 * time.Second},
	reqreply.WithNativeRequester(nativeRequester{b: backend}))
```

The specification. Until a driver exposes its own, `reqreply.NativeRequester`
has no real implementation in this repository and the `RequestReply`
capability claim has no behavioural probe in `testkit`.

## `Config.Timeout` is a fallback, not a cap

The specification makes `reqreply.Config.Timeout` the deadline a request gets when its
context has **none**. A program whose context always has one — a service
request, a test — therefore inherits the outer deadline, and
`Config.Timeout` never applies. The example gives every call its own
`context.WithTimeout`, and the difference is measurable: with the responder
answering nothing, the failure took sixty seconds per arm before and three
seconds after.

## NATS Core has no settlement

A delivery is complete the moment the handler has it, and `Ack` reports
`mq.ErrNotSettleable` rather than pretending otherwise — so a request whose
reply is lost is **not** redelivered. A request/reply service on NATS Core is
a call and not a workflow: the timeout is the only failure handling there
is. For a retryable request, use `drivers/jetstream` or a broker with leases.
