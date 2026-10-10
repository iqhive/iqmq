# nats-reqreply — request/reply over NATS, both paths

A responder and a requester over [`drivers/nats`](../../drivers/nats), showing
the four answers a request can get and the one difference between iqmq's two
request/reply paths that changes how a caller writes code.

    go run .            # against a NATS server on 127.0.0.1:4222
    go test ./...       # runs the same program against an in-process server

## What it shows

`reqreply` (the API) has two paths:

- **native** — the broker has request/reply of its own. NATS's is an *inbox*:
  the requester subscribes to a private subject, the request carries it as the
  reply subject, and the server correlates. No reply topic is created, no reply
  consumer is opened, and the server will tell the requester when **nobody is
  subscribed**.
- **emulated** — a reply topic, a subscription on it, and a correlation id in
  the message. It works on every broker, it costs a subscription, and it cannot
  tell a responder that is slow from a responder that does not exist.

The program runs both against the same server and prints what each one
produces:

```
native requester: native=true, reply topology=none
native reply: SKU-1 costs 1299 cents
native timeout: a subscribed responder that does not answer -> the caller's deadline (iqmq code DeadlineExceeded)
native cancelled: a request the caller gave up on -> context canceled
native nobody: no subscriber on rpc.nobody -> iqmq code NotFound, before the deadline
emulated requester: native=false, reply topology=rpc.replies
emulated reply: SKU-2 costs 1499 cents
emulated nobody: no subscriber on rpc.nobody -> the caller's deadline (iqmq code DeadlineExceeded), indistinguishable from slow
emulated pending after the timeout: 0 correlation slots
```

Line by line:

| Line | What it is |
|---|---|
| `native=true, reply topology=none` | the native path opened no reply topic and no reply consumer |
| `native reply` | the ordinary case |
| `native timeout` | a responder that is up, subscribed and deliberately does not answer *this* request. Only the caller's deadline ends the wait |
| `native cancelled` | the caller gave up. The request had already reached the responder, so this is a cancelled request and not a publication that never left |
| `native nobody` | **the line the example exists for.** The server volunteers "no responders", so the caller gets the specification's `NotFound` at once instead of waiting five seconds |
| `emulated native=false` | the same portable `Request` call over two ordinary topics |
| `emulated nobody` | the same unsubscribed topic, and all the emulated path can say is "my deadline passed" |
| `emulated pending … 0` | the one failure the emulated design can have. A request whose reply never arrives releases its correlation slot before it returns; a client that leaked one per timeout would grow without bound on exactly the workload that makes timeouts common |

## The adapter you have to write today

`reqreply.New` takes the native path when `mq.Client.Native()` satisfies
`reqreply.NativeRequester`. This driver **has** the method — `Backend.Request`
is `RequestNative`'s signature exactly — but `Native()` answers the underlying
`*nats.Conn`, so nothing in the repository satisfies that interface. The whole
adapter is the rename:

```go
type nativeRequester struct{ b *nq.Backend }

func (n nativeRequester) RequestNative(ctx context.Context, topic string, m *mq.Message) (*mq.Message, error) {
	return n.b.Request(ctx, topic, m)
}

rr, err := reqreply.New(ctx, client, reqreply.Config{Timeout: 5 * time.Second},
	reqreply.WithNativeRequester(nativeRequester{b: backend}))
```

Without it, `reqreply.New` returns `reqreply.ErrNotEnabled` rather than
creating a reply topic nobody asked for, and the caller has to opt in to
emulation with `WithEmulatedRequestReply()`. Making the driver expose its own
is recorded in this repository.

## Settlement

NATS Core has no settlement. A delivery is complete the moment the handler has
it, and `Ack` reports `mq.ErrNotSettleable` rather than pretending otherwise —
so a request whose reply is lost is **not** redelivered. That is why a
request/reply service on NATS Core is a call and not a workflow: the specification's
timeout is the only failure handling there is. For a retryable request, use
JetStream (`drivers/jetstream`) or a broker with leases.

## Running it against a broker

`go test ./...` always runs the example against an **in-process**
`nats-server`, which is the real server compiled into the test binary. To run
it against a container as well, start one and name it — the variable is
`conformance/harness`'s own convention, `IQMQ_<BROKER>_ADDR`:

```sh
docker run -d --name nats-example -p 4222:4222 \
  nats@sha256:ad7a43eb7e3337c3c38ce5d784d1461791f95f730f252d2b25eee699752a0ca3
IQMQ_NATS_ADDR=127.0.0.1:4222 go test -count=1 -v -run TestRunAgainstABroker ./...
docker rm -f nats-example
```

(The digest is the one pinned in `conformance/harness/images.lock`.) With
`IQMQ_NATS_ADDR` unset that test **skips** and says so; with
`IQMQ_REQUIRE_BROKERS=1` an unset variable is a **failure** instead, which is
what a run that meant to test against a broker sets. `make examples` prints
which examples reached a broker and which skipped, by name, so a run in which
nothing did says so rather than reading as green.

## Files

| File | What is in it |
|---|---|
| `main.go` | the program: `open`, the responder, `nativePath`, `emulatedPath` |
| `main_test.go` | both arms, and the expected output compared byte for byte |
| `report_test.go` | the `IQMQ_EXAMPLE_REPORT` contract `make examples` reads |
