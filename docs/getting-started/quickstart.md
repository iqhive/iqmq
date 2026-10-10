# Quick start

No broker, no container, no Docker. The in-process engine is a real driver —
the same `Backend`, the same runner, the same settlement state machine as
Kafka — so this program exercises the actual code path and not a mock.

```go
package main

import (
    "context"
    "fmt"
    "log"
    "sync"

    mq "github.com/iqhive/iqmq"
    "github.com/iqhive/iqmq/memory" // swap for kafka.Open / dsn.Open(ctx, "kafka://…")
)

func main() {
    ctx := context.Background()

    client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
    if err != nil {
        log.Fatal(err)
    }
    defer func() { _ = client.Shutdown(ctx) }()

    consumer, err := client.Consumer(ctx, mq.ConsumerConfig{
        Topic:        "orders",
        Subscription: "billing",
    })
    if err != nil {
        log.Fatal(err)
    }

    runCtx, stop := context.WithCancel(ctx)
    defer stop()
    handled := make(chan struct{})
    var once sync.Once
    go func() {
        _ = consumer.Run(runCtx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
            fmt.Printf("order %s: %s\n", d.Key, d.Body)
            once.Do(func() { close(handled) })
            return nil // ack
        }), mq.WithConcurrency(8), mq.WithOrderingByKey())
    }()

    if err := client.Publish(ctx, "orders", &mq.Message{
        Key:  []byte("cust-42"),
        Body: []byte(`{"id":1}`),
    }); err != nil {
        log.Fatal(err)
    }
    <-handled
}
```

```
order cust-42: {"id":1}
```

That program is `examples/quickstart-memory`, in a module of its own outside
this repository's, so it is compiled and **run** by `make examples` in every
`make ci` against the published API and nothing else. The version here is the
same code with the example's test scaffolding removed.

## Five things in it worth noticing

**`d.Key` and `d.Body`, not `d.Message().Key`.** `mq.Delivery` embeds its
`Message` *by value*, so the
fields are reached directly and a received message is not a second allocation.

**`once.Do` is not defensive decoration.** The runner redelivers a message
whose handler failed, so a handler that closes a channel has to be written as
though it may be called again. That is the specification showing up in ten lines of
example code, and it is the single most common thing to get wrong.

**`return nil` is the ack.** Returning an error is the nack, and what happens
next is the runner's retry policy — see
[acks and leases](../concepts/acks-and-leases.md). Under the default
`mq.SettlementAuto` the handler may also settle for itself and is not
overruled.

**`mq.WithConcurrency(8)` with `mq.WithOrderingByKey()` does not reorder a
key.** Routing every delivery of one key to one lane, one lane at a time, is
the runner's default routing with or without the option; what the option adds
is `mq.OrderingStrict`, which decides what a *failure* does to the deliveries
behind it.

**`memory.Open` is the only line that names a driver.** Swapping it for
`dsn.Open(ctx, "kafka://…")` is the whole of what the specification promises about changing
brokers — with the caveat that the capabilities change too, which is what
[capability negotiation](../concepts/capabilities.md) is for.

## In a test, use `NewForTest`

```go
func TestBilling(t *testing.T) {
    client, broker := memory.NewForTest(t)   // AutoCreate on, fake clock, leak check on cleanup
    _ = broker.Advance(30 * time.Second)     // drive the scheduler instead of sleeping
    …
}
```

`memory.NewForTest` turns on `AutoCreate`, installs a fake clock, and registers
a goroutine-leak check on `t.Cleanup`. `Broker.Advance` moves that clock, so a
test of a 30-second backoff takes microseconds and is deterministic rather than
flaky. See testing and
synctest.

## Next

* [Installation](installation.md) — the module for your broker
* [Your driver page](../drivers/index.md) — what your broker actually does
* [Examples](../examples/index.md) — runnable programs, each its own module
