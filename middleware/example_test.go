package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/middleware"
)

// Example is the middleware workflow: the set, composed once with
// mq.Chain and given to the runner.
//
// Chain applies outermost-first — the first middleware sees the delivery
// before the ones inside it and their result after them — and the chain is
// built when Chain is called rather than per message (rule 6). So
// Recover outermost is what turns a panic in anything inside it into an error
// the runner can route, and Timeout inside it bounds the handler and not the
// recovery.
//
// Every one of these is off by default. The set is opt-in because a
// middleware a caller did not ask for is work on the message path they cannot
// see.
func Example() {
	ctx := context.Background()
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Shutdown(ctx) }()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		panic(err)
	}

	panics := make(chan string, 1)
	chain := mq.Chain(
		middleware.Recover(func(_ context.Context, _ *mq.Delivery, err *mq.PanicError) {
			panics <- err.Error()
		}),
		middleware.Timeout(time.Second),
	)

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		_ = consumer.Run(runCtx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			panic("the handler exploded")
		}), mq.WithMiddleware(chain), mq.WithMaxAttempts(1))
	}()

	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("boom")}); err != nil {
		panic(err)
	}
	fmt.Println("recovered:", <-panics != "")
	// Output: recovered: true
}

// ExampleFilter is the other shape ships, and the reason the package has
// two: a middleware that decides a delivery is not for this consumer at all.
//
// Filter acks what the predicate rejects, because a delivery nobody will
// process must still be settled — an unsettled delivery comes back. FilterWith
// is the same thing with a callback, for a consumer that wants to count what
// it dropped.
func ExampleFilter() {
	keep := middleware.Filter(func(d *mq.Delivery) bool {
		region, _ := d.Headers.Get("x-region")
		return string(region) == "eu"
	})

	handled := 0
	h := keep(mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		handled++
		return nil
	}))

	for _, region := range []string{"eu", "us"} {
		var d mq.Delivery
		d.Headers.Set("x-region", []byte(region))
		if err := h.Handle(context.Background(), &d); err != nil && !errors.Is(err, mq.ErrNotSettleable) {
			panic(err)
		}
	}
	fmt.Println("handled:", handled)
	// Output: handled: 1
}

// ExampleID is the publish side, which exists because stamping an
// identifier or a timestamp is something done to a message on its way OUT and
// there is no delivery to wrap.
//
// ID writes into the caller's message, so a publisher that stamps ids gives
// the caller back the id it used — which is what makes an outbox or an
// idempotency store able to reference it.
func ExampleID() {
	n := 0
	stamp := middleware.ID(func() string {
		n++
		return fmt.Sprintf("msg-%d", n)
	})

	// A PublishFunc takes a BATCH, because the publish half is one call
	// that publishes a slice: a single-message publish is a batch of one, so
	// one chain covers both shapes.
	pub := stamp(func(_ context.Context, _ string, ms []*mq.Message) error {
		for _, m := range ms {
			fmt.Println("publishing", m.ID)
		}
		return nil
	})

	m := &mq.Message{Body: []byte("one")}
	if err := pub(context.Background(), "orders", []*mq.Message{m}); err != nil {
		panic(err)
	}
	fmt.Println("caller sees:", m.ID)
	// Output:
	// publishing msg-1
	// caller sees: msg-1
}
