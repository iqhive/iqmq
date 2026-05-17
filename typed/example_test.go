package typed_test

import (
	"context"
	"fmt"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/codec"
	"bitbucket.org/iqhive/iqmq/memory"
	"bitbucket.org/iqhive/iqmq/typed"
)

// Invoice is the example's message type.
type Invoice struct {
	ID    int    `json:"id"`
	Buyer string `json:"buyer"`
}

// Example is the typed-codec workflow: the generic layer over the
// bytes-first core, in the spelling the specification uses.
//
// The core stays bytes in, bytes out — nothing here is mandatory, no codec is
// imported by the root package, and a program that already holds bytes pays
// for none of it. What the typed layer adds is the compiler: a
// Publisher[Order] refuses an Invoice where a bytes publisher would have
// published it.
func Example() {
	ctx := context.Background()
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Shutdown(ctx) }()

	// The subscription has to exist before the publication: a subscription
	// starts at the end of the topic unless it asks otherwise, so a
	// message published before it existed is not one it sees.
	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		panic(err)
	}
	pub, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		panic(err)
	}
	orders := typed.NewPublisher(pub, codec.JSON[Invoice]())
	if err := orders.Publish(ctx, Invoice{ID: 1, Buyer: "ada"}); err != nil {
		panic(err)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	seen := make(chan Invoice, 1)
	go func() {
		_ = typed.Run(runCtx, consumer, codec.JSON[Invoice](), func(_ context.Context, d *typed.Delivery[Invoice]) error {
			seen <- d.Value
			return nil
		})
	}()

	got := <-seen
	fmt.Printf("invoice %d from %s (%s)\n", got.ID, got.Buyer, orders.ContentType())
	// Output: invoice 1 from ada (application/json)
}

// ExampleNewHandler is the decode-failure path, which is the one case this
// package exists to get right.
//
// The two obvious implementations are both wrong: a panic takes the worker
// with it, and a silent drop destroys a message the specification says must never be
// skipped by default. So a body that will not decode is an error the handler
// never sees, and what becomes of the delivery is a named policy —
// HandlerConfig.OnDecodeError, whose default is DeadLetter because a decode
// failure is not transient: the same bytes will not decode on the next attempt
// either, so retrying is a loop and discarding is data loss.
//
// Discard is chosen here so that the example can show the handler NOT being
// called without needing a dead-letter destination.
func ExampleNewHandler() {
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
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("not json")}); err != nil {
		panic(err)
	}

	called := make(chan struct{}, 1)
	h := typed.NewHandler(codec.JSON[Invoice](),
		typed.HandlerFunc[Invoice](func(context.Context, *typed.Delivery[Invoice]) error {
			called <- struct{}{}
			return nil
		}),
		typed.HandlerConfig{OnDecodeError: typed.Discard})

	dst := make([]mq.Delivery, 1)
	if _, err := consumer.Receive(ctx, dst); err != nil {
		panic(err)
	}
	// The policy's answer is a CONTROL error, not a failure: it is what the
	// runner reads to decide the delivery's fate. Discard here means "ack it
	// without success, log it, count it" (DirectiveDiscard) — which is
	// choosing to lose the message, and is never the default.
	err = h.Handle(ctx, &dst[0])
	fmt.Println("handler called:", len(called) == 1)
	directive, _ := mq.DirectiveOf(err)
	fmt.Println("directive:", directive)
	// Output:
	// handler called: false
	// directive: discard
}
