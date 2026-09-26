package iqmq_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory" // swap for kafka.Open / dsn.Open(ctx, "kafka://...")
)

// Example is the API's quick start, compiled and run.
//
// It exists because a quick start that does not compile is the first
// documentation defect anybody meets, and this one is checked by `go test`
// against the real memory driver on every run.
//
// It differs from the listing in three places, all of them the
// specification being older than the decisions that followed it:
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
	handled := make(chan struct{})
	go func() {
		_ = consumer.Run(ctx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			fmt.Printf("order %s: %s\n", d.Key, d.Body)
			close(handled)
			return nil // ack
		}), mq.WithConcurrency(8), mq.WithOrderingByKey())
	}()

	if err := client.Publish(ctx, "orders", &mq.Message{Key: []byte("cust-42"), Body: []byte(`{"id":1}`)}); err != nil {
		panic(err)
	}
	<-handled
	// Output: order cust-42: {"id":1}
}

// ExampleConsumerHandle_Run_deadLetter is the failure policy: five
// attempts with jittered exponential backoff, and a message that runs out of
// them goes to the dead-letter topic with its provenance rather than being
// dropped.
//
// The original is acked only after the dead-letter publication is confirmed
// (ADR-011), so a dead-letter destination that is refusing
// publications keeps the message alive instead of losing it.
func ExampleConsumerHandle_Run_deadLetter() {
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
	dead, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders.dlq", Subscription: "audit"})
	if err != nil {
		panic(err)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		_ = consumer.Run(runCtx, mq.HandlerFunc(func(_ context.Context, _ *mq.Delivery) error {
			return errUndecodableOrder
		}),
			mq.WithMaxAttempts(1),
			mq.WithRetryBackoff(mq.ExponentialBackoff{
				Base:   200 * time.Millisecond,
				Max:    30 * time.Second,
				Jitter: mq.FullJitter,
			}),
			mq.WithDeadLetter("orders.dlq"),
			mq.WithFailurePolicy(mq.FailureDeadLetter))
	}()

	if err := client.Publish(ctx, "orders", &mq.Message{ID: "o-1", Body: []byte("not json")}); err != nil {
		panic(err)
	}
	ms, err := memory.Drain(ctx, dead, 1)
	if err != nil {
		panic(err)
	}
	origin, _ := ms[0].Headers.Get(mq.HeaderOriginTopic)
	failure, _ := ms[0].Headers.Get(mq.HeaderFailure)
	fmt.Printf("%s from %s: %s\n", ms[0].ID, origin, failure)
	// Output: o-1 from orders: the order is not valid JSON
}

// errUndecodableOrder is the handler's own failure. The dead-letter route
// records it in the iqmq-failure header, which is how the operator reading
// the dead-letter topic learns why the message is there.
var errUndecodableOrder = errors.New("the order is not valid JSON")
