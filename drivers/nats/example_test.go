package nats_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nats"
	"github.com/iqhive/iqmq/dsn"
)

// Example shows the shape of a NATS Core client: a publisher, a consumer whose
// subscription is a queue group, and the settlement answer this broker gives.
func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := nq.Open(ctx, nq.Config{Addrs: []string{exampleAddr}})
	if err != nil {
		fmt.Println("open:", err)
		return
	}
	defer func() { _ = client.Close() }()

	// The subscription name is the queue group, so two processes running this
	// code compete for each message rather than each receiving it.
	cons, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic:        "orders.created",
		Subscription: "billing",
	})
	if err != nil {
		fmt.Println("consumer:", err)
		return
	}
	defer func() { _ = cons.Close() }()

	if err := client.Publish(ctx, "orders.created", &mq.Message{Body: []byte("order-1")}); err != nil {
		fmt.Println("publish:", err)
		return
	}

	dst := make([]mq.Delivery, 1)
	n, err := cons.Receive(ctx, dst)
	if n == 0 {
		fmt.Println("receive:", err)
		return
	}
	fmt.Printf("received %s\n", dst[0].Body)

	// NATS Core has no settlement: the delivery is complete the moment the
	// handler has it, and the core says so rather than pretending otherwise.
	fmt.Println("ack:", errors.Is(dst[0].Ack(), mq.ErrNotSettleable))
	// Output:
	// received order-1
	// ack: true
}

// ExampleRegister shows the DSN route: the driver claims its scheme on a
// registry the program owns, and there is no global registry to claim it on.
func ExampleRegister() {
	registry := dsn.NewRegistry()
	if err := nq.Register(registry); err != nil {
		fmt.Println("register:", err)
		return
	}
	fmt.Println(registry.Schemes())

	// A parameter this driver does not have is a construction error naming it,
	// not a setting that is quietly ignored.
	_, err := nq.ParseDSN("nats://broker:4222?pendingmesages=10")
	fmt.Println("typo refused:", err != nil)
	// Output:
	// [nats]
	// typo refused: true
}
