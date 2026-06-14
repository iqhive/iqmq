package redis_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	rd "bitbucket.org/iqhive/iqmq/drivers/redis"
	"bitbucket.org/iqhive/iqmq/internal/streams/fakestreams"
)

// Example shows the whole round trip: a publisher, a consumer, a settlement.
//
// It runs against the in-memory server so that the example is a real,
// executable program rather than a snippet — `go test` runs it and checks its
// output. Against a real Redis the only line that differs is the open:
//
//	client, err := redis.Open(ctx, redis.Config{Addrs: []string{"127.0.0.1:6379"}})
func Example() {
	ctx := context.Background()
	srv := fakestreams.New(fakestreams.Options{AOF: true})
	client, err := rd.OpenWithCommander(ctx, srv, driverkit.ClientOwned, rd.Config{
		AutoCreate:  true,
		AckDeadline: 30 * time.Second,
		Block:       50 * time.Millisecond,
	})
	if err != nil {
		fmt.Println("open:", err)
		return
	}
	defer func() { _ = client.Close() }()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic:        "acme.orders",
		Subscription: "workers",
		Start:        mq.StartEarliest(),
	})
	if err != nil {
		fmt.Println("consumer:", err)
		return
	}
	defer func() { _ = consumer.Close() }()

	publisher, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "acme.orders"})
	if err != nil {
		fmt.Println("publisher:", err)
		return
	}
	defer func() { _ = publisher.Close() }()

	if err := publisher.Publish(ctx, &mq.Message{
		Key:  []byte("order-4711"),
		Body: []byte("shipped"),
	}); err != nil {
		fmt.Println("publish:", err)
		return
	}

	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	batch := make([]mq.Delivery, 1)
	n, err := consumer.Receive(rctx, batch)
	if err != nil {
		fmt.Println("receive:", err)
		return
	}
	for i := range n {
		d := &batch[i]
		fmt.Printf("%s attempt=%d\n", d.Body, d.Info().Attempt)
		if err := d.Ack(); err != nil {
			fmt.Println("ack:", err)
		}
	}
	// Output:
	// shipped attempt=1
}

// Example_nackStrategies shows what the arms cost a caller, which is
// the one configuration decision this driver asks for.
//
// Redis Streams has no nack, so "make this message deliverable again" is
// built out of the commands there are — and the four ways to build it differ
// in what they preserve. The capability table says which the bound strategy
// is and what it can do, so a program can ask rather than assume.
func Example_nackStrategies() {
	ctx := context.Background()
	for _, kind := range []string{"leavepending", "zsetmover", "readd", "stop"} {
		cfg, err := rd.ParseDSN("redis://127.0.0.1:6379?nack=" + kind + "&autocreate=true")
		if err != nil {
			fmt.Println("dsn:", err)
			return
		}
		srv := fakestreams.New(fakestreams.Options{AOF: true})
		cfg.Addrs = nil
		b, err := rd.BackendWithCommander(ctx, srv, driverkit.ClientOwned, cfg)
		if err != nil {
			fmt.Println("open:", err)
			return
		}
		sup := b.Capabilities().Support(mq.FeatureDelayedRedelivery)
		fmt.Printf("%-12s delayed redelivery: %v\n", kind, sup.Level)
		_ = b.Close()
	}
	// Output:
	// leavepending delayed redelivery: conditional
	// zsetmover    delayed redelivery: emulateddurable
	// readd        delayed redelivery: conditional
	// stop         delayed redelivery: conditional
}

// Example_confirmDurable shows the durable boundary and its condition.
//
// XADD's reply is the memory-durable confirmation; WAITAOF is the conditional
// durable one, and "conditional" means the condition has to be stated. A
// publisher asking for ConfirmDurable on a server with no append-only file is
// refused at CONSTRUCTION, which is the difference between an application
// that fails to start and one that has already told a user their data is safe.
func Example_confirmDurable() {
	ctx := context.Background()
	for _, aof := range []bool{true, false} {
		srv := fakestreams.New(fakestreams.Options{Version: "7.4.2", AOF: aof})
		b, err := rd.BackendWithCommander(ctx, srv, driverkit.ClientOwned, rd.Config{AutoCreate: true})
		if err != nil {
			fmt.Println("open:", err)
			return
		}
		_, err = b.OpenSink(ctx, mq.SinkConfig{Topic: "acme.orders", Confirmation: mq.ConfirmDurable})
		switch {
		case err == nil:
			fmt.Printf("appendonly=%v: ConfirmDurable accepted\n", aof)
		case errors.Is(err, mq.ErrUnsupported):
			fmt.Printf("appendonly=%v: ConfirmDurable refused at construction\n", aof)
		default:
			fmt.Println("unexpected:", err)
		}
		_ = b.Close()
	}
	// Output:
	// appendonly=true: ConfirmDurable accepted
	// appendonly=false: ConfirmDurable refused at construction
}
