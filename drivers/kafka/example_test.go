package kafka_test

import (
	"context"
	"fmt"
	"log"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit/nack"
	kq "bitbucket.org/iqhive/iqmq/drivers/kafka"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// Open dials a cluster and publishes one message, waiting for the durable
// confirmation.
func Example() {
	ctx := context.Background()
	client, err := kq.Open(ctx, kq.Config{Brokers: []string{exampleBroker}})
	if err != nil {
		log.Print(err)
		return
	}
	defer func() { _ = client.Close() }()

	if err := client.Publish(ctx, exampleTopic, &mq.Message{
		Key:  []byte("order-42"),
		Body: []byte(`{"id":42}`),
	}); err != nil {
		log.Print(err)
		return
	}
	fmt.Println("published")
	// Output: published
}

// Consume reads one message and acknowledges it. The acknowledgement completes
// the offset; the commit that makes it durable happens on the commit ticker,
// at a revoke, and at Close.
func Example_consume() {
	ctx := context.Background()
	client, err := kq.Open(ctx, kq.Config{Brokers: []string{exampleBroker}})
	if err != nil {
		log.Print(err)
		return
	}
	defer func() { _ = client.Close() }()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic:        exampleTopic,
		Subscription: "example-group",
		Start:        mq.StartEarliest(),
	})
	if err != nil {
		log.Print(err)
		return
	}
	defer func() { _ = consumer.Close() }()

	dst := make([]mq.Delivery, 1)
	n, err := consumer.Receive(ctx, dst)
	if err != nil || n == 0 {
		log.Print(err)
		return
	}
	fmt.Printf("partition %d offset %d\n", dst[0].Info().Partition, dst[0].Info().Offset)
	if err := dst[0].Ack(); err != nil {
		log.Print(err)
	}
	// Output: partition 0 offset 0
}

// ParseDSN builds the same configuration from a connection string. The
// rendering redacts the credential, so a Config is safe to log.
func ExampleParseDSN() {
	cfg, err := kq.ParseDSN("kafka://svc:hunter2@kafka-1:9092,kafka-2:9092/acme.?sasl=scram-sha-512&nack=retrytopic&retries=3")
	if err != nil {
		log.Print(err)
		return
	}
	fmt.Println(cfg.Brokers)
	fmt.Println(cfg.TopicPrefix, cfg.SASL, cfg.Nack, cfg.NackConfig.Retries)
	fmt.Println(cfg.Password)
	// Output:
	// [kafka-1:9092 kafka-2:9092]
	// acme. scram-sha-512 retrytopic 3
	// redacted
}

// Register makes the driver reachable by its DSN scheme through a registry the
// caller owns (ADR-012): there is no global registry in the root module.
func ExampleRegister() {
	registry := dsn.NewRegistry()
	if err := kq.Register(registry); err != nil {
		log.Fatal(err)
	}
	fmt.Println(kq.Scheme)
	// Output: kafka
}

// The negative-settlement strategy is chosen explicitly, because two of the
// four change a partition's ordering and one of them halts the subscription
func Example_nackStrategy() {
	cfg := kq.Config{
		Brokers: []string{exampleBroker},
		// Retry topics keep the partition moving and reorder what they
		// move. The default is block-partition, which keeps the order
		// and stops the partition while a record is retried.
		Nack: nack.RetryTopic,
		NackConfig: nack.Config{
			Retries:         3,
			MaxAttempts:     5,
			DeadLetterTopic: "orders.dlq",
		},
	}
	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}
	fmt.Println(cfg.Nack, cfg.NackConfig.Retries)
	// Output: retrytopic 3
}
