package jetstream_test

import (
	"context"
	"fmt"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/dsn"
)

// Example shows the shape of a JetStream client: a stream, a publisher whose
// confirmation is a PubAck, a durable consumer, and the settlement this profile
// exists for.
func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client, err := nq.Open(ctx, nq.Config{
		Addrs: []string{sharedAddr},
		// The stream is the one piece of topology JetStream really has.
		// ManageStream is off by default — the specification forbids provisioning behind
		// the caller's back — so a deployment that wants the driver to own
		// the stream asks for it.
		Stream:        "EXAMPLE",
		SubjectPrefix: "example",
		ManageStream:  true,
	})
	if err != nil {
		fmt.Println("open:", err)
		return
	}
	defer func() { _ = client.Close() }()

	// The subscription name becomes a durable pull consumer, so two processes
	// running this code compete for each message rather than each receiving
	// it, and a restart resumes where the last one stopped.
	cons, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic:        "orders.created",
		Subscription: "billing",
	})
	if err != nil {
		fmt.Println("consumer:", err)
		return
	}
	defer func() { _ = cons.Close() }()

	// The default confirmation waits for the stream's PubAck, which the server
	// sends after the message is committed to storage. On this broker that is
	// also what ConfirmDurable means.
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
	fmt.Printf("received %s on attempt %d\n", dst[0].Body, dst[0].Info().Attempt)
	fmt.Println("ack:", dst[0].Ack())
	// Output:
	// received order-1 on attempt 1
	// ack: <nil>
}

// ExampleRegister shows the DSN route: the driver claims its own scheme on a
// registry the program owns, and "nats://" stays the Core driver's.
func ExampleRegister() {
	registry := dsn.NewRegistry()
	if err := nq.Register(registry); err != nil {
		fmt.Println("register:", err)
		return
	}
	fmt.Println(registry.Schemes())

	// A parameter this driver does not have is a construction error naming it,
	// not a setting that is quietly ignored.
	_, err := nq.ParseDSN("natsjs://broker:4222?strem=ORDERS")
	fmt.Println("typo refused:", err != nil)

	// And a value the struct reads as "not set" is refused rather than
	// silently defaulted.
	_, err = nq.ParseDSN("natsjs://broker:4222?replicas=0")
	fmt.Println("zero replicas refused:", err != nil)
	// Output:
	// [natsjs]
	// typo refused: true
	// zero replicas refused: true
}

// ExampleConfig_DSN shows what a configuration renders as: everything that
// decides behaviour, and no credential.
func ExampleConfig_DSN() {
	cfg := nq.Config{
		Addrs:           []string{"nats://broker:4222"},
		Username:        "svc",
		Password:        dsn.Secret("s3cret"),
		Stream:          "ORDERS",
		SubjectPrefix:   "acme",
		DeadLetterTopic: "orders/dlq",
		DoubleAck:       true,
	}
	rendered := cfg.DSN()
	fmt.Println("carries the password:", contains(rendered, "s3cret"))
	fmt.Println("marks the redaction:", contains(rendered, dsn.Redacted))
	fmt.Println("names the stream:", contains(rendered, "stream=ORDERS"))
	fmt.Println("names the settlement boundary:", contains(rendered, "doubleack=true"))
	// Output:
	// carries the password: false
	// marks the redaction: true
	// names the stream: true
	// names the settlement boundary: true
}

// contains is strings.Contains under a name that reads in an example's output.
func contains(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

// indexOf is strings.Index, written out so that the example above reads as one
// block rather than as a strings import.
func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
