package nsq_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nsq"
	"github.com/iqhive/iqmq/dsn"
)

// Example shows the shape of an NSQ client: a publisher, a consumer whose
// subscription is an nsqd channel, and the one thing a caller has to decide
// on this broker that they do not have to decide anywhere else.
func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := nq.Open(ctx, nq.Config{
		Addrs:        []string{"127.0.0.1:4150"},
		LookupdAddrs: []string{"http://127.0.0.1:4161"},
		// NSQ has no headers at all, so the promise is kept by the
		// portable envelope and by nothing else. MetadataAdaptive envelopes
		// the messages that carry metadata and leaves a bare body alone,
		// which is what a mixed deployment wants; without it, a message
		// with a header or a key is refused before any I/O.
		Metadata: mq.MetadataAdaptive,
	})
	if err != nil {
		fmt.Println("open:", err)
		return
	}
	defer func() { _ = client.Close() }()

	// The subscription name is the nsqd CHANNEL, so two processes running
	// this code compete for each message, and a second channel on the same
	// topic receives its own copy of every message.
	cons, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic:        "orders.created",
		Subscription: "billing",
	})
	if err != nil {
		fmt.Println("consumer:", err)
		return
	}
	defer func() { _ = cons.Close() }()

	m := &mq.Message{Body: []byte("order-1")}
	m.Headers.Add("trace", []byte{0x00, 0xff})
	if err := client.Publish(ctx, "orders.created", m); err != nil {
		fmt.Println("publish:", err)
		return
	}

	buf := make([]mq.Delivery, 1)
	n, err := cons.Receive(ctx, buf)
	if err != nil {
		fmt.Println("receive:", err)
		return
	}
	for i := range n {
		d := &buf[i]
		fmt.Printf("%s attempt=%d\n", d.Body, d.Info().Attempt)
		if err := d.Ack(); err != nil {
			fmt.Println("ack:", err)
		}
	}
}

// ExampleParseDSN shows the connection string and what it renders back as: the
// credential is redacted, so a Config is safe to log and safe to put in an
// error.
func ExampleParseDSN() {
	cfg, err := nq.ParseDSN("nsq://nsqd:4150?lookupd=http://lookupd:4161" +
		"&authsecret=a-shared-secret&metadata=adaptive")
	if err != nil {
		fmt.Println("parse:", err)
		return
	}
	fmt.Println(cfg.Addrs[0], cfg.LookupdAddrs[0], cfg.Metadata)
	fmt.Println(cfg.AuthSecret == dsn.Secret("a-shared-secret"))
	// Output:
	// nsqd:4150 http://lookupd:4161 adaptive
	// true
}

// ExampleRepresentable shows what MetadataNativeStrict refuses on this broker,
// which is the shortest list of any driver: NSQ carries a body and nothing
// else, so a header, an identifier, a typed property and a key all need the
// envelope.
func ExampleRepresentable() {
	m := &mq.Message{Body: []byte("b"), Key: []byte("k")}
	err := nq.Representable(m)
	fmt.Println(errors.Is(err, mq.ErrUnsupported))
	fmt.Println(err)
	// Output:
	// true
	// iqmq/nsq: NSQ has no message key; use MetadataPortableEnvelope or MetadataAdaptive
}
