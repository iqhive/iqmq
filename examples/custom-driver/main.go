// Command custom-driver uses a driver this module wrote itself, through
// iqmq's portable API and nothing else.
//
// The driver is in ./toydriver and is the point of the module: a complete
// iqmq backend implemented from outside the repository, against the published
// SPI, held to the same conformance suite the memory driver is held to.
// Run `go test ./toydriver` to see that suite; what this program shows is the
// other half — that an APPLICATION written against iqmq does not know or care
// which driver is underneath.
//
// Nothing below names the toy broker except the one line that opens it. Every
// other line is the same code the quickstart-memory example runs against the
// memory driver, and that is the claim the specification makes about changing brokers.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/examples/custom-driver/toydriver"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		log.Fatal(err)
	}
}

// run publishes an order through the toy driver, fails it once, lets the
// redelivery succeed, and rejects a second one into the dead-letter topic.
//
// The three things it demonstrates are the three a driver author most wants to
// see working end to end through the portable API: fan-out to two
// subscriptions, a nack that comes back, and a Reject that lands somewhere an
// operator can read.
func run(ctx context.Context, out io.Writer) error {
	// The only driver-specific line. The specification forbids Open from creating
	// production topology, and this driver has no AutoCreate, so the
	// topology is declared here.
	client, err := toydriver.Open(toydriver.Config{
		Topics: []toydriver.TopicSpec{
			{Name: "orders.dlq", Subscriptions: []string{"audit"}},
			{
				Name:            "orders",
				Subscriptions:   []string{"billing", "analytics"},
				DeadLetterTopic: "orders.dlq",
				AckDeadline:     30 * time.Second,
			},
		},
	})
	if err != nil {
		return err
	}
	defer func() { _ = client.Shutdown(ctx) }()

	_, _ = fmt.Fprintf(out, "driver %s, at-least-once=%v, replay=%v\n",
		client.Info().Name,
		client.Capabilities().Has(mq.FeatureAtLeastOnce),
		client.Capabilities().Has(mq.FeatureReplay))

	billing, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		return err
	}
	analytics, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "analytics"})
	if err != nil {
		return err
	}
	audit, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders.dlq", Subscription: "audit"})
	if err != nil {
		return err
	}

	if err := client.Publish(ctx, "orders", &mq.Message{ID: "o-1", Body: []byte(`{"id":1}`)}); err != nil {
		return err
	}
	// Fan-out: one publication, two subscriptions, and settling one copy
	// does not settle the other.
	if err := one(ctx, billing, func(d *mq.Delivery) error {
		_, _ = fmt.Fprintf(out, "billing saw %s on attempt %d\n", d.ID, d.Info().Attempt)
		// A nack the driver honours: the record goes back to the front of
		// the queue and comes straight back.
		return d.Nack(mq.NackRequeue())
	}); err != nil {
		return err
	}
	if err := one(ctx, billing, func(d *mq.Delivery) error {
		_, _ = fmt.Fprintf(out, "billing saw %s again on attempt %d (redelivered=%v)\n",
			d.ID, d.Info().Attempt, d.Info().Redelivered)
		return d.Ack()
	}); err != nil {
		return err
	}
	if err := one(ctx, analytics, func(d *mq.Delivery) error {
		_, _ = fmt.Fprintf(out, "analytics saw %s on attempt %d\n", d.ID, d.Info().Attempt)
		return d.Ack()
	}); err != nil {
		return err
	}

	// A Reject: the driver moves the message to the topic's dead-letter
	// destination and then settles it, in that order, so a move that failed
	// would leave the message redeliverable rather than lost.
	if err := client.Publish(ctx, "orders", &mq.Message{ID: "o-2", Body: []byte("not an order")}); err != nil {
		return err
	}
	if err := one(ctx, billing, func(d *mq.Delivery) error { return d.Reject() }); err != nil {
		return err
	}
	if err := one(ctx, analytics, func(d *mq.Delivery) error { return d.Ack() }); err != nil {
		return err
	}
	if err := one(ctx, audit, func(d *mq.Delivery) error {
		_, _ = fmt.Fprintf(out, "audit found %s in the dead-letter topic\n", d.ID)
		return d.Ack()
	}); err != nil {
		return err
	}

	// And what the driver says it cannot do, it refuses. A caller reads the
	// capability table to find out; a caller that does not gets
	// ErrUnsupported instead of silence.
	_, err = client.Consumer(ctx, mq.ConsumerConfig{
		Topic: "orders", Subscription: "replay", Start: mq.StartEarliest(),
	})
	if !errors.Is(err, mq.ErrUnsupported) {
		return fmt.Errorf("a replaying consumer on a broker with no retention reported %w, want ErrUnsupported", err)
	}
	_, _ = fmt.Fprintln(out, "a replaying consumer is refused: this broker retains nothing")
	return nil
}

// one receives exactly one delivery and hands it to fn, bounded so that a
// message that never arrives is an error rather than a hang.
func one(ctx context.Context, c *mq.ConsumerHandle, fn func(*mq.Delivery) error) error {
	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := c.Receive(rctx, dst)
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("receive returned no delivery and no error")
	}
	return fn(&dst[0])
}
