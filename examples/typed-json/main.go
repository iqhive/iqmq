// Command typed-json publishes and consumes Go values through iqmq's generic
// layer over encoding/json/v2.
//
// Three packages meet here and each one is a deliberate choice a reader has to
// make for themselves:
//
//   - github.com/iqhive/iqmq is bytes in, bytes out. It imports no codec and
//     never serialises a value on its own, so a program that already holds
//     bytes pays for none of this.
//   - github.com/iqhive/iqmq/typed is the compiler's half: a Publisher[Order]
//     refuses an Invoice, and a handler is given an *Order rather than a body
//     it has to decode itself.
//   - github.com/iqhive/iqmq/codec/jsonv2 is a MODULE of its own, not a
//     package of the root, and that is what makes "the core is stdlib-only"
//     true while encoding/json/v2 is still arriving: it reports whether the
//     running toolchain has v2 (Go 1.27 and later) instead of failing to
//     compile on one that does not.
//
// What this example is really about is the decode-failure path, because it is
// the one place a typed layer can quietly lose a message. A body that will not
// decode never reaches the handler; what becomes of the delivery is a named
// policy, HandlerConfig.OnDecodeError, and its default is DeadLetter — a decode
// failure is not transient, so retrying is a loop and discarding is data loss.
// The second half of the program publishes a body that cannot be decoded and
// shows where it ends up.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/codec/jsonv2"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/typed"
)

// poisonBody is a body no Order can be decoded out of: a JSON object that
// stops after a member name. It is a package-level variable so that the test
// can prove it really does not decode — a "poison" message that quietly parses
// would make the dead-letter half of this example pass for the wrong reason.
var poisonBody = []byte(`{"id":`)

// Order is the message type. The struct tags are encoding/json's, which v2
// reads the same way; what v2 changes is the strictness — duplicate member
// names and invalid UTF-8 are rejected rather than repaired, and field names
// match case-sensitively.
type Order struct {
	ID    string `json:"id"`
	Items int    `json:"items"`
	Total int64  `json:"total_cents"`
}

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		log.Fatal(err)
	}
}

// run publishes one well-formed order and one body that is not an order, and
// prints what happened to each.
func run(ctx context.Context, out io.Writer) error {
	orders, err := jsonv2.Typed[Order]()
	if err != nil {
		// codec.ErrUnavailable, on a toolchain without encoding/json/v2.
		// The honest answer is to say so rather than to fall back to v1
		// behind the caller's back: the two differ in what they REJECT, and
		// a program that asked for v2 asked for the rejections.
		return fmt.Errorf("this example needs encoding/json/v2 (Go 1.27 or later): %w", err)
	}
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		return err
	}
	defer func() { _ = client.Shutdown(ctx) }()

	pub, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		return err
	}
	// The publisher is not closed here. The specification makes Client.Shutdown close
	// every handle the client opened, in the order the specification fixes — publishers
	// before consumers drain — and a publisher closed by hand first is a
	// publisher the runner's dead-letter route can no longer reach.

	// A typed publisher over a bytes publisher. It sets the body AND the
	// content type from the codec, which is what lets the receiving side
	// notice a body encoded by something else.
	typedPub := typed.NewPublisher[Order](pub, orders)

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "fulfilment"})
	if err != nil {
		return err
	}
	dlq, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders.dlq", Subscription: "audit"})
	if err != nil {
		return err
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	var once sync.Once
	handled := make(chan struct{})
	go func() {
		// NewHandler is ConsumerHandle.Run with the codec applied: the
		// handler is given a *typed.Delivery[Order] whose Value is already
		// decoded, and whose embedded *mq.Delivery is the untyped one
		// underneath for the headers, the attempt count and the settlement.
		//
		// typed.Run(ctx, consumer, orders, fn) is the shorter spelling of
		// the same thing and takes the DEFAULT HandlerConfig; this
		// example builds the handler by hand because the configuration is
		// what it is about.
		h := typed.NewHandler[Order](orders, typed.HandlerFunc[Order](
			func(_ context.Context, d *typed.Delivery[Order]) error {
				_, _ = fmt.Fprintf(out, "order %s: %d item(s), %d cents\n",
					d.Value.ID, d.Value.Items, d.Value.Total)
				once.Do(func() { close(handled) })
				return nil
			},
		), typed.HandlerConfig{
			// Both fields are named rather than left to the zero value, so
			// that a reader sees the decision instead of inheriting it.
			// OnDecodeError: nil would also select DeadLetter;
			// ContentType: the zero value is Ignore, which is NOT what a
			// service reading a topic other programs publish to wants.
			OnDecodeError: typed.DeadLetter,
			ContentType:   typed.ContentTypeRequire,
		})
		// The dead-letter destination is the RUNNER's, not the topology's.
		// Both routes exist and they are not the same thing: a topology with
		// a DeadLetterTopic makes Reject a native action the driver performs,
		// which for the memory driver republishes the message and nothing
		// else, while mq.WithDeadLetter is the specification's own route and stamps the
		// five provenance headers an operator reading the dead-letter topic
		// needs — iqmq-origin-topic and iqmq-failure among them. Configuring
		// the topology and expecting the headers is the mistake this comment
		// exists to stop.
		_ = consumer.Run(runCtx, h,
			mq.WithConcurrency(1),
			mq.WithDeadLetter("orders.dlq"),
			mq.WithFailurePolicy(mq.FailureDeadLetter))
	}()

	if err := typedPub.Publish(ctx, Order{ID: "o-1", Items: 3, Total: 4995}); err != nil {
		return err
	}
	select {
	case <-handled:
	case <-ctx.Done():
		return ctx.Err()
	}

	// The second half: a body that is not an Order at all. It is published
	// through the BYTES publisher, because a typed one could not produce it —
	// which is the whole argument for the typed layer, and also why the
	// decode path still has to be defined: the bytes arrive from somewhere
	// else in production.
	poison := &mq.Message{ID: "o-2", Body: poisonBody}
	poison.Properties.ContentType = codec.ContentTypeJSON
	if err := pub.Publish(ctx, poison); err != nil {
		return err
	}
	dead, err := drain(ctx, dlq, 1)
	if err != nil {
		return err
	}
	origin, _ := dead[0].Headers.Get(mq.HeaderOriginTopic)
	attempts, _ := dead[0].Headers.Get(mq.HeaderAttempts)
	reason, hasReason := dead[0].Headers.Get(mq.HeaderFailure)
	if !hasReason || len(reason) == 0 {
		return errors.New("the dead-lettered message carries no " + mq.HeaderFailure + " header")
	}
	// Neither the reason nor its length is printed: it is a standard-library
	// decoder's wording, and an example whose expected output depends on
	// another package's error string breaks the next time that package
	// rewords it. What is printed is what the specification promises to be there.
	_, _ = fmt.Fprintf(out, "dead-lettered %s from %s after %s attempt(s), with a recorded reason\n",
		dead[0].ID, origin, attempts)
	return nil
}

// drain takes n messages off a consumer and acknowledges them, bounded so that
// a message that never arrives is an error rather than a hang.
func drain(ctx context.Context, c *mq.ConsumerHandle, n int) ([]*mq.Message, error) {
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ms, err := memory.Drain(dctx, c, n)
	if err != nil {
		return nil, err
	}
	if len(ms) < n {
		return nil, errors.New("the dead-letter topic produced nothing")
	}
	return ms, nil
}
