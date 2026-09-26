// Command quickstart-memory is the API's quick start as a program
// that runs, in a module of its own outside the repository's.
//
// The module boundary is the point of it. The repository's own
// github.com/iqhive/iqmq example_test.go compiles inside the root
// package, where every internal helper is in reach; this builds the same
// program from a separate module against the published API and nothing else,
// which is what a reader who has just run `go get` has.
//
// It differs from the listing in five places. The first three are the ones
// github.com/iqhive/iqmq's own Example names — the specification being older
// than the decisions that followed it — and are repeated here rather than
// silently dropped, because a reader of this file has not read that one:
//
//   - the specification writes m:= d.Message(). The specification (ADR-005, DelivValue) makes
//     Delivery embed its Message BY VALUE, so the fields are reached directly
//     and a received message is not a separate allocation. d.Key and d.Body
//     below are the embedded message's.
//   - passes mq.WithOrderingByKey() and so does this. With no argument it
//     selects StrictOrdering: a failed delivery blocks its lane until it
//     is settled or terminally routed. The ROUTING it implies — every delivery
//     of one key to one lane, one lane at a time — is the runner's default with
//     or without the option, so raising the concurrency to eight does not
//     reorder a key.
//   - logs and returns, and a program that returns before its consumer has
//     run prints nothing. This one waits for the handler.
//
// The last two are this file's own, and both exist so that the example can be
// checked rather than eyeballed:
//
//   - the work is in run(ctx, out) with an io.Writer instead of the log
//     package, so the test can read what the handler printed. A service would
//     use log/slog.
//   - Run is given a cancellable context of its own, so that the program exits
//     when the message has been handled instead of consuming for ever. The specification's
//     ctx is the process's, which is right for a service and wrong for a
//     program with an expected output.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory" // swap for kafka.Open / dsn.Open(ctx, "kafka://...")
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		log.Fatal(err)
	}
}

// run publishes one order and prints it from the handler that received it.
//
// Everything here is the portable API: memory.Open is the only line that names
// a driver, and swapping it for dsn.Open(ctx, "kafka://…") is the whole of what
// The specification promises about changing brokers.
func run(ctx context.Context, out io.Writer) error {
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		return err
	}
	defer func() { _ = client.Shutdown(ctx) }()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		return err
	}

	// Run consumes until its context is done. The handler closes handled, and
	// once.Do is not defensive decoration: the runner redelivers a message
	// whose handler failed, so a handler that closes a channel has to be
	// written as though it may be called again.
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	handled := make(chan struct{})
	var once sync.Once
	go func() {
		_ = consumer.Run(runCtx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			_, _ = fmt.Fprintf(out, "order %s: %s\n", d.Key, d.Body)
			once.Do(func() { close(handled) })
			return nil // ack
		}), mq.WithConcurrency(8), mq.WithOrderingByKey())
	}()

	if err := client.Publish(ctx, "orders", &mq.Message{Key: []byte("cust-42"), Body: []byte(`{"id":1}`)}); err != nil {
		return err
	}
	select {
	case <-handled:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
