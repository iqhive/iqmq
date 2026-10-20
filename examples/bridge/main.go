// Command bridge moves messages from one broker to another and shows exactly
// what the seam between them does and does not guarantee.
//
// # Why a bridge needs its own example
//
// Moving messages between two brokers looks like a loop and is not. The two
// brokers know nothing about each other, so there is no transaction that can
// span them, and the order of the three steps decides what happens when the
// run dies in the middle:
//
//	receive -> publish -> confirm -> settle the source     at-least-once
//	receive -> settle the source -> publish                at-most-once, LOSSY
//
// The first order is the one this program uses and the one `iqmq bridge`
// implements. It means the destination can receive a message twice: if the
// process stops after the destination has confirmed and before the source has
// been settled, the source redelivers a message the destination already has.
// The third step's failure is not a rare case to be waved at -- it is a
// visible, reproducible outcome, so this program PRODUCES it rather than
// describing it: the last transfer deliberately does not settle its source
// delivery, and the output shows the duplicate arriving.
//
// The second order is not offered. A duplicate is something a destination can
// be built to tolerate -- de-duplicate by a key, make the handler idempotent
// -- and a lost message is not.
//
// # Why these two brokers
//
// The source is JetStream, because a bridge's source needs real settlement:
// the whole story is about what happens between a confirmation and an
// acknowledgement, and a broker with no acknowledgement (NATS Core, whose
// Ack answers mq.ErrNotSettleable) has no story to tell. The destination is
// the in-process memory engine, because the point is the SEAM and not the
// destination's own behaviour.
//
// The stream is created by this program with [nq.Config.ManageStream], and
// the destination's topic and subscription with the portable Admin surface of
// the API. Neither is created behind your back: the specification forbids Open
// from provisioning production topology, which is why a bridge that assumed
// its destination topic existed would fail on the first publication.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/memory"
)

// The source's JetStream names. The subject prefix is what the managed stream
// captures, and the topic below lives under it.
const (
	streamName   = "BRIDGEEX"
	subjectRoot  = "bridgeex"
	sourceTopic  = subjectRoot + ".orders"
	sourceSub    = "bridge"
	destTopic    = "mirror"
	destSub      = "reader"
	messageCount = 3
)

// sourceAckWait is how long JetStream waits for an acknowledgement before it
// redelivers.
//
// It is short because this program WANTS the redelivery: the unsettled
// delivery at the end of the run is the duplicate the seam is documented to
// allow, and a thirty-second default would make the example take thirty
// seconds to show it. Nothing sleeps for it -- the redelivery is waited for
// with a Receive, which blocks until the server sends it.
const sourceAckWait = time.Second

// settlementStatement is the sentence a bridge owes its reader.
const settlementStatement = "at-least-once across the seam -- publish, confirm, " +
	"then settle the source"

func main() {
	if err := demo(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// demo is main's body, separated so that every failure returns rather than
// calling os.Exit: an Exit after a `defer cancel()` does not run the defer,
// and here it would also skip the two clients' shutdown.
func demo() error {
	addr := os.Getenv("IQMQ_NATS_ADDR")
	if addr == "" {
		return errors.New("IQMQ_NATS_ADDR is unset: start a NATS server with JetStream " +
			"enabled (docker run -p 4222:4222 nats -js) and set it to host:port. " +
			"The tests run this program against an embedded server and need no container")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	src, dst, closeAll, err := open(ctx, addr)
	if err != nil {
		return fmt.Errorf("opening the two brokers: %w", err)
	}
	defer closeAll(ctx)
	if err := run(ctx, os.Stdout, src, dst); err != nil {
		return fmt.Errorf("running the bridge: %w", err)
	}
	return nil
}

// open connects to both ends and provisions the topology each needs.
//
// The destination is created here rather than lazily because provisioning is
// the step a bridge cannot skip: the specification keeps it out of Open, so somebody has to
// do it explicitly, and `iqmq topology apply` is the command that does this
// for a deployment.
func open(ctx context.Context, addr string) (
	src, dst *mq.Client, closeAll func(context.Context), err error,
) {
	src, err = nq.Open(ctx, nq.Config{
		Addrs:         []string{normalise(addr)},
		Stream:        streamName,
		SubjectPrefix: subjectRoot,
		// The stream is this program's to create. Without it, Open binds to
		// a stream that has to exist already and fails with a 404 -- which
		// is the specification working correctly and the reason `iqmq topology apply`
		// exists.
		ManageStream: true,
		Storage:      nq.StorageMemory,
		AckWait:      sourceAckWait,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("opening the JetStream source: %w", err)
	}

	broker, err := memory.NewBroker(memory.Config{})
	if err != nil {
		_ = src.Shutdown(ctx)
		return nil, nil, nil, fmt.Errorf("building the memory destination: %w", err)
	}
	dst, err = memory.OpenWithClient(ctx, broker, memory.ClientOwned)
	if err != nil {
		_ = src.Shutdown(ctx)
		// The broker has no client to close it, so it is closed directly.
		// The specification gives Close no context on purpose: a prompt close is not an
		// operation a caller cancels.
		_ = broker.Close() //nolint:contextcheck // Close takes no context, and this broker has nothing admitted to drain
		return nil, nil, nil, fmt.Errorf("opening the memory destination: %w", err)
	}
	if err := provision(ctx, dst); err != nil {
		_ = src.Shutdown(ctx)
		_ = dst.Shutdown(ctx)
		return nil, nil, nil, err
	}
	// the graceful close, in the order the seam needs: the DESTINATION
	// first, so anything the bridge admitted to it is flushed before the
	// source goes away, and then the source.
	return src, dst, func(ctx context.Context) {
		_ = dst.Shutdown(ctx)
		_ = src.Shutdown(ctx)
	}, nil
}

// provision creates the destination's topic and subscription over the specification's
// portable Admin surface.
//
// The subscription is created BEFORE anything is published, and that ordering
// is not decoration: a queue or topic retains nothing for a subscription that
// does not exist yet, so a bridge whose destination subscription is created
// afterwards delivers into a void and reports success.
func provision(ctx context.Context, dst *mq.Client) error {
	admin, err := dst.Admin()
	if err != nil {
		return fmt.Errorf("the destination has no Admin surface: %w", err)
	}
	if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: destTopic}); err != nil {
		return fmt.Errorf("creating the destination topic: %w", err)
	}
	if err := admin.EnsureSubscription(ctx,
		mq.SubscriptionSpec{Topic: destTopic, Name: destSub}); err != nil {
		return fmt.Errorf("creating the destination subscription: %w", err)
	}
	return nil
}

// normalise accepts both spellings of the address the harness convention uses:
// a bare host:port, and a nats:// URL a reader may already have.
func normalise(addr string) string {
	if strings.Contains(addr, "://") {
		return addr
	}
	return "nats://" + addr
}

// run fills the source, bridges it, and then produces the duplicate.
func run(ctx context.Context, out io.Writer, src, dst *mq.Client) error {
	p := func(format string, args ...any) {
		_, _ = fmt.Fprintf(out, format, args...)
	}
	p("settlement: %s\n", settlementStatement)

	if err := fill(ctx, src); err != nil {
		return err
	}
	con, err := src.Consumer(ctx, mq.ConsumerConfig{
		Topic: sourceTopic, Subscription: sourceSub, Start: mq.StartEarliest(),
		// A bridge must be able to give a delivery BACK: every failure path
		// nacks, and a source that cannot nack turns a refused publication
		// into a lost message.
		Requirements: []mq.Requirement{mq.RequireAtLeast(mq.FeatureNack, mq.Conditional)},
	})
	if err != nil {
		return fmt.Errorf("opening the source consumer: %w", err)
	}
	defer func() { _ = con.Shutdown(ctx) }()

	pub, err := dst.Publisher(ctx, mq.PublisherConfig{
		Topic: destTopic, Confirmation: mq.ConfirmAccepted,
	})
	if err != nil {
		return fmt.Errorf("opening the destination publisher: %w", err)
	}
	defer func() { _ = pub.Flush(ctx) }()

	moved, err := transfer(ctx, con, pub, messageCount, true)
	if err != nil {
		return err
	}
	p("moved %d of %d\n", moved, messageCount)
	bodies, err := held(ctx, dst, messageCount)
	if err != nil {
		return err
	}
	p("destination holds: %s\n", strings.Join(bodies, " "))

	// The interruption. One more message is published to the source,
	// transferred with the settlement DELIBERATELY skipped, and then waited
	// for: the source's ack deadline expires and it redelivers a message the
	// destination already confirmed.
	if err := publishOne(ctx, src, "order-4"); err != nil {
		return err
	}
	if _, err := transfer(ctx, con, pub, 1, false); err != nil {
		return err
	}
	p("interrupted transfer: confirmed on the destination, source delivery NOT settled\n")

	attempt, err := awaitRedelivery(ctx, con, pub)
	if err != nil {
		return err
	}
	p("after the source's ack deadline: redelivered attempt=%d, and published again\n", attempt)
	bodies, err = held(ctx, dst, 2)
	if err != nil {
		return err
	}
	p("destination now holds: %s\n", strings.Join(bodies, " "))
	p("duplicate on the destination: order-4 twice, from one publication and one redelivery\n")
	p("the seam cannot do better without a transaction spanning both brokers, which " +
		"the specification does not offer\n")
	return nil
}

// fill publishes the messages the bridge will move.
func fill(ctx context.Context, src *mq.Client) error {
	pub, err := src.Publisher(ctx, mq.PublisherConfig{
		Topic: sourceTopic, Confirmation: mq.ConfirmDurable,
	})
	if err != nil {
		return fmt.Errorf("opening the source publisher: %w", err)
	}
	defer func() { _ = pub.Shutdown(ctx) }()
	for i := 1; i <= messageCount; i++ {
		m := &mq.Message{Body: []byte(fmt.Sprintf("order-%d", i))}
		m.Headers.Add("origin", []byte("jetstream"))
		if err := pub.Publish(ctx, m); err != nil {
			return fmt.Errorf("publishing to the source: %w", err)
		}
	}
	return pub.Flush(ctx)
}

// publishOne publishes one more message to the source.
func publishOne(ctx context.Context, src *mq.Client, body string) error {
	pub, err := src.Publisher(ctx, mq.PublisherConfig{
		Topic: sourceTopic, Confirmation: mq.ConfirmDurable,
	})
	if err != nil {
		return fmt.Errorf("opening the source publisher: %w", err)
	}
	defer func() { _ = pub.Shutdown(ctx) }()
	if err := pub.Publish(ctx, &mq.Message{Body: []byte(body)}); err != nil {
		return fmt.Errorf("publishing to the source: %w", err)
	}
	return pub.Flush(ctx)
}

// transfer moves up to n messages, in the only order that is not lossy.
//
// settle says whether the source delivery is acknowledged after the
// destination has confirmed. It is false exactly once in this program, to
// produce the duplicate, and a real bridge always passes true -- the
// interesting thing is that "always true" is not enough, because the process
// can stop between the two calls whatever the code says.
func transfer(
	ctx context.Context, con *mq.ConsumerHandle, pub *mq.PublisherHandle, n int, settle bool,
) (int, error) {
	moved := 0
	dst := make([]mq.Delivery, n)
	for moved < n {
		fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		got, err := con.Receive(fetchCtx, dst)
		cancel()
		for i := range got {
			d := &dst[i]
			// The specification borrows the received body until the delivery is settled,
			// and the settlement is after the publication, so the message
			// handed to Publish is a CLONE: a destination driver that
			// retained the buffer would otherwise hold storage the source is
			// free to reuse the moment Ack returns.
			m := d.CloneMessage()
			m.Topic = destTopic
			if perr := pub.Publish(ctx, m); perr != nil {
				// Nothing reached the destination, so the message goes back
				// to the source rather than being dropped.
				_ = d.Nack(mq.NackRequeue())
				return moved, fmt.Errorf("publishing to the destination: %w", perr)
			}
			// CONFIRMED, not merely admitted: Publish may have handed the
			// message to a buffer, and settling the source against an
			// admission is the same lost message as settling before
			// publishing.
			if ferr := pub.Flush(ctx); ferr != nil {
				_ = d.Nack(mq.NackRequeue())
				return moved, fmt.Errorf("confirming on the destination: %w", ferr)
			}
			if settle {
				if aerr := d.Ack(); aerr != nil {
					// The destination has it and the source still owes it:
					// the duplicate window, reported rather than hidden.
					return moved, fmt.Errorf("the destination confirmed and the source would "+
						"not settle, so it will redeliver: %w", aerr)
				}
			}
			moved++
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return moved, fmt.Errorf("waiting for the source: %w", err)
			}
			return moved, fmt.Errorf("receiving from the source: %w", err)
		}
	}
	return moved, nil
}

// awaitRedelivery waits for the source to redeliver the message that was never
// settled, publishes it again, and returns the attempt number the source
// reported.
//
// Nothing sleeps: Receive blocks until JetStream's ack deadline expires and
// the server sends the message again, so the wait is the broker's own timing
// and not a guess about it.
func awaitRedelivery(
	ctx context.Context, con *mq.ConsumerHandle, pub *mq.PublisherHandle,
) (int, error) {
	dst := make([]mq.Delivery, 1)
	waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for {
		n, err := con.Receive(waitCtx, dst)
		if n > 0 {
			d := &dst[0]
			attempt := d.Attempt
			m := d.CloneMessage()
			m.Topic = destTopic
			if perr := pub.Publish(ctx, m); perr != nil {
				_ = d.Nack(mq.NackRequeue())
				return 0, fmt.Errorf("republishing the redelivery: %w", perr)
			}
			if ferr := pub.Flush(ctx); ferr != nil {
				_ = d.Nack(mq.NackRequeue())
				return 0, fmt.Errorf("confirming the redelivery: %w", ferr)
			}
			if aerr := d.Ack(); aerr != nil {
				return 0, fmt.Errorf("settling the redelivery: %w", aerr)
			}
			return attempt, nil
		}
		if err != nil {
			return 0, fmt.Errorf("waiting for the redelivery: %w", err)
		}
	}
}

// held reads up to n messages out of the destination and returns their bodies.
//
// It acknowledges what it reads, so two calls do not see the same message
// twice: the second call's output is what arrived SINCE the first, which is
// what makes the duplicate visible as a line of its own.
func held(ctx context.Context, dst *mq.Client, n int) ([]string, error) {
	con, err := dst.Consumer(ctx, mq.ConsumerConfig{Topic: destTopic, Subscription: destSub})
	if err != nil {
		return nil, fmt.Errorf("opening the destination consumer: %w", err)
	}
	defer func() { _ = con.Shutdown(ctx) }()
	out := make([]string, 0, n)
	batch := make([]mq.Delivery, n)
	for len(out) < n {
		fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		got, rerr := con.Receive(fetchCtx, batch)
		cancel()
		for i := range got {
			out = append(out, string(batch[i].Body))
			if aerr := batch[i].Ack(); aerr != nil {
				return nil, fmt.Errorf("acknowledging on the destination: %w", aerr)
			}
		}
		if rerr != nil {
			return out, nil
		}
	}
	return out, nil
}
