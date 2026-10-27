package pulsar_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// the specification's own dead-letter route is the subject of this file (). The route publishes a COPY of the poison message stamped with five
// provenance headers through the ORDINARY publish path, and every one of those
// five is under mq.ReservedHeaderPrefix — so a driver whose Representable
// refuses any key under that prefix refuses iqmq's own provenance, the
// publication fails before any I/O, the runner requeues the message and counts
// a DeadLetterFailures, and the message is redelivered for ever.

// deadLetterCopy is the message the route publishes, built exactly as
// dlq.go's deadLetterMessage builds it.
func deadLetterCopy() *mq.Message {
	m := &mq.Message{Key: []byte("order-42"), Body: []byte(`{"id":42}`)}
	m.Headers.Set("content-kind", []byte("order"))
	m.Headers.Set(mq.HeaderOriginTopic, []byte("orders"))
	m.Headers.Set(mq.HeaderOriginSubscription, []byte("billing"))
	m.Headers.Set(mq.HeaderAttempts, []byte("3"))
	m.Headers.Set(mq.HeaderFailure, []byte("schema mismatch"))
	m.Headers.Set(mq.HeaderFirstDeliveredAt, []byte("2026-09-18T10:11:12.5Z"))
	return m
}

// provenance is the five keys in the order dlq.go writes them.
var provenance = [...]string{
	mq.HeaderOriginTopic, mq.HeaderOriginSubscription, mq.HeaderAttempts,
	mq.HeaderFailure, mq.HeaderFirstDeliveredAt,
}

// TestTheCoreDeadLetterRouteIsRepresentable is the mapping half: strict mode
// accepts the stamped copy, and the round trip hands every provenance header
// back to whoever reads the dead-letter topic.
func TestTheCoreDeadLetterRouteIsRepresentable(t *testing.T) {
	m := deadLetterCopy()
	if err := pd.Representable(m); err != nil {
		t.Fatalf("Representable(the dead-letter copy the specification publishes) = %v, want nil: "+
			"every one of the five provenance headers is the CORE's and not a caller's", err)
	}

	b, _ := newRig(t, nil)
	snk := sinkOf(t, b, "dl-map", mq.MetadataNativeStrict)
	src := sourceOf(t, b, "dl-map", "audit", mq.MetadataNativeStrict, 4)
	publish(t, snk, m)
	d := fetchOne(t, src)
	got := d.Message
	for _, k := range provenance {
		v, ok := got.Headers.Get(k)
		if !ok {
			t.Errorf("the round trip dropped %q: an operator reading the dead-letter topic cannot see where the message came from", k)
			continue
		}
		want, _ := m.Headers.Get(k)
		if !bytes.Equal(v, want) {
			t.Errorf("%q came back as %q, want %q", k, v, want)
		}
	}
	if v, ok := got.Headers.Get("content-kind"); !ok || string(v) != "order" {
		t.Errorf("the caller's own header came back as %q, %v", v, ok)
	}
}

// TestACallersReservedHeaderIsStillRefused keeps the widening to exactly five
// keys: the rule about a CALLER's header under the reserved prefix is
// unchanged.
func TestACallersReservedHeaderIsStillRefused(t *testing.T) {
	for _, k := range []string{
		mq.ReservedHeaderPrefix + "key",
		mq.ReservedHeaderPrefix + "origin",
		mq.ReservedHeaderPrefix + "attempt",
		pd.PropID,
		driverkit.EnvelopeMarker,
	} {
		m := &mq.Message{Body: []byte("b")}
		m.Headers.Set(k, []byte("v"))
		if err := pd.Representable(m); err == nil {
			t.Errorf("Representable accepted the caller's reserved header %q", k)
		} else if mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("Representable(%q) = %v (code %v), want CodeInvalidArgument", k, err, mq.CodeOf(err))
		}
	}
}

// TestTheCoreDeadLetterRouteReachesTheDeadLetterTopic is the end-to-end half
// over the in-process model: a handler that always fails, mq.WithDeadLetter,
// and the stamped copy read off the dead-letter topic.
func TestTheCoreDeadLetterRouteReachesTheDeadLetterTopic(t *testing.T) {
	const topic, dlqTopic = "dl-orders", "dl-orders-dlq"
	server := pulsarfake.New()
	client, err := pd.OpenWithClient(context.Background(), server.Client(),
		driverkit.ClientOwned, rigConfig(server))
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dlq, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: dlqTopic, Subscription: "audit"})
	if err != nil {
		t.Fatalf("dead-letter consumer: %v", err)
	}
	defer func() { _ = dlq.Close() }()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = consumer.Close() }()

	m := &mq.Message{Key: []byte("order-42"), Body: []byte(`{"id":42}`)}
	m.Headers.Set("content-kind", []byte("order"))
	if err := client.Publish(ctx, topic, m); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(runCtx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			return errors.New("schema mismatch")
		}), mq.WithMaxAttempts(2), mq.WithDeadLetter(dlqTopic),
			mq.WithOrderingByKey(mq.OrderingStrict),
			mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	}()

	recvCtx, recvCancel := context.WithTimeout(ctx, 20*time.Second)
	defer recvCancel()
	dst := make([]mq.Delivery, 1)
	n, rerr := dlq.Receive(recvCtx, dst)
	if n != 1 {
		t.Fatalf("nothing arrived on %q (Receive = %d, %v); the dead-letter publication never landed", dlqTopic, n, rerr)
	}
	got := dst[0].Message
	for _, k := range provenance {
		if _, ok := got.Headers.Get(k); !ok {
			t.Errorf("the dead-lettered copy carries no %q", k)
		}
	}
	if v, _ := got.Headers.Get(mq.HeaderFailure); string(v) != "schema mismatch" {
		t.Errorf("%s = %q, want %q", mq.HeaderFailure, v, "schema mismatch")
	}
	if v, _ := got.Headers.Get(mq.HeaderOriginTopic); string(v) != topic {
		t.Errorf("%s = %q, want %q", mq.HeaderOriginTopic, v, topic)
	}
	if string(got.Body) != `{"id":42}` {
		t.Errorf("the copy's body is %q, want the original's", got.Body)
	}
	// The copy is a new message, so its count starts again, and the
	// attempt that failed is in the provenance rather than in the count.
	if dst[0].Attempt != 1 {
		t.Errorf("the copy's first delivery reports attempt %d, want 1", dst[0].Attempt)
	}
	if v, _ := got.Headers.Get(mq.HeaderAttempts); string(v) != "2" {
		t.Errorf("%s = %q, want the failed attempt %q", mq.HeaderAttempts, v, "2")
	}
	if err := dst[0].Ack(); err != nil {
		t.Errorf("acking the dead-lettered copy: %v", err)
	}

	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	if st := consumer.Stats(); st.DeadLetterFailures != 0 {
		t.Errorf("DeadLetterFailures = %d, want 0: the route was refused and the message requeued", st.DeadLetterFailures)
	}
}
