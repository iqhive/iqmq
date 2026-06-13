package nats_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/nats"
)

// the specification's own dead-letter route is the subject of this file (). The route publishes a COPY of the poison message stamped with five
// provenance headers through the ORDINARY publish path, and every one of those
// five is under mq.ReservedHeaderPrefix — so a driver whose Representable
// refuses any key under that prefix refuses iqmq's own provenance, the
// publication fails before any I/O, the runner requeues the message and counts
// a DeadLetterFailures, and the message is redelivered for ever.
//
// This driver had the defect twice over: Representable refused the copy, and
// Encode DROPPED every reserved key it was given, so even a widened admission
// check would have published a copy with no provenance on it at all.

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
// accepts the stamped copy, Encode puts all five on the wire, and the round
// trip hands them back to whoever reads the dead-letter topic.
func TestTheCoreDeadLetterRouteIsRepresentable(t *testing.T) {
	m := deadLetterCopy()
	if err := nq.Representable(m); err != nil {
		t.Fatalf("Representable(the dead-letter copy the specification publishes) = %v, want nil: "+
			"every one of the five provenance headers is the CORE's and not a caller's", err)
	}

	var msg natsgo.Msg
	msg.Subject = "orders.dlq"
	var dropped []string
	nq.Encode(&msg, m, func(k string) { dropped = append(dropped, k) })
	if len(dropped) != 0 {
		t.Errorf("Encode dropped %q from the dead-letter copy", dropped)
	}
	for _, k := range provenance {
		if vals := msg.Header.Values(k); len(vals) == 0 {
			t.Errorf("Encode left %q off the wire", k)
		}
	}

	var got mq.Message
	nq.Decode(&got, &msg, "")
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
// unchanged, and Encode still drops one under MetadataLenientDrop.
func TestACallersReservedHeaderIsStillRefused(t *testing.T) {
	for _, k := range []string{
		mq.ReservedHeaderPrefix + "key",
		mq.ReservedHeaderPrefix + "origin",
		mq.ReservedHeaderPrefix + "attempt",
		nq.HeaderID,
		nq.HeaderEnvelope,
	} {
		m := &mq.Message{Body: []byte("b")}
		m.Headers.Set(k, []byte("v"))
		if err := nq.Representable(m); err == nil {
			t.Errorf("Representable accepted the caller's reserved header %q", k)
		} else if mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("Representable(%q) = %v (code %v), want CodeInvalidArgument", k, err, mq.CodeOf(err))
		}
		var msg natsgo.Msg
		msg.Subject = "s"
		var dropped []string
		nq.Encode(&msg, m, func(key string) { dropped = append(dropped, key) })
		if len(dropped) != 1 || dropped[0] != k {
			t.Errorf("Encode dropped %q, want exactly %q", dropped, k)
		}
	}
}

// TestTheCoreDeadLetterRouteReachesTheDeadLetterTopic is the end-to-end half
// over the embedded server: a handler that always fails, mq.WithDeadLetter,
// and the stamped copy read off the dead-letter topic.
//
// The dead-letter consumer is opened BEFORE the poison message is published,
// because NATS Core has no retention: a subject with no subscriber discards
// what is published to it.
//
// The run asks for OrderingStrict, and that is not decoration. NATS Core
// cannot settle a delivery at all — Nack answers mq.ErrNotSettleable — and it
// keeps no attempt counter, so under the default OrderingRelaxed the runner
// requeues by nack, the nack fails, runner.exhausted reads the broker's zero
// and the message is NEVER dead-lettered: measured as
// `SettleFailures:3 DeadLetterFailures:0` with nothing on the dead-letter
// subject. OrderingStrict holds the retry in process, which is what makes the
// attempt count exact on such a broker , and is
// therefore the only shape in which the route is reachable here at all.
// That is a separate finding from the reserved-prefix refusal this file fixes,
// and it is recorded.
func TestTheCoreDeadLetterRouteReachesTheDeadLetterTopic(t *testing.T) {
	const topic, dlqTopic = "dl.orders", "dl.orders.dlq"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := nq.Open(ctx, nq.Config{Addrs: []string{exampleAddr}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = client.Close() }()

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

	// The run has to be subscribed before the poison message is published, and
	// a NATS subscription is asynchronous: publish until the handler has been
	// reached, which is what the dead-letter arrival proves.
	m := &mq.Message{Key: []byte("order-42"), Body: []byte(`{"id":42}`)}
	m.Headers.Set("content-kind", []byte("order"))

	dst := make([]mq.Delivery, 1)
	deadline := time.Now().Add(20 * time.Second)
	var n int
	for time.Now().Before(deadline) {
		if err := client.Publish(ctx, topic, m); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		pollCtx, pollCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		n, err = dlq.Receive(pollCtx, dst)
		pollCancel()
		if n > 0 {
			break
		}
	}
	if n == 0 {
		t.Fatalf("nothing arrived on %q (last receive: %v); the dead-letter publication never landed", dlqTopic, err)
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

	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	if st := consumer.Stats(); st.DeadLetterFailures != 0 {
		t.Errorf("DeadLetterFailures = %d, want 0: the route was refused and the message requeued", st.DeadLetterFailures)
	}
}
