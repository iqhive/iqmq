package jetstream_test

import (
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// publishPoison writes a message an enveloping consumer cannot decode: a plain
// body from a native publisher, which a MetadataPortableEnvelope consumer
// refuses because it is not an envelope.
func publishPoison(tb testing.TB, c *mq.Client, topic string) {
	tb.Helper()
	m := &mq.Message{Body: []byte("not an envelope")}
	m.Headers.Set("content-kind", []byte("order"))
	publish(tb, c, topic, m)
}

// publishGood writes a message the same consumer can decode.
func publishGood(tb testing.TB, c *mq.Client, topic string) {
	tb.Helper()
	p, err := c.Publisher(tb.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if err := p.Publish(tb.Context(), &mq.Message{Body: []byte("good")}); err != nil {
		tb.Fatal(err)
	}
}

// TestAnUndecodableMessageIsNotRedeliveredInAHotLoop is the consumer that spun.
//
// With no dead-letter topic an undecodable message used to be naked at once,
// the server redelivered it at once, and MaxDeliver is unlimited by default, so
// the consume callback went round at thousands of deliveries a second for as
// long as the message existed. It is now naked with a delay of one lease, so
// it comes back once per AckWait and the rest of the prefetch carries the
// messages behind it.
//
// The prefetch is two and not one on purpose. JetStream keeps a naked message
// in the consumer's pending set, with or without a delay, so it holds one
// MaxAckPending slot until it is acknowledged, terminated or runs out of
// MaxDeliver: with a prefetch of one and no dead-letter topic nothing behind it
// can arrive, and TestAnUndecodableMessageIsDeadLettered is the configuration
// that frees the slot.
func TestAnUndecodableMessageIsNotRedeliveredInAHotLoop(t *testing.T) {
	const ackWait = 2 * time.Second
	b := openBackend(t, func(cfg *nq.Config) { cfg.AckWait = ackWait })
	c := mq.NewClient(b)
	topic := newTopic("poison")
	cons := consumerWith(t, c, mq.ConsumerConfig{
		Topic: topic, Subscription: "workers", Prefetch: 2,
		Metadata: mq.MetadataPortableEnvelope,
	})
	publishPoison(t, c, topic)
	publishGood(t, c, topic)

	d := receiveOne(t, cons, 10*time.Second)
	if string(d.Body) != "good" {
		t.Fatalf("the delivery behind the undecodable one is %q, want %q", d.Body, "good")
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
	// Every delivery of the undecodable message is one consume error, so
	// the count is the number of times the server sent it. Half a lease
	// after the first, a delayed nak has had it sent once; an immediate one
	// had it sent thousands of times.
	waitFor(t, 10*time.Second, func() bool { return b.ConsumeErrors() >= 1 })
	time.Sleep(ackWait / 2)
	if got := b.ConsumeErrors(); got > 2 {
		t.Fatalf("the undecodable message was delivered %d times in half a lease: it is naked straight back into the consumer", got)
	}
	// And it is not lost: it comes back once the delay is up.
	first := b.ConsumeErrors()
	waitFor(t, 4*ackWait, func() bool { return b.ConsumeErrors() > first })
}

// TestAnUndecodableMessageIsDeadLettered is the same message with somewhere to
// go. A raw copy lands on the dead-letter subject with the reason it was
// refused, the original is terminated, so it never comes back, and the
// consumer's only prefetch slot is free for the message behind it.
//
// Health stays Ready: the message was moved where an operator reads it, so
// there is nothing wrong with the client to report.
func TestAnUndecodableMessageIsDeadLettered(t *testing.T) {
	const ackWait = time.Second
	dlq := "dlq.undecodable"
	b := openBackend(t, func(cfg *nq.Config) {
		cfg.DeadLetterTopic = dlq
		cfg.AckWait = ackWait
	})
	c := mq.NewClient(b)
	topic := newTopic("poison")
	cons := consumerWith(t, c, mq.ConsumerConfig{
		Topic: topic, Subscription: "workers", Prefetch: 1,
		Metadata: mq.MetadataPortableEnvelope,
	})
	dead := consumerWith(t, c, mq.ConsumerConfig{
		Topic: dlq, Subscription: "dlq-readers", Start: mq.StartEarliest(),
	})
	publishPoison(t, c, topic)
	publishGood(t, c, topic)

	d := receiveOne(t, cons, 10*time.Second)
	if string(d.Body) != "good" {
		t.Fatalf("the delivery behind the undecodable one is %q, want %q", d.Body, "good")
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}

	routed := receiveOne(t, dead, 10*time.Second)
	if string(routed.Body) != "not an envelope" {
		t.Errorf("the dead-letter destination holds %q, want the original payload", routed.Body)
	}
	if v, ok := routed.Headers.Get("content-kind"); !ok || string(v) != "order" {
		t.Errorf("the dead-letter copy lost the publisher's header: %q (present %v)", v, ok)
	}
	if v, ok := routed.Headers.Get(nq.HeaderDeadLetterReason); !ok || !strings.HasPrefix(string(v), "undecodable: ") {
		t.Errorf("the dead-letter copy carries reason %q, want one starting %q", v, "undecodable: ")
	}
	if v, ok := routed.Headers.Get(nq.HeaderDeadLetterFrom); !ok || len(v) == 0 {
		t.Error("the dead-letter copy does not name the subject it came from")
	}
	if v, ok := routed.Headers.Get(nq.HeaderDeadLetterAttempt); !ok || string(v) != "1" {
		t.Errorf("the dead-letter copy carries attempt %q, want 1", v)
	}
	if err := routed.Ack(); err != nil {
		t.Fatalf("acking the dead-lettered copy: %v", err)
	}

	// The original was terminated, so two leases later it has not come back:
	// a redelivery would have been dead-lettered a second time.
	time.Sleep(2 * ackWait)
	if got := b.DeadLettered(); got != 1 {
		t.Errorf("DeadLettered = %d, want 1: the original was redelivered after its copy was stored", got)
	}
	if extra, ok := tryReceive(t, dead, ackWait); ok {
		t.Errorf("a second dead-letter copy arrived (attempt %s)", headerOf(extra, nq.HeaderDeadLetterAttempt))
	}
	if got := b.ConsumeErrors(); got != 0 {
		t.Errorf("ConsumeErrors = %d, want 0: a message that was dead-lettered is not a background failure", got)
	}
	if got := b.Health().State; got != mq.HealthReady {
		t.Errorf("health is %v after a successful dead-letter, want Ready", got)
	}
}

// waitFor polls cond until it holds or the budget runs out.
func waitFor(tb testing.TB, budget time.Duration, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(budget)
	for !cond() {
		if time.Now().After(deadline) {
			tb.Fatalf("the condition did not hold within %v", budget)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// headerOf is one header's value as a string, for a failure message.
func headerOf(d mq.Delivery, key string) string {
	v, _ := d.Headers.Get(key)
	return string(v)
}
