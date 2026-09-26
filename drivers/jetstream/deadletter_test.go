package jetstream_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// the specification's own dead-letter route is the subject of this file (). The route publishes a COPY of the poison message stamped with five
// provenance headers through the ORDINARY publish path, and every one of those
// five is under mq.ReservedHeaderPrefix — so a driver whose Representable
// refuses any key under that prefix refuses iqmq's own provenance, the
// publication fails before any I/O, the runner requeues the message and counts
// a DeadLetterFailures, and the message is redelivered for ever.
//
// This driver had the defect twice over: Representable refused the copy, and
// Encode DROPPED every reserved key it was given.

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
	nq.Decode(&got, msg.Subject, msg.Reply, msg.Header, msg.Data, "")
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
// over the embedded JetStream server: a handler that always fails,
// mq.WithDeadLetter, and the stamped copy read off the dead-letter subject.
//
// JetStream settles and counts attempts, so the default OrderingRelaxed is
// enough here — unlike NATS Core, where the route is reachable only under
// OrderingStrict.
func TestTheCoreDeadLetterRouteReachesTheDeadLetterTopic(t *testing.T) {
	client := openClient(t)
	topic := newTopic("dl")
	dlqTopic := newTopic("dl")

	dlq := consumerOn(t, client, dlqTopic, "audit")
	consumer := consumerOn(t, client, topic, "billing")

	m := &mq.Message{Key: []byte("order-42"), Body: []byte(`{"id":42}`)}
	m.Headers.Set("content-kind", []byte("order"))
	publish(t, client, topic, m)

	runCtx, stop := context.WithCancel(t.Context())
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(runCtx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			return errors.New("schema mismatch")
		}), mq.WithMaxAttempts(1), mq.WithDeadLetter(dlqTopic),
			mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	}()

	d := receiveOne(t, dlq, 30*time.Second)
	got := d.Message
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
	if err := d.Ack(); err != nil {
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
