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

// The specification's own dead-letter route is the subject of this file. The route publishes a COPY of the poison message stamped with five
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

// TestTheCoreDeadLetterRouteTakesARejectedMessage: a message this driver's
// Reject route dead-lettered carries iqmq-dead-letter-from, -reason and
// -attempt, and Decode hands them to the dead-letter subject's consumer. The
// core's dead-letter copy kept them, and Representable refuses them as a
// caller's keys, so mq.WithDeadLetter over the dead-letter subject was
// refused before any I/O for every message, which was requeued and counted
// in DeadLetterFailures on every delivery. The copy now leaves the earlier
// hop's provenance out and lands, its own origin the dead-letter subject.
func TestTheCoreDeadLetterRouteTakesARejectedMessage(t *testing.T) {
	if v, old := rootModuleBefore(t, "v1.1.4"); old {
		t.Skipf("built with root module %s: the dead-letter copy leaves an earlier hop's provenance out from v1.1.4", v)
	}
	native := "dlq.rejected"
	b := openBackend(t, func(cfg *nq.Config) { cfg.DeadLetterTopic = native })
	c := mq.NewClient(b)
	topic := newTopic("poison")
	routed := newTopic("routed")
	cons := consumerOn(t, c, topic, "workers")
	dead := consumerWith(t, c, mq.ConsumerConfig{Topic: native, Subscription: "dlq-readers", Start: mq.StartEarliest()})
	audit := consumerOn(t, c, routed, "audit")
	publish(t, c, topic, &mq.Message{Body: []byte("poison")})
	d := receiveOne(t, cons, 10*time.Second)
	if err := d.Nack(mq.NackDeadLetter(), mq.NackReason("unparsable")); err != nil {
		t.Fatalf("Reject: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	rejected := make(chan bool, 1)
	done := make(chan error, 1)
	go func() {
		done <- dead.Run(ctx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			_, from := d.Headers.Get(nq.HeaderDeadLetterFrom)
			select {
			case rejected <- from:
			default:
			}
			return errors.New("still poison")
		}), mq.WithDeadLetter(routed), mq.WithMaxAttempts(1))
	}()
	got, ok := tryReceive(t, audit, 20*time.Second)
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run: %v", err)
	}
	st := dead.Stats()
	if !ok {
		t.Fatalf("the route's dead-letter subject received nothing; DeadLetterFailures = %d: "+
			"the copy carried this driver's own dead-letter headers, which Representable refuses as a caller's", st.DeadLetterFailures)
	}
	select {
	case carried := <-rejected:
		if !carried {
			t.Error("the rejected message reached the handler without its dead-letter headers: nothing here tests the copy")
		}
	default:
		t.Error("the handler never ran")
	}
	for _, k := range []string{nq.HeaderDeadLetterFrom, nq.HeaderDeadLetterReason, nq.HeaderDeadLetterAttempt} {
		if v, ok := got.Headers.Get(k); ok {
			t.Errorf("the dead-letter copy carries the earlier hop's %s=%q", k, v)
		}
	}
	if v, _ := got.Headers.Get(mq.HeaderOriginTopic); string(v) != native {
		t.Errorf("%s = %q, want the dead-letter subject the route consumed it from, %q", mq.HeaderOriginTopic, v, native)
	}
	// A copy is published again only if the delivery's AckWait runs out
	// before the ack, which a slow runner can make happen; one or more is
	// the route working, and DeadLetterFailures is the refusal.
	if st.DeadLetterFailures != 0 || st.DeadLetterPublished < 1 {
		t.Errorf("DeadLetterFailures = %d and DeadLetterPublished = %d, want 0 and at least 1", st.DeadLetterFailures, st.DeadLetterPublished)
	}
	_ = got.Ack()
}

// TestARejectWhoseDeadLetterCopyIsAmbiguousFreesItsSlot holds that a Reject
// whose dead-letter copy may or may not have been stored gives up the
// delivery's token slot, as an ambiguous ack, nak or term does. The slot was
// kept until this source was handed the record again, which never happens
// once the durable's MaxDeliver is reached or when the redelivery goes to
// another consumer of the durable, so Prefetch+4 such Rejects filled the
// token table and the source handed out nothing again.
func TestARejectWhoseDeadLetterCopyIsAmbiguousFreesItsSlot(t *testing.T) {
	cfg := baseConfig(t)
	cfg.StreamSubjects = []string{cfg.SubjectPrefix + ".orders"}
	cfg.DeadLetterTopic = "dead"
	cfg.PublishTimeout = 300 * time.Millisecond
	cfg.AckWait = 2 * time.Second
	// The server never redelivers a message to this source, so nothing but
	// the Reject can free its slot.
	cfg.MaxDeliver = 1

	// A subscriber that takes the copies and never answers stands in for a
	// dead-letter stream whose PubAck is lost: the copy times out, which is
	// ambiguous.
	raw := natsConn(t)
	if _, err := raw.Subscribe(cfg.SubjectPrefix+".dead", func(*natsgo.Msg) {}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := raw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	c, err := nq.Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	h := consumerWith(t, c, mq.ConsumerConfig{Topic: "orders", Subscription: "workers", Prefetch: 1})

	// Prefetch 1 makes a token table of five slots.
	const rejects = 5
	for i := range rejects {
		publish(t, c, "orders", &mq.Message{Body: []byte{byte('a' + i)}})
		d := receiveOne(t, h, 10*time.Second)
		if err := d.Reject(); !mq.IsAmbiguous(err) {
			t.Fatalf("reject %d = %v (code %v), want an ambiguous failure", i+1, err, mq.CodeOf(err))
		}
	}
	publish(t, c, "orders", &mq.Message{Body: []byte("next")})
	d, ok := tryReceive(t, h, 3*cfg.AckWait)
	if !ok {
		t.Fatalf("after %d Rejects whose dead-letter copies were ambiguous, a new message was not handed out within %v: a Prefetch 1 source's five token slots are still held",
			rejects, 3*cfg.AckWait)
	}
	_ = d.Ack()
}
