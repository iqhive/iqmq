package kafka_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// the specification's own dead-letter route is the subject of this file (). The route publishes a COPY of the poison message stamped with five
// provenance headers through the ORDINARY publish path, and every one of those
// five is under mq.ReservedHeaderPrefix — so a driver whose Representable
// refuses any key under that prefix refuses iqmq's own provenance, the
// publication fails before any I/O, the runner requeues the message and counts
// a DeadLetterFailures, and the message is redelivered for ever.
//
// The five keys are named through the ROOT package's constants on purpose: the
// contract is the core's, and a test that spelled the strings itself would
// still pass on the day dlq.go renamed one.

// deadLetterCopy is the message the route publishes, built exactly as
// dlq.go's deadLetterMessage builds it: the original's key and body, plus the
// five provenance headers.
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
	if err := kq.Representable(m); err != nil {
		t.Fatalf("Representable(the dead-letter copy the specification publishes) = %v, want nil: "+
			"every one of the five provenance headers is the CORE's and not a caller's", err)
	}

	var rec kgo.Record
	_ = kq.Encode(&rec, m, "orders.dlq", nil)
	var got mq.Message
	kq.Decode(&got, &rec, "")
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

// TestACallersReservedHeaderIsStillRefused is the other half, and it is what
// keeps the widening to exactly five keys: the rule about a CALLER's header
// under the reserved prefix is unchanged.
func TestACallersReservedHeaderIsStillRefused(t *testing.T) {
	for _, k := range []string{
		mq.ReservedHeaderPrefix + "key",
		mq.ReservedHeaderPrefix + "origin",
		mq.ReservedHeaderPrefix + "attempt",
		kq.HeaderID,
		kq.HeaderEnvelope,
	} {
		m := &mq.Message{Body: []byte("b")}
		m.Headers.Set(k, []byte("v"))
		if err := kq.Representable(m); err == nil {
			t.Errorf("Representable accepted the caller's reserved header %q", k)
		} else if mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("Representable(%q) = %v (code %v), want CodeInvalidArgument", k, err, mq.CodeOf(err))
		}
	}
}

// TestTheCoreDeadLetterRouteReachesTheDeadLetterTopic is the end-to-end half:
// a handler that always fails, mq.WithDeadLetter, and the stamped copy read
// back off the dead-letter topic with its provenance intact.
//
// It asserts ConsumerStats.DeadLetterFailures is zero as well as reading the
// copy, because the defect's signature was a SILENT failure: the runner counts
// and requeues rather than returning the error, so a route that cannot publish
// looks from the caller's side like a message that is taking a while.
func TestTheCoreDeadLetterRouteReachesTheDeadLetterTopic(t *testing.T) {
	const topic, dlqTopic, group = "dl-orders", "dl-orders-dlq", "dl-billing"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic, dlqTopic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()
	addr := cluster.ListenAddrs()[0]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := kq.Open(ctx, kq.Config{Brokers: []string{addr}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = client.Close() }()

	m := &mq.Message{Key: []byte("order-42"), Body: []byte(`{"id":42}`)}
	m.Headers.Set("content-kind", []byte("order"))
	if err := client.Publish(ctx, topic, m); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic: topic, Subscription: group, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = consumer.Close() }()

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(runCtx, mq.HandlerFunc(func(_ context.Context, _ *mq.Delivery) error {
			return errors.New("schema mismatch")
		}), mq.WithMaxAttempts(1), mq.WithDeadLetter(dlqTopic))
	}()

	// Read the dead-letter topic with a raw client: the copy has to be
	// readable by whoever operates the dead-letter topic, and that is not
	// necessarily an iqmq consumer.
	raw, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics(dlqTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer raw.Close()

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 25*time.Second)
	defer fetchCancel()
	fetches := raw.PollRecords(fetchCtx, 1)
	if err := fetches.Err0(); err != nil {
		t.Fatalf("polling %q: %v; the dead-letter publication never landed", dlqTopic, err)
	}
	recs := fetches.Records()
	if len(recs) == 0 {
		t.Fatal("nothing arrived on the dead-letter topic")
	}
	var got mq.Message
	kq.Decode(&got, recs[0], "")
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
	st := consumer.Stats()
	if st.DeadLetterFailures != 0 {
		t.Errorf("DeadLetterFailures = %d, want 0: the route was refused and the message requeued", st.DeadLetterFailures)
	}
}
