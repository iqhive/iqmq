package toydriver_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/examples/custom-driver/toydriver"
)

// openEnvelopeToy opens a toy broker over topics, publishes a record that is
// not an envelope to "t" (a producer in another metadata mode, or one that is
// not iqmq) and then an enveloped one, and returns the broker and a client.
func openEnvelopeToy(t *testing.T, topics ...toydriver.TopicSpec) (*toydriver.Broker, *mq.Client) {
	t.Helper()
	b, err := toydriver.New(toydriver.Config{Topics: topics})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client := mq.NewClient(b)
	t.Cleanup(func() { _ = client.Close() })
	ctx := t.Context()
	raw, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "t", Metadata: mq.MetadataNativeStrict})
	if err != nil {
		t.Fatalf("native Publisher: %v", err)
	}
	env, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "t", Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("envelope Publisher: %v", err)
	}
	if err := raw.Publish(ctx, &mq.Message{Body: []byte("not an envelope")}); err != nil {
		t.Fatalf("native Publish: %v", err)
	}
	if err := env.Publish(ctx, &mq.Message{Body: []byte("good")}); err != nil {
		t.Fatalf("envelope Publish: %v", err)
	}
	return b, client
}

// runUntilGood runs an envelope-mode consumer of "t"/"s" until its handler has
// seen the enveloped record, and fails the test if Run ends any other way.
//
// One delivery in flight makes the record that is not an envelope the whole
// of the first fetch, so the wrapper has nothing left to hand over from it.
func runUntilGood(t *testing.T, client *mq.Client) {
	t.Helper()
	c, err := client.Consumer(t.Context(), mq.ConsumerConfig{Topic: "t", Subscription: "s", Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var good, other atomic.Int64
	h := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		if string(d.Body) == "good" {
			good.Add(1)
			cancel()
		} else {
			other.Add(1)
		}
		return nil
	})
	err = c.Run(ctx, h, mq.WithMaxInFlight(1))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run ended with %v (code %v) before the enveloped record behind the one that is not an envelope was handled (handled %d)",
			err, mq.CodeOf(err), good.Load())
	}
	if good.Load() != 1 || other.Load() != 0 {
		t.Fatalf("the handler saw the enveloped record %d time(s) and %d other record(s), want 1 and 0", good.Load(), other.Load())
	}
}

// TestRunKeepsGoingPastARecordThatIsNotAnEnvelope holds that a record the
// envelope wrapper refuses is a background failure and not the end of the
// Run. With no dead-letter route the refusal leaves the record unsettled, so
// it comes back each time its lease runs out; a Run that ended on it would
// end on it again after every restart, and the record behind it would never
// be handled.
func TestRunKeepsGoingPastARecordThatIsNotAnEnvelope(t *testing.T) {
	b, client := openEnvelopeToy(t, toydriver.TopicSpec{Name: "t", Subscriptions: []string{"s"}, AckDeadline: time.Second})
	runUntilGood(t, client)

	// The refusal is counted, and with nowhere to move the record it
	// degrades health until a successful Ping.
	if got := b.Undecodable(); got != 1 {
		t.Errorf("Undecodable = %d after one refused record, want 1", got)
	}
	h := b.Health()
	if h.State != mq.HealthDegraded || mq.CodeOf(h.Err) != mq.CodeCorruptMessage {
		t.Errorf("health after a record left on the subscription is %v (%v, code %v), want Degraded with CodeCorruptMessage", h.State, h.Err, mq.CodeOf(h.Err))
	}
	// The report names where the record was refused, from the delivery's
	// own metadata.
	var op *mq.OpError
	if !errors.As(h.Err, &op) || op.Topic != "t" || op.Subscription != "s" {
		t.Errorf("health reports %v, want an error naming topic t and subscription s", h.Err)
	}
	if err := b.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if h := b.Health(); h.State != mq.HealthReady || h.Err != nil {
		t.Errorf("health after a successful Ping is %v (%v), want Ready", h.State, h.Err)
	}

	// The refused record was left for redelivery, not destroyed: once its
	// lease has run out, a consumer that reads native bodies receives it.
	if err := b.Advance(2 * time.Second); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	native, err := client.Consumer(t.Context(), mq.ConsumerConfig{Topic: "t", Subscription: "s", Metadata: mq.MetadataNativeStrict})
	if err != nil {
		t.Fatalf("native Consumer: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 2)
	n, err := native.Receive(ctx, dst)
	if err != nil || n != 1 || string(dst[0].Body) != "not an envelope" {
		t.Fatalf("after the lease ran out the subscription gave %d record(s) (%v), want the one that is not an envelope", n, err)
	}
}

// TestRunKeepsGoingPastARecordItDeadLettered holds that a refused record the
// source moved to its dead-letter route does not end the Run either: it has
// been dealt with, and the Run goes on to the record behind it.
func TestRunKeepsGoingPastARecordItDeadLettered(t *testing.T) {
	b, client := openEnvelopeToy(t,
		toydriver.TopicSpec{Name: "t.dlq", Subscriptions: []string{"audit"}},
		toydriver.TopicSpec{Name: "t", Subscriptions: []string{"s"}, DeadLetterTopic: "t.dlq"},
	)
	runUntilGood(t, client)

	// Counted, and health left alone: the record is where an operator looks
	// for records that failed.
	if got := b.Undecodable(); got != 1 {
		t.Errorf("Undecodable = %d after one dead-lettered record, want 1", got)
	}
	if h := b.Health(); h.State != mq.HealthReady {
		t.Errorf("health after a record was dead-lettered is %v (%v), want Ready", h.State, h.Err)
	}

	audit, err := client.Consumer(t.Context(), mq.ConsumerConfig{Topic: "t.dlq", Subscription: "audit"})
	if err != nil {
		t.Fatalf("audit Consumer: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 2)
	n, err := audit.Receive(ctx, dst)
	if err != nil || n != 1 || string(dst[0].Body) != "not an envelope" {
		t.Fatalf("the dead-letter topic gave %d record(s) (%v), want the one that is not an envelope", n, err)
	}
}
