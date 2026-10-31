package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// deliveredBackend is a memory broker whose destination topic delivers every
// publication and then answers it with err: a broker that took the message
// and could not say so.
type deliveredBackend struct {
	mq.Backend
	err error
}

func (b *deliveredBackend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	s, err := b.Backend.OpenSink(ctx, cfg)
	if err != nil || cfg.Topic != destTopic {
		return s, err
	}
	return deliveredSink{Sink: s, err: b.err}, nil
}

type deliveredSink struct {
	mq.Sink
	err error
}

func (s deliveredSink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if err := s.Sink.Publish(ctx, m, o); err != nil {
		return err
	}
	return s.err
}

// memorySource is a memory broker holding one message on "orders", and a
// consumer of it.
func memorySource(t *testing.T) (*memory.Broker, *mq.ConsumerHandle) {
	t.Helper()
	cl, b := memory.NewForTest(t, memory.TestTopics("orders"))
	con, err := cl.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: sourceSub})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = con.Close() })
	if err := cl.Publish(t.Context(), "orders", &mq.Message{Body: []byte("order-1")}); err != nil {
		t.Fatal(err)
	}
	return b, con
}

// destination opens a publisher for destTopic on cl.
func destination(t *testing.T, cl *mq.Client) *mq.PublisherHandle {
	t.Helper()
	pub, err := cl.Publisher(t.Context(), mq.PublisherConfig{Topic: destTopic})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	return pub
}

// TestTransferSettlesAPartialFanout: a destination that took the message for
// some of its subscriptions and refused it for the others (mq.ErrPartial) has
// published it. transfer used to say "Nothing reached the destination", nack
// the source delivery and stop, so the redelivery put a second copy on every
// subscription that had the first.
func TestTransferSettlesAPartialFanout(t *testing.T) {
	ctx := t.Context()
	source, con := memorySource(t)
	dst, _ := memory.NewForTest(t, memory.TestConfig(memory.Config{
		DefaultTopology: memory.TopologyConfig{
			CapacityMessages: 1,
			Overflow:         memory.OverflowReject,
			Fanout:           memory.FanoutIndependent,
		},
	}), memory.TestTopics(destTopic))
	full, err := dst.Consumer(ctx, mq.ConsumerConfig{Topic: destTopic, Subscription: "full"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = full.Close() })
	// Only "full" exists yet, so this one record fills it.
	if err := dst.Publish(ctx, destTopic, &mq.Message{Body: []byte("filler")}); err != nil {
		t.Fatal(err)
	}
	reader, err := dst.Consumer(ctx, mq.ConsumerConfig{Topic: destTopic, Subscription: destSub})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	moved, err := transfer(ctx, con, destination(t, dst), 1, true)
	if err != nil || moved != 1 {
		t.Fatalf("transfer = %d, %v; want the message moved: the destination's %q subscription has it", moved, err, destSub)
	}
	snap, err := source.Inspect("orders", sourceSub)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Acked != 1 {
		t.Fatalf("the source acked %d deliveries, want 1: a returned delivery is published again", snap.Acked)
	}
}

// TestTransferSaysAnAmbiguousPublicationMayHaveArrived: a publication whose
// outcome is unknown may be in the destination. transfer returns the delivery
// to the source, which is right, and used to say that nothing had reached the
// destination, which is not: the redelivery may be a duplicate there.
func TestTransferSaysAnAmbiguousPublicationMayHaveArrived(t *testing.T) {
	source, con := memorySource(t)
	broker, err := memory.NewBroker(memory.Config{AutoCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	ambiguous := &mq.OpError{
		Operation: mq.OpPublish, Topic: destTopic, Code: mq.CodeUnavailable, Ambiguous: true,
		Err: errors.New("the connection closed after the message was written"),
	}
	dst := mq.NewClient(&deliveredBackend{Backend: broker, err: ambiguous})
	t.Cleanup(func() { _ = dst.Close() })

	moved, err := transfer(t.Context(), con, destination(t, dst), 1, true)
	if moved != 0 || !errors.Is(err, ambiguous) || !strings.Contains(err.Error(), "may already hold") {
		t.Fatalf("transfer = %d, %v; want the ambiguous failure reported as one the destination may already hold", moved, err)
	}
	snap, err := source.Inspect("orders", sourceSub)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Acked != 0 {
		t.Fatalf("the source acked %d deliveries, want 0: the delivery goes back", snap.Acked)
	}
}
