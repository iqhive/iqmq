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

// refusingBackend is a memory broker whose destination topic refuses every
// publication with a definite error (refuse), or takes it and then cannot
// confirm it (unconfirmed): the destination failures transfer returns a
// delivery to the source for.
type refusingBackend struct {
	mq.Backend
	refuse, unconfirmed error
}

func (b *refusingBackend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	s, err := b.Backend.OpenSink(ctx, cfg)
	if err != nil || cfg.Topic != destTopic {
		return s, err
	}
	return refusingSink{Sink: s, refuse: b.refuse, unconfirmed: b.unconfirmed}, nil
}

type refusingSink struct {
	mq.Sink
	refuse, unconfirmed error
}

func (s refusingSink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if s.refuse != nil {
		return s.refuse
	}
	return s.Sink.Publish(ctx, m, o)
}

// Flush makes the sink an mq.FlushSink, so the publisher's Flush asks it.
func (s refusingSink) Flush(context.Context) error { return s.unconfirmed }

// TestTransferReturnsTheRestOfAFailedBatch is tt6-10. transfer receives up to
// n deliveries in one Receive, and when the publication of one failed it
// nacked that one and returned, so the deliveries after it in the batch were
// neither published nor settled. They sat leased until their lease expired,
// which is what the README says a bridge must never do, and what the
// `iqmq bridge` command does not do: they go back to the source too.
func TestTransferReturnsTheRestOfAFailedBatch(t *testing.T) {
	for _, c := range []struct {
		name string
		be   func(mq.Backend) mq.Backend
	}{
		{"the destination refuses the publication", func(b mq.Backend) mq.Backend {
			return &refusingBackend{Backend: b, refuse: &mq.OpError{
				Operation: mq.OpPublish, Topic: destTopic, Code: mq.CodeUnavailable,
				Err: errors.New("the destination refused the message"),
			}}
		}},
		{"the destination cannot confirm it", func(b mq.Backend) mq.Backend {
			return &refusingBackend{Backend: b, unconfirmed: &mq.OpError{
				Operation: mq.OpPublish, Topic: destTopic, Code: mq.CodeUnavailable,
				Err: errors.New("the destination could not confirm the message"),
			}}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := t.Context()
			cl, source := memory.NewForTest(t, memory.TestTopics("orders"))
			con, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: sourceSub, Prefetch: 8})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = con.Close() })
			for _, body := range []string{"order-1", "order-2", "order-3"} {
				if err := cl.Publish(ctx, "orders", &mq.Message{Body: []byte(body)}); err != nil {
					t.Fatal(err)
				}
			}
			broker, err := memory.NewBroker(memory.Config{AutoCreate: true})
			if err != nil {
				t.Fatal(err)
			}
			dst := mq.NewClient(c.be(broker))
			t.Cleanup(func() { _ = dst.Close() })

			moved, err := transfer(ctx, con, destination(t, dst), 3, true)
			if moved != 0 || err == nil {
				t.Fatalf("transfer = %d, %v; want 0 and the destination's failure", moved, err)
			}
			snap, err := source.Inspect("orders", sourceSub)
			if err != nil {
				t.Fatal(err)
			}
			if snap.InFlight != 0 || snap.Ready != 3 || snap.Acked != 0 {
				t.Errorf("the source after a failed transfer: ready=%d in-flight=%d nacked=%d acked=%d; "+
					"want all 3 ready again and none leased: a delivery of the failed batch left leased "+
					"waits out its lease instead of going back to the source",
					snap.Ready, snap.InFlight, snap.Nacked, snap.Acked)
			}
		})
	}
}
