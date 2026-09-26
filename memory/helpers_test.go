package memory_test

import (
	"context"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// The helpers in this file reach the SPI directly, through the broker's own
// mq.Backend surface, for the properties the core's Delivery does not carry:
// the partition, the sequence, the lease expiry and the resolved topic all
// live in DriverDelivery.Info, which is the side this increment fills in.
// A test that wants a Delivery uses the client instead.

// publish sends one message through a sink opened on the broker.
func publish(t *testing.T, ctx context.Context, b *memory.Broker, topic string, m *mq.Message, opts ...mq.PublishOption) {
	t.Helper()
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: topic})
	if err != nil {
		t.Fatalf("OpenSink(%q): %v", topic, err)
	}
	defer func() { _ = sink.Close() }()
	var o mq.PublishOptions
	if err := o.Apply(opts...); err != nil {
		t.Fatalf("options: %v", err)
	}
	if err := sink.Publish(ctx, m, &o); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// openSource opens a source on the broker with the core's own resolved
// defaults, and closes it on cleanup.
func openSource(t *testing.T, ctx context.Context, b *memory.Broker, topic, sub string) mq.Source {
	t.Helper()
	src, err := b.OpenSource(ctx, mq.SourceConfig{
		Topic:        topic,
		Subscription: sub,
		Prefetch:     mq.DefaultPrefetch,
		Start:        mq.StartLatest(),
	})
	if err != nil {
		t.Fatalf("OpenSource(%q,%q): %v", topic, sub, err)
	}
	t.Cleanup(func() { _ = src.Close() })
	return src
}

// receiveViaSPI opens a source, runs publish, and fetches exactly want
// deliveries out of it. The source is opened first so that the subscription
// exists before anything is published: the queue and topic topologies retain
// nothing.
func receiveViaSPI(
	t *testing.T,
	ctx context.Context,
	b *memory.Broker,
	topic, sub string,
	publish func(),
	want int,
) []mq.DriverDelivery {
	t.Helper()
	src := openSource(t, ctx, b, topic, sub)
	publish()
	out := make([]mq.DriverDelivery, 0, want)
	buf := make([]mq.DriverDelivery, want)
	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for len(out) < want {
		n, err := src.Fetch(deadline, buf[:want-len(out)])
		if err != nil {
			t.Fatalf("Fetch after %d of %d deliveries: %v", len(out), want, err)
		}
		out = append(out, buf[:n]...)
	}
	return out
}

// receiveN receives exactly n deliveries through a consumer handle, waiting
// for them, and returns the slice they live in.
func receiveN(t *testing.T, ctx context.Context, c *mq.ConsumerHandle, n int) []mq.Delivery {
	t.Helper()
	out := make([]mq.Delivery, 0, n)
	buf := make([]mq.Delivery, n)
	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for len(out) < n {
		got, err := c.Receive(deadline, buf[:n-len(out)])
		if err != nil {
			t.Fatalf("Receive after %d of %d deliveries: %v", len(out), n, err)
		}
		out = append(out, buf[:got]...)
	}
	return out
}
