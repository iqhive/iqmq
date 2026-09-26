package testkit

import (
	"context"
	"strconv"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// RunBenchmarks runs the driver-layer benchmarks against a harness.
//
// They are the layer-3 benchmark — a driver against a real
// broker — and they exist so that every driver is measured the same way rather
// than each inventing its own loop. The names follow:
//
//	BenchmarkDriver/<driver>/Publish
//	BenchmarkDriver/<driver>/PubSubAck
//	BenchmarkDriver/<driver>/PublishBatch/size=N
//
// A driver calls it from a benchmark of its own, behind the same environment
// gate its conformance test uses, so that a run with no broker does not try to
// measure one.
func RunBenchmarks(b *testing.B, h Harness) {
	b.Helper()
	if err := h.validate(); err != nil {
		b.Fatalf("testkit: %v", err)
	}
	name := h.Driver
	if h.Profile != "" {
		name += "/" + h.Profile
	}
	b.Run(name+"/Publish", func(b *testing.B) { benchPublish(b, h) })
	b.Run(name+"/PubSubAck", func(b *testing.B) { benchPubSubAck(b, h) })
	for _, size := range []int{10, 100} {
		b.Run(name+"/PublishBatch/size="+strconv.Itoa(size), func(b *testing.B) { benchBatch(b, h, size) })
	}
}

// benchEnv opens a client for one benchmark.
func benchEnv(b *testing.B, h Harness, purpose string) (*mq.Client, Need) {
	b.Helper()
	need := Need{Topic: h.topicName(purpose), Subscriptions: []string{"bench"}}
	client, err := h.Open(context.Background(), need)
	if err != nil {
		b.Skipf("testkit: opening a client: %v", err)
	}
	b.Cleanup(func() { _ = client.Close() })
	return client, need
}

// benchPublish measures one publication through a bound publisher.
func benchPublish(b *testing.B, h Harness) {
	client, need := benchEnv(b, h, "bench-publish")
	p, err := client.Publisher(context.Background(), mq.PublisherConfig{Topic: need.Topic})
	if err != nil {
		b.Skipf("testkit: opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	ctx := context.Background()
	m := &mq.Message{Body: make([]byte, 256)}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := p.Publish(ctx, m); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

// benchPubSubAck measures the whole cycle: publish, receive, acknowledge.
func benchPubSubAck(b *testing.B, h Harness) {
	client, need := benchEnv(b, h, "bench-pubsuback")
	ctx := context.Background()
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: need.Topic})
	if err != nil {
		b.Skipf("testkit: opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	c, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic: need.Topic, Subscription: need.Sub(0), Start: mq.StartEarliest(),
	})
	if err != nil {
		b.Skipf("testkit: opening a consumer: %v", err)
	}
	defer func() { _ = c.Close() }()
	m := &mq.Message{Body: make([]byte, 256)}
	dst := make([]mq.Delivery, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := p.Publish(ctx, m); err != nil {
			b.Fatalf("Publish: %v", err)
		}
		n, err := c.Receive(ctx, dst)
		if err != nil || n != 1 {
			b.Fatalf("Receive: %d, %v", n, err)
		}
		if err := dst[0].Ack(); err != nil {
			b.Fatalf("Ack: %v", err)
		}
	}
}

// benchBatch measures a native batch publication.
func benchBatch(b *testing.B, h Harness, size int) {
	client, need := benchEnv(b, h, "bench-batch")
	ctx := context.Background()
	if !client.Capabilities().Has(mq.FeatureBatchPublish) {
		b.Skip("testkit: the driver does not advertise batch publication")
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: need.Topic})
	if err != nil {
		b.Skipf("testkit: opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	ms := make([]*mq.Message, size)
	for i := range ms {
		ms[i] = &mq.Message{Body: make([]byte, 256)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := p.PublishBatch(ctx, ms); err != nil {
			b.Fatalf("PublishBatch: %v", err)
		}
	}
}
