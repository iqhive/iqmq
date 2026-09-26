package iqmq_test

import (
	"context"
	"strconv"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// BenchmarkPublish is the layer-1 name for the publication paths:
// BenchmarkPublish/{bound,percall,cached}/{sync,async,owned}, plus the
// compiled and batched arms the specification adds to the same tree.
//
// The first axis is the publisher forms. `bound` is the recommended
// one — a handle bound to its destination, which is the row's winner — and
// `cached` is Client.Publish, whose per-topic publisher cache is the
// convenience path the specification describes. `percall` opens a publisher, publishes
// through it and closes it, which is the form the row keeps as a profile;
// it is measured so that the cost of the shape a caller might reach for by
// accident is on the record and not a guess.
//
// The driver under all of them discards and allocates nothing per message, so
// what is measured is the core's own cost. The single-message budget is a
// statement about the overhead over a raw client, and this is the half of it
// iqmq owns.
func BenchmarkPublish(b *testing.B) {
	ctx := context.Background()
	m := &mq.Message{Topic: "orders", Key: []byte("cust-42"), Body: []byte("body")}

	b.Run("bound", func(b *testing.B) {
		b.Run("sync", func(b *testing.B) {
			_, p := benchPublisher(b, openAllocsPlain, mq.PublisherConfig{})
			b.ReportAllocs()
			for b.Loop() {
				if err := p.Publish(ctx, m); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("sync/options", func(b *testing.B) {
			_, p := benchPublisher(b, openAllocsPlain, mq.PublisherConfig{})
			b.ReportAllocs()
			for b.Loop() {
				if err := p.Publish(ctx, m, mq.Delay(benchDelay), mq.TTL(benchTTL)); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("compiled", func(b *testing.B) {
			_, p := benchPublisher(b, openAllocsPlain, mq.PublisherConfig{})
			set, err := p.CompileOptions(mq.PublishOptions{Delay: benchDelay, TTL: benchTTL})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if err := p.PublishCompiled(ctx, m, set); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("async", func(b *testing.B) {
			_, p := benchPublisher(b, openAllocsAsync, mq.PublisherConfig{})
			cb := func(mq.PublishResult) {}
			b.ReportAllocs()
			for b.Loop() {
				if err := p.PublishAsync(ctx, m, cb); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("owned", func(b *testing.B) {
			_, p := benchPublisher(b, openAllocsOwned, mq.PublisherConfig{})
			release := func(*mq.Message) {}
			b.ReportAllocs()
			for b.Loop() {
				if err := p.PublishOwned(ctx, mq.OwnedMessage{Message: m, Release: release}); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("batched", func(b *testing.B) {
			for _, n := range []int{16, 128} {
				b.Run("size="+strconv.Itoa(n), func(b *testing.B) {
					_, p := benchPublisher(b, openAllocsBatch, mq.PublisherConfig{
						Batching: mq.BatchingConfig{MaxMessages: n},
					})
					cb := func(mq.PublishResult) {}
					b.ReportAllocs()
					for b.Loop() {
						if err := p.PublishAsync(ctx, m, cb); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					if err := p.Flush(ctx); err != nil {
						b.Fatal(err)
					}
				})
			}
		})
		b.Run("batch", func(b *testing.B) {
			// The explicit batch API, for the same sizes: what a caller pays
			// per message when it hands the core a whole batch.
			for _, n := range []int{16, 128} {
				b.Run("size="+strconv.Itoa(n), func(b *testing.B) {
					_, p := benchPublisher(b, openAllocsBatch, mq.PublisherConfig{})
					ms := make([]*mq.Message, n)
					for i := range ms {
						ms[i] = m
					}
					b.ReportAllocs()
					b.SetBytes(int64(n * len(m.Body)))
					for b.Loop() {
						if err := p.PublishBatch(ctx, ms); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
		b.Run("items", func(b *testing.B) {
			for _, n := range []int{16, 128} {
				b.Run("size="+strconv.Itoa(n), func(b *testing.B) {
					_, p := benchPublisher(b, openAllocsBatch, mq.PublisherConfig{})
					items := make([]mq.PublishItem, n)
					for i := range items {
						items[i] = mq.PublishItem{Message: m}
					}
					b.ReportAllocs()
					for b.Loop() {
						if err := p.PublishItems(ctx, items); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	})

	b.Run("cached", func(b *testing.B) {
		b.Run("sync", func(b *testing.B) {
			cl := benchClient(b, openAllocsPlain)
			b.ReportAllocs()
			for b.Loop() {
				if err := cl.Publish(ctx, "orders", m); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("percall", func(b *testing.B) {
		// A publisher per publication: the form that resolves its
		// destination, asserts the driver's optional interfaces and opens a
		// sink for every message. It is here to be compared against, not
		// recommended.
		b.Run("sync", func(b *testing.B) {
			cl := benchClient(b, openAllocsPlain)
			b.ReportAllocs()
			for b.Loop() {
				p, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
				if err != nil {
					b.Fatal(err)
				}
				if err := p.Publish(ctx, m); err != nil {
					b.Fatal(err)
				}
				if err := p.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

// benchClient opens a client over the allocation harness.
func benchClient(b *testing.B, open func(*advBroker, mq.SinkConfig) mq.Sink) *mq.Client {
	b.Helper()
	cl := mq.NewClient(newAdvBroker(open))
	b.Cleanup(func() {
		if err := cl.Close(); err != nil {
			b.Errorf("closing the client: %v", err)
		}
	})
	return cl
}

// benchPublisher opens a client and a bound publisher over the harness.
func benchPublisher(b *testing.B, open func(*advBroker, mq.SinkConfig) mq.Sink, cfg mq.PublisherConfig) (*advBroker, *mq.PublisherHandle) {
	b.Helper()
	return allocsAdvPublisher(b, open, cfg)
}
