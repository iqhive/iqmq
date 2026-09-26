package v1compat_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestPubSubClientKindsList announces the kinds, which is what
// and `bakeoff lint` read to know what to measure
func TestPubSubClientKindsList(t *testing.T) {
	arms := a34Arms()
	parts := make([]string, 0, len(arms))
	for _, a := range arms {
		parts = append(parts, a.slug+"="+a.ident)
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
	if len(arms) < 2 {
		t.Fatalf("The specification has %d arm(s); a bake-off needs two", len(arms))
	}
}

// a34Size is one payload size of the sweep.
type a34Size struct {
	name  string
	bytes int
}

// a34Sizes are the three payload sizes the family is measured at. They are the specification, because the two families answer the same question about two
// different services and a reader comparing them should not have to convert.
func a34Sizes() []a34Size {
	return []a34Size{{"64B", 64}, {"1KiB", 1024}, {"64KiB", 64 * 1024}}
}

// a34BatchSize is how many messages the batch operation publishes at once.
const a34BatchSize = 100

// BenchmarkPubSubClient is.
//
//	BenchmarkPubSubClient/{v2,v1compat}/{publish,batch,consume,endtoend}/{64B,1KiB,64KiB}
func BenchmarkPubSubClient(b *testing.B) {
	conn := service(b)
	for _, arm := range a34Arms() {
		b.Run(arm.slug, func(b *testing.B) {
			// Two clients per arm and not one per leaf: opening a client
			// builds a gRPC stack, and 24 rows times the campaign's ten
			// counts would pay for 240 of them. The bundler's count
			// threshold is the only configuration that differs between the
			// operations.
			single := arm.open(b, conn, armConfig(1))
			batched := arm.open(b, conn, armConfig(a34BatchSize))
			for _, op := range []string{"publish", "batch", "consume", "endtoend"} {
				b.Run(op, func(b *testing.B) {
					for _, size := range a34Sizes() {
						b.Run(size.name, func(b *testing.B) {
							c := single
							if op == "batch" {
								c = batched
							}
							a34Leaf(b, c, op, size)
						})
					}
				})
			}
		})
	}
}

// a34Leaf measures one operation at one payload size.
func a34Leaf(b *testing.B, c *mq.Client, op string, size a34Size) {
	b.Helper()
	topic, sub := newName("bench"), "bench"
	provision(b, c, topic, sub)
	ctx := context.Background()
	pub, err := c.Publisher(ctx, mq.PublisherConfig{Topic: topic})
	if err != nil {
		b.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	body := make([]byte, size.bytes)
	for i := range body {
		body[i] = byte(i)
	}
	msg := func(i int) *mq.Message {
		return &mq.Message{Key: []byte("k" + strconv.Itoa(i&63)), Body: body}
	}

	switch op {
	case "publish":
		b.SetBytes(int64(size.bytes))
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			if err := pub.Publish(ctx, msg(i)); err != nil {
				b.Fatalf("Publish: %v", err)
			}
		}
	case "batch":
		batch := make([]*mq.Message, a34BatchSize)
		for i := range batch {
			batch[i] = msg(i)
		}
		b.SetBytes(int64(size.bytes) * a34BatchSize)
		b.ResetTimer()
		for b.Loop() {
			if err := pub.PublishBatch(ctx, batch); err != nil {
				b.Fatalf("PublishBatch: %v", err)
			}
		}
	case "consume":
		cons, cerr := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: sub, Prefetch: 1024})
		if cerr != nil {
			b.Fatalf("Consumer: %v", cerr)
		}
		defer func() { _ = cons.Close() }()
		const prefill = 128
		dst := make([]mq.Delivery, 1)
		fill := func(n int) {
			for i := range n {
				if err := pub.Publish(ctx, msg(i)); err != nil {
					b.Fatalf("prefill: %v", err)
				}
			}
		}
		fill(prefill)
		left := prefill
		b.SetBytes(int64(size.bytes))
		b.ResetTimer()
		for b.Loop() {
			if left == 0 {
				b.StopTimer()
				fill(prefill)
				left = prefill
				b.StartTimer()
			}
			n, rerr := cons.Receive(ctx, dst)
			if n == 0 {
				b.Fatalf("Receive returned nothing: %v", rerr)
			}
			if err := dst[0].Ack(); err != nil {
				b.Fatalf("Ack: %v", err)
			}
			left--
		}
		b.StopTimer()
	case "endtoend":
		cons, cerr := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: sub, Prefetch: 1024})
		if cerr != nil {
			b.Fatalf("Consumer: %v", cerr)
		}
		defer func() { _ = cons.Close() }()
		dst := make([]mq.Delivery, 1)
		b.SetBytes(int64(size.bytes))
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			if err := pub.Publish(ctx, msg(i)); err != nil {
				b.Fatalf("Publish: %v", err)
			}
			n, rerr := cons.Receive(ctx, dst)
			if n == 0 {
				b.Fatalf("Receive returned nothing: %v", rerr)
			}
			if err := dst[0].Ack(); err != nil {
				b.Fatalf("Ack: %v", err)
			}
		}
		b.StopTimer()
	default:
		b.Fatalf("unknown operation %q", op)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}
