package rabbitmq_test

import (
	"strconv"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"bitbucket.org/iqhive/iqmq/internal/rabbit/confirm"
)

// It is not the family's own measurement. `internal/rabbit/confirm`'s
// `BenchmarkRabbitConfirm` measures the strategies against a reference
// simulation with a virtual round trip, which is what the bake-off decides the
// family on; this measures the DRIVER's own code on the same path — the
// encoding, the reusable header table, the tracker, the waiter, the pending
// free list — against an in-process broker that answers immediately. So the
// round trip is nearly zero here and the number is the driver's overhead
// rather than the strategy's benefit, which is exactly the part the family's
// own simulation cannot see.
//
//	BenchmarkRabbitConfirm/<kind>/batch=<1|16>
//
// The broker is the scripted fake rather than a container, and the driver page
// says so: a container would put a real broker's own work in front of every
// measurement, and that work is identical for the three arms, so it is pure
// noise added to exactly the quantity being compared.
func BenchmarkRabbitConfirm(b *testing.B) {
	for _, kind := range []confirm.Kind{confirm.PerMessage, confirm.Windowed, confirm.Async} {
		b.Run(kind.String(), func(b *testing.B) {
			for _, batch := range []int{1, 16} {
				b.Run("batch="+strconv.Itoa(batch), func(b *testing.B) {
					benchConfirm(b, kind, batch)
				})
			}
		})
	}
}

// benchConfirm drives one strategy at one batch width.
func benchConfirm(b *testing.B, kind confirm.Kind, batch int) {
	broker := rabbitfake.New(rabbitfake.Config{})
	b.Cleanup(func() { _ = broker.Close() })
	cfg := rq.Config{Addrs: []string{"localhost:5672"}, Declare: true, ConfirmKind: kind}
	if kind != confirm.PerMessage {
		cfg.ConfirmWindow = 64
	}
	client, err := rq.OpenWithDialer(b.Context(), broker.Dialer(), cfg)
	if err != nil {
		b.Fatalf("opening a client: %v", err)
	}
	b.Cleanup(func() { _ = client.Close() })
	// A consumer so that the publications are routable: an unroutable
	// `mandatory` publication is returned, which is a different path and a
	// different cost.
	cons, err := client.Consumer(b.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "bench", Prefetch: 1024})
	if err != nil {
		b.Fatalf("opening a consumer: %v", err)
	}
	b.Cleanup(func() { _ = cons.Close() })
	pub, err := client.Publisher(b.Context(), mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		b.Fatalf("opening a publisher: %v", err)
	}
	b.Cleanup(func() { _ = pub.Close() })

	msgs := make([]*mq.Message, batch)
	for i := range msgs {
		msgs[i] = &mq.Message{Body: make([]byte, 256), Key: []byte("order-" + strconv.Itoa(i))}
	}
	// The queue is drained as the benchmark runs, or the fake's own arrival
	// log grows without bound and the measurement becomes a measurement of
	// that. The consumer's prefetch is wide, so the drain is one call.
	dst := make([]mq.Delivery, 1024)

	b.ReportAllocs()
	published := 0
	for b.Loop() {
		if batch == 1 {
			if err := pub.Publish(b.Context(), msgs[0]); err != nil {
				b.Fatalf("publishing: %v", err)
			}
		} else if err := pub.PublishBatch(b.Context(), msgs); err != nil {
			b.Fatalf("publishing a batch: %v", err)
		}
		published += batch
		if published >= 512 {
			drain(b, cons, dst)
			published = 0
		}
	}
	b.StopTimer()
	drain(b, cons, dst)
}

// drain settles whatever is waiting, so that the broker's own bookkeeping does
// not grow into the measurement.
func drain(b *testing.B, cons *mq.ConsumerHandle, dst []mq.Delivery) {
	b.Helper()
	for {
		ctx, cancel := benchContext(b)
		n, err := cons.Receive(ctx, dst)
		cancel()
		if err != nil || n == 0 {
			return
		}
		for i := range n {
			if err := dst[i].Ack(); err != nil {
				b.Fatalf("acknowledging: %v", err)
			}
		}
		if n < len(dst) {
			return
		}
	}
}
