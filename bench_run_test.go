package iqmq_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// BenchmarkRun/Dispatch is the name for the runner's own hot path and the specification's "runner dispatch (fetch→lane→handler) ≤ 300 ns/msg, 0 allocs steady
// state".
//
// What it measures is everything the core does per message between a driver
// handing a delivery over and the driver being told the outcome: the fetch
// buffer, the admission slot, the lane routing, the lane queue, the worker
// hand-off, the handler call and the settlement. The driver below is as close
// to nothing as a driver can be — one preallocated message, no locks, no
// allocation, an acknowledgement that returns — so what is left is the core's.
//
// The dimensions are the specification: the worker count, whether the lease-derived
// handler context is asked for (the one thing on this path that allocates,
// which is why it is not a default), and whether the handler is a Handler or a
// BatchHandler. The row is the lease=off arm, which is the default
// configuration and the path the budget's words describe.
func BenchmarkRun(b *testing.B) {
	b.Run("Dispatch", func(b *testing.B) {
		for _, workers := range []int{1, 4} {
			for _, lease := range []bool{false, true} {
				for _, batch := range []bool{false, true} {
					if batch && lease {
						// The batch shape's deadline is the leader's, so the
						// lease arm says the same thing it says for a single
						// delivery; one of the two is enough.
						continue
					}
					name := "workers=" + strconv.Itoa(workers) +
						"/lease=" + onOff(lease) + "/shape=" + shapeName(batch)
					b.Run(name, func(b *testing.B) { benchRunDispatch(b, workers, lease, batch) })
				}
			}
		}
	})
}

func onOff(v bool) string {
	if v {
		return "on"
	}
	return "off"
}

func shapeName(batch bool) string {
	if batch {
		return "batch"
	}
	return "single"
}

// benchRunDispatch runs b.N messages through one runner and reports the cost
// per message.
func benchRunDispatch(b *testing.B, workers int, lease, batch bool) {
	src := newBenchSource(lease)
	cl := mq.NewClient(&benchBackend{src: src})
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{
		Topic:        "orders",
		Subscription: "billing",
		Prefetch:     128,
		// the reuse mode, which is the configuration the specification's
		// "0 with reuse" is written about: without it the consumer allocates
		// one settlement record per delivery by design (ADR-009).
		BufferReuse: true,
	})
	if err != nil {
		b.Fatalf("opening the consumer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	target := int64(b.N)
	src.left.Store(target)
	var seen atomic.Int64
	done := make(chan struct{})
	count := func() {
		if seen.Add(1) == target {
			close(done)
		}
	}
	opts := []mq.RunOption{mq.WithConcurrency(workers), mq.WithOrdering(mq.OrderingUnordered)}
	if lease {
		// the derived handler context, which is off by default because
		// it breaches the allocation budget. This
		// arm is what that costs.
		opts = append(opts, mq.WithLeaseDeadline())
	}
	ran := make(chan error, 1)

	b.ReportAllocs()
	b.ResetTimer()
	go func() {
		if batch {
			ran <- c.RunBatch(ctx, benchBatchHandler{count: count}, opts...)
			return
		}
		ran <- c.Run(ctx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			count()
			return nil
		}), opts...)
	}()
	select {
	case <-done:
	case err := <-ran:
		b.Fatalf("the runner stopped early: %v", err)
	}
	b.StopTimer()

	cancel()
	<-ran
	if err := cl.Close(); err != nil {
		b.Fatalf("closing the client: %v", err)
	}
}

// benchBatchHandler is the batch shape of the same no-op handler.
type benchBatchHandler struct{ count func() }

// HandleBatch implements mq.BatchHandler.
func (h benchBatchHandler) HandleBatch(_ context.Context, ds []*mq.Delivery) error {
	for range ds {
		h.count()
	}
	return nil
}

// benchBackend is the smallest Backend the SPI allows: one source, one
// settler, and nothing behind either.
type benchBackend struct{ src *benchSource }

func (b *benchBackend) Info() mq.DriverInfo { return mq.DriverInfo{Name: "bench", DriverVersion: "v0"} }

func (b *benchBackend) Capabilities() mq.Capabilities {
	var caps mq.Capabilities
	for _, f := range []mq.Feature{mq.FeatureAtLeastOnce, mq.FeatureExplicitAck, mq.FeatureNack} {
		if err := caps.Set(f, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic}); err != nil {
			panic(err)
		}
	}
	return caps
}

func (b *benchBackend) OpenSink(context.Context, mq.SinkConfig) (mq.Sink, error) {
	return nil, mq.ErrUnsupported
}

func (b *benchBackend) OpenSource(context.Context, mq.SourceConfig) (mq.Source, error) {
	return b.src, nil
}

func (b *benchBackend) Ping(context.Context) error { return nil }
func (b *benchBackend) Native() any                { return nil }
func (b *benchBackend) Close() error               { return nil }

// benchSource hands out a preallocated message until its budget is spent and
// then parks, which is what a driver does when its subscription is empty.
//
// It takes no lock and allocates nothing: every delivery it fills in is the
// same message and the same metadata, so what the benchmark measures is the
// core's per-message cost and not the driver's.
type benchSource struct {
	left  atomic.Int64
	tok   atomic.Uint64
	lease bool
	msg   mq.Message
	gone  chan struct{}
	once  sync.Once
}

// newBenchSource builds a source that hands out one 64-byte message.
func newBenchSource(lease bool) *benchSource {
	return &benchSource{
		lease: lease,
		msg:   mq.Message{ID: "1", Body: make([]byte, 64)},
		gone:  make(chan struct{}),
	}
}

// Fetch implements mq.Source.
func (s *benchSource) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	var expires time.Time
	if s.lease {
		expires = time.Now().Add(time.Hour)
	}
	for {
		n := len(dst)
		for i := range n {
			if s.left.Add(-1) < 0 {
				s.left.Add(1)
				n = i
				break
			}
			dst[i] = mq.DriverDelivery{
				Message: &s.msg,
				Info: mq.DeliveryInfo{
					Topic:        "orders",
					Subscription: "billing",
					Attempt:      1,
					LeaseExpires: expires,
				},
				Token: s.tok.Add(1),
				Gen:   1,
			}
		}
		if n > 0 {
			return n, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-s.gone:
			return 0, mq.ErrClosed
		}
	}
}

// Settler implements mq.Source.
func (s *benchSource) Settler() mq.Settler { return benchSettler{} }

// Close implements mq.Source.
func (s *benchSource) Close() error {
	s.once.Do(func() { close(s.gone) })
	return nil
}

// benchSettler accepts everything without doing anything, so the settlement's
// cost in the numbers above is the core's state machine and not a broker.
type benchSettler struct{}

func (benchSettler) Ack(context.Context, *mq.DriverDelivery) error { return nil }

func (benchSettler) Nack(context.Context, *mq.DriverDelivery, mq.NackOptions) error { return nil }
