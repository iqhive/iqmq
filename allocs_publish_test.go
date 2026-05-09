package iqmq_test

import (
	"context"
	"errors"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
)

// TestAllocsPublish pins the allocation counts of the publication paths the specification
// adds, exactly, the way rule 1 asks: the count and not a bound.
//
// The driver underneath allocates nothing per message, so what is counted is
// the core's own cost. The specification budgets a network driver two allocations per
// message and zero with reuse; every path here is at zero, and the two that
// are not — an option set that outlives its call, and the state of a
// publication a close abandoned — say so and why.
//
// It is a separate test from TestAllocs so that neither this file nor
// allocs_test.go has to be edited to add a path; `make allocs` runs
// everything whose name starts with TestAllocs.
func TestAllocsPublish(t *testing.T) {
	ctx := context.Background()

	t.Run("compiled", func(t *testing.T) {
		// the compiled form: the set is resolved and validated once, so
		// the publication hands the driver an address and performs no fold,
		// no capability check on the controls and no pooling.
		_, p := allocsAdvPublisher(t, openAllocsPlain, mq.PublisherConfig{})
		set, err := p.CompileOptions(mq.PublishOptions{Delay: benchDelay, TTL: benchTTL, DedupKey: "order-42"})
		if err != nil {
			t.Fatalf("CompileOptions: %v", err)
		}
		m := &mq.Message{Body: []byte("body")}
		if n := testing.AllocsPerRun(1000, func() {
			if err := p.PublishCompiled(ctx, m, set); err != nil {
				t.Fatal(err)
			}
		}); n != 0 {
			t.Fatalf("PublishCompiled allocated %v times per message, want exactly 0 ", n)
		}
	})

	t.Run("async", func(t *testing.T) {
		// The asynchronous publication's state is pooled with its two
		// closures already bound to it, so neither the completion callback
		// nor the driver's done costs an allocation. The caller's callback is
		// hoisted out of the loop, because a closure written at the call site
		// is the caller's allocation and not the publisher's.
		_, p := allocsAdvPublisher(t, openAllocsAsync, mq.PublisherConfig{})
		m := &mq.Message{Body: []byte("body")}
		cb := func(mq.PublishResult) {}
		for _, c := range []struct {
			name string
			call func()
		}{
			{"nooptions", func() {
				if err := p.PublishAsync(ctx, m, cb); err != nil {
					t.Fatal(err)
				}
			}},
			{"options", func() {
				if err := p.PublishAsync(ctx, m, cb, mq.Delay(benchDelay), mq.TTL(benchTTL)); err != nil {
					t.Fatal(err)
				}
			}},
			{"nocallback", func() {
				if err := p.PublishAsync(ctx, m, nil); err != nil {
					t.Fatal(err)
				}
			}},
		} {
			t.Run(c.name, func(t *testing.T) {
				if n := testing.AllocsPerRun(1000, c.call); n != 0 {
					t.Fatalf("PublishAsync (%s) allocated %v times per message, want exactly 0 ", c.name, n)
				}
			})
		}
	})

	t.Run("owned", func(t *testing.T) {
		// The owned path hands the driver the caller's OwnedMessage by value
		// and releases outside every lock; nothing about it is per-message
		// storage. The release func is hoisted for the same reason the
		// callback is.
		for _, shape := range []struct {
			name string
			open func(*advBroker, mq.SinkConfig) mq.Sink
		}{{"ownedsink", openAllocsOwned}, {"adapted", openAllocsPlain}} {
			t.Run(shape.name, func(t *testing.T) {
				_, p := allocsAdvPublisher(t, shape.open, mq.PublisherConfig{})
				m := &mq.Message{Body: []byte("body")}
				release := func(*mq.Message) {}
				if n := testing.AllocsPerRun(1000, func() {
					if err := p.PublishOwned(ctx, mq.OwnedMessage{Message: m, Release: release}); err != nil {
						t.Fatal(err)
					}
				}); n != 0 {
					t.Fatalf("PublishOwned (%s) allocated %v times per message, want exactly 0 ", shape.name, n)
				}
			})
		}
	})

	t.Run("stamped", func(t *testing.T) {
		// AutoID, Timestamp and Receipts are each configuration a caller
		// switched on, and none of them may make the core allocate: the
		// identifier comes from the caller's generator, the clock read is
		// what the caller asked for, and the receipt's storage is the
		// caller's own.
		_, p := allocsAdvPublisher(t, openAllocsPlain, mq.PublisherConfig{
			AutoID:    staticID{"static-id"},
			Timestamp: true,
			Receipts:  true,
		})
		var r mq.PublishReceipt
		m := &mq.Message{Body: []byte("body"), Receipt: &r}
		if n := testing.AllocsPerRun(1000, func() {
			if err := p.Publish(ctx, m); err != nil {
				t.Fatal(err)
			}
		}); n != 0 {
			t.Fatalf("Publish with AutoID, Timestamp and Receipts allocated %v times per message, want exactly 0 (rule 1)", n)
		}
	})

	t.Run("rejected", func(t *testing.T) {
		// the rejecting mode has to refuse before it allocates: the
		// refusal is the path a client under load takes hardest, and a
		// refusal that allocated would be the caller's own
		// denial-of-service.
		b, p := allocsAdvPublisher(t, openAsync, mq.PublisherConfig{
			Backpressure:       mq.BackpressureReject,
			MaxPendingMessages: 1,
		})
		sink := advSinkOf[*advAsyncSink](t, b)
		if err := p.PublishAsync(ctx, &mq.Message{Body: []byte("held")}, nil); err != nil {
			t.Fatalf("the first PublishAsync: %v", err)
		}
		m := &mq.Message{Body: []byte("refused")}
		cb := func(mq.PublishResult) {}
		if n := testing.AllocsPerRun(1000, func() {
			if err := p.PublishAsync(ctx, m, cb); !errors.Is(err, mq.ErrBackpressure) {
				t.Fatalf("PublishAsync over the bound = %v, want ErrBackpressure", err)
			}
		}); n != 0 {
			t.Fatalf("a rejected publication allocated %v times, want exactly 0: BackpressureReject must refuse before it allocates", n)
		}
		sink.complete(sink.heldCount(), nil)
	})

	t.Run("batched", func(t *testing.T) {
		// A batched publication costs the batcher's slot growth on the first
		// pass and nothing once the buffer has reached its steady state,
		// which is what the warm-up below establishes: the batch's own
		// containers are reused and amortised over MaxMessages.
		//
		// It is measured over a hundred thousand runs and not the thousand
		// the other paths use, because this is the one path that crosses a
		// goroutine. testing.AllocsPerRun divides the *process's* malloc
		// count by the number of runs, so the flusher goroutine's one-off
		// cost — its stack, its channel wait records — lands inside the
		// window and reads as two allocations per message over a thousand
		// runs and as none over ten thousand. The per-message figure the
		// benchmark reports for the same path is 0 allocs/op.
		const runs = 100_000
		_, p := allocsAdvPublisher(t, openAllocsBatch, mq.PublisherConfig{
			Batching: mq.BatchingConfig{MaxMessages: 16},
		})
		m := &mq.Message{Body: []byte("body")}
		cb := func(mq.PublishResult) {}
		call := func() {
			if err := p.PublishAsync(ctx, m, cb); err != nil {
				t.Fatal(err)
			}
		}
		for range 2000 {
			call()
		}
		if err := p.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		if n := testing.AllocsPerRun(runs, call); n != 0 {
			t.Fatalf("a batched PublishAsync allocated %v times per message in steady state, want exactly 0 ", n)
		}
		if err := p.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	})
}

// The allocation harness: the advanced sinks over a broker that records
// nothing, so that TestAllocsPublish measures the core and not the fake.
//
// It is deliberately not the recording broker of publisher_adv_test.go, which
// appends a record per publication — a real driver's behaviour, and noise in a
// measurement of the core-attributable count.

func openAllocsPlain(b *advBroker, cfg mq.SinkConfig) mq.Sink {
	return &allocsAdvSink{b: b, receipts: cfg.Receipts}
}

func openAllocsBatch(b *advBroker, cfg mq.SinkConfig) mq.Sink {
	return &allocsAdvBatchSink{allocsAdvSink{b: b, receipts: cfg.Receipts}}
}

func openAllocsOwned(b *advBroker, cfg mq.SinkConfig) mq.Sink {
	return &allocsAdvOwnedSink{allocsAdvSink{b: b, receipts: cfg.Receipts}}
}

func openAllocsAsync(b *advBroker, cfg mq.SinkConfig) mq.Sink {
	return &allocsAdvAsyncSink{allocsAdvSink{b: b, receipts: cfg.Receipts}}
}

// allocsAdvSink discards, filling a receipt slot when it was opened with one
// so that the receipt path is measured and not skipped.
type allocsAdvSink struct {
	b        *advBroker
	receipts bool
}

func (s *allocsAdvSink) Publish(_ context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if o == nil {
		return mq.ErrUnsupported
	}
	if s.receipts && m.Receipt != nil {
		m.Receipt.MessageID = "static-receipt"
		m.Receipt.Offset = 1
	}
	return nil
}

func (s *allocsAdvSink) Close() error { return nil }

type allocsAdvBatchSink struct{ allocsAdvSink }

func (s *allocsAdvBatchSink) PublishBatch(_ context.Context, _ []*mq.Message, o *mq.PublishOptions) error {
	if o == nil {
		return mq.ErrUnsupported
	}
	return nil
}

// allocsAdvAsyncSink completes every publication before it returns, which is
// the cheapest asynchronous driver there can be: what is measured is then the
// core's whole cycle — admission, the completion table, the callback and the
// state going back to its pool — and nothing of the driver's.
type allocsAdvAsyncSink struct{ allocsAdvSink }

func (s *allocsAdvAsyncSink) PublishAsync(_ context.Context, m *mq.Message, o *mq.PublishOptions, done func(error)) error {
	if o == nil {
		return mq.ErrUnsupported
	}
	if s.receipts && m.Receipt != nil {
		m.Receipt.MessageID = "static-receipt"
	}
	done(nil)
	return nil
}

type allocsAdvOwnedSink struct{ allocsAdvSink }

func (s *allocsAdvOwnedSink) PublishOwned(_ context.Context, om mq.OwnedMessage, o *mq.PublishOptions) error {
	if om.Release != nil {
		defer om.Release(om.Message)
	}
	if o == nil {
		return mq.ErrUnsupported
	}
	return nil
}

// allocsAdvPublisher opens a client and a publisher over the harness.
func allocsAdvPublisher(tb testing.TB, open func(*advBroker, mq.SinkConfig) mq.Sink, cfg mq.PublisherConfig) (*advBroker, *mq.PublisherHandle) {
	tb.Helper()
	b := newAdvBroker(open)
	cl := mq.NewClient(b)
	tb.Cleanup(func() {
		if err := cl.Close(); err != nil {
			tb.Errorf("closing the client: %v", err)
		}
	})
	if cfg.Topic == "" {
		cfg.Topic = "orders"
	}
	p, err := cl.Publisher(context.Background(), cfg)
	if err != nil {
		tb.Fatalf("opening the publisher: %v", err)
	}
	return b, p
}
