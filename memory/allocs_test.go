package memory_test

import (
	"context"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// allocsRuns is how many times testing.AllocsPerRun executes a call. It also
// decides how big a backlog the receive cases publish first: the measured
// call must not publish, or it would be measuring the publication.
const allocsRuns = 200

// TestAllocs pins the allocation counts through the real
// driver, exactly.
//
// The specification measured the receive path against a fake backend that allocates
// nothing: one allocation per message in the default safe mode, which is
// ADR-009's settlement record, and none under BufferReuse. This test is the
// claim that going through the MEMORY driver does not add one — the thing an
// adapter is most likely to cost and least likely to be caught costing,
// because a benchmark reports it as a few nanoseconds and a budget gate
// reports it as a number nobody wrote down.
//
// The specification rule 1 asks for the exact count and not a bound, so that a path which
// starts allocating one object fails here rather than at a release benchmark.
func TestAllocs(t *testing.T) {
	t.Run("Receive", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			reuse bool
			batch int
			want  float64
		}{
			// The safe mode's one allocation per message is the core's
			// settlement record. The driver adds nothing: its fetch fills
			// the core's buffer out of a claim buffer allocated once at
			// open, and the handle a settlement needs is a pointer stored
			// directly in DriverDelivery.Ref's interface word.
			{"safe/batch=1", false, 1, 1},
			{"safe/batch=32", false, 32, 32},
			// the specification's "0 with reuse": the record comes back from the
			// consumer's own free list and the driver's path has nothing
			// left to allocate at all.
			{"reuse/batch=1", true, 1, 0},
			{"reuse/batch=32", true, 32, 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				p, c := allocsPair(t, tc.reuse, tc.batch)
				m := &mq.Message{Key: []byte("cust-42"), Body: []byte("body")}
				// The backlog is published BEFORE the measurement: safe
				// publication clones the message, and a call that published
				// would be measuring the clone rather than the receive.
				// AllocsPerRun runs the call once to warm up and then
				// allocsRuns times.
				for range (allocsRuns + 2) * tc.batch {
					if err := p.Publish(ctx, m); err != nil {
						t.Fatalf("Publish: %v", err)
					}
				}
				dst := make([]mq.Delivery, tc.batch)
				call := func() {
					for taken := 0; taken < tc.batch; {
						n, err := c.Receive(ctx, dst[taken:])
						if err != nil {
							t.Fatalf("Receive: %v", err)
						}
						for i := taken; i < taken+n; i++ {
							if err := dst[i].Ack(); err != nil {
								t.Fatalf("Ack: %v", err)
							}
						}
						taken += n
					}
				}
				// One warm-up round of our own, so that the consumer's free
				// list and the engine's pools are in the steady state
				// the budget is stated against.
				call()
				if got := testing.AllocsPerRun(allocsRuns, call); got != tc.want {
					t.Fatalf("receive+ack of %d messages through the memory driver allocated %v times, want exactly %v ",
						tc.batch, got, tc.want)
				}
			})
		}
	})

	t.Run("Fetch", func(t *testing.T) {
		// The driver's own half, with the core taken out of it: the specification requires
		// Fetch to allocate nothing per fetch, because the caller owns dst
		// and reuses it, and nothing per message either.
		ctx := context.Background()
		_, b := memory.NewForTest(t, memory.TestTopics("orders"))
		src := openSource(t, ctx, b, "orders", "billing")
		sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders"})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		t.Cleanup(func() { _ = sink.Close() })
		st := src.Settler()
		m := &mq.Message{Key: []byte("cust-42"), Body: []byte("body")}
		var o mq.PublishOptions
		const batch = 8
		for range (allocsRuns + 2) * batch {
			if err := sink.Publish(ctx, m, &o); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}
		dst := make([]mq.DriverDelivery, batch)
		call := func() {
			for taken := 0; taken < batch; {
				n, ferr := src.Fetch(ctx, dst[taken:])
				if ferr != nil {
					t.Fatalf("Fetch: %v", ferr)
				}
				for i := taken; i < taken+n; i++ {
					if err := st.Ack(ctx, &dst[i]); err != nil {
						t.Fatalf("Ack: %v", err)
					}
				}
				taken += n
			}
		}
		call()
		if got := testing.AllocsPerRun(allocsRuns, call); got != 0 {
			t.Fatalf("Fetch+Ack of %d records allocated %v times, want exactly 0 ", batch, got)
		}
	})

	t.Run("Cycle", func(t *testing.T) {
		// The whole cycle in its steady state, which is where ADR-009's
		// decision and the allocation target are both stated.
		//
		// Safe publication clones the message, and for a *mq.Message that
		// is three allocations: the message value, its key and its body.
		// That is the specification's "1 (clone) in safe mode" counted in objects rather
		// than in copies, and it is the whole of what the safe mode costs —
		// the engine's envelope comes from its pool, the option set from the
		// core's, and the fetch from the source's own claim buffer.
		//
		// The owned-and-reuse row is the specification's "0 allocs/op owned/reuse
		// steady state", end to end through the public API: the caller hands
		// its storage over and gets it back through the release callback,
		// and the consumer recycles the settlement record. Nothing in the
		// core, the driver or the engine allocates per message.
		//
		// A driver-side pool of safe clones would take the safe row to the
		// same place and is NOT safe to have: a settled Delivery may still
		// alias the payload in the default safe receive mode (the specification's
		// "borrowed until successful terminal settlement"), and a sink
		// cannot see whether the consumer asked for reuse. Recorded as a
		// finding rather than done here.
		//
		// The counts below are the release build's, which is the build
		// the figures are stated for, and they hold under the race
		// detector and under the iqmq_debug tag as well — but not under BOTH
		// at once, where the debug release ledger's allocation is attributed
		// sometimes and not others: the same row measured 4 in one tagged
		// race run and 3 in another. There the assertion is a one-object
		// band rather than an exact count, which still fails a regression of
		// two and does not pretend to a precision the build does not have.
		for _, tc := range []struct {
			name  string
			owned bool
			reuse bool
			want  float64
		}{
			{"safe", false, false, 4},
			{"safe/reuse=on", false, true, 3},
			{"owned", true, false, 1},
			{"owned/reuse=on", true, true, 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				client, b := memory.NewForTest(t)
				c, err := client.Consumer(ctx, mq.ConsumerConfig{
					Topic:        "orders",
					Subscription: "billing",
					BufferReuse:  tc.reuse,
				})
				if err != nil {
					t.Fatalf("Consumer: %v", err)
				}
				sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders"})
				if err != nil {
					t.Fatalf("OpenSink: %v", err)
				}
				t.Cleanup(func() { _ = sink.Close() })
				owned, _ := sink.(mq.OwnedSink)
				if tc.owned && owned == nil {
					t.Fatal("the memory sink does not implement OwnedSink")
				}
				key, body := []byte("cust-42"), []byte("body")
				// The caller's own message pool, which is the reuse
				// discipline the owned mode exists for: the release callback
				// is what hands the storage back.
				free := make(chan *mq.Message, 4)
				for range cap(free) {
					free <- &mq.Message{Key: make([]byte, 0, 16), Body: make([]byte, 0, 16)}
				}
				release := func(m *mq.Message) {
					select {
					case free <- m:
					default:
					}
				}
				plain := &mq.Message{Key: key, Body: body}
				var o mq.PublishOptions
				dst := make([]mq.Delivery, 1)
				call := func() {
					if tc.owned {
						m := <-free
						m.Key = append(m.Key[:0], key...)
						m.Body = append(m.Body[:0], body...)
						if err := owned.PublishOwned(ctx, mq.OwnedMessage{Message: m, Release: release}, &o); err != nil {
							t.Fatalf("PublishOwned: %v", err)
						}
					} else if err := sink.Publish(ctx, plain, &o); err != nil {
						t.Fatalf("Publish: %v", err)
					}
					n, rerr := c.Receive(ctx, dst)
					if rerr != nil || n != 1 {
						t.Fatalf("Receive = (%d, %v)", n, rerr)
					}
					if err := dst[0].Ack(); err != nil {
						t.Fatalf("Ack: %v", err)
					}
				}
				call()
				got := testing.AllocsPerRun(allocsRuns, call)
				if allocsDebugCanary && allocsRaceDetector {
					if got < tc.want || got > tc.want+1 {
						t.Fatalf("publish+receive+ack (%s) allocated %v times per message, want %v or %v — the release count, plus at most the one object the iqmq_debug ledger costs under the race detector ",
							tc.name, got, tc.want, tc.want+1)
					}
					return
				}
				if got != tc.want {
					t.Fatalf("publish+receive+ack (%s) allocated %v times per message, want exactly %v (ADR-009)",
						tc.name, got, tc.want)
				}
			})
		}
	})
}

// allocsPair opens a publisher and a consumer whose configuration is the one
// the counts above are stated against: one lane, no priorities, no dedup, an
// unbounded lane so that a long backlog fits, and a prefetch that can hold
// the batch.
func allocsPair(t *testing.T, reuse bool, batch int) (*mq.PublisherHandle, *mq.ConsumerHandle) {
	t.Helper()
	ctx := context.Background()
	client, _ := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
		CapacityMessages: -1,
	}))
	c, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic:        "orders",
		Subscription: "billing",
		Prefetch:     max(batch, 32),
		BufferReuse:  reuse,
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	return p, c
}
