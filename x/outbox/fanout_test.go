package outbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/outbox"
)

// TestAPartialFanoutIsMarkedSent: under the memory broker's FanoutIndependent
// policy a publication that one subscription has no room for and another
// takes answers mq.ErrPartial, which the engine documents as a publication
// the broker took and a publisher must not repeat: the subscriptions that
// took it would receive it twice. The relay marks the row sent. It used to
// read CodePartial as the broker being down, release the row and publish it
// again on every pass, so the subscription that took it received one more
// copy per pass for as long as the other stayed full.
func TestAPartialFanoutIsMarkedSent(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestConfig(memory.Config{
		DefaultTopology: memory.TopologyConfig{
			CapacityMessages: 1,
			Overflow:         memory.OverflowReject,
			Fanout:           memory.FanoutIndependent,
		},
	}))
	full, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "full"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = full.Close() })
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("filler")}); err != nil {
		t.Fatal(err)
	}
	room, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "room"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = room.Close() })

	s := newStore(t, outbox.MemoryOptions{})
	if _, err := s.Append(outbox.Row{Topic: "orders", MessageID: "order-1", Body: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	partials := 0
	publish := func(ctx context.Context, topic string, ms []*mq.Message, opts []mq.PublishOption) error {
		err := client.Publish(ctx, topic, ms[0], opts...)
		if errors.Is(err, mq.ErrPartial) {
			partials++
		}
		return err
	}
	r, err := outbox.New(outbox.Config{Store: s, Publish: publish})
	if err != nil {
		t.Fatal(err)
	}
	// "room" drains what it is handed, as a live consumer would, so it always
	// has room for another copy; "full" never does.
	dst := make([]mq.Delivery, 1)
	copies := 0
	for range 5 {
		if _, err := r.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		rctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		n, _ := room.Receive(rctx, dst)
		cancel()
		if n == 1 {
			copies++
			_ = dst[0].Ack()
		}
	}
	snap, err := b.Inspect("orders", "room")
	if err != nil {
		t.Fatal(err)
	}
	st := r.Stats()
	if partials != 1 || copies != 1 || snap.Published != 1 {
		t.Fatalf("five passes published the row %d time(s) and the subscription that took it received %d cop(ies) "+
			"(%d published to it): want one publication and one copy", partials, copies, snap.Published)
	}
	if s.Unsent() != 0 || st.Sent != 1 || st.Failed != 0 {
		t.Fatalf("after the partial fan-out %d row(s) are unsent and Stats = %+v: want the row marked sent, "+
			"Sent 1 and Failed 0", s.Unsent(), st)
	}
}

// TestAOneEntryBatchErrorIsClassifiedByItsEntry: the relay publishes one
// message per call, and a custom PublishFunc built on a batch publisher may
// answer with a *mq.BatchError of one entry. That error carries CodePartial
// whatever its entry says, and the relay classified the batch rather than the
// message: a nil entry (the message was accepted) and a partial fan-out were
// both read as the broker being down and published again on every pass, and
// a row fault never made the row poison. The entry is the message's outcome.
func TestAOneEntryBatchErrorIsClassifiedByItsEntry(t *testing.T) {
	ctx := context.Background()
	partial := &mq.OpError{Operation: mq.OpPublish, Topic: "orders", Code: mq.CodePartial, Err: mq.ErrPartial}
	for _, tc := range []struct {
		name   string
		entry  error
		passes int
		want   outbox.Stats
	}{
		{name: "accepted", entry: nil, passes: 3, want: outbox.Stats{Sent: 1}},
		{name: "a partial fan-out", entry: partial, passes: 3, want: outbox.Stats{Sent: 1}},
		// MaxAttempts 1: the first claim fails and is released, the second is
		// past the bound and makes the row poison.
		{name: "a row fault", entry: rowFault("invalid"), passes: 3, want: outbox.Stats{Failed: 1, Poison: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newStore(t, outbox.MemoryOptions{})
			if _, err := s.Append(outbox.Row{Topic: "orders", MessageID: "order-1", Body: []byte("one")}); err != nil {
				t.Fatal(err)
			}
			calls := 0
			publish := func(context.Context, string, []*mq.Message, []mq.PublishOption) error {
				calls++
				return &mq.BatchError{Errs: []error{tc.entry}}
			}
			r, err := outbox.New(outbox.Config{Store: s, Publish: publish, MaxAttempts: 1})
			if err != nil {
				t.Fatal(err)
			}
			for range tc.passes {
				if _, err := r.Drain(ctx); err != nil {
					t.Fatal(err)
				}
			}
			st := r.Stats()
			if st.Sent != tc.want.Sent || st.Failed != tc.want.Failed || st.Poison != tc.want.Poison || st.Ambiguous != 0 {
				t.Fatalf("after %d passes and %d publication(s), Stats = %+v; want Sent %d, Failed %d, Poison %d",
					tc.passes, calls, st, tc.want.Sent, tc.want.Failed, tc.want.Poison)
			}
		})
	}
}
