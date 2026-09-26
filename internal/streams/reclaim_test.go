package streams_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

const (
	testTopic = "acme.orders"
	testGroup = "workers"
	testLease = 5 * time.Second
)

// rig is a fake server, a consumer and the key naming they share.
type rig struct {
	srv  *fakestreams.Server
	cons *streams.Consumer
	keys streams.Keys
	ctx  context.Context
}

// newRig builds a one-shard topic with a consumer group and one consumer.
func newRig(t testing.TB, name string, shards int, opts ...func(*streams.ConsumerConfig)) *rig {
	t.Helper()
	keys, err := streams.NewKeys(testTopic, shards)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	cfg := streams.ConsumerConfig{
		Keys:        keys,
		Group:       testGroup,
		Name:        name,
		Prefetch:    16,
		AckDeadline: testLease,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	cons, err := streams.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	srv := fakestreams.New(fakestreams.Options{AOF: true})
	t.Cleanup(func() { _ = srv.Close() })
	ctx := context.Background()
	if err := cons.Provision(ctx, srv, streams.IDZero, true); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	return &rig{srv: srv, cons: cons, keys: keys, ctx: ctx}
}

// publish adds one entry to shard 0 and returns its identifier.
func (r *rig) publish(t testing.TB, body string) streams.ID {
	t.Helper()
	m := &mq.Message{Topic: testTopic, Body: []byte(body)}
	v, err := r.srv.Do(r.ctx, streams.XAdd(streams.XAddArgs{
		Key:    r.keys.Stream(0),
		ID:     streams.IDAuto,
		Fields: streams.Encode(nil, m),
	}))
	if err != nil {
		t.Fatalf("XADD: %v", err)
	}
	id, err := streams.ParseXAdd(v)
	if err != nil {
		t.Fatalf("ParseXAdd: %v", err)
	}
	return id
}

// read reads whatever is available.
func (r *rig) read(t testing.TB) []streams.Delivery {
	t.Helper()
	ds, _, err := r.cons.Read(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return ds
}

// TestReclaimDoesNotSettleTheSupersededDelivery is the disqualifier of
// the performance budgets held in place, in both of its forms.
//
// XACK removes a pending entry whoever holds it, so a driver that sends one
// for a delivery that has been claimed away acknowledges another consumer's
// work. If that consumer then dies, the message is gone: nothing will deliver
// it again and nobody finished it.
func TestReclaimDoesNotSettleTheSupersededDelivery(t *testing.T) {
	t.Run("another-consumer-claimed-it", func(t *testing.T) {
		r := newRig(t, "source-a", 1)
		id := r.publish(t, "payload")
		ds := r.read(t)
		if len(ds) != 1 {
			t.Fatalf("Read returned %d deliveries, want 1", len(ds))
		}
		// Another process claims the entry away. The pending row now names
		// its consumer, which is the only evidence a server keeps.
		if !r.srv.Deliver(r.keys.Stream(0), testGroup, "source-b", id) {
			t.Fatal("the fake could not hand the entry to another consumer")
		}
		errs := r.cons.Ack(r.ctx, r.srv, ds)
		if !errors.Is(errs[0], mq.ErrLeaseLost) {
			t.Fatalf("Ack of a delivery another consumer now holds returned %v, want ErrLeaseLost: "+
				"acknowledging it removes the entry the new holder is processing, and if that holder "+
				"dies the message is lost ", errs[0])
		}
		// And the entry is still pending, still owned by the new holder:
		// the refusal has to refuse, not merely report.
		pending := r.srv.Pending(r.keys.Stream(0), testGroup)
		if len(pending) != 1 {
			t.Fatalf("the pending list holds %d entries after the refused ack, want 1: the entry was acknowledged anyway", len(pending))
		}
		if pending[0].Consumer != "source-b" {
			t.Fatalf("the pending entry is owned by %q, want source-b", pending[0].Consumer)
		}
	})

	t.Run("our-own-sweep-superseded-it", func(t *testing.T) {
		r := newRig(t, "source-a", 1)
		r.publish(t, "payload")
		first := r.read(t)
		if len(first) != 1 {
			t.Fatalf("Read returned %d deliveries, want 1", len(first))
		}
		// The handler is slow: the lease runs out and our own reclaim loop
		// takes the entry back and delivers it again.
		r.srv.Advance(testLease + time.Second)
		second, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if len(second) != 1 {
			t.Fatalf("Sweep reclaimed %d entries, want 1", len(second))
		}
		if second[0].Gen == first[0].Gen {
			t.Fatalf("the reclaimed delivery carries generation %d, the same as the delivery it "+
				"superseded: a settlement of the old one is then indistinguishable from a settlement of the new",
				second[0].Gen)
		}
		if second[0].Superseded != first[0].Gen {
			t.Fatalf("the reclaimed delivery reports superseding generation %d, want %d",
				second[0].Superseded, first[0].Gen)
		}
		if second[0].Attempt < 2 {
			t.Fatalf("the reclaimed delivery reports attempt %d, want at least 2", second[0].Attempt)
		}
		// The slow handler finally finishes and acknowledges. Its delivery
		// has been superseded, so the acknowledgement must be refused
		// BEFORE a command is sent.
		errs := r.cons.Ack(r.ctx, r.srv, first)
		if !errors.Is(errs[0], mq.ErrLeaseLost) {
			t.Fatalf("Ack of the superseded delivery returned %v, want ErrLeaseLost", errs[0])
		}
		if n := len(r.srv.Pending(r.keys.Stream(0), testGroup)); n != 1 {
			t.Fatalf("the pending list holds %d entries, want 1: the superseded ack was sent anyway", n)
		}
		// The current delivery settles.
		if errs := r.cons.Ack(r.ctx, r.srv, second); errs[0] != nil {
			t.Fatalf("Ack of the current delivery: %v", errs[0])
		}
		if n := len(r.srv.Pending(r.keys.Stream(0), testGroup)); n != 0 {
			t.Fatalf("the pending list holds %d entries after the current delivery was acknowledged, want 0", n)
		}
	})

	t.Run("a-settlement-in-flight-is-not-redelivered", func(t *testing.T) {
		// The other side of the same race: a sweep that runs while a
		// settlement is in flight must not hand the entry out again, or the
		// driver manufactures a duplicate out of a message that was about to
		// be acknowledged.
		r := newRig(t, "source-a", 1)
		r.publish(t, "payload")
		ds := r.read(t)
		if err := r.cons.Reclaimer().Begin(ds[0].Key, ds[0].Gen); err != nil {
			t.Fatalf("Begin: %v", err)
		}
		r.srv.Advance(testLease + time.Second)
		again, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if len(again) != 0 {
			t.Fatalf("the sweep handed out %d deliveries for an entry whose settlement is in flight, want 0", len(again))
		}
	})
}

// TestGuardNoneLeavesTheWindowOpen is the negative direction of the guard: it
// proves the guarded acknowledgement is doing the work, rather than something
// else happening to refuse the ack.
//
// With the guard off the very same sequence acknowledges another consumer's
// delivery, which is the loss the default setting prevents. A test that only
// showed the safe configuration working would not distinguish "the guard
// works" from "the fake refuses everything".
func TestGuardNoneLeavesTheWindowOpen(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Guard = streams.GuardNone })
	id := r.publish(t, "payload")
	ds := r.read(t)
	if !r.srv.Deliver(r.keys.Stream(0), testGroup, "source-b", id) {
		t.Fatal("the fake could not hand the entry to another consumer")
	}
	if errs := r.cons.Ack(r.ctx, r.srv, ds); errs[0] != nil {
		t.Fatalf("with GuardNone the ack was refused by something other than the guard: %v", errs[0])
	}
	if n := len(r.srv.Pending(r.keys.Stream(0), testGroup)); n != 0 {
		t.Fatalf("with GuardNone the pending list still holds %d entries, so the unguarded XACK did not reach the server "+
			"and this test proves nothing about the guard", n)
	}
}

// TestExtendKeepsTheAttemptCount pins the lease extension's one requirement:
// XCLAIM JUSTID must not increment the delivery counter, or a handler that is
// merely slow looks like a message that keeps failing and MaxAttempts
// dead-letters it.
func TestExtendKeepsTheAttemptCount(t *testing.T) {
	r := newRig(t, "source-a", 1)
	r.publish(t, "payload")
	ds := r.read(t)
	before := r.srv.Pending(r.keys.Stream(0), testGroup)
	if len(before) != 1 {
		t.Fatalf("pending = %d, want 1", len(before))
	}
	for range 20 {
		r.srv.Advance(testLease / 2)
		if errs := r.cons.Extend(r.ctx, r.srv, ds); errs[0] != nil {
			t.Fatalf("Extend: %v", errs[0])
		}
	}
	after := r.srv.Pending(r.keys.Stream(0), testGroup)
	if len(after) != 1 {
		t.Fatalf("pending = %d after twenty extensions, want 1", len(after))
	}
	if after[0].Delivered != before[0].Delivered {
		t.Fatalf("twenty lease extensions moved the delivery counter from %d to %d; "+
			"a slow handler would then be dead-lettered as a failing message",
			before[0].Delivered, after[0].Delivered)
	}
	if after[0].Idle >= testLease {
		t.Fatalf("the entry is idle for %v after an extension, which is at or past its %v lease: "+
			"the extension did not reset the idle clock", after[0].Idle, testLease)
	}
	// The extension really did keep the lease: a sweep must not reclaim it.
	got, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("the sweep reclaimed %d entries whose lease was extended, want 0", len(got))
	}
}

// TestExtendOfALostLeaseIsRefused is the extension's negative direction.
func TestExtendOfALostLeaseIsRefused(t *testing.T) {
	r := newRig(t, "source-a", 1)
	id := r.publish(t, "payload")
	ds := r.read(t)
	if !r.srv.Deliver(r.keys.Stream(0), testGroup, "source-b", id) {
		t.Fatal("hand-off failed")
	}
	// XCLAIM with min-idle 0 would take the entry BACK from source-b, which
	// is not an extension — so the driver's Extend must report the loss
	// rather than steal it. The reclaimer is the thing that knows.
	r.cons.Reclaimer().Lost(ds[0].Key)
	if errs := r.cons.Extend(r.ctx, r.srv, ds); !errors.Is(errs[0], mq.ErrLeaseLost) {
		t.Fatalf("Extend of a lost lease returned %v, want ErrLeaseLost", errs[0])
	}
}

// TestCloseLeavesPendingEntriesForTheSweep pins the shutdown rule that is one
// command away from deleting messages.
//
// XGROUP DELCONSUMER removes a consumer's pending entries along with the
// consumer, so a Close that removed the consumer while it still held entries
// would delete those messages from the group for ever. The entries must stay
// pending instead, where another consumer's reclaim loop can find them.
func TestCloseLeavesPendingEntriesForTheSweep(t *testing.T) {
	r := newRig(t, "source-a", 1)
	r.publish(t, "one")
	r.publish(t, "two")
	if ds := r.read(t); len(ds) != 2 {
		t.Fatalf("Read returned %d deliveries, want 2", len(ds))
	}
	if err := r.cons.Close(r.ctx, r.srv); err != nil {
		t.Fatalf("Close: %v", err)
	}
	pending := r.srv.Pending(r.keys.Stream(0), testGroup)
	if len(pending) != 2 {
		t.Fatalf("the pending list holds %d entries after Close, want 2: Close deleted messages "+
			"the consumer had not settled", len(pending))
	}
	// And a fresh consumer's sweep gets them, which is what "abandon" has to
	// mean if it is to be honest.
	other := newRig(t, "source-b", 1)
	other.srv = r.srv
	other.srv.Advance(testLease + time.Second)
	got, _, err := other.cons.Sweep(other.ctx, other.srv, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("another consumer's sweep reclaimed %d of the 2 abandoned entries", len(got))
	}
}

// TestCloseRemovesAnEmptyConsumer is the other half: a consumer that holds
// nothing is removed, so a process that starts and stops repeatedly does not
// leave a row in XINFO CONSUMERS for every run.
func TestCloseRemovesAnEmptyConsumer(t *testing.T) {
	r := newRig(t, "source-a", 1)
	r.publish(t, "one")
	ds := r.read(t)
	if errs := r.cons.Ack(r.ctx, r.srv, ds); errs[0] != nil {
		t.Fatalf("Ack: %v", errs[0])
	}
	if err := r.cons.Close(r.ctx, r.srv); err != nil {
		t.Fatalf("Close: %v", err)
	}
	v, err := r.srv.Do(r.ctx, streams.Cmd{
		Tokens:   []string{"XINFO", "CONSUMERS", r.keys.Stream(0), testGroup},
		KeyStart: 2, KeyCount: 1,
	})
	if err != nil {
		t.Fatalf("XINFO CONSUMERS: %v", err)
	}
	if v.Len() != 0 {
		t.Fatalf("the group still lists %d consumers after a Close with nothing pending", v.Len())
	}
}
