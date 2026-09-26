package iqmq_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// The delivery accessors of the API: the metadata, the escape
// hatch, the lease extension and the escape from borrowed storage.
//
// Every test here was watched failing: the accessor was broken in the
// implementation, the test run, the message recorded, and the implementation
// restored.

// TestDeliveryInfo pins the metadata one delivery carries, field by field,
// against a driver that fills every one of them.
//
// The Has flags are the half worth pinning: a partition of zero and an offset
// of zero are real values, so "the broker did not say" cannot be the same
// answer, and pairs each optional field with a flag precisely for that.
func TestDeliveryInfo(t *testing.T) {
	cl, b := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	d := receiveOne(t, c)

	got := d.Info()
	if got.Topic != "orders" {
		t.Errorf("Info().Topic = %q, want the resolved topic %q", got.Topic, "orders")
	}
	if got.Source != fakeStream+"orders" {
		t.Errorf("Info().Source = %q, want the broker-side origin %q", got.Source, fakeStream+"orders")
	}
	if got.Subscription != "billing" {
		t.Errorf("Info().Subscription = %q, want %q", got.Subscription, "billing")
	}
	if got.Attempt != 1 {
		t.Errorf("Info().Attempt = %d, want 1 on the first delivery", got.Attempt)
	}
	if got.Redelivered {
		t.Error("Info().Redelivered is true on a first delivery")
	}
	if got.ReceivedAt.IsZero() {
		t.Error("Info().ReceivedAt is the zero time; the driver said when it received the message")
	}
	if !got.LeaseExpires.After(got.ReceivedAt) {
		t.Errorf("Info().LeaseExpires = %v, want a lease after ReceivedAt = %v", got.LeaseExpires, got.ReceivedAt)
	}
	if !got.HasPartition || got.Partition != fakePartition {
		t.Errorf("Info() reports partition %d (has=%v), want %d (has=true) — the driver pages document it",
			got.Partition, got.HasPartition, fakePartition)
	}
	if !got.HasOffset {
		t.Error("Info().HasOffset is false although the driver set an offset")
	}
	if !got.HasSequence {
		t.Error("Info().HasSequence is false although the driver set a sequence")
	}
	// The specification gives the attempt twice, and the two must never disagree.
	if d.Attempt != int(got.Attempt) {
		t.Errorf("Delivery.Attempt = %d and Info().Attempt = %d disagree", d.Attempt, got.Attempt)
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
	if n := b.counts("orders", "billing"); n.acked != 1 {
		t.Fatalf("the broker holds %+v, want 1 acked", n)
	}
}

// TestDeliveryInfoRedelivery pins the other half of the attempt: a
// redelivered delivery says so, and both copies of the attempt move together.
func TestDeliveryInfoRedelivery(t *testing.T) {
	cl, b := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	first := receiveOne(t, c)
	if first.Info().Redelivered {
		t.Fatal("the first delivery reports itself redelivered")
	}
	b.expire("orders", "billing")
	again := receiveOne(t, c)
	got := again.Info()
	if got.Attempt != 2 || !got.Redelivered {
		t.Fatalf("the redelivery reports attempt %d, redelivered %v; want 2 and true", got.Attempt, got.Redelivered)
	}
	if again.Attempt != int(got.Attempt) {
		t.Fatalf("Delivery.Attempt = %d and Info().Attempt = %d disagree on the redelivery", again.Attempt, got.Attempt)
	}
	if err := again.Ack(); err != nil {
		t.Fatal(err)
	}
}

// TestDeliveryInfoWithoutADelivery pins the answer for the two deliveries
// that carry no metadata: the zero value and one a caller built itself.
//
// Info returns a value rather than an error, so this is the only way it can
// say "there is no delivery here" — and a hand-built Delivery's own Attempt
// field is the caller's, which is why Info's is the authoritative one.
func TestDeliveryInfoWithoutADelivery(t *testing.T) {
	var zero mq.Delivery
	if got := zero.Info(); got != (mq.DeliveryInfo{}) {
		t.Errorf("the zero Delivery reports %+v, want the zero DeliveryInfo", got)
	}
	if got := zero.Native(); got != nil {
		t.Errorf("the zero Delivery reports a native object %v", got)
	}
	built := mq.Delivery{Message: mq.Message{Topic: "orders", Body: []byte("by hand")}, Attempt: 7}
	if got := built.Info(); got != (mq.DeliveryInfo{}) {
		t.Errorf("a hand-built Delivery reports %+v, want the zero DeliveryInfo", got)
	}
	if got := built.Native(); got != nil {
		t.Errorf("a hand-built Delivery reports a native object %v", got)
	}
	if _, ok := mq.NativeAs[*fakeRecord](&built); ok {
		t.Error("NativeAs reported a native object for a hand-built Delivery")
	}
	if err := built.Extend(time.Minute); !errors.Is(err, mq.ErrNotSettleable) {
		t.Errorf("extending a hand-built Delivery = %v, want ErrNotSettleable", err)
	}
	// CloneMessage is the one accessor that needs no delivery at all: it
	// copies the embedded Message.
	if got := built.CloneMessage(); got == nil || string(got.Body) != "by hand" {
		t.Errorf("CloneMessage of a hand-built Delivery = %+v", got)
	}
}

// TestDeliveryInfoOutlivesTheDelivery pins the answer the specification chose for a
// released record, which Info has to have because it returns a value: the
// metadata it was handed over with, unchanged, for all three ways a delivery
// leaves circulation.
//
// The alternative — the zero value once the delivery is settled — cannot be
// implemented without a lock on the settlement path, because reading the
// release reason and then the metadata leaves exactly the window the release
// would be writing in. So the metadata is written once and never cleared, and
// a settled, lost or closed delivery still names its own topic.
func TestDeliveryInfoOutlivesTheDelivery(t *testing.T) {
	for _, tc := range []struct {
		name string
		// leave takes the delivery out of circulation and reports the error
		// a settlement of it must then answer with.
		leave func(t *testing.T, cl *mq.Client, c *mq.ConsumerHandle, b *fakeBroker, d *mq.Delivery) error
	}{
		{"settled", func(t *testing.T, _ *mq.Client, _ *mq.ConsumerHandle, _ *fakeBroker, d *mq.Delivery) error {
			if err := d.Ack(); err != nil {
				t.Fatalf("Ack = %v", err)
			}
			return mq.ErrAlreadySettled
		}},
		{"leaselost", func(t *testing.T, _ *mq.Client, c *mq.ConsumerHandle, b *fakeBroker, d *mq.Delivery) error {
			b.expire("orders", "billing")
			_ = receiveOne(t, c)
			if err := d.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
				t.Fatalf("acking the stale delivery = %v, want ErrLeaseLost", err)
			}
			return mq.ErrLeaseLost
		}},
		{"closed", func(t *testing.T, _ *mq.Client, c *mq.ConsumerHandle, _ *fakeBroker, _ *mq.Delivery) error {
			if err := c.Close(); err != nil {
				t.Fatalf("Close = %v", err)
			}
			return mq.ErrClosed
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl, b := openFake(t, fakeConfig{})
			c := consume(t, cl, mq.ConsumerConfig{Prefetch: 4})
			publishN(t, cl, 1)
			d := receiveOne(t, c)
			before, native := d.Info(), d.Native()
			if before.Topic == "" {
				t.Fatal("the live delivery has no topic")
			}
			want := tc.leave(t, cl, c, b, &d)
			if got := d.Info(); got != before {
				t.Errorf("Info() after the delivery left circulation = %+v, want the metadata it was handed over with %+v", got, before)
			}
			if got := d.Native(); got != native {
				t.Errorf("Native() after the delivery left circulation = %v, want %v", got, native)
			}
			// The settlement rules are unchanged by any of it: the record
			// answers for the reason it was released.
			if err := d.Ack(); !errors.Is(err, want) {
				t.Errorf("Ack after the delivery left circulation = %v, want %v", err, want)
			}
			if err := d.Extend(time.Minute); !errors.Is(err, want) {
				t.Errorf("Extend after the delivery left circulation = %v, want %v", err, want)
			}
		})
	}
}

// TestDeliveryNative pins the escape hatch: the driver's own delivery
// object, and the type assertion every use of it ends in.
//
// The negative half is the half that matters. NativeAs of the wrong type must
// report false rather than panic, because a driver's object is documented on
// its page and a caller cannot prove at compile time that the client it is
// holding is the driver it thinks it is.
func TestDeliveryNative(t *testing.T) {
	cl, _ := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	d := receiveOne(t, c)

	native := d.Native()
	if native == nil {
		t.Fatal("Native() is nil although the driver surfaced its own delivery object")
	}
	rec, ok := mq.NativeAs[*fakeRecord](&d)
	if !ok {
		t.Fatalf("NativeAs[*fakeRecord] reported false for a %T", native)
	}
	if rec == nil || rec.tok == 0 {
		t.Fatalf("NativeAs returned %+v, want the driver's record", rec)
	}
	if any(rec) != native {
		t.Errorf("NativeAs returned %p and Native() returned %p", rec, native)
	}
	if _, ok := mq.NativeAs[string](&d); ok {
		t.Error("NativeAs[string] reported true for a driver record")
	}
	if _, ok := mq.NativeAs[*mq.Message](&d); ok {
		t.Error("NativeAs[*Message] reported true for a driver record")
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
}

// TestDeliveryExtend pins the lease extension: what it does when the
// driver can extend a lease, and what it answers when it cannot.
//
// The rule that costs the most to get wrong is the first one: an extension is
// not a settlement. A successful Extend must leave the delivery exactly as
// settleable as it was, or a handler that extends its lease has lost the
// ability to ack the message it is still working on.
func TestDeliveryExtend(t *testing.T) {
	t.Run("extends", func(t *testing.T) {
		cl, b := openFake(t, fakeConfig{extend: true})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		d := receiveOne(t, c)
		if err := d.Extend(2 * time.Minute); err != nil {
			t.Fatalf("Extend = %v, want nil on a driver that extends leases", err)
		}
		got := b.counts("orders", "billing")
		if got.extended != 1 || got.extendedBy != 2*time.Minute {
			t.Fatalf("the driver extended %d lease(s) by %v, want 1 by 2m", got.extended, got.extendedBy)
		}
		// Not a settlement: the delivery is untouched and still settleable.
		if d.Settled() {
			t.Fatal("Settled() is true after an extension: an extension is not a settlement")
		}
		if got.inflight != 1 {
			t.Fatalf("the broker holds %+v, want the delivery still in flight", got)
		}
		if st := c.Stats(); st.InFlight != 1 || st.Acked != 0 || st.SettleFailures != 0 {
			t.Fatalf("Stats = %+v after an extension, want 1 in flight and nothing settled", st)
		}
		if err := d.Extend(time.Minute); err != nil {
			t.Fatalf("a second Extend = %v, want nil: an extension is repeatable", err)
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack after an extension = %v, want nil: a successful extension leaves the delivery settleable", err)
		}
		if n := b.counts("orders", "billing"); n.acked != 1 || n.extended != 2 || !n.conserved() {
			t.Fatalf("the broker holds %+v, want 1 acked, 2 extensions and conservation", n)
		}
	})

	t.Run("unsupported", func(t *testing.T) {
		// The settler here implements Settler and nothing else, which is
		// what a broker with no lease to extend looks like. The
		// assertion the core makes when it opens the source is what turns
		// that into ErrUnsupported, and it is the core's answer: the driver
		// is never called.
		cl, b := openFake(t, fakeConfig{})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		d := receiveOne(t, c)
		if err := d.Extend(time.Minute); !errors.Is(err, mq.ErrUnsupported) {
			t.Fatalf("Extend on a settler that is no DeadlineExtender = %v, want ErrUnsupported", err)
		}
		if got := b.counts("orders", "billing"); got.extended != 0 {
			t.Fatalf("the driver was asked to extend %d lease(s)", got.extended)
		}
		// ErrUnsupported settles nothing, so the delivery is still live.
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack after a refused extension = %v, want nil", err)
		}
	})

	t.Run("range", func(t *testing.T) {
		cl, b := openFake(t, fakeConfig{extend: true})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		d := receiveOne(t, c)
		for _, by := range []time.Duration{0, -time.Second} {
			if err := d.Extend(by); !isCode(err, mq.CodeInvalidArgument) {
				t.Fatalf("Extend(%v) = %v, want CodeInvalidArgument", by, err)
			}
		}
		if got := b.counts("orders", "billing"); got.extended != 0 {
			t.Fatalf("the driver was asked to extend a lease by nothing (%+v)", got)
		}
		if err := d.Ack(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("notsettleable", func(t *testing.T) {
		cl, _ := openFake(t, fakeConfig{noSettler: true})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		d := receiveOne(t, c)
		if err := d.Extend(time.Minute); !errors.Is(err, mq.ErrNotSettleable) {
			t.Fatalf("Extend on AckNone model = %v, want ErrNotSettleable", err)
		}
	})

	t.Run("leaselost", func(t *testing.T) {
		// The broker holds the generation, so a stale extension is refused
		// where a stale ack is refused. It does not take the delivery out
		// of circulation: the settlement that follows is what does that,
		// with the accounting.
		cl, b := openFake(t, fakeConfig{extend: true})
		c := consume(t, cl, mq.ConsumerConfig{Prefetch: 4})
		publishN(t, cl, 1)
		stale := receiveOne(t, c)
		b.expire("orders", "billing")
		fresh := receiveOne(t, c)
		if err := stale.Extend(time.Minute); !errors.Is(err, mq.ErrLeaseLost) {
			t.Fatalf("extending a stale delivery = %v, want ErrLeaseLost", err)
		}
		if got := b.counts("orders", "billing"); got.extended != 0 {
			t.Fatalf("the broker extended a stale delivery's lease (%+v)", got)
		}
		if st := c.Stats(); st.InFlight != 2 {
			t.Fatalf("InFlight = %d after a refused extension, want 2: an extension releases nothing", st.InFlight)
		}
		if err := fresh.Extend(time.Minute); err != nil {
			t.Fatalf("extending the redelivery = %v, want nil", err)
		}
		if err := fresh.Ack(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("closed", func(t *testing.T) {
		// The specification: after close every operation answers ErrClosed. The check
		// order says so even for a driver that could never have extended
		// the lease, which is why both arms are here: ErrUnsupported would
		// be a true statement about the driver and the wrong answer about
		// the consumer.
		for _, extend := range []bool{true, false} {
			cl, _ := openFake(t, fakeConfig{extend: extend})
			c := consume(t, cl, mq.ConsumerConfig{})
			publishN(t, cl, 1)
			d := receiveOne(t, c)
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			if err := d.Extend(time.Minute); !errors.Is(err, mq.ErrClosed) {
				t.Fatalf("Extend after close (extender=%v) = %v, want ErrClosed", extend, err)
			}
		}
	})
}

// TestDeliveryExtendWaitsForCloseOrder is the extension's half of the defect
// class hack/PLAYBOOK.md calls the engine's recurring one: an operation that
// reaches a driver after the driver's source has been closed.
//
// An extension is not a settlement, so nothing in the state machine bars
// it; the lifecycle ticket is the only thing that does. The event log has to
// show the source closing after the extension left the driver.
func TestDeliveryExtendWaitsForCloseOrder(t *testing.T) {
	cl, b := openFake(t, fakeConfig{extend: true})
	gate := &settleGate{entered: make(chan uint64, 4), release: make(chan struct{})}
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	d := receiveOne(t, c)
	b.setGate(gate)

	extended := make(chan error, 1)
	go func() { extended <- d.Extend(time.Minute) }()
	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the extension never reached the driver")
	}

	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-extended:
		if err == nil {
			t.Fatal("the extension held inside the driver reported success although the consumer closed under it")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the extension was never unblocked by the close")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close never returned")
	}

	log := b.eventLog()
	closeAt, leaveAt := -1, -1
	for i, e := range log {
		switch e {
		case "source-close":
			if closeAt < 0 {
				closeAt = i
			}
		case "extend-leave":
			leaveAt = i
		default:
		}
	}
	if closeAt < 0 || leaveAt < 0 {
		t.Fatalf("the event log is %v, want both an extend-leave and a source-close", log)
	}
	if closeAt < leaveAt {
		t.Fatalf("the source closed at %d and the extension left at %d: an extension was in the driver while its source was being closed (%v)",
			closeAt, leaveAt, log)
	}
}

// TestDeliveryExtendReportsTheHook pins the third settlement kind the specification
// names: SettleExtend, which nothing used to fire.
//
// The event carries the delivery's own topic, subscription and attempt when
// the extension reached the delivery, and the consumer's own when it did not,
// so the metric builds from these has no empty label. It also pins the
// disabled-observer rule from the other side: an extension counts no
// settlement failure, because the specification gives lease extension a metric of its own.
func TestDeliveryExtendReportsTheHook(t *testing.T) {
	var seen []mq.SettleEvent
	hooks := mq.Hooks{OnSettle: func(_ context.Context, e mq.SettleEvent) { seen = append(seen, e) }}
	b := newFakeBroker(fakeConfig{extend: true}, "orders")
	cl := mq.NewClient(b, mq.WithHooks(hooks))
	t.Cleanup(func() {
		if err := cl.Close(); err != nil {
			t.Error(err)
		}
	})
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	d := receiveOne(t, c)

	if err := d.Extend(time.Minute); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 {
		t.Fatalf("the hook saw %d events for one extension", len(seen))
	}
	e := seen[0]
	if e.Kind != mq.SettleExtend {
		t.Errorf("the event's kind is %v, want extend", e.Kind)
	}
	if e.Topic != "orders" || e.Subscription != "billing" || e.Attempt != 1 {
		t.Errorf("the event names topic %q, subscription %q, attempt %d; want the delivery's own",
			e.Topic, e.Subscription, e.Attempt)
	}
	if e.Err != nil {
		t.Errorf("the event's Err = %v for a successful extension", e.Err)
	}
	if st := c.Stats(); st.SettleFailures != 0 {
		t.Errorf("a successful extension counted %d settlement failure(s)", st.SettleFailures)
	}

	// An extension that never reached the delivery still reports an event,
	// and it still names a topic and a subscription.
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
	seen = seen[:0]
	if err := d.Extend(time.Minute); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("extending a settled delivery = %v, want ErrAlreadySettled", err)
	}
	if len(seen) != 1 {
		t.Fatalf("the hook saw %d events for a refused extension", len(seen))
	}
	if e := seen[0]; e.Kind != mq.SettleExtend || e.Topic != "orders" || e.Subscription != "billing" ||
		!errors.Is(e.Err, mq.ErrAlreadySettled) {
		t.Errorf("the refused extension's event is %+v", e)
	}
	if st := c.Stats(); st.SettleFailures != 0 {
		t.Errorf("a refused extension counted %d settlement failure(s); the specification counts extensions separately", st.SettleFailures)
	}
}

// TestDeliveryCloneMessage pins the escape from borrowed storage: the one
// call whose result outlives the delivery.
//
// The storage the clone must not share is the caller's own slice, which is
// where a delivery lives: the next Receive writes over it, and a
// "clone" that pointed into it would hand the caller the next message's body.
func TestDeliveryCloneMessage(t *testing.T) {
	cl, _ := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{BufferReuse: true, Prefetch: 4})
	ctx := context.Background()
	first := &mq.Message{ID: "first", Key: []byte("cust-1"), Body: []byte("first body")}
	first.Headers.Add("x-trace", []byte("t1"))
	if err := cl.Publish(ctx, "orders", first); err != nil {
		t.Fatal(err)
	}
	dst := make([]mq.Delivery, 1)
	if n, err := c.Receive(ctx, dst); n != 1 || err != nil {
		t.Fatalf("Receive = (%d, %v)", n, err)
	}
	clone := dst[0].CloneMessage()
	if clone == nil {
		t.Fatal("CloneMessage returned nil")
	}
	if clone.ID != "first" || string(clone.Body) != "first body" || string(clone.Key) != "cust-1" {
		t.Fatalf("the clone is %+v, want a copy of the delivery's message", clone)
	}
	if got, ok := clone.Headers.Get("x-trace"); !ok || string(got) != "t1" {
		t.Fatalf("the clone's headers = %q (present=%v), want the delivery's", got, ok)
	}
	if &clone.Body[0] == &dst[0].Body[0] {
		t.Fatal("the clone's body shares the delivery's storage: the specification asks for an independent deep copy")
	}
	if &clone.Key[0] == &dst[0].Key[0] {
		t.Fatal("the clone's key shares the delivery's storage")
	}

	// Settle it and let the consumer recycle both the record and the
	// caller's slot, which is what BufferReuse says it may do. The clone
	// carries none of it.
	if err := dst[0].Ack(); err != nil {
		t.Fatal(err)
	}
	second := &mq.Message{ID: "second", Key: []byte("cust-2"), Body: []byte("second body")}
	if err := cl.Publish(ctx, "orders", second); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Receive(ctx, dst); n != 1 || err != nil {
		t.Fatalf("the second Receive = (%d, %v)", n, err)
	}
	if dst[0].ID != "second" {
		t.Fatalf("the slot holds %q, want the second message: the test is not measuring reuse", dst[0].ID)
	}
	if clone.ID != "first" || string(clone.Body) != "first body" || string(clone.Key) != "cust-1" {
		t.Fatalf("the clone is %+v after its delivery was settled and its slot reused, want the first message", clone)
	}
	if err := dst[0].Ack(); err != nil {
		t.Fatal(err)
	}
	// A clone is the caller's: mutating it reaches nothing.
	clone.Body[0] = 'X'
	if string(second.Body) != "second body" {
		t.Fatalf("mutating the clone changed the broker's message: %q", second.Body)
	}
}

// TestDeliveryAccessorsAreSafeFromAnyGoroutine races every accessor against
// the settlement of the delivery it reads, which is the shape the settlement
// event's own race had: one goroutine settling, another reading the record.
//
// The accessors cannot take a settlement's ticket — they are not settlements
// — so what makes them safe is that nothing writes the delivery's metadata
// after it is handed over. This is the test that says so
// under -race; without it, the accessors would have reintroduced exactly the
// hazard the event's capture removed.
func TestDeliveryAccessorsAreSafeFromAnyGoroutine(t *testing.T) {
	const (
		rounds  = 300
		readers = 4
	)
	var reads atomic.Int64
	for round := range rounds {
		cl, _ := openFake(t, fakeConfig{extend: true})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		d := receiveOne(t, c)
		done := make(chan struct{})
		for range readers {
			go func() {
				defer func() { done <- struct{}{} }()
				for range 4 {
					if d.Info().Topic == "orders" {
						reads.Add(1)
					}
					_, _ = mq.NativeAs[*fakeRecord](&d)
					_ = d.CloneMessage()
					_ = d.Extend(time.Minute)
				}
			}()
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("round %d: Ack = %v", round, err)
		}
		for range readers {
			<-done
		}
		if got := d.Info(); got.Topic != "orders" {
			t.Fatalf("round %d: the settled delivery reports topic %q", round, got.Topic)
		}
	}
	if got, want := reads.Load(), int64(rounds*readers*4); got != want {
		t.Fatalf("%d of %d reads saw the delivery's topic", got, want)
	}
}
