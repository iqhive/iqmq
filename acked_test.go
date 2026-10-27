package iqmq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestDeliveryAckedIsTheAckAlone pins Acked against every verb: true after a
// successful Ack and false after every other successful settlement, which is
// what a middleware that records a delivery as done needs and Settled cannot
// say — Settled is true after a Nack or a Reject as well.
func TestDeliveryAckedIsTheAckAlone(t *testing.T) {
	for _, tc := range []struct {
		name   string
		settle func(d *mq.Delivery) error
		acked  bool
	}{
		{"ack", (*mq.Delivery).Ack, true},
		{"nack", func(d *mq.Delivery) error { return d.Nack() }, false},
		{"drop", func(d *mq.Delivery) error { return d.Nack(mq.NackDrop()) }, false},
		{"reject", (*mq.Delivery).Reject, false},
		{"dead-letter", func(d *mq.Delivery) error { return d.Nack(mq.NackDeadLetter()) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl, _ := openFake(t, fakeConfig{deadLetter: true})
			c := consume(t, cl, mq.ConsumerConfig{})
			publishN(t, cl, 1)
			d := receiveOne(t, c)
			if d.Acked() {
				t.Fatal("Acked is true before anything settled the delivery")
			}
			if err := tc.settle(&d); err != nil {
				t.Fatalf("settling = %v, want nil", err)
			}
			if !d.Settled() {
				t.Fatal("Settled is false after a successful settlement")
			}
			if got := d.Acked(); got != tc.acked {
				t.Fatalf("Acked = %v after a successful %s, want %v", got, tc.name, tc.acked)
			}
			if allocs := testing.AllocsPerRun(100, func() { _ = d.Acked() }); allocs != 0 {
				t.Fatalf("Acked allocates %v times, want 0", allocs)
			}
		})
	}
}

// TestDeliveryAckedIsFalseWhereSettledIs pins the other half of Acked's
// contract: no settlement, a refused one and an unknown one are not an Ack,
// and the verb that counts is the one that succeeded — a refused Ack followed
// by a successful Nack is a Nack.
func TestDeliveryAckedIsFalseWhereSettledIs(t *testing.T) {
	t.Run("no-settlement-right", func(t *testing.T) {
		var d mq.Delivery
		if d.Acked() {
			t.Fatal("a zero Delivery reports Acked")
		}
	})

	t.Run("refused", func(t *testing.T) {
		cl, b := openFake(t, fakeConfig{})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 2)
		dst := make([]mq.Delivery, 2)
		if n, err := c.Receive(context.Background(), dst); err != nil || n != 2 {
			t.Fatalf("Receive = (%d, %v), want (2, nil)", n, err)
		}
		refused := errors.New("the broker refused this settlement")
		b.mu.Lock()
		b.settlerErr = refused
		b.mu.Unlock()
		for i := range dst {
			if err := dst[i].Ack(); !errors.Is(err, refused) {
				t.Fatalf("a refused Ack = %v, want the driver's error", err)
			}
			if dst[i].Settled() || dst[i].Acked() {
				t.Fatal("a refused Ack reported the delivery settled or acked")
			}
		}
		b.mu.Lock()
		b.settlerErr = nil
		b.mu.Unlock()
		if err := dst[0].Ack(); err != nil {
			t.Fatalf("the Ack after the refusal = %v, want nil", err)
		}
		if !dst[0].Acked() {
			t.Fatal("Acked is false after the Ack that followed a refused one succeeded")
		}
		if err := dst[1].Nack(mq.NackDrop()); err != nil {
			t.Fatalf("the Nack after the refused Ack = %v, want nil", err)
		}
		if !dst[1].Settled() || dst[1].Acked() {
			t.Fatalf("a Nack after a refused Ack: Settled = %v, Acked = %v, want true and false",
				dst[1].Settled(), dst[1].Acked())
		}
	})

	t.Run("unknown", func(t *testing.T) {
		cl, b := openFake(t, fakeConfig{})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		d := receiveOne(t, c)
		b.setAmbiguous(1, true)
		if err := d.Ack(); !errors.Is(err, mq.ErrSettlementUnknown) {
			t.Fatalf("an ambiguous Ack = %v, want ErrSettlementUnknown", err)
		}
		if d.Acked() {
			t.Fatal("an Ack whose outcome is unknown reported the delivery acked")
		}
	})
}

// TestDeliveryAckedNeverAnswersForTheRecordsNextDelivery is Acked under
// BufferReuse, where the record of a settled delivery carries the next one.
// While that next delivery is live the first one still has its own answer,
// and once the next one has begun a settlement of its own — here an Ack the
// broker refused, which leaves the record carrying it — its verb must not
// become the first delivery's.
func TestDeliveryAckedNeverAnswersForTheRecordsNextDelivery(t *testing.T) {
	t.Run("acked", func(t *testing.T) {
		cl, _ := openFake(t, fakeConfig{})
		c := consume(t, cl, mq.ConsumerConfig{BufferReuse: true, Prefetch: 1})
		publishN(t, cl, 2)
		first := receiveOne(t, c)
		if err := first.Ack(); err != nil {
			t.Fatalf("Ack = %v", err)
		}
		second := receiveOne(t, c)
		if !first.Acked() {
			t.Fatal("the acked delivery reports unacked once its record carries the next one")
		}
		if second.Acked() || second.Settled() {
			t.Fatal("the next delivery reports the first one's Ack")
		}
	})

	t.Run("nacked", func(t *testing.T) {
		cl, b := openFake(t, fakeConfig{})
		c := consume(t, cl, mq.ConsumerConfig{BufferReuse: true, Prefetch: 1})
		publishN(t, cl, 2)
		first := receiveOne(t, c)
		if err := first.Nack(mq.NackDrop()); err != nil {
			t.Fatalf("Nack = %v", err)
		}
		second := receiveOne(t, c)
		refused := errors.New("the broker refused this settlement")
		b.mu.Lock()
		b.settlerErr = refused
		b.mu.Unlock()
		if err := second.Ack(); !errors.Is(err, refused) {
			t.Fatalf("the next delivery's Ack = %v, want the driver's refusal", err)
		}
		if !first.Settled() {
			t.Fatal("the nacked delivery reports unsettled while its record carries the next one")
		}
		if first.Acked() {
			t.Fatal("the nacked delivery reports the Ack the next delivery of its record attempted")
		}
		b.mu.Lock()
		b.settlerErr = nil
		b.mu.Unlock()
		if err := second.Ack(); err != nil {
			t.Fatalf("the next delivery's Ack = %v", err)
		}
		if first.Acked() {
			t.Fatal("the nacked delivery reports the next delivery's Ack")
		}
		if !second.Acked() {
			t.Fatal("the next delivery does not report its own Ack")
		}
	})
}

// TestDeliveryAckedAnswersForTheHandlersDeliveryUnderBufferReuse is the
// middleware's question asked under BufferReuse, where the fetch loop runs
// beside the handler. The handler settles its own delivery and waits while
// the next fetch takes a record and another handler acks that delivery; then
// the middleware around the first handler asks Settled and Acked. Without the
// runner holding the first record, the next fetch re-filled it, the second
// Ack released it again, and the middleware was answered for that release:
// not settled at all, so the runner acked a delivery the handler had already
// settled, and a middleware committing on "settled and acked, or not settled"
// recorded a dropped message as done.
func TestDeliveryAckedAnswersForTheHandlersDeliveryUnderBufferReuse(t *testing.T) {
	for _, tc := range []struct {
		name   string
		settle func(d *mq.Delivery) error
		acked  bool
	}{
		{"ack", (*mq.Delivery).Ack, true},
		{"drop", func(d *mq.Delivery) error { return d.Nack(mq.NackDrop()) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl, _ := openFakeWith(t, fakeConfig{}, "orders")
			c := consume(t, cl, mq.ConsumerConfig{BufferReuse: true, Prefetch: 1})
			type answer struct{ settled, acked bool }
			answers := make(chan answer, 1)
			mw := func(next mq.Handler) mq.Handler {
				return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
					first := d.ID == "first"
					err := next.Handle(ctx, d)
					if first {
						answers <- answer{settled: d.Settled(), acked: d.Acked()}
					}
					return err
				})
			}
			settled := make(chan struct{})
			secondDone := make(chan struct{})
			h := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
				if d.ID != "first" {
					defer close(secondDone)
					return d.Ack()
				}
				if err := tc.settle(d); err != nil {
					return err
				}
				close(settled)
				select {
				case <-secondDone:
				case <-time.After(runWait):
					t.Error("the second delivery was never handled beside the first")
				}
				return nil
			})
			publish := func(id string) {
				if err := cl.Publish(context.Background(), "orders", &mq.Message{ID: id, Body: []byte("body")}); err != nil {
					t.Fatalf("publishing %s: %v", id, err)
				}
			}
			publish("first")
			run := startRun(t, c, h, mq.WithMiddleware(mw), mq.WithConcurrency(2), mq.WithMaxInFlight(2))
			select {
			case <-settled:
			case <-time.After(runWait):
				t.Fatal("the first handler never settled its delivery")
			}
			publish("second")
			var got answer
			select {
			case got = <-answers:
			case <-time.After(runWait):
				t.Fatal("the middleware never saw the first handler return")
			}
			if !got.settled || got.acked != tc.acked {
				t.Fatalf("after a %s the middleware read Settled = %v and Acked = %v, want true and %v",
					tc.name, got.settled, got.acked, tc.acked)
			}
			waitFor(t, "both deliveries to leave InFlight", func() bool {
				s := c.Stats()
				return s.Received == 2 && s.InFlight == 0
			})
			if err := run.stop(); !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want context.Canceled", err)
			}
			if got := c.Stats().SettleFailures; got != 0 {
				t.Fatalf("SettleFailures = %d, want 0: the runner settled a delivery its handler already had", got)
			}
		})
	}
}

// TestDeliveryAckedAnswersForTheBatchHandlersDeliveryUnderBufferReuse is the
// same question asked by a batch handler of its own deliveries, which the
// batch runner holds until it has settled the batch.
func TestDeliveryAckedAnswersForTheBatchHandlersDeliveryUnderBufferReuse(t *testing.T) {
	for _, tc := range []struct {
		name   string
		settle func(d *mq.Delivery) error
		acked  bool
	}{
		{"ack", (*mq.Delivery).Ack, true},
		{"drop", func(d *mq.Delivery) error { return d.Nack(mq.NackDrop()) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl, _ := openFakeWith(t, fakeConfig{}, "orders")
			c := consume(t, cl, mq.ConsumerConfig{BufferReuse: true, Prefetch: 1})
			type answer struct{ settled, acked bool }
			answers := make(chan answer, 1)
			settled := make(chan struct{})
			secondDone := make(chan struct{})
			h := mq.BatchHandlerFunc(func(_ context.Context, ds []*mq.Delivery) error {
				d := ds[0]
				if d.ID != "first" {
					defer close(secondDone)
					return d.Ack()
				}
				if err := tc.settle(d); err != nil {
					return err
				}
				close(settled)
				select {
				case <-secondDone:
				case <-time.After(runWait):
					t.Error("the second batch was never handled beside the first")
				}
				answers <- answer{settled: d.Settled(), acked: d.Acked()}
				return nil
			})
			publish := func(id string) {
				if err := cl.Publish(context.Background(), "orders", &mq.Message{ID: id, Body: []byte("body")}); err != nil {
					t.Fatalf("publishing %s: %v", id, err)
				}
			}
			publish("first")
			run := startRunBatch(t, c, h, mq.WithConcurrency(2), mq.WithMaxInFlight(2))
			select {
			case <-settled:
			case <-time.After(runWait):
				t.Fatal("the first batch handler never settled its delivery")
			}
			publish("second")
			var got answer
			select {
			case got = <-answers:
			case <-time.After(runWait):
				t.Fatal("the first batch handler never answered")
			}
			if !got.settled || got.acked != tc.acked {
				t.Fatalf("after a %s the batch handler read Settled = %v and Acked = %v, want true and %v",
					tc.name, got.settled, got.acked, tc.acked)
			}
			waitFor(t, "both deliveries to leave InFlight", func() bool {
				s := c.Stats()
				return s.Received == 2 && s.InFlight == 0
			})
			if err := run.stop(); !errors.Is(err, context.Canceled) {
				t.Fatalf("RunBatch = %v, want context.Canceled", err)
			}
			if got := c.Stats().SettleFailures; got != 0 {
				t.Fatalf("SettleFailures = %d, want 0: the runner settled a delivery its handler already had", got)
			}
		})
	}
}
