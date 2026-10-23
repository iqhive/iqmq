package iqmq_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// errSettleRefused is the fake broker's transient, definite refusal of a
// settlement (fakeBroker.settlerFails).
var errSettleRefused = errors.New("the broker refused the settlement for now")

// TestRunRetriesASettlementTheDriverRefused is the runner keeping the
// promise a definite refusal makes: the delivery is still settleable, so the
// runner offers the settlement again rather than recycling the slot and
// leaving the delivery to a lease expiry.
func TestRunRetriesASettlementTheDriverRefused(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{lease: -1}, "orders")
	b.settlerFails = 1
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	run := startRun(t, c, &collector{}, mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	waitFor(t, "the refused ack to be retried", func() bool { return c.Stats().Acked == 1 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got := c.Stats().InFlight; got != 0 {
		t.Fatalf("InFlight = %d, want 0", got)
	}
	if got := b.counts("orders", "billing"); got.acked != 1 || got.inflight != 0 {
		t.Fatalf("broker counts = %+v, want the one delivery acked", got)
	}
}

// TestRunGivesUpADeliveryItCannotSettle is the bound on that retry: a
// settlement still refused after its retries releases the delivery as lost,
// so the consumer stops counting it in flight and the broker keeps the
// message to redeliver.
func TestRunGivesUpADeliveryItCannotSettle(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{lease: -1}, "orders")
	b.settlerErr = errSettleRefused
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	run := startRun(t, c, &collector{}, mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	waitFor(t, "the unsettleable delivery to leave InFlight", func() bool {
		s := c.Stats()
		return s.Received == 1 && s.InFlight == 0
	})
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got := c.Stats().SettleFailures; got != 3 {
		t.Fatalf("SettleFailures = %d, want 3: the ack and its two retries", got)
	}
	if got := b.counts("orders", "billing"); got.inflight != 1 || got.acked != 0 {
		t.Fatalf("broker counts = %+v, want the message still the broker's", got)
	}
}

// TestRunGivesBackWhatItFetchedWhenTheFetchStops is a fetch loop stopped
// while it waits for room in a full lane. The deliveries it had fetched and
// not yet placed reached no handler, and the consumer stays open after Run
// returns, so nothing else would settle them: they go back to the broker.
func TestRunGivesBackWhatItFetchedWhenTheFetchStops(t *testing.T) {
	const inFlight = 8
	cl, b := openFakeWith(t, fakeConfig{lease: -1}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	for i := range inFlight + 4 {
		// One key, so every delivery routes to the same lane and the fetch
		// loop meets a full one.
		m := &mq.Message{ID: strconv.Itoa(i), Key: []byte("k"), Body: []byte("body")}
		if err := cl.Publish(context.Background(), "orders", m); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	h := mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return nil
	})
	run := startRun(t, c, h, mq.WithConcurrency(2), mq.WithMaxInFlight(inFlight))
	<-entered
	waitFor(t, "the runner to fetch its whole in-flight bound", func() bool {
		return b.counts("orders", "billing").inflight == inFlight
	})
	// The handler is still parked, so the lane is still full: whatever the
	// broker gets back now is what the stopped fetch loop had not placed.
	run.cancel()
	waitFor(t, "the unplaced deliveries to go back", func() bool {
		return b.counts("orders", "billing").requeued > 0
	})
	close(release)
	if err := run.wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got := c.Stats().InFlight; got != 0 {
		t.Fatalf("InFlight = %d after Run returned, want 0: fetched deliveries were abandoned", got)
	}
	got := b.counts("orders", "billing")
	if got.inflight != 0 || !got.conserved() {
		t.Fatalf("broker counts = %+v, want every fetched delivery back in the queue", got)
	}
}

// TestExtendDoesNotReachTheNextDeliveryOfARecycledRecord is an extension in
// flight while its delivery is acked under BufferReuse. The ack releases the
// record, and the next fetch would re-fill it while the driver is still
// reading it: the extension then reads, and extends, a delivery that is not
// its own. The record waits for the extension instead.
func TestExtendDoesNotReachTheNextDeliveryOfARecycledRecord(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{extend: true}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{BufferReuse: true})
	publishN(t, cl, 2)
	first := receiveOne(t, c)
	gate := &settleGate{entered: make(chan uint64, 1), release: make(chan struct{})}
	b.mu.Lock()
	b.extendGate = gate
	b.mu.Unlock()
	extended := make(chan error, 1)
	go func() { extended <- first.Extend(time.Minute) }()
	<-gate.entered
	if err := first.Ack(); err != nil {
		t.Fatalf("acking the first delivery: %v", err)
	}
	second := receiveOne(t, c)
	close(gate.release)
	if err := <-extended; !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("extending the acked delivery = %v, want ErrLeaseLost", err)
	}
	if got := b.counts("orders", "billing").extended; got != 0 {
		t.Fatalf("the broker extended %d leases, want 0: the extension reached the next delivery", got)
	}
	if err := second.Ack(); err != nil {
		t.Fatalf("acking the second delivery: %v", err)
	}
}
