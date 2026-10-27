package iqmq_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
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

// recordingBackoff records the attempt every delay was asked for, and asks
// for no wait.
type recordingBackoff struct {
	mu       sync.Mutex
	attempts []int
}

// Delay implements mq.Backoff.
func (b *recordingBackoff) Delay(attempt int) time.Duration {
	b.mu.Lock()
	b.attempts = append(b.attempts, attempt)
	b.mu.Unlock()
	return 0
}

// TestRunSpacesEverySettlementRetry is the retry's backoff counted from the
// settlement's own attempts: the first retry is the second attempt, and
// Backoff.Delay(1) is no wait at all for every backoff this package ships,
// so asking for attempt 1 made the first retry immediate.
func TestRunSpacesEverySettlementRetry(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{lease: -1}, "orders")
	b.settlerFails = 2
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	bo := &recordingBackoff{}
	run := startRun(t, c, &collector{}, mq.WithRetryBackoff(bo))
	waitFor(t, "the twice refused ack to be retried", func() bool { return c.Stats().Acked == 1 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	bo.mu.Lock()
	got := append([]int(nil), bo.attempts...)
	bo.mu.Unlock()
	if len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("the settlement retries asked the backoff for attempts %v, want [2 3]", got)
	}
}

// TestRunAcceptsAZeroMaxInFlight is WithMaxInFlight(0) doing what its
// documentation says: falling back to ConsumerConfig.MaxInFlight rather than
// being refused as an invalid bound.
func TestRunAcceptsAZeroMaxInFlight(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	h := &collector{}
	run := startRun(t, c, h, mq.WithMaxInFlight(0))
	// A refused option returns from Run at once, so either is an answer.
	waitFor(t, "the delivery to be handled", func() bool { return h.count() == 1 || len(run.done) == 1 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if h.count() != 1 {
		t.Fatalf("the handler ran %d times, want 1", h.count())
	}
}

// TestRunLeavesInFlightAtZeroWhenTheConsumerClosesUnderAHandler is a
// consumer closed while a runner's handler is still running. The close
// zeroes InFlight, and the handler's ack then answers ErrClosed: that is not
// a refusal worth retrying, and giving the delivery up as lost would count
// it out a second time — InFlight -1 — and, under BufferReuse, hand its
// record to the closed consumer's free list.
func TestRunLeavesInFlightAtZeroWhenTheConsumerClosesUnderAHandler(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{lease: -1}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{BufferReuse: true})
	publishN(t, cl, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h := mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		once.Do(func() { close(entered) })
		<-release
		return nil
	})
	run := startRun(t, c, h, mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	<-entered
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(release)
	_ = run.wait()
	s := c.Stats()
	if s.InFlight != 0 {
		t.Fatalf("InFlight = %d after the close, want 0", s.InFlight)
	}
	if s.SettleFailures != 1 {
		t.Fatalf("SettleFailures = %d, want 1: a closed consumer's refusal was retried", s.SettleFailures)
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
