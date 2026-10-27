package iqmq_test

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// The settlement rules of the message contract and, one test each.
//

// TestSettlementExactlyOneWinner pins the specification's "exactly one settlement wins",
// sequentially and under a race.
//
// The sequential half is the plain double settlement. The concurrent half
// runs many goroutines at one delivery and requires that exactly one of them
// is told nil: the others may be told ErrAlreadySettled or
// ErrSettlementInProgress depending on whether the winner had finished, and
// nothing may be told anything else.
func TestSettlementExactlyOneWinner(t *testing.T) {
	t.Run("sequential", func(t *testing.T) {
		cl, b := openFake(t, fakeConfig{})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		d := receiveOne(t, c)
		if err := d.Ack(); err != nil {
			t.Fatalf("the first Ack = %v, want nil", err)
		}
		if err := d.Ack(); !errors.Is(err, mq.ErrAlreadySettled) {
			t.Fatalf("the second Ack = %v, want ErrAlreadySettled", err)
		}
		if err := d.Nack(); !errors.Is(err, mq.ErrAlreadySettled) {
			t.Fatalf("a Nack after the Ack = %v, want ErrAlreadySettled", err)
		}
		if err := d.Reject(); !errors.Is(err, mq.ErrAlreadySettled) {
			t.Fatalf("a Reject after the Ack = %v, want ErrAlreadySettled", err)
		}
		if !d.Settled() {
			t.Fatal("Settled reported false after a successful Ack")
		}
		if got := b.counts("orders", "billing"); got.acked != 1 {
			t.Fatalf("the broker counted %d acks: more than one settlement reached it", got.acked)
		}
		if got := c.Stats(); got.Acked != 1 || got.InFlight != 0 {
			t.Fatalf("Stats = %+v, want 1 acked and 0 in flight — a release that ran twice shows up here", got)
		}
	})

	t.Run("concurrent", func(t *testing.T) {
		const racers = 8
		for round := range 200 {
			cl, b := openFake(t, fakeConfig{})
			c := consume(t, cl, mq.ConsumerConfig{})
			publishN(t, cl, 1)
			d := receiveOne(t, c)
			var start sync.WaitGroup
			start.Add(1)
			var done sync.WaitGroup
			results := make([]error, racers)
			for i := range racers {
				done.Add(1)
				go func() {
					defer done.Done()
					start.Wait()
					if i%2 == 0 {
						results[i] = d.Ack()
					} else {
						results[i] = d.Nack()
					}
				}()
			}
			start.Done()
			done.Wait()
			winners := 0
			for i, err := range results {
				switch {
				case err == nil:
					winners++
				case errors.Is(err, mq.ErrAlreadySettled), errors.Is(err, mq.ErrSettlementInProgress):
				default:
					t.Fatalf("round %d racer %d = %v, want nil, ErrAlreadySettled or ErrSettlementInProgress", round, i, err)
				}
			}
			if winners != 1 {
				t.Fatalf("round %d: %d of %d settlements were told they won", round, winners, racers)
			}
			settled := b.counts("orders", "billing")
			if settled.acked+settled.requeued != 1 {
				t.Fatalf("round %d: the broker saw %d acks and %d requeues, want exactly one settlement",
					round, settled.acked, settled.requeued)
			}
			if got := c.Stats(); got.InFlight != 0 {
				t.Fatalf("round %d: InFlight = %d, want 0 — a release ran twice or not at all", round, got.InFlight)
			}
			if err := cl.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
}

// TestSettlementInProgress pins ErrSettlementInProgress: while one
// terminal attempt is in flight in the driver, a second is refused rather
// than being sent as well.
//
// It is the rule the gate exists for: without holding a settlement inside
// the driver, the window is too narrow to enter deliberately.
func TestSettlementInProgress(t *testing.T) {
	cl, b := openFake(t, fakeConfig{})
	gate := &settleGate{entered: make(chan uint64, 4), release: make(chan struct{})}
	b.setGate(gate)
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	d := receiveOne(t, c)

	first := make(chan error, 1)
	go func() { first <- d.Ack() }()
	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first settlement never reached the driver")
	}
	if err := d.Ack(); !errors.Is(err, mq.ErrSettlementInProgress) {
		t.Fatalf("a settlement while one is in flight = %v, want ErrSettlementInProgress", err)
	}
	if d.Settled() {
		t.Fatal("Settled reported true while the settlement was still in flight")
	}
	close(gate.release)
	if err := <-first; err != nil {
		t.Fatalf("the settlement that won = %v, want nil", err)
	}
	if err := d.Ack(); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("a settlement after the winner finished = %v, want ErrAlreadySettled", err)
	}
	if got := b.counts("orders", "billing"); got.acked != 1 {
		t.Fatalf("the broker counted %d acks, want 1", got.acked)
	}
}

// TestSettlementLeaseLost pins the specification's "stale tokens cannot settle newer
// deliveries".
//
// The record is redelivered under a new generation while the first
// delivery's handle is still held, and settling that handle must fail rather
// than acknowledge the redelivery — which is the disqualifier "lets a
// stale ack settle a later redelivery". The driver holds the generation the
// broker knows about, so this is the rule as a real driver enforces it; the
// core then takes the delivery out of circulation, so a second attempt is
// answered without the driver being asked again.
func TestSettlementLeaseLost(t *testing.T) {
	cl, b := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	stale := receiveOne(t, c)

	// The lease expires and the record is redelivered under a new
	// generation.
	b.expire("orders", "billing")
	fresh := receiveOne(t, c)
	if fresh.Attempt != 2 {
		t.Fatalf("the redelivery is attempt %d, want 2", fresh.Attempt)
	}

	if err := stale.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("acking the stale delivery = %v, want ErrLeaseLost", err)
	}
	if stale.Settled() {
		t.Fatal("the stale delivery reported itself settled")
	}
	if got := b.counts("orders", "billing"); got.acked != 0 {
		t.Fatalf("the broker counted %d acks: the stale settlement reached it", got.acked)
	}
	// The core released the stale delivery, so the second attempt is
	// answered above the driver — and with the same rule.
	if err := stale.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a second ack of the stale delivery = %v, want ErrLeaseLost", err)
	}
	if err := fresh.Ack(); err != nil {
		t.Fatalf("acking the redelivery = %v, want nil", err)
	}
	if got := c.Stats(); got.InFlight != 0 {
		t.Fatalf("InFlight = %d, want 0: a lost lease releases the delivery too", got.InFlight)
	}
	if got := b.counts("orders", "billing"); got.acked != 1 || !got.conserved() {
		t.Fatalf("the broker holds %+v, want 1 acked and conservation", got)
	}
}

// TestSettlementGoneReleasesOnce pins the terminal-release invariant for the
// two driver answers that take a delivery out of circulation: its lease is
// gone, or its source has closed.
//
// The core used to put the cell back in Active and only then revoke or close
// it, and it released the record whatever the revoke or close reported. A
// settlement racing the first could begin in that moment, reach the driver
// with a lease the driver had already called gone, and release the record a
// second time: InFlight went to -1 and, under BufferReuse, the free list held
// the record twice, so one record backed two deliveries and settling one sent
// the other's token. The racers spin on the delivery while the first
// settlement is in the driver, so that one of them is ready to begin the
// moment the cell would be Active again.
func TestSettlementGoneReleasesOnce(t *testing.T) {
	const (
		rounds = 1000
		racers = 4
	)
	for _, tc := range []struct {
		name  string
		err   error
		reuse bool
	}{
		{"lease-lost", mq.ErrLeaseLost, false},
		{"lease-lost/buffer-reuse", mq.ErrLeaseLost, true},
		{"closed", mq.ErrClosed, false},
		{"closed/buffer-reuse", mq.ErrClosed, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for round := range rounds {
				cl, b := openFake(t, fakeConfig{})
				c := consume(t, cl, mq.ConsumerConfig{BufferReuse: tc.reuse})
				publishN(t, cl, 1)
				d := receiveOne(t, c)
				b.mu.Lock()
				b.settlerErr = tc.err
				b.mu.Unlock()

				var start, done sync.WaitGroup
				start.Add(1)
				results := make([]error, racers)
				deadline := time.Now().Add(5 * time.Second)
				for i := range racers {
					done.Go(func() {
						start.Wait()
						for time.Now().Before(deadline) {
							results[i] = d.Ack()
							if !errors.Is(results[i], mq.ErrSettlementInProgress) {
								return
							}
						}
					})
				}
				start.Done()
				done.Wait()

				for i, err := range results {
					if !errors.Is(err, tc.err) {
						t.Fatalf("round %d racer %d = %v, want %v", round, i, err, tc.err)
					}
				}
				if got := c.Stats(); got.InFlight != 0 {
					t.Fatalf("round %d: InFlight = %d, want 0: the delivery was released more than once", round, got.InFlight)
				}
				entered := 0
				for _, e := range b.eventLog() {
					if e == "settle-enter" {
						entered++
					}
				}
				if entered != 1 {
					t.Fatalf("round %d: %d settlements reached the driver, want 1: one began after the driver said the delivery was gone",
						round, entered)
				}
				if err := cl.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// TestSettlementAfterCloseIsErrClosed pins the specification's "after close all ops
// return ErrClosed", for the consumer's own close and for the client's.
func TestSettlementAfterCloseIsErrClosed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		close func(cl *mq.Client, c *mq.ConsumerHandle) error
	}{
		{"consumer", func(_ *mq.Client, c *mq.ConsumerHandle) error { return c.Close() }},
		{"client", func(cl *mq.Client, _ *mq.ConsumerHandle) error { return cl.Close() }},
		{"consumershutdown", func(_ *mq.Client, c *mq.ConsumerHandle) error {
			return c.Shutdown(context.Background())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl, b := openFake(t, fakeConfig{})
			c := consume(t, cl, mq.ConsumerConfig{})
			publishN(t, cl, 2)
			d := receiveOne(t, c)
			if err := tc.close(cl, c); err != nil {
				t.Fatalf("closing: %v", err)
			}
			if err := d.Ack(); !errors.Is(err, mq.ErrClosed) {
				t.Fatalf("Ack after close = %v, want ErrClosed", err)
			}
			if err := d.Nack(); !errors.Is(err, mq.ErrClosed) {
				t.Fatalf("Nack after close = %v, want ErrClosed", err)
			}
			dst := make([]mq.Delivery, 1)
			if n, err := c.Receive(context.Background(), dst); n != 0 || !errors.Is(err, mq.ErrClosed) {
				t.Fatalf("Receive after close = (%d, %v), want (0, ErrClosed)", n, err)
			}
			if got := c.Stats(); got.InFlight != 0 {
				t.Fatalf("InFlight = %d after close, want 0: a closed consumer holds nothing", got.InFlight)
			}
			// The specification: closing requeues what was never settled, so the message
			// is redelivered rather than lost.
			got := b.counts("orders", "billing")
			if got.acked != 0 || got.inflight != 0 || got.ready != 2 || !got.conserved() {
				t.Fatalf("the broker holds %+v, want 0 acked, 0 in flight, 2 ready", got)
			}
		})
	}
}

// TestSettlementAfterCloseIsTheAPIsRule is the half of the rule above that
// the driver cannot be credited with: the settler here acknowledges anything
// it is handed, including a delivery whose source has closed, and the API
// still answers ErrClosed and the broker still counts no ack.
//
// Without it the ErrClosed test is satisfied by the fake's own refusal and
// would pass for a core with no rule at all — the vacuity this repository
// has found in eight gates.
func TestSettlementAfterCloseIsTheAPIsRule(t *testing.T) {
	cl, b := openFake(t, fakeConfig{settlePermissive: true})
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	d := receiveOne(t, c)
	// The permissive driver acknowledges while the consumer is open, so the
	// test is measuring the close and not the driver.
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack on the permissive driver = %v, want nil", err)
	}
	publishN(t, cl, 1)
	second := receiveOne(t, c)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	before := b.counts("orders", "billing").acked
	if err := second.Ack(); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Ack after close on a driver that would have accepted it = %v, want ErrClosed", err)
	}
	if after := b.counts("orders", "billing").acked; after != before {
		t.Fatalf("the driver was told to ack after the consumer closed (%d acks, was %d)", after, before)
	}
}

// TestSettlementNotSettleable pins AckNone model and the delivery a
// caller built itself: neither can be settled, and both say so.
func TestSettlementNotSettleable(t *testing.T) {
	t.Run("acknone", func(t *testing.T) {
		cl, b := openFake(t, fakeConfig{noSettler: true})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		d := receiveOne(t, c)
		if err := d.Ack(); !errors.Is(err, mq.ErrNotSettleable) {
			t.Fatalf("Ack on a source with no settler = %v, want ErrNotSettleable", err)
		}
		if err := d.Nack(); !errors.Is(err, mq.ErrNotSettleable) {
			t.Fatalf("Nack on a source with no settler = %v, want ErrNotSettleable", err)
		}
		if err := d.Reject(); !errors.Is(err, mq.ErrNotSettleable) {
			t.Fatalf("Reject on a source with no settler = %v, want ErrNotSettleable", err)
		}
		if d.Settled() {
			t.Fatal("a delivery that cannot be settled reported itself settled")
		}
		if got := b.counts("orders", "billing"); got.acked != 0 {
			t.Fatalf("the broker counted %d acks for a non-settleable source", got.acked)
		}
	})
	t.Run("handbuilt", func(t *testing.T) {
		var d mq.Delivery
		if err := d.Ack(); !errors.Is(err, mq.ErrNotSettleable) {
			t.Fatalf("acking the zero Delivery = %v, want ErrNotSettleable", err)
		}
		d = mq.Delivery{Message: mq.Message{Body: []byte("built by hand")}, Attempt: 1}
		if err := d.Nack(); !errors.Is(err, mq.ErrNotSettleable) {
			t.Fatalf("nacking a hand-built Delivery = %v, want ErrNotSettleable", err)
		}
		if d.Settled() {
			t.Fatal("a hand-built Delivery reported itself settled")
		}
	})
}

// TestSettlementUnknownIsSticky pins the rule the row calls out: once a
// settlement's outcome is unknown it stays unknown and never resolves itself
// into success.
//
// The broker answers the first attempt ambiguously and is then made healthy
// again. Every later attempt still reports ErrSettlementUnknown — the root
// exports no way to leave the unknown state, so a retry cannot turn an
// ambiguous round trip into an acknowledgement — the delivery never reports
// itself settled, and the record stays in flight at the broker, which is
// what "unknown" means.
func TestSettlementUnknownIsSticky(t *testing.T) {
	cl, b := openFake(t, fakeConfig{deadLetter: true})
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	d := receiveOne(t, c)

	b.setAmbiguous(1, true)
	if err := d.Ack(); !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("an ambiguous Ack = %v, want ErrSettlementUnknown", err)
	}
	if d.Settled() {
		t.Fatal("an ambiguous settlement reported the delivery settled")
	}
	// The broker recovers. Nothing about the delivery does.
	b.setAmbiguous(1, false)
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"ack", d.Ack},
		{"nack", func() error { return d.Nack() }},
		{"reject", d.Reject},
	} {
		if err := tc.call(); !errors.Is(err, mq.ErrSettlementUnknown) {
			t.Fatalf("%s after an ambiguous settlement = %v, want ErrSettlementUnknown", tc.name, err)
		}
		if d.Settled() {
			t.Fatalf("%s after an ambiguous settlement reported the delivery settled", tc.name)
		}
	}
	got := b.counts("orders", "billing")
	if got.acked != 0 || got.inflight != 1 {
		t.Fatalf("the broker holds %+v, want 0 acked and 1 still in flight", got)
	}
	if c.Stats().SettleFailures == 0 {
		t.Fatal("an ambiguous settlement was not counted as a settlement failure")
	}
}

// TestSettlementRefusalKeepsTheDeliverySettleable pins the other half of the state machine: a definite rejection returns the delivery to
// "active", so the caller may try again — the difference between a refusal
// and an ambiguity is exactly whether a retry is allowed.
func TestSettlementRefusalKeepsTheDeliverySettleable(t *testing.T) {
	cl, b := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	d := receiveOne(t, c)

	refused := errors.New("the broker refused this settlement")
	b.mu.Lock()
	b.settlerErr = refused
	b.mu.Unlock()
	if err := d.Ack(); !errors.Is(err, refused) {
		t.Fatalf("a refused Ack = %v, want the driver's own error", err)
	}
	if d.Settled() {
		t.Fatal("a refused settlement reported the delivery settled")
	}
	b.mu.Lock()
	b.settlerErr = nil
	b.mu.Unlock()
	if err := d.Ack(); err != nil {
		t.Fatalf("the retry after a definite refusal = %v, want nil", err)
	}
	if !d.Settled() {
		t.Fatal("the delivery did not report itself settled after the retry succeeded")
	}
	if got := b.counts("orders", "billing"); got.acked != 1 || !got.conserved() {
		t.Fatalf("the broker holds %+v, want 1 acked and conservation", got)
	}
}

// TestSettlementWaitsForCloseOrder pins the shutdown order against the
// defect class the contributor notes calls the engine's recurring one: a
// settlement that reaches a driver after the driver's source has been
// closed.
//
// A settlement is held inside the driver, the consumer is closed, and the
// event log has to show the source closing AFTER the settlement left. The
// lifecycle's gate is what makes that true: the Committing phase bars the
// settlement class and waits for every admitted settlement to leave before
// the Closed phase closes the source.
func TestSettlementWaitsForCloseOrder(t *testing.T) {
	cl, b := openFake(t, fakeConfig{})
	gate := &settleGate{entered: make(chan uint64, 4), release: make(chan struct{})}
	b.setGate(gate)
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	d := receiveOne(t, c)

	settled := make(chan error, 1)
	go func() { settled <- d.Ack() }()
	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the settlement never reached the driver")
	}

	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()

	// The close cancels the handle's settlement context, so the settlement
	// parked in the driver comes back rather than holding the shutdown for
	// ever — the specification's "Close unblocks blocked operations".
	select {
	case err := <-settled:
		if err == nil {
			t.Fatal("the settlement held inside the driver reported success although the consumer closed under it")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the settlement was never unblocked by the close")
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
		case "settle-leave":
			leaveAt = i
		default:
		}
	}
	if closeAt < 0 || leaveAt < 0 {
		t.Fatalf("the event log is %v, want both a settle-leave and a source-close", log)
	}
	if closeAt < leaveAt {
		t.Fatalf("the source closed at %d and the settlement left at %d: a settlement was in the driver while its source was being closed (%v)",
			closeAt, leaveAt, log)
	}
}

// TestSettlementRacesClose runs the placement against the close for
// thousands of rounds, which is what the contributor notes asks of any route that
// hands a record over while the handle may be closing: the known instances
// of this defect appeared at five to twenty failures per two or three
// thousand rounds, so a hundred rounds proves nothing.
//
// What it checks after every round is the conservation: every published
// record is acked, ready or in flight at the broker, and never two of those
// and never none. A delivery handed to a caller that nothing can settle and
// nothing requeues would show up here as a record in flight after the
// source closed.
func TestSettlementRacesClose(t *testing.T) {
	const rounds = 3000
	for round := range rounds {
		b := newFakeBroker(fakeConfig{}, "orders")
		cl := mq.NewClient(b)
		c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing", Prefetch: 4})
		if err != nil {
			t.Fatal(err)
		}
		const messages = 4
		for i := range messages {
			if err := cl.Publish(context.Background(), "orders", &mq.Message{ID: strconv.Itoa(i), Body: []byte("b")}); err != nil {
				t.Fatalf("round %d: publish: %v", round, err)
			}
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			dst := make([]mq.Delivery, messages)
			for {
				n, err := c.Receive(context.Background(), dst)
				for i := range n {
					// The settlement may be refused for every reason the specification
					// names; what may not happen is a record nobody owns.
					_ = dst[i].Ack()
				}
				if err != nil {
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			runtime.Gosched()
			if err := c.Close(); err != nil {
				t.Errorf("round %d: Close: %v", round, err)
			}
		}()
		wg.Wait()
		got := b.counts("orders", "billing")
		if !got.conserved() {
			t.Fatalf("round %d: the broker holds %+v: the conservation is broken", round, got)
		}
		if got.inflight != 0 {
			t.Fatalf("round %d: %d records are still in flight after the source closed: a delivery was handed over that nothing released",
				round, got.inflight)
		}
		if got.acked+got.ready != messages {
			t.Fatalf("round %d: %d acked and %d ready, want %d between them", round, got.acked, got.ready, messages)
		}
		if st := c.Stats(); st.InFlight != 0 {
			t.Fatalf("round %d: the consumer reports %d in flight after close", round, st.InFlight)
		}
		if err := cl.Close(); err != nil {
			t.Fatalf("round %d: closing the client: %v", round, err)
		}
	}
}

// TestSettlementNoGoroutinesRemain pins the last clause of the invariant
// list: after close, no goroutines remain.
//
// The root module is stdlib-only, so this is a goroutine count rather than
// goleak: it samples before and after and allows the shutdown's own
// goroutine a moment to finish, which is what internal/lifecycle's Close
// joins.
func TestSettlementNoGoroutinesRemain(t *testing.T) {
	before := runtime.NumGoroutine()
	for range 50 {
		b := newFakeBroker(fakeConfig{}, "orders")
		cl := mq.NewClient(b)
		c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
		if err != nil {
			t.Fatal(err)
		}
		p, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders"})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.Publish(context.Background(), &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		d := receiveOne(t, c)
		if err := d.Ack(); err != nil {
			t.Fatal(err)
		}
		if err := cl.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		after := runtime.NumGoroutine()
		if after <= before+2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines before, %d after 50 client lifetimes", before, after)
		}
		runtime.Gosched()
	}
}

// TestSettlementBufferReuseRecyclesRecords pins the reuse mode: the
// settlement storage comes back after a settlement, and the rules do not
// change because it did.
//
// A Delivery must not be retained past its settlement in this mode, and the
// mode's contract is what the allocation test measures; what is checked here
// is that recycling cannot make a settled delivery look unsettled, or a
// second settlement look like a first.
func TestSettlementBufferReuseRecyclesRecords(t *testing.T) {
	cl, b := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{BufferReuse: true, Prefetch: 4})
	dst := make([]mq.Delivery, 4)
	for round := range 20 {
		publishN(t, cl, 4)
		n, err := c.Receive(context.Background(), dst)
		if err != nil || n != 4 {
			t.Fatalf("round %d: Receive = (%d, %v)", round, n, err)
		}
		for i := range n {
			if err := dst[i].Ack(); err != nil {
				t.Fatalf("round %d delivery %d: Ack = %v", round, i, err)
			}
			if !dst[i].Settled() {
				t.Fatalf("round %d delivery %d: Settled is false after a successful Ack on a recycled record", round, i)
			}
			if err := dst[i].Ack(); !errors.Is(err, mq.ErrAlreadySettled) {
				t.Fatalf("round %d delivery %d: the second Ack = %v, want ErrAlreadySettled", round, i, err)
			}
		}
		if st := c.Stats(); st.InFlight != 0 {
			t.Fatalf("round %d: InFlight = %d, want 0", round, st.InFlight)
		}
	}
	if got := b.counts("orders", "billing"); got.acked != 80 || !got.conserved() {
		t.Fatalf("the broker holds %+v, want 80 acked and conservation", got)
	}
}

// TestSettlementBufferReuseBindsTheGeneration pins the hand-over the
// runner relies on under BufferReuse: a handler that settles its own delivery
// returns the record to the free list, the next fetch claims it for another
// message, and the runner's Settled check and settlement that follow must
// still answer for the handler's delivery. Before the Delivery carried its
// generation they read the record's next delivery instead, saw it unsettled
// and acked it before its handler had run.
func TestSettlementBufferReuseBindsTheGeneration(t *testing.T) {
	cl, b := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{BufferReuse: true, Prefetch: 1})
	publishN(t, cl, 1)
	first := receiveOne(t, c)
	if err := first.Ack(); err != nil {
		t.Fatalf("Ack = %v", err)
	}
	publishN(t, cl, 1)
	second := receiveOne(t, c)

	if !first.Settled() {
		t.Fatal("the acked delivery reports unsettled once its record carries the next one")
	}
	if err := first.Ack(); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("a second Ack of the acked delivery = %v, want ErrAlreadySettled", err)
	}
	if err := first.Extend(time.Second); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("Extend of the acked delivery = %v, want ErrAlreadySettled", err)
	}
	if second.Settled() {
		t.Fatal("the next delivery was settled through the acked one's handle")
	}
	if got := b.counts("orders", "billing"); got.acked != 1 || got.extended != 0 {
		t.Fatalf("the broker holds %+v, want 1 acked and nothing extended", got)
	}
	if err := second.Ack(); err != nil {
		t.Fatalf("Ack of the next delivery = %v", err)
	}
	if got := b.counts("orders", "billing"); got.acked != 2 || !got.conserved() {
		t.Fatalf("the broker holds %+v, want 2 acked and conservation", got)
	}
}

// TestSettlementEventDoesNotRaceTheRecord pins the ONE dimension the tests
// above miss: an OnSettle hook.
//
// TestSettlementExactlyOneWinner already races N settlers at one delivery and
// TestSettlementBufferReuseRecyclesRecords already recycles records, but
// neither installs a hook — so neither ever built a SettleEvent, and the
// event's fields used to be read from the very struct the winner's release
// overwrites and the next fetch re-fills. Two settlers plus a hook is a data
// race on a non-atomic struct field; three tests passed over it because the
// three dimensions never met.
//
// The hook reads every field of the event it is handed, so nothing here is
// dead code. The two arms are the two writers of that struct: in safe mode
// the winner's release is the only one, and under BufferReuse the fetch that
// claims the released record out of the free list is a second, running while
// the losers are still building their events.
func TestSettlementEventDoesNotRaceTheRecord(t *testing.T) {
	const (
		rounds   = 200
		racers   = 8
		messages = 4
	)
	for _, tc := range []struct {
		name  string
		reuse bool
	}{
		{"safe", false},
		// The reuse arm deliberately retains a Delivery past its
		// settlement, which the specification forbids and the specification records the
		// consequence of: a racer may settle the NEXT delivery of a
		// recycled record, so this arm cannot count winners. It is here for
		// the writer, not for the count.
		{"reuse", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var events, named atomic.Int64
			hooks := mq.Hooks{OnSettle: func(_ context.Context, e mq.SettleEvent) {
				events.Add(1)
				if e.Topic != "" && e.Subscription != "" && e.Kind.Valid() && e.Attempt >= 0 {
					named.Add(1)
				}
			}}
			for round := range rounds {
				b := newFakeBroker(fakeConfig{}, "orders")
				cl := mq.NewClient(b, mq.WithHooks(hooks))
				c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{
					Topic: "orders", Subscription: "billing",
					Prefetch: messages, BufferReuse: tc.reuse,
				})
				if err != nil {
					t.Fatal(err)
				}
				publishN(t, cl, 1)
				d := receiveOne(t, c)

				var start, done sync.WaitGroup
				start.Add(1)
				results := make([]error, racers)
				for i := range racers {
					done.Add(1)
					go func() {
						defer done.Done()
						start.Wait()
						results[i] = d.Ack()
					}()
				}
				if tc.reuse {
					// The fetch that recycles: the settlement above returns
					// its record to the consumer's free list and these
					// deliveries claim it again, while the losing settlers
					// are still building their events.
					done.Add(1)
					go func() {
						defer done.Done()
						start.Wait()
						dst := make([]mq.Delivery, messages)
						for range messages {
							if err := cl.Publish(context.Background(), "orders", &mq.Message{Body: []byte("b")}); err != nil {
								return
							}
							n, ferr := c.Receive(context.Background(), dst)
							for i := range n {
								_ = dst[i].Ack()
							}
							if ferr != nil {
								return
							}
						}
					}()
				}
				start.Done()
				done.Wait()

				winners := 0
				for i, err := range results {
					switch {
					case err == nil:
						winners++
					case errors.Is(err, mq.ErrAlreadySettled), errors.Is(err, mq.ErrSettlementInProgress),
						errors.Is(err, mq.ErrLeaseLost), errors.Is(err, mq.ErrClosed):
					default:
						t.Fatalf("round %d racer %d = %v", round, i, err)
					}
				}
				if !tc.reuse && winners != 1 {
					t.Fatalf("round %d: %d of %d settlements won", round, winners, racers)
				}
				if err := cl.Close(); err != nil {
					t.Fatal(err)
				}
			}
			// Every settlement reports one event, and every event names a
			// topic, a subscription and a kind: a failure that never reached
			// the driver carries the consumer's own, so the metric
			// builds from these has no empty label.
			if got, want := events.Load(), int64(rounds*racers); got < want {
				t.Fatalf("the hook saw %d events for %d settlements", got, want)
			}
			if got, want := named.Load(), events.Load(); got != want {
				t.Fatalf("%d of %d events named a topic, a subscription and a kind", got, want)
			}
		})
	}
}
