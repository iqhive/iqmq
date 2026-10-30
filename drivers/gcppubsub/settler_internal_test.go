package gcppubsub

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// TestAnExpiredDeliveryLeavesTheInflightMap holds that a delivery the local
// lease check finds lost is forgotten as well: a lost delivery is terminal, so
// the in-flight map keeping it is a leak of one entry per expired message.
func TestAnExpiredDeliveryLeavesTheInflightMap(t *testing.T) {
	src := &source{b: &Backend{}, inflight: map[string]*delivery{}}
	set := newSettler(src)
	expired := func(id string) mq.DriverDelivery {
		ref := &delivery{id: id, src: src}
		ref.expires.Store(1)
		src.supersede(ref)
		return mq.DriverDelivery{Ref: ref}
	}
	claimed, extended := expired("claimed"), expired("extended")
	if _, err := set.claim(&claimed); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("claim of an expired delivery returned %v, want ErrLeaseLost", err)
	}
	if err := set.Extend(t.Context(), &extended, time.Second); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Extend of an expired delivery returned %v, want ErrLeaseLost", err)
	}
	src.mu.Lock()
	n := len(src.inflight)
	src.mu.Unlock()
	if n != 0 {
		t.Fatalf("the in-flight map holds %d lost deliveries, want 0", n)
	}
}

// TestAnAmbiguousSettlementLeavesTheInflightMap holds that an exactly-once
// acknowledgement whose answer never came is forgotten as well. The unknown
// state is terminal: the library has sent the one settlement it sends for the
// message, so if it landed the message never comes back, and the in-flight map
// kept the delivery, with the whole message, until the source closed. Leaving
// the map does not make it settleable again: a second settlement still reports
// ErrSettlementUnknown, and a redelivery of the message does not change that.
func TestAnAmbiguousSettlementLeavesTheInflightMap(t *testing.T) {
	src := leaseSource(t, 1, time.Minute)
	src.info.ExactlyOnce = true
	rec := &recordedSettlement{refuse: context.DeadlineExceeded}
	ref := &delivery{id: "m", gen: 1, src: src, settle: rec}
	ref.received.Store(time.Now().UnixNano())
	ref.expires.Store(time.Now().Add(time.Minute).UnixNano())
	src.supersede(ref)
	d := mq.DriverDelivery{Ref: ref, Gen: ref.gen}
	if err := src.set.Ack(t.Context(), &d); !mq.IsAmbiguous(err) {
		t.Fatalf("an acknowledgement whose wait ended reported %v, want an ambiguous answer", err)
	}
	src.mu.Lock()
	n := len(src.inflight)
	src.mu.Unlock()
	if n != 0 {
		t.Errorf("the in-flight map holds %d delivery(ies) after an ambiguous acknowledgement, want 0: it is "+
			"terminal, and the map kept the whole message until Close", n)
	}
	src.supersede(&delivery{id: "m", gen: 2, src: src, settle: &recordedSettlement{}})
	if err := src.set.Ack(t.Context(), &d); !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Errorf("a second Ack of the ambiguous delivery reported %v, want ErrSettlementUnknown", err)
	}
	if got := rec.acks.Load() + rec.nacks.Load(); got != 1 {
		t.Errorf("the library's message was settled %d time(s), want once", got)
	}
}

// TestCloseGivesBackAMessageASettlementHandsBackDuringIt holds Close to
// settling every message the source handed out, including one a settlement had
// claimed when Close ran. Close leaves a claimed delivery to its settlement, so
// as not to nack underneath it. A Reject whose dead-letter publication then
// failed handed the delivery back as settleable, after Close had passed it
// over, and nobody nacked its message. The client library's shutdown waits for
// every message it handed to the callback, so Close waited for that one until
// Config.MaxExtension ran out: ten minutes on the default configuration. A
// publication fails at that moment readily, because closing a consumer ends
// the context its handlers settle under.
func TestCloseGivesBackAMessageASettlementHandsBackDuringIt(t *testing.T) {
	src := leaseSource(t, 1, time.Minute)
	src.b.cfg.DeadLetterTopic = "dlq"
	src.dlq = &redeliveringPublisher{during: func() { _ = src.Close() }, err: errors.New("injected: refused")}
	rec := &recordedSettlement{}
	ref := &delivery{id: "m", gen: 1, src: src, settle: rec}
	ref.received.Store(time.Now().UnixNano())
	ref.expires.Store(time.Now().Add(time.Minute).UnixNano())
	src.supersede(ref)
	d := mq.DriverDelivery{Message: &mq.Message{Body: []byte("x")}, Ref: ref, Gen: ref.gen}
	if err := src.set.Nack(t.Context(), &d, mq.NackOptions{Action: mq.NackActionDeadLetter}); err == nil {
		t.Fatal("Reject succeeded while the dead-letter publication was refused")
	}
	if got := rec.acks.Load() + rec.nacks.Load(); got != 1 {
		t.Errorf("the message of a delivery whose Reject failed during Close was settled %d time(s), want once: "+
			"the client library's shutdown waits for it until Config.MaxExtension, and Close with it", got)
	}
	if got := ref.state.Load(); got == stateActive {
		t.Errorf("after Close, the delivery whose Reject failed is settleable again (state %d)", got)
	}
}

// TestReleasingADeliveryLeavesASettlementsAnswer holds release, which a
// redelivery and Close both use, to the deliveries a settlement has finished
// with. It used to store stateLost over them, so a settlement that completed
// between release's read and its store had its answer replaced: a second
// settlement of a settled delivery reported ErrLeaseLost rather than
// ErrAlreadySettled, and of an unknown one ErrLeaseLost rather than
// ErrSettlementUnknown. Nothing is sent to the library for either.
func TestReleasingADeliveryLeavesASettlementsAnswer(t *testing.T) {
	for _, state := range []uint32{stateSettled, stateUnknown, stateLost} {
		rec := &recordedSettlement{}
		ref := &delivery{id: "m", settle: rec}
		ref.state.Store(state)
		ref.release()
		if got := ref.state.Load(); got != state {
			t.Errorf("release turned a delivery in state %d into state %d, want it left as the settlement wrote it",
				state, got)
		}
		if n := rec.acks.Load() + rec.nacks.Load(); n != 0 {
			t.Errorf("release of a delivery in state %d settled its message %d time(s), want none", state, n)
		}
	}
}

// fixedClock is an [mq.Clock] whose time moves only when a test moves it.
type fixedClock struct{ at time.Time }

func (c *fixedClock) Now() time.Time { return c.at }

func (c *fixedClock) NewTimer(time.Duration) mq.Timer { panic("not used") }

// TestExtendNeverShortensTheLease holds the specification's rule that Extend
// moves the deadline by at least `by` and never earlier: a short extension on
// a delivery whose local lease runs for the whole budget must not cut it, or
// a later acknowledgement is refused as ErrLeaseLost and the message is
// redelivered.
func TestExtendNeverShortensTheLease(t *testing.T) {
	clk := &fixedClock{at: time.Unix(1_000_000, 0)}
	src := &source{b: &Backend{clock: clk}, budget: 10 * time.Minute, inflight: map[string]*delivery{}}
	set := newSettler(src)
	ref := &delivery{id: "m", src: src}
	ref.received.Store(clk.at.UnixNano())
	ref.expires.Store(clk.at.Add(src.budget).UnixNano())
	src.supersede(ref)
	d := mq.DriverDelivery{Ref: ref}

	clk.at = clk.at.Add(time.Second)
	if err := set.Extend(t.Context(), &d, 30*time.Second); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	clk.at = clk.at.Add(39 * time.Second)
	if _, err := set.claim(&d); err != nil {
		t.Fatalf("claim at t=40s after Extend(30s) on a 10m lease returned %v, want nil", err)
	}
}

// TestASupersededDeliveryGoesBackToTheLibrary holds that a delivery a
// redelivery supersedes has its library message settled once, wherever the
// redelivery finds it. The client library goes on extending an acknowledgement
// identifier nobody settles, and its shutdown waits for it, so a superseded
// delivery left unsettled held Close until Config.MaxExtension ran out.
//
// The last three cases are a settlement that has claimed the delivery when the
// redelivery arrives. The supersession cannot nack underneath a settlement, so
// the settlement has to. A Reject that then ends without settling it hands it
// back, and returning the delivery to the caller instead made it settleable
// again and kept its message from the library for good. An exactly-once Ack
// has given the message to the library already, which sends nothing more for
// it, so a nack after the refusal would be a second settlement the library
// ignores. A delayed Nack that has yet to hold it held it anyway, which made a
// delivery a later one owns settleable again and kept its nack back for a delay
// that no longer meant anything.
func TestASupersededDeliveryGoesBackToTheLibrary(t *testing.T) {
	refusal := errors.New("injected: refused")
	for _, tc := range []struct {
		name string
		// place puts the delivery where the redelivery finds it and calls
		// redeliver there.
		place func(t *testing.T, src *source, d *mq.DriverDelivery, redeliver func())
	}{
		{"handed-out", func(_ *testing.T, _ *source, _ *mq.DriverDelivery, redeliver func()) { redeliver() }},
		{"held-by-a-delayed-nack", func(_ *testing.T, _ *source, d *mq.DriverDelivery, redeliver func()) {
			d.Ref.(*delivery).state.Store(stateHeld)
			redeliver()
		}},
		{"claimed-by-a-reject-the-dead-letter-topic-refuses", func(t *testing.T, src *source, d *mq.DriverDelivery, redeliver func()) {
			src.dlq = &redeliveringPublisher{during: redeliver, err: refusal}
			if err := src.set.Nack(t.Context(), d, mq.NackOptions{Action: mq.NackActionDeadLetter}); err == nil {
				t.Fatal("Reject succeeded while the dead-letter publication was refused")
			}
		}},
		{"claimed-by-an-exactly-once-ack-the-service-refuses", func(t *testing.T, src *source, d *mq.DriverDelivery, redeliver func()) {
			src.info.ExactlyOnce = true
			rec, _ := d.Ref.(*delivery).settle.(*recordedSettlement)
			rec.during, rec.refuse = redeliver, refusal
			if err := src.set.Ack(t.Context(), d); err == nil {
				t.Fatal("Ack succeeded while the service refused it")
			}
		}},
		{"claimed-by-a-delayed-nack-before-it-holds", func(t *testing.T, src *source, d *mq.DriverDelivery, redeliver func()) {
			// The hold's timer is made after the claim and before the hold,
			// so a clock whose timers redeliver puts the redelivery there.
			src.b.clock = timerHookClock{hook: redeliver}
			src.budget = time.Hour
			src.holds, src.stop = map[*delivery]mq.Timer{}, make(chan struct{})
			t.Cleanup(func() { close(src.stop); src.holdWG.Wait() })
			d.Ref.(*delivery).received.Store(time.Now().UnixNano())
			err := src.set.Nack(t.Context(), d, mq.NackOptions{Delay: time.Minute})
			if !errors.Is(err, mq.ErrLeaseLost) {
				t.Errorf("a delayed Nack whose delivery a redelivery superseded before the hold reported %v, want "+
					"ErrLeaseLost: the message is already redelivered, so there is nothing left to delay", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &source{
				b:        &Backend{cfg: Config{ProjectID: "p", DeadLetterTopic: "dlq"}, errs: newErrorMapper()},
				enc:      mustEncoder(t, Config{ProjectID: "p"}, mq.MetadataPortableEnvelope),
				inflight: map[string]*delivery{},
			}
			src.set = newSettler(src)
			rec := &recordedSettlement{}
			first := &delivery{id: "m", gen: 1, src: src, settle: rec}
			first.expires.Store(time.Now().Add(time.Hour).UnixNano())
			src.supersede(first)
			d := mq.DriverDelivery{Message: &mq.Message{Body: []byte("x")}, Ref: first, Gen: first.gen}
			redeliver := func() { src.supersede(&delivery{id: "m", gen: 2, src: src, settle: &recordedSettlement{}}) }

			tc.place(t, src, &d, redeliver)
			if got := rec.acks.Load() + rec.nacks.Load(); got != 1 {
				t.Errorf("the superseded delivery's message was settled %d time(s), want once: the client library "+
					"holds an identifier nobody settles until Config.MaxExtension, and Close waits for it, and it "+
					"sends only the first settlement of a message", got)
			}
			if got := first.state.Load(); got != stateLost {
				t.Errorf("the superseded delivery is in state %d, want stateLost: a later delivery owns the message", got)
			}
		})
	}
}

// TestASupersessionRacingAHandBackStillGivesTheMessageBack is the moment the
// table cannot reach: the redelivery arrives as a refused Reject hands its
// delivery back. The supersession found the delivery claimed and then marked
// it lost with a plain store, and a hand-back that landed between the two had
// already seen it not superseded and made it active again. Nobody nacked the
// library's message, so Close waited for it until Config.MaxExtension.
//
// Nothing can be put inside a gap that narrow, so the test races the two and
// steers the redelivery towards the hand-back, which is where the gap is. A
// round whose message the hand-back nacked had its redelivery too early, and
// one the supersession nacked had it too late. Before the fix this found the
// gap within the first thousand rounds and about one round in a hundred after
// that; after it, every round settles the message exactly once.
func TestASupersessionRacingAHandBackStillGivesTheMessageBack(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("the race needs the Reject and the redelivery running at once")
	}
	const rounds = 5000
	refusal := errors.New("injected: refused")
	enc := mustEncoder(t, Config{ProjectID: "p"}, mq.MetadataPortableEnvelope)
	spin, lost := 0, 0
	for round := range rounds {
		var open atomic.Bool
		src := &source{
			b:        &Backend{cfg: Config{ProjectID: "p", DeadLetterTopic: "dlq"}, errs: newErrorMapper()},
			enc:      enc,
			inflight: map[string]*delivery{},
			dlq:      &gatedPublisher{open: &open, err: refusal},
		}
		src.set = newSettler(src)
		rec := &handBackSettlement{}
		first := &delivery{id: "m", gen: 1, src: src, settle: rec}
		first.expires.Store(time.Now().Add(time.Hour).UnixNano())
		src.supersede(first)
		d := mq.DriverDelivery{Message: &mq.Message{Body: []byte("x")}, Ref: first, Gen: first.gen}

		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = src.set.Nack(context.Background(), &d, mq.NackOptions{Action: mq.NackActionDeadLetter})
		}()
		for first.state.Load() != stateSettling {
			runtime.Gosched()
		}
		// The Reject is parked in its publication. Opening the gate lets it
		// run on to its hand-back while this goroutine waits spin turns and
		// redelivers.
		open.Store(true)
		for range spin {
		}
		src.supersede(&delivery{id: "m", gen: 2, src: src, settle: &recordedSettlement{}})
		<-done

		if n := rec.nacks.Load() + rec.acks.Load(); n != 1 {
			lost++
			if lost == 1 {
				t.Errorf("round %d: the superseded delivery's message was settled %d time(s), want once, and the "+
					"delivery is in state %d: the client library holds an identifier nobody settles until "+
					"Config.MaxExtension, and Close waits for it", round, n, first.state.Load())
			}
		}
		// The step grows with the wait so that the first rounds climb to the
		// gap quickly, wherever this machine puts it.
		step := 1 + spin/64
		switch {
		case rec.handedBack.Load():
			spin += step
		default:
			spin = max(spin-step, 0)
		}
	}
	if lost > 0 {
		t.Errorf("%d of %d rounds left the superseded delivery's message unsettled or settled it twice", lost, rounds)
	}
}

// gatedPublisher is a dead-letter publisher that holds every publication until
// open is set and then refuses it.
type gatedPublisher struct {
	open *atomic.Bool
	err  error
}

// Publish implements psapi.Publisher.
func (p *gatedPublisher) Publish(context.Context, *psapi.Outgoing) psapi.Result {
	for !p.open.Load() {
	}
	return refusedResult{err: p.err}
}

// ResumePublish implements psapi.Publisher.
func (*gatedPublisher) ResumePublish(string) {}

// Flush implements psapi.Publisher.
func (*gatedPublisher) Flush() {}

// Stop implements psapi.Publisher.
func (*gatedPublisher) Stop() {}

// handBackSettlement is a library message that also records whether its nack
// came from a settlement handing its delivery back, which is how the race
// above tells which side came first.
type handBackSettlement struct {
	recordedSettlement
	handedBack atomic.Bool
}

// Nack implements psapi.Settlement.
func (h *handBackSettlement) Nack() {
	h.recordedSettlement.Nack()
	pcs := make([]uintptr, 32)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, ".(*delivery).giveBack") {
			h.handedBack.Store(true)
		}
		if !more {
			return
		}
	}
}

// timerHookClock is an [mq.Clock] on the wall clock that runs hook each time a
// timer is made, and whose timers do not fire within a test.
type timerHookClock struct{ hook func() }

// Now implements mq.Clock.
func (timerHookClock) Now() time.Time { return time.Now() }

// NewTimer implements mq.Clock.
func (c timerHookClock) NewTimer(time.Duration) mq.Timer {
	c.hook()
	return wallTimer{time.NewTimer(time.Hour)}
}

// recordedSettlement is a library message that counts how it was settled. An
// acknowledgement with a result runs during while it waits and then returns
// refuse, which is a redelivery arriving while the service answers.
type recordedSettlement struct {
	acks, nacks atomic.Int32
	during      func()
	refuse      error
}

// Ack implements psapi.Settlement.
func (r *recordedSettlement) Ack() { r.acks.Add(1) }

// Nack implements psapi.Settlement.
func (r *recordedSettlement) Nack() { r.nacks.Add(1) }

// AckWithResult implements psapi.Settlement.
func (r *recordedSettlement) AckWithResult(context.Context) error {
	r.acks.Add(1)
	if r.during != nil {
		r.during()
	}
	return r.refuse
}

// NackWithResult implements psapi.Settlement.
func (r *recordedSettlement) NackWithResult(context.Context) error {
	r.nacks.Add(1)
	return nil
}

// redeliveringPublisher is a dead-letter publisher that runs during while the
// publication is in flight and then refuses it.
type redeliveringPublisher struct {
	during func()
	err    error
}

// Publish implements psapi.Publisher.
func (p *redeliveringPublisher) Publish(context.Context, *psapi.Outgoing) psapi.Result {
	p.during()
	return refusedResult{err: p.err}
}

// ResumePublish implements psapi.Publisher.
func (*redeliveringPublisher) ResumePublish(string) {}

// Flush implements psapi.Publisher.
func (*redeliveringPublisher) Flush() {}

// Stop implements psapi.Publisher.
func (*redeliveringPublisher) Stop() {}

// refusedResult is a publication the service refused.
type refusedResult struct{ err error }

// Get implements psapi.Result.
func (r refusedResult) Get(context.Context) (string, error) { return "", r.err }
