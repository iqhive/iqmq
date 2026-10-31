package gcppubsub

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
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

// TestAnAbandonedDeliveryIsGivenBackOnceItsLeaseRunsOut is the in-flight
// map's last way out. Run offers a settlement the driver refused definitely
// two more times and then abandons the delivery without telling the driver,
// and a Reject whose dead-letter publication is refused leaves the delivery
// active. Only a redelivery of the message to this source, or Close, took it
// out of the map. On a subscription several hosts consume, the redelivery can
// go to another host, and this source kept the entry, with the library's
// message, until it closed; one whose lease ran out while the library was
// still extending the message also held the message back until the
// library's own budget ended. A later callback now gives back every delivery
// whose lease has run out, at most once an acknowledgement deadline.
func TestAnAbandonedDeliveryIsGivenBackOnceItsLeaseRunsOut(t *testing.T) {
	clk := &fixedClock{at: time.Unix(1_000_000, 0)}
	src := leaseSource(t, 4, time.Minute)
	src.b.clock = clk
	src.deadline = 10 * time.Second
	t.Cleanup(func() { _ = src.Close() })
	ctx := t.Context()
	inflight := func() []string {
		src.mu.Lock()
		defer src.mu.Unlock()
		ids := make([]string, 0, len(src.inflight))
		for id := range src.inflight {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		return ids
	}
	// abandoned is a delivery nobody will settle whose lease ends at
	// leaseEnds and whose message the library extends until letsGo: later,
	// when the message waited in the library before its callback.
	abandoned := func(id string, leaseEnds, letsGo time.Time) *recordedSettlement {
		rec := &recordedSettlement{}
		ref := &delivery{id: id, gen: src.gen.Add(1), src: src, settle: rec, kept: letsGo.UnixNano()}
		ref.received.Store(leaseEnds.Add(-src.budget).UnixNano())
		ref.expires.Store(leaseEnds.UnixNano())
		src.supersede(ref)
		return rec
	}

	// One handed out and abandoned, whose library budget ends with its lease,
	// and one the library still extends when its lease runs out.
	handed := src.incoming(t, "handed out")
	src.admit(ctx, handed)
	fetched := make([]mq.DriverDelivery, 4)
	if n, err := src.Fetch(ctx, fetched); n != 1 {
		t.Fatalf("Fetch handed out %d, %v", n, err)
	}
	extended := abandoned("extended", clk.at.Add(src.budget), clk.at.Add(src.budget+30*time.Second))
	// And two that a settlement and a delayed Nack's hold have, which settle
	// the library's message themselves.
	settling := abandoned("settling", clk.at.Add(src.budget), clk.at.Add(src.budget+30*time.Second))
	held := abandoned("held", clk.at.Add(src.budget), clk.at.Add(src.budget+30*time.Second))
	src.inflight["settling"].state.Store(stateSettling)
	src.inflight["held"].state.Store(stateHeld)

	// Every lease runs out, and the redeliveries go to other hosts. This
	// source's next callback is for another message.
	clk.at = clk.at.Add(src.budget + time.Second)
	src.admit(ctx, src.incoming(t, "next"))
	if got := inflight(); !slices.Equal(got, []string{"held", "next", "settling"}) {
		t.Fatalf("the in-flight map holds %q after a callback past every lease, want the new delivery and the two "+
			"a settlement and a hold have: the abandoned ones leave it, and the other two stay theirs", got)
	}
	if got := settling.nacks.Load() + held.nacks.Load(); got != 0 {
		t.Errorf("the sweep nacked %d message(s) a settlement or a hold has, want none", got)
	}
	if got := extended.nacks.Load(); got != 1 {
		t.Errorf("the message whose lease ran out while the library still extended it was nacked %d time(s), "+
			"want once: the library held it back until its own budget ended", got)
	}
	if got := handed.Settle.(*recordedSettlement).nacks.Load(); got != 0 {
		t.Errorf("the message the library had already let go was nacked %d time(s), want none", got)
	}
	if err := src.set.Ack(ctx, &fetched[0]); !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("an Ack of the abandoned delivery after it was given back returned %v, want ErrLeaseLost", err)
	}

	// A sweep runs at most once an acknowledgement deadline.
	swept := clk.at
	late := abandoned("late", swept, swept.Add(time.Minute))
	clk.at = swept.Add(time.Second)
	src.admit(ctx, src.incoming(t, "within a deadline"))
	if got := late.nacks.Load(); got != 0 {
		t.Errorf("a callback within one acknowledgement deadline of the last sweep swept again (%d nack(s))", got)
	}
	clk.at = swept.Add(src.deadline)
	src.admit(ctx, src.incoming(t, "a deadline later"))
	if got := late.nacks.Load(); got != 1 {
		t.Errorf("the delivery whose lease ran out was nacked %d time(s) a deadline after the last sweep, want once", got)
	}
	if got := inflight(); slices.Contains(got, "late") {
		t.Errorf("the in-flight map still holds the abandoned delivery a deadline after the last sweep: %q", got)
	}
}

// TestASweepNeverOverwritesTheSettlementThatTookTheDelivery holds the sweep to
// taking a delivery with one compare-and-swap, under the lock that found it.
// It read the state under the lock and stored the lost state after letting go
// of it, through delivery.expire, so a settlement whose clock read the lease as
// still running could claim the delivery in between, and the store landed over
// its answer. An Ack returned nil and the delivery ended lost, so a second
// settlement answered ErrLeaseLost instead of ErrAlreadySettled. A delayed
// Nack's hold ended lost, so its timer never sent the nack. Every settlement
// here reads the clock a nanosecond before the leases end, and the sweep is
// handed the end.
//
// "in the gap" puts the settlement there every time the map's iteration order
// allows: it runs from the nack the sweep sends, after unlocking, for another
// delivery whose message the library still extends, so a round catches the
// old order whenever that delivery comes first, about half the time.
// "concurrently" is the reviewer's race, with the settlement and the sweep on
// two goroutines.
func TestASweepNeverOverwritesTheSettlementThatTookTheDelivery(t *testing.T) {
	end := time.Unix(1_000_000, 0)
	newSource := func() *source {
		return &source{
			b:        &Backend{clock: stillClock{at: end.Add(-time.Nanosecond)}},
			budget:   time.Minute,
			deadline: 10 * time.Second,
			inflight: map[string]*delivery{},
			holds:    map[*delivery]mq.Timer{},
			stop:     make(chan struct{}),
		}
	}
	// lapsing is a delivery nobody has settled whose lease runs out at end
	// and whose message the library extends until kept.
	lapsing := func(src *source, id string, settle psapi.Settlement, kept time.Time) mq.DriverDelivery {
		ref := &delivery{id: id, gen: 1, src: src, settle: settle, kept: kept.UnixNano()}
		ref.received.Store(end.Add(-src.budget).UnixNano())
		ref.expires.Store(end.UnixNano())
		src.supersede(ref)
		return mq.DriverDelivery{Ref: ref, Gen: 1}
	}
	for _, tc := range []struct {
		name   string
		settle func(*settler, *mq.DriverDelivery) error
	}{
		{"an Ack", func(set *settler, d *mq.DriverDelivery) error {
			return set.Ack(context.Background(), d)
		}},
		{"a delayed Nack", func(set *settler, d *mq.DriverDelivery) error {
			return set.Nack(context.Background(), d, mq.NackOptions{Action: mq.NackActionRequeue, Delay: time.Nanosecond})
		}},
	} {
		// survived checks that the settlement's answer survived the sweep: an
		// Ack that returned nil left the delivery settled, a hold that was
		// accepted sent its nack, and the library's message was settled once.
		survived := func(t *testing.T, round int, src *source, d mq.DriverDelivery, rec *recordedSettlement, err error) {
			t.Helper()
			src.holdWG.Wait()
			ref := d.Ref.(*delivery)
			switch {
			case errors.Is(err, mq.ErrLeaseLost):
				// The sweep took the delivery first.
			case err != nil:
				t.Fatalf("round %d: the settlement returned %v", round, err)
			case tc.name == "an Ack" && ref.state.Load() != stateSettled:
				t.Fatalf("round %d: the Ack returned nil and sent %d acknowledgement(s), and the sweep then left the "+
					"delivery in state %d, not settled: a second settlement would answer ErrLeaseLost",
					round, rec.acks.Load(), ref.state.Load())
			case tc.name == "a delayed Nack" && rec.nacks.Load() != 1:
				t.Fatalf("round %d: the delayed Nack was accepted and its hold sent %d nack(s), want one: the sweep "+
					"stored the lost state over the hold", round, rec.nacks.Load())
			}
			if n := rec.acks.Load() + rec.nacks.Load(); n > 1 {
				t.Fatalf("round %d: the library's message was settled %d times", round, n)
			}
		}
		t.Run(tc.name+" in the gap", func(t *testing.T) {
			for round := range 64 {
				src := newSource()
				set := newSettler(src)
				rec := &recordedSettlement{}
				// The library let go of this message as its lease ran out, so
				// the sweep sends nothing for it.
				d := lapsing(src, "settled", rec, end)
				var err error
				ran := false
				lapsing(src, "extended", &nackHook{hook: func() {
					ran = true
					err = tc.settle(set, &d)
				}}, end.Add(time.Minute))
				src.sweep(end)
				if !ran {
					t.Fatalf("round %d: the sweep sent no nack for the message the library still extends", round)
				}
				survived(t, round, src, d, rec, err)
			}
		})
		t.Run(tc.name+" concurrently", func(t *testing.T) {
			src := newSource()
			set := newSettler(src)
			for round := range 5000 {
				rec := &recordedSettlement{}
				d := lapsing(src, "m"+strconv.Itoa(round), rec, end)
				src.swept.Store(0)
				start := make(chan struct{})
				var wg sync.WaitGroup
				var err error
				wg.Add(2)
				go func() {
					defer wg.Done()
					<-start
					src.sweep(end)
				}()
				go func() {
					defer wg.Done()
					<-start
					err = tc.settle(set, &d)
				}()
				close(start)
				wg.Wait()
				survived(t, round, src, d, rec, err)
			}
		})
	}
}

// stillClock is an [mq.Clock] that always reads at, and whose timers are real
// ones.
type stillClock struct{ at time.Time }

// Now implements mq.Clock.
func (c stillClock) Now() time.Time { return c.at }

// NewTimer implements mq.Clock.
func (stillClock) NewTimer(d time.Duration) mq.Timer { return wallTimer{time.NewTimer(d)} }

// nackHook is a library message that runs hook when it is nacked.
type nackHook struct {
	recordedSettlement
	hook func()
}

// Nack implements psapi.Settlement.
func (n *nackHook) Nack() {
	n.recordedSettlement.Nack()
	n.hook()
}

// TestASettlementsAnswerOutlivesItsLease holds a second settlement of a
// delivery whose exactly-once settlement has finished to that settlement's
// answer after the delivery's lease has run out as well. A message whose
// acknowledgement did not land comes back once its deadline passes, so the
// redelivery an ambiguous Ack is followed by comes after the lease. The claim
// checked the lease before the state: a second settlement then reported
// ErrLeaseLost, and once the client library had let the message go it stored
// the lost state over the sticky unknown one, and over a settled one too.
func TestASettlementsAnswerOutlivesItsLease(t *testing.T) {
	for _, tc := range []struct {
		name   string
		refuse error
		// held says whether the client library is still extending the
		// message when the second settlement comes.
		held  bool
		state uint32
		want  error
	}{
		{"unknown, still held", context.DeadlineExceeded, true, stateUnknown, mq.ErrSettlementUnknown},
		{"unknown, let go", context.DeadlineExceeded, false, stateUnknown, mq.ErrSettlementUnknown},
		{"settled, still held", nil, true, stateSettled, mq.ErrAlreadySettled},
		{"settled, let go", nil, false, stateSettled, mq.ErrAlreadySettled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := leaseSource(t, 1, time.Minute)
			src.info.ExactlyOnce = true
			rec := &recordedSettlement{refuse: tc.refuse}
			now := time.Now()
			kept := now.Add(-time.Second)
			if tc.held {
				kept = now.Add(time.Hour)
			}
			ref := &delivery{id: "m", gen: 1, src: src, settle: rec, kept: kept.UnixNano()}
			ref.received.Store(now.UnixNano())
			ref.expires.Store(now.Add(time.Minute).UnixNano())
			src.supersede(ref)
			d := mq.DriverDelivery{Ref: ref, Gen: ref.gen}
			_ = src.set.Ack(t.Context(), &d)
			if got := ref.state.Load(); got != tc.state {
				t.Fatalf("the first Ack left the delivery in state %d, want %d", got, tc.state)
			}
			// The lease runs out and the message comes back, or not.
			ref.expires.Store(now.Add(-time.Millisecond).UnixNano())
			src.supersede(&delivery{id: "m", gen: 2, src: src, settle: &recordedSettlement{}})
			if err := src.set.Ack(t.Context(), &d); !errors.Is(err, tc.want) {
				t.Errorf("a second Ack after the lease reported %v, want %v", err, tc.want)
			}
			if got := ref.state.Load(); got != tc.state {
				t.Errorf("the delivery is in state %d after the second Ack, want %d: its answer was overwritten",
					got, tc.state)
			}
			if got := rec.acks.Load() + rec.nacks.Load(); got != 1 {
				t.Errorf("the library's message was settled %d time(s), want once", got)
			}
		})
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
