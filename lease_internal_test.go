package iqmq

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/iqhive/iqmq/internal/clock"
)

// TestLeaseNextInIsAlwaysPositiveAndAtMostHalfTheGrant is the specification's
// arithmetic. The scheduler re-schedules each extension at now plus this
// interval, so an interval at or below zero fires the same ticket immediately
// and for ever — which is what a lead at or above the lease used to produce.
//
// Two properties are asserted for every row rather than only the expected
// number, because the number is a consequence and the properties are the
// contract: the interval is positive, and it is strictly less than the amount
// granted, so the next extension lands before the deadline it is renewing.
func TestLeaseNextInIsAlwaysPositiveAndAtMostHalfTheGrant(t *testing.T) {
	for _, tc := range []struct {
		name string
		lead time.Duration
		by   time.Duration
		want time.Duration
	}{
		{"a lead well under half the lease is honoured exactly", time.Second, time.Minute, 59 * time.Second},
		{"a lead of exactly half the lease is the floor", 30 * time.Second, time.Minute, 30 * time.Second},
		{"a lead over half the lease falls back to the floor", 40 * time.Second, time.Minute, 30 * time.Second},
		{"a lead equal to the lease does not schedule at zero", time.Minute, time.Minute, 30 * time.Second},
		{"a lead four times the lease does not schedule in the past", 200 * time.Millisecond, 50 * time.Millisecond, 25 * time.Millisecond},
		{"a two-nanosecond lease still schedules forwards", time.Hour, 2 * time.Nanosecond, time.Nanosecond},
		// Half of one nanosecond is zero, so the floor needs its own floor:
		// an interval of zero is the same spin arrived at by rounding
		// instead of by subtraction.
		{"a one-nanosecond lease does not round the interval to zero", time.Hour, time.Nanosecond, time.Nanosecond},
		{"a one-nanosecond lease and a one-nanosecond lead", time.Nanosecond, time.Nanosecond, time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &leaseScheduler{lead: tc.lead}
			got := s.nextIn(tc.by)
			if got != tc.want {
				t.Fatalf("nextIn(%v) with lead %v = %v, want %v", tc.by, tc.lead, got, tc.want)
			}
			// Property one, which holds for every input: the interval is
			// positive, so the run goroutine parks instead of re-firing the
			// same ticket.
			if got <= 0 {
				t.Fatalf("nextIn(%v) with lead %v = %v: the scheduler fires the same ticket for ever", tc.by, tc.lead, got)
			}
			// Property two: the next extension lands before the deadline it
			// is renewing. It is unachievable for a one-nanosecond lease —
			// there is no duration strictly between zero and one — and a
			// lease that short is a broken driver rather than a broker. The
			// scheduler gives up the race and keeps the parking, which is the
			// right way round: a lost lease is reported at the settlement,
			// and a saturated core is reported by nobody.
			if tc.by > time.Nanosecond && got >= tc.by {
				t.Fatalf("nextIn(%v) with lead %v = %v, which is not before the deadline it renews", tc.by, tc.lead, got)
			}
		})
	}
}

// TestLeaseAddNamesTheTicketUnderTheLock pins where the scheduler entry is
// pointed at its ticket: in add, under the lock the run goroutine fires
// entries under, so that a ticket reused by the next delivery in its slot is
// never written while that goroutine may be reading it.
func TestLeaseAddNamesTheTicketUnderTheLock(t *testing.T) {
	s, err := newLeaseScheduler(clock.Real{}, time.Second, time.Minute)
	if err != nil {
		t.Fatalf("newLeaseScheduler: %v", err)
	}
	defer s.close()
	var tk leaseTicket
	s.add(&tk, s.at(time.Now().Add(time.Hour)))
	s.mu.Lock()
	got := tk.entry.Value
	s.mu.Unlock()
	s.release(&tk)
	if got != &tk {
		t.Fatalf("the scheduled entry names %p, want its own ticket %p", got, &tk)
	}
}

// wallStepped returns t with its wall-clock reading moved by d and its
// monotonic reading kept, which is what time.Now returns once the host's
// clock has been stepped (by NTP, a VM resume, an operator setting the date):
// a step moves the wall reading only. The time package builds no such value,
// so it puts t's monotonic reading into t.Add(d), whose wall reading is the
// stepped one. ok is false when t has no monotonic reading or time.Time is not
// laid out the way this relies on.
func wallStepped(t time.Time, d time.Duration) (stepped time.Time, ok bool) {
	type layout struct {
		wall uint64
		ext  int64
		loc  *time.Location
	}
	if unsafe.Sizeof(t) != unsafe.Sizeof(layout{}) {
		return t, false
	}
	u := t.Add(d)
	(*layout)(unsafe.Pointer(&u)).ext = (*layout)(unsafe.Pointer(&t)).ext
	return u, u.Sub(t) == 0 && u.Round(0).Sub(t.Round(0)) == d
}

// steppedClock is the real clock of a host whose wall clock has been stepped
// by off. Now's wall reading moves; its monotonic reading and the timers, which
// the runtime runs on the monotonic clock, do not.
type steppedClock struct{ off atomic.Int64 }

func (c *steppedClock) Now() time.Time {
	t, _ := wallStepped(time.Now(), time.Duration(c.off.Load()))
	return t
}

func (c *steppedClock) NewTimer(d time.Duration) clock.Timer { return clock.Real{}.NewTimer(d) }

// extensionLog is a settlement handle whose only live part is extend: it
// records when the scheduler asked for each extension of the lease it
// reports.
type extensionLog struct {
	inf DeliveryInfo
	mu  sync.Mutex
	at  []time.Time
}

// newExtensionLog reports a lease stamped with time.Now, monotonic reading
// included, as most drivers stamp theirs.
func newExtensionLog(lease time.Duration) *extensionLog {
	now := time.Now()
	return &extensionLog{inf: DeliveryInfo{ReceivedAt: now, LeaseExpires: now.Add(lease)}}
}

// newWallOnlyExtensionLog reports a lease granted at now and stamped with
// time.Unix, with no monotonic reading, as the SQS and Pub/Sub drivers stamp
// theirs.
func newWallOnlyExtensionLog(now time.Time, lease time.Duration) *extensionLog {
	return &extensionLog{inf: DeliveryInfo{
		ReceivedAt:   time.Unix(0, now.UnixNano()),
		LeaseExpires: time.Unix(0, now.Add(lease).UnixNano()),
	}}
}

func (h *extensionLog) first() (time.Time, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.at) == 0 {
		return time.Time{}, false
	}
	return h.at[0], true
}

func (h *extensionLog) settle(uint32, settleOp, NackOptions) error { return nil }
func (h *extensionLog) settled(uint32) bool                        { return false }
func (h *extensionLog) info() DeliveryInfo                         { return h.inf }
func (h *extensionLog) native() any                                { return nil }
func (h *extensionLog) active(uint32) bool                         { return true }
func (h *extensionLog) abandon(uint32)                             {}
func (h *extensionLog) acked(uint32) bool                          { return false }
func (h *extensionLog) pin()                                       {}
func (h *extensionLog) unpin()                                     {}
func (h *extensionLog) unkeep(uint32)                              {}
func (h *extensionLog) kept(uint32) bool                           { return true }

func (h *extensionLog) extend(uint32, time.Duration) error {
	h.mu.Lock()
	h.at = append(h.at, time.Now())
	h.mu.Unlock()
	return nil
}

func (h *extensionLog) extensions() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.at)
}

// TestLeaseSchedulerIsNotMovedByAWallClockStep: a lease is extended a lead
// before it runs out, measured on the clock the scheduler's timer runs on,
// whatever the host's wall clock does meanwhile. The scheduler held its times
// as wall readings (UnixNano) and armed its timer on the monotonic clock. A
// step back delayed every extension by the step, so the broker took back
// deliveries the handlers still held while LeaseKept said they were kept; a
// step forward extended every lease due within the step at the next wake.
func TestLeaseSchedulerIsNotMovedByAWallClockStep(t *testing.T) {
	if _, ok := wallStepped(time.Now(), time.Minute); !ok {
		t.Skip("this runtime's time.Time is not laid out as wallStepped expects")
	}
	const lead = 200 * time.Millisecond
	t.Run("backward", func(t *testing.T) {
		clk := &steppedClock{}
		s, err := newLeaseScheduler(clk, lead, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		defer s.close()
		h := newExtensionLog(400 * time.Millisecond)
		var tk leaseTicket
		s.track(&tk, &Delivery{handle: h, gen: 1}, clk.Now())
		defer s.release(&tk)
		clk.off.Store(-int64(5 * time.Second))
		time.Sleep(time.Second)
		if h.extensions() == 0 {
			t.Fatal("a 400ms lease with a 200ms lead was not extended within a second of a five-second step back: the broker has taken the delivery back")
		}
	})
	t.Run("forward", func(t *testing.T) {
		clk := &steppedClock{}
		s, err := newLeaseScheduler(clk, lead, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		defer s.close()
		near, far := newExtensionLog(400*time.Millisecond), newExtensionLog(30*time.Second)
		var nearTk, farTk leaseTicket
		s.track(&nearTk, &Delivery{handle: near, gen: 1}, clk.Now())
		defer s.release(&nearTk)
		s.track(&farTk, &Delivery{handle: far, gen: 1}, clk.Now())
		defer s.release(&farTk)
		clk.off.Store(int64(time.Minute))
		// The near lease wakes the scheduler after the step.
		deadline := time.Now().Add(time.Second)
		for near.extensions() == 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if near.extensions() == 0 {
			t.Fatal("a 400ms lease with a 200ms lead was not extended within a second")
		}
		time.Sleep(50 * time.Millisecond)
		if n := far.extensions(); n != 0 {
			t.Fatalf("a 30s lease was extended within a second of its grant (%d extensions), after a one-minute step forward", n)
		}
	})
}

// TestLeaseSchedulerMeasuresAWallOnlyLeaseFromItsArrival: the SQS and Pub/Sub
// drivers stamp a lease with time.Unix, which carries no monotonic reading,
// so the scheduler can only measure it on the wall clock. The host's clock
// steps after the Run began and before a delivery arrives with a 600 ms lease
// and a 200 ms lead; its first extension is due about 400 ms after it
// arrived, whatever the wall clock did before. The scheduler measured such a
// lease from the wall reading of the moment the Run began, so it took in the
// step: one forward delayed the extension by the step, past the lease, and
// one back asked for it at once.
func TestLeaseSchedulerMeasuresAWallOnlyLeaseFromItsArrival(t *testing.T) {
	if _, ok := wallStepped(time.Now(), time.Minute); !ok {
		t.Skip("this runtime's time.Time is not laid out as wallStepped expects")
	}
	for _, step := range []time.Duration{time.Minute, -time.Minute} {
		t.Run(step.String(), func(t *testing.T) {
			clk := &steppedClock{}
			s, err := newLeaseScheduler(clk, 200*time.Millisecond, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			clk.off.Store(int64(step))
			h := newWallOnlyExtensionLog(clk.Now(), 600*time.Millisecond)
			start := time.Now()
			var tk leaseTicket
			s.track(&tk, &Delivery{handle: h, gen: 1}, clk.Now())
			defer s.release(&tk)
			time.Sleep(time.Second)
			first, ok := h.first()
			switch {
			case !ok:
				t.Errorf("after a %v step before it arrived, a 600ms wall-only lease with a 200ms lead was not extended within 1s: the lease ran out", step)
			case first.Sub(start) < 300*time.Millisecond:
				t.Errorf("after a %v step before it arrived, a 600ms wall-only lease was first extended after %v, want about 400ms", step, first.Sub(start))
			}
		})
	}
}

// slowExtensionLog is an extensionLog whose extend takes a broker round trip,
// as ChangeMessageVisibility or ModifyAckDeadline does, and is recorded as
// the broker sees it, halfway through.
type slowExtensionLog struct {
	*extensionLog
	rtt time.Duration
}

func (h *slowExtensionLog) extend(gen uint32, by time.Duration) error {
	time.Sleep(h.rtt / 2)
	err := h.extensionLog.extend(gen, by)
	time.Sleep(h.rtt / 2)
	return err
}

// TestLeaseSchedulerExtendsLeasesDueTogetherSideBySide: fifty deliveries from
// one fetch share a 600ms lease, the lead is 200ms and an extension takes a
// 20ms round trip. Every lease must be extended before it runs out. The
// scheduler extended the deliveries due together one at a time, so the k-th
// was asked for k round trips after it was due: 40 of the 50 leases ran out
// first, the last by about 800ms. It extends them side by side now, on up to
// maxExtenders goroutines, so the last is asked for about four round trips
// after it was due.
//
// Real time and not a synctest bubble: release waits on the ticket mutex an
// extension holds across its round trip, and a mutex wait is not durable.
func TestLeaseSchedulerExtendsLeasesDueTogetherSideBySide(t *testing.T) {
	const (
		n     = 50
		rtt   = 20 * time.Millisecond
		lead  = 200 * time.Millisecond
		lease = 600 * time.Millisecond
	)
	s, err := newLeaseScheduler(clock.Real{}, lead, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	expires := start.Add(lease)
	hs := make([]*slowExtensionLog, n)
	tks := make([]leaseTicket, n)
	for i := range hs {
		hs[i] = &slowExtensionLog{
			extensionLog: &extensionLog{inf: DeliveryInfo{ReceivedAt: start, LeaseExpires: expires}},
			rtt:          rtt,
		}
		s.track(&tks[i], &Delivery{handle: hs[i], gen: 1}, start)
	}
	time.Sleep(lease + 200*time.Millisecond)
	late, worst := 0, time.Duration(0)
	for _, h := range hs {
		first, ok := h.first()
		if !ok || !first.Before(expires) {
			late++
			if ok {
				worst = max(worst, first.Sub(expires))
			}
		}
	}
	var wg sync.WaitGroup
	for i := range tks {
		wg.Go(func() { s.release(&tks[i]) })
	}
	wg.Wait()
	s.close()
	if late > 0 {
		t.Fatalf("%d of %d deliveries were first extended after their lease had run out (the worst %v after), with a lead of %v, %d round trips", late, n, worst, lead, lead/rtt)
	}
}

// gatedExtensionLog is an extensionLog whose extend reports that it has
// begun and then waits until the test lets it go.
type gatedExtensionLog struct {
	*extensionLog
	entered chan<- struct{}
	gate    <-chan struct{}
}

func (h *gatedExtensionLog) extend(gen uint32, by time.Duration) error {
	h.entered <- struct{}{}
	<-h.gate
	return h.extensionLog.extend(gen, by)
}

// TestLeaseSchedulerCloseWaitsForTheExtensionsInFlight: two leases come due
// together, so they are extended side by side, and both extensions are with
// the broker at once. close returns only once both have returned: the
// goroutines a wake extends on are the scheduler's, and none outlives it.
func TestLeaseSchedulerCloseWaitsForTheExtensionsInFlight(t *testing.T) {
	s, err := newLeaseScheduler(clock.Real{}, 100*time.Millisecond, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	entered := make(chan struct{}, 2)
	gate := make(chan struct{})
	var opened sync.Once
	open := func() { opened.Do(func() { close(gate) }) }
	// Opened before the deferred close, on a failure too, so that nothing
	// is left waiting at the gate.
	defer open()
	now := time.Now()
	var tks [2]leaseTicket
	for i := range tks {
		h := &gatedExtensionLog{
			extensionLog: &extensionLog{inf: DeliveryInfo{ReceivedAt: now, LeaseExpires: now.Add(110 * time.Millisecond)}},
			entered:      entered,
			gate:         gate,
		}
		s.track(&tks[i], &Delivery{handle: h, gen: 1}, now)
	}
	for range tks {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the two leases due together were not both being extended at once")
		}
	}
	closed := make(chan struct{})
	go func() {
		s.close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("close returned with two extensions still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	open()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not return once the extensions had")
	}
	for i := range tks {
		s.release(&tks[i])
	}
}

// busyExtensionLog is an extensionLog whose extend takes a round trip, can
// answer that the lease is lost, and counts the extensions in flight at once.
type busyExtensionLog struct {
	*extensionLog
	rtt      time.Duration
	lost     bool
	inflight *atomic.Int32
	peak     *atomic.Int32
}

func (h *busyExtensionLog) extend(gen uint32, by time.Duration) error {
	n := h.inflight.Add(1)
	for p := h.peak.Load(); n > p && !h.peak.CompareAndSwap(p, n); p = h.peak.Load() {
	}
	time.Sleep(h.rtt)
	h.inflight.Add(-1)
	err := h.extensionLog.extend(gen, by)
	if h.lost {
		return ErrLeaseLost
	}
	return err
}

// TestLeaseSchedulerWakeSurvivesSlotsReusedWhileItExtends is a busy consumer
// during a wake of many leases: 64 come due together and are being extended
// side by side, with 10ms round trips and every fourth answering that the
// lease is lost, while a third of their slots are released and tracked again
// for new deliveries, a third are released for good, and more leases are
// added. The extending goroutines read each fired entry's ticket without the
// scheduler's lock, which tracking a reused slot writes it under, and -race
// reported the race about once in 20 runs; the tickets are read under the
// lock now. Run under -race: no data race, close returns, and no more than
// maxExtenders extensions are in flight at once.
func TestLeaseSchedulerWakeSurvivesSlotsReusedWhileItExtends(t *testing.T) {
	const n = 64
	s, err := newLeaseScheduler(clock.Real{}, 50*time.Millisecond, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var inflight, peak atomic.Int32
	lease := func(d time.Duration, lost bool) *Delivery {
		now := time.Now()
		return &Delivery{gen: 1, handle: &busyExtensionLog{
			extensionLog: &extensionLog{inf: DeliveryInfo{ReceivedAt: now, LeaseExpires: now.Add(d)}},
			rtt:          10 * time.Millisecond, lost: lost, inflight: &inflight, peak: &peak,
		}}
	}
	tks := make([]leaseTicket, n)
	for i := range tks {
		s.track(&tks[i], lease(60*time.Millisecond, i%4 == 3), time.Now())
	}
	time.Sleep(25 * time.Millisecond) // the wake is extending
	var wg sync.WaitGroup
	for i := range tks {
		switch i % 3 {
		case 0: // the slot is reused for a new delivery
			wg.Go(func() {
				s.release(&tks[i])
				s.track(&tks[i], lease(time.Hour, false), time.Now())
			})
		case 1:
			wg.Go(func() { s.release(&tks[i]) })
		}
	}
	extra := make([]leaseTicket, 8)
	for i := range extra {
		s.track(&extra[i], lease(55*time.Millisecond, false), time.Now())
	}
	wg.Wait()
	time.Sleep(150 * time.Millisecond)
	for i := range tks {
		s.release(&tks[i])
	}
	for i := range extra {
		s.release(&extra[i])
	}
	closed := make(chan struct{})
	go func() {
		s.close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not return")
	}
	if p := peak.Load(); p > maxExtenders {
		t.Errorf("%d extensions were in flight at once, want at most %d", p, maxExtenders)
	}
}
