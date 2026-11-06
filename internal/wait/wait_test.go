package wait

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/testguard"
)

// TestKindsList announces the family's kinds to benchmarks/cmd/bakeoff.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, len(Kinds()))
	for _, k := range Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

func TestKindProperties(t *testing.T) {
	if len(Kinds()) != 7 {
		t.Fatalf("Kinds() = %v", Kinds())
	}
	for _, k := range Kinds() {
		if !k.Valid() || k == Auto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if p, ok := ParseKind(k.String()); !ok || p != k {
			t.Fatalf("ParseKind(%q) = %v %v", k, p, ok)
		}
		if mustNew(t, k) == nil {
			t.Fatalf("New(%v) nil", k)
		}
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Valid() || AutoKind == Auto {
		t.Fatal("Auto binding")
	}
	if Kind(99).Valid() || Kind(99).Ident() != "" || Kind(99).String() != "kind(?)" {
		t.Fatal("invalid kind")
	}
	if _, ok := ParseKind("x"); ok {
		t.Fatal("ParseKind junk")
	}
	if Spin.Parks() || Yield.Parks() || !Chan.Parks() || !Hybrid.Parks() {
		t.Fatal("Parks")
	}
	if Spin.WakesOne() || EventCount.WakesOne() || !Chan.WakesOne() || !Cond.WakesOne() || !SpinPark.WakesOne() {
		t.Fatal("WakesOne")
	}
	if _, err := New(Kind(99)); err == nil {
		t.Fatal("New(invalid) should fail")
	}
}

func mustNew(tb testing.TB, k Kind) Strategy {
	tb.Helper()
	s, err := New(k)
	if err != nil {
		tb.Fatalf("New(%v): %v", k, err)
	}
	return s
}

func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "wait")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds wait yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds wait to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestAllocs pins the steady-state Prepare/Notify/Wait cycle. Cond allocates
// its cancellation hook and EventCount a generation per notification; the
// other kinds must not allocate.
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			s := mustNew(t, k)
			ctx := context.Background()
			cycle := func() {
				tk := s.Prepare()
				s.Notify()
				if err := s.Wait(ctx, tk); err != nil {
					t.Fatal(err)
				}
			}
			for range 100 {
				cycle()
			}
			allocs := testing.AllocsPerRun(1000, cycle)
			t.Logf("%v: %.2f allocs/cycle", k, allocs)
			if allocs != 0 && k != Cond && k != EventCount {
				t.Fatalf("%v: %.2f allocs per cycle", k, allocs)
			}
		})
	}
}

// TestCancelRacesWait cancels the context while Wait is entering its park,
// over and over. A cancellation that lands between Wait's last look at the
// context and the park must still wake it: Cond broadcast it without the
// mutex, so a waiter in that window slept through it.
func TestCancelRacesWait(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			s := mustNew(t, k)
			for i := range 2000 {
				tk := s.Prepare()
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { done <- s.Wait(ctx, tk) }()
				cancel()
				select {
				case err := <-done:
					if err == nil {
						t.Fatalf("round %d: Wait returned nil with no notification", i)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("round %d: Wait slept through the cancellation", i)
				}
			}
		})
	}
}

// TestNotifyIsNotSpentOnARecycledWaiter: Notify took the head waiter off the
// list under the lock and set its state only after letting go. In between,
// the head's owner could withdraw and give the waiter back to the free list,
// and a new Prepare could take it, so the notification landed on a ticket
// made after Notify was called while a waiter parked before the call slept
// on. Notify then woke nobody registered before it, the one thing it
// promises.
//
// The window is a few instructions wide, so the test opens it on purpose. On
// one processor, a sync.Mutex in starvation mode hands itself to the next
// waiter at Unlock and the unlocking goroutine yields. The test queues Notify
// for the lock in front of the withdrawing owner (A) and a new Prepare (C), so
// Notify's own Unlock runs A's withdrawal and C's Prepare before Notify sets
// the state. B is parked throughout and is the waiter Notify has to wake.
//
// Another goroutine (the collector, a timer) can take the processor at the
// wrong moment, so a round that did not run C's Prepare inside Notify's call
// is run again, and the test reports a skip only when no round did.
func TestNotifyIsNotSpentOnARecycledWaiter(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	for _, k := range []Kind{Chan, SpinPark, Hybrid} {
		t.Run(k.String(), func(t *testing.T) {
			for range 20 {
				if notifyRecycleRound(t, k) {
					return
				}
			}
			t.Skip("in 20 rounds the scheduler never ran C's Prepare inside Notify's call, so nothing was tested")
		})
	}
}

// notifyRecycleRound runs one round of TestNotifyIsNotSpentOnARecycledWaiter
// on a fresh strategy and reports whether C's Prepare ran inside Notify's
// call, the schedule the round needs.
func notifyRecycleRound(t *testing.T, k Kind) bool {
	t.Helper()
	s, ok := mustNew(t, k).(*listStrategy)
	if !ok {
		t.Fatalf("%v is not a list strategy", k)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctxA, cancelA := context.WithCancel(ctx)
	defer cancelA()
	tA, tB := s.Prepare(), s.Prepare()
	aDone, bDone, cDone := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	// The waiters' goroutines are joined before the round returns, the
	// failing round too: there B sleeps until the cleanup's NotifyAll.
	var waiters sync.WaitGroup
	waiters.Go(func() { aDone <- s.Wait(ctxA, tA) })
	waiters.Go(func() { bDone <- s.Wait(ctx, tB) })
	for tA.w.state.Load() != stParked || tB.w.state.Load() != stParked {
		runtime.Gosched()
	}

	// order tells whether C's Prepare ran before Notify returned.
	var order atomic.Int32
	var notifiedAt int32
	s.mu.Lock()
	notified := make(chan struct{})
	go func() {
		s.Notify()
		notifiedAt = order.Add(1)
		close(notified)
	}()
	runtime.Gosched() // Notify queues for the lock first
	cancelA()
	runtime.Gosched() // A withdraws and queues to give its waiter back
	type prepared struct {
		at     int32
		reused bool
	}
	cPrepared := make(chan prepared, 1)
	waiters.Go(func() {
		tC := s.Prepare()
		cPrepared <- prepared{at: order.Add(1), reused: tC.w == tA.w}
		cDone <- s.Wait(ctx, tC)
	})
	runtime.Gosched() // C queues to Prepare
	// The three have waited over a millisecond when the lock is let go, so
	// Notify, finding it taken again, puts the mutex in starvation mode, and
	// every Unlock after that hands it on.
	time.Sleep(2 * time.Millisecond)
	s.mu.Unlock()
	s.mu.Lock()
	runtime.Gosched()
	s.mu.Unlock()
	<-notified
	c := <-cPrepared
	defer func() {
		s.NotifyAll()
		waiters.Wait()
		if err := <-cDone; err != nil {
			t.Errorf("C's Wait = %v", err)
		}
	}()

	if err := <-aDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("A's Wait = %v, want it withdrawn by its cancellation", err)
	}
	opened := c.at < notifiedAt
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("B's Wait = %v, want the notification", err)
		}
	case <-time.After(time.Second):
		t.Errorf("B, parked before Notify was called, slept through it; C prepared inside Notify's call (%v) and reused the waiter A gave back (%v)",
			opened, c.reused)
	}
	return opened
}

// TestNotifyAllNotifiesEveryWaiterBeforeLettingGo: NotifyAll took the whole
// list off under the lock and set each waiter's state only after letting go,
// so for a moment the list was empty and its waiters not yet notified. In that
// moment the owner of one of them could withdraw and give it back, a new
// Prepare take it, and the new ticket's owner begin to withdraw as well. The
// late state then turned that withdrawal's stCanceled into stNotified, and the
// next Notify, finding a notified waiter at the head of the list, took it for
// one whose owner was spinning and returned. Nobody forwarded it, and a waiter
// parked before that Notify slept on: a lost wake-up, which a memory engine
// fetcher parked beside a ready record slept through.
//
// The test does not race for that moment; it checks that there is none. The
// first time anyone can take the list lock after NotifyAll took the list,
// every waiter it took must already carry its notification. A long list makes
// the moment wide where it exists, so a test that takes the lock as soon as
// NotifyAll lets go of it finds the last waiters still unnotified.
func TestNotifyAllNotifiesEveryWaiterBeforeLettingGo(t *testing.T) {
	// NotifyAll runs beside the test, which takes the lock as it is let go.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(max(2, runtime.GOMAXPROCS(0))))
	for _, k := range []Kind{Chan, SpinPark, Hybrid} {
		t.Run(k.String(), func(t *testing.T) {
			s, ok := mustNew(t, k).(*listStrategy)
			if !ok {
				t.Fatalf("%v is not a list strategy", k)
			}
			const n = 1 << 17
			tickets := make([]Ticket, n)
			for i := range tickets {
				tickets[i] = s.Prepare()
			}
			notified := make(chan struct{})
			go func() {
				s.NotifyAll()
				close(notified)
			}()
			// The lock is taken the moment NotifyAll lets go of it, and held:
			// no withdrawal and no Prepare can run until it is let go again.
			for {
				s.mu.Lock()
				if s.head == nil {
					break
				}
				s.mu.Unlock()
				runtime.Gosched()
			}
			late := 0
			for _, tk := range tickets {
				if tk.w.state.Load() != stNotified {
					late++
				}
			}
			s.mu.Unlock()
			<-notified
			for _, tk := range tickets {
				if err := s.Wait(context.Background(), tk); err != nil {
					t.Fatalf("Wait after NotifyAll = %v, want the notification", err)
				}
			}
			if late > 0 {
				t.Fatalf("NotifyAll let go of the list lock with %d of the %d waiters it took not yet notified: a withdrawal and a new Prepare in that window turn the late notification into a lost wake-up for the next Notify", late, n)
			}
		})
	}
}
