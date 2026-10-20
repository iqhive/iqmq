package wait

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// TestModel checks the protocol semantics of every kind.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("notify-after-prepare", func(t *testing.T) { modelNotifyAfterPrepare(t, k) })
			t.Run("notify-before-prepare-ignored", func(t *testing.T) { modelStaleNotify(t, k) })
			t.Run("cancel", func(t *testing.T) { modelCancel(t, k) })
			t.Run("ctx-cancel", func(t *testing.T) { modelCtxCancel(t, k) })
			t.Run("wake-one-or-all", func(t *testing.T) { modelWakeOne(t, k) })
			t.Run("notify-all", func(t *testing.T) { modelNotifyAll(t, k) })
			t.Run("cancel-forwards", func(t *testing.T) { modelCancelForwards(t, k) })
			t.Run("reuse", func(t *testing.T) { modelReuse(t, k) })
		})
	}
}

func modelNotifyAfterPrepare(t *testing.T, k Kind) {
	s := mustNew(t, k)
	tk := s.Prepare()
	if !s.HasWaiters() {
		t.Fatal("HasWaiters false after Prepare")
	}
	s.Notify()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Wait(ctx, tk); err != nil {
		t.Fatalf("Wait after Notify: %v", err)
	}
	if s.HasWaiters() {
		t.Fatal("HasWaiters true after Wait")
	}
}

func modelStaleNotify(t *testing.T, k Kind) {
	s := mustNew(t, k)
	s.Notify()
	s.NotifyAll()
	tk := s.Prepare()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.Wait(ctx, tk); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale notification woke the waiter: %v", err)
	}
	if s.HasWaiters() {
		t.Fatal("HasWaiters after timeout")
	}
}

func modelCancel(t *testing.T, k Kind) {
	s := mustNew(t, k)
	a, b := s.Prepare(), s.Prepare()
	s.Cancel(a)
	if !s.HasWaiters() {
		t.Fatal("second ticket still outstanding")
	}
	s.Cancel(b)
	if s.HasWaiters() {
		t.Fatal("HasWaiters after cancelling all")
	}
}

func modelCtxCancel(t *testing.T, k Kind) {
	s := mustNew(t, k)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Wait(ctx, s.Prepare()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait with cancelled ctx: %v", err)
	}
	// Cancel while parked.
	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan error, 1)
	tk := s.Prepare()
	go func() { done <- s.Wait(ctx, tk) }()
	time.Sleep(5 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after ctx cancel")
	}
	if s.HasWaiters() {
		t.Fatal("HasWaiters after ctx cancel")
	}
}

// startWaiters parks n waiters and returns a channel that receives one value
// per returned Wait.
func startWaiters(t *testing.T, s Strategy, n int) (<-chan error, *sync.WaitGroup) {
	t.Helper()
	out := make(chan error, n)
	var ready sync.WaitGroup
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	for range n {
		tk := s.Prepare()
		ready.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready.Done()
			out <- s.Wait(ctx, tk)
		}()
	}
	ready.Wait()
	time.Sleep(10 * time.Millisecond) // let them reach the park point
	return out, &wg
}

// drain receives up to want results from out, giving up after d.
func drain(out <-chan error, want int, d time.Duration) int {
	n := 0
	timer := time.NewTimer(d)
	defer timer.Stop()
	for n < want {
		select {
		case err := <-out:
			if err != nil {
				return -1
			}
			n++
		case <-timer.C:
			return n
		}
	}
	return n
}

func modelWakeOne(t *testing.T, k Kind) {
	s := mustNew(t, k)
	out, wg := startWaiters(t, s, 2)
	s.Notify()
	got := drain(out, 2, 100*time.Millisecond) // a wake-one kind leaves the second waiter parked
	switch {
	case got < 0:
		t.Fatal("waiter returned error")
	case k.WakesOne() && got != 1:
		t.Fatalf("Notify woke %d waiters, want exactly 1", got)
	case !k.WakesOne() && got != 2:
		t.Fatalf("broadcast Notify woke %d waiters, want 2", got)
	}
	s.Notify()
	if rest := drain(out, 2-got, 2*time.Second); got+rest != 2 {
		t.Fatalf("after second Notify %d of 2 woke", got+rest)
	}
	wg.Wait()
	if s.HasWaiters() {
		t.Fatal("HasWaiters after all woke")
	}
}

func modelNotifyAll(t *testing.T, k Kind) {
	s := mustNew(t, k)
	out, wg := startWaiters(t, s, 4)
	s.NotifyAll()
	if got := drain(out, 4, 2*time.Second); got != 4 {
		t.Fatalf("NotifyAll woke %d of 4", got)
	}
	wg.Wait()
}

// modelCancelForwards: with wake-one kinds a notification delivered to a
// ticket that is then cancelled must reach another waiter.
func modelCancelForwards(t *testing.T, k Kind) {
	if !k.WakesOne() {
		t.Skip("broadcast kind")
	}
	s := mustNew(t, k)
	first := s.Prepare() // FIFO head, never waits
	out, wg := startWaiters(t, s, 1)
	s.Notify() // delivered to first for list kinds; Cond signals whoever
	time.Sleep(5 * time.Millisecond)
	s.Cancel(first)
	if got := drain(out, 1, 2*time.Second); got != 1 {
		t.Fatalf("forwarded notification did not wake the parked waiter (%d)", got)
	}
	wg.Wait()
}

func modelReuse(t *testing.T, k Kind) {
	s := mustNew(t, k)
	ctx := context.Background()
	for i := range 1000 {
		tk := s.Prepare()
		if i%2 == 0 {
			s.Cancel(tk)
			continue
		}
		s.Notify()
		if err := s.Wait(ctx, tk); err != nil {
			t.Fatal(err)
		}
	}
	if s.HasWaiters() {
		t.Fatal("leaked waiters")
	}
}

// TestLostWakeup runs the producer/consumer protocol with many goroutines;
// a lost wake-up leaves a consumer blocked until the deadline and fails the
// test. Parking kinds additionally run inside a synctest bubble, where a
// lost wake-up is a detected deadlock.
func TestLostWakeup(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("realtime", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				if err := runProtocol(ctx, mustNew(t, k), 4, 4, 2000, true); err != nil {
					t.Fatal(err)
				}
			})
			if !k.Parks() {
				return
			}
			t.Run("synctest", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					if err := runProtocol(t.Context(), mustNew(t, k), 3, 3, 300, false); err != nil {
						t.Fatal(err)
					}
				})
			})
		})
	}
}

// runProtocol moves producers*per work items through a counter guarded by s.
// spin controls whether idle goroutines yield with Gosched (real time) or
// rely on the bubble scheduler (synctest).
func runProtocol(ctx context.Context, s Strategy, producers, consumers, per int, spin bool) error {
	var avail, consumed atomic.Int64
	total := int64(producers * per)
	var wg sync.WaitGroup
	errs := make(chan error, consumers)
	for range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range per {
				avail.Add(1)
				if s.HasWaiters() {
					s.Notify()
				}
				if spin && i%64 == 0 {
					runtime.Gosched()
				}
			}
		}()
	}
	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if consumed.Load() >= total {
					return
				}
				if take(&avail) {
					if consumed.Add(1) == total {
						s.NotifyAll() // release everyone still parked
					}
					continue
				}
				tk := s.Prepare()
				if avail.Load() > 0 || consumed.Load() >= total {
					s.Cancel(tk)
					continue
				}
				if err := s.Wait(ctx, tk); err != nil {
					errs <- errors.New("consumer blocked: " + err.Error() + " (lost wake-up?)")
					return
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}
	if consumed.Load() != total {
		return errors.New("work left unconsumed")
	}
	if s.HasWaiters() {
		return errors.New("waiters outstanding after drain")
	}
	return nil
}

// take decrements avail if positive.
func take(avail *atomic.Int64) bool {
	for {
		n := avail.Load()
		if n <= 0 {
			return false
		}
		if avail.CompareAndSwap(n, n-1) {
			return true
		}
	}
}
