package wait

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// FuzzWait runs fuzz-chosen producer/consumer counts and notification mixes
// through the protocol; a lost wake-up shows up as a consumer stuck until
// the deadline.
func FuzzWait(f *testing.F) {
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), ki)
		w.Int(1, 4, 2)
		w.Int(1, 4, 2)
		w.Int(1, 300, 40)
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		producers, consumers, per := r.Int(1, 4), r.Int(1, 4), r.Int(1, 300)
		if k == Spin {
			// A Spin waiter never gives its processor up, so it is only a
			// waiter while it has one of its own, and the producer that is
			// to wake it needs another. With more spinners than that the
			// producer runs only when the runtime preempts one, every ten
			// milliseconds: the nightly's crasher -- four Spin consumers,
			// one producer that yields after every record -- took 23 s at
			// GOMAXPROCS=1 and 9 s at GOMAXPROCS=2 on one CPU, against the
			// microseconds every other kind takes, and the deadline below
			// then reported a lost wake-up with every record consumed. The
			// run measured the scheduler's quantum, not the protocol, so
			// it is not run where the kind's own premise does not hold.
			//
			// What that gives up is nothing of the protocol. Spin and Yield
			// are one epochStrategy -- Prepare, the epoch comparison,
			// Cancel, Notify and HasWaiters are shared code -- and differ
			// only in the loop Wait polls with (spinForever, yieldForever),
			// so every consumer count this takes from Spin, Yield still
			// runs, with waiters that give the producer its processor.
			// TestLostWakeup also still runs Spin itself with four
			// consumers at any GOMAXPROCS.
			spare := runtime.GOMAXPROCS(0) - 1
			if spare < 1 {
				t.Skip("Spin needs a processor for its waiter and another for the producer; GOMAXPROCS=1 has one")
			}
			consumers = min(consumers, spare)
		}
		s := mustNew(t, k)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var avail, consumed atomic.Int64
		total := int64(producers * per)
		var stuck atomic.Bool
		var wg sync.WaitGroup
		fuzzutil.Actors(t, data[len(data)-r.Remaining():], producers+consumers, func(id int, ar *fuzzutil.Reader) {
			wg.Add(1)
			defer wg.Done()
			if id < producers {
				for i := 0; i < per && !stuck.Load(); i++ {
					avail.Add(1)
					switch ar.Int(0, 3) {
					case 0:
						s.Notify()
					case 1:
						s.NotifyAll()
					default:
						if s.HasWaiters() {
							s.Notify()
						}
					}
					fuzzutil.MaybeYield(ar)
				}
				return
			}
			for !stuck.Load() {
				if consumed.Load() >= total {
					return
				}
				if take(&avail) {
					if consumed.Add(1) == total {
						s.NotifyAll()
					}
					continue
				}
				tk := s.Prepare()
				if avail.Load() > 0 || consumed.Load() >= total || ar.Int(0, 3) == 0 {
					s.Cancel(tk)
					runtime.Gosched()
					continue
				}
				if err := s.Wait(ctx, tk); err != nil {
					stuck.Store(true)
					return
				}
			}
		})
		wg.Wait()
		if stuck.Load() {
			t.Fatalf("%v P=%d C=%d per=%d: consumer blocked past deadline with %d of %d consumed", k, producers, consumers, per, consumed.Load(), total)
		}
		if consumed.Load() != total || s.HasWaiters() {
			t.Fatalf("%v: consumed %d of %d, waiters=%v", k, consumed.Load(), total, s.HasWaiters())
		}
	})
}
