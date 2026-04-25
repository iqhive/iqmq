package testkit_test

import (
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// FuzzClock drives the deterministic clock with an op script and checks it
// against a reference model.
//
// The clock is what a driver's tests make a lease expire and a backoff elapse
// with, so a defect in it does not fail — it makes a driver's own tests pass for
// the wrong reason. The invariants after every step:
//
//   - Time moves only when Advance is called, and by exactly what it was given.
//   - A timer fires when, and only when, the clock passes its deadline. Never
//     early, and never skipped by a jump that steps over it.
//   - Stop and Reset leave no stale value on the channel, which is the Go 1.23
//     timer contract mq.Timer states; a timer that delivered one would make a
//     shared lease scheduler act on an extension it never scheduled.
//   - Timers() counts the armed timers and nothing else, which is what the specification's
//     "never a timer per delivery" is asserted with.
func FuzzClock(f *testing.F) {
	seed := func(steps func(w *fuzzutil.Writer)) {
		w := fuzzutil.NewWriter()
		steps(w)
		f.Add(w.Data())
	}
	seed(func(w *fuzzutil.Writer) {
		w.Choice(4, 0)
		w.Duration(time.Hour, time.Second)
		w.Choice(4, 1)
		w.Duration(time.Hour, 2*time.Second)
	})
	seed(func(w *fuzzutil.Writer) {
		w.Choice(4, 0)
		w.Duration(time.Hour, 0)
	})
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8})

	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		c := testkit.NewClock(start)
		type armed struct {
			timer mq.Timer
			at    time.Time
			live  bool
			ready bool // an expiry is sitting unread on the channel
		}
		var model []*armed
		now := start
		const maxTimers = 8
		for !r.Exhausted() {
			switch r.Choice(4) {
			case 0: // arm a timer
				if len(model) >= maxTimers {
					continue
				}
				d := r.Duration(time.Hour)
				tm := c.NewTimer(d)
				a := &armed{timer: tm, at: now.Add(d), live: d > 0, ready: d <= 0}
				model = append(model, a)
			case 1: // advance
				d := r.Duration(time.Hour)
				if d <= 0 {
					continue
				}
				now = now.Add(d)
				c.Advance(d)
				for _, a := range model {
					if a.live && !a.at.After(now) {
						a.live, a.ready = false, true
					}
				}
			case 2: // stop a timer
				if len(model) == 0 {
					continue
				}
				a := model[r.Int(0, len(model)-1)]
				was := a.timer.Stop()
				if was != a.live {
					t.Fatalf("Stop reported %v, the model says the timer was armed=%v", was, a.live)
				}
				a.live, a.ready = false, false
			case 3: // reset a timer
				if len(model) == 0 {
					continue
				}
				a := model[r.Int(0, len(model)-1)]
				d := r.Duration(time.Hour)
				was := a.timer.Reset(d)
				if was != a.live {
					t.Fatalf("Reset reported %v, the model says the timer was armed=%v", was, a.live)
				}
				a.at, a.live, a.ready = now.Add(d), d > 0, d <= 0
			}
			if got := c.Now(); !got.Equal(now) {
				t.Fatalf("Now is %v, the model says %v", got, now)
			}
			live := 0
			for _, a := range model {
				if a.live {
					live++
				}
			}
			if got := c.Timers(); got != live {
				t.Fatalf("Timers is %d, the model holds %d armed", got, live)
			}
		}
		// Every channel holds exactly what the model says it holds: an
		// expiry for a timer that fired and nothing was read from, and
		// nothing at all for one that was stopped or reset.
		for i, a := range model {
			select {
			case at := <-a.timer.C():
				if !a.ready {
					t.Fatalf("timer %d delivered %v with nothing due: a stale value after Stop or Reset", i, at)
				}
				if at.After(now) {
					t.Fatalf("timer %d fired at %v, which is after the clock's %v", i, at, now)
				}
			default:
				if a.ready {
					t.Fatalf("timer %d was due and delivered nothing", i)
				}
			}
		}
	})
}
