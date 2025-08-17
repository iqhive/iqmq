package clock

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestRealTimerFires(t *testing.T) {
	var c Real
	tm := c.NewTimer(time.Millisecond)
	select {
	case <-tm.C():
	case <-time.After(5 * time.Second):
		t.Fatal("real timer did not fire")
	}
	if tm.Stop() {
		t.Error("Stop after firing reported active")
	}
	if !c.Now().After(time.Time{}) {
		t.Error("Now is zero")
	}
}

func TestRealInsideSynctestBubbleUsesBubbleTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c Real
		start := c.Now()
		tm := c.NewTimer(time.Hour)
		var fired atomic.Bool
		go func() {
			<-tm.C()
			fired.Store(true)
		}()
		synctest.Wait()
		if fired.Load() {
			t.Fatal("timer fired before bubble time advanced")
		}
		time.Sleep(time.Hour)
		synctest.Wait()
		if !fired.Load() {
			t.Fatal("timer did not fire after bubble time advanced")
		}
		if got := c.Now().Sub(start); got != time.Hour {
			t.Fatalf("Now advanced by %v, want 1h", got)
		}
	})
}

func TestFakeAdvanceFiresInOrder(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := NewFake(start)
	t3 := f.NewTimer(3 * time.Second)
	t1 := f.NewTimer(time.Second)
	t2 := f.NewTimer(2 * time.Second)
	if f.Timers() != 3 {
		t.Fatalf("Timers = %d, want 3", f.Timers())
	}
	if next, ok := f.NextDeadline(); !ok || !next.Equal(start.Add(time.Second)) {
		t.Fatalf("NextDeadline = %v,%v", next, ok)
	}
	f.Advance(2 * time.Second)
	if f.Timers() != 1 {
		t.Fatalf("Timers after advance = %d, want 1", f.Timers())
	}
	select {
	case at := <-t1.C():
		if !at.Equal(start.Add(time.Second)) {
			t.Errorf("t1 fired at %v", at)
		}
	default:
		t.Error("t1 did not fire")
	}
	select {
	case at := <-t2.C():
		if !at.Equal(start.Add(2 * time.Second)) {
			t.Errorf("t2 fired at %v", at)
		}
	default:
		t.Error("t2 did not fire")
	}
	select {
	case <-t3.C():
		t.Error("t3 fired early")
	default:
	}
	if !f.Now().Equal(start.Add(2 * time.Second)) {
		t.Errorf("Now = %v", f.Now())
	}
	f.Advance(time.Second)
	select {
	case <-t3.C():
	default:
		t.Error("t3 did not fire")
	}
}

func TestFakeStopAndReset(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	tm := f.NewTimer(time.Second)
	if !tm.Stop() {
		t.Error("Stop on active timer returned false")
	}
	if tm.Stop() {
		t.Error("second Stop returned true")
	}
	f.Advance(2 * time.Second)
	select {
	case <-tm.C():
		t.Error("stopped timer fired")
	default:
	}
	if tm.Reset(time.Second) {
		t.Error("Reset on stopped timer reported active")
	}
	if !tm.Reset(2 * time.Second) {
		t.Error("Reset on active timer reported inactive")
	}
	f.Advance(time.Second)
	select {
	case <-tm.C():
		t.Error("reset timer fired at the old deadline")
	default:
	}
	f.Advance(time.Second)
	select {
	case <-tm.C():
	default:
		t.Error("reset timer did not fire at the new deadline")
	}
	// A fired-but-unreceived value must be discarded by Reset.
	tm.Reset(time.Second)
	f.Advance(time.Second)
	tm.Reset(time.Second)
	select {
	case <-tm.C():
		t.Error("stale value delivered after Reset")
	default:
	}
}

func TestFakeSet(t *testing.T) {
	f := NewFake(time.Unix(100, 0))
	tm := f.NewTimer(10 * time.Second)
	f.Set(time.Unix(50, 0))
	if !f.Now().Equal(time.Unix(50, 0)) {
		t.Errorf("Now after backwards Set = %v", f.Now())
	}
	select {
	case <-tm.C():
		t.Error("timer fired on backwards Set")
	default:
	}
	f.Set(time.Unix(200, 0))
	select {
	case at := <-tm.C():
		if !at.Equal(time.Unix(110, 0)) {
			t.Errorf("fired at %v, want 110", at)
		}
	default:
		t.Error("timer did not fire on forward Set")
	}
}

func TestFakeZeroAndNegativeDurations(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	tm := f.NewTimer(-time.Second)
	f.Advance(0)
	select {
	case <-tm.C():
	default:
		t.Error("negative-duration timer did not fire on zero advance")
	}
	f.Advance(-time.Second)
	if !f.Now().Equal(time.Unix(0, 0)) {
		t.Error("negative Advance moved the clock")
	}
}

func TestCounting(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	c := NewCounting(f)
	c.NewTimer(time.Second)
	c.NewTimer(time.Second)
	if c.Created() != 2 {
		t.Errorf("Created = %d", c.Created())
	}
	c.ResetCount()
	if c.Created() != 0 {
		t.Errorf("Created after reset = %d", c.Created())
	}
	if !c.Now().Equal(f.Now()) {
		t.Error("Counting.Now does not delegate")
	}
}

// FuzzFakeClock drives a Fake clock with a random script of timer
// operations and checks, against a simple model, that every timer fires
// exactly once per arming, never early, and never after Stop.
func FuzzFakeClock(f *testing.F) {
	f.Add([]byte{0, 5, 1, 10, 2, 0, 3, 0})
	f.Add([]byte{0, 1, 0, 1, 1, 200, 4, 0, 0, 3, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		type model struct {
			tm       Timer
			armed    bool
			deadline time.Time
			pending  bool // fired value not yet drained
		}
		start := time.Unix(1000, 0)
		fc := NewFake(start)
		var ms []*model
		check := func() {
			for i, m := range ms {
				select {
				case at := <-m.tm.C():
					if !m.pending {
						t.Fatalf("timer %d fired unexpectedly", i)
					}
					if !at.Equal(m.deadline) {
						t.Fatalf("timer %d fired at %v, deadline %v", i, at, m.deadline)
					}
					m.pending = false
				default:
					if m.pending {
						t.Fatalf("timer %d should have a pending value", i)
					}
				}
			}
		}
		for i := 0; i+1 < len(data); i += 2 {
			op, arg := data[i]%5, time.Duration(data[i+1])*time.Millisecond
			switch op {
			case 0: // new timer
				if len(ms) < 16 {
					ms = append(ms, &model{tm: fc.NewTimer(arg), armed: true, deadline: fc.Now().Add(arg)})
				}
			case 1: // advance
				target := fc.Now().Add(arg)
				fc.Advance(arg)
				for _, m := range ms {
					if m.armed && !m.deadline.After(target) {
						m.armed = false
						m.pending = true
					}
				}
			case 2: // stop
				if len(ms) > 0 {
					m := ms[int(arg/time.Millisecond)%len(ms)]
					was := m.tm.Stop()
					if was != m.armed {
						t.Fatalf("Stop returned %v, model armed %v", was, m.armed)
					}
					m.armed = false
					m.pending = false
				}
			case 3: // reset
				if len(ms) > 0 {
					m := ms[int(arg/time.Millisecond)%len(ms)]
					was := m.tm.Reset(arg)
					if was != m.armed {
						t.Fatalf("Reset returned %v, model armed %v", was, m.armed)
					}
					m.armed = true
					m.pending = false
					m.deadline = fc.Now().Add(arg)
				}
			case 4: // drain checks happen below
			}
			check()
			armed := 0
			for _, m := range ms {
				if m.armed {
					armed++
				}
			}
			if fc.Timers() != armed {
				t.Fatalf("Timers() = %d, model %d", fc.Timers(), armed)
			}
		}
	})
}
