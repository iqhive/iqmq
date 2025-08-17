// Package clock abstracts time for every component of iqmq that sleeps,
// schedules or measures.
//
// Real delegates to package time and is therefore driven by the bubble clock
// when used inside testing/synctest. Fake is an explicitly controlled clock
// for tests that run outside a bubble. Counting wraps any Clock and counts
// the timers it creates, which benchmarks report as timers/op.
package clock

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Clock is the minimal time source used by iqmq.
//
// Implementations must be safe for concurrent use.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// NewTimer returns a Timer that fires once after d. A non-positive d
	// fires as soon as possible.
	NewTimer(d time.Duration) Timer
}

// Timer is a single-shot timer with the Go 1.23+ semantics of time.Timer:
// after Stop or Reset returns, no stale value is delivered on C.
type Timer interface {
	// C returns the channel on which the expiry time is delivered.
	C() <-chan time.Time
	// Stop prevents the timer from firing. It reports whether the call
	// stopped the timer, as opposed to the timer having already fired or
	// been stopped.
	Stop() bool
	// Reset re-arms the timer to fire after d. It reports whether the timer
	// had been active.
	Reset(d time.Duration) bool
}

// Real is the wall-clock implementation backed by package time.
type Real struct{}

// Now implements Clock.
func (Real) Now() time.Time { return time.Now() }

// NewTimer implements Clock.
func (Real) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Stop() bool                 { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// Fake is a manually advanced clock. Time only moves through Advance or Set.
//
// Timers created by a Fake fire during Advance, in deadline order; while a
// timer fires, Now reports that timer's deadline, so code that reads the clock
// from a timer callback observes monotonic time.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	seq    uint64
}

// NewFake returns a Fake clock reading start.
func NewFake(start time.Time) *Fake {
	return &Fake{now: start}
}

// Now implements Clock.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NewTimer implements Clock.
func (f *Fake) NewTimer(d time.Duration) Timer {
	t := &fakeTimer{f: f, ch: make(chan time.Time, 1)}
	f.mu.Lock()
	f.seq++
	t.id = f.seq
	f.arm(t, d)
	f.mu.Unlock()
	return t
}

// arm schedules t to fire at now+d. f.mu must be held.
func (f *Fake) arm(t *fakeTimer, d time.Duration) {
	if d < 0 {
		d = 0
	}
	t.when = f.now.Add(d)
	t.active = true
	f.timers = append(f.timers, t)
}

// remove deletes t from the active list. f.mu must be held.
func (f *Fake) remove(t *fakeTimer) {
	for i, x := range f.timers {
		if x == t {
			last := len(f.timers) - 1
			f.timers[i] = f.timers[last]
			f.timers[last] = nil
			f.timers = f.timers[:last]
			return
		}
	}
}

// Advance moves the clock forward by d, firing every timer whose deadline is
// reached, in deadline order (ties by creation order). A negative d is
// ignored.
func (f *Fake) Advance(d time.Duration) {
	if d < 0 {
		return
	}
	f.mu.Lock()
	target := f.now.Add(d)
	f.advanceTo(target)
	f.mu.Unlock()
}

// Set moves the clock to t, firing timers as Advance does. Moving backwards
// only changes Now and fires nothing.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	if t.After(f.now) {
		f.advanceTo(t)
	} else {
		f.now = t
	}
	f.mu.Unlock()
}

// advanceTo fires due timers up to target and sets now = target. f.mu held.
func (f *Fake) advanceTo(target time.Time) {
	for {
		var next *fakeTimer
		for _, t := range f.timers {
			if t.when.After(target) {
				continue
			}
			if next == nil || t.when.Before(next.when) || (t.when.Equal(next.when) && t.id < next.id) {
				next = t
			}
		}
		if next == nil {
			break
		}
		if next.when.After(f.now) {
			f.now = next.when
		}
		next.active = false
		f.remove(next)
		select {
		case next.ch <- f.now:
		default:
		}
	}
	f.now = target
}

// Timers returns the number of armed timers.
func (f *Fake) Timers() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

// NextDeadline returns the earliest armed deadline and true, or the zero time
// and false when no timer is armed.
func (f *Fake) NextDeadline() (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.timers) == 0 {
		return time.Time{}, false
	}
	sorted := make([]*fakeTimer, len(f.timers))
	copy(sorted, f.timers)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].when.Before(sorted[j].when) })
	return sorted[0].when, true
}

type fakeTimer struct {
	f      *Fake
	ch     chan time.Time
	when   time.Time
	active bool
	id     uint64
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	was := t.active
	if was {
		t.active = false
		t.f.remove(t)
	}
	t.drain()
	return was
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	was := t.active
	if was {
		t.f.remove(t)
	}
	t.drain()
	t.f.arm(t, d)
	return was
}

// drain discards a fired-but-unreceived value so that Stop/Reset guarantee no
// stale delivery, mirroring time.Timer since Go 1.23.
func (t *fakeTimer) drain() {
	select {
	case <-t.ch:
	default:
	}
}

// Counting wraps a Clock and counts the timers created through it. It is
// used by benchmarks to report timers/op and by tests asserting that no timer
// is created per message.
type Counting struct {
	Clock
	created atomic.Int64
}

// NewCounting wraps c.
func NewCounting(c Clock) *Counting { return &Counting{Clock: c} }

// NewTimer implements Clock and increments the created counter.
func (c *Counting) NewTimer(d time.Duration) Timer {
	c.created.Add(1)
	return c.Clock.NewTimer(d)
}

// Created returns the number of timers created so far.
func (c *Counting) Created() int64 { return c.created.Load() }

// ResetCount sets the created counter to zero.
func (c *Counting) ResetCount() { c.created.Store(0) }
