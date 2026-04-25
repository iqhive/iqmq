package testkit

import (
	"context"
	"sync"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
)

// Clock is a deterministic [bitbucket.org/iqhive/iqmq.Clock] for driver tests.
//
// A driver takes a Clock so that nothing in it reads the wall clock (rule
// 4), and this is the one a test drives: time moves when [Clock.Advance]
// is called and at no other moment, so a lease expiry, a redelivery delay and a
// reconnection backoff are all reproducible rather than slow.
//
// It is safe for concurrent use, and every timer it hands out fires when time
// passes its deadline.
type Clock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewClock returns a clock stopped at start. A zero start is taken as an
// arbitrary fixed instant, so that a test that does not care about the absolute
// time still gets a stable one.
func NewClock(start time.Time) *Clock {
	if start.IsZero() {
		start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return &Clock{now: start}
}

// Now returns the clock's current time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimer returns a timer that fires once the clock passes d from now.
func (c *Clock) NewTimer(d time.Duration) mq.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: c, ch: make(chan time.Time, 1), at: c.now.Add(d), armed: true}
	if d <= 0 {
		t.fireLocked(c.now)
		return t
	}
	t.queued = true
	c.timers = append(c.timers, t)
	return t
}

// Advance moves the clock forward and fires every timer that is now due.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	kept := c.timers[:0]
	for _, t := range c.timers {
		switch {
		case !t.armed:
			// Stopped, reset to a non-positive delay, or already fired:
			// nothing is waiting for it, so it leaves the list rather
			// than accumulating there for the life of the clock.
			t.queued = false
		case !t.at.After(c.now):
			t.fireLocked(c.now)
			t.queued = false
		default:
			kept = append(kept, t)
		}
	}
	clear(c.timers[len(kept):])
	c.timers = kept
}

// Timers is how many armed timers the clock holds, which is what a test asserts
// on to show that the specification's "never a timer per delivery" holds.
func (c *Clock) Timers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if t.armed {
			n++
		}
	}
	return n
}

// fakeTimer is one of the clock's timers.
//
// queued is whether the timer is in the clock's pending list, and it is what
// keeps Reset from adding it a second time: a re-armed timer that was already
// pending would otherwise be counted twice by Timers and fired twice by one
// Advance. FuzzClock found it as "Timers is 2, the model holds 1 armed".
type fakeTimer struct {
	c      *Clock
	ch     chan time.Time
	at     time.Time
	armed  bool
	queued bool
}

// C returns the channel the expiry is delivered on.
func (t *fakeTimer) C() <-chan time.Time { return t.ch }

// Stop disarms the timer and reports whether it was still armed.
func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := t.armed
	t.armed = false
	t.drainLocked()
	return was
}

// Reset re-arms the timer and reports whether it had been armed.
func (t *fakeTimer) Reset(d time.Duration) bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := t.armed
	t.drainLocked()
	t.at, t.armed = t.c.now.Add(d), true
	if d <= 0 {
		t.fireLocked(t.c.now)
		return was
	}
	if !t.queued {
		t.queued = true
		t.c.timers = append(t.c.timers, t)
	}
	return was
}

// fireLocked delivers the expiry, dropping it when nobody read the last one,
// which is what a real timer's one-deep buffer does.
func (t *fakeTimer) fireLocked(now time.Time) {
	t.armed = false
	select {
	case t.ch <- now:
	default:
	}
}

// drainLocked removes an undelivered expiry, so that Go 1.23's "no stale value
// after Stop or Reset" holds here too.
func (t *fakeTimer) drainLocked() {
	select {
	case <-t.ch:
	default:
	}
}

// Recorder records the observation points, so that a driver's test can
// assert on what a client reported rather than on what it logged.
//
// It is safe for concurrent use: a hook runs on the goroutine that reached the
// point it observes, and several of those run at once.
type Recorder struct {
	publishStarts counter
	publishDone   counter
	publishErrs   counter
	receives      counter
	messages      counter
	settles       counter
	settleErrs    counter
	reconnects    counter

	mu     sync.Mutex
	lastPD mq.PublishDoneEvent
	lastS  mq.SettleEvent
}

// NewRecorder returns an empty recorder.
func NewRecorder() *Recorder { return &Recorder{} }

// Hooks returns the hook set to build a client with.
func (r *Recorder) Hooks() mq.Hooks {
	return mq.Hooks{
		OnPublishStart: func(context.Context, mq.PublishStartEvent) { r.publishStarts.add(1) },
		OnPublishDone: func(_ context.Context, e mq.PublishDoneEvent) {
			r.publishDone.add(1)
			if e.Err != nil {
				r.publishErrs.add(1)
			}
			r.mu.Lock()
			r.lastPD = e
			r.mu.Unlock()
		},
		OnReceive: func(_ context.Context, e mq.ReceiveEvent) {
			r.receives.add(1)
			r.messages.add(int64(e.Messages))
		},
		OnSettle: func(_ context.Context, e mq.SettleEvent) {
			r.settles.add(1)
			if e.Err != nil {
				r.settleErrs.add(1)
			}
			r.mu.Lock()
			r.lastS = e
			r.mu.Unlock()
		},
		OnReconnect: func(context.Context, mq.ReconnectEvent) { r.reconnects.add(1) },
	}
}

// PublishStarts is how many publications began.
func (r *Recorder) PublishStarts() int64 { return r.publishStarts.load() }

// PublishesDone is how many publications finished.
func (r *Recorder) PublishesDone() int64 { return r.publishDone.load() }

// PublishFailures is how many publications failed.
func (r *Recorder) PublishFailures() int64 { return r.publishErrs.load() }

// Receives is how many fetches returned.
func (r *Recorder) Receives() int64 { return r.receives.load() }

// Messages is how many deliveries those fetches carried.
func (r *Recorder) Messages() int64 { return r.messages.load() }

// Settles is how many settlements finished.
func (r *Recorder) Settles() int64 { return r.settles.load() }

// SettleFailures is how many settlements failed.
func (r *Recorder) SettleFailures() int64 { return r.settleErrs.load() }

// Reconnects is how many reconnection events a driver reported.
func (r *Recorder) Reconnects() int64 { return r.reconnects.load() }

// LastPublish is the most recent completed publication.
func (r *Recorder) LastPublish() mq.PublishDoneEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastPD
}

// LastSettle is the most recent settlement.
func (r *Recorder) LastSettle() mq.SettleEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastS
}
