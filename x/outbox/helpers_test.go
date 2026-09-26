package outbox_test

import (
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/outbox"
)

// ambiguousByCode returns an error carrying CodeAmbiguous.
func ambiguousByCode() error {
	return &mq.OpError{Operation: "publish", Code: mq.CodeAmbiguous, Err: errPublish}
}

// ambiguousByFlag returns an error whose *OpError sets the Ambiguous flag
// without the code, which is the other half of the surface a driver may
// use.
func ambiguousByFlag() error {
	return &mq.OpError{Operation: "publish", Code: mq.CodeUnavailable, Ambiguous: true, Err: errPublish}
}

var errPublish = errSentinel("the broker did not answer")

type errSentinel string

func (e errSentinel) Error() string { return string(e) }

// fakeClock is a manually advanced mq.Clock, so that a lock's lapse is
// watched rather than slept through.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTimer(d time.Duration) mq.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{ch: make(chan time.Time, 1), at: c.now.Add(d)}
	c.timers = append(c.timers, t)
	return t
}

// advance moves the clock and fires every timer due at or before the new
// time.
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	due := c.timers[:0:0]
	kept := c.timers[:0:0]
	for _, t := range c.timers {
		if !t.at.After(now) {
			due = append(due, t)
			continue
		}
		kept = append(kept, t)
	}
	c.timers = kept
	c.mu.Unlock()
	for _, t := range due {
		select {
		case t.ch <- now:
		default:
		}
	}
}

type fakeTimer struct {
	ch chan time.Time
	at time.Time
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }
func (t *fakeTimer) Stop() bool          { return true }
func (t *fakeTimer) Reset(time.Duration) bool {
	return true
}

// compile-time assertions that the fixtures satisfy the interfaces they
// stand in for, so a signature change is a build failure here rather than a
// confusing test failure elsewhere.
var (
	_ mq.Clock     = (*fakeClock)(nil)
	_ mq.Timer     = (*fakeTimer)(nil)
	_ outbox.Store = (*outbox.MemoryStore)(nil)
)
