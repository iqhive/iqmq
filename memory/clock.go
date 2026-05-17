package memory

import (
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/clock"
)

// TestClock is a manually advanced time source for a memory broker
// (the memory engine's "advance deterministic time").
//
// Time only moves through [Broker.Advance], which moves this clock and then
// runs the sweeps the move implies — records past their time to live,
// retained records past their age bound, leases past their deadline — and
// wakes the consumers the promotions woke. Moving the clock without running
// those sweeps would leave the broker in a state no real passage of time
// produces, so this type deliberately has no Advance of its own.
//
// A test that would rather not drive time at all uses testing/synctest
// instead: the default wall clock follows the bubble clock, and a forgotten
// Close then surfaces as a bubble deadlock, which is the free leak
// detection.
//
// It is safe for concurrent use.
type TestClock struct{ fake *clock.Fake }

// NewTestClock returns a clock reading start. A zero start reads as the Unix
// epoch, so that a test never has to name a time it does not care about and
// two runs of the same test see the same timestamps.
func NewTestClock(start time.Time) *TestClock {
	if start.IsZero() {
		start = time.Unix(0, 0).UTC()
	}
	return &TestClock{fake: clock.NewFake(start)}
}

// Now returns the clock's current time.
func (c *TestClock) Now() time.Time { return c.fake.Now() }

// NewTimer returns a timer that fires when the clock is advanced past d.
func (c *TestClock) NewTimer(d time.Duration) mq.Timer { return c.fake.NewTimer(d) }

// Timers returns the number of timers waiting on this clock, which is what a
// test asserting the specification's "no timer per message" reads.
func (c *TestClock) Timers() int { return c.fake.Timers() }

// realClock is the wall clock as an mq.Clock, which is what Broker.Clock
// answers for a broker that was given none.
type realClock struct{}

// Now returns the wall-clock time.
func (realClock) Now() time.Time { return clock.Real{}.Now() }

// NewTimer returns a wall-clock timer.
func (realClock) NewTimer(d time.Duration) mq.Timer { return clock.Real{}.NewTimer(d) }

// clockBridge adapts a caller's mq.Clock to the engine's.
//
// The two interfaces declare the same method sets and their Timers are the
// same type, but Go's interface satisfaction is invariant in return types, so
// an mq.Clock is not an engine clock however identical the timers look — the
// same one-line crossing the root package makes for its own WithClock.
// Nothing on a hot path crosses it: the engine takes a
// clock once, at construction, and the specification forbids a timer per delivery.
type clockBridge struct{ c mq.Clock }

// Now returns the caller's clock's time.
func (b clockBridge) Now() time.Time { return b.c.Now() }

// NewTimer returns the caller's timer under the engine's interface.
func (b clockBridge) NewTimer(d time.Duration) clock.Timer { return b.c.NewTimer(d) }
