package iqmq

import (
	"time"

	"bitbucket.org/iqhive/iqmq/internal/clock"
)

// Clock is the time source iqmq reads. Every component that sleeps, schedules
// or measures takes one, so a test can drive time deterministically and no hot
// path calls time.Now unless an observer is active (rule 4).
//
// Implementations must be safe for concurrent use. The default is the wall
// clock, which delegates to package time and is therefore driven by the bubble
// clock inside testing/synctest.
//
// internal/clock declares the same method sets and its values are NOT
// assignable here: Go's interface satisfaction is invariant in return types,
// so the boundary takes a one-line adapter.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// NewTimer returns a Timer that fires once after d. A non-positive d fires
	// as soon as possible.
	NewTimer(d time.Duration) Timer
}

// Timer is a single-shot timer with the Go 1.23 semantics of *time.Timer:
// after Stop or Reset returns, no stale value is delivered on C.
type Timer interface {
	// C returns the channel the expiry time is delivered on.
	C() <-chan time.Time
	// Stop prevents the timer from firing. It reports whether the call stopped
	// the timer, as opposed to the timer having already fired or been stopped.
	Stop() bool
	// Reset re-arms the timer to fire after d. It reports whether the timer had
	// been active.
	Reset(d time.Duration) bool
}

// clockBridge is the one-line adapter between the root's Clock and
// internal/clock's, and the only place the two cross.
//
// The two packages declare the same method sets, and internal/clock's Timer
// satisfies the root's Timer directly — neither Timer mentions a Clock — but
// Go's interface satisfaction is invariant in return types, so a Clock whose
// NewTimer returns iqmq.Timer is not a clock.Clock however identical the
// timers are. A caller's Clock therefore arrives through WithClock and is
// re-wrapped here.
//
// Nothing on a hot path crosses it: a clock is taken once per handle, and the specification forbids a timer per delivery.
type clockBridge struct{ c Clock }

// Now returns the caller's clock's time.
func (b clockBridge) Now() time.Time { return b.c.Now() }

// NewTimer returns the caller's timer under internal/clock's interface.
func (b clockBridge) NewTimer(d time.Duration) clock.Timer { return b.c.NewTimer(d) }
