package driverkit

import "time"

// timeNow is the one place this package reads the wall clock, and it exists so
// that forbidigo's ban on time.Now has a single, reviewed exception: the default
// [bitbucket.org/iqhive/iqmq.Clock] has to read the clock somewhere. Nothing on a
// hot path calls it — a helper takes a Clock once, at construction — and a
// driver that wants determinism passes its own.
//
//nolint:forbidigo // the default wall clock is where package time is read; every other path takes an injected Clock
func timeNow() time.Time { return time.Now() }
