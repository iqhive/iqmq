//go:build iqmq_debug

package dispatch

import "runtime"

// debugWindows reports whether the close-path scheduling seams are compiled
// in. They are, in an iqmq_debug build.
const debugWindows = true

// refusalWindow yields the processor in the middle of the window a submission
// the closing flag refused holds a reservation on the admission gate. It is a
// no-op in an ordinary build.
//
// The window is where the deadlock of the specification lived, and it is two
// instructions wide, so the race test that guards it needs roughly 34,000
// rounds to find the interleaving on its own. Yielding here brings that down
// to a few hundred, which is what makes TestCloseRaceWithdrawnReservations a
// guard rather than a lottery in the test-tags gate.
func refusalWindow() { runtime.Gosched() }
