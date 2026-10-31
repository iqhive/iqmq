package flush

import "time"

// hybrid is [Hybrid]: send at whichever of the count and the interval comes
// first.
//
// It is the expected winner ("hybrid (≤ 100 ms flush)") and the reason is
// that the two halves bound different things. The count bounds the COST: a
// full batch never waits, so a busy consumer pays one round trip per MaxItems
// however fast it settles. The interval bounds the WAIT: a partial batch goes
// with the next tick, which comes one interval after the previous tick's
// sends finished, so the last settlement of a burst is not held for ever the
// way [Count] holds it. The caller's Ack returns once the batch's round trip
// has come back, so the latency is that wait plus the round trip: see the
// package documentation.
//
// What it costs against [Immediate] is the window: a settlement that has been
// queued and not sent is one the caller is still waiting inside Ack for, so a
// crash in that window loses nothing the application believes — the Ack has
// not returned — but a crash after the send and before the reply leaves the
// outcome unknown for a whole batch rather than for one settlement. That is
// the trade the bake-off is deciding, and it is why [Immediate] is the
// reference and not the expected winner.
type hybrid struct{ policy }

// Submitted sends once the queue has reached MaxItems.
func (h hybrid) Submitted(pending int) bool { return pending >= h.cfg.MaxItems }

// Ticked sends whatever has accumulated, and nothing when nothing has.
func (hybrid) Ticked(pending int) bool { return pending > 0 }

// Ticker is the configured interval.
func (h hybrid) Ticker() (time.Duration, bool) { return h.cfg.Interval, true }

// MaxHeld is MaxItems-1: a full batch is sent by Submitted, so a tick can only
// ever find fewer.
func (h hybrid) MaxHeld() int { return h.cfg.MaxItems - 1 }
