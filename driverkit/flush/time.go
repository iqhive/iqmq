package flush

import "time"

// timed is [Time]: send every Interval, whatever has accumulated.
//
// Its cost falls with the arrival rate (at ten settlements per interval it is
// a tenth of [Immediate]'s round trips, at one it is the same), and the wait
// before a settlement is sent is bounded at every rate, which is the property
// [Count] does not have. The bound is one interval plus round trips: the next
// interval starts once the tick's sends have finished, and a settlement also
// waits for the batches its own tick sends ahead of it. The caller's Ack
// returns a round trip after that: see the package documentation. One timer
// per settler and never one per delivery the ticker is the kit, and this
// policy only names its period.
//
// A tick that finds an empty queue sends nothing, so an idle consumer costs
// the timer and no requests at all.
type timed struct{ policy }

// Submitted never sends: this kind waits for the tick.
func (timed) Submitted(int) bool { return false }

// Ticked sends whatever has accumulated, and nothing when nothing has.
func (timed) Ticked(pending int) bool { return pending > 0 }

// Ticker is the configured interval.
func (t timed) Ticker() (time.Duration, bool) { return t.cfg.Interval, true }

// MaxHeld is -1: this kind bounds nothing.
//
// Between two ticks every arrival is queued and nothing sends, so the queue is
// as long as the arrival rate times the interval — which is a property of the
// workload, not of the policy. It is the one kind of the four with no bound of
// its own, and the reason [Hybrid] exists.
func (timed) MaxHeld() int { return -1 }
