package flush

import "time"

// count is [Count]: send once MaxItems settlements have accumulated.
//
// It is the cheapest kind per settlement — one round trip per MaxItems, and
// the broker's own batch limit is the number — and the one whose worst case is
// unbounded. A burst of MaxItems-1 settlements followed by silence is held for
// ever: nothing arrives to complete the batch, no ticker fires, and every one
// of those callers is still inside Ack. That is not a defect of the
// implementation, it is what the policy says, and it is why the specification measures "ack
// latency" beside cost.
//
// What bounds it in practice is [driverkit.BatchSettler.Close] and
// [driverkit.BatchSettler.Flush], which send whatever is queued whether the
// policy asked for it or not. So the settlements are never LOST — the
// shutdown-correctness half of the criterion holds — they are merely late,
// and how late depends on something outside the policy.
type count struct{ policy }

// Submitted sends once the queue has reached MaxItems.
func (c count) Submitted(pending int) bool { return pending >= c.cfg.MaxItems }

// Ticked never sends: this kind has no ticker to fire.
func (count) Ticked(int) bool { return false }

// Ticker reports that this kind needs no timer.
func (count) Ticker() (time.Duration, bool) { return 0, false }

// MaxHeld is MaxItems-1: the largest queue that is still waiting for one more.
func (c count) MaxHeld() int { return c.cfg.MaxItems - 1 }
