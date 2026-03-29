package engine

// sidePlacer is the SideQueue placement: a redelivery goes to
// the tail of the lane's redelivery queue, so the failures are retried in the
// order they failed and a record that fails repeatedly does not keep taking
// the head from the ones behind it.
//
// The queue is still served before the lane's ready queue — it is a side
// queue, not a second-class one — so a redelivery is never behind the records
// published after it. Under strict ordering the placement blocks the lane and
// keeps publication order, like every other kind.
type sidePlacer[P any] struct{}

// Requeue appends e to the redelivery queue.
func (sidePlacer[P]) Requeue(ln *lane[P], e *Envelope[P], at, now int64, _ bool) {
	if at <= now {
		at = 0
	}
	ln.retries.push(e, at, false)
}

// Kind returns SideQueue.
func (sidePlacer[P]) Kind() RetryLane { return SideQueue }
