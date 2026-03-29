package engine

// frontPlacer is the PushFront placement: a redelivery goes to
// the head of the lane's redelivery queue, which is served before the ready
// queue, so the record a consumer just failed on is the next one it sees.
// That is the redelivery-first behaviour of a broker that re-heads its
// channel, and it is what the memory engine names first.
//
// Two redeliveries outstanding at once are therefore served newest first,
// which is the one way the placement disturbs the lane's order — and only
// while the topic does not ask for strict ordering, under which the placement
// keeps publication order and blocks the lane like every other kind.
type frontPlacer[P any] struct{}

// Requeue puts e at the head of the redelivery queue.
func (frontPlacer[P]) Requeue(ln *lane[P], e *Envelope[P], at, now int64, _ bool) {
	if at <= now {
		at = 0
	}
	ln.retries.push(e, at, true)
}

// Kind returns PushFront.
func (frontPlacer[P]) Kind() RetryLane { return PushFront }
