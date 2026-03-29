package engine

// blockPlacer is the BlockLane placement (the reference kind): a
// redelivery holds its lane. The record goes back in publication order and
// the lane serves nothing else — not the records published behind it, and not
// a second redelivery — until it has been settled or dead-lettered.
//
// It is the placement that needs no ordering decision: whatever the topic
// asks for, the lane's delivery order is its publication order, and a record
// that keeps failing stops its lane rather than being overtaken. That is also
// its cost, and the failrate dimension of BenchmarkMemory/OrderedRetry is
// where it is paid.
type blockPlacer[P any] struct{}

// Requeue holds the lane on e.
func (blockPlacer[P]) Requeue(ln *lane[P], e *Envelope[P], at, now int64, _ bool) {
	if at <= now {
		at = 0
	}
	ln.retries.push(e, at, false)
}

// Kind returns BlockLane.
func (blockPlacer[P]) Kind() RetryLane { return BlockLane }
