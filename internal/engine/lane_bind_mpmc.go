//go:build iqmq_queue_mpmc

package engine

import "github.com/iqhive/iqmq/internal/queue"

// The iqmq_queue_mpmc binding of A lane configured with the
// SeqMPMC kind holds it as queue.MPMC, a concrete type, so its pushes and
// pops are static calls the compiler may inline. Every other kind still
// works — it takes the interface field, exactly as the untagged build does —
// so the tag narrows the fast path and not the configuration, which is what
// keeps the differential tests running under it.
const (
	laneDispatch    = DispatchConcrete
	laneDispatchTag = "iqmq_queue_mpmc"
)

// readyQueue is the lane's ready set: the bound kind as a concrete type, or
// any other kind behind the interface.
type readyQueue[T any] struct {
	c *queue.MPMC[T]
	q queue.Queue[T]
}

// newReadyQueue builds the ready set of one lane.
func newReadyQueue[T any](k queue.Kind, capacity int) (readyQueue[T], error) {
	if k.Resolve() == queue.SeqMPMC {
		return readyQueue[T]{c: queue.NewMPMC[T](capacity)}, nil
	}
	q, err := queue.New[T](k, capacity)
	return readyQueue[T]{q: q}, err
}

// TryPush adds v unless the queue is full.
func (r readyQueue[T]) TryPush(v T) bool {
	if r.c != nil {
		return r.c.TryPush(v)
	}
	return r.q.TryPush(v)
}

// TryPop removes the next value unless the queue is empty.
func (r readyQueue[T]) TryPop() (T, bool) {
	if r.c != nil {
		return r.c.TryPop()
	}
	return r.q.TryPop()
}

// Len returns the number of values the queue holds.
func (r readyQueue[T]) Len() int {
	if r.c != nil {
		return r.c.Len()
	}
	return r.q.Len()
}
