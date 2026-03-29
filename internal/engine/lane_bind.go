//go:build !iqmq_queue_mpmc && !iqmq_queue_native

package engine

import "bitbucket.org/iqhive/iqmq/internal/queue"

// The untagged binding of The lane's ready queue is a kind behind queue.Queue, and every push and pop is an indirect call.
// It is the reference the other two are measured against, and the only build
// in which every kind is on the fast path.
const (
	laneDispatch    = DispatchInterface
	laneDispatchTag = ""
)

// readyQueue is the lane's ready set. The wrapper exists so that the tagged
// bindings can add a field without every caller changing.
type readyQueue[T any] struct {
	q queue.Queue[T]
}

// newReadyQueue builds the ready set of one lane.
func newReadyQueue[T any](k queue.Kind, capacity int) readyQueue[T] {
	return readyQueue[T]{q: queue.New[T](k, capacity)}
}

// TryPush adds v unless the queue is full.
func (r readyQueue[T]) TryPush(v T) bool { return r.q.TryPush(v) }

// TryPop removes the next value unless the queue is empty.
func (r readyQueue[T]) TryPop() (T, bool) { return r.q.TryPop() }

// Len returns the number of values the queue holds.
func (r readyQueue[T]) Len() int { return r.q.Len() }
