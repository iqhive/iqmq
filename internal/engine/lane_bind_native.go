//go:build iqmq_queue_native && !iqmq_queue_mpmc

package engine

import "github.com/iqhive/iqmq/internal/queue"

// The iqmq_queue_native binding of A lane configured with the
// SeqMPMC kind holds this package's own ring, so the whole push and pop is
// visible to the compiler at the call site and there is no cross-package
// call to inline or not. It is the floor the other two mechanisms are
// measured against.
//
// iqmq_queue_mpmc wins when both tags are set, which is why the constraint
// above excludes it: two bindings of one field would not compile, and the
// concrete one is the binding the row's gate names.
const (
	laneDispatch    = DispatchNative
	laneDispatchTag = "iqmq_queue_native"
)

// readyQueue is the lane's ready set: this package's ring for the bound kind,
// or any other kind behind the interface.
type readyQueue[T any] struct {
	n *nativeRing[T]
	q queue.Queue[T]
}

// newReadyQueue builds the ready set of one lane.
func newReadyQueue[T any](k queue.Kind, capacity int) readyQueue[T] {
	if k.Resolve() == queue.SeqMPMC {
		return readyQueue[T]{n: newNativeRing[T](capacity)}
	}
	return readyQueue[T]{q: queue.New[T](k, capacity)}
}

// TryPush adds v unless the queue is full.
func (r readyQueue[T]) TryPush(v T) bool {
	if r.n != nil {
		return r.n.TryPush(v)
	}
	return r.q.TryPush(v)
}

// TryPop removes the next value unless the queue is empty.
func (r readyQueue[T]) TryPop() (T, bool) {
	if r.n != nil {
		return r.n.TryPop()
	}
	return r.q.TryPop()
}

// Len returns the number of values the queue holds.
func (r readyQueue[T]) Len() int {
	if r.n != nil {
		return r.n.Len()
	}
	return r.q.Len()
}
