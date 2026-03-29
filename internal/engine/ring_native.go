package engine

import "sync/atomic"

// nativeRing is Vyukov's bounded multi-producer, multi-consumer queue, open
// coded in this package.
//
// It is the native kind (the specification A): the same
// algorithm internal/queue's SeqMPMC uses, compiled here so that a push or a
// pop is neither an interface call nor a cross-package one, and the compiler
// sees the whole operation at the call site. BenchmarkAPI/Dispatch measures
// it against the two that do cross a boundary, and the iqmq_queue_native tag
// binds the lane's ready set to it.
//
// It exists to be measured, not to be configured: the specification owns the queue
// kinds and the engine reaches this one only through lane_bind_native.go. The
// duplication is the point — a copy behind the same package boundary would
// measure the boundary, not its absence — and the engine's model, fuzz and
// differential suites run over it under the tag, which is what holds it to
// the same behaviour as the kind it mirrors.
type nativeRing[T any] struct {
	slots []nativeSlot[T]
	mask  uint64

	_   [cacheLineBytes]byte
	enq atomic.Uint64
	_   [cacheLineBytes - 8]byte
	deq atomic.Uint64
	_   [cacheLineBytes - 8]byte
}

// cacheLineBytes is the assumed cache-line size that keeps the two cursors
// off one another's line.
const cacheLineBytes = 64

// nativeSlot is one ring slot: its value and the sequence number that says
// whose turn it is.
type nativeSlot[T any] struct {
	seq atomic.Uint64
	val T
}

// newNativeRing returns a ring holding at least capacity values, rounded up
// to a power of two so that the cursor wraps with a mask.
//
// Two slots is the floor, for the reason internal/queue's newRing gives: with
// a single slot the sequence value that means "written for pos" (pos+1) and
// the one that means "free again for pos+cap" (pos+cap) coincide, and a
// consumer cannot tell a full ring from an empty one.
func newNativeRing[T any](capacity int) *nativeRing[T] {
	n := uint64(1)
	for n < uint64(max(capacity, 2)) {
		n <<= 1
	}
	r := &nativeRing[T]{slots: make([]nativeSlot[T], n), mask: n - 1}
	for i := range r.slots {
		r.slots[i].seq.Store(uint64(i))
	}
	return r
}

// TryPush adds v and reports whether it fitted. It never blocks: a full ring
// is reported, not waited for.
func (r *nativeRing[T]) TryPush(v T) bool {
	for {
		pos := r.enq.Load()
		s := &r.slots[pos&r.mask]
		diff := int64(s.seq.Load()) - int64(pos)
		switch {
		case diff == 0:
			if r.enq.CompareAndSwap(pos, pos+1) {
				s.val = v
				s.seq.Store(pos + 1)
				return true
			}
		case diff < 0:
			// The slot still holds a value nobody has read: the ring is
			// full for this producer's turn.
			return false
		}
	}
}

// TryPop removes the next value and reports whether there was one.
func (r *nativeRing[T]) TryPop() (T, bool) {
	for {
		pos := r.deq.Load()
		s := &r.slots[pos&r.mask]
		diff := int64(s.seq.Load()) - int64(pos+1)
		switch {
		case diff == 0:
			if r.deq.CompareAndSwap(pos, pos+1) {
				v := s.val
				var zero T
				s.val = zero
				s.seq.Store(pos + r.mask + 1)
				return v, true
			}
		case diff < 0:
			// Empty, or reserved by a producer that has not published yet.
			var zero T
			return zero, false
		}
	}
}

// Len returns the number of values the ring holds. It is exact while the ring
// is quiescent, which is the condition every invariant check runs under.
func (r *nativeRing[T]) Len() int {
	n := int(r.enq.Load() - r.deq.Load())
	switch {
	case n < 0:
		return 0
	case n > len(r.slots):
		return len(r.slots)
	default:
		return n
	}
}

// Cap returns the number of values the ring can hold.
func (r *nativeRing[T]) Cap() int { return len(r.slots) }
