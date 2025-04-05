package queue

import (
	"runtime"
	"sync/atomic"
)

// disruptor is a Disruptor-style multi-producer sequencer. Producers claim a
// sequence on the shared claim cursor with a CAS that admits it only while
// the ring has room -- the Disruptor's tryNext -- then publish the slot's
// round; consumers claim the shared consumer cursor with CAS, read, and
// release the slot's round. A producer whose slot's previous occupant is
// still being read spins until that consumer releases it, which it does
// right after reading.
//
// The claim was a single fetch-add (no CAS retry loop), the Disruptor's
// blocking next, behind a racy fullness check. A fetch-add cannot be taken
// back, so two producers that passed the check with one slot free both held
// a sequence, and the second waited for a consumer to take the slot's
// previous occupant -- waiting for capacity, which the package promises no
// kind does, and for ever when no consumer came.
type disruptor[T any] struct {
	buf      []T
	avail    []atomic.Int64 // round published per slot, -1 initially
	released []atomic.Int64 // round released per slot, -1 initially
	mask     uint64
	shift    uint

	claim  loadedCursor // next sequence for producers
	cursor loadedCursor // next sequence for consumers
}

func newDisruptor[T any](capacity int) *disruptor[T] {
	n := pow2ceil(capacity)
	q := &disruptor[T]{
		buf:      make([]T, n),
		avail:    make([]atomic.Int64, n),
		released: make([]atomic.Int64, n),
		mask:     uint64(n - 1),
	}
	for n > 1 {
		q.shift++
		n >>= 1
	}
	for i := range q.avail {
		q.avail[i].Store(-1)
		q.released[i].Store(-1)
	}
	return q
}

func (q *disruptor[T]) round(seq uint64) int64 { return int64(seq >> q.shift) }

func (q *disruptor[T]) TryPush(v T) bool {
	var seq uint64
	for {
		seq = q.claim.v.Load()
		// Signed: a cursor read after other producers moved the claim on
		// can be past seq, and then the CAS below fails and retries rather
		// than calling the ring full.
		if int64(seq-q.cursor.v.Load()) > int64(q.mask) {
			return false // full at this instant
		}
		if q.claim.v.CompareAndSwap(seq, seq+1) {
			break
		}
	}
	idx := seq & q.mask
	r := q.round(seq)
	// The slot's previous occupant was behind the consumer cursor when the
	// claim was admitted, so a consumer has already taken it: only that
	// consumer finishing its read can delay this, and only briefly.
	for q.released[idx].Load() != r-1 {
		runtime.Gosched()
	}
	q.buf[idx] = v
	q.avail[idx].Store(r)
	return true
}

func (q *disruptor[T]) TryPop() (T, bool) {
	for {
		seq := q.cursor.v.Load()
		idx := seq & q.mask
		if q.avail[idx].Load() != q.round(seq) {
			var zero T
			return zero, false // empty, or claimed but not yet published
		}
		if q.cursor.v.CompareAndSwap(seq, seq+1) {
			v := q.buf[idx]
			var zero T
			q.buf[idx] = zero
			q.released[idx].Store(q.round(seq))
			return v, true
		}
	}
}

func (q *disruptor[T]) PushBatch(vs []T) int {
	for i, v := range vs {
		if !q.TryPush(v) {
			return i
		}
	}
	return len(vs)
}

func (q *disruptor[T]) PopBatch(dst []T) int {
	for i := range dst {
		v, ok := q.TryPop()
		if !ok {
			return i
		}
		dst[i] = v
	}
	return len(dst)
}

func (q *disruptor[T]) Len() int {
	return clampLen(q.claim.v.Load(), q.cursor.v.Load(), int(q.mask)+1)
}

func (q *disruptor[T]) Cap() int { return int(q.mask) + 1 }
