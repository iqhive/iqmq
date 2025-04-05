package queue

// seqMPMC is Vyukov's bounded MPMC queue: producers CAS the enqueue cursor to
// reserve a slot, write it, then publish by bumping the slot sequence;
// consumers mirror that on the dequeue cursor. Full and empty are detected
// from the slot sequence alone, so neither side reads the other's cursor on
// the fast path.
type seqMPMC[T any] struct {
	ring[T]
	enq loadedCursor
	deq loadedCursor
}

func newSeqMPMC[T any](capacity int) *seqMPMC[T] {
	return &seqMPMC[T]{ring: newRing[T](capacity)}
}

func (q *seqMPMC[T]) TryPush(v T) bool {
	for {
		pos := q.enq.v.Load()
		s := q.slot(pos)
		diff := int64(s.seq.Load()) - int64(pos)
		switch {
		case diff == 0:
			if q.enq.v.CompareAndSwap(pos, pos+1) {
				s.val = v
				s.seq.Store(pos + 1)
				return true
			}
		case diff < 0:
			return false // slot still holds an unread element: full
		}
	}
}

func (q *seqMPMC[T]) TryPop() (T, bool) {
	for {
		pos := q.deq.v.Load()
		s := q.slot(pos)
		diff := int64(s.seq.Load()) - int64(pos+1)
		switch {
		case diff == 0:
			if q.deq.v.CompareAndSwap(pos, pos+1) {
				v := s.val
				var zero T
				s.val = zero
				s.seq.Store(pos + q.mask + 1)
				return v, true
			}
		case diff < 0:
			var zero T
			return zero, false // empty, or reserved but not yet published
		}
	}
}

func (q *seqMPMC[T]) PushBatch(vs []T) int {
	for i, v := range vs {
		if !q.TryPush(v) {
			return i
		}
	}
	return len(vs)
}

func (q *seqMPMC[T]) PopBatch(dst []T) int {
	for i := range dst {
		v, ok := q.TryPop()
		if !ok {
			return i
		}
		dst[i] = v
	}
	return len(dst)
}

func (q *seqMPMC[T]) Len() int {
	return clampLen(q.enq.v.Load(), q.deq.v.Load(), q.capacity())
}

func (q *seqMPMC[T]) Cap() int { return q.capacity() }
