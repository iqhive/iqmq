package queue

// mpscQueue is a Vyukov ring specialised for one consumer: producers reserve
// with CAS exactly as SeqMPMC does, but the consumer owns the dequeue cursor
// and advances it with a plain store instead of a CAS.
type mpscQueue[T any] struct {
	ring[T]
	enq loadedCursor
	deq loadedCursor // written only by the single consumer
}

func newMPSC[T any](capacity int) *mpscQueue[T] {
	return &mpscQueue[T]{ring: newRing[T](capacity)}
}

func (q *mpscQueue[T]) TryPush(v T) bool {
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
			return false
		}
	}
}

func (q *mpscQueue[T]) TryPop() (T, bool) {
	pos := q.deq.v.Load()
	s := q.slot(pos)
	if s.seq.Load() != pos+1 {
		var zero T
		return zero, false
	}
	v := s.val
	var zero T
	s.val = zero
	s.seq.Store(pos + q.mask + 1)
	q.deq.v.Store(pos + 1)
	return v, true
}

func (q *mpscQueue[T]) PushBatch(vs []T) int {
	for i, v := range vs {
		if !q.TryPush(v) {
			return i
		}
	}
	return len(vs)
}

func (q *mpscQueue[T]) PopBatch(dst []T) int {
	pos := q.deq.v.Load()
	n := 0
	for n < len(dst) {
		s := q.slot(pos)
		if s.seq.Load() != pos+1 {
			break
		}
		dst[n] = s.val
		var zero T
		s.val = zero
		s.seq.Store(pos + q.mask + 1)
		pos++
		n++
	}
	if n > 0 {
		q.deq.v.Store(pos)
	}
	return n
}

func (q *mpscQueue[T]) Len() int {
	return clampLen(q.enq.v.Load(), q.deq.v.Load(), q.capacity())
}

func (q *mpscQueue[T]) Cap() int { return q.capacity() }
