package queue

import "sync/atomic"

// spscQueue is a Lamport ring for one producer and one consumer. Each side
// caches the other's cursor and re-reads it only when the cached value says
// the queue is full (producer) or empty (consumer).
type spscQueue[T any] struct {
	buf  []T
	mask uint64

	_          pad
	head       atomic.Uint64 // next position to pop; written by the consumer
	cachedTail uint64        // consumer's view of tail
	_          pad
	tail       atomic.Uint64 // next position to push; written by the producer
	cachedHead uint64        // producer's view of head
	_          pad
}

func newSPSC[T any](capacity int) *spscQueue[T] {
	n := pow2ceil(capacity)
	return &spscQueue[T]{buf: make([]T, n), mask: uint64(n - 1)}
}

func (q *spscQueue[T]) TryPush(v T) bool {
	t := q.tail.Load()
	if t-q.cachedHead > q.mask {
		q.cachedHead = q.head.Load()
		if t-q.cachedHead > q.mask {
			return false
		}
	}
	q.buf[t&q.mask] = v
	q.tail.Store(t + 1)
	return true
}

func (q *spscQueue[T]) TryPop() (T, bool) {
	h := q.head.Load()
	if h == q.cachedTail {
		q.cachedTail = q.tail.Load()
		if h == q.cachedTail {
			var zero T
			return zero, false
		}
	}
	v := q.buf[h&q.mask]
	var zero T
	q.buf[h&q.mask] = zero
	q.head.Store(h + 1)
	return v, true
}

func (q *spscQueue[T]) PushBatch(vs []T) int {
	t := q.tail.Load()
	used := t - q.cachedHead
	if used+uint64(len(vs)) > q.mask+1 {
		q.cachedHead = q.head.Load()
		used = t - q.cachedHead
	}
	k := min(len(vs), int(q.mask+1-used))
	for i := range k {
		q.buf[(t+uint64(i))&q.mask] = vs[i]
	}
	if k > 0 {
		q.tail.Store(t + uint64(k))
	}
	return k
}

func (q *spscQueue[T]) PopBatch(dst []T) int {
	h := q.head.Load()
	avail := q.cachedTail - h
	if uint64(len(dst)) > avail {
		q.cachedTail = q.tail.Load()
		avail = q.cachedTail - h
	}
	k := min(len(dst), int(avail))
	var zero T
	for i := range k {
		idx := (h + uint64(i)) & q.mask
		dst[i] = q.buf[idx]
		q.buf[idx] = zero
	}
	if k > 0 {
		q.head.Store(h + uint64(k))
	}
	return k
}

func (q *spscQueue[T]) Len() int {
	return clampLen(q.tail.Load(), q.head.Load(), int(q.mask)+1)
}

func (q *spscQueue[T]) Cap() int { return int(q.mask) + 1 }
