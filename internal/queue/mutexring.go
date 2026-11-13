package queue

import "sync"

// mutexRing is the Safe reference: a ring buffer under one mutex.
//
// Its capacity is a bound and not an allocation. The buffer starts empty and
// doubles as values arrive, up to the capacity, so a queue that never holds
// much never costs much: an engine lane sizes its ready queue at the lane's
// CapacityMessages, which may be far larger than the lane ever holds. The
// buffer keeps the size it grew to.
type mutexRing[T any] struct {
	mu       sync.Mutex
	buf      []T
	head     int
	n        int
	capacity int
}

// minRing is the buffer a ring takes for its first value, or its capacity
// when that is smaller.
const minRing = 16

func newMutexRing[T any](capacity int) *mutexRing[T] {
	return &mutexRing[T]{capacity: capacity}
}

// grow makes the buffer hold at least want values, doubling it and never
// past the capacity, and lays the values out from the start of the new
// buffer. want must not exceed the capacity. The caller holds the mutex.
func (q *mutexRing[T]) grow(want int) {
	size := min(max(2*len(q.buf), minRing, want), q.capacity)
	buf := make([]T, size)
	k := copy(buf, q.buf[q.head:min(q.head+q.n, len(q.buf))])
	copy(buf[k:], q.buf[:q.n-k])
	q.buf, q.head = buf, 0
}

func (q *mutexRing[T]) TryPush(v T) bool {
	q.mu.Lock()
	if q.n == len(q.buf) {
		if q.n == q.capacity {
			q.mu.Unlock()
			return false
		}
		q.grow(q.n + 1)
	}
	i := q.head + q.n
	if i >= len(q.buf) {
		i -= len(q.buf)
	}
	q.buf[i] = v
	q.n++
	q.mu.Unlock()
	return true
}

func (q *mutexRing[T]) TryPop() (T, bool) {
	var zero T
	q.mu.Lock()
	if q.n == 0 {
		q.mu.Unlock()
		return zero, false
	}
	v := q.buf[q.head]
	q.buf[q.head] = zero
	q.head++
	if q.head == len(q.buf) {
		q.head = 0
	}
	q.n--
	q.mu.Unlock()
	return v, true
}

func (q *mutexRing[T]) PushBatch(vs []T) int {
	q.mu.Lock()
	k := min(len(vs), q.capacity-q.n)
	if q.n+k > len(q.buf) {
		q.grow(q.n + k)
	}
	for _, v := range vs[:k] {
		i := q.head + q.n
		if i >= len(q.buf) {
			i -= len(q.buf)
		}
		q.buf[i] = v
		q.n++
	}
	q.mu.Unlock()
	return k
}

func (q *mutexRing[T]) PopBatch(dst []T) int {
	var zero T
	q.mu.Lock()
	k := min(len(dst), q.n)
	for i := range k {
		dst[i] = q.buf[q.head]
		q.buf[q.head] = zero
		q.head++
		if q.head == len(q.buf) {
			q.head = 0
		}
	}
	q.n -= k
	q.mu.Unlock()
	return k
}

func (q *mutexRing[T]) Len() int {
	q.mu.Lock()
	n := q.n
	q.mu.Unlock()
	return n
}

func (q *mutexRing[T]) Cap() int { return q.capacity }
