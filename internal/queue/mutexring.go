package queue

import "sync"

// mutexRing is the Safe reference: a ring buffer under one mutex.
type mutexRing[T any] struct {
	mu   sync.Mutex
	buf  []T
	head int
	n    int
}

func newMutexRing[T any](capacity int) *mutexRing[T] {
	return &mutexRing[T]{buf: make([]T, capacity)}
}

func (q *mutexRing[T]) TryPush(v T) bool {
	q.mu.Lock()
	if q.n == len(q.buf) {
		q.mu.Unlock()
		return false
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
	k := min(len(vs), len(q.buf)-q.n)
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

func (q *mutexRing[T]) Cap() int { return len(q.buf) }
