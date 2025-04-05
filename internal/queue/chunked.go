package queue

import "sync"

// chunkSize is the number of elements per chunk.
const chunkSize = 128

type chunk[T any] struct {
	vals [chunkSize]T
	next *chunk[T]
}

// chunked is an unbounded queue of fixed-size chunks under one mutex. One
// emptied chunk is kept as a spare so steady-state traffic does not allocate.
type chunked[T any] struct {
	mu    sync.Mutex
	head  *chunk[T] // chunk being read
	tail  *chunk[T] // chunk being written
	hoff  int       // read offset in head
	toff  int       // write offset in tail
	n     int
	spare *chunk[T]
}

func newChunked[T any]() *chunked[T] {
	c := &chunk[T]{}
	return &chunked[T]{head: c, tail: c}
}

func (q *chunked[T]) grow() {
	c := q.spare
	if c != nil {
		q.spare = nil
		c.next = nil
	} else {
		c = &chunk[T]{}
	}
	q.tail.next = c
	q.tail = c
	q.toff = 0
}

func (q *chunked[T]) pushLocked(v T) {
	if q.toff == chunkSize {
		q.grow()
	}
	q.tail.vals[q.toff] = v
	q.toff++
	q.n++
}

func (q *chunked[T]) popLocked() T {
	var zero T
	v := q.head.vals[q.hoff]
	q.head.vals[q.hoff] = zero
	q.hoff++
	q.n--
	if q.hoff == chunkSize {
		if q.head.next == nil {
			// Single chunk fully consumed: rewind in place.
			q.hoff, q.toff = 0, 0
		} else {
			old := q.head
			q.head = old.next
			q.hoff = 0
			old.next = nil
			q.spare = old
		}
	}
	return v
}

func (q *chunked[T]) TryPush(v T) bool {
	q.mu.Lock()
	q.pushLocked(v)
	q.mu.Unlock()
	return true
}

func (q *chunked[T]) TryPop() (T, bool) {
	q.mu.Lock()
	if q.n == 0 {
		q.mu.Unlock()
		var zero T
		return zero, false
	}
	v := q.popLocked()
	q.mu.Unlock()
	return v, true
}

func (q *chunked[T]) PushBatch(vs []T) int {
	q.mu.Lock()
	for _, v := range vs {
		q.pushLocked(v)
	}
	q.mu.Unlock()
	return len(vs)
}

func (q *chunked[T]) PopBatch(dst []T) int {
	q.mu.Lock()
	k := min(len(dst), q.n)
	for i := range k {
		dst[i] = q.popLocked()
	}
	q.mu.Unlock()
	return k
}

func (q *chunked[T]) Len() int {
	q.mu.Lock()
	n := q.n
	q.mu.Unlock()
	return n
}

func (q *chunked[T]) Cap() int { return 0 }
