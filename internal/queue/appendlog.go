package queue

import (
	"sync"
	"sync/atomic"
)

// appendLog serialises writers with a mutex (one appender at a time, no CAS
// storm between producers) and lets readers advance a lock-free cursor over
// the committed prefix. Each slot carries the position it holds so a reader
// knows the entry is committed and a writer knows the slot was consumed.
type appendLog[T any] struct {
	mu    sync.Mutex
	tail  uint64 // next position to write; guarded by mu
	buf   []T
	state []atomic.Int64 // committed position+1 (>0) or -(consumed position+1) (<0); 0 initially
	_     pad
	head  atomic.Uint64 // next position to read; CAS by readers
	_     pad
}

func newAppendLog[T any](capacity int) *appendLog[T] {
	return &appendLog[T]{buf: make([]T, capacity), state: make([]atomic.Int64, capacity)}
}

func (q *appendLog[T]) index(pos uint64) int { return int(pos % uint64(len(q.buf))) }

// free reports whether the slot for pos may be written: it must never have
// been used, or its previous occupant (pos-cap) must have been consumed.
func (q *appendLog[T]) free(pos uint64) bool {
	st := q.state[q.index(pos)].Load()
	if pos < uint64(len(q.buf)) {
		return st == 0
	}
	return st == -int64(pos-uint64(len(q.buf))+1)
}

func (q *appendLog[T]) TryPush(v T) bool {
	q.mu.Lock()
	if !q.free(q.tail) {
		q.mu.Unlock()
		return false
	}
	i := q.index(q.tail)
	q.buf[i] = v
	q.state[i].Store(int64(q.tail + 1))
	q.tail++
	q.mu.Unlock()
	return true
}

func (q *appendLog[T]) TryPop() (T, bool) {
	for {
		h := q.head.Load()
		i := q.index(h)
		if q.state[i].Load() != int64(h+1) {
			var zero T
			return zero, false
		}
		if q.head.CompareAndSwap(h, h+1) {
			v := q.buf[i]
			var zero T
			q.buf[i] = zero
			q.state[i].Store(-int64(h + 1))
			return v, true
		}
	}
}

func (q *appendLog[T]) PushBatch(vs []T) int {
	q.mu.Lock()
	n := 0
	for n < len(vs) && q.free(q.tail) {
		i := q.index(q.tail)
		q.buf[i] = vs[n]
		q.state[i].Store(int64(q.tail + 1))
		q.tail++
		n++
	}
	q.mu.Unlock()
	return n
}

func (q *appendLog[T]) PopBatch(dst []T) int {
	for i := range dst {
		v, ok := q.TryPop()
		if !ok {
			return i
		}
		dst[i] = v
	}
	return len(dst)
}

func (q *appendLog[T]) Len() int {
	q.mu.Lock()
	t := q.tail
	q.mu.Unlock()
	return clampLen(t, q.head.Load(), len(q.buf))
}

func (q *appendLog[T]) Cap() int { return len(q.buf) }
