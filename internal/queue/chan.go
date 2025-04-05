package queue

// chanQueue wraps a buffered channel; the runtime's channel lock is the
// synchronisation.
type chanQueue[T any] struct {
	ch chan T
}

func newChanQueue[T any](capacity int) *chanQueue[T] {
	return &chanQueue[T]{ch: make(chan T, capacity)}
}

func (q *chanQueue[T]) TryPush(v T) bool {
	select {
	case q.ch <- v:
		return true
	default:
		return false
	}
}

func (q *chanQueue[T]) TryPop() (T, bool) {
	select {
	case v := <-q.ch:
		return v, true
	default:
		var zero T
		return zero, false
	}
}

func (q *chanQueue[T]) PushBatch(vs []T) int {
	for i, v := range vs {
		if !q.TryPush(v) {
			return i
		}
	}
	return len(vs)
}

func (q *chanQueue[T]) PopBatch(dst []T) int {
	for i := range dst {
		v, ok := q.TryPop()
		if !ok {
			return i
		}
		dst[i] = v
	}
	return len(dst)
}

func (q *chanQueue[T]) Len() int { return len(q.ch) }

func (q *chanQueue[T]) Cap() int { return cap(q.ch) }
