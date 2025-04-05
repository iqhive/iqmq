package queue

import "runtime"

// maxShards caps the shard count; more shards than cores only add scanning.
const maxShards = 8

// sharded spreads pushes round-robin over independent SeqMPMC shards and
// lets pops scan from a rotating start. It trades FIFO order for lower
// cursor contention.
type sharded[T any] struct {
	shards []*seqMPMC[T]
	mask   uint64
	pushRR loadedCursor
	popRR  loadedCursor
}

func newSharded[T any](capacity int) *sharded[T] {
	n := pow2ceil(min(runtime.GOMAXPROCS(0), maxShards))
	per := (capacity + n - 1) / n
	q := &sharded[T]{shards: make([]*seqMPMC[T], n), mask: uint64(n - 1)}
	for i := range q.shards {
		q.shards[i] = newSeqMPMC[T](per)
	}
	return q
}

func (q *sharded[T]) TryPush(v T) bool {
	start := q.pushRR.v.Add(1)
	for i := range q.shards {
		if q.shards[(start+uint64(i))&q.mask].TryPush(v) {
			return true
		}
	}
	return false
}

func (q *sharded[T]) TryPop() (T, bool) {
	start := q.popRR.v.Add(1)
	for i := range q.shards {
		if v, ok := q.shards[(start+uint64(i))&q.mask].TryPop(); ok {
			return v, true
		}
	}
	var zero T
	return zero, false
}

func (q *sharded[T]) PushBatch(vs []T) int {
	for i, v := range vs {
		if !q.TryPush(v) {
			return i
		}
	}
	return len(vs)
}

func (q *sharded[T]) PopBatch(dst []T) int {
	for i := range dst {
		v, ok := q.TryPop()
		if !ok {
			return i
		}
		dst[i] = v
	}
	return len(dst)
}

func (q *sharded[T]) Len() int {
	n := 0
	for _, s := range q.shards {
		n += s.Len()
	}
	return n
}

func (q *sharded[T]) Cap() int { return len(q.shards) * q.shards[0].Cap() }
