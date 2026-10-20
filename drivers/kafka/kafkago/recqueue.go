package kafkago

import (
	"context"
	"sync"
)

// recQueue carries records from the per-partition fetch loops to Fetch: a
// bounded ring under a mutex, oldest first.
//
// It is bounded so that a fetch loop whose records nobody is consuming
// waits in push, which is how a partition is paused: kafka-go has no pause
// to call. The two one-token channels are only the wake-ups that let either
// side wait alongside a context; the records themselves never travel
// through a channel.
type recQueue struct {
	mu    sync.Mutex
	ring  []fetched
	head  int
	count int
	// ready is signalled when the ring goes from empty to holding a record,
	// and room when it goes from full to having a free slot. A waiter that
	// wakes and still finds work for the next waiter passes the token on,
	// so one token serves any number of waiters in turn.
	ready chan struct{}
	room  chan struct{}
}

func newRecQueue(capacity int) *recQueue {
	return &recQueue{
		ring:  make([]fetched, capacity),
		ready: make(chan struct{}, 1),
		room:  make(chan struct{}, 1),
	}
}

// push appends f, waiting while the ring is full. It reports false, with f
// not queued, when ctx ends first.
func (q *recQueue) push(ctx context.Context, f fetched) bool {
	parked := false
	for {
		if q.tryPush(f, parked) {
			return true
		}
		parked = true
		select {
		case <-q.room:
		case <-ctx.Done():
			return false
		}
	}
}

// tryPush is push without the wait.
func (q *recQueue) tryPush(f fetched, parked bool) bool {
	q.mu.Lock()
	if q.count == len(q.ring) {
		q.mu.Unlock()
		return false
	}
	tail := q.head + q.count
	if tail >= len(q.ring) {
		tail -= len(q.ring)
	}
	q.ring[tail] = f
	q.count++
	wasEmpty := q.count == 1
	more := q.count < len(q.ring)
	q.mu.Unlock()
	if wasEmpty {
		signal(q.ready)
	}
	if parked && more {
		signal(q.room)
	}
	return true
}

// tryPop takes the oldest record, or reports that there is none. woken says
// the caller waited on ready to get here, and such a caller passes the token
// on when it leaves a record behind, so a second waiter is not stranded by
// the token it consumed. A caller that never waited sends nothing.
func (q *recQueue) tryPop(woken bool) (fetched, bool) {
	q.mu.Lock()
	if q.count == 0 {
		q.mu.Unlock()
		return fetched{}, false
	}
	f := q.ring[q.head]
	q.ring[q.head] = fetched{}
	q.head++
	if q.head == len(q.ring) {
		q.head = 0
	}
	wasFull := q.count == len(q.ring)
	q.count--
	more := q.count > 0
	q.mu.Unlock()
	if wasFull {
		signal(q.room)
	}
	if woken && more {
		signal(q.ready)
	}
	return f, true
}

// signal leaves a wake-up token unless one is already waiting.
func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
