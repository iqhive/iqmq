package iqmq

import (
	"context"
	"sync"
)

// runFree is the runner's free list of admission slots: a fixed ring of
// exactly MaxInFlight items, guarded by a mutex, taken from by the fetch
// loop and refilled by whichever worker settles a delivery.
//
// The slots come back in the order they were returned, so a slot that has
// been idle longest is the next one fetched into, as it was when this was a
// buffered channel. Taking a whole batch is one lock rather than one channel
// receive per slot.
type runFree struct {
	mu    sync.Mutex
	ring  []*runItem
	head  int
	count int
	// avail wakes the fetch loop when a slot comes back to an empty list.
	// One token is enough because there is one taker, and a token left over
	// from a wake nobody needed costs that taker one empty look.
	avail chan struct{}
}

// newRunFree allocates n slots and puts every one of them on the list.
func newRunFree(n int) *runFree {
	f := &runFree{ring: make([]*runItem, n), count: n, avail: make(chan struct{}, 1)}
	for i := range f.ring {
		f.ring[i] = &runItem{}
	}
	return f
}

// put returns one slot to the list.
func (f *runFree) put(it *runItem) {
	f.mu.Lock()
	tail := f.head + f.count
	if tail >= len(f.ring) {
		tail -= len(f.ring)
	}
	f.ring[tail] = it
	f.count++
	wasEmpty := f.count == 1
	f.mu.Unlock()
	if wasEmpty {
		select {
		case f.avail <- struct{}{}:
		default:
		}
	}
}

// take fills dst with as many free slots as there are, up to len(dst),
// waiting for the first one until ctx is done.
func (f *runFree) take(ctx context.Context, dst []*runItem) (int, error) {
	for {
		if n := f.tryTake(dst); n > 0 {
			return n, nil
		}
		select {
		case <-f.avail:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// tryTake is take without the wait.
func (f *runFree) tryTake(dst []*runItem) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := min(f.count, len(dst))
	for i := range n {
		dst[i] = f.ring[f.head]
		f.ring[f.head] = nil
		f.head++
		if f.head == len(f.ring) {
			f.head = 0
		}
	}
	f.count -= n
	return n
}
