package main

import "sync"

// inFlight bounds how many settlements sub has running at
// once. It is a counter under a mutex rather than a buffered channel of
// tokens: the count is shared state, and the condition variable is what
// parks the loop that starts the settlements until one of them ends.
type inFlight struct {
	mu    sync.Mutex
	freed sync.Cond
	n     int
	limit int
}

func newInFlight(limit int) *inFlight {
	f := &inFlight{limit: limit}
	f.freed.L = &f.mu
	return f
}

// enter waits until fewer than limit settlements are running and counts
// one more.
func (f *inFlight) enter() {
	f.mu.Lock()
	for f.n >= f.limit {
		f.freed.Wait()
	}
	f.n++
	f.mu.Unlock()
}

// leave counts one settlement as ended and wakes the loop if it is waiting.
func (f *inFlight) leave() {
	f.mu.Lock()
	f.n--
	f.mu.Unlock()
	f.freed.Signal()
}
