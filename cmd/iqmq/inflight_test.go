package main

import (
	"sync"
	"testing"
	"time"
)

// TestInFlightBoundsTheSettlements holds the limiter sub --concurrency runs
// on to its bound in both directions: it lets exactly limit settlements run at
// once, and the next one waits until one of them leaves.
func TestInFlightBoundsTheSettlements(t *testing.T) {
	const limit = 3
	f := newInFlight(limit)
	for range limit {
		f.enter()
	}
	entered := make(chan struct{})
	go func() {
		f.enter()
		close(entered)
	}()
	select {
	case <-entered:
		t.Fatalf("a settlement beyond the limit of %d was let in", limit)
	case <-time.After(20 * time.Millisecond):
	}
	f.leave()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("a leave did not let the waiting settlement in")
	}

	var mu sync.Mutex
	running, peak := 0, 0
	var wg sync.WaitGroup
	for range limit {
		f.leave()
	}
	for range 64 {
		f.enter()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer f.leave()
			mu.Lock()
			running++
			peak = max(peak, running)
			mu.Unlock()
			time.Sleep(time.Millisecond)
			mu.Lock()
			running--
			mu.Unlock()
		}()
	}
	wg.Wait()
	if peak > limit {
		t.Fatalf("%d settlements ran at once, want at most %d", peak, limit)
	}
}
