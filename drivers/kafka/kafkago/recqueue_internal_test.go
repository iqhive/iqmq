package kafkago

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestRecQueueIsABoundedFIFO holds the record queue to what the buffered
// channel it replaced gave the source: records come out in the order they
// went in, a push to a full queue waits until a pop makes room, and a waiting
// push gives up when its context ends.
func TestRecQueueIsABoundedFIFO(t *testing.T) {
	q := newRecQueue(2)
	ctx := context.Background()
	for i := range uint64(2) {
		if !q.push(ctx, fetched{gen: i}) {
			t.Fatalf("push %d to a queue with room refused", i)
		}
	}

	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if q.push(short, fetched{gen: 99}) {
		t.Fatal("a push to a full queue did not wait")
	}
	if !errors.Is(short.Err(), context.DeadlineExceeded) {
		t.Fatal("the push to a full queue returned before its context ended")
	}

	pushed := make(chan bool, 1)
	go func() { pushed <- q.push(ctx, fetched{gen: 2}) }()
	select {
	case <-pushed:
		t.Fatal("a push to a full queue went through without a pop")
	case <-time.After(20 * time.Millisecond):
	}
	for want := range uint64(3) {
		f, ok := popWithin(t, q)
		if !ok || f.gen != want {
			t.Fatalf("pop %d = (%d, %v), want (%d, true)", want, f.gen, ok, want)
		}
		if want == 0 && !<-pushed {
			t.Fatal("the parked push was refused after a pop made room")
		}
	}
	if _, ok := q.tryPop(false); ok {
		t.Fatal("an empty queue handed out a record")
	}
}

// TestRecQueueWakesEveryParkedPush is the case a single wake-up token has to
// get right: several fetch loops parked on a full queue are all let through as
// the consumer drains it, none stranded.
func TestRecQueueWakesEveryParkedPush(t *testing.T) {
	const producers = 8
	q := newRecQueue(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if !q.push(ctx, fetched{gen: 0}) {
		t.Fatal("the first push refused")
	}
	var wg sync.WaitGroup
	for i := range uint64(producers) {
		wg.Go(func() {
			if !q.push(ctx, fetched{gen: i + 1}) {
				t.Errorf("parked push %d gave up", i+1)
			}
		})
	}
	seen := 0
	for seen < producers+1 {
		if _, ok := popWithin(t, q); ok {
			seen++
		}
	}
	wg.Wait()
}

// popWithin pops one record, waiting on ready the way poll does.
func popWithin(t *testing.T, q *recQueue) (fetched, bool) {
	t.Helper()
	f, ok := q.tryPop(false)
	deadline := time.After(10 * time.Second)
	for !ok {
		select {
		case <-q.ready:
		case <-deadline:
			t.Fatal("no record arrived within 10s")
		}
		f, ok = q.tryPop(true)
	}
	return f, ok
}
