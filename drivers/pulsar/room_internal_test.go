package pulsar

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// partitionedProducer is a producer that says how many partitions it has, as
// pulsar-client-go v0.21's partitioned producer does. Everything else is the
// embedded nil interface, which a room never reaches.
type partitionedProducer struct {
	pulsar.Producer
	n atomic.Uint32
}

func (p *partitionedProducer) NumPartitions() uint32 { return p.n.Load() }

// stillOpen is a client that has not closed the producer.
func stillOpen(*pulsar.ProducerMessage) bool { return false }

// TestAFirstRefusalFindsAPlaceThatCameBack is a place coming back between the
// client's first refusal of a publication and that publication joining the
// room's line.
//
// The place found nobody in the line to wake and waited among the free places,
// and the refused publication, the only one in the line, waited out its pause
// before it tried again, and for ever if no other place came back. A first
// refusal that finds a free place now wakes the line itself.
func TestAFirstRefusalFindsAPlaceThatCameBack(t *testing.T) {
	r := newRoom(2, nil, stillOpen)
	ctx := context.Background()
	if err := r.take(ctx, nil, nil); err != nil { // in flight at the client
		t.Fatal(err)
	}
	if err := r.take(ctx, nil, nil); err != nil { // refused, the client being full
		t.Fatal(err)
	}
	r.give() // the one in flight is answered
	e := r.refused(nil)
	wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := r.retry(wctx, nil, e, time.Hour); err != nil {
		t.Fatalf("a refused publication was not woken for a place that came back just before it joined the line: "+
			"%v", err)
	}
	r.leave(e)
	r.give()
	if free := r.freePlaces(); free != 2 {
		t.Fatalf("the room ended with %d free places of 2", free)
	}
}

// TestRoomGrowsForWhoIsWaiting is partitions added to a producer while
// publications wait for a place in its room.
//
// The places the new partitions brought reached the waiting publications only
// when another publication came to take one: a place coming back handed over
// its own place alone. It now hands them over too.
func TestRoomGrowsForWhoIsWaiting(t *testing.T) {
	p := &partitionedProducer{}
	p.n.Store(1)
	r := newRoom(1, p, stillOpen)
	ctx := context.Background()
	if err := r.take(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	got := make(chan struct{}, 3)
	for range 3 {
		go func() {
			if r.take(ctx, nil, nil) == nil {
				got <- struct{}{}
			}
		}()
	}
	waitForRoom(t, "three publications to wait for a place", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.waiters) == 3
	})
	p.n.Store(4)
	r.give()
	for i := range 3 {
		select {
		case <-got:
		case <-time.After(2 * time.Second):
			t.Fatalf("%d of three waiting publications got a place once a place came back after three partitions "+
				"were added", i)
		}
	}
	// The first place went back above; the three waiting hold the rest.
	for range 3 {
		r.give()
	}
	if free := r.freePlaces(); free != 4 {
		t.Fatalf("the room ended with %d free places of 4", free)
	}
}

// TestRoomGrowsForTheLine is partitions added while the client refuses the
// publications in a room's line: the places they bring wake the line, which
// nothing else would until the first in it ends its pause.
func TestRoomGrowsForTheLine(t *testing.T) {
	p := &partitionedProducer{}
	p.n.Store(1)
	r := newRoom(1, p, stillOpen)
	ctx := context.Background()
	if err := r.take(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	e := r.refused(nil)
	p.n.Store(2)
	// A newcomer looks at the room and finds the new place, but waits
	// behind the line.
	newcomer := make(chan error, 1)
	go func() { newcomer <- r.take(ctx, nil, nil) }()
	wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := r.retry(wctx, nil, e, time.Hour); err != nil {
		t.Fatalf("the publication in the line was not woken for the place a new partition brought: %v", err)
	}
	r.leave(e)
	if err := <-newcomer; err != nil {
		t.Fatalf("the newcomer reported %v", err)
	}
	r.give()
	r.give()
	if free := r.freePlaces(); free != 2 {
		t.Fatalf("the room ended with %d free places of 2", free)
	}
}

// TestAWaitForAPlaceAsksWhetherTheClientClosedTheProducer is a publication
// waiting for a place that no publication will give back: the client has
// closed the producer for good and dropped the ones holding every place.
//
// The wait ended only with a place, its context or a Close, so it waited for
// ever. It now asks the client, with its own message, while no place comes
// back, keeps waiting while the producer is alive, and ends once the client
// says it has closed it.
func TestAWaitForAPlaceAsksWhetherTheClientClosedTheProducer(t *testing.T) {
	var (
		closed atomic.Bool
		asked  atomic.Int32
	)
	m := &pulsar.ProducerMessage{Payload: []byte("waiting")}
	r := newRoom(1, nil, func(got *pulsar.ProducerMessage) bool {
		if got != m {
			t.Errorf("the client was asked with %p, not the waiting publication's message %p", got, m)
		}
		asked.Add(1)
		return closed.Load()
	})
	ctx := context.Background()
	if err := r.take(ctx, nil, nil); err != nil { // dropped by the client
		t.Fatal(err)
	}
	ended := make(chan error, 1)
	go func() { ended <- r.take(ctx, nil, m) }()
	waitForRoom(t, "the waiting publication to ask twice", func() bool { return asked.Load() >= 2 })
	select {
	case err := <-ended:
		t.Fatalf("the wait ended with %v while the client said the producer was open", err)
	default:
	}
	closed.Store(true)
	select {
	case err := <-ended:
		if !errors.Is(err, errClosedForGood) {
			t.Fatalf("the wait on a producer the client had closed ended with %v, want errClosedForGood", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the wait went on after the client said it had closed the producer for good")
	}
	r.mu.Lock()
	waiters := len(r.waiters)
	r.mu.Unlock()
	if waiters != 0 {
		t.Fatalf("the room still counts %d waiting publications", waiters)
	}
	r.give()
	if free := r.freePlaces(); free != 1 {
		t.Fatalf("the room ended with %d free places of 1", free)
	}
}

// freePlaces is how many places the room has free.
func (r *room) freePlaces() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.free
}

// waitForRoom waits until cond holds, for at most three seconds.
func waitForRoom(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
