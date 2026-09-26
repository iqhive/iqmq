package driverkit_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// delivery builds a delivery whose body is n bytes.
func delivery(id string, n int) mq.DriverDelivery {
	return mq.DriverDelivery{
		Message: &mq.Message{ID: id, Body: make([]byte, n)},
		Info:    mq.DeliveryInfo{Topic: "orders"},
	}
}

// waitFor polls until cond holds or the deadline passes. It is how a test
// observes a goroutine that is parked: there is nothing to receive from, so the
// only evidence is the adapter's own counter.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestPushAdapterBackpressure is the reason this helper exists: a full ring must
// hold the broker off, not grow. The check is that a push into a full ring is
// still parked — Blocked() counts it — and that it completes only once a Fetch
// has made room.
//
// A push adapter that buffered without bound would pass every other test in
// this file and fail this one, which is the whole point of writing it.
func TestPushAdapterBackpressure(t *testing.T) {
	ctx := context.Background()
	p, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: 2})
	if err != nil {
		t.Fatalf("NewPushAdapter: %v", err)
	}
	for i := range 2 {
		if err := p.Push(ctx, delivery(string(rune('a'+i)), 8)); err != nil {
			t.Fatalf("Push %d: %v", i, err)
		}
	}
	if p.Len() != 2 {
		t.Fatalf("Len is %d, want 2", p.Len())
	}
	// The third push has nowhere to go. The broker's callback goroutine is
	// what is parked here.
	var pushed atomic.Bool
	go func() {
		if err := p.Push(ctx, delivery("c", 8)); err == nil {
			pushed.Store(true)
		}
	}()
	waitFor(t, "the third push to park", func() bool { return p.Blocked() == 1 })
	if pushed.Load() {
		t.Fatal("a push into a full ring returned: the adapter is buffering, not applying backpressure")
	}
	if n := p.Len(); n != 2 {
		t.Fatalf("the ring grew past its capacity: Len is %d", n)
	}
	// Taking one out releases exactly the room the parked push needs.
	dst := make([]mq.DriverDelivery, 1)
	n, err := p.Fetch(ctx, dst)
	if err != nil || n != 1 {
		t.Fatalf("Fetch: %d, %v", n, err)
	}
	if dst[0].Message.ID != "a" {
		t.Errorf("Fetch returned %q, want the oldest delivery", dst[0].Message.ID)
	}
	waitFor(t, "the parked push to complete", pushed.Load)
	waitFor(t, "the blocked count to fall back", func() bool { return p.Blocked() == 0 })
	if p.Len() != 2 {
		t.Fatalf("Len is %d after the parked push landed, want 2", p.Len())
	}
}

// TestPushAdapterByteBound is the second half of the bound: a ring with room for
// more messages still refuses one that would take it past its byte budget, and
// an oversized delivery is admitted into an empty ring rather than deadlocking
// on room that can never appear.
func TestPushAdapterByteBound(t *testing.T) {
	ctx := context.Background()
	p, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: 16, MaxBytes: 100})
	if err != nil {
		t.Fatalf("NewPushAdapter: %v", err)
	}
	if err := p.Push(ctx, delivery("a", 60)); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if got := p.Bytes(); got != 60 {
		t.Fatalf("Bytes is %d, want 60", got)
	}
	ok, err := p.TryPush(delivery("b", 60))
	if err != nil {
		t.Fatalf("TryPush: %v", err)
	}
	if ok {
		t.Fatal("the byte bound admitted a delivery that took the ring past MaxBytes")
	}
	var landed atomic.Bool
	go func() {
		if err := p.Push(ctx, delivery("b", 60)); err == nil {
			landed.Store(true)
		}
	}()
	waitFor(t, "the oversized push to park", func() bool { return p.Blocked() == 1 })
	dst := make([]mq.DriverDelivery, 4)
	if _, err := p.Fetch(ctx, dst); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	waitFor(t, "the parked push to complete", landed.Load)

	// A delivery larger than the whole budget still fits an empty ring: the
	// alternative is a deadlock caused by an operator's byte bound.
	q, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: 4, MaxBytes: 10})
	if err != nil {
		t.Fatalf("NewPushAdapter: %v", err)
	}
	if err := q.Push(ctx, delivery("big", 1000)); err != nil {
		t.Fatalf("an oversized delivery into an empty ring: %v", err)
	}
	if q.Len() != 1 {
		t.Fatalf("Len is %d", q.Len())
	}
}

// TestPushAdapterFetchWaitsAndDrains pins Fetch: one blocking wait and then
// a non-blocking drain of everything that is there.
func TestPushAdapterFetchWaitsAndDrains(t *testing.T) {
	ctx := context.Background()
	p, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: 8})
	if err != nil {
		t.Fatalf("NewPushAdapter: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var n int
	dst := make([]mq.DriverDelivery, 8)
	go func() {
		defer wg.Done()
		n, err = p.Fetch(ctx, dst)
	}()
	for i := range 3 {
		if perr := p.Push(ctx, delivery(string(rune('a'+i)), 4)); perr != nil {
			t.Errorf("Push: %v", perr)
		}
	}
	wg.Wait()
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if n < 1 || n > 3 {
		t.Fatalf("Fetch returned %d, want between 1 and 3", n)
	}
	// A fetch with nowhere to put anything returns immediately.
	if got, ferr := p.Fetch(ctx, nil); got != 0 || ferr != nil {
		t.Errorf("Fetch(nil): %d, %v", got, ferr)
	}
}

// TestPushAdapterCloseUnblocksBothSides is reaching a driver: Close wakes
// a parked Fetch and a parked Push with ErrClosed, and Drain hands back what the
// ring still held so that a closing source can requeue it.
func TestPushAdapterCloseUnblocksBothSides(t *testing.T) {
	ctx := context.Background()
	p, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: 1})
	if err != nil {
		t.Fatalf("NewPushAdapter: %v", err)
	}
	fetchErr := make(chan error, 1)
	go func() {
		_, ferr := p.Fetch(ctx, make([]mq.DriverDelivery, 1))
		fetchErr <- ferr
	}()
	// Give the fetch time to park, then close underneath it.
	waitFor(t, "the fetch to park", func() bool { return true })
	time.Sleep(10 * time.Millisecond)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-fetchErr:
		if !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("a parked Fetch woke with %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock a parked Fetch")
	}
	if err := p.Push(ctx, delivery("x", 1)); !errors.Is(err, mq.ErrClosed) {
		t.Errorf("Push after Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("a second Close: %v", err)
	}
	if !p.Closed() {
		t.Error("Closed reported false after Close")
	}
}

// TestPushAdapterCloseUnblocksParkedPush is the other side of the same rule: a
// broker callback parked for room must not be left there by a close.
func TestPushAdapterCloseUnblocksParkedPush(t *testing.T) {
	ctx := context.Background()
	p, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: 1})
	if err != nil {
		t.Fatalf("NewPushAdapter: %v", err)
	}
	if err := p.Push(ctx, delivery("a", 1)); err != nil {
		t.Fatalf("Push: %v", err)
	}
	pushErr := make(chan error, 1)
	go func() { pushErr <- p.Push(ctx, delivery("b", 1)) }()
	waitFor(t, "the push to park", func() bool { return p.Blocked() == 1 })
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-pushErr:
		if !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("a parked Push woke with %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock a parked Push")
	}
	// Drain is what the closing source requeues with.
	var drained []string
	if n := p.Drain(func(d *mq.DriverDelivery) { drained = append(drained, d.Message.ID) }); n != 1 {
		t.Fatalf("Drain returned %d, want 1", n)
	}
	if len(drained) != 1 || drained[0] != "a" {
		t.Fatalf("Drain handed back %q", drained)
	}
	if p.Len() != 0 {
		t.Errorf("Len is %d after Drain", p.Len())
	}
}

// TestPushAdapterContextCancellation pins that a parked push answers its
// callback's context: a driver that is being closed must not leave a broker
// goroutine parked here for ever.
func TestPushAdapterContextCancellation(t *testing.T) {
	p, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: 1})
	if err != nil {
		t.Fatalf("NewPushAdapter: %v", err)
	}
	if err := p.Push(context.Background(), delivery("a", 1)); err != nil {
		t.Fatalf("Push: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	pushErr := make(chan error, 1)
	go func() { pushErr <- p.Push(ctx, delivery("b", 1)) }()
	waitFor(t, "the push to park", func() bool { return p.Blocked() == 1 })
	cancel()
	select {
	case err := <-pushErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("a cancelled Push returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelling the context did not release the parked Push")
	}
	waitFor(t, "the blocked count to fall back", func() bool { return p.Blocked() == 0 })

	fctx, fcancel := context.WithCancel(context.Background())
	q, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: 1})
	if err != nil {
		t.Fatalf("NewPushAdapter: %v", err)
	}
	fetchErr := make(chan error, 1)
	go func() {
		_, ferr := q.Fetch(fctx, make([]mq.DriverDelivery, 1))
		fetchErr <- ferr
	}()
	time.Sleep(10 * time.Millisecond)
	fcancel()
	select {
	case err := <-fetchErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("a cancelled Fetch returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelling the context did not release the parked Fetch")
	}
}

// TestPushAdapterRefusals pins the construction rules. An unbounded push adapter
// is the thing this type exists to prevent, so there is no way to ask for one.
func TestPushAdapterRefusals(t *testing.T) {
	for _, cfg := range []driverkit.PushConfig{
		{Capacity: 0},
		{Capacity: -1},
		{Capacity: 4, MaxBytes: -1},
	} {
		if _, err := driverkit.NewPushAdapter(cfg); mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Errorf("NewPushAdapter(%+v): %v", cfg, err)
		}
	}
}

// TestPushAdapterConcurrent races many pushers against one fetcher, which is the
// shape every push driver has, and checks that nothing is lost or duplicated and
// that the ring never exceeds its bound.
func TestPushAdapterConcurrent(t *testing.T) {
	ctx := context.Background()
	const producers, each, capacity = 8, 500, 16
	p, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: capacity})
	if err != nil {
		t.Fatalf("NewPushAdapter: %v", err)
	}
	var wg sync.WaitGroup
	for g := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				d := delivery("x", 1)
				d.Token = uint64(g*each + i)
				if err := p.Push(ctx, d); err != nil {
					t.Errorf("Push: %v", err)
					return
				}
			}
		}()
	}
	seen := make([]bool, producers*each)
	got := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		dst := make([]mq.DriverDelivery, 8)
		for got < producers*each {
			n, ferr := p.Fetch(ctx, dst)
			if ferr != nil {
				t.Errorf("Fetch: %v", ferr)
				return
			}
			if over := p.Len(); over > capacity {
				t.Errorf("the ring held %d, past its capacity of %d", over, capacity)
				return
			}
			for i := range n {
				tok := dst[i].Token
				if seen[tok] {
					t.Errorf("delivery %d was fetched twice", tok)
					return
				}
				seen[tok] = true
				got++
			}
		}
	}()
	wg.Wait()
	<-done
	if got != producers*each {
		t.Fatalf("fetched %d of %d", got, producers*each)
	}
}

// TestPushAdapterRefusesAnAdmissionAfterCancellation is the half of the
// cancellation contract that a parked push cannot show, and the half a driver
// depends on: a cancelled context must refuse an admission that has ROOM.
//
// It is what makes "cancel, then drain" an ordering a driver can rely on.
// `drivers/rabbitmq`'s `source.claimLost` cancels its pump's context and then
// drains the ring, so that a delivery from the channel that just died cannot
// be handed to a caller who could never settle it. With the context consulted
// only in the wait for room, both halves of that leaked: a push into a ring
// with room never looked at the context at all, and a parked push woken by
// `Drain`'s own `notFull` broadcast could take that branch of a `select` whose
// context branch was equally ready and insert straight into the ring the drain
// had just emptied.
func TestPushAdapterRefusesAnAdmissionAfterCancellation(t *testing.T) {
	p, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: 4})
	if err != nil {
		t.Fatalf("NewPushAdapter: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// An empty ring: there is room, and the context is what must refuse it.
	if err := p.Push(ctx, delivery("a", 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("a Push with room and a cancelled context returned %v, want context.Canceled", err)
	}
	if n := p.Len(); n != 0 {
		t.Fatalf("the ring holds %d deliveries after a refused Push: a delivery admitted after its context was cancelled is one the driver has already decided nobody may be given", n)
	}

	// And the parked case, released by the drain rather than by the cancel:
	// the woken push must still refuse.
	if err := p.Push(context.Background(), delivery("b", 1)); err != nil {
		t.Fatalf("filling the ring: %v", err)
	}
	q, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: 1})
	if err != nil {
		t.Fatalf("NewPushAdapter: %v", err)
	}
	if err := q.Push(context.Background(), delivery("c", 1)); err != nil {
		t.Fatalf("filling the ring: %v", err)
	}
	pctx, pcancel := context.WithCancel(context.Background())
	pushErr := make(chan error, 1)
	go func() { pushErr <- q.Push(pctx, delivery("d", 1)) }()
	waitFor(t, "the push to park", func() bool { return q.Blocked() == 1 })
	pcancel()
	q.Drain(nil)
	select {
	case err := <-pushErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("a push parked on a cancelled context returned %v after a Drain made room, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the parked Push never returned")
	}
	if n := q.Len(); n != 0 {
		t.Fatalf("the ring holds %d deliveries after the drain: the parked push inserted behind the drain that woke it", n)
	}
}
