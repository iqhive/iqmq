package engine

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/timer"
)

// TestSchedulerNeverEarly: a delayed record is not delivered one instant
// before its due time, and is delivered as soon as it comes due — without
// anybody asking. The engine scheduler is what moves it, so the test never
// fetches until after the record must already be ready.
func TestSchedulerNeverEarly(t *testing.T) {
	for _, a := range Arches() {
		t.Run(a.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := newTestEngine(t, KindSet{Arch: a}, TopologyConfig{Partitions: 1})
				top, sub, c := queueSub(t, e)
				const delay = 50 * time.Millisecond
				if err := top.Publish(context.Background(), Publish[int64]{
					Payload: 1, Size: 1, Delay: delay,
				}); err != nil {
					t.Fatal(err)
				}
				dst := make([]Claim[int64], 1)
				elapsed := time.Duration(0)
				for _, at := range []time.Duration{0, delay / 2, delay - time.Millisecond} {
					time.Sleep(at - elapsed)
					elapsed = at
					synctest.Wait()
					if n, _ := c.Fetch(poll(t), dst); n != 0 {
						t.Fatalf("delivered %v before the due time", at)
					}
					if s := sub.Inspect(); s.Delayed != 1 || s.Ready != 0 {
						t.Fatalf("at %v: %+v", at, s)
					}
				}
				// One tick past the due time the scheduler has moved it into
				// the ready set on its own.
				time.Sleep(2 * time.Millisecond)
				synctest.Wait()
				if s := sub.Inspect(); s.Ready != 1 || s.Delayed != 0 {
					t.Fatalf("the scheduler did not promote the record: %+v", s)
				}
				if n, _ := c.Fetch(poll(t), dst); n != 1 || dst[0].Payload != 1 {
					t.Fatalf("fetch after the due time: n=%d", n)
				}
			})
		})
	}
}

// TestSchedulerWakesParkedConsumer: a consumer parked on an empty
// subscription is woken by the scheduler when a delayed record comes due,
// with no fetch in between. Under synctest a wake-up that never comes is a
// bubble deadlock, so this also proves the notification is not lost.
func TestSchedulerWakesParkedConsumer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
		top, _, c := queueSub(t, e)
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: 7, Size: 1, Delay: 250 * time.Millisecond,
		}); err != nil {
			t.Fatal(err)
		}
		done := make(chan int64, 1)
		go func() {
			dst := make([]Claim[int64], 1)
			n, err := c.Fetch(context.Background(), dst)
			if err != nil || n != 1 {
				done <- -1
				return
			}
			done <- dst[0].Payload
		}()
		synctest.Wait()
		select {
		case v := <-done:
			t.Fatalf("the fetch returned %d before the record was due", v)
		default:
		}
		time.Sleep(300 * time.Millisecond)
		if v := <-done; v != 7 {
			t.Fatalf("the parked consumer received %d", v)
		}
	})
}

// TestSchedulerExpiresLease: the lease deadline of a delivery is reclaimed by
// the scheduler at the deadline, without a fault call, and the record is
// redelivered with its attempt count raised.
func TestSchedulerExpiresLease(t *testing.T) {
	for _, a := range Arches() {
		t.Run(a.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := newTestEngine(t, KindSet{Arch: a}, TopologyConfig{
					Partitions: 1, AckDeadline: 40 * time.Millisecond,
				})
				top, sub, c := queueSub(t, e)
				if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
					t.Fatal(err)
				}
				dst := make([]Claim[int64], 1)
				if n, _ := c.Fetch(poll(t), dst); n != 1 {
					t.Fatal("nothing fetched")
				}
				time.Sleep(20 * time.Millisecond)
				synctest.Wait()
				if s := sub.Inspect(); s.InFlight != 1 || s.Expired != 0 {
					t.Fatalf("a live lease was reclaimed: %+v", s)
				}
				time.Sleep(30 * time.Millisecond)
				synctest.Wait()
				s := sub.Inspect()
				if s.InFlight != 0 || s.Ready != 1 || s.Expired != 1 || s.Requeued != 1 {
					t.Fatalf("the deadline did not reclaim the lease: %+v", s)
				}
				if err := c.Ack(dst[0].Env, dst[0].Gen); !errors.Is(err, errcode.ErrLeaseLost) {
					t.Fatalf("ack after expiry: %v", err)
				}
				if n, _ := c.Fetch(poll(t), dst); n != 1 || dst[0].Info.Attempts != 2 {
					t.Fatalf("redelivery: n=%d attempts=%d", n, dst[0].Info.Attempts)
				}
			})
		})
	}
}

// TestSchedulerExtendMovesTheDeadline: an extension moves the lane's
// scheduler entry with it, so the lease survives the deadline it had and
// expires at the one it was given.
func TestSchedulerExtendMovesTheDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{
			Partitions: 1, AckDeadline: 30 * time.Millisecond,
		})
		top, sub, c := queueSub(t, e)
		if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
			t.Fatal(err)
		}
		dst := make([]Claim[int64], 1)
		if n, _ := c.Fetch(poll(t), dst); n != 1 {
			t.Fatal("nothing fetched")
		}
		if err := c.Extend(dst[0].Env, dst[0].Gen, 100*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		// Past the original deadline, still in flight.
		time.Sleep(60 * time.Millisecond)
		synctest.Wait()
		if s := sub.Inspect(); s.InFlight != 1 || s.Expired != 0 {
			t.Fatalf("the extended lease expired at its old deadline: %+v", s)
		}
		// Past the extended one, reclaimed.
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()
		if s := sub.Inspect(); s.InFlight != 0 || s.Expired != 1 {
			t.Fatalf("the extended deadline did not reclaim the lease: %+v", s)
		}
		// Shortening a lease brings the deadline forward again.
		if n, _ := c.Fetch(poll(t), dst); n != 1 {
			t.Fatal("nothing redelivered")
		}
		if err := c.Extend(dst[0].Env, dst[0].Gen, 5*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		if s := sub.Inspect(); s.InFlight != 0 || s.Expired != 2 {
			t.Fatalf("the shortened deadline did not reclaim the lease: %+v", s)
		}
	})
}

// TestSchedulerBubbleExit: the scheduler goroutine is durably blocked while
// it has nothing to do and exits when the engine closes, so a bubble that
// built an engine can end. A goroutine that ticked, spun or outlived Close
// would deadlock the bubble instead of failing here.
func TestSchedulerBubbleExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, err := New(Config[int64]{
			Clone:           func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{Partitions: 2, AckDeadline: time.Second},
		})
		if err != nil {
			t.Fatal(err)
		}
		top, err := e.CreateTopic("t", TopologyConfig{})
		if err != nil {
			t.Fatal(err)
		}
		sub, err := top.Subscribe("s", SubscriptionConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sub.Attach(ConsumerConfig{}); err != nil {
			t.Fatal(err)
		}
		for i := range 8 {
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: int64(i), Size: 1, Partition: i % 2,
				Delay: time.Duration(i+1) * time.Second,
			}); err != nil {
				t.Fatal(err)
			}
		}
		// An idle engine parks: the bubble reaches a durably blocked state
		// with records still scheduled.
		synctest.Wait()
		if s := sub.Inspect(); s.Delayed != 8 {
			t.Fatalf("%+v", s)
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		// Close joins the goroutine, so nothing of the engine is left in the
		// bubble; a second Close is a no-op.
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

// TestWaitIdleIsSchedulerDriven: WaitIdle returns as soon as the last record
// leaves, and the delayed mode waits for the scheduler to release the ones
// that are still holding their due time.
func TestWaitIdleIsSchedulerDriven(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
		top, _, c := queueSub(t, e)
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: 1, Size: 1, Delay: 100 * time.Millisecond,
		}); err != nil {
			t.Fatal(err)
		}
		// Nothing is deliverable, so the delivered mode is already idle.
		if err := e.WaitIdle(context.Background(), IdleDelivered); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- e.WaitIdle(context.Background(), IdleIncludingDelayed) }()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("WaitIdle returned %v while a record was still delayed", err)
		default:
		}
		// Let the record come due, then take it and settle it: the last
		// record to leave wakes the waiter.
		time.Sleep(150 * time.Millisecond)
		synctest.Wait()
		dst := make([]Claim[int64], 1)
		if n, _ := c.Fetch(poll(t), dst); n != 1 {
			t.Fatal("the delayed record never came due")
		}
		if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatalf("WaitIdle: %v", err)
		}
	})
}

// TestRecordTimeToLive: a record that outlives its time to live is dropped by
// the scheduler and never delivered, whatever the architecture holding it.
func TestRecordTimeToLive(t *testing.T) {
	for _, a := range Arches() {
		t.Run(a.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := newTestEngine(t, KindSet{Arch: a}, TopologyConfig{Partitions: 1})
				top, sub, c := queueSub(t, e)
				// One record that expires, one that does not.
				if err := top.Publish(context.Background(), Publish[int64]{
					Payload: 1, Size: 1, TTL: 40 * time.Millisecond,
				}); err != nil {
					t.Fatal(err)
				}
				if err := top.Publish(context.Background(), Publish[int64]{Payload: 2, Size: 1}); err != nil {
					t.Fatal(err)
				}
				time.Sleep(20 * time.Millisecond)
				synctest.Wait()
				if s := sub.Inspect(); s.Ready != 2 || s.Dropped != 0 {
					t.Fatalf("a live record was discarded: %+v", s)
				}
				time.Sleep(30 * time.Millisecond)
				synctest.Wait()
				s := sub.Inspect()
				if s.Ready != 1 || s.Dropped != 1 || !s.Conserved() {
					t.Fatalf("the time to live was not enforced: %+v", s)
				}
				dst := make([]Claim[int64], 2)
				n, _ := c.Fetch(poll(t), dst)
				if n != 1 || dst[0].Payload != 2 {
					t.Fatalf("an expired record was delivered: n=%d payload=%d", n, dst[0].Payload)
				}
			})
		})
	}
}

// TestTimeToLiveOnDelayedRecord: the time to live of a delayed publication
// runs from its delivery time, so a record with a delay longer than its time
// to live is still delivered, and one that is not taken in time is dropped
// where it waits.
func TestTimeToLiveOnDelayedRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
		top, sub, c := queueSub(t, e)
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: 1, Size: 1, Delay: 100 * time.Millisecond, TTL: 20 * time.Millisecond,
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(105 * time.Millisecond)
		synctest.Wait()
		dst := make([]Claim[int64], 1)
		if n, _ := c.Fetch(poll(t), dst); n != 1 {
			t.Fatal("the delayed record was discarded before it was due")
		}
		if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
			t.Fatal(err)
		}
		// A second one nobody takes is dropped once its time to live passes.
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: 2, Size: 1, Delay: 10 * time.Millisecond, TTL: 20 * time.Millisecond,
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(40 * time.Millisecond)
		synctest.Wait()
		if s := sub.Inspect(); s.Ready != 0 || s.Dropped != 1 {
			t.Fatalf("%+v", s)
		}
		if n := e.Faults().ExpireRecords(); n != 0 {
			t.Fatalf("the scheduler left %d expired records behind", n)
		}
	})
}

// TestTimeToLiveOnRequeue: a record whose time to live ran out while it was
// in flight is released rather than redelivered.
func TestTimeToLiveOnRequeue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, AckDeadline: -1})
		top, sub, c := queueSub(t, e)
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: 1, Size: 1, TTL: 20 * time.Millisecond,
		}); err != nil {
			t.Fatal(err)
		}
		dst := make([]Claim[int64], 1)
		if n, _ := c.Fetch(poll(t), dst); n != 1 {
			t.Fatal("nothing fetched")
		}
		time.Sleep(40 * time.Millisecond)
		synctest.Wait()
		// In flight, the lease governs the record: it is still there.
		if s := sub.Inspect(); s.InFlight != 1 {
			t.Fatalf("an in-flight record was discarded: %+v", s)
		}
		if err := c.Nack(dst[0].Env, dst[0].Gen, 0); err != nil {
			t.Fatal(err)
		}
		s := sub.Inspect()
		if s.Ready != 0 || s.Dropped != 1 || !s.Conserved() {
			t.Fatalf("an expired record was requeued: %+v", s)
		}
	})
}

// TestNoteTimeToLiveDuringSweep: an expiry the lane holds covers every later
// one, including once it has passed. A value that has passed is one the sweep
// has not taken yet, and lane.expireTTL takes the value it acts on before it
// scans the store, while a publication notes its expiry only once its record
// is in the store (lane.admit). So the sweep that takes the passed value scans
// after the record arrived and finds it, and a sweep that took the value
// earlier has left the lane with nothing for the publication to read as
// covering it.
//
// A passed value used to be replaced by the later expiry instead. The
// scheduler then woke for the passed one, found the lane's expiry ahead and
// swept nothing, and under steady publishing the next publication did the
// same to the new value: the sweep never ran
// (TestTimeToLiveSweepRunsUnderSteadyPublishing).
func TestNoteTimeToLiveDuringSweep(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
	_, sub, _ := queueSub(t, e)
	ln := sub.lanes[0]
	now := e.now()
	mine := now + int64(time.Minute)

	ln.ttlAt.Store(now - 1) // the value a sweep is due to act on
	ln.noteTTL(mine)
	if got := ln.ttlAt.Load(); got != now-1 {
		t.Fatalf("a later expiry (%d) replaced the one the sweep is due for (%d): the lane's expiry is now %d",
			mine, now-1, got)
	}
	// A sweep that finds nothing leaves the lane with nothing to wake for.
	if n := ln.expireTTL(now); n != 0 {
		t.Fatalf("the sweep dropped %d records from an empty lane", n)
	}
	if got := ln.ttlAt.Load(); got != 0 {
		t.Fatalf("the lane is still armed for %d after an empty sweep", got)
	}
	ln.noteTTL(mine)
	if got := ln.ttlAt.Load(); got != mine {
		t.Fatalf("a lane with no expiry noted %d, want %d", got, mine)
	}
	// A live expiry that is already earlier covers a later one, which is what
	// keeps the publish path off the atomic in the common case.
	ln.noteTTL(now + int64(2*time.Minute))
	if got := ln.ttlAt.Load(); got != mine {
		t.Fatalf("the lane moved its expiry later, to %d", got)
	}
	// An earlier one takes its place.
	sooner := now + int64(time.Second)
	ln.noteTTL(sooner)
	if got := ln.ttlAt.Load(); got != sooner {
		t.Fatalf("the lane kept %d over the earlier %d", got, sooner)
	}
	// A record with no time to live notes nothing.
	before := ln.ttlAt.Load()
	ln.noteTTL(0)
	if got := ln.ttlAt.Load(); got != before {
		t.Fatalf("a record without a time to live changed the lane expiry to %d", got)
	}
}

// TestTimeToLiveSweepRunsUnderSteadyPublishing pins the sweep a steady
// publisher used to put off for ever.
//
// A publication that arrived after the lane's noted expiry had passed, but
// before the scheduler had swept for it, replaced the note with its own later
// expiry. The scheduler then woke for the old one, found the lane's expiry
// still ahead and swept nothing, and the next publication did the same to the
// new note. The expired records stayed counted in the lane until it was full
// and refused every publication with ErrQueueFull (or, under OverflowBlock,
// parked the publisher).
//
// The window is the time between an expiry and the sweep for it. A wheel
// timer kind, the default among them, rounds a due time up to its 1ms tick,
// so a publication every millisecond, 0.8ms into the tick, lands in that
// window every time: a record published at k+0.8ms expires at k+2.3ms, is
// swept at k+3ms, and the publication at k+2.8ms falls between the two.
func TestTimeToLiveSweepRunsUnderSteadyPublishing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			ttl       = 1500 * time.Microsecond
			capacity  = 4
			publishes = 20
		)
		e := newTestEngine(t, KindSet{Timer: timer.HashedWheel},
			TopologyConfig{Partitions: 1, CapacityMessages: capacity})
		top, sub, _ := queueSub(t, e)
		time.Sleep(800 * time.Microsecond)
		for i := range publishes {
			err := top.Publish(context.Background(), Publish[int64]{Payload: int64(i), Size: 1, TTL: ttl})
			if err != nil {
				t.Fatalf("publication %d: %v (%+v)\n"+
					"    every record expires %v after it is published and nothing holds more than %d at once: "+
					"the lane is full of records the sweep never collected", i, err, sub.Inspect(), ttl, capacity-1)
			}
			time.Sleep(time.Millisecond)
		}
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		if s := sub.Inspect(); s.Ready != 0 || s.Dropped != publishes || !s.Conserved() {
			t.Fatalf("after every record expired: %+v, want all %d dropped", s, publishes)
		}
	})
}
