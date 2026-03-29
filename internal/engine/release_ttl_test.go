package engine

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/lifecycle"
)

// TestExpiredRecordReleasesOutsideTheFetchTicket proves the same property as
// TestFetchDoesNotHoldTheGateOverAReleaseCallback, on the other route into the
// same code: a claim that gives a record up, where that test has a close the
// fetch loses to.
//
// It was written when that test reached its drop by RACING a close against a
// fetch, which could not land with one P and was left skipping on small
// machines. That test now forces its interleaving and
// proves its route everywhere ; this one stays for its own.
//
// The time-to-live route reaches the same code with no race at all. A record
// that outlived its TTL is given up by the lease loop, and lane.lease says why
// that matters in its own comment: "an expired record's last reference is the
// publisher's payload and dropping it here would run that callback with the
// ticket held". So the drop goes through fetchDrops like every other, and
// fetchDrops runs after Consumer.take has left its ticket.
//
// The assertion is the property itself rather than a timing: the callback
// starts a Close and waits for the phase to pass StopFetch. StopFetch waits
// for every fetch ticket, so a callback holding one could never see the phase
// move, however long it waited — and a callback that is not holding one sees
// it move at once. Both directions are decided by the engine, not by the
// scheduler.
func TestExpiredRecordReleasesOutsideTheFetchTicket(t *testing.T) {
	const ttl = time.Second
	eng, fake := newReleaseEngine(t, 5*time.Second, TopologyConfig{})
	top := newReleaseTopic(t, eng, TopologyConfig{
		Partitions: 1, CapacityMessages: 64, AckDeadline: time.Second,
	})
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	lc := eng.Lifecycle()

	var (
		released atomic.Int64
		stuck    atomic.Int64
		once     sync.Once
		closed   = make(chan struct{})
	)
	release := func(int64) {
		released.Add(1)
		// The first callback is the one under test: it asks for a shutdown
		// and then waits for the barrier that would be waiting for THIS
		// goroutine's fetch ticket, if it still held one.
		once.Do(func() {
			go func() { defer close(closed); _ = eng.Close() }()
			deadline := time.Now().Add(5 * time.Second)
			for lc.Phase() <= lifecycle.StopFetch {
				if time.Now().After(deadline) {
					stuck.Add(1)
					return
				}
				time.Sleep(time.Millisecond)
			}
		})
	}

	const n = 4
	for i := range n {
		if err := top.Publish(t.Context(), Publish[int64]{
			Payload: int64(i + 1), Size: 1, Mode: CopyOwned, Release: release, TTL: ttl,
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	// A record past its time to live is also what the engine's own TTL sweep
	// gives up, and the advance below is what would wake it. The sweep runs
	// on the scheduler goroutine, which StopFetch waits for, so a callback
	// it ran would wait out its own deadline for a phase that cannot move --
	// the one exception Publish.Release documents, and not the route this
	// test is about. Left running, the sweep won the race for the records in
	// about one run in sixty-five on two CPUs under -race: "Engine.Close
	// never returned" when it won after Attach, "Attach: iqmq: closed" when
	// it won before. Stopped, the fetch's own claim is
	// the only route to them, as the fetch-expiry release route arranges it
	// too; Engine.Close's own call to scheduler.close is idempotent.
	eng.sched.close()

	// Past the time to live, with nothing yet delivered: every record is one
	// the lease loop must give up rather than hand over.
	fake.Advance(ttl + time.Millisecond)

	cons, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	dst := make([]Claim[int64], n)
	got, _ := cons.Fetch(t.Context(), dst)

	if got != 0 {
		t.Errorf("Fetch handed over %d expired record(s); a record past its TTL is never delivered", got)
	}
	if released.Load() == 0 {
		t.Fatalf("no Release callback ran for %d expired record(s): the test proves nothing, and the "+
			"TTL route is supposed to need no race to reach", n)
	}
	if stuck.Load() > 0 {
		t.Fatalf("a Release callback waited out its deadline for the phase to pass StopFetch: the fetch " +
			"ticket was still held while the callback ran, so Engine.Close could never complete ")
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Engine.Close never returned after a Release callback started it from the fetch route")
	}
}
