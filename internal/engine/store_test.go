package engine

import (
	"context"
	"testing"
	"time"
)

// awaitSweep waits for the engine scheduler to have dropped want records.
func awaitSweep(t *testing.T, sub *Subscription[int64], want int64) {
	t.Helper()
	deadline := time.Now().Add(wedgeLimit)
	for {
		got := sub.Inspect().Dropped
		if got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the sweep dropped %d records, wanted %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestChanStoreDrainKeepsReadyOrder: a time-to-live sweep on a channel-backed
// lane that is deeper than the channel buffer must put the survivors back in
// the order it took them out. The sweep drains the store and requeues what is
// left, so a Drain that returned the overflow before the channel would move
// every record beyond maxChanCap ahead of the ones in front of it — a
// reordering by the sweep itself, on an architecture Arch.Ordered calls
// first-in-first-out.
func TestChanStoreDrainKeepsReadyOrder(t *testing.T) {
	const n = maxChanCap + 64
	e, err := New(Config[int64]{
		Kinds:           KindSet{Arch: ChanFanout},
		Clone:           func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{Partitions: 1, CapacityMessages: n + 1, AckDeadline: -1},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = e.Close() }()
	top, sub, c := queueSub(t, e)
	ctx := context.Background()
	// One record that expires, then a lane deep enough to overflow the
	// channel. The first one is what makes the sweep run at all.
	if err := top.Publish(ctx, Publish[int64]{
		Payload: 0, Size: 1, TTL: 500 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		if err := top.Publish(ctx, Publish[int64]{Payload: int64(i), Size: 1}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	awaitSweep(t, sub, 1)
	if s := sub.Inspect(); s.Ready != n {
		t.Fatalf("after the sweep: %+v", s)
	}
	dst := make([]Claim[int64], 64)
	for want := int64(1); want <= int64(n); {
		got, err := c.Fetch(poll(t), dst)
		if err != nil || got == 0 {
			t.Fatalf("fetch at %d: got=%d err=%v", want, got, err)
		}
		for i := range got {
			if dst[i].Payload != want {
				t.Fatalf("the sweep permuted the lane: record %d came back as %d", want, dst[i].Payload)
			}
			if err := c.Ack(dst[i].Env, dst[i].Gen); err != nil {
				t.Fatalf("ack %d: %v", want, err)
			}
			want++
		}
	}
}

// TestLogStoreClaimAfterExpiry: a claim that follows a time-to-live sweep on a
// shared-log lane reads back exactly the window it handed the log. The sweep
// leaves the store's scratch slice empty with its capacity intact, so a claim
// that indexed the scratch rather than that window read past its end.
func TestLogStoreClaimAfterExpiry(t *testing.T) {
	e, err := New(Config[int64]{
		Kinds:           KindSet{Arch: LogCursor},
		Clone:           func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{Partitions: 1, AckDeadline: -1},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = e.Close() }()
	top, sub, c := queueSub(t, e)
	ctx := context.Background()
	if err := top.Publish(ctx, Publish[int64]{
		Payload: 1, Size: 1, TTL: 20 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	awaitSweep(t, sub, 1)
	if err := top.Publish(ctx, Publish[int64]{Payload: 2, Size: 1}); err != nil {
		t.Fatal(err)
	}
	// One claim at a time: the window is then narrower than the scratch the
	// sweep left behind, which is the case that read past its end.
	dst := make([]Claim[int64], 1)
	n, err := c.Fetch(poll(t), dst)
	if err != nil || n != 1 || dst[0].Payload != 2 {
		t.Fatalf("fetch after the sweep: n=%d payload=%d err=%v", n, dst[0].Payload, err)
	}
	if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatal(err)
	}
	if s := sub.Inspect(); s.Dropped != 1 || s.Acked != 1 || !s.Conserved() {
		t.Fatalf("%+v", s)
	}
}

// logEngine builds an engine on the shared partition log with one topic, one
// subscription and one consumer. Every family but the architecture follows its
// binding, which is what a test of the general contract wants.
func logEngine(t *testing.T, capacity int) (*Engine[int64], *Topic[int64], *Subscription[int64], *Consumer[int64]) {
	t.Helper()
	return logEngineKinds(t, capacity, KindSet{Arch: LogCursor})
}

// logEngineKinds is logEngine with the rest of the kinds named, for a test
// whose subject is one of them.
func logEngineKinds(t *testing.T, capacity int, ks KindSet) (*Engine[int64], *Topic[int64], *Subscription[int64], *Consumer[int64]) {
	t.Helper()
	e, err := New(Config[int64]{
		Kinds: ks,
		Clone: func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{
			Partitions: 1, CapacityMessages: capacity, AckDeadline: -1,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	top, sub, c := queueSub(t, e)
	return e, top, sub, c
}

// TestLogRingExpiryBehindTheCursor: logRing.expireTTL takes the run of
// expired slots at the cursor and names the earliest expiry still to come. A
// slot that has already expired but sits behind one that has not is neither
// of those: the cursor cannot pass the live record, so no sweep can reach it
// — the claim path drops it when the cursor arrives there. Naming its time
// handed lane.nextEvent an instant that had already passed, which minRearm
// turns into a wake-up of the engine's single scheduler goroutine every
// millisecond, per lane, for as long as the subscription stays stalled.
func TestLogRingExpiryBehindTheCursor(t *testing.T) {
	e, top, sub, _ := logEngine(t, 16)
	ctx := context.Background()
	// A record with no time to live holds the cursor; one with a time to
	// live sits behind it.
	if err := top.Publish(ctx, Publish[int64]{Payload: 1, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if err := top.Publish(ctx, Publish[int64]{Payload: 2, Size: 1, TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	now := e.now() + int64(2*time.Hour)
	dst, cursor, next := top.rings[0].expireTTL(0, now, nil)
	if len(dst) != 0 || cursor != 0 {
		t.Fatalf("the sweep took %d records and left the cursor at %d, "+
			"but the record at the cursor has not expired", len(dst), cursor)
	}
	if next != 0 && next <= now {
		t.Fatalf("the log named %d as its next expiry, which is %v in the past",
			next, time.Duration(now-next))
	}
	ln := sub.lanes[0]
	if n := ln.expireTTL(now); n != 0 {
		t.Fatalf("the sweep dropped %d records from behind a live one", n)
	}
	if got := ln.ttlAt.Load(); got != 0 && got <= now {
		t.Fatalf("the lane re-armed itself for %d, which has passed: the scheduler "+
			"would then look at a stalled lane every %v", got, minRearm)
	}
	if s := sub.Inspect(); s.Ready != 2 || !s.Conserved() {
		t.Fatalf("%+v", s)
	}
}

// TestLogStoreRetryQueueOrder: the redelivery queue of a shared-log
// subscription is served oldest first and before the log, and a claim pops it
// by advancing a head index rather than shifting the slice down. The order
// has to survive the compaction that index needs, which happens part-way
// through a deep queue.
//
// The head index and its compaction are the *sealed* half of
// retryQueue.claim, so the placement is pinned to BlockLane rather than left
// at the binding: an unsealed lane rebuilds the tail of the deque in place
// (the `kept` path) and never advances the head at all, so under any other
// placement this test exercises none of what it is about. It broke when the
// campaign bound PushFront — "the redelivery queue is out of order: got 200,
// want 1" — for a lane that had asked for no ordering, which is PushFront
// serving the newest failure first exactly as documented. What every
// placement owes its own redelivery order is TestModel/<placement>/order
// and .../blocks-lane (adr/ADR-017-assumptions.md).
func TestLogStoreRetryQueueOrder(t *testing.T) {
	const n = 200
	_, top, sub, c := logEngineKinds(t, n+1, KindSet{Arch: LogCursor, RetryLane: BlockLane})
	ctx := context.Background()
	for i := 1; i <= n; i++ {
		if err := top.Publish(ctx, Publish[int64]{Payload: int64(i), Size: 1}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	dst := make([]Claim[int64], n)
	got, err := c.Fetch(poll(t), dst)
	if got != n || err != nil {
		t.Fatalf("fetch: n=%d err=%v", got, err)
	}
	// Every delivery goes back, in order, which is the deep queue.
	for i := range n {
		if err := c.Nack(dst[i].Env, dst[i].Gen, 0); err != nil {
			t.Fatalf("nack %d: %v", i, err)
		}
	}
	// Draining it in small batches pops far enough past the head for the
	// queue to be compacted more than once.
	batch := make([]Claim[int64], 7)
	for want := int64(1); want <= n; {
		got, err := c.Fetch(poll(t), batch)
		if got == 0 || err != nil {
			t.Fatalf("fetch at %d: got=%d err=%v", want, got, err)
		}
		for i := range got {
			if batch[i].Payload != want {
				t.Fatalf("the redelivery queue is out of order: got %d, want %d",
					batch[i].Payload, want)
			}
			if err := c.Ack(batch[i].Env, batch[i].Gen); err != nil {
				t.Fatalf("ack %d: %v", want, err)
			}
			want++
		}
	}
	if s := sub.Inspect(); s.Ready != 0 || s.InFlight != 0 || s.Acked != n || !s.Conserved() {
		t.Fatalf("%+v", s)
	}
}
