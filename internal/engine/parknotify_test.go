package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// This file holds the reproducers of the park/notify family: a permit or a
// slot that somebody parks on, returned on a withdrawal path that skips the
// notification its ordinary counterpart makes. Four incidents have had this
// shape already -- dispatch.gate, lane.reserve, Engine.reserveTotal and
// pendingBound.admit -- and the audit that followed the fourth found four
// more that had never been observed. Each test below is one of them, and each
// was watched failing before the fix it pins (to).
//
// parkguard_test.go is the class-level check that says there is no fifth.

// waitForGate blocks until somebody has registered on the gate and taken the
// channel it will park on, and returns that channel.
//
// It is what makes a park/notify test deterministic rather than a race: once
// a waiter has taken the channel and the condition it re-checks is still
// false, it is committed to selecting on *that* channel object, whether it
// has reached the select yet or not -- a close therefore wakes it however the
// two goroutines interleave. Polling the gauge alone would not do: waiting is
// incremented before the channel is taken, so a notify in between closes
// nothing and is lost, which is the very defect these tests are about.
func waitForGate(t *testing.T, g *notifyGate) <-chan struct{} {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		g.mu.Lock()
		ch := g.ch
		g.mu.Unlock()
		if ch != nil {
			return ch
		}
		if time.Now().After(deadline) {
			t.Fatalf("nobody registered on the gate within 15s (waiting=%d)", g.waiting.Load())
		}
		time.Sleep(50 * time.Microsecond)
	}
}

// TestBlockedLogPublisherIsWokenByAWithdrawnEngineReservation is the shared
// log's version of TestBlockedPublisherIsWokenByAWithdrawnReservation: the
// subject is the engine-wide record slots Engine.reserveTotal takes before it
// looks at the byte capacity, and gives back when the bytes refuse.
//
// That half of the fix landed by inspection beside its observed sibling
//
//	and had no test at all, which was hack/FOLLOWUPS.md's
//
// first open finding. The parker is Topic.waitLogRoom, which registers on the
// engine's gate only when the engine has a total limit, so the reproducer
// needs MaxTotalMessages *and* MaxTotalBytes, Arch: LogCursor so that the
// publisher parks on the log rather than on a lane, and a publication the
// byte limit refuses racing one that fits under the record limit.
//
// The arithmetic is what makes the window reachable. The engine holds two
// records and the blocking topic holds one of them throughout, so the
// publication under test needs the one slot that is left. A 9 990-byte
// publication on the spinning topic passes the record check (1+1 <= 2), is
// refused by the bytes (64+9990 > 10 000) and gives the slot back -- and
// while it holds it the engine is at its record limit, so the 64-byte
// publication is refused and parks. The withdrawal is then the only event
// that can wake it: nothing is settled while it waits, so the log's own gate
// never closes, and the count falls back to where it already was, so no
// other publication has to happen.
//
// The spinning topic takes OverflowReject rather than OverflowBlock on
// purpose: a refused publisher there returns instead of parking, so the
// spinners cycle through the withdrawal window continuously instead of
// parking on the first refusal.
//
// Two measurements against the withdrawal restored to its silent form (the
// notify deleted from reserveTotal), both with the wake in place afterwards
// and clean. Instrumented, with a counter at waitLogRoom's park and 32
// spinners: 400 attempts enter the wait 27 times, park 11 times and lose all
// 11 wakes. As it stands, with 16: "attempt 292: the publication that fits
// was not admitted: context deadline exceeded". The rate is the reason for
// the attempt count -- one attempt in a hundred or so reaches the window, so
// a round or two of anything proves nothing here.
func TestBlockedLogPublisherIsWokenByAWithdrawnEngineReservation(t *testing.T) {
	const (
		attempts = 2000
		spinners = 16
		big      = 9_990
		small    = 64
		maxMsgs  = 2
		maxBytes = 10_000
	)
	e, err := New(Config[int64]{
		// A shared partition log is what puts the publisher in waitLogRoom;
		// the per-lane waitRoom is the sibling test's subject.
		Kinds:            KindSet{Arch: LogCursor},
		Clone:            func(v int64) int64 { return v },
		MaxTotalMessages: maxMsgs,
		MaxTotalBytes:    maxBytes,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = e.Close() }()
	block, err := e.CreateTopic("block", TopologyConfig{Partitions: 1, AckDeadline: -1, Overflow: OverflowBlock})
	if err != nil {
		t.Fatalf("CreateTopic(block): %v", err)
	}
	bsub, err := block.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatalf("Subscribe(block): %v", err)
	}
	bc, err := bsub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	spin, err := e.CreateTopic("spin", TopologyConfig{Partitions: 1, AckDeadline: -1, Overflow: OverflowReject})
	if err != nil {
		t.Fatalf("CreateTopic(spin): %v", err)
	}
	if _, err := spin.Subscribe("s", SubscriptionConfig{}); err != nil {
		t.Fatalf("Subscribe(spin): %v", err)
	}
	// The seed holds one of the engine's two record slots and the bytes that
	// make the spinning topic's publication impossible. Every round below
	// admits one record and settles one, so the engine stays at one.
	if err := block.Publish(context.Background(), Publish[int64]{Payload: 1, Size: small}); err != nil {
		t.Fatalf("seed publish: %v", err)
	}
	var stop atomic.Bool
	var spun atomic.Int64
	var wg sync.WaitGroup
	for range spinners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				_ = spin.Publish(context.Background(), Publish[int64]{Payload: 2, Size: big})
				spun.Add(1)
			}
		}()
	}
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()
	// The spinners have to be running before the first attempt: the window
	// the attempt has to land in is theirs, and a loop that outruns their
	// scheduling tests nothing at all.
	warm := time.Now().Add(30 * time.Second)
	for spun.Load() < int64(4*spinners) {
		if time.Now().After(warm) {
			t.Fatalf("the spinners made only %d publications in 30s", spun.Load())
		}
		time.Sleep(time.Millisecond)
	}
	dst := make([]Claim[int64], 1)
	for a := range attempts {
		// Generous on purpose: the wake is immediate when it comes at all, so
		// the only thing this bound decides is how long a lost one takes to
		// report itself on a loaded host.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := block.Publish(ctx, Publish[int64]{Payload: 3, Size: small})
		cancel()
		if err != nil {
			t.Fatalf("attempt %d: the publication that fits was not admitted: %v\n"+
				"    the engine-wide record slots a byte refusal gives back are a fall in the count "+
				"Topic.waitLogRoom re-checks, and the withdrawal is the only event that can announce it", a, err)
		}
		fctx, fcancel := context.WithTimeout(context.Background(), 30*time.Second)
		n, err := bc.Fetch(fctx, dst)
		fcancel()
		if err != nil || n != 1 {
			t.Fatalf("attempt %d: Fetch = %d, %v, want one record: the round has to settle what it "+
				"admitted or the engine is at its record limit for good", a, n, err)
		}
		if err := bc.Ack(dst[0].Env, dst[0].Gen); err != nil {
			t.Fatalf("attempt %d: Ack: %v", a, err)
		}
	}
}

// TestARolledBackEngineReservationWakesAnIdleWaiter pins the second of
// hack/FOLLOWUPS.md's findings: Engine.releaseTotal announced a fall in the
// engine-wide record count on e.room and not on e.idle, and for
// IdleIncludingDelayed that count is the whole condition.
//
// isIdle returns true as soon as e.msgs is zero and, in that mode, false on
// any non-zero count without looking further, so a WaitIdle that samples the
// count while a reservation that is about to be rolled back stands parks for
// a fall that nothing announces. The idle gate is notified by eng.settled()
// instead, which the settle path calls beside the release -- and no rollback
// path calls it: Topic.unreserveAll, reserveAll's own refusal,
// Consumer.duplicateTo, stream.go's retain rollback and store_log.go's all
// lower e.msgs through releaseTotal alone.
//
// The reservation is taken directly here so that the wait is a fact rather
// than a race: while it stands the condition cannot become true, so once the
// waiter has taken the channel it is committed to parking on it, and the
// rollback either closes that channel or the waiter waits for its context.
// The rollback paths themselves are driven by the test below.
func TestARolledBackEngineReservationWakesAnIdleWaiter(t *testing.T) {
	const size = 64
	e := newTestEngine(t, KindSet{}, TopologyConfig{})
	if !e.reserveTotal(1, size) {
		t.Fatal("reserveTotal refused an unbounded engine")
	}
	idle := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		idle <- e.WaitIdle(ctx, IdleIncludingDelayed)
	}()
	waitForGate(t, &e.idle)
	// The rollback: the count falls back to where it was before the
	// reservation, which is the only event that can end this wait.
	e.releaseTotal(1, size)
	if err := <-idle; err != nil {
		t.Fatalf("WaitIdle(IdleIncludingDelayed) did not return after the reservation was rolled back: %v\n"+
			"    e.msgs is the whole condition of that mode, so every fall in it has to be announced on "+
			"the idle gate and not only on the room gate ", err)
	}
}

// TestARefusedPublicationAnnouncesTheEngineWideFallOnTheIdleGate drives the
// two rollback paths a publication can reach on its own and holds each to the
// announcement, which is what the specification moved into releaseTotal.
//
// It asserts the notification rather than a wake, because the window a real
// rollback leaves is two instructions wide and a test that raced for it would
// pass by luck either way: the gate is registered on and its channel taken
// before the publication, so a notify during it closes that channel and the
// check reads the result afterwards.
func TestARefusedPublicationAnnouncesTheEngineWideFallOnTheIdleGate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fanout FanoutPolicy
		why    string
	}{
		{
			name: "atomic-rollback", fanout: Atomic,
			why: "Topic.unreserveAll gives back the reservation the admitted subscription held",
		},
		{
			name: "independent-refusal", fanout: Independent,
			why: "reserveAll gives back the engine-wide slot the refusing lane would not take",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{
				Partitions: 1, AckDeadline: -1, Overflow: OverflowReject, Fanout: tc.fanout,
			})
			tp, err := e.CreateTopic("t", TopologyConfig{})
			if err != nil {
				t.Fatalf("CreateTopic: %v", err)
			}
			if _, err := tp.Subscribe("room", SubscriptionConfig{}); err != nil {
				t.Fatalf("Subscribe(room): %v", err)
			}
			// A lane whose byte capacity no publication of this test can
			// fit: it takes the engine-wide reservation and then refuses,
			// which is the rollback under test.
			if _, err := tp.Subscribe("tiny", SubscriptionConfig{CapacityBytes: 8}); err != nil {
				t.Fatalf("Subscribe(tiny): %v", err)
			}
			// Nobody is really waiting; the gauge is what makes notify do
			// its work, and the channel is what records that it did.
			e.idle.waiting.Add(1)
			defer e.idle.waiting.Add(-1)
			ch := e.idle.chanFor()
			err = tp.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 64})
			switch tc.fanout {
			case Atomic:
				if !errors.Is(err, errcode.ErrQueueFull) {
					t.Fatalf("Publish = %v, want ErrQueueFull: the refusal is the rollback", err)
				}
			default:
				if !errors.Is(err, errcode.ErrPartial) {
					t.Fatalf("Publish = %v, want ErrPartial: one lane took it and one refused", err)
				}
			}
			select {
			case <-ch:
			default:
				msgs, _, _ := e.Totals()
				t.Fatalf("the refusal lowered the engine-wide count to %d and announced nothing on the "+
					"idle gate: %s, and IdleIncludingDelayed reads that count alone ",
					msgs, tc.why)
			}
		})
	}
}

// TestAPublisherTheEngineWideLimitRefusedParksInsteadOfSpinning pins the
// third of hack/FOLLOWUPS.md's findings: Topic.waitRoom re-checked the lane
// alone, so a publisher the *engine* refused returned from it immediately,
// retried, was refused again and spun for as long as the engine stayed full.
//
// reserveAll records a refuser whether the refusal came from reserveTotal or
// from the lane, and the per-lane wait then asked only
// s.lanes[part].hasRoom(size) -- which is true, because the lane is not the
// thing that is full. Topic.waitLogRoom right below it was restructured for
// exactly this and says why in its doc comment; the per-lane path never got
// the same treatment. It burns a processor rather than losing a wake, which
// is why no test had noticed.
//
// So this test has to observe the spin, and what it observes is the gate
// registration: waitRoom registers on entry and deregisters on return, so a
// publisher that is really parked holds the registration for as long as it
// waits, and one that is retrying gives it up on every iteration. Nothing in
// this test settles anything until the sampling window is over, so under the
// fix the gauge cannot legitimately fall back to zero. The last two steps
// then show the wake itself works: the seed is settled and the publication
// the engine refused is admitted, which it can only be if the wait was
// registered on the engine's gate as well as the subscription's.
func TestAPublisherTheEngineWideLimitRefusedParksInsteadOfSpinning(t *testing.T) {
	const (
		size   = 64
		sample = 50 * time.Millisecond
	)
	e, err := New(Config[int64]{
		Kinds: KindSet{Arch: featureArch},
		Clone: func(v int64) int64 { return v },
		// One record for the whole engine, against a lane that holds
		// DefaultCapacityMessages: the lane always has room and the engine
		// never does, which is the configuration the per-lane wait could not
		// tell apart.
		MaxTotalMessages: 1,
		DefaultTopology:  TopologyConfig{Partitions: 1, AckDeadline: -1, Overflow: OverflowBlock},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = e.Close() }()
	tp, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := tp.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if err := tp.Publish(context.Background(), Publish[int64]{Payload: 1, Size: size}); err != nil {
		t.Fatalf("seed publish: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- tp.Publish(ctx, Publish[int64]{Payload: 2, Size: size})
	}()
	// The publisher is in waitRoom once it has registered on the gate.
	entered := time.Now().Add(30 * time.Second)
	for sub.room.waiting.Load() == 0 {
		if time.Now().After(entered) {
			t.Fatal("the refused publisher never registered on the subscription's room gate")
		}
	}
	var samples, left int64
	for deadline := time.Now().Add(sample); time.Now().Before(deadline); {
		samples++
		if sub.room.waiting.Load() == 0 {
			left++
		}
	}
	if left > 0 {
		t.Errorf("the refused publisher left the subscription's room gate in %d of %d samples over %v: "+
			"it is retrying, not parked\n"+
			"    the engine-wide limit refused it and the lane has room, so a wait that re-checks the "+
			"lane alone returns at once and the publisher spins until the engine empties "+
			"", left, samples, sample)
	}
	// The wake: settling the seed is the only thing that can admit the
	// parked publication, and it happens on the engine's gate.
	dst := make([]Claim[int64], 1)
	fctx, fcancel := context.WithTimeout(context.Background(), 30*time.Second)
	n, err := c.Fetch(fctx, dst)
	fcancel()
	if err != nil || n != 1 {
		t.Fatalf("Fetch = %d, %v, want the seed", n, err)
	}
	if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("the publication was not admitted once the engine had room: %v\n"+
			"    a publisher refused by the engine-wide limit has to be woken by the return of it, "+
			"which is the engine's own gate ", err)
	}
}

// TestARebalanceWakesTheConsumerItHandsAReadyLaneTo pins the fourth of
// hack/FOLLOWUPS.md's findings, which is the same class of mistake on a
// different gate: Subscription.rebalanceLocked stored the new assignment and
// announced nothing, and revoke announced only when it had requeued an
// in-flight record.
//
// A rebalance that hands a lane holding *ready* records to a consumer that is
// already parked in Fetch therefore flipped that consumer's ln.assigned(c)
// from false to true with no wake at all: there is no lease to revoke, so
// revoke's requeued count is zero, and the consumer waited for its context
// while a record it now owns sat ready in its lane.
//
// Four lanes and a sticky assignment are what make the hand-over happen
// without anything else announcing it. Two consumers deal the lanes 0,2 to
// the first and 1,3 to the second; the record is published to lane 3, which
// the first consumer may not claim from, so its fetch parks. A third consumer
// then joins and the deal becomes 0 to the first, 1 to the second, 2 to the
// third and 3 to the first -- and the first consumer's own parked fetch is
// the one that has to be woken. A consumer *leaving* would not show it:
// Consumer.Close announces itself whether or not it requeued anything
// , which is the same repair one level along.
func TestARebalanceWakesTheConsumerItHandsAReadyLaneTo(t *testing.T) {
	top := TopologyConfig{Partitions: 4, AckDeadline: -1}
	e := newTestEngine(t, KindSet{Arch: archFor(top)}, top)
	tp, err := e.CreateTopic("t", top)
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := tp.Subscribe("s", SubscriptionConfig{Sticky: true})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	first, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach(first): %v", err)
	}
	if _, err := sub.Attach(ConsumerConfig{}); err != nil {
		t.Fatalf("Attach(second): %v", err)
	}
	// Lane 3 belongs to the second consumer, which never fetches: the record
	// is ready, unclaimed, and unreachable from the consumer that is about
	// to park.
	if err := tp.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 8, Partition: 3}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, owners := sub.Assignment(); owners[3] == first.id {
		t.Fatalf("lane 3 is already the first consumer's (owners=%v): the rebalance has nothing to hand over", owners)
	}
	got := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		dst := make([]Claim[int64], 1)
		n, err := first.Fetch(ctx, dst)
		switch {
		case err != nil:
			got <- err
		case n != 1:
			got <- errcode.ErrPartial
		default:
			got <- first.Ack(dst[0].Env, dst[0].Gen)
		}
	}()
	// The fetch is parked once it has announced its intent to wait, which is
	// what HasWaiters exists for: a sleep proves nothing on a loaded host.
	deadline := time.Now().Add(30 * time.Second)
	for !sub.HasWaiters() {
		if time.Now().After(deadline) {
			t.Fatal("the first consumer's fetch never parked")
		}
		time.Sleep(50 * time.Microsecond)
	}
	// The join deals lane 3 to the parked consumer.
	if _, err := sub.Attach(ConsumerConfig{}); err != nil {
		t.Fatalf("Attach(third): %v", err)
	}
	if _, owners := sub.Assignment(); owners[3] != first.id {
		t.Fatalf("lane 3 is %d after the join, not the first consumer's (owners=%v)", owners[3], owners)
	}
	if err := <-got; err != nil {
		t.Fatalf("the parked consumer was not woken by the rebalance that gave it lane 3: %v\n"+
			"    a rebalance changes which lanes a consumer may claim from, so it is a change in the "+
			"condition a parked fetch waits on and has to be announced whether or not it revoked a "+
			"lease ", err)
	}
}
