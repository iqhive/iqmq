package engine

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
)

// This file holds a publication that displaces records under an engine-wide
// limit against a publication of another topic that runs while it drops. A
// record a publication drops gave its engine-wide share back to the engine,
// where the other publication could take it before the one that dropped it
// booked it, and the publication that paid for the room was refused, or
// reached fewer subscriptions, with the records already gone. The share now
// stays booked for the publication that made the drop.

// otherTopic builds the topic the concurrent publication goes to: one
// subscription, refused rather than parked or displaced when there is no
// room.
func otherTopic(t *testing.T, e *Engine[int64]) *Topic[int64] {
	t.Helper()
	other, err := e.CreateTopic("u", TopologyConfig{Overflow: OverflowReject, Fanout: Atomic})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := other.Subscribe("s", SubscriptionConfig{}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	return other
}

// stealDuringDrops publishes x to top with trap armed and, once the
// publication is held at the clock read the trap names, between two of its
// drops, calls atPark and publishes one record to other. It returns the two
// publications' answers. The publication is held inside a drop, under the
// lane mutex Subscription.Inspect takes, so atPark reads the counters.
func stealDuringDrops(t *testing.T, trap *clockTrap, top, other *Topic[int64], x Publish[int64], atPark func()) (xerr, yerr error) {
	t.Helper()
	trap.arm()
	done := make(chan error, 1)
	go func() { done <- top.Publish(t.Context(), x) }()
	select {
	case <-trap.parked:
	case err := <-done:
		t.Fatalf("the publication never reached its second drop: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the publication never reached its second drop")
	}
	atPark()
	yerr = other.Publish(t.Context(), Publish[int64]{Payload: 99, Size: 1})
	trap.let()
	return <-done, yerr
}

// TestADisplacingPublicationKeepsTheRoomItsDropsMake: under an engine-wide
// limit a publication makes room by dropping records, and each drop gave its
// share of the limit back to the engine. A publication of another topic
// running between two of those drops took the share, so the drops ran out
// before the publication had its room: it was refused under the Atomic
// fan-out, a subscription was refused under DropSlow, and a lane was refused
// under DropOldest, each with the records already dropped for it. A drop now
// hands its share to the publication that made it, so the other topic's
// publication is the one refused, and the records dropped buy the room they
// were dropped for.
//
// The clock trap holds the publication at the clock read a subscription's
// store makes to take a delayed record, after its first drop and before its
// second.
func TestADisplacingPublicationKeepsTheRoomItsDropsMake(t *testing.T) {
	for _, tc := range []struct {
		fan FanoutPolicy
		ov  OverflowPolicy
	}{
		{Atomic, OverflowDropOldest},
		{DropSlow, OverflowReject},
	} {
		t.Run("persubenvelopes/"+tc.fan.String()+"/"+tc.ov.String(), func(t *testing.T) {
			// MaxTotalMessages 3: a holds x.1 ready, and b holds x.1 on a
			// lease and y.1 delayed, which the wildcard filters keep out of a.
			// The publication of x.2 needs a record for each, which the two
			// drops make together (makeRoomTogether).
			trap := newClockTrap(clock.Real{}, "(*Topic[", ").makeRoomTogether")
			e, err := New(Config[int64]{
				Kinds: KindSet{Arch: PerSubEnvelopes}, MaxTotalMessages: 3, Clock: trap,
				Clone: func(v int64) int64 { return v },
				DefaultTopology: TopologyConfig{
					Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: tc.ov, Fanout: tc.fan,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			top, err := e.CreateTopic("t", TopologyConfig{Wildcards: true})
			if err != nil {
				t.Fatal(err)
			}
			a, err := top.Subscribe("a", SubscriptionConfig{Filter: "x.*"})
			if err != nil {
				t.Fatal(err)
			}
			b, err := top.Subscribe("b", SubscriptionConfig{Filter: ">"})
			if err != nil {
				t.Fatal(err)
			}
			cb, err := b.Attach(ConsumerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			other := otherTopic(t, e)
			if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 1, Subject: "x.1"}); err != nil {
				t.Fatal(err)
			}
			if cl := fetchSeqs(t, cb, 1); len(cl) != 1 {
				t.Fatalf("fetch = %v", cl)
			}
			if err := top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 1, Subject: "y.1", Delay: time.Hour}); err != nil {
				t.Fatal(err)
			}
			var parkedAt int64
			xerr, yerr := stealDuringDrops(t, trap, top, other, Publish[int64]{Payload: 3, Size: 1, Subject: "x.2"},
				func() { parkedAt = a.st.dropped.Load() + b.st.dropped.Load() })
			if parkedAt != 1 {
				t.Fatalf("the publication was held after %d drops, want 1: the test no longer reaches the window", parkedAt)
			}
			as, bs := a.Inspect(), b.Inspect()
			if xerr != nil || as.Published != 2 || bs.Published != 3 || as.Dropped != 1 || bs.Dropped != 1 {
				t.Fatalf("the publication answered %v with a %+v and b %+v, want it admitted to both for the "+
					"record each dropped: another topic took the room a drop made", xerr, as, bs)
			}
			if !errors.Is(yerr, errcode.ErrQueueFull) {
				t.Fatalf("the other topic's publication = %v, want ErrQueueFull: the room was the "+
					"displacing publication's", yerr)
			}
			if msgs, _, _ := e.Totals(); msgs != 3 {
				t.Fatalf("the engine holds %d records, want 3", msgs)
			}
		})
	}
	t.Run("persubenvelopes/atomic/dropold/free-room", func(t *testing.T) {
		// MaxTotalMessages 4 with one free: a holds z.a delayed, b holds x.b
		// on a lease, and the other topic one record. The publication of x.a
		// needs a record for each, the free one and the one a's drop makes,
		// and the other topic's publication runs during that drop. The free
		// room was booked for nobody, so the other publication took it and
		// the drop made only half the room.
		trap := newClockTrap(clock.Real{}, "(*Topic[", ").makeRoomTogether")
		e, err := New(Config[int64]{
			Kinds: KindSet{Arch: PerSubEnvelopes}, MaxTotalMessages: 4, Clock: trap,
			Clone: func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{
				Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowDropOldest, Fanout: Atomic,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close() })
		top, err := e.CreateTopic("t", TopologyConfig{Wildcards: true})
		if err != nil {
			t.Fatal(err)
		}
		a, err := top.Subscribe("a", SubscriptionConfig{Filter: "*.a"})
		if err != nil {
			t.Fatal(err)
		}
		b, err := top.Subscribe("b", SubscriptionConfig{Filter: "x.*"})
		if err != nil {
			t.Fatal(err)
		}
		cb, err := b.Attach(ConsumerConfig{})
		if err != nil {
			t.Fatal(err)
		}
		other := otherTopic(t, e)
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 1, Subject: "z.a", Delay: time.Hour}); err != nil {
			t.Fatal(err)
		}
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 1, Subject: "x.b"}); err != nil {
			t.Fatal(err)
		}
		if cl := fetchSeqs(t, cb, 1); len(cl) != 1 {
			t.Fatalf("fetch = %v", cl)
		}
		if err := other.Publish(t.Context(), Publish[int64]{Payload: 98, Size: 1}); err != nil {
			t.Fatal(err)
		}
		var parkedAt int64
		xerr, yerr := stealDuringDrops(t, trap, top, other, Publish[int64]{Payload: 3, Size: 1, Subject: "x.a"},
			func() { parkedAt = a.st.dropped.Load() + b.st.dropped.Load() })
		if parkedAt != 0 {
			t.Fatalf("the publication was held after %d drops, want 0: the test no longer reaches the window", parkedAt)
		}
		as, bs := a.Inspect(), b.Inspect()
		if xerr != nil || as.Published != 2 || bs.Published != 2 || as.Dropped != 1 {
			t.Fatalf("the publication answered %v with a %+v and b %+v, want it admitted to both for the "+
				"free record and the one a dropped: another topic took the free room it counted on", xerr, as, bs)
		}
		if !errors.Is(yerr, errcode.ErrQueueFull) {
			t.Fatalf("the other topic's publication = %v, want ErrQueueFull: the free room was the "+
				"displacing publication's", yerr)
		}
		if msgs, _, _ := e.Totals(); msgs != 4 {
			t.Fatalf("the engine holds %d records, want 4", msgs)
		}
	})
	t.Run("persubenvelopes/independent/dropold/lane/free-room", func(t *testing.T) {
		// MaxTotalBytes 3 with one byte free: the lane holds r0 on a lease
		// and d1 delayed. The 2-byte publication needs the free byte and the
		// one the lane's drop makes, and the other topic's publication runs
		// during that drop.
		trap := newClockTrap(clock.Real{}, "(*Topic[", ").reserveTotalIn")
		e, err := New(Config[int64]{
			Kinds: KindSet{Arch: PerSubEnvelopes}, MaxTotalBytes: 3, Clock: trap,
			Clone: func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{
				Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowDropOldest, Fanout: Independent,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close() })
		top, a, ca := queueSub(t, e)
		other := otherTopic(t, e)
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 0, Size: 1}); err != nil {
			t.Fatal(err)
		}
		if cl := fetchSeqs(t, ca, 1); len(cl) != 1 {
			t.Fatalf("fetch = %v", cl)
		}
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 1, Delay: time.Hour}); err != nil {
			t.Fatal(err)
		}
		var parkedAt int64
		xerr, yerr := stealDuringDrops(t, trap, top, other, Publish[int64]{Payload: 3, Size: 2},
			func() { parkedAt = a.st.dropped.Load() })
		if parkedAt != 0 {
			t.Fatalf("the publication was held after %d drops, want 0: the test no longer reaches the window", parkedAt)
		}
		if as := a.Inspect(); xerr != nil || as.Published != 3 || as.Dropped != 1 {
			t.Fatalf("the publication answered %v with the lane %+v, want it admitted for the free byte and "+
				"the one the lane dropped: another topic took the free room it counted on", xerr, as)
		}
		if !errors.Is(yerr, errcode.ErrQueueFull) {
			t.Fatalf("the other topic's publication = %v, want ErrQueueFull: the free room was the "+
				"displacing publication's", yerr)
		}
		if _, bytes, _ := e.Totals(); bytes != 3 {
			t.Fatalf("the engine holds %d bytes, want 3", bytes)
		}
	})
	t.Run("persubenvelopes/independent/dropold/lane", func(t *testing.T) {
		// MaxTotalBytes 3: the lane holds r0 on a lease, r1 ready and d2
		// delayed, a byte each. The 2-byte publication needs both drops,
		// which the lane makes for its own share (reserveTotalIn).
		trap := newClockTrap(clock.Real{}, "(*Topic[", ").reserveTotalIn")
		e, err := New(Config[int64]{
			Kinds: KindSet{Arch: PerSubEnvelopes}, MaxTotalBytes: 3, Clock: trap,
			Clone: func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{
				Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowDropOldest, Fanout: Independent,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close() })
		top, a, ca := queueSub(t, e)
		other := otherTopic(t, e)
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 0, Size: 1}); err != nil {
			t.Fatal(err)
		}
		if cl := fetchSeqs(t, ca, 1); len(cl) != 1 {
			t.Fatalf("fetch = %v", cl)
		}
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
			t.Fatal(err)
		}
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 1, Delay: time.Hour}); err != nil {
			t.Fatal(err)
		}
		var parkedAt int64
		xerr, yerr := stealDuringDrops(t, trap, top, other, Publish[int64]{Payload: 3, Size: 2},
			func() { parkedAt = a.st.dropped.Load() })
		if parkedAt != 1 {
			t.Fatalf("the publication was held after %d drops, want 1: the test no longer reaches the window", parkedAt)
		}
		if as := a.Inspect(); xerr != nil || as.Published != 4 || as.Dropped != 2 {
			t.Fatalf("the publication answered %v with the lane %+v, want it admitted for the two records "+
				"the lane dropped: another topic took the room a drop made", xerr, as)
		}
		if !errors.Is(yerr, errcode.ErrQueueFull) {
			t.Fatalf("the other topic's publication = %v, want ErrQueueFull: the room was the "+
				"displacing publication's", yerr)
		}
		if _, bytes, _ := e.Totals(); bytes != 3 {
			t.Fatalf("the engine holds %d bytes, want 3", bytes)
		}
	})
	for _, tc := range []struct {
		fan FanoutPolicy
		ov  OverflowPolicy
	}{
		{Atomic, OverflowDropOldest},
		{DropSlow, OverflowReject},
	} {
		t.Run("logcursor/"+tc.fan.String()+"/"+tc.ov.String(), func(t *testing.T) {
			// MaxTotalMessages 4 on a shared log: a holds d at its cursor,
			// b holds r on a lease and d in its own delayed structure, and
			// the other topic holds one record. The publication needs a
			// record for each subscription, which their drops of d make
			// (makeRoomShared).
			trap := newClockTrap(clock.Real{}, "(*Topic[", ").makeRoomShared")
			e, err := New(Config[int64]{
				Kinds: KindSet{Arch: LogCursor}, MaxTotalMessages: 4, Clock: trap,
				Clone: func(v int64) int64 { return v },
				DefaultTopology: TopologyConfig{
					Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: tc.ov, Fanout: tc.fan,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			top, subs, cs := fanTopic(t, e, SubscriptionConfig{}, SubscriptionConfig{})
			other := otherTopic(t, e)
			if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
				t.Fatal(err)
			}
			if err := top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 1, Delay: time.Hour}); err != nil {
				t.Fatal(err)
			}
			if cl := fetchSeqs(t, cs[1], 1); len(cl) != 1 {
				t.Fatalf("fetch = %v", cl)
			}
			// The fetch that finds d not due moves it into b's delayed
			// structure, from where taking it reads the clock.
			if cl := fetchSeqs(t, cs[1], 1); len(cl) != 0 {
				t.Fatalf("fetched %v before its due time", cl)
			}
			cl := fetchSeqs(t, cs[0], 1)
			if len(cl) != 1 {
				t.Fatalf("fetch = %v", cl)
			}
			if err := cs[0].Ack(cl[0].Env, cl[0].Gen); err != nil {
				t.Fatal(err)
			}
			if err := other.Publish(t.Context(), Publish[int64]{Payload: 98, Size: 1}); err != nil {
				t.Fatal(err)
			}
			var parkedAt int64
			xerr, yerr := stealDuringDrops(t, trap, top, other, Publish[int64]{Payload: 3, Size: 1},
				func() { parkedAt = subs[0].st.dropped.Load() + subs[1].st.dropped.Load() })
			if parkedAt != 1 {
				t.Fatalf("the publication was held after %d drops, want 1: the test no longer reaches the window", parkedAt)
			}
			as, bs := subs[0].Inspect(), subs[1].Inspect()
			if xerr != nil || as.Published != 3 || bs.Published != 3 || as.Dropped != 1 || bs.Dropped != 1 {
				t.Fatalf("the publication answered %v with a %+v and b %+v, want it admitted for the "+
					"record each dropped: another topic took the room a drop made", xerr, as, bs)
			}
			if !errors.Is(yerr, errcode.ErrQueueFull) {
				t.Fatalf("the other topic's publication = %v, want ErrQueueFull: the room was the "+
					"displacing publication's", yerr)
			}
			if msgs, _, _ := e.Totals(); msgs != 4 {
				t.Fatalf("the engine holds %d records, want 4", msgs)
			}
		})
	}
}

// TestAPassGivesBackTheRoomItKeptBeforeTheReleaseCallbacks: an Atomic
// DropOldest pass drops one record per lane per round, so with one record
// free and two subscriptions it keeps three records' room and books two. The
// one left over goes back to the engine at the end of the pass, before the
// publication runs the displaced records' Release callbacks (Topic.after),
// which are the caller's code on the publishing goroutine. A one-record
// publication of another topic made from that callback is admitted.
func TestAPassGivesBackTheRoomItKeptBeforeTheReleaseCallbacks(t *testing.T) {
	e, err := New(Config[int64]{
		Kinds: KindSet{Arch: PerSubEnvelopes}, MaxTotalMessages: 5,
		Clone: func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{
			Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowDropOldest, Fanout: Atomic,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	top, subs, _ := fanTopic(t, e, SubscriptionConfig{}, SubscriptionConfig{})
	other := otherTopic(t, e)
	var (
		calls    int
		msgsSeen int64
		otherErr error
	)
	release := func(v int64) {
		if v != 1 {
			return
		}
		calls++
		if calls > 1 {
			return
		}
		msgsSeen, _, _ = e.Totals()
		otherErr = other.Publish(context.Background(), Publish[int64]{Payload: 99, Size: 1})
	}
	ctx := context.Background()
	for i := int64(1); i <= 2; i++ {
		if err := top.Publish(ctx, Publish[int64]{Payload: i, Size: 1, Release: release}); err != nil {
			t.Fatal(err)
		}
	}
	if msgs, _, _ := e.Totals(); msgs != 4 {
		t.Fatalf("the engine holds %d records, want 4", msgs)
	}
	if err := top.Publish(ctx, Publish[int64]{Payload: 3, Size: 1}); err != nil {
		t.Fatalf("the displacing publication: %v", err)
	}
	if calls == 0 {
		t.Fatal("the displaced record's Release never ran")
	}
	if msgsSeen != 4 || otherErr != nil {
		t.Fatalf("inside the displaced record's Release the engine held %d records (want 4) and a one-record "+
			"publication of another topic answered %v (want nil): the pass still held the room it kept and "+
			"did not need", msgsSeen, otherErr)
	}
	if as, bs := subs[0].Inspect(), subs[1].Inspect(); as.Dropped != 1 || bs.Dropped != 1 {
		t.Fatalf("a %+v b %+v, want one drop each", as, bs)
	}
}

// waitBlockedOnMutexIn polls every goroutine's stack until one is waiting on
// a mutex with fn on its stack, which is the only sign a test has that a
// goroutine has reached a lock another holds.
func waitBlockedOnMutexIn(t *testing.T, fn string) {
	t.Helper()
	buf := make([]byte, 1<<20)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		for g := range strings.SplitSeq(string(buf[:n]), "\n\n") {
			if strings.Contains(g, fn) && (strings.Contains(g, "Mutex).Lock") || strings.Contains(g, "lockSlow") ||
				strings.Contains(g, "SemacquireMutex")) {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no goroutine blocked on a mutex in %s", fn)
}

// TestAnAtomicPassThatCannotKeepItsRoomDropsNothing: the publication's look
// (fitsTogether) counts a's two records and then waits for b's lane mutex,
// which a Purge of b holds at its clock read. While it waits a consumer
// leases one of a's records, so the look is stale: the look makeRoomTogether
// takes again once the free room is kept fails, and the pass refuses before
// any lane drops. A pass that went on to the lanes had lane a drop its other
// record for its own engine-wide share, and lane b, with nothing to drop,
// then refused the publication with a's record lost.
func TestAnAtomicPassThatCannotKeepItsRoomDropsNothing(t *testing.T) {
	trap := newClockTrap(clock.Real{}, "(*lane[", ").purgeClaim")
	e, err := New(Config[int64]{
		Kinds: KindSet{Arch: PerSubEnvelopes}, MaxTotalMessages: 2, Clock: trap,
		Clone: func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{
			Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowDropOldest, Fanout: Atomic,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	top, err := e.CreateTopic("t", TopologyConfig{Wildcards: true})
	if err != nil {
		t.Fatal(err)
	}
	a, err := top.Subscribe("a", SubscriptionConfig{Filter: ">"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := top.Subscribe("b", SubscriptionConfig{Filter: "p.*"})
	if err != nil {
		t.Fatal(err)
	}
	ca, err := a.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for i, subj := range []string{"q.1", "q.2"} {
		if err := top.Publish(t.Context(), Publish[int64]{Payload: int64(i + 1), Size: 1, Subject: subj}); err != nil {
			t.Fatal(err)
		}
	}
	trap.arm()
	purged := make(chan int, 1)
	go func() { purged <- b.Purge() }()
	select {
	case <-trap.parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the purge never reached its clock read")
	}
	done := make(chan error, 1)
	go func() { done <- top.Publish(t.Context(), Publish[int64]{Payload: 3, Size: 1, Subject: "p.1"}) }()
	waitBlockedOnMutexIn(t, ").fitsTogether")
	cl := fetchSeqs(t, ca, 1)
	trap.let()
	<-purged
	xerr := <-done
	if len(cl) != 1 {
		t.Fatalf("fetch = %v", cl)
	}
	as := a.Inspect()
	msgs, _, _ := e.Totals()
	if !errors.Is(xerr, errcode.ErrQueueFull) || as.Dropped != 0 || msgs != 2 {
		t.Fatalf("the publication answered %v with a %+v and %d records in the engine, want ErrQueueFull, "+
			"nothing dropped and 2 records: the pass dropped a record for a publication it then refused",
			xerr, as, msgs)
	}
}

// TestASharedLogDisplacementKeepsTheFreeRoomItCounted: the engine has one
// record free of the two a shared-log publication needs, and a holds a
// delayed record b never saw. The displacement keeps the free record for the
// publication (keepFreeFor) before a's drop, which the trap holds while
// another topic publishes, through makeRoomShared when the log has room and
// through reclaimBase when the log is full. The free record was booked for
// nobody: the other topic took it, and the publication was refused with a's
// record already dropped.
func TestASharedLogDisplacementKeepsTheFreeRoomItCounted(t *testing.T) {
	for _, tc := range []struct {
		name      string
		trap      string
		logRecord int // the log's record capacity
	}{
		{"log-has-room", ").makeRoomShared", 8},
		{"log-full", ").reclaimBase", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trap := newClockTrap(clock.Real{}, "(*Topic[", tc.trap)
			e, err := New(Config[int64]{
				Kinds: KindSet{Arch: LogCursor}, MaxTotalMessages: 2, Clock: trap,
				Clone: func(v int64) int64 { return v },
				DefaultTopology: TopologyConfig{
					Partitions: 1, CapacityMessages: tc.logRecord, AckDeadline: -1,
					Overflow: OverflowDropOldest, Fanout: Atomic,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			top, err := e.CreateTopic("t", TopologyConfig{})
			if err != nil {
				t.Fatal(err)
			}
			subscribe := func(name string) (*Subscription[int64], *Consumer[int64]) {
				sub, err := top.Subscribe(name, SubscriptionConfig{})
				if err != nil {
					t.Fatal(err)
				}
				c, err := sub.Attach(ConsumerConfig{})
				if err != nil {
					t.Fatal(err)
				}
				return sub, c
			}
			a, ca := subscribe("a")
			if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 1, Delay: time.Hour}); err != nil {
				t.Fatal(err)
			}
			// The fetch that finds the record not due moves it into a's
			// delayed structure, from where taking it reads the clock.
			if cl := fetchSeqs(t, ca, 1); len(cl) != 0 {
				t.Fatalf("fetched %v before its due time", cl)
			}
			b, _ := subscribe("b")
			other := otherTopic(t, e)
			var parkedAt int64
			xerr, yerr := stealDuringDrops(t, trap, top, other, Publish[int64]{Payload: 3, Size: 1},
				func() { parkedAt = a.st.dropped.Load() + b.st.dropped.Load() })
			if parkedAt != 0 {
				t.Fatalf("the publication was held after %d drops, want 0: the test no longer reaches the window", parkedAt)
			}
			as, bs := a.Inspect(), b.Inspect()
			msgs, _, _ := e.Totals()
			if xerr != nil || !errors.Is(yerr, errcode.ErrQueueFull) || as.Dropped != 1 || bs.Published != 1 || msgs != 2 {
				t.Fatalf("the publication answered %v and the other topic's %v, with a %+v, b %+v and %d records: "+
					"want the publication admitted for the record a dropped and the free record it counted on",
					xerr, yerr, as, bs, msgs)
			}
		})
	}
}
