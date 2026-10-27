package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

func TestOverflowPolicyProperties(t *testing.T) {
	if len(OverflowPolicies()) != 4 {
		t.Fatalf("OverflowPolicies() = %v", OverflowPolicies())
	}
	seen := map[string]bool{}
	for _, o := range OverflowPolicies() {
		if !o.Valid() || o == OverflowDefault || o.Resolve() != o || o.Ident() == "" {
			t.Fatalf("%v: basic properties", o)
		}
		if seen[o.String()] {
			t.Fatalf("duplicate slug %q", o)
		}
		seen[o.String()] = true
		if p, ok := ParseOverflowPolicy(o.String()); !ok || p != o {
			t.Fatalf("ParseOverflowPolicy(%q) = %v %v", o, p, ok)
		}
	}
	if OverflowDefault.Resolve() != OverflowReject {
		t.Fatalf("OverflowDefault resolves to %v", OverflowDefault.Resolve())
	}
	if !OverflowBlock.Blocks() || OverflowReject.Blocks() || OverflowDefault.Blocks() {
		t.Fatal("Blocks")
	}
	if OverflowPolicy(99).Valid() || OverflowPolicy(99).String() != "overflow(?)" ||
		OverflowPolicy(99).Ident() != "" {
		t.Fatal("invalid policy")
	}
	if _, ok := ParseOverflowPolicy("nope"); ok {
		t.Fatal("ParseOverflowPolicy junk")
	}
	// The slugs are the ones the performance budgets gives the benchmark arms.
	if got := strings.Join([]string{
		OverflowBlock.String(), OverflowReject.String(),
		OverflowDropNewest.String(), OverflowDropOldest.String(),
	}, ","); got != "block,reject,dropnew,dropold" {
		t.Fatalf("benchmark slugs = %q", got)
	}
}

// TestOverflowPolicies fills a lane and checks what each policy does with the
// publication that finds no room, and how it accounts for it.
func TestOverflowPolicies(t *testing.T) {
	const capacity = 2
	for _, o := range []OverflowPolicy{OverflowReject, OverflowDropNewest, OverflowDropOldest} {
		t.Run(o.String(), func(t *testing.T) {
			e := newTestEngine(t, KindSet{}, TopologyConfig{
				Partitions: 1, CapacityMessages: capacity, AckDeadline: -1, Overflow: o,
			})
			top, sub, c := queueSub(t, e)
			released := map[int64]int{}
			pub := func(v int64) error {
				return top.Publish(t.Context(), Publish[int64]{
					Payload: v, Size: 4, Release: func(x int64) { released[x]++ },
				})
			}
			for i := range capacity {
				if err := pub(int64(i + 1)); err != nil {
					t.Fatalf("publish %d: %v", i, err)
				}
			}
			err := pub(99)
			snap := sub.Inspect()
			switch o { //nolint:exhaustive // the policies this table covers; Block has its own tests
			case OverflowReject:
				if !errors.Is(err, errcode.ErrQueueFull) {
					t.Fatalf("publish into a full lane: %v", err)
				}
				if snap.Rejected != 1 || snap.Overflowed != 0 || snap.Dropped != 0 {
					t.Fatalf("%+v", snap)
				}
				if released[99] != 0 {
					t.Fatal("a refused publication released the caller's payload")
				}
			case OverflowDropNewest:
				if err != nil {
					t.Fatalf("publish into a full lane: %v", err)
				}
				if snap.Overflowed != 1 || snap.Rejected != 0 || snap.Dropped != 0 || snap.Published != capacity {
					t.Fatalf("%+v", snap)
				}
				if released[99] != 1 {
					t.Fatalf("a discarded publication released %d times", released[99])
				}
			case OverflowDropOldest:
				if err != nil {
					t.Fatalf("publish into a full lane: %v", err)
				}
				if snap.Dropped != 1 || snap.Overflowed != 0 || snap.Published != capacity+1 {
					t.Fatalf("%+v", snap)
				}
				if released[1] != 1 {
					t.Fatalf("the displaced record released %d times", released[1])
				}
			}
			if snap.Ready != capacity || !snap.Conserved() {
				t.Fatalf("%+v", snap)
			}
			// Whatever the policy did, the lane still delivers what it holds.
			got := fetchSeqs(t, c, capacity+1)
			if len(got) != capacity {
				t.Fatalf("fetched %v", seqs(got))
			}
			if o == OverflowDropOldest && got[0].Payload != 2 {
				t.Fatalf("the displaced record is still in the lane: %v", got[0].Payload)
			}
		})
	}
}

// TestOverflowDropOldestFreesBytes: a byte-bounded lane displaces as many of
// its oldest records as the new one needs, and no more.
func TestOverflowDropOldestFreesBytes(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{
		Partitions: 1, CapacityMessages: 8, CapacityBytes: 8, AckDeadline: -1,
		Overflow: OverflowDropOldest,
	})
	top, sub, _ := queueSub(t, e)
	for range 4 {
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 2}); err != nil {
			t.Fatal(err)
		}
	}
	if snap := sub.Inspect(); snap.Ready != 4 || snap.Bytes != 8 {
		t.Fatalf("%+v", snap)
	}
	if err := top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 6}); err != nil {
		t.Fatal(err)
	}
	snap := sub.Inspect()
	if snap.Dropped != 3 || snap.Ready != 2 || snap.Bytes != 8 {
		t.Fatalf("%+v", snap)
	}
	// A record the lane could never hold displaces nothing.
	err := top.Publish(t.Context(), Publish[int64]{Payload: 3, Size: 9})
	if !errors.Is(err, errcode.ErrQueueFull) {
		t.Fatalf("oversized publication: %v", err)
	}
	if snap := sub.Inspect(); snap.Dropped != 3 || snap.Ready != 2 {
		t.Fatalf("an oversized publication emptied the lane: %+v", snap)
	}
}

// TestOverflowDropOldestSparesLeasedBytes: a byte-bounded lane whose leases
// alone leave no room for a record refuses it without dropping anything:
// the ready records it could give up would not make room, so dropping them
// loses them for a refusal.
func TestOverflowDropOldestSparesLeasedBytes(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{
		Partitions: 1, CapacityMessages: 8, CapacityBytes: 8, AckDeadline: -1,
		Overflow: OverflowDropOldest,
	})
	top, sub, c := queueSub(t, e)
	if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 6}); err != nil {
		t.Fatal(err)
	}
	if cl := fetchSeqs(t, c, 1); len(cl) != 1 {
		t.Fatalf("fetch = %v", cl)
	}
	if err := top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 2}); err != nil {
		t.Fatal(err)
	}
	err := top.Publish(t.Context(), Publish[int64]{Payload: 3, Size: 4})
	if !errors.Is(err, errcode.ErrQueueFull) {
		t.Fatalf("publication behind the leases: %v", err)
	}
	if snap := sub.Inspect(); snap.Dropped != 0 || snap.Ready != 1 || snap.InFlight != 1 {
		t.Fatalf("a refused publication displaced records: %+v", snap)
	}
}

// TestOverflowDropOldestAtomicRefusalDropsNothing: under the Atomic fan-out
// policy one subscription's refusal refuses the publication for all of
// them, so a subscription that could make room must not drop its oldest
// record for a publication another one refuses.
func TestOverflowDropOldestAtomicRefusalDropsNothing(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{
		Partitions: 1, CapacityMessages: 1, AckDeadline: -1,
		Fanout: Atomic, Overflow: OverflowDropOldest,
	})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var subs [2]*Subscription[int64]
	for i, name := range []string{"a", "b"} {
		if subs[i], err = top.Subscribe(name, SubscriptionConfig{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
		t.Fatal(err)
	}
	// b's only record is leased, so b has nothing to drop.
	c, err := subs[1].Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if cl := fetchSeqs(t, c, 1); len(cl) != 1 {
		t.Fatalf("fetch = %v", cl)
	}
	err = top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 1})
	if !errors.Is(err, errcode.ErrQueueFull) {
		t.Fatalf("publication b has no room for: %v", err)
	}
	if snap := subs[0].Inspect(); snap.Dropped != 0 || snap.Ready != 1 {
		t.Fatalf("a dropped its record for a refused publication: %+v", snap)
	}
}

// TestOverflowBlockWaitsForRoom: a publisher under OverflowBlock parks until
// a record leaves the lane, and is released by the settlement that makes room
// rather than by polling.
func TestOverflowBlockWaitsForRoom(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{
			Partitions: 1, CapacityMessages: 1, AckDeadline: -1, Overflow: OverflowBlock,
		})
		top, sub, c := queueSub(t, e)
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 4}); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- top.Publish(context.Background(), Publish[int64]{Payload: 2, Size: 4}) }()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("the publisher did not park: %v", err)
		default:
		}
		if snap := sub.Inspect(); snap.Ready != 1 || snap.Rejected != 0 || snap.Overflowed != 0 {
			t.Fatalf("while the publisher waits: %+v", snap)
		}
		cl := fetchSeqs(t, c, 1)
		if len(cl) != 1 {
			t.Fatalf("fetch = %v", cl)
		}
		if err := c.Ack(cl[0].Env, cl[0].Gen); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatalf("the parked publication failed: %v", err)
		}
		if snap := sub.Inspect(); snap.Published != 2 || snap.Ready != 1 {
			t.Fatalf("%+v", snap)
		}
	})
}

// TestOverflowBlockHonoursContext: a parked publisher leaves on its context,
// and on the engine closing under it.
func TestOverflowBlockHonoursContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{
			Partitions: 1, CapacityMessages: 1, AckDeadline: -1, Overflow: OverflowBlock,
		})
		top, _, _ := queueSub(t, e)
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 4}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- top.Publish(ctx, Publish[int64]{Payload: 2, Size: 4}) }()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled publisher: %v", err)
		}
		closing := make(chan error, 1)
		go func() { closing <- top.Publish(context.Background(), Publish[int64]{Payload: 3, Size: 4}) }()
		synctest.Wait()
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-closing; !errors.Is(err, errcode.ErrClosed) {
			t.Fatalf("publisher parked across a close: %v", err)
		}
	})
}

// TestOverflowBlockRefusesTheImpossible: a record no lane could ever hold is
// refused rather than parked for ever.
func TestOverflowBlockRefusesTheImpossible(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{
		Partitions: 1, CapacityMessages: 4, CapacityBytes: 8, AckDeadline: -1,
		Overflow: OverflowBlock,
	})
	top, _, _ := queueSub(t, e)
	if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 8}); err != nil {
		t.Fatal(err)
	}
	if err := top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 9}); !errors.Is(err, errcode.ErrQueueFull) {
		t.Fatalf("oversized publication under OverflowBlock: %v", err)
	}
}

// TestOverflowBlockKeepsTheSubjectFilter: a publisher parked under
// OverflowBlock on a wildcard topic re-reads the subscription set when it is
// woken, and the record it admits then still reaches only the subscriptions
// whose filter matches its subject. The retry after the wait is an admission
// like any other, so the filter applies to it too
// (adr/ADR-017-assumptions.md); an unfiltered re-read hands a
// subscription a record it never subscribed to.
//
// OverflowBlock is deliberately outside the differential — a parked publisher
// would stop the op-script that drives it — so the combination of wildcards
// and blocking has no other cover than this test.
func TestOverflowBlockKeepsTheSubjectFilter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{
			Partitions: 1, CapacityMessages: 1, AckDeadline: -1, Overflow: OverflowBlock,
		})
		top, err := e.CreateTopic("t", TopologyConfig{Wildcards: true})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		sa, err := top.Subscribe("a", SubscriptionConfig{Filter: "a.*"})
		if err != nil {
			t.Fatalf("Subscribe(a): %v", err)
		}
		sb, err := top.Subscribe("b", SubscriptionConfig{Filter: "b.*"})
		if err != nil {
			t.Fatalf("Subscribe(b): %v", err)
		}
		ca, err := sa.Attach(ConsumerConfig{})
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 1, Subject: "a.one"}); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			done <- top.Publish(context.Background(), Publish[int64]{Payload: 2, Size: 1, Subject: "a.two"})
		}()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("the publisher did not park on the full a.* lane: %v", err)
		default:
		}
		cl := fetchSeqs(t, ca, 1)
		if len(cl) != 1 {
			t.Fatalf("fetch = %v", cl)
		}
		if err := ca.Ack(cl[0].Env, cl[0].Gen); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatalf("the parked publication failed: %v", err)
		}
		if snap := sb.Inspect(); snap.Published != 0 || snap.Ready != 0 {
			t.Fatalf("subscription b (filter %q) was handed the records published on a.*: %+v", "b.*", snap)
		}
		if snap := sa.Inspect(); snap.Published != 2 || snap.Ready != 1 {
			t.Fatalf("subscription a (filter %q) after the wait: %+v", "a.*", snap)
		}
	})
}

// TestDeliverAt: a publication scheduled for an absolute time is not
// delivered before it, and one whose time has passed is delivered at once.
func TestDeliverAt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{
			Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowReject,
		})
		top, sub, c := queueSub(t, e)
		at := e.Clock().Now().Add(20 * time.Millisecond)
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 4, DeliverAt: at}); err != nil {
			t.Fatal(err)
		}
		// A DeliverAt in the past is due now, and beats a Delay that is also
		// set: DeliverAt is the absolute form of the same field.
		past := e.Clock().Now().Add(-time.Second)
		if err := top.Publish(t.Context(), Publish[int64]{
			Payload: 2, Size: 4, DeliverAt: past, Delay: time.Hour,
		}); err != nil {
			t.Fatal(err)
		}
		if snap := sub.Inspect(); snap.Delayed != 1 || snap.Ready != 1 {
			t.Fatalf("%+v", snap)
		}
		got := fetchSeqs(t, c, 4)
		if len(got) != 1 || got[0].Payload != 2 {
			t.Fatalf("before the due time, fetch = %v", got)
		}
		if err := c.Ack(got[0].Env, got[0].Gen); err != nil {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
		synctest.Wait()
		got = fetchSeqs(t, c, 4)
		if len(got) != 1 || got[0].Payload != 1 {
			t.Fatalf("after the due time, fetch = %v", got)
		}
	})
}

// FuzzEngineOverflow is the differential over the overflow policies: a
// fuzz-chosen policy, capacity and script are applied to the engine and to
// internal/model/memq, which checks that every discarded record is accounted
// for (OverflowAccounted) and that the capacity is never exceeded.
func FuzzEngineOverflow(f *testing.F) {
	sc := engineScript()
	for _, seed := range overflowSeeds(f, sc) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		ks := referenceKinds()
		ks.RetryLane = RetryLanes()[r.Choice(len(RetryLanes()))]
		cfg := harnessConfig{
			kinds:       ks,
			partitions:  r.Int(1, 2),
			capMessages: r.Int(1, 4),
			capBytes:    int64(r.Int(0, 64)),
			ackDeadline: time.Duration(r.Int(1, 20)) * tick,
			maxTotal:    r.Int(0, 16),
			overflow:    scriptOverflows()[r.Choice(len(scriptOverflows()))],
		}
		rest := data[len(data)-r.Remaining():]
		t.Logf("engine %v partitions=%d capacity=%d bytes=%d overflow=%v total=%d",
			ks.Resolve(), cfg.partitions, cfg.capMessages, cfg.capBytes, cfg.overflow, cfg.maxTotal)
		synctest.Test(t, func(t *testing.T) {
			h := newHarness(t, cfg)
			defer h.close()
			sc.Run(t, rest, h)
			h.check()
		})
	})
}

// overflowSeeds writes readable scripts for FuzzEngineOverflow: one per
// policy, each filling a lane past its capacity and then draining it.
func overflowSeeds(tb testing.TB, sc *fuzzutil.Script[*harness]) [][]byte {
	seeds := make([][]byte, 0, len(scriptOverflows()))
	for policy := range scriptOverflows() {
		w := fuzzutil.NewWriter()
		w.Choice(len(RetryLanes()), 0)
		w.Int(1, 2, 1)
		w.Int(1, 4, 2)
		w.Int(0, 64, 16)
		w.Int(1, 20, 8)
		w.Int(0, 16, 8)
		w.Choice(len(scriptOverflows()), policy)
		sc.WriteStep(tb, w, "subscribe")
		w.Choice(maxSubs, 0)
		sc.WriteStep(tb, w, "attach")
		w.Choice(maxSubs, 0)
		for range 6 {
			sc.WriteStep(tb, w, "publish")
			w.Int(-1, 2, 0)
			w.Int(0, 32, 4)
		}
		sc.WriteStep(tb, w, "fetch")
		w.Choice(maxSubs, 0)
		w.Choice(maxConsumers, 0)
		w.Int(1, maxFetch, 2)
		sc.WriteStep(tb, w, "ack")
		w.Choice(maxHandles, 0)
		writeDelta(w, 0)
		for range 2 {
			sc.WriteStep(tb, w, "publish")
			w.Int(-1, 2, 0)
			w.Int(0, 32, 4)
		}
		seeds = append(seeds, w.Data())
	}
	return seeds
}

// sharedTopic builds a topic on the shared partition log (Arch: LogCursor)
// with two subscriptions and one consumer each. One slot carries the record
// for both of them and one reservation admits it for both, so the overflow
// policies act on the log rather than on a lane.
func sharedTopic(t *testing.T, top TopologyConfig) (
	*Topic[int64], []*Subscription[int64], []*Consumer[int64],
) {
	t.Helper()
	e := newTestEngine(t, KindSet{Arch: LogCursor}, top)
	tp, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subs := make([]*Subscription[int64], 2)
	cs := make([]*Consumer[int64], len(subs))
	for i := range subs {
		if subs[i], err = tp.Subscribe(string(rune('a'+i)), SubscriptionConfig{}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if cs[i], err = subs[i].Attach(ConsumerConfig{}); err != nil {
			t.Fatalf("Attach: %v", err)
		}
	}
	return tp, subs, cs
}

// TestOverflowPoliciesOnASharedLog is TestOverflowPolicies over the shared
// partition log: the policies are implemented a second time in publishShared,
// where a full log refuses, discards or displaces for every subscription at
// once and the accounting has to reach all of them.
func TestOverflowPoliciesOnASharedLog(t *testing.T) {
	const capacity = 2
	for _, o := range []OverflowPolicy{OverflowReject, OverflowDropNewest, OverflowDropOldest} {
		t.Run(o.String(), func(t *testing.T) {
			top, subs, cs := sharedTopic(t, TopologyConfig{
				Partitions: 1, CapacityMessages: capacity, AckDeadline: -1, Overflow: o,
			})
			released := map[int64]int{}
			pub := func(v int64) error {
				return top.Publish(t.Context(), Publish[int64]{
					Payload: v, Size: 4, Release: func(x int64) { released[x]++ },
				})
			}
			for i := range capacity {
				if err := pub(int64(i + 1)); err != nil {
					t.Fatalf("publish %d: %v", i, err)
				}
			}
			err := pub(99)
			for i, sub := range subs {
				snap := sub.Inspect()
				switch o { //nolint:exhaustive // the policies this table covers; Block has its own test
				case OverflowReject:
					if !errors.Is(err, errcode.ErrQueueFull) {
						t.Fatalf("publish into a full log: %v", err)
					}
					if snap.Rejected != 1 || snap.Overflowed != 0 || snap.Dropped != 0 {
						t.Fatalf("subscription %d: %+v", i, snap)
					}
				case OverflowDropNewest:
					if err != nil {
						t.Fatalf("publish into a full log: %v", err)
					}
					if snap.Overflowed != 1 || snap.Rejected != 0 || snap.Published != capacity {
						t.Fatalf("subscription %d: %+v", i, snap)
					}
				case OverflowDropOldest:
					if err != nil {
						t.Fatalf("publish into a full log: %v", err)
					}
					if snap.Dropped != 1 || snap.Overflowed != 0 || snap.Published != capacity+1 {
						t.Fatalf("subscription %d: %+v", i, snap)
					}
				}
				if snap.Ready != capacity || !snap.Conserved() {
					t.Fatalf("subscription %d: %+v", i, snap)
				}
			}
			// The payload is released exactly once however many subscriptions
			// read the slot: the record is shared, not copied.
			switch o { //nolint:exhaustive // as above
			case OverflowReject:
				if released[99] != 0 {
					t.Fatal("a refused publication released the caller's payload")
				}
			case OverflowDropNewest:
				if released[99] != 1 {
					t.Fatalf("a discarded publication released %d times", released[99])
				}
			case OverflowDropOldest:
				if released[1] != 1 {
					t.Fatalf("the displaced record released %d times", released[1])
				}
			}
			// Whatever the policy did, every subscription still sees what the
			// log holds.
			for i, c := range cs {
				got := fetchSeqs(t, c, capacity+1)
				if len(got) != capacity {
					t.Fatalf("subscription %d fetched %v", i, seqs(got))
				}
				if o == OverflowDropOldest && got[0].Payload != 2 {
					t.Fatalf("subscription %d still holds the displaced record: %v", i, got[0].Payload)
				}
			}
		})
	}
}

// TestOverflowDropOldestFreesBytesOnASharedLog: a byte-bounded log displaces
// as many of its oldest slots as the new record needs, which takes the
// re-reservation loop rather than one displacement, and refuses a record no
// log could ever hold.
func TestOverflowDropOldestFreesBytesOnASharedLog(t *testing.T) {
	top, subs, _ := sharedTopic(t, TopologyConfig{
		Partitions: 1, CapacityMessages: 8, CapacityBytes: 8, AckDeadline: -1,
		Overflow: OverflowDropOldest,
	})
	for range 4 {
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 2}); err != nil {
			t.Fatal(err)
		}
	}
	if snap := subs[0].Inspect(); snap.Ready != 4 {
		t.Fatalf("%+v", snap)
	}
	if err := top.Publish(t.Context(), Publish[int64]{Payload: 2, Size: 6}); err != nil {
		t.Fatal(err)
	}
	for i, sub := range subs {
		snap := sub.Inspect()
		if snap.Dropped != 3 || snap.Ready != 2 || !snap.Conserved() {
			t.Fatalf("subscription %d: %+v", i, snap)
		}
	}
	// A record the log could never hold displaces nothing.
	err := top.Publish(t.Context(), Publish[int64]{Payload: 3, Size: 9})
	if !errors.Is(err, errcode.ErrQueueFull) {
		t.Fatalf("oversized publication: %v", err)
	}
	if snap := subs[0].Inspect(); snap.Dropped != 3 || snap.Ready != 2 {
		t.Fatalf("an oversized publication emptied the log: %+v", snap)
	}
}

// TestOverflowBlockWaitsForRoomOnASharedLog: under OverflowBlock a publisher
// waits for a slot of the shared log, and a slot is reclaimed only once every
// subscription has settled the record it holds.
//
// Deliberately not a synctest bubble. What this test exists to catch is a
// publisher that never leaves the wait, and the shape that has already taken
// twice is a busy retry rather than a park — a bubble none of whose
// goroutines block durably makes synctest.Wait wait for ever, so the whole
// package dies of the test binary's timeout instead of naming the defect.
// Every wait below is bounded and says what it was waiting for; the park
// itself is still observed exactly, on the gate the publisher registers on
// rather than after a sleep.
func TestOverflowBlockWaitsForRoomOnASharedLog(t *testing.T) {
	top, subs, cs := sharedTopic(t, TopologyConfig{
		Partitions: 1, CapacityMessages: 1, AckDeadline: -1, Overflow: OverflowBlock,
	})
	if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 4}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- top.Publish(ctx, Publish[int64]{Payload: 2, Size: 4}) }()
	ring := top.rings[0]
	if !awaitTrue(10*time.Second, func() bool { return ring.room.waiting.Load() > 0 }) {
		t.Fatal("the publisher never parked on the full log: it is retrying rather than waiting")
	}
	select {
	case err := <-done:
		t.Fatalf("the publisher did not park although the log was full: %v", err)
	default:
	}
	// One subscription settling is not enough: the slot is still held by
	// the other cursor.
	cl := fetchOne(t, cs[0], 10*time.Second)
	if err := cs[0].Ack(cl.Env, cl.Gen); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("the publisher left while the slot was still held by the other cursor: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cl = fetchOne(t, cs[1], 10*time.Second)
	if err := cs[1].Ack(cl.Env, cl.Gen); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the parked publication failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the publisher was not woken by the settlement that reclaimed the last reference to the slot")
	}
	for i, sub := range subs {
		if snap := sub.Inspect(); snap.Published != 2 || snap.Ready != 1 {
			t.Fatalf("subscription %d: %+v", i, snap)
		}
	}
}

// TestOverflowBlockOnASharedLogHonoursContext: a publisher parked on the
// shared log leaves on its context and on the engine closing under it, and a
// record no log could ever hold is refused rather than parked for ever.
func TestOverflowBlockOnASharedLogHonoursContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		top, _, _ := sharedTopic(t, TopologyConfig{
			Partitions: 1, CapacityMessages: 1, CapacityBytes: 8, AckDeadline: -1,
			Overflow: OverflowBlock,
		})
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 4}); err != nil {
			t.Fatal(err)
		}
		if err := top.Publish(t.Context(), Publish[int64]{
			Payload: 2, Size: 9,
		}); !errors.Is(err, errcode.ErrQueueFull) {
			t.Fatalf("oversized publication under OverflowBlock: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- top.Publish(ctx, Publish[int64]{Payload: 3, Size: 4}) }()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled publisher: %v", err)
		}
		closing := make(chan error, 1)
		go func() { closing <- top.Publish(context.Background(), Publish[int64]{Payload: 4, Size: 4}) }()
		synctest.Wait()
		if err := top.eng.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-closing; !errors.Is(err, errcode.ErrClosed) {
			t.Fatalf("publisher parked across a close: %v", err)
		}
	})
}

// sharedBlockEngine builds a LogCursor engine under OverflowBlock whose
// engine-wide record limit is the binding one: the logs are roomy, so a
// publication is refused by Config.MaxTotalMessages rather than by a log's
// own capacity, which is the half of the shared-log wait's condition that
// only a settlement elsewhere can satisfy.
func sharedBlockEngine(t *testing.T, maxTotal int) *Engine[int64] {
	t.Helper()
	e, err := New(Config[int64]{
		Kinds:            KindSet{Arch: LogCursor},
		MaxTotalMessages: maxTotal,
		Clone:            func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{
			Partitions: 1, CapacityMessages: 16, AckDeadline: -1, Overflow: OverflowBlock,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// blockSub gives a topic of e one subscription and one consumer.
func blockSub(t *testing.T, e *Engine[int64], name string) (*Topic[int64], *Consumer[int64]) {
	t.Helper()
	top, err := e.CreateTopic(name, TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic(%s): %v", name, err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	return top, c
}

// awaitTrue polls cond until it holds and reports whether it did within d. It
// is how a test waits for a goroutine to park without a sleep long enough to
// hide the answer.
func awaitTrue(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			return cond()
		}
		time.Sleep(50 * time.Microsecond)
	}
	return true
}

// fetchOne claims exactly one record for c, waiting up to d for it.
func fetchOne(t *testing.T, c *Consumer[int64], d time.Duration) Claim[int64] {
	t.Helper()
	var cl []Claim[int64]
	if !awaitTrue(d, func() bool {
		cl = fetchSeqs(t, c, 1)
		return len(cl) == 1
	}) {
		t.Fatalf("nothing to claim after %v", d)
	}
	return cl[0]
}

// TestOverflowBlockOnASharedLogWakesOnEngineWideRoom: a publisher parked on a
// shared log waits on the whole reservation — the log's own capacity and the
// engine-wide limit together (logRing.hasRoom) — so every release of either
// half has to be a wake, after the budget it returns is actually back.
//
// Neither half of this is the log's own gate. The engine-wide record a
// parked publisher is waiting for may be returned by a settlement of another
// topic altogether, which notifies no gate of this log; and on this very log
// lane.discard reclaims the slot — notifying the log's gate — three
// statements before it returns the engine-wide record, so a publisher woken
// in that window finds the reservation still refused and parks again. Both
// arms strand for ever on a gate only a settlement of this log can close.
func TestOverflowBlockOnASharedLogWakesOnEngineWideRoom(t *testing.T) {
	t.Run("othertopic", func(t *testing.T) {
		e := sharedBlockEngine(t, 2)
		ta, _ := blockSub(t, e, "a")
		tb, cb := blockSub(t, e, "b")
		for i, top := range []*Topic[int64]{ta, tb} {
			if err := top.Publish(t.Context(), Publish[int64]{Payload: int64(i + 1), Size: 4}); err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
		}
		// The engine-wide limit is spent although both logs are nearly empty.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- ta.Publish(ctx, Publish[int64]{Payload: 99, Size: 4}) }()
		ring := ta.rings[0]
		if !awaitTrue(10*time.Second, func() bool { return ring.room.waiting.Load() > 0 }) {
			t.Fatal("the publisher never parked on the log it could not reserve a slot of")
		}
		select {
		case err := <-done:
			t.Fatalf("the publisher did not park although the engine-wide limit was spent: %v", err)
		default:
		}
		// A settlement of the other topic returns the engine-wide record the
		// parked publisher is waiting for. Nothing settles on its own log.
		cl := fetchOne(t, cb, 10*time.Second)
		if err := cb.Ack(cl.Env, cl.Gen); err != nil {
			t.Fatalf("ack: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("the parked publication failed: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the publisher parked for ever although another topic returned the engine-wide record it was waiting for")
		}
	})
	t.Run("samelog", func(t *testing.T) {
		// The ack races the publisher's park, which is what puts the
		// publisher between the settlement's notify and the release of the
		// engine-wide record it is waiting for.
		const rounds = 3000
		e := sharedBlockEngine(t, 1)
		top, c := blockSub(t, e, "t")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := top.Publish(ctx, Publish[int64]{Payload: 0, Size: 4}); err != nil {
			t.Fatalf("publish: %v", err)
		}
		for r := range rounds {
			done := make(chan error, 1)
			go func() { done <- top.Publish(ctx, Publish[int64]{Payload: int64(r + 1), Size: 4}) }()
			cl := fetchOne(t, c, 10*time.Second)
			if err := c.Ack(cl.Env, cl.Gen); err != nil {
				t.Fatalf("round %d: ack: %v", r, err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("round %d: the parked publication failed: %v", r, err)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("round %d: the publisher parked for ever after the ack on its own log returned the engine-wide record it was waiting for", r)
			}
		}
	})
}

// TestOverflowBlockOnASharedLogRefusesAReservationThatNeverFits: a
// publication to a shared log books one engine-wide record and its bytes for
// every subscription that will read the slot, so a topic with more
// subscriptions than the engine-wide limit can hold — or whose record costs
// more bytes once multiplied — is refused rather than parked on an idle
// engine for a reservation nothing can satisfy.
func TestOverflowBlockOnASharedLogRefusesAReservationThatNeverFits(t *testing.T) {
	for _, tc := range []struct {
		name     string
		maxMsgs  int
		maxBytes int64
	}{
		{"records", 1, 0},
		{"bytes", 0, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, err := New(Config[int64]{
				Kinds:            KindSet{Arch: LogCursor},
				MaxTotalMessages: tc.maxMsgs,
				MaxTotalBytes:    tc.maxBytes,
				Clone:            func(v int64) int64 { return v },
				DefaultTopology: TopologyConfig{
					Partitions: 1, CapacityMessages: 16, AckDeadline: -1, Overflow: OverflowBlock,
				},
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = e.Close() })
			top, err := e.CreateTopic("t", TopologyConfig{})
			if err != nil {
				t.Fatalf("CreateTopic: %v", err)
			}
			// Two subscriptions read every slot, so one publication of four
			// bytes costs two engine-wide records and eight bytes: more than
			// either limit allows, on an engine holding nothing at all.
			for i := range 2 {
				if _, err := top.Subscribe(string(rune('a'+i)), SubscriptionConfig{}); err != nil {
					t.Fatalf("Subscribe: %v", err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- top.Publish(ctx, Publish[int64]{Payload: 1, Size: 4}) }()
			select {
			case err := <-done:
				if !errors.Is(err, errcode.ErrQueueFull) {
					t.Fatalf("publication that can never fit: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the publisher parked on an idle engine for a reservation that can never succeed")
			}
		})
	}
}

// TestOverflowDropOldestOnEveryQueueKind: the displacing policy is covered
// for every ready-queue kind by what it accounts for rather than
// by which record it named. A kind that does not order what it holds does not
// promise which of its records is the oldest, which is why the differential
// harness runs DropOldest on the ordered kinds only (newHarness) — the direct
// coverage that replaces it is this: whatever record goes, one goes per
// record admitted, the gauges follow it, it is released exactly once, and the
// lane still delivers exactly what it kept.
func TestOverflowDropOldestOnEveryQueueKind(t *testing.T) {
	for _, k := range QueueKinds() {
		t.Run(k.String(), func(t *testing.T) {
			const capacity = 2
			e := newTestEngine(t, KindSet{Queue: k}, TopologyConfig{
				Partitions: 1, CapacityMessages: capacity, CapacityBytes: 8, AckDeadline: -1,
				Overflow: OverflowDropOldest,
			})
			top, sub, c := queueSub(t, e)
			released := map[int64]int{}
			pub := func(v int64) error {
				return top.Publish(t.Context(), Publish[int64]{
					Payload: v, Size: 4, Release: func(x int64) { released[x]++ },
				})
			}
			for i := range capacity + 1 {
				if err := pub(int64(i + 1)); err != nil {
					t.Fatalf("publish %d: %v", i, err)
				}
			}
			snap := sub.Inspect()
			if snap.Dropped != 1 || snap.Overflowed != 0 || snap.Rejected != 0 ||
				snap.Published != capacity+1 || snap.Ready != capacity || snap.Bytes != 8 ||
				!snap.Conserved() {
				t.Fatalf("%+v", snap)
			}
			// The displaced record is released exactly once and it is one of
			// the records the lane held, whichever one the kind gave up.
			gone := int64(0)
			for v, n := range released {
				if n != 1 {
					t.Fatalf("payload %d released %d times", v, n)
				}
				if gone != 0 {
					t.Fatalf("a single displacement released both %d and %d", gone, v)
				}
				gone = v
			}
			if gone == 0 {
				t.Fatal("nothing was released although a record was displaced")
			}
			if gone == capacity+1 {
				t.Fatal("the publication that made the lane overflow is the one that was displaced")
			}
			// The lane delivers what it kept: the records it still holds are
			// the published ones bar the displaced one, each of them once.
			seen := map[int64]bool{}
			for _, cl := range fetchSeqs(t, c, capacity+1) {
				if seen[cl.Payload] {
					t.Fatalf("payload %d delivered twice", cl.Payload)
				}
				if cl.Payload == gone {
					t.Fatalf("the displaced record %d is still in the lane", gone)
				}
				seen[cl.Payload] = true
				if err := c.Ack(cl.Env, cl.Gen); err != nil {
					t.Fatalf("ack %d: %v", cl.Payload, err)
				}
			}
			if len(seen) != capacity {
				t.Fatalf("the lane delivered %d of its %d records", len(seen), capacity)
			}
			// A byte-bounded lane displaces as many records as the new one
			// needs, and a record it could never hold displaces nothing.
			for i := range capacity {
				if err := pub(int64(10 + i)); err != nil {
					t.Fatalf("refill %d: %v", i, err)
				}
			}
			if err := top.Publish(t.Context(), Publish[int64]{Payload: 20, Size: 8}); err != nil {
				t.Fatalf("byte-bounded displacement: %v", err)
			}
			if snap := sub.Inspect(); snap.Dropped != 3 || snap.Ready != 1 || snap.Bytes != 8 ||
				!snap.Conserved() {
				t.Fatalf("byte-bounded displacement: %+v", snap)
			}
			err := top.Publish(t.Context(), Publish[int64]{Payload: 21, Size: 9})
			if !errors.Is(err, errcode.ErrQueueFull) {
				t.Fatalf("oversized publication: %v", err)
			}
			if snap := sub.Inspect(); snap.Dropped != 3 || snap.Ready != 1 {
				t.Fatalf("an oversized publication emptied the lane: %+v", snap)
			}
		})
	}
}

// TestBlockedPublisherIsWokenByAWithdrawnReservation covers the one fall in a
// lane's depth that used not to be announced.
//
// lane.reserve takes the message slot before it looks at the byte capacity,
// so a publication the bytes refuse holds a slot for the two instructions in
// between — a slot that hasRoom reads and that unreserve never sees, because
// the reservation is withdrawn rather than released. A publisher under
// OverflowBlock that samples the depth inside that window parks on
// sub.room for a slot that was never really taken, and the withdrawal, which
// notified nobody, was the only event that could tell it so. Nothing else
// can: the withdrawal restores the depth to what it already was, so no
// settlement follows it and no other publication has to happen.
//
// The lane below has room for the seed record and one more, and the big
// publication can never be admitted beside the seed, so it takes a slot and
// gives it straight back on every attempt. Thirty-two of them start on one
// barrier with the small publication that must be admitted. Measured against
// the withdrawal restored to its silent form, this fails at 14 rounds of 300
// (`-race`, `-tags iqmq_unsafe,iqmq_debug`); the same 300 rounds cost 0.42 s
// when the wake is there. Widening the window with a 2 ms sleep takes the
// silent form to 3 rounds of 10 and leaves this test at 0 of 60, which is
// what pins the mechanism rather than the timing.
func TestBlockedPublisherIsWokenByAWithdrawnReservation(t *testing.T) {
	const (
		rounds   = 300
		refusers = 32
		big      = 9_990
		small    = 64
		capBytes = 10_000
	)
	top := TopologyConfig{Partitions: 1, AckDeadline: -1, Overflow: OverflowBlock}
	for r := range rounds {
		// The refusal under test is the byte limit of a per-subscription
		// lane, which a shared partition log has no place for. The shared log's own version of this wake --
		// Topic.waitLogRoom against Engine.reserveTotal's withdrawal -- is
		// TestBlockedLogPublisherIsWokenByAWithdrawnEngineReservation
		// (parknotify_test.go), which needs two topics
		// and a warm-up to reach its window.
		e := newTestEngine(t, KindSet{Arch: featureArch}, top)
		tp, err := e.CreateTopic("t", top)
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		if _, err := tp.Subscribe("s", SubscriptionConfig{
			CapacityMessages: 2, CapacityBytes: capBytes,
		}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := tp.Publish(context.Background(), Publish[int64]{Payload: 1, Size: small}); err != nil {
			t.Fatalf("seed publish: %v", err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range refusers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Microsecond)
				defer cancel()
				<-start
				_ = tp.Publish(ctx, Publish[int64]{Payload: 2, Size: big})
			}()
		}
		fits := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			<-start
			fits <- tp.Publish(ctx, Publish[int64]{Payload: 3, Size: small})
		}()
		close(start)
		err = <-fits
		wg.Wait()
		if err != nil {
			t.Fatalf("round %d: the publication that fits was not admitted: %v", r, err)
		}
		if err := e.Close(); err != nil {
			t.Fatalf("round %d: Close: %v", r, err)
		}
	}
}

// TestDisplacementAtTheEngineLimit pins the displacement a full engine used to
// refuse.
//
// DropOldest and DropSlow make room for a publication by discarding a record
// of the lane it is for, and a record a lane drops gives back its share of the
// engine-wide limits as well as the lane's. The per-subscription reservation
// took the engine-wide share first and refused the publication as soon as the
// engine was at Config.MaxTotalMessages, before the lane could drop anything:
// a lane holding records it could discard refused with ErrQueueFull, which
// the memory adapter documents DropOldest as doing only when the lane holds
// nothing to discard. The shared log already displaced for an engine-wide
// refusal (Topic.publishShared).
func TestDisplacementAtTheEngineLimit(t *testing.T) {
	for _, arm := range []struct {
		name string
		top  TopologyConfig
	}{
		{"dropoldest", TopologyConfig{Overflow: OverflowDropOldest}},
		{"dropslow", TopologyConfig{Fanout: DropSlow, Overflow: OverflowReject}},
	} {
		for _, a := range Arches() {
			t.Run(arm.name+"/"+a.String(), func(t *testing.T) {
				top := arm.top
				top.Partitions, top.CapacityMessages, top.AckDeadline = 1, 8, -1
				e, err := New(Config[int64]{
					Kinds: KindSet{Arch: a}, Clone: func(v int64) int64 { return v },
					MaxTotalMessages: 2, DefaultTopology: top,
				})
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				t.Cleanup(func() { _ = e.Close() })
				tp, sub, c := queueSub(t, e)
				for i := range 3 {
					if err := tp.Publish(context.Background(), Publish[int64]{Payload: int64(i), Size: 1}); err != nil {
						t.Fatalf("publication %d at the engine limit: %v (%+v)\n"+
							"    the lane holds %d records it may discard", i, err, sub.Inspect(), i)
					}
				}
				if s := sub.Inspect(); s.Ready != 2 || s.Dropped != 1 || !s.Conserved() {
					t.Fatalf("after displacing at the engine limit: %+v, want 2 ready and 1 dropped", s)
				}
				dst := make([]Claim[int64], 2)
				if n, _ := c.Fetch(poll(t), dst); n != 2 || dst[0].Payload != 1 || dst[1].Payload != 2 {
					t.Fatalf("fetched %v, want the two newest records", dst[:n])
				}
			})
		}
	}
}
