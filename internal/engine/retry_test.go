package engine

import (
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// TestRetryKindsList announces alone,
func TestRetryKindsList(t *testing.T) {
	parts := make([]string, 0, len(RetryLanes()))
	for _, k := range RetryLanes() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

func TestRetryLaneProperties(t *testing.T) {
	if len(RetryLanes()) != 3 {
		t.Fatalf("RetryLanes() = %v", RetryLanes())
	}
	seen := map[string]bool{}
	for _, k := range RetryLanes() {
		if !k.Valid() || k == RetryLaneAuto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if seen[k.String()] {
			t.Fatalf("duplicate slug %q", k)
		}
		seen[k.String()] = true
		if p, ok := ParseRetryLane(k.String()); !ok || p != k {
			t.Fatalf("ParseRetryLane(%q) = %v %v", k, p, ok)
		}
		if got := newRetryPlacer[int64](k).Kind(); got != k {
			t.Fatalf("newRetryPlacer(%v).Kind() = %v", k, got)
		}
	}
	if RetryLaneAuto.Resolve() != RetryLaneAutoKind || RetryLane(99).Valid() ||
		RetryLane(99).String() != "retrylane(?)" || RetryLane(99).Ident() != "" {
		t.Fatal("RetryLaneAuto/invalid")
	}
	if _, ok := ParseRetryLane("nope"); ok {
		t.Fatal("ParseRetryLane junk")
	}
	if !BlockLane.Blocks() || PushFront.Blocks() || SideQueue.Blocks() {
		t.Fatal("Blocks")
	}
	if !BlockLane.Ordered() || PushFront.Ordered() || SideQueue.Ordered() {
		t.Fatal("Ordered")
	}
}

func TestOrderingModeProperties(t *testing.T) {
	if len(OrderingModes()) != 2 {
		t.Fatalf("OrderingModes() = %v", OrderingModes())
	}
	for _, m := range OrderingModes() {
		if !m.Valid() || m == OrderingDefault || m.Resolve() != m || m.Ident() == "" {
			t.Fatalf("%v: basic properties", m)
		}
		if p, ok := ParseOrderingMode(m.String()); !ok || p != m {
			t.Fatalf("ParseOrderingMode(%q) = %v %v", m, p, ok)
		}
	}
	if OrderingDefault.Resolve() != OrderingRelaxed || OrderingStrict.Resolve() != OrderingStrict {
		t.Fatal("Resolve")
	}
	if !OrderingStrict.Strict() || OrderingRelaxed.Strict() || OrderingDefault.Strict() {
		t.Fatal("Strict")
	}
	if OrderingMode(99).Valid() || OrderingMode(99).String() != "ordering(?)" || OrderingMode(99).Ident() != "" {
		t.Fatal("invalid mode")
	}
	if _, ok := ParseOrderingMode("nope"); ok {
		t.Fatal("ParseOrderingMode junk")
	}
}

// retryEngine builds a single-lane engine with the given retry placement and
// ordering mode, plus one subscription and the requested number of consumers.
func retryEngine(t *testing.T, k RetryLane, ordering OrderingMode, consumers int) (*Topic[int64], *Subscription[int64], []*Consumer[int64]) {
	t.Helper()
	ks := referenceKinds()
	ks.RetryLane = k
	e := newTestEngine(t, ks, TopologyConfig{
		Partitions:       1,
		CapacityMessages: 16,
		AckDeadline:      -1,
		Ordering:         ordering,
		Overflow:         OverflowReject,
	})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cs := make([]*Consumer[int64], consumers)
	for i := range cs {
		if cs[i], err = sub.Attach(ConsumerConfig{}); err != nil {
			t.Fatal(err)
		}
	}
	return top, sub, cs
}

// fetchSeqs claims up to n records for c and returns the claims.
func fetchSeqs(t *testing.T, c *Consumer[int64], n int) []Claim[int64] {
	t.Helper()
	dst := make([]Claim[int64], n)
	got, err := c.Fetch(poll(t), dst)
	if err != nil && !errors.Is(err, errcode.ErrCanceled) && got == 0 {
		return nil
	}
	return dst[:got]
}

// seqs returns the record sequences of a batch of claims.
func seqs(cl []Claim[int64]) []uint64 {
	out := make([]uint64, len(cl))
	for i := range cl {
		out[i] = cl[i].Info.Seq
	}
	return out
}

// retryOrder holds one retry placement to its own redelivery
// order. It is an arm of TestModel because the redelivery order is the whole
// of what the family varies, and TestModel/<slug> is the only test
// attributes to a kind: a placement whose
// order is wrong is then disqualified step 1 before a binding is
// written for it, which is what criterion -- "correctness for
// strict ordering, then latency" -- has always claimed happens.
//
// Before this it was asserted by two top-level tests, which the campaign's
// gate attributes to no kind, and -- the defect class this closes -- by two
// general tests that happened to have been written while BlockLane was bound.
func retryOrder(t *testing.T, k RetryLane, ordering OrderingMode) {
	t.Helper()
	if k.Blocks() || ordering.Strict() {
		retryOrderSealed(t, k, ordering)
		return
	}
	retryOrderRelaxed(t, k)
}

func retryOrderSealed(t *testing.T, k RetryLane, ordering OrderingMode) {
	t.Helper()
	top, _, cs := retryEngine(t, k, ordering, 2)
	for i := range 3 {
		if err := top.Publish(t.Context(), Publish[int64]{Payload: int64(i), Size: 4}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	first := fetchSeqs(t, cs[0], 1)
	if len(first) != 1 || first[0].Info.Seq != 1 {
		t.Fatalf("first fetch = %v", seqs(first))
	}
	if err := cs[0].Nack(first[0].Env, first[0].Gen, 0); err != nil {
		t.Fatalf("nack: %v", err)
	}
	// The lane is now blocked on record 1. The other consumer
	// may take that record and nothing else, however many it
	// asks for.
	second := fetchSeqs(t, cs[1], 3)
	if len(second) != 1 || second[0].Info.Seq != 1 {
		t.Fatalf("while blocked, fetch = %v, want [1]", seqs(second))
	}
	// While the redelivery is outstanding the lane hands out
	// nothing at all.
	if third := fetchSeqs(t, cs[0], 3); len(third) != 0 {
		t.Fatalf("fetch while the redelivery is in flight = %v", seqs(third))
	}
	if err := cs[1].Ack(second[0].Env, second[0].Gen); err != nil {
		t.Fatalf("ack: %v", err)
	}
	// Settled: the lane opens and serves the rest in order.
	rest := fetchSeqs(t, cs[0], 3)
	if got := seqs(rest); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("after the block, fetch = %v, want [2 3]", got)
	}
}

// retryOrderRelaxed is the other half: without strict ordering the two
// non-blocking placements let the records behind a failed one carry on, both
// serve their redeliveries before the lane's store, and they differ only in
// the order of the redeliveries among themselves.
func retryOrderRelaxed(t *testing.T, k RetryLane) {
	t.Helper()
	top, _, cs := retryEngine(t, k, OrderingRelaxed, 2)
	for i := range 4 {
		if err := top.Publish(t.Context(), Publish[int64]{Payload: int64(i), Size: 4}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	first := fetchSeqs(t, cs[0], 2)
	if got := seqs(first); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("first fetch = %v", got)
	}
	// Fail both, newest first, so that the two placements disagree
	// about which redelivery comes back first.
	for i := len(first) - 1; i >= 0; i-- {
		if err := cs[0].Nack(first[i].Env, first[i].Gen, 0); err != nil {
			t.Fatalf("nack: %v", err)
		}
	}
	got := seqs(fetchSeqs(t, cs[1], 4))
	if len(got) != 4 {
		t.Fatalf("fetch = %v, want four records", got)
	}
	// Both placements serve the redeliveries before the ready queue
	// and neither blocks the lane; they differ in the order of the
	// redeliveries among themselves. Record 2 failed first and
	// record 1 second, so PushFront — which re-heads the queue —
	// serves 1 before 2, and SideQueue serves them in the order they
	// failed.
	want := []uint64{1, 2, 3, 4}
	if k == SideQueue {
		want = []uint64{2, 1, 3, 4}
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%v served %v, want %v", k, got, want)
		}
	}
}

// TestRetryAttempts: every redelivery increments the attempt count, whatever
// route it came back by.
func TestRetryAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ks := referenceKinds()
		e := newTestEngine(t, ks, TopologyConfig{
			Partitions: 1, CapacityMessages: 8, AckDeadline: 20 * time.Millisecond,
			Overflow: OverflowReject,
		})
		top, _, c := queueSub(t, e)
		if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 4}); err != nil {
			t.Fatal(err)
		}
		cl := fetchSeqs(t, c, 1)
		if len(cl) != 1 || cl[0].Info.Attempts != 1 {
			t.Fatalf("first delivery: %v", cl)
		}
		if err := c.Nack(cl[0].Env, cl[0].Gen, 0); err != nil {
			t.Fatal(err)
		}
		cl = fetchSeqs(t, c, 1)
		if len(cl) != 1 || cl[0].Info.Attempts != 2 {
			t.Fatalf("after nack: %v", cl)
		}
		// Let the lease expire rather than settling it.
		time.Sleep(30 * time.Millisecond)
		synctest.Wait()
		cl = fetchSeqs(t, c, 1)
		if len(cl) != 1 || cl[0].Info.Attempts != 3 {
			t.Fatalf("after lease expiry: %v", cl)
		}
		if err := c.Ack(cl[0].Env, cl[0].Gen); err != nil {
			t.Fatal(err)
		}
	})
}

// TestDelayedRetryHoldsSealedLane: a negative acknowledgement with a delay
// still blocks a lane that seals its redeliveries, because a record behind it
// must not overtake it while it waits.
func TestDelayedRetryHoldsSealedLane(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ks := referenceKinds()
		ks.RetryLane = BlockLane
		e := newTestEngine(t, ks, TopologyConfig{
			Partitions: 1, CapacityMessages: 8, AckDeadline: -1,
			Ordering: OrderingStrict, Overflow: OverflowReject,
		})
		top, sub, c := queueSub(t, e)
		for i := range 2 {
			if err := top.Publish(t.Context(), Publish[int64]{Payload: int64(i), Size: 4}); err != nil {
				t.Fatal(err)
			}
		}
		cl := fetchSeqs(t, c, 1)
		if len(cl) != 1 {
			t.Fatalf("fetch = %v", seqs(cl))
		}
		if err := c.Nack(cl[0].Env, cl[0].Gen, 20*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if snap := sub.Inspect(); snap.Delayed != 1 || snap.Ready != 1 {
			t.Fatalf("after a delayed nack: %+v", snap)
		}
		if got := fetchSeqs(t, c, 2); len(got) != 0 {
			t.Fatalf("the lane served %v while a delayed redelivery held it", seqs(got))
		}
		time.Sleep(30 * time.Millisecond)
		synctest.Wait()
		got := fetchSeqs(t, c, 2)
		if s := seqs(got); len(s) != 1 || s[0] != 1 {
			t.Fatalf("after the delay, fetch = %v, want [1]", s)
		}
	})
}

// retryModelScript drives one retry placement through every route a record
// takes back to its lane, comparing engine and model after each step.
func retryModelScript(t *testing.T, k RetryLane, ordering OrderingMode) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		ks := referenceKinds()
		ks.RetryLane = k
		h := newHarness(t, harnessConfig{
			kinds:       ks,
			partitions:  1,
			capMessages: 4,
			ackDeadline: 10 * tick,
			ordering:    ordering,
			overflow:    OverflowReject,
			maxAttempts: 3,
			deadLetter:  true,
		})
		defer h.close()
		h.subscribe(0)
		h.attach(0)
		h.attach(0)
		for range 4 {
			h.publish(0, 4, 0)
		}
		h.check()
		// Immediate redelivery.
		h.fetch(0, 0, 2)
		h.settle(0, false, 0, 0)
		h.check()
		// Delayed redelivery, and the clock that releases it.
		h.fetch(0, 1, 2)
		h.settle(0, false, 0, 3*tick)
		h.check()
		h.advance(4 * tick)
		h.check()
		// Lease expiry.
		h.fetch(0, 0, 2)
		h.advance(12 * tick)
		h.check()
		// Attempts run out: the record is dead-lettered.
		for range 6 {
			h.fetch(0, 1, 2)
			h.settle(0, false, 0, 0)
			h.check()
		}
		// A consumer that closes gives its records back.
		h.fetch(0, 0, 2)
		h.closeConsumer(0, 0)
		h.check()
		h.fetch(0, 1, 4)
		h.settle(0, true, 0, 0)
		h.check()
		h.closeEngine()
		h.check()
	})
}

// FuzzEngineOrdering is the strict-ordering differential: one lane, strict
// ordering and a fuzz-chosen retry placement, with the harness holding every
// fetch to the deliverable prefix of the subscription's publication order
// (harness.checkOrder) on top of the model comparison.
func FuzzEngineOrdering(f *testing.F) {
	sc := engineScript()
	for _, seed := range orderingSeeds(sc) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		ks := referenceKinds()
		ks.RetryLane = RetryLanes()[r.Choice(len(RetryLanes()))]
		ks.Queue = QueueKinds()[r.Choice(len(QueueKinds()))]
		cfg := harnessConfig{
			kinds:       ks,
			partitions:  1,
			capMessages: r.Int(1, 8),
			ackDeadline: time.Duration(r.Int(1, 20)) * tick,
			ordering:    OrderingStrict,
			overflow:    OverflowReject,
		}
		rest := data[len(data)-r.Remaining():]
		t.Logf("engine %v capacity=%d deadline=%v ordering=strict", ks.Resolve(), cfg.capMessages, cfg.ackDeadline)
		synctest.Test(t, func(t *testing.T) {
			h := newHarness(t, cfg)
			defer h.close()
			sc.Run(t, rest, h)
			h.check()
		})
	})
}

// orderingSeeds writes readable scripts for FuzzEngineOrdering: a redelivery
// that must block its lane, a lease expiry that must not let anything past,
// and a delayed publication, which strict ordering says nothing about.
func orderingSeeds(sc *fuzzutil.Script[*harness]) [][]byte {
	head := func(w *fuzzutil.Writer, retry, queue, capacity, deadline int) {
		w.Choice(len(RetryLanes()), retry)
		w.Choice(len(QueueKinds()), queue)
		w.Int(1, 8, capacity)
		w.Int(1, 20, deadline)
	}
	seeds := make([][]byte, 0, len(RetryLanes()))
	for retry := range RetryLanes() {
		w := fuzzutil.NewWriter()
		head(w, retry, 0, 4, 6)
		sc.WriteStep(w, "subscribe")
		w.Choice(maxSubs, 0)
		sc.WriteStep(w, "attach")
		w.Choice(maxSubs, 0)
		sc.WriteStep(w, "attach")
		w.Choice(maxSubs, 0)
		for range 3 {
			sc.WriteStep(w, "publish")
			w.Int(-1, 2, 0)
			w.Int(0, 32, 4)
		}
		sc.WriteStep(w, "fetch")
		w.Choice(maxSubs, 0)
		w.Choice(maxConsumers, 0)
		w.Int(1, maxFetch, 1)
		sc.WriteStep(w, "nack")
		w.Choice(maxHandles, 0)
		w.Bool(false)
		writeDelta(w, 0)
		sc.WriteStep(w, "fetch")
		w.Choice(maxSubs, 0)
		w.Choice(maxConsumers, 1)
		w.Int(1, maxFetch, 4)
		sc.WriteStep(w, "advance")
		w.Int(1, 16, 8)
		sc.WriteStep(w, "expire")
		sc.WriteStep(w, "fetch")
		w.Choice(maxSubs, 0)
		w.Choice(maxConsumers, 0)
		w.Int(1, maxFetch, 4)
		seeds = append(seeds, w.Data())
	}
	return seeds
}

// TestExpiredRedeliveryIsSwept: a record whose time to live runs out while it
// is waiting for redelivery is discarded by the sweep, not delivered again.
func TestExpiredRedeliveryIsSwept(t *testing.T) {
	for _, k := range RetryLanes() {
		t.Run(k.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ks := referenceKinds()
				ks.RetryLane = k
				e := newTestEngine(t, ks, TopologyConfig{
					Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowReject,
				})
				top, sub, c := queueSub(t, e)
				if err := top.Publish(t.Context(), Publish[int64]{
					Payload: 1, Size: 4, TTL: 30 * time.Millisecond,
				}); err != nil {
					t.Fatal(err)
				}
				cl := fetchSeqs(t, c, 1)
				if len(cl) != 1 {
					t.Fatalf("fetch = %v", cl)
				}
				if err := c.Nack(cl[0].Env, cl[0].Gen, 0); err != nil {
					t.Fatal(err)
				}
				// The record is in the lane's redelivery queue with a time to
				// live that runs out before anyone claims it.
				if snap := sub.Inspect(); snap.Ready != 1 {
					t.Fatalf("%+v", snap)
				}
				time.Sleep(40 * time.Millisecond)
				synctest.Wait()
				snap := sub.Inspect()
				if snap.Ready != 0 || snap.Dropped != 1 || !snap.Conserved() {
					t.Fatalf("%+v", snap)
				}
				if got := fetchSeqs(t, c, 1); len(got) != 0 {
					t.Fatalf("an expired redelivery was delivered: %v", seqs(got))
				}
			})
		})
	}
}

// TestExpiredRedeliveriesReArmTheSweep: a lane holding two redeliveries with
// different times to live is swept twice. The sweep re-arms the lane from the
// earliest expiry still queued, which it can only know by merging what the
// redelivery queue kept with what the store kept (retryQueue.expireTTL's
// returned next); a sweep that forgets the survivor stops waking and the
// second record is never collected.
func TestExpiredRedeliveriesReArmTheSweep(t *testing.T) {
	for _, k := range RetryLanes() {
		t.Run(k.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ks := referenceKinds()
				ks.RetryLane = k
				e := newTestEngine(t, ks, TopologyConfig{
					Partitions: 1, CapacityMessages: 8, AckDeadline: -1, Overflow: OverflowReject,
				})
				top, sub, c := queueSub(t, e)
				for i, ttl := range []time.Duration{20 * time.Millisecond, 60 * time.Millisecond} {
					if err := top.Publish(t.Context(), Publish[int64]{
						Payload: int64(i + 1), Size: 4, TTL: ttl,
					}); err != nil {
						t.Fatal(err)
					}
				}
				// Both records go to the redelivery queue, so the store holds
				// no expiry of its own and the lane's next wake-up can only
				// come from the queue.
				cl := fetchSeqs(t, c, 2)
				if len(cl) != 2 {
					t.Fatalf("fetch = %v", seqs(cl))
				}
				for _, one := range cl {
					if err := c.Nack(one.Env, one.Gen, 0); err != nil {
						t.Fatal(err)
					}
				}
				if snap := sub.Inspect(); snap.Ready != 2 {
					t.Fatalf("before the first expiry: %+v", snap)
				}
				time.Sleep(30 * time.Millisecond)
				synctest.Wait()
				if snap := sub.Inspect(); snap.Ready != 1 || snap.Dropped != 1 {
					t.Fatalf("after the first time to live: %+v", snap)
				}
				time.Sleep(40 * time.Millisecond)
				synctest.Wait()
				snap := sub.Inspect()
				if snap.Ready != 0 || snap.Dropped != 2 || !snap.Conserved() {
					t.Fatalf("after the second time to live: %+v", snap)
				}
				if got := fetchSeqs(t, c, 2); len(got) != 0 {
					t.Fatalf("an expired redelivery was delivered: %v", seqs(got))
				}
			})
		})
	}
}
