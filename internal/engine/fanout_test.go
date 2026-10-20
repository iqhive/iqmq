package engine

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// fanScript drives one fan-out configuration through the operations that can
// change what a subscription holds, comparing every one of them with
// internal/model/fanout.
func fanScript(t *testing.T, cfg fanConfig) {
	t.Helper()
	h := newFanHarness(t, cfg)
	defer h.close()

	// Fill every subscription to its capacity and past it, which is where
	// the policies part company.
	for range cfg.capacity + 2 {
		_ = h.publish(4)
		h.check()
	}
	// One subscription drains while the others stay full: the slow ones are
	// what every policy is about.
	for range cfg.capacity + 2 {
		h.claim(0)
		h.settle(0)
		_ = h.publish(4)
		h.check()
	}
	// A subscription that holds its deliveries without settling them is the
	// case DropSlow cannot drop its way out of.
	h.claim(1)
	_ = h.publish(4)
	h.check()
	// A late subscription receives only what follows it.
	h.subscribe()
	_ = h.publish(4)
	h.check()
	// Closing one subscription releases everything it held.
	h.closeSub(1)
	h.check()
	_ = h.publish(4)
	h.check()
	for i := range h.subs {
		h.claim(i)
		h.settle(i)
	}
	h.check()
}

// TestModel runs the differential for every kind of the engine's three
// variant families: one subtest per architecture, one per admission
// policy and one per retry placement, which is what
// benchmarks/cmd/bakeoff lint pairs with the benchmark names.
func TestModel(t *testing.T) {
	for _, a := range Arches() {
		t.Run(a.String(), func(t *testing.T) {
			t.Parallel()
			// The ownership contract of every terminal route, per
			// architecture. It is an arm of TestModel because that is the
			// test the bake-off's correctness gate attributes to a kind, so
			// an architecture that leaks an owned payload is disqualified
			// before a binding is written for it (archown_test.go,
			// ).
			t.Run("ownership", func(t *testing.T) { archOwnership(t, a) })
			for _, p := range FanoutPolicies() {
				t.Run(p.String(), func(t *testing.T) {
					fanScript(t, fanConfig{arch: a, policy: p, subs: 3, capacity: 2})
				})
			}
		})
	}
	for _, p := range FanoutPolicies() {
		t.Run(p.String(), func(t *testing.T) {
			t.Parallel()
			for _, a := range Arches() {
				t.Run(a.String(), func(t *testing.T) {
					fanScript(t, fanConfig{arch: a, policy: p, subs: 3, capacity: 3})
				})
			}
		})
	}
	// Every retry placement, under both ordering modes, against
	// internal/model/memq -- plus the redelivery order each placement owes,
	// which is the whole of what the family varies and now lives here rather
	// than in the general suite (retry_test.go's retryOrder, and).
	for _, k := range RetryLanes() {
		t.Run(k.String(), func(t *testing.T) {
			t.Parallel()
			for _, ordering := range OrderingModes() {
				t.Run(ordering.String(), func(t *testing.T) {
					t.Run("script", func(t *testing.T) { retryModelScript(t, k, ordering) })
					t.Run("order", func(t *testing.T) { retryOrder(t, k, ordering) })
				})
			}
		})
	}
}

// fanoutScript is the op script FuzzEngineFanout and FuzzEngineGroupIsolation
// share. slow, when it is not negative, names a subscription the script never
// claims from, which is the slow subscriber the isolation target is about.
func fanoutScript(slow int) *fuzzutil.Script[*fanHarness] {
	skip := func(h *fanHarness, i int) bool { return slow >= 0 && i == slow && i < len(h.subs) }
	return &fuzzutil.Script[*fanHarness]{
		MaxSteps: 256,
		Steps: []fuzzutil.Step[*fanHarness]{
			{Name: "publish", Weight: 8, Run: func(_ testing.TB, r *fuzzutil.Reader, h *fanHarness) {
				_ = h.publish(r.Int(0, 8))
			}},
			{Name: "claim", Weight: 5, Run: func(_ testing.TB, r *fuzzutil.Reader, h *fanHarness) {
				i := r.Choice(maxFanSubs)
				if skip(h, i) {
					return
				}
				h.claim(i)
			}},
			{Name: "settle", Weight: 5, Run: func(_ testing.TB, r *fuzzutil.Reader, h *fanHarness) {
				i := r.Choice(maxFanSubs)
				if skip(h, i) {
					return
				}
				h.settle(i)
			}},
			{Name: "subscribe", Weight: 1, Run: func(_ testing.TB, _ *fuzzutil.Reader, h *fanHarness) {
				h.subscribe()
			}},
			{Name: "closesub", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, h *fanHarness) {
				i := r.Choice(maxFanSubs)
				if skip(h, i) {
					return
				}
				h.closeSub(i)
			}},
		},
		Check: func(_ testing.TB, h *fanHarness) { h.check() },
	}
}

// fanConfigFrom decodes the topic shape from the head of the script.
func fanConfigFrom(r *fuzzutil.Reader) fanConfig {
	arches, policies := Arches(), FanoutPolicies()
	return fanConfig{
		arch:     arches[r.Choice(len(arches))],
		policy:   policies[r.Choice(len(policies))],
		subs:     r.Int(1, 4),
		capacity: r.Int(1, 4),
		capBytes: int64(r.Int(0, 64)),
	}
}

// writeFanConfig mirrors fanConfigFrom for readable seeds.
func writeFanConfig(w *fuzzutil.Writer, arch, policy, subs, capacity, capBytes int) {
	w.Choice(len(Arches()), arch)
	w.Choice(len(FanoutPolicies()), policy)
	w.Int(1, 4, subs)
	w.Int(1, 4, capacity)
	w.Int(0, 64, capBytes)
}

// fanSeeds authors readable scripts over every architecture and policy: fill
// a subscription past its capacity, drain another, hold a delivery, add and
// remove a subscription.
func fanSeeds(tb testing.TB, sc *fuzzutil.Script[*fanHarness]) [][]byte {
	type step func(w *fuzzutil.Writer)
	pub := func(size int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, "publish"); w.Int(0, 8, size) }
	}
	one := func(name string, i int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, name); w.Choice(maxFanSubs, i) }
	}
	sub := func() step { return func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, "subscribe") } }

	body := []step{
		pub(4), pub(4), pub(4), pub(4),
		one("claim", 0), one("settle", 0), pub(4),
		one("claim", 1), pub(4),
		sub(), pub(4),
		one("closesub", 1), pub(4),
		one("claim", 0), one("settle", 0), one("claim", 2), one("settle", 2),
	}
	seeds := make([][]byte, 0, len(Arches())*len(FanoutPolicies()))
	for a := range len(Arches()) {
		for p := range len(FanoutPolicies()) {
			w := fuzzutil.NewWriter()
			writeFanConfig(w, a, p, 3, 2, 0)
			for _, st := range body {
				st(w)
			}
			seeds = append(seeds, w.Data())
		}
	}
	return seeds
}

// FuzzEngineFanout is the differential of the fan-out admission: a
// fuzz-chosen architecture, policy and topic shape are driven through a
// script, and which subscriptions each publication reached, what the
// publisher was told and every counter must agree with
// internal/model/fanout after every step.
func FuzzEngineFanout(f *testing.F) {
	sc := fanoutScript(-1)
	for _, s := range fanSeeds(f, sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		cfg := fanConfigFrom(r)
		rest := data[len(data)-r.Remaining():]
		synctest.Test(t, func(t *testing.T) {
			h := newFanHarness(t, cfg)
			defer h.close()
			sc.Run(t, rest, h)
			h.check()
		})
	})
}

// FuzzEngineGroupIsolation is the same differential with one subscription
// that never claims and never settles. It is the slow-subscriber case: the
// model knows exactly which records the other subscriptions must still
// receive under each policy and architecture — everything, until the slow one
// takes the capacity they share — so a fast subscription that loses a record
// to a slow one, or keeps one it should have lost, fails here.
func FuzzEngineGroupIsolation(f *testing.F) {
	sc := fanoutScript(0)
	for _, s := range fanSeeds(f, sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		cfg := fanConfigFrom(r)
		cfg.subs = max(cfg.subs, 2)
		rest := data[len(data)-r.Remaining():]
		synctest.Test(t, func(t *testing.T) {
			h := newFanHarness(t, cfg)
			defer h.close()
			sc.Run(t, rest, h)
			h.check()
		})
	})
}

// TestSlowSubscriberIsolation is the headline property and together: one subscription that never consumes, two that keep up,
// and the question of whether the publisher can go on serving the two.
//
// Under a per-subscription architecture the answer is the policy's: Atomic
// stops for everybody, and Independent, BestEffort and DropSlow keep the fast
// subscriptions fed. Under the shared log the answer is the architecture's:
// the slow subscription holds the slots the others need, so the fast ones
// stop with it whatever the policy says — the trade the bake-off measures.
func TestSlowSubscriberIsolation(t *testing.T) {
	const capacity = 4
	for _, a := range Arches() {
		for _, p := range FanoutPolicies() {
			t.Run(a.String()+"/"+p.String(), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					h := newFanHarness(t, fanConfig{arch: a, policy: p, subs: 3, capacity: capacity})
					defer h.close()
					// Fill the lanes. The slow subscription (0) never claims.
					for range capacity {
						_ = h.publish(1)
					}
					h.check()
					// Now keep the two fast subscriptions draining while the
					// publisher keeps going.
					delivered := 0
					for range 4 * capacity {
						for i := 1; i < 3; i++ {
							h.claim(i)
							h.settle(i)
						}
						before := h.subs[1].sub.Inspect().Published
						_ = h.publish(1)
						if h.subs[1].sub.Inspect().Published > before {
							delivered++
						}
					}
					h.check()
					// A publication gets past a full subscription when the
					// policy lets the others decide for themselves, or when
					// DropSlow reclaims the room the slow one is holding.
					isolated := p == DropSlow || (!a.Shared() && p != Atomic)
					if isolated && delivered == 0 {
						t.Fatalf("%v/%v: the fast subscriptions received nothing "+
							"while a slow one was full", a, p)
					}
					if !isolated && delivered != 0 {
						t.Fatalf("%v/%v: %d publications got past a full subscription "+
							"that holds the capacity", a, p, delivered)
					}
				})
			})
		}
	}
}

// TestFanoutPartialDelivery pins what each policy tells the publisher when
// exactly one subscription is full. Only Atomic refuses the publication
// outright; Independent reports the partial admission as errcode.ErrPartial,
// which is not a refusal and must not be read as one.
func TestFanoutPartialDelivery(t *testing.T) {
	for _, tc := range []struct {
		policy   FanoutPolicy
		wantErr  error
		wantFast int64
		wantSlow int64
		rejected int64
		dropped  int64
	}{
		{Atomic, errcode.ErrQueueFull, 0, 0, 1, 0},
		{Independent, errcode.ErrPartial, 1, 0, 1, 0},
		{BestEffort, nil, 1, 0, 1, 0},
		// DropSlow does not refuse: it drops the record the full
		// subscription was holding and admits the new one in its place.
		{DropSlow, nil, 1, 1, 0, 1},
	} {
		t.Run(tc.policy.String(), func(t *testing.T) {
			h := newFanHarness(t, fanConfig{arch: PerSubEnvelopes, policy: tc.policy, subs: 2, capacity: 1})
			defer h.close()
			// Subscription 1 drains; subscription 0 fills up.
			_ = h.publish(1)
			h.claim(1)
			h.settle(1)
			h.check()
			before := h.subs[1].sub.Inspect().Published
			err := h.publish(1)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("publish returned %v, want %v", err, tc.wantErr)
			}
			if refused(err) != (tc.wantErr != nil && errors.Is(tc.wantErr, errcode.ErrQueueFull)) {
				t.Fatalf("publish returned %v, which reads as a refusal", err)
			}
			if got := h.subs[1].sub.Inspect().Published - before; got != tc.wantFast {
				t.Fatalf("the subscription with room received %d records, want %d", got, tc.wantFast)
			}
			slow := h.subs[0].sub.Inspect()
			if slow.Rejected != tc.rejected || slow.Dropped != tc.dropped {
				t.Fatalf("the full subscription counted %d rejections and %d drops, want %d and %d",
					slow.Rejected, slow.Dropped, tc.rejected, tc.dropped)
			}
			if got := slow.Published - 1; got != tc.wantSlow {
				t.Fatalf("the full subscription received %d further records, want %d", got, tc.wantSlow)
			}
			h.check()
		})
	}
}

// TestFanoutReleaseFollowsAdmission is the ownership contract of an owned
// publication (Publish.Release, adr/ADR-017-assumptions.md):
// the engine releases the payload of every publication it admitted to any
// subscription, and of no publication it refused, and the caller can tell the
// two apart from the error alone. Independent is the policy that used to make
// that undecidable — it admits the record wherever it fits and still returns
// an error — so a caller that followed the documented rule and re-used or
// released the payload after it double-freed a record the engine still held.
func TestFanoutReleaseFollowsAdmission(t *testing.T) {
	for _, tc := range []struct {
		policy   FanoutPolicy
		wantErr  error
		admitted bool
	}{
		{Atomic, errcode.ErrQueueFull, false},
		{Independent, errcode.ErrPartial, true},
		{BestEffort, nil, true},
		{DropSlow, nil, true},
	} {
		t.Run(tc.policy.String(), func(t *testing.T) {
			// The case is one subscription full while another has room,
			// which is a per-subscription reservation: under a shared
			// partition log every policy behaves as Atomic because one
			// reservation admits the publication for all of them.
			// The shared architecture's own ownership
			// contract is covered for every terminal route by
			// TestModel/<arch>/ownership.
			e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{
				Kind: TopologyTopic, Fanout: tc.policy, Partitions: 1,
				CapacityMessages: 1, AckDeadline: -1,
			})
			top, err := e.CreateTopic("t", TopologyConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := top.Subscribe("slow", SubscriptionConfig{}); err != nil {
				t.Fatal(err)
			}
			fast, err := top.Subscribe("fast", SubscriptionConfig{})
			if err != nil {
				t.Fatal(err)
			}
			c, err := fast.Attach(ConsumerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			released := map[int64]int{}
			pub := func(v int64) error {
				return top.Publish(context.Background(), Publish[int64]{
					Payload: v, Size: 1, Release: func(x int64) { released[x]++ },
				})
			}
			// One record fills both lanes; draining the fast one leaves
			// exactly one subscription full for the next publication.
			if err := pub(1); err != nil {
				t.Fatalf("first publication: %v", err)
			}
			dst := make([]Claim[int64], 1)
			if n, err := c.Fetch(poll(t), dst); n != 1 || err != nil {
				t.Fatalf("fetch: n=%d err=%v", n, err)
			}
			if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
				t.Fatal(err)
			}
			if err := pub(2); !errors.Is(err, tc.wantErr) {
				t.Fatalf("publish into a full subscription: %v, want %v", err, tc.wantErr)
			}
			if got := released[2]; got != 0 {
				t.Fatalf("the payload came back %d times while the engine still holds it", got)
			}
			// Close drops what the engine holds, which releases exactly the
			// payloads it took ownership of.
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			if got := released[1]; got != 1 {
				t.Fatalf("an admitted payload was released %d times, want once", got)
			}
			want := 0
			if tc.admitted {
				want = 1
			}
			if got := released[2]; got != want {
				t.Fatalf("policy %v returned %v and released the payload %d times, want %d: "+
					"the error must say whether the engine took ownership",
					tc.policy, tc.wantErr, got, want)
			}
		})
	}
}
