package engine

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"unsafe"

	"github.com/iqhive/iqmq/internal/payload"
	"github.com/iqhive/iqmq/internal/pool"
	"github.com/iqhive/iqmq/internal/queue"
	"github.com/iqhive/iqmq/internal/wait"
)

// TestEnvelopeSize asserts the per-subscription envelope stays within the
// 128-byte budget of the memory engine for every payload representation the
// engine is instantiated with.
func TestEnvelopeSize(t *testing.T) {
	const budget = 128
	sizes := []struct {
		name string
		size uintptr
	}{
		{"*payload.Payload", unsafe.Sizeof(Envelope[*payload.Payload]{})},
		{"payload.Payload", unsafe.Sizeof(Envelope[payload.Payload]{})},
		{"[]byte", unsafe.Sizeof(Envelope[[]byte]{})},
		{"int64", unsafe.Sizeof(Envelope[int64]{})},
	}
	for _, s := range sizes {
		t.Run(s.name, func(t *testing.T) {
			t.Logf("sizeof Envelope[%s] = %d B", s.name, s.size)
			if s.size > budget {
				t.Fatalf("Envelope[%s] is %d B, budget %d B", s.name, s.size, budget)
			}
		})
	}
	if n := unsafe.Sizeof(record[*payload.Payload]{}); n > budget {
		t.Fatalf("record is %d B, budget %d B", n, budget)
	}
}

// allocRig is one engine configured for the zero-allocation steady state:
// a retaining pool and one lane.
//
// The architecture is pinned, because what this rig varies is the pool, the
// ready queue and the wait strategy. A copying architecture allocates one
// clone per subscription by construction and needs a Config.Clone to be built
// at all, so a rig that followed the binding would either refuse to start or
// measure the architecture instead of the kind under test. Each
// architecture's own budget is TestArchAllocs, which states it per kind
// (adr/ADR-017-assumptions.md).
func allocRig(t *testing.T, pk pool.Kind, qk queue.Kind, wk wait.Kind) (*Topic[*payload.Payload], *Consumer[*payload.Payload], func()) {
	t.Helper()
	eng, err := New(Config[*payload.Payload]{
		Kinds:           KindSet{Arch: featureArch, Pool: pk, Queue: qk, Wait: wk},
		DefaultTopology: TopologyConfig{Partitions: 1, CapacityMessages: 1024},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	top, err := eng.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	return top, c, func() { _ = eng.Close() }
}

// TestAllocs asserts that the owned publish, fetch and acknowledge cycle
// allocates nothing once the pools are warm (the memory engine). It runs
// for every pool kind that retains objects, and for every ready-queue and
// wait kind over the pool that retains unconditionally. The None kind is the
// GC-only reference and allocates by construction; SyncPool is emptied by
// every collection (pool.Kind.GCCleared) and is held to a bounded budget
// rather than to zero, which is of
// adr/ADR-017-assumptions.md — the one deviation from the literal "zero for
// owned publish/fetch/ack" gate, narrowed by a declared capability.
func TestAllocs(t *testing.T) {
	body := make([]byte, 64)
	p := &payload.Payload{Body: body}
	released := 0
	release := func(*payload.Payload) { released++ }
	dst := make([]Claim[*payload.Payload], 1)
	ctx := context.Background()
	// owned asserts that the block that has just run handed an owned payload
	// back. The assertion belongs to the subtest that establishes it: it used
	// to sit at the end of TestAllocs, outside every subtest, where
	// `go test -run '^TestAllocs$/^wildcards$'` made it fail because no
	// *selected* subtest had published an owned payload -- a message about the
	// filter ("the owned payload was never released") that reads as a leak in
	// the engine, and was diagnosed as one
	// (adr/ADR-017-assumptions.md).
	owned := func(t *testing.T, before int) {
		t.Helper()
		if released <= before {
			t.Fatalf("%d owned publications came home, want at least one", released-before)
		}
	}

	cycle := func(top *Topic[*payload.Payload], c *Consumer[*payload.Payload]) func() {
		return func() {
			if err := top.Publish(ctx, Publish[*payload.Payload]{
				Payload: p, Size: len(body), Partition: 0, Release: release,
			}); err != nil {
				t.Fatalf("publish: %v", err)
			}
			n, err := c.Fetch(ctx, dst)
			if err != nil || n != 1 {
				t.Fatalf("fetch: n=%d err=%v", n, err)
			}
			if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
				t.Fatalf("ack: %v", err)
			}
		}
	}

	for _, pk := range pool.Kinds() {
		if !pk.Retains() {
			continue
		}
		t.Run(pk.String(), func(t *testing.T) {
			before := released
			defer func() { owned(t, before) }()
			top, c, done := allocRig(t, pk, queue.MutexRing, wait.Chan)
			defer done()
			run := cycle(top, c)
			for range 512 {
				run()
			}
			allocs := testing.AllocsPerRun(2000, run)
			if pk.GCCleared() {
				// A sync.Pool is emptied by every garbage collection, so it
				// cannot promise a zero-allocation steady state: a cycle that
				// follows a collection allocates its envelope and its record
				// again. The budget for it is what one collection costs, not
				// zero; the retaining kinds carry the strict assertion
				// (adr/ADR-017-assumptions.md).
				if allocs > 2 {
					t.Fatalf("%v allocates %.2f objects per cycle", pk, allocs)
				}
				return
			}
			if allocs != 0 {
				t.Fatalf("owned publish/fetch/ack allocates %.2f objects per cycle", allocs)
			}
		})
	}

	// Every MPMC-capable ready queue and every wait strategy must keep the
	// same budget: the pools, not the variant kinds, own the allocations.
	for _, qk := range QueueKinds() {
		t.Run("queue="+qk.String(), func(t *testing.T) {
			before := released
			defer func() { owned(t, before) }()
			top, c, done := allocRig(t, pool.FreeList, qk, wait.Chan)
			defer done()
			run := cycle(top, c)
			for range 512 {
				run()
			}
			if allocs := testing.AllocsPerRun(2000, run); allocs != 0 {
				t.Fatalf("queue %v allocates %.2f objects per cycle", qk, allocs)
			}
		})
	}
	for _, wk := range wait.Kinds() {
		t.Run("wait="+wk.String(), func(t *testing.T) {
			before := released
			defer func() { owned(t, before) }()
			top, c, done := allocRig(t, pool.FreeList, queue.MutexRing, wk)
			defer done()
			run := cycle(top, c)
			for range 512 {
				run()
			}
			if allocs := testing.AllocsPerRun(2000, run); allocs != 0 {
				t.Fatalf("wait %v allocates %.2f objects per cycle", wk, allocs)
			}
		})
	}
	// A wildcard topic routes on the publication's subject, which is the
	// only per-publication decision the lock-free path makes: a
	// publication every subscription takes must still allocate nothing.
	t.Run("wildcards", func(t *testing.T) {
		before := released
		defer func() { owned(t, before) }()
		eng, err := New(Config[*payload.Payload]{
			// The architecture is pinned: a topic with wildcards is refused
			// under a shared partition log, which makes no per-subscription
			// routing decision , so this subtest would
			// otherwise test the binding rather than the wildcard route.
			Kinds: KindSet{
				Arch: featureArch, Pool: pool.FreeList,
				Queue: queue.MutexRing, Wait: wait.Chan,
			},
			DefaultTopology: TopologyConfig{Partitions: 1, CapacityMessages: 1024},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer func() { _ = eng.Close() }()
		top, err := eng.CreateTopic("w", TopologyConfig{Wildcards: true})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		sub, err := top.Subscribe("s", SubscriptionConfig{Filter: "a.>"})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		c, err := sub.Attach(ConsumerConfig{})
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		run := func() {
			if err := top.Publish(ctx, Publish[*payload.Payload]{
				Payload: p, Size: len(body), Partition: 0, Subject: "a.b", Release: release,
			}); err != nil {
				t.Fatalf("publish: %v", err)
			}
			n, err := c.Fetch(ctx, dst)
			if err != nil || n != 1 {
				t.Fatalf("fetch: n=%d err=%v", n, err)
			}
			if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
				t.Fatalf("ack: %v", err)
			}
		}
		for range 512 {
			run()
		}
		if allocs := testing.AllocsPerRun(2000, run); allocs != 0 {
			t.Fatalf("a wildcard publish/fetch/ack allocates %.2f objects per cycle", allocs)
		}
	})
}

// archRig is one engine of architecture a with n subscriptions on one topic,
// each with a consumer, configured for the zero-allocation steady state.
func archRig(t *testing.T, a Arch, p FanoutPolicy, n int) (*Topic[*payload.Payload], []*Consumer[*payload.Payload], func()) {
	t.Helper()
	eng, err := New(Config[*payload.Payload]{
		Kinds: KindSet{Arch: a, Pool: pool.FreeList, Queue: queue.MutexRing, Wait: wait.Chan},
		// A copying architecture needs a clone; the others never call it.
		Clone: func(v *payload.Payload) *payload.Payload { c := *v; return &c },
		DefaultTopology: TopologyConfig{
			Partitions: 1, CapacityMessages: 1024, Fanout: p,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	top, err := eng.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	cons := make([]*Consumer[*payload.Payload], n)
	for i := range cons {
		sub, err := top.Subscribe("s"+strconv.Itoa(i), SubscriptionConfig{})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if cons[i], err = sub.Attach(ConsumerConfig{}); err != nil {
			t.Fatalf("Attach: %v", err)
		}
	}
	return top, cons, func() { _ = eng.Close() }
}

// TestArchAllocs holds every architecture to the allocation budget of
// the memory engine, on its own and under a fan-out that exercises the
// admission mask, the per-architecture claim path and the consumer's claim
// scratch. Three of the four architectures share one reference-counted record
// and allocate nothing at all; FullCopy is defined by the copy it makes and
// costs exactly one object per subscription — the copy — and nothing else,
// which is the difference the bake-off (the specification) is measuring.
func TestArchAllocs(t *testing.T) {
	body := make([]byte, 64)
	p := &payload.Payload{Body: body}
	ctx := context.Background()
	dst := make([]Claim[*payload.Payload], 1)

	cycle := func(t *testing.T, top *Topic[*payload.Payload], cons []*Consumer[*payload.Payload]) func() {
		return func() {
			if err := top.Publish(ctx, Publish[*payload.Payload]{
				Payload: p, Size: len(body), Partition: 0,
			}); err != nil {
				t.Fatalf("publish: %v", err)
			}
			for _, c := range cons {
				n, err := c.Fetch(ctx, dst)
				if err != nil || n != 1 {
					t.Fatalf("fetch: n=%d err=%v", n, err)
				}
				if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
					t.Fatalf("ack: %v", err)
				}
			}
		}
	}

	for _, a := range Arches() {
		for _, pol := range FanoutPolicies() {
			// One subscription pins the claim path of the architecture; four
			// pin the fan-out, where the admission mask and, for the shared
			// log, the slot reference count come in.
			for _, subs := range []int{1, 4} {
				name := fmt.Sprintf("arch=%v/policy=%v/subs=%d", a, pol, subs)
				t.Run(name, func(t *testing.T) {
					top, cons, done := archRig(t, a, pol, subs)
					defer done()
					run := cycle(t, top, cons)
					for range 512 {
						run()
					}
					allocs := testing.AllocsPerRun(2000, run)
					want := float64(0)
					if a.Copies() {
						want = float64(subs)
					}
					if allocs != want {
						t.Fatalf("%v/%v with %d subscriptions allocates %.2f objects per cycle, budget %.0f",
							a, pol, subs, allocs, want)
					}
				})
			}
		}
	}
}
