package memory_test

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/engine"
	"github.com/iqhive/iqmq/internal/hash"
	"github.com/iqhive/iqmq/internal/pool"
	"github.com/iqhive/iqmq/internal/queue"
	"github.com/iqhive/iqmq/internal/registry"
	"github.com/iqhive/iqmq/internal/stats"
	"github.com/iqhive/iqmq/internal/timer"
	"github.com/iqhive/iqmq/internal/wait"
	"github.com/iqhive/iqmq/memory"
)

// TestEngineConfigMirrorsTheFamilies is what makes the numeric alignment
// between the public kind enumerations and the engine's own a fact rather
// than an assumption.
//
// EngineConfig converts to the engine's kind set with a cast per family,
// which is one line instead of nine conversion tables — and which is wrong
// the moment a family gains, loses or reorders a kind. This test is the
// alarm: it compares every public kind's NAME against the internal kind at
// the same number, and the count of each family against the internal count,
// so a divergence fails here instead of binding a caller's "SeqMPMC" to
// whatever now sits at 3.
func TestEngineConfigMirrorsTheFamilies(t *testing.T) {
	t.Run("queue", func(t *testing.T) {
		names := make([]string, 0, len(memory.QueueKinds()))
		for _, k := range memory.QueueKinds() {
			names = append(names, k.String())
		}
		assertFamily(t, len(queue.Kinds()), names, func(i int) string { return queue.Kind(i).String() })
	})
	t.Run("arch", func(t *testing.T) {
		names := make([]string, 0, len(memory.ArchKinds()))
		for _, k := range memory.ArchKinds() {
			names = append(names, k.String())
		}
		assertFamily(t, len(engine.Arches()), names, func(i int) string { return engine.Arch(i).String() })
	})
	t.Run("wait", func(t *testing.T) {
		names := make([]string, 0, len(memory.WaitKinds()))
		for _, k := range memory.WaitKinds() {
			names = append(names, k.String())
		}
		assertFamily(t, len(wait.Kinds()), names, func(i int) string { return wait.Kind(i).String() })
	})
	t.Run("timer", func(t *testing.T) {
		names := make([]string, 0, len(memory.TimerKinds()))
		for _, k := range memory.TimerKinds() {
			names = append(names, k.String())
		}
		assertFamily(t, len(timer.Kinds()), names, func(i int) string { return timer.Kind(i).String() })
	})
	t.Run("hash", func(t *testing.T) {
		names := make([]string, 0, len(memory.HashKinds()))
		for _, k := range memory.HashKinds() {
			names = append(names, k.String())
		}
		assertFamily(t, len(hash.Kinds()), names, func(i int) string { return hash.Kind(i).String() })
	})
	t.Run("pool", func(t *testing.T) {
		names := make([]string, 0, len(memory.PoolKinds()))
		for _, k := range memory.PoolKinds() {
			names = append(names, k.String())
		}
		assertFamily(t, len(pool.Kinds()), names, func(i int) string { return pool.Kind(i).String() })
	})
	t.Run("registry", func(t *testing.T) {
		names := make([]string, 0, len(memory.RegistryKinds()))
		for _, k := range memory.RegistryKinds() {
			names = append(names, k.String())
		}
		assertFamily(t, len(registry.Kinds()), names, func(i int) string { return registry.Kind(i).String() })
	})
	t.Run("stats", func(t *testing.T) {
		names := make([]string, 0, len(memory.StatsKinds()))
		for _, k := range memory.StatsKinds() {
			names = append(names, k.String())
		}
		assertFamily(t, len(stats.Kinds()), names, func(i int) string { return stats.Kind(i).String() })
	})
	t.Run("retrylane", func(t *testing.T) {
		names := make([]string, 0, len(memory.RetryLaneKinds()))
		for _, k := range memory.RetryLaneKinds() {
			names = append(names, k.String())
		}
		assertFamily(t, len(engine.RetryLanes()), names, func(i int) string { return engine.RetryLane(i).String() })
	})
}

// assertFamily holds one public family to its internal one: the same number
// of concrete kinds, and the same name at every number including Auto's.
func assertFamily(t *testing.T, internalConcrete int, publicNames []string, internalName func(int) string) {
	t.Helper()
	if len(publicNames) != internalConcrete+1 {
		t.Fatalf("the public family lists %d kinds and the internal one has %d concrete kinds plus Auto",
			len(publicNames), internalConcrete)
	}
	for i, got := range publicNames {
		if want := internalName(i); got != want {
			t.Fatalf("public kind %d is %q and internal kind %d is %q: the numeric alignment EngineConfig casts through is broken",
				i, got, i, want)
		}
	}
}

// TestEngineConfigResolveAndString re-expresses TestKindSetResolveAndString:
// the zero selection resolves to the recorded winners and renders as the specification's
// dimensions in a fixed order.
func TestEngineConfigResolveAndString(t *testing.T) {
	auto := memory.EngineConfig{}.Resolve()
	if auto.Queue == memory.QueueAuto || auto.Arch == memory.ArchAuto || auto.Wait == memory.WaitAuto ||
		auto.Timer == memory.TimerAuto || auto.Hash == memory.HashAuto || auto.Pool == memory.PoolAuto ||
		auto.Registry == memory.RegistryAuto || auto.Stats == memory.StatsAuto ||
		auto.RetryLane == memory.RetryLaneAuto {
		t.Fatalf("Resolve left an Auto in %+v", auto)
	}
	s := memory.EngineConfig{}.String()
	for _, dim := range []string{"arch=", "queue=", "wait=", "timer=", "hash=", "pool=", "registry=", "stats=", "retry="} {
		if !strings.Contains(s, dim) {
			t.Fatalf("EngineConfig.String() is %q and names no %s dimension", s, dim)
		}
	}
	safe := memory.SafeEngine().String()
	if !strings.HasSuffix(safe, "/safe") {
		t.Fatalf("SafeEngine().String() is %q, want the reference configuration's /safe suffix", safe)
	}
	if !memory.SafeEngine().Valid() {
		t.Fatal("SafeEngine is not a valid selection")
	}
}

// TestQueueKindsTheEngineRefuses re-expresses TestQueueKindsAreMPMC: a lane is
// fed by any number of publishers and drained by any number of consumers, so
// the single-producer and single-consumer specialisations are refused rather
// than used unsafely.
func TestQueueKindsTheEngineRefuses(t *testing.T) {
	for _, k := range []memory.QueueKind{memory.QueueSPSC, memory.QueueMPSC} {
		if (memory.EngineConfig{Queue: k}).Valid() {
			t.Fatalf("a lane accepts the %v ready queue, which is not multi-producer multi-consumer", k)
		}
		if _, err := memory.NewBroker(memory.Config{Engine: memory.EngineConfig{Queue: k}}); mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Fatalf("NewBroker with %v: %v (code %v)", k, err, mq.CodeOf(err))
		}
	}
	for _, k := range memory.QueueKinds() {
		if k == memory.QueueSPSC || k == memory.QueueMPSC {
			continue
		}
		if !(memory.EngineConfig{Queue: k}).Valid() {
			t.Fatalf("the engine refuses the %v ready queue", k)
		}
	}
}

// TestEveryKindSetPublishesAndReceives is the literal-Phase-0 check on the
// adapter: every kind of every family is selectable through EngineConfig, and
// a message goes through each of them. No kind is a stub, and a kind that
// stopped working through the public surface fails here rather than in a
// bake-off run nobody was watching.
func TestEveryKindSetPublishesAndReceives(t *testing.T) {
	ctx := context.Background()
	// One family at a time, every other family at Auto: the full cross is
	// 10×5×8×7×6×6×5×4×4 engines and the engine's own TestEngineMatrix is
	// what sweeps that. What this covers is the adapter's plumbing of each
	// kind, which is a per-family property.
	type variant struct {
		name string
		cfg  memory.EngineConfig
	}
	var variants []variant
	for _, k := range memory.QueueKinds() {
		if k == memory.QueueSPSC || k == memory.QueueMPSC {
			continue
		}
		variants = append(variants, variant{"queue=" + k.String(), memory.EngineConfig{Queue: k}})
	}
	for _, k := range memory.ArchKinds() {
		variants = append(variants, variant{"arch=" + k.String(), memory.EngineConfig{Arch: k}})
	}
	for _, k := range memory.WaitKinds() {
		variants = append(variants, variant{"wait=" + k.String(), memory.EngineConfig{Wait: k}})
	}
	for _, k := range memory.TimerKinds() {
		variants = append(variants, variant{"timer=" + k.String(), memory.EngineConfig{Timer: k}})
	}
	for _, k := range memory.HashKinds() {
		variants = append(variants, variant{"hash=" + k.String(), memory.EngineConfig{Hash: k}})
	}
	for _, k := range memory.PoolKinds() {
		variants = append(variants, variant{"pool=" + k.String(), memory.EngineConfig{Pool: k}})
	}
	for _, k := range memory.RegistryKinds() {
		variants = append(variants, variant{"registry=" + k.String(), memory.EngineConfig{Registry: k}})
	}
	for _, k := range memory.StatsKinds() {
		variants = append(variants, variant{"stats=" + k.String(), memory.EngineConfig{Stats: k}})
	}
	for _, k := range memory.RetryLaneKinds() {
		variants = append(variants, variant{"retry=" + k.String(), memory.EngineConfig{RetryLane: k}})
	}
	variants = append(variants, variant{"safe", memory.SafeEngine()})
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			client, b := memory.NewForTest(t, memory.TestEngine(v.cfg))
			t.Logf("engine %v", b.Engine())
			c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			const n = 8
			for i := range n {
				if err := client.Publish(ctx, "orders", &mq.Message{
					Key:  []byte{byte(i)},
					Body: []byte{byte(i)},
				}); err != nil {
					t.Fatalf("Publish %d: %v", i, err)
				}
			}
			ds := receiveN(t, ctx, c, n)
			// One negative settlement per run, so that the retry placement
			// and the wait strategy are both on the path.
			if err := ds[0].Nack(); err != nil {
				t.Fatalf("Nack: %v", err)
			}
			for i := 1; i < n; i++ {
				if err := ds[i].Ack(); err != nil {
					t.Fatalf("Ack %d: %v", i, err)
				}
			}
			again := receiveN(t, ctx, c, 1)
			if err := again[0].Ack(); err != nil {
				t.Fatalf("Ack of the redelivery: %v", err)
			}
			snap, err := b.Inspect("orders", "billing")
			if err != nil {
				t.Fatalf("Inspect: %v", err)
			}
			if snap.Acked != n || snap.Ready != 0 || snap.InFlight != 0 {
				t.Fatalf("snapshot %+v, want %d acknowledged records and nothing held", snap, n)
			}
			if !snap.Conserved() {
				t.Fatalf("conservation broken under %v: %+v", v.name, snap)
			}
		})
	}
}

// TestSchedulerResolutionIsCheckedRatherThanIgnored pins a
// resolution the selected scheduler kind cannot provide is refused, and one
// it can is reported.
func TestSchedulerResolutionIsCheckedRatherThanIgnored(t *testing.T) {
	// The wheels quantise to a millisecond; the heaps fire at the exact due
	// time and report a resolution of zero.
	b, err := memory.NewBroker(memory.Config{
		SchedulerResolution: time.Millisecond,
		Engine:              memory.EngineConfig{Timer: memory.TimerHashedWheel},
	})
	if err != nil {
		t.Fatalf("a millisecond resolution on a millisecond wheel: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if got := b.SchedulerResolution(); got != time.Millisecond {
		t.Fatalf("SchedulerResolution is %v, want the wheel's millisecond", got)
	}
	exact, err := memory.NewBroker(memory.Config{
		SchedulerResolution: time.Microsecond,
		Engine:              memory.EngineConfig{Timer: memory.TimerHeap},
	})
	if err != nil {
		t.Fatalf("a microsecond resolution on a heap: %v", err)
	}
	t.Cleanup(func() { _ = exact.Close() })
	if got := exact.SchedulerResolution(); got != 0 {
		t.Fatalf("SchedulerResolution is %v on a heap, want 0 for an exact scheduler", got)
	}
}

// TestConfigurationErrorsEndInAWord pins the text of the refusals a caller
// reads when a configuration is wrong. Three of them ended in a space, where
// the published text had lost a reference.
func TestConfigurationErrorsEndInAWord(t *testing.T) {
	for name, cfg := range map[string]memory.Config{
		"MaxInFlight":         {DefaultTopology: memory.TopologyConfig{MaxInFlight: 1}},
		"MaxDelayed":          {DefaultTopology: memory.TopologyConfig{MaxDelayed: 1}},
		"SchedulerResolution": {SchedulerResolution: time.Nanosecond},
	} {
		t.Run(name, func(t *testing.T) {
			b, err := memory.NewBroker(cfg)
			if err == nil {
				_ = b.Close()
				t.Fatalf("NewBroker accepted %s", name)
			}
			if msg := err.Error(); strings.TrimSpace(msg) != msg {
				t.Fatalf("NewBroker refused %s with %q, which ends in a space", name, msg)
			}
		})
	}
}

// emptySubscriptionCost reports what one empty subscription allocates on a
// topic of four lanes, with four priority levels each: 16 lane stores.
func emptySubscriptionCost(t *testing.T, eng memory.EngineConfig, capacity int) uint64 {
	t.Helper()
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestEngine(eng), memory.TestTopology(memory.TopologyConfig{
		CapacityMessages: capacity, Partitions: 4, Priority: true,
	}))
	adm, err := b.Admin()
	if err != nil {
		t.Fatal(err)
	}
	if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatal(err)
	}
	const subs = 2
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := range subs {
		if err := adm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders", Name: "s" + strconv.Itoa(i)}); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / subs
}

// TestAnEmptySubscriptionCostsAFewKiBPerLane pins what an empty
// subscription costs in the default engine and the Safe one. Every lane
// store has a scheduler for its delayed records, and the default kind, the
// hashed wheel, used to carry about 132 KiB of slots from the moment the
// lane was built: 2 MiB for a subscription of 16 stores, and about 528 MiB
// for one under the DSN's partitions=1024&priority=true, before anything was
// published or delayed. The wheel now takes its slots at the first delayed
// record.
func TestAnEmptySubscriptionCostsAFewKiBPerLane(t *testing.T) {
	for _, tc := range []struct {
		name string
		eng  memory.EngineConfig
	}{{"default", memory.EngineConfig{}}, {"safe", memory.EngineConfig{Safe: true}}} {
		t.Run(tc.name, func(t *testing.T) {
			const most = 16 * 8 << 10 // 8 KiB for each of the 16 stores
			if got := emptySubscriptionCost(t, tc.eng, 16); got > most {
				t.Fatalf("an empty subscription of 16 lane stores allocated %d KiB, want at most %d KiB",
					got>>10, most>>10)
			}
		})
	}
}

// TestCapacityMessagesIsABoundNotAnAllocation pins that CapacityMessages
// costs nothing until records arrive. It bounds the records a lane holds,
// and the lane's ready queue used to be made at that size when the lane was
// built: 512 KiB per lane at the default capacity, four times that per lane
// on a priority topic, and a DSN capacity of a billion ended the process at
// the first Subscribe. An empty subscription now costs the same whatever its
// bound, in the default engine and in the Safe one a DSN's engine=safe
// selects.
func TestCapacityMessagesIsABoundNotAnAllocation(t *testing.T) {
	allocated := emptySubscriptionCost
	for _, tc := range []struct {
		name string
		eng  memory.EngineConfig
	}{{"default", memory.EngineConfig{}}, {"safe", memory.EngineConfig{Safe: true}}} {
		t.Run(tc.name, func(t *testing.T) {
			small := allocated(t, tc.eng, 16)
			// Room for what the runtime allocates beside the call; the
			// ready queues alone used to cost 128 times this at the
			// default capacity and 2,048 times it at a million.
			const slack = 64 << 10
			for _, capacity := range []int{0, 1 << 20} {
				if large := allocated(t, tc.eng, capacity); large > small+slack {
					t.Errorf("an empty subscription allocated %d KiB at CapacityMessages %d and %d KiB at 16: "+
						"the capacity is allocated rather than enforced as a bound", large>>10, capacity, small>>10)
				}
			}
		})
	}
}

// TestAdvanceNeedsAClockTheBrokerOwns pins the other half of the fault
// injector's honesty: a fault that could not do anything says so.
func TestAdvanceNeedsAClockTheBrokerOwns(t *testing.T) {
	wall, err := memory.NewBroker(memory.Config{})
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	t.Cleanup(func() { _ = wall.Close() })
	if err := wall.Advance(time.Second); !isUnsupported(err) {
		t.Fatalf("Advance on a wall-clock broker: %v, want ErrUnsupported", err)
	}
	deterministic, err := memory.NewBroker(memory.Config{Clock: memory.NewTestClock(time.Time{})})
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	t.Cleanup(func() { _ = deterministic.Close() })
	before := deterministic.Clock().Now()
	if err := deterministic.Advance(time.Minute); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if got := deterministic.Clock().Now().Sub(before); got != time.Minute {
		t.Fatalf("Advance moved the clock by %v, want a minute", got)
	}
	// NewForTest installs one, which is what makes every delay and expiry
	// test in this package deterministic.
	_, forTest := memory.NewForTest(t)
	if err := forTest.Advance(time.Minute); err != nil {
		t.Fatalf("Advance on a NewForTest broker: %v", err)
	}
}

// isUnsupported reports whether err carries CodeUnsupported.
func isUnsupported(err error) bool { return mq.CodeOf(err) == mq.CodeUnsupported }
