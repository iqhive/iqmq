package engine

import "testing"

// featureArch is the architecture a test of a topology feature pins.
//
// A stream, priorities, wildcards and a per-subscription capacity are refused
// under a shared partition log — `CreateTopic` refuses the first three and
// `Subscribe` the fourth (adr/ADR-017-assumptions.md, and) — and the per-subscription fan-out policies collapse to Atomic there,
// because one reservation admits a publication for every subscription at once.
// A test of any of those that leaves `KindSet.Arch` at
// `ArchAuto` therefore does not test the feature at all: it tests whichever
// architecture the bake-off last bound to `ArchAutoKind`, and fails outright
// with `ErrInvalidConfig` when that is a shared one. Pinning is what keeps the
// test testing its own subject under every binding
// (adr/ADR-017-assumptions.md).
const featureArch = PerSubEnvelopes

// archFor returns the architecture a topology has to run on: featureArch when
// the topology asks for something a shared partition log refuses, and ArchAuto
// — the bake-off's binding — for every other topology, so that a table-driven
// test keeps exercising the bound default on the cases that can carry it.
func archFor(cfg TopologyConfig) Arch {
	if cfg.Kind == TopologyStream || cfg.Priority || cfg.Wildcards {
		return featureArch
	}
	return ArchAuto
}

// TestFeatureArchOffersWhatTheFeatureTestsPin holds the pin itself to the
// reason it exists: an architecture that shares a partition log cannot carry
// the tests that pin it.
func TestFeatureArchOffersWhatTheFeatureTestsPin(t *testing.T) {
	if featureArch.Shared() {
		t.Fatalf("featureArch = %v, which shares a partition log and refuses the features its tests pin it for", featureArch)
	}
	if featureArch == ArchAuto {
		t.Fatal("featureArch is ArchAuto, so the tests that pin it follow the binding after all")
	}
}

// TestAutoBindingOffersEveryTopologyProfile refuses a binding that takes a
// specified feature away from the engine's default configuration.
//
// the memory engine lists retention/replay, wildcards and priority among
// what the in-memory engine offers, the specification makes Stream one of its three
// topology profiles and the specification puts Wildcards, Priority and Ordering in
// TopologyConfig. A caller that names no kinds gets ArchAuto and the rest of
// the Auto bindings, so whatever the bake-off writes into the generated
// auto_*.go files is what `New(Config{})` can do — and the contribution policy
// puts the correctness and maintenance gates before the geomean, so a kind
// that scores best and cannot honour a specified option is not the default:
// the family's default is the best-scoring kind that can.
//
// This is a statement about the *binding*, not about the kinds: LogCursor
// legitimately refuses three of these (adr/ADR-017-assumptions.md) and is still a kind a caller may select and the bake-off must
// measure. What it may not silently be is `ArchAuto`
// (adr/ADR-017-assumptions.md).
//
// It is not specific. `KindSet.QueueOrdered` folds in the
// ready-queue kind as well, and CreateTopic refuses strict ordering
// whenever it is false, so an the specification binding of ShardedMPMC takes strict
// ordering away from the default by exactly the same route. The failure
// names the kind that did it.
//
// The scheduler kind reached this guard too until the campaign bound a
// wheel and it fired: the fold was written before the engine sorted the
// batch it promotes, and a wheel's unordered firing never reaches a consumer.
// What holds the scheduler half now is
// TestStrictOrderingHoldsOnEverySchedulerKind, over every kind rather than
// over the bound one; the kinds are still printed here, because a binding
// that did take the option away would want them named.
func TestAutoBindingOffersEveryTopologyProfile(t *testing.T) {
	for _, tc := range []struct {
		name string
		why  string
		top  TopologyConfig
		sub  SubscriptionConfig
	}{
		{
			name: "stream", why: "the memory engine's third topology profile",
			top: TopologyConfig{Kind: TopologyStream, Retention: Retention{MaxMessages: 4}},
		},
		{
			name: "priority", why: "the memory engine and TopologyConfig.Priority",
			top: TopologyConfig{Priority: true},
		},
		{
			name: "wildcards", why: "the memory engine and TopologyConfig.Wildcards",
			top: TopologyConfig{Wildcards: true}, sub: SubscriptionConfig{Filter: "a.>"},
		},
		{
			name: "strict-ordering", why: "the message contract's default when ordering is enabled",
			top: TopologyConfig{Ordering: OrderingStrict},
		},
		{
			name: "subscription-capacity", why: "the memory engine's per-subscription capacity",
			top: TopologyConfig{}, sub: SubscriptionConfig{CapacityMessages: 4},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// No Kinds at all: this is the configuration a caller that names
			// nothing gets, which is what the bindings decide.
			eng, err := New(Config[int64]{
				Clone:           func(v int64) int64 { return v },
				DefaultTopology: TopologyConfig{Partitions: 1, CapacityMessages: 8},
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer func() { _ = eng.Close() }()
			top, err := eng.CreateTopic("t", tc.top)
			if err == nil {
				_, err = top.Subscribe("s", tc.sub)
			}
			if err == nil {
				return
			}
			ks := KindSet{}.Resolve()
			t.Fatalf("the default configuration cannot offer %s (%s): %v\n"+
				"    bound kinds: %v\n"+
				"    A kind that scores best and cannot honour a specified option is not the default:"+
				" the contribution policy puts the correctness and maintenance gates before the geomean, so"+
				" the family's default is the best-scoring kind that can."+
				" A shared partition log refuses a stream, priorities, wildcards and a per-subscription"+
				" capacity by design, and strict ordering is refused whenever the"+
				" architecture or the ready queue does not order what it holds"+
				" (arch=%v ordered=%v, queue=%v ordered=%v; timer=%v ordered=%v and retry=%v ordered=%v"+
				" are not terms of KindSet.QueueOrdered).",
				tc.name, tc.why, err, ks,
				ks.Arch, ks.Arch.Ordered(), ks.Queue, ks.Queue.Ordered(),
				ks.Timer, ks.Timer.Ordered(), ks.RetryLane, ks.RetryLane.Ordered())
		})
	}
}
