package engine

import (
	"errors"
	"strings"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/testguard"
)

// TestKindsList announces the engine's own variant families to
// benchmarks/cmd/bakeoff. The engine hosts three of them — the fan-out
// storage architecture, the fan-out admission policy and the retry
// placement — so the list is their union, and each family has its own
// list test to measure one at a time.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, len(Arches())+len(FanoutPolicies())+len(RetryLanes()))
	for _, a := range Arches() {
		parts = append(parts, a.String()+"="+a.Ident())
	}
	for _, f := range FanoutPolicies() {
		parts = append(parts, f.String()+"="+f.Ident())
	}
	for _, k := range RetryLanes() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

// TestArchKindsList announces alone.
func TestArchKindsList(t *testing.T) {
	parts := make([]string, 0, len(Arches()))
	for _, a := range Arches() {
		parts = append(parts, a.String()+"="+a.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

// TestFanoutKindsList announces alone.
func TestFanoutKindsList(t *testing.T) {
	parts := make([]string, 0, len(FanoutPolicies()))
	for _, f := range FanoutPolicies() {
		parts = append(parts, f.String()+"="+f.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

// TestCopyModeKindsList announces — the payload copy modes and the
// zero-copy receive of the memory engine, which
// points at it.
//
// The family is deliberately not part of TestKindsList: that list is the one
// `bakeoff lint` holds to a TestModel/<slug> subtest and a benchmark name for
// every kind, and it enumerates the kinds an *engine* is built with. A copy
// mode is chosen per publication instead, so it is pinned by the
// differential's copy-mode sweep and measured through the mode arms of
// BenchmarkMemory/PubSubAck.
func TestCopyModeKindsList(t *testing.T) {
	parts := make([]string, 0, len(CopyModes())+1)
	for _, m := range CopyModes() {
		parts = append(parts, m.String()+"="+m.Ident())
	}
	parts = append(parts, ReceiveZeroCopy.String()+"="+ReceiveZeroCopy.Ident())
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

func TestArchProperties(t *testing.T) {
	if len(Arches()) != 4 {
		t.Fatalf("Arches() = %v", Arches())
	}
	seen := map[string]bool{}
	for _, a := range Arches() {
		if !a.Valid() || a == ArchAuto || a.Resolve() != a || a.Ident() == "" {
			t.Fatalf("%v: basic properties", a)
		}
		if seen[a.String()] {
			t.Fatalf("duplicate slug %q", a)
		}
		seen[a.String()] = true
		if p, ok := ParseArch(a.String()); !ok || p != a {
			t.Fatalf("ParseArch(%q) = %v %v", a, p, ok)
		}
	}
	if ArchAuto.Resolve() != ArchAutoKind || Arch(99).Valid() ||
		Arch(99).String() != "arch(?)" || Arch(99).Ident() != "" {
		t.Fatal("ArchAuto/invalid")
	}
	if _, ok := ParseArch("nope"); ok {
		t.Fatal("ParseArch junk")
	}
	if !LogCursor.Shared() || PerSubEnvelopes.Shared() || ChanFanout.Shared() || FullCopy.Shared() {
		t.Fatal("Shared")
	}
	if !FullCopy.Copies() || PerSubEnvelopes.Copies() || LogCursor.Copies() {
		t.Fatal("Copies")
	}
	if LogCursor.Ordered() || !PerSubEnvelopes.Ordered() || !ChanFanout.Ordered() || !FullCopy.Ordered() {
		t.Fatal("Ordered")
	}
}

func TestFanoutPolicyProperties(t *testing.T) {
	if len(FanoutPolicies()) != 4 {
		t.Fatalf("FanoutPolicies() = %v", FanoutPolicies())
	}
	seen := map[string]bool{}
	for _, f := range FanoutPolicies() {
		if !f.Valid() || f == FanoutAuto || f.Resolve() != f || f.Ident() == "" {
			t.Fatalf("%v: basic properties", f)
		}
		if seen[f.String()] {
			t.Fatalf("duplicate slug %q", f)
		}
		seen[f.String()] = true
		if p, ok := ParseFanoutPolicy(f.String()); !ok || p != f {
			t.Fatalf("ParseFanoutPolicy(%q) = %v %v", f, p, ok)
		}
	}
	if FanoutAuto.Resolve() != FanoutAutoKind || FanoutPolicy(99).Valid() ||
		FanoutPolicy(99).String() != "fanout(?)" || FanoutPolicy(99).Ident() != "" {
		t.Fatal("FanoutAuto/invalid")
	}
	if _, ok := ParseFanoutPolicy("nope"); ok {
		t.Fatal("ParseFanoutPolicy junk")
	}
	if !Atomic.AllOrNothing() || Independent.AllOrNothing() ||
		BestEffort.AllOrNothing() || DropSlow.AllOrNothing() {
		t.Fatal("AllOrNothing")
	}
}

// TestAdmissionMask exercises the reservation mask past its inline capacity,
// which is where a topic with more subscriptions than the words hold falls
// back to an allocated one.
func TestAdmissionMask(t *testing.T) {
	for _, n := range []int{1, 64, 256, 257, 1000} {
		var adm admission
		adm.reset(n)
		for i := range n {
			if adm.has(i) {
				t.Fatalf("n=%d: bit %d set on a fresh mask", n, i)
			}
		}
		for i := 0; i < n; i += 3 {
			adm.set(i)
		}
		for i := range n {
			if adm.has(i) != (i%3 == 0) {
				t.Fatalf("n=%d: bit %d = %v", n, i, adm.has(i))
			}
		}
		if want := (n + 2) / 3; adm.admitted != want {
			t.Fatalf("n=%d: admitted %d, want %d", n, adm.admitted, want)
		}
		if adm.all() != (n == 1) {
			t.Fatalf("n=%d: all() = %v", n, adm.all())
		}
	}
}

// TestAutoBindingMatchesADR checks each of the engine's families against the
// ADR that will record its bake-off.
func TestAutoBindingMatchesADR(t *testing.T) {
	for _, f := range []struct {
		family string
		auto   string
		prov   bool
	}{
		{"arch", ArchAutoKind.String(), ArchAutoProvisional},
		{"fanout", FanoutAutoKind.String(), FanoutAutoProvisional},
		{"retry", RetryLaneAutoKind.String(), RetryLaneAutoProvisional},
	} {
		slug, bound, err := testguard.ADRAutoBinding("../..", f.family)
		if err != nil {
			t.Fatal(err)
		}
		if !bound {
			t.Logf("no ADR binds %s yet; auto=%v (provisional=%v)", f.family, f.auto, f.prov)
			continue
		}
		if slug != f.auto {
			t.Fatalf("ADR binds %s to %q but the auto binding is %q", f.family, slug, f.auto)
		}
	}
}

// TestSharedArchRefusesPerSubscriptionCapacity: under a shared partition log
// one reservation admits a record for every subscription, so a
// per-subscription capacity is not a quantity the architecture has. Asking
// for one is refused rather than silently ignored
// (adr/ADR-017-assumptions.md).
func TestSharedArchRefusesPerSubscriptionCapacity(t *testing.T) {
	for _, a := range Arches() {
		t.Run(a.String(), func(t *testing.T) {
			e := newTestEngine(t, KindSet{Arch: a}, TopologyConfig{Partitions: 1})
			top, err := e.CreateTopic("t", TopologyConfig{})
			if err != nil {
				t.Fatal(err)
			}
			for _, cfg := range []SubscriptionConfig{
				{CapacityMessages: 4},
				{CapacityBytes: 64},
			} {
				_, err := top.Subscribe("s", cfg)
				if a.Shared() {
					if !errors.Is(err, ErrInvalidConfig) {
						t.Fatalf("%+v under a shared log: %v", cfg, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%+v: %v", cfg, err)
				}
			}
			// The deadline is a per-delivery quantity and is honoured under
			// every architecture.
			sub, err := top.Subscribe("d", SubscriptionConfig{AckDeadline: time.Minute})
			if err != nil {
				t.Fatalf("AckDeadline under %v: %v", a, err)
			}
			if sub.ackDeadline != time.Minute {
				t.Fatalf("AckDeadline is %v", sub.ackDeadline)
			}
		})
	}
}
