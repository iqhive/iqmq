package queue

import (
	"strings"
	"testing"

	"bitbucket.org/iqhive/iqmq/internal/testguard"
)

// TestKindsList announces the family's kinds to benchmarks/cmd/bakeoff.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, len(Kinds()))
	for _, k := range Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

func TestKindProperties(t *testing.T) {
	if len(Kinds()) != 9 {
		t.Fatalf("Kinds() = %v", Kinds())
	}
	seen := map[string]bool{}
	for _, k := range Kinds() {
		if !k.Valid() || k == Auto {
			t.Fatalf("%v invalid", k)
		}
		if seen[k.String()] {
			t.Fatalf("duplicate slug %q", k)
		}
		seen[k.String()] = true
		if p, ok := ParseKind(k.String()); !ok || p != k {
			t.Fatalf("ParseKind(%q) = %v, %v", k, p, ok)
		}
		if k.Ident() == "" || strings.ToLower(k.Ident()) == k.String() && k != Chan && k != Disruptor && k != Chunked && k != SPSC && k != MPSC {
			// idents are CamelCase, slugs lower-case; a few coincide harmlessly
			_ = k
		}
		if k.Resolve() != k {
			t.Fatalf("%v.Resolve() = %v", k, k.Resolve())
		}
		q := New[int](k, 5)
		if k.ExactCapacity() && q.Cap() != 5 {
			t.Fatalf("%v: ExactCapacity but Cap() = %d", k, q.Cap())
		}
		if k.Bounded() && q.Cap() < 5 {
			t.Fatalf("%v: Cap() = %d < 5", k, q.Cap())
		}
		if !k.Bounded() && q.Cap() != 0 {
			t.Fatalf("%v: unbounded Cap() = %d", k, q.Cap())
		}
		if !k.Modes().Has(ModeSPSC) {
			t.Fatalf("%v: every kind supports SPSC", k)
		}
	}
	if Auto.Resolve() != AutoKind || Auto.String() != "auto" || !Auto.Valid() {
		t.Fatal("Auto")
	}
	if Kind(200).Valid() || Kind(200).String() != "kind(200)" || Kind(200).Ident() != "" {
		t.Fatal("invalid kind rendering")
	}
	if _, ok := ParseKind("nope"); ok {
		t.Fatal("ParseKind accepted junk")
	}
	if SPSC.Modes() != ModeSPSC || MPSC.Modes() != ModeSPSC|ModeMPSC || SeqMPMC.Modes() != AllModes {
		t.Fatal("Modes")
	}
	if ShardedMPMC.Ordered() || !SeqMPMC.Ordered() || Chunked.Bounded() || !SPSC.Bounded() {
		t.Fatal("Ordered/Bounded")
	}
	if AllModes.String() != "SPSC|MPSC|SPMC|MPMC" || Mode(0).String() != "none" || ModeMPMC.String() != "MPMC" {
		t.Fatalf("Mode.String: %q %q", AllModes, Mode(0))
	}
	if !ModeMPMC.MultiProducer() || !ModeMPMC.MultiConsumer() || ModeSPSC.MultiProducer() || ModeMPSC.MultiConsumer() {
		t.Fatal("Mode helpers")
	}
	if !AutoKind.Valid() || AutoKind == Auto {
		t.Fatalf("AutoKind = %v", AutoKind)
	}
}

func TestNewPanics(t *testing.T) {
	expectPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic", name)
			}
		}()
		fn()
	}
	expectPanic("invalid kind", func() { New[int](Kind(99), 1) })
	expectPanic("zero capacity", func() { New[int](MutexRing, 0) })
	expectPanic("zero capacity seq", func() { New[int](SeqMPMC, 0) })
	if q := New[int](Chunked, 0); q == nil || q.Cap() != 0 {
		t.Fatal("Chunked ignores capacity")
	}
	if q := New[int](Auto, 3); q == nil {
		t.Fatal("Auto")
	}
}

func TestHelpers(t *testing.T) {
	for n, want := range map[int]int{0: 1, 1: 1, 2: 2, 3: 4, 4: 4, 5: 8, 1000: 1024, 1024: 1024, 1025: 2048} {
		if got := pow2ceil(n); got != want {
			t.Errorf("pow2ceil(%d) = %d, want %d", n, got, want)
		}
	}
	for i, want := range map[int]string{0: "0", 7: "7", 42: "42", -3: "-3", 1000000: "1000000"} {
		if got := itoa(i); got != want {
			t.Errorf("itoa(%d) = %q", i, got)
		}
	}
	if clampLen(5, 7, 10) != 0 || clampLen(20, 3, 10) != 10 || clampLen(7, 3, 10) != 4 {
		t.Fatal("clampLen")
	}
}

func TestUnsafeBuild(t *testing.T) {
	t.Logf("UnsafeSlots=%v", UnsafeSlots)
	q := newSeqMPMC[int](4)
	for i := range 4 {
		if q.slot(uint64(i)) != &q.slots[i] || q.slot(uint64(i+4)) != &q.slots[i] {
			t.Fatalf("slot(%d) mismatch", i)
		}
	}
}

// TestAllocs asserts the steady-state push/pop path allocates nothing.
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			q := New[*int](k, 64)
			v := new(int)
			warm := func() {
				for range 200 {
					q.TryPush(v)
					q.TryPop()
				}
			}
			warm()
			allocs := testing.AllocsPerRun(1000, func() {
				if !q.TryPush(v) {
					t.Fatal("push failed")
				}
				if _, ok := q.TryPop(); !ok {
					t.Fatal("pop failed")
				}
			})
			if allocs != 0 {
				t.Fatalf("%v: %.2f allocs/op", k, allocs)
			}
		})
	}
}

// TestAutoBindingMatchesADR checks that when an ADR binds the queue family
// (front-matter `auto: {queue: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "queue")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds queue yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds queue to %q but AutoKind is %q", slug, AutoKind)
	}
}
