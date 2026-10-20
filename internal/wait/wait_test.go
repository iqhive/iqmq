package wait

import (
	"context"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/testguard"
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
	if len(Kinds()) != 7 {
		t.Fatalf("Kinds() = %v", Kinds())
	}
	for _, k := range Kinds() {
		if !k.Valid() || k == Auto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if p, ok := ParseKind(k.String()); !ok || p != k {
			t.Fatalf("ParseKind(%q) = %v %v", k, p, ok)
		}
		if mustNew(t, k) == nil {
			t.Fatalf("New(%v) nil", k)
		}
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Valid() || AutoKind == Auto {
		t.Fatal("Auto binding")
	}
	if Kind(99).Valid() || Kind(99).Ident() != "" || Kind(99).String() != "kind(?)" {
		t.Fatal("invalid kind")
	}
	if _, ok := ParseKind("x"); ok {
		t.Fatal("ParseKind junk")
	}
	if Spin.Parks() || Yield.Parks() || !Chan.Parks() || !Hybrid.Parks() {
		t.Fatal("Parks")
	}
	if Spin.WakesOne() || EventCount.WakesOne() || !Chan.WakesOne() || !Cond.WakesOne() || !SpinPark.WakesOne() {
		t.Fatal("WakesOne")
	}
	if _, err := New(Kind(99)); err == nil {
		t.Fatal("New(invalid) should fail")
	}
}

func mustNew(tb testing.TB, k Kind) Strategy {
	tb.Helper()
	s, err := New(k)
	if err != nil {
		tb.Fatalf("New(%v): %v", k, err)
	}
	return s
}

func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "wait")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds wait yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds wait to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestAllocs pins the steady-state Prepare/Notify/Wait cycle. Cond allocates
// its cancellation hook and EventCount a generation per notification; the
// other kinds must not allocate.
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			s := mustNew(t, k)
			ctx := context.Background()
			cycle := func() {
				tk := s.Prepare()
				s.Notify()
				if err := s.Wait(ctx, tk); err != nil {
					t.Fatal(err)
				}
			}
			for range 100 {
				cycle()
			}
			allocs := testing.AllocsPerRun(1000, cycle)
			t.Logf("%v: %.2f allocs/cycle", k, allocs)
			if allocs != 0 && k != Cond && k != EventCount {
				t.Fatalf("%v: %.2f allocs per cycle", k, allocs)
			}
		})
	}
}
