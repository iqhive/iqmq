package stats

import (
	"strings"
	"testing"
	"unsafe"

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
	if len(Kinds()) != 3 {
		t.Fatalf("Kinds() = %v", Kinds())
	}
	seen := map[string]bool{}
	for _, k := range Kinds() {
		if !k.Valid() || k == Auto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if seen[k.String()] {
			t.Fatalf("duplicate slug %q", k)
		}
		seen[k.String()] = true
		if p, ok := ParseKind(k.String()); !ok || p != k {
			t.Fatalf("ParseKind(%q) = %v %v", k, p, ok)
		}
		for _, w := range []int{-1, 0, 1, 3, 8, 64} {
			c := New(k, w)
			if c == nil || c.Load() != 0 {
				t.Fatalf("New(%v, %d) not a zero counter", k, w)
			}
			if k.Cells(w) < 1 || (w > 1 && k != Atomic && k.Cells(w) < w) {
				t.Fatalf("%v.Cells(%d) = %d", k, w, k.Cells(w))
			}
		}
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Valid() || AutoKind == Auto || Auto.String() != "auto" {
		t.Fatal("Auto binding")
	}
	if Kind(99).Valid() || Kind(99).Ident() != "" || Kind(99).String() != "kind(?)" {
		t.Fatal("invalid kind")
	}
	if _, ok := ParseKind("x"); ok {
		t.Fatal("ParseKind junk")
	}
	if Atomic.Cells(64) != 1 || Striped.Cells(5) != 8 || PerWorker.Cells(5) != 9 || Auto.Cells(3) != AutoKind.Cells(3) {
		t.Fatal("Cells")
	}
	if !Atomic.SharedAdd() || Striped.SharedAdd() || !PerWorker.SharedAdd() {
		t.Fatal("SharedAdd")
	}
}

func TestNewPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New(invalid) should panic")
		}
	}()
	New(Kind(99), 1)
}

func TestHelpers(t *testing.T) {
	for n, want := range map[int]int{0: 1, 1: 1, 2: 2, 3: 4, 4: 4, 5: 8, 63: 64, 64: 64, 65: 128} {
		if got := pow2ceil(n); got != want {
			t.Errorf("pow2ceil(%d) = %d, want %d", n, got, want)
		}
	}
	if unsafe.Sizeof(cell{}) != cacheLine {
		t.Fatalf("cell is %d bytes, want one cache line", unsafe.Sizeof(cell{}))
	}
	hits := make([]int, 8)
	for range 8000 {
		hits[pick(7)]++
	}
	for i, h := range hits {
		if h < 500 || h > 1500 {
			t.Fatalf("pick is not spreading: cell %d hit %d of 8000 (%v)", i, h, hits)
		}
	}
	if pick(0) != 0 {
		t.Fatal("pick(0) must return 0")
	}
}

// TestAllocs asserts that Add, AddAt and Load allocate nothing.
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			c := New(k, 8)
			check := func(name string, fn func()) {
				t.Helper()
				if n := testing.AllocsPerRun(1000, fn); n != 0 {
					t.Errorf("%v %s: %.2f allocs/op", k, name, n)
				}
			}
			check("Add", func() { c.Add(1) })
			check("AddAt", func() { c.AddAt(3, 1) })
			check("Load", func() { _ = c.Load() })
		})
	}
}

// TestAutoBindingMatchesADR checks that when an ADR binds the stats family
// (front-matter `auto: {stats: <slug>}`), AutoKind agrees.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "stats")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds stats yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds stats to %q but AutoKind is %q", slug, AutoKind)
	}
}
