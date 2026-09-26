package timer

import (
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

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
	if len(Kinds()) != 6 {
		t.Fatalf("Kinds() = %v", Kinds())
	}
	for _, k := range Kinds() {
		if !k.Valid() || k == Auto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if p, ok := ParseKind(k.String()); !ok || p != k {
			t.Fatalf("ParseKind(%q) = %v %v", k, p, ok)
		}
		s := New[int](k, 0)
		if s.Resolution() != k.Resolution() {
			t.Fatalf("%v: Resolution mismatch", k)
		}
		if _, ok := s.Next(); ok || s.Len() != 0 {
			t.Fatalf("%v: empty scheduler", k)
		}
	}
	if Heap.Resolution() != 0 || HierarchicalWheel.Resolution() != time.Millisecond || Calendar.Resolution() != 0 {
		t.Fatal("Resolution")
	}
	if !Heap.Ordered() || !Calendar.Ordered() || HashedWheel.Ordered() || HybridHeapWheel.Ordered() {
		t.Fatal("Ordered")
	}
	if Auto.Resolve() != AutoKind || Kind(99).Valid() || Kind(99).String() != "kind(?)" || Kind(99).Ident() != "" {
		t.Fatal("Auto/invalid")
	}
	if _, ok := ParseKind("nope"); ok {
		t.Fatal("ParseKind junk")
	}
	if ceilTick(0) != 0 || ceilTick(1) != 1 || ceilTick(int64(tick)) != 1 || ceilTick(int64(tick)+1) != 2 || ceilTick(-1) != 0 || ceilTick(-int64(tick)-1) != -1 {
		t.Fatal("ceilTick")
	}
	if floorTick(0) != 0 || floorTick(int64(tick)-1) != 0 || floorTick(int64(tick)) != 1 || floorTick(-1) != -1 || floorTick(-int64(tick)) != -1 {
		t.Fatal("floorTick")
	}
	// The ends of the range: rounding must not wrap (a wrapped ceilTick
	// makes a wheel fire a math.MaxInt64 entry immediately).
	maxTick := int64(math.MaxInt64) / int64(tick)
	if ceilTick(math.MaxInt64) != maxTick+1 || floorTick(math.MaxInt64) != maxTick {
		t.Fatalf("ceilTick/floorTick(MaxInt64) = %d/%d", ceilTick(math.MaxInt64), floorTick(math.MaxInt64))
	}
	minTick := int64(math.MinInt64) / int64(tick)
	if ceilTick(math.MinInt64) != minTick || floorTick(math.MinInt64) != minTick-1 {
		t.Fatalf("ceilTick/floorTick(MinInt64) = %d/%d", ceilTick(math.MinInt64), floorTick(math.MinInt64))
	}
	if tickTime(2) != 2*int64(tick) || tickTime(-2) != -2*int64(tick) ||
		tickTime(maxTick+1) != math.MaxInt64 || tickTime(minTick-1) != math.MinInt64 {
		t.Fatal("tickTime")
	}
	if tickNone >= ceilTick(math.MinInt64) {
		t.Fatal("tickNone collides with a real tick")
	}
}

// TestExtremeDue pins the ends of the due range. No kind may fire an entry
// early there, and Next must never wrap to a time in the past: a due within
// one tick of math.MaxInt64 rounds up past the representable range, so a
// wheel kind never reaches it (the "never" such a sentinel asks for) while
// the exact kinds fire it at now == due.
func TestExtremeDue(t *testing.T) {
	maxRounded := int64(math.MaxInt64) / int64(tick) * int64(tick)
	for _, k := range Kinds() {
		for _, due := range []int64{math.MaxInt64, math.MaxInt64 - 1000, maxRounded, 1 << 62} {
			s := New[int](k, 0)
			e := &Entry[int]{}
			s.Add(e, due)
			for _, now := range []int64{0, int64(time.Hour), 1 << 40, 1 << 61} {
				if got := s.Fire(now, nil); len(got) != 0 {
					t.Fatalf("%v: due=%d fired early at now=%d", k, due, now)
				}
				if next, ok := s.Next(); !ok || next < now {
					t.Fatalf("%v: due=%d Next=%d ok=%v at now=%d", k, due, next, ok, now)
				}
			}
			if !s.Cancel(e) || s.Len() != 0 {
				t.Fatalf("%v: due=%d not cancellable", k, due)
			}
			s.Add(e, due)
			reachable := k.Resolution() == 0 || ceilTick(due) <= floorTick(math.MaxInt64)
			if got := len(s.Fire(math.MaxInt64, nil)) == 1; got != reachable {
				t.Fatalf("%v: due=%d fired at MaxInt64 = %v, want %v", k, due, got, reachable)
			}
		}
		// The other end is simply already past.
		s := New[int](k, 0)
		e := &Entry[int]{}
		s.Add(e, math.MinInt64)
		if out := s.Fire(0, nil); len(out) != 1 || out[0] != e {
			t.Fatalf("%v: due=MinInt64 fired %d entries", k, len(out))
		}
	}
}

func TestPanics(t *testing.T) {
	expectPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic", name)
			}
		}()
		fn()
	}
	expectPanic("invalid kind", func() { New[int](Kind(99), 0) })
	for _, k := range Kinds() {
		s := New[int](k, 0)
		e := &Entry[int]{}
		s.Add(e, 10)
		expectPanic(k.String()+" double add", func() { s.Add(e, 20) })
	}
}

func TestEntryAccessors(t *testing.T) {
	for _, k := range Kinds() {
		s := New[string](k, 0)
		e := &Entry[string]{Value: "x"}
		if e.Scheduled() || e.Gen != 0 {
			t.Fatal("fresh entry")
		}
		s.Add(e, 5*int64(tick))
		if !e.Scheduled() || e.Due() != 5*int64(tick) || e.Gen != 1 {
			t.Fatalf("%v: after Add: scheduled=%v due=%d gen=%d", k, e.Scheduled(), e.Due(), e.Gen)
		}
		if !s.Cancel(e) || e.Scheduled() || e.Gen != 2 || s.Cancel(e) {
			t.Fatalf("%v: Cancel", k)
		}
		s.Add(e, 0)
		out := s.Fire(int64(tick), nil)
		if len(out) != 1 || out[0] != e || e.Scheduled() || e.Gen != 4 || e.Value != "x" {
			t.Fatalf("%v: Fire = %v gen=%d", k, out, e.Gen)
		}
	}
}

func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "timer")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds timer yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds timer to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestAllocs pins Add, Cancel and Fire (into a preallocated slice) at zero
// allocations once the structures are warm.
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			s := New[int](k, 0)
			entries := make([]Entry[int], 64)
			dst := make([]*Entry[int], 0, 64)
			now := int64(0)
			cycle := func() {
				for i := range entries {
					s.Add(&entries[i], now+int64(i+1)*int64(tick))
				}
				s.Cancel(&entries[0])
				now += 100 * int64(tick)
				dst = s.Fire(now, dst[:0])
				if len(dst) != 63 {
					t.Fatalf("fired %d", len(dst))
				}
			}
			cycle()
			cycle()
			if allocs := testing.AllocsPerRun(20, cycle); allocs != 0 {
				t.Fatalf("%v: %.2f allocs per cycle", k, allocs)
			}
		})
	}
}

// TestFireNeverInvertsDueOrder is the property the memory engine's strict
// ordering rests on, and it holds for every kind including the three whose
// Ordered() is false: a kind may return one firing's entries in any order,
// but it never fires an entry while an entry with an earlier or equal due
// time is still armed. Rounding cannot break it, because ceilTick is
// non-decreasing in the due time -- whenever a wheel's target reaches the
// later entry's tick it has already reached the earlier one's -- and a
// caller that sorts each firing therefore sees a globally non-decreasing due
// order under any kind.
//
// Watched failing by firing slot by slot in hashedWheel.visit without the
// tick test, which fires a later lap's entry ahead of this lap's.
func TestFireNeverInvertsDueOrder(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			for _, dist := range []string{"uniform", "skewed", "burst"} {
				const n = 4000
				rng := rand.New(rand.NewPCG(uint64(k), 7))
				base := int64(1 << 40)
				offs := dues(n, dist, base, rng)
				s := New[int](k, base)
				entries := make([]Entry[int], n)
				armed := make(map[*Entry[int]]int64, n)
				for i := range entries {
					s.Add(&entries[i], offs[i])
					armed[&entries[i]] = offs[i]
				}
				var dst []*Entry[int]
				// Advance in irregular steps so a firing spans one tick,
				// several ticks and more than a whole lap of every wheel.
				for step := 1; len(armed) > 0; step++ {
					now := base + int64(step)*int64(step)*int64(37*time.Microsecond)
					dst = s.Fire(now, dst[:0])
					fired := int64(math.MinInt64)
					for _, e := range dst {
						if armed[e] > fired {
							fired = armed[e]
						}
						delete(armed, e)
					}
					if len(dst) == 0 {
						continue
					}
					for e, due := range armed {
						if due <= fired {
							t.Fatalf("%v/%s: fired an entry due=%d while due=%d was still armed at now=%d (gen %d)",
								k, dist, fired, due, now, e.Gen)
						}
					}
				}
			}
		})
	}
}
