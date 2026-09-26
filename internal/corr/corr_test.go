package corr

import (
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/iqhive/iqmq/internal/testguard"
)

// call stands in for the waiter a request/reply client parks: the store only
// ever moves the pointer, so the tests can check identity.
type call struct {
	id    ID
	reply chan struct{}
}

func newCall(id ID) *call { return &call{id: id, reply: make(chan struct{}, 1)} }

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
		s := New[*call](k, Config{})
		if s.Kind() != k || s.Len() != 0 {
			t.Fatalf("New(%v): kind %v len %d", k, s.Kind(), s.Len())
		}
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Valid() || AutoKind == Auto || Auto.String() != "auto" {
		t.Fatal("Auto binding")
	}
	if Kind(99).Valid() || Kind(99).Ident() != "" || Kind(99).String() != "kind(?)" {
		t.Fatal("invalid kind rendering")
	}
	if _, ok := ParseKind("nope"); ok {
		t.Fatal("ParseKind accepted junk")
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
	expectPanic("invalid kind", func() { New[*call](Kind(99), Config{}) })
	expectPanic("negative capacity", func() { New[*call](Sharded, Config{Capacity: -1}) })
	expectPanic("negative shards", func() { New[*call](Sharded, Config{Shards: -1}) })
	if s := New[*call](Auto, Config{}); s.Kind() != AutoKind {
		t.Fatalf("New(Auto).Kind() = %v", s.Kind())
	}
}

func TestConfig(t *testing.T) {
	n := Config{}.Normalize()
	if n.Capacity != DefaultCapacity || n.Shards != DefaultShards {
		t.Fatalf("zero config normalised to %+v", n)
	}
	if n := (Config{Capacity: 5, Shards: 5}).Normalize(); n.Capacity != 5 || n.Shards != 8 {
		t.Fatalf("shards not rounded up: %+v", n)
	}
	if err := (Config{Capacity: 1, Shards: 1}).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

// TestSemantics walks the store contract for one kind at a time.
func TestSemantics(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			s := New[*call](k, Config{Capacity: 4, Shards: 2})
			c1, c2 := newCall(1), newCall(2)
			if !s.Put(1, 100, c1) || !s.Put(2, 200, c2) {
				t.Fatal("Put refused a fresh id")
			}
			if s.Len() != 2 {
				t.Fatalf("Len = %d", s.Len())
			}
			// Zero ids and duplicates are refused.
			if s.Put(0, 100, c1) {
				t.Fatal("Put accepted the zero id")
			}
			if s.Put(1, 300, newCall(1)) {
				t.Fatal("Put accepted a duplicate id")
			}
			// Take is exact and removes.
			if got, ok := s.Take(1); !ok || got != c1 {
				t.Fatalf("Take(1) = %v %v", got, ok)
			}
			if _, ok := s.Take(1); ok {
				t.Fatal("Take twice matched a reply twice")
			}
			if _, ok := s.Take(99); ok {
				t.Fatal("Take of an unknown id matched")
			}
			if s.Len() != 1 {
				t.Fatalf("Len after Take = %d", s.Len())
			}
			// Expire frees exactly the entries whose deadline has passed.
			if !s.Put(3, 150, newCall(3)) || !s.Put(4, 250, newCall(4)) {
				t.Fatal("Put refused")
			}
			var dst []Pending[*call]
			dst = s.Expire(100, dst)
			if len(dst) != 0 {
				t.Fatalf("Expire(100) returned %d entries", len(dst))
			}
			dst = s.Expire(200, dst)
			if len(dst) != 2 {
				t.Fatalf("Expire(200) returned %d entries, want 2", len(dst))
			}
			ids := map[ID]bool{}
			for _, p := range dst {
				ids[p.ID] = true
				if p.Value.id != p.ID || p.Deadline > 200 {
					t.Fatalf("expired entry %+v does not match its value", p)
				}
			}
			if !ids[2] || !ids[3] {
				t.Fatalf("expired the wrong entries: %v", ids)
			}
			if s.Len() != 1 {
				t.Fatalf("Len after Expire = %d", s.Len())
			}
			if _, ok := s.Take(2); ok {
				t.Fatal("an expired entry was still takeable")
			}
			// A far-future sweep drains the store: this is what Close does.
			dst = s.Expire(math.MaxInt64, dst[:0])
			if len(dst) != 1 || dst[0].ID != 4 || s.Len() != 0 {
				t.Fatalf("drain returned %d entries, Len %d", len(dst), s.Len())
			}
			// Capacity: the store refuses beyond it and accepts again after a
			// Take frees a slot.
			for i := ID(10); i < 14; i++ {
				if !s.Put(i, 1000, newCall(i)) {
					t.Fatalf("Put(%d) refused below capacity (Len %d)", i, s.Len())
				}
			}
			if s.Put(14, 1000, newCall(14)) {
				t.Fatal("Put accepted beyond capacity")
			}
			if _, ok := s.Take(10); !ok {
				t.Fatal("Take(10)")
			}
			if !s.Put(14, 1000, newCall(14)) {
				t.Fatal("Put refused after a Take freed a slot")
			}
		})
	}
}

// TestShardCounts checks the Sharded kind at both ends of its range: one
// shard is the plain locked map the bake-off starts from.
func TestShardCounts(t *testing.T) {
	for _, shards := range []int{1, 2, 64} {
		s := New[*call](Sharded, Config{Capacity: 256, Shards: shards})
		for i := ID(1); i <= 200; i++ {
			if !s.Put(i, int64(i), newCall(i)) {
				t.Fatalf("shards=%d: Put(%d)", shards, i)
			}
		}
		if s.Len() != 200 {
			t.Fatalf("shards=%d: Len = %d", shards, s.Len())
		}
		var dst []Pending[*call]
		dst = s.Expire(100, dst)
		if len(dst) != 100 {
			t.Fatalf("shards=%d: expired %d", shards, len(dst))
		}
		for i := ID(101); i <= 200; i++ {
			if got, ok := s.Take(i); !ok || got.id != i {
				t.Fatalf("shards=%d: Take(%d) = %v %v", shards, i, got, ok)
			}
		}
		if s.Len() != 0 {
			t.Fatalf("shards=%d: Len = %d after draining", shards, s.Len())
		}
	}
}

// TestTakeExactlyOnceUnderRace runs many takers and one sweeper against the
// same entries: a reply must be matched at most once, and never lost.
func TestTakeExactlyOnceUnderRace(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			const n = 2000
			s := New[*call](k, Config{Capacity: n, Shards: 8})
			for i := ID(1); i <= n; i++ {
				if !s.Put(i, int64(i), newCall(i)) {
					t.Fatalf("Put(%d)", i)
				}
			}
			var mu sync.Mutex
			seen := make(map[ID]int, n)
			record := func(id ID, c *call) {
				mu.Lock()
				defer mu.Unlock()
				if c.id != id {
					t.Errorf("entry %d carried call %d", id, c.id)
				}
				seen[id]++
			}
			var wg sync.WaitGroup
			for w := range 8 {
				wg.Go(func() {
					for i := ID(1 + w); i <= n; i += 8 {
						if c, ok := s.Take(i); ok {
							record(i, c)
						}
					}
				})
			}
			wg.Go(func() {
				var dst []Pending[*call]
				for range 50 {
					dst = s.Expire(math.MaxInt64, dst[:0])
					for _, p := range dst {
						record(p.ID, p.Value)
					}
				}
			})
			wg.Wait()
			var dst []Pending[*call]
			for _, p := range s.Expire(math.MaxInt64, dst) {
				record(p.ID, p.Value)
			}
			if len(seen) != n {
				t.Fatalf("%d of %d entries came back", len(seen), n)
			}
			for id, count := range seen {
				if count != 1 {
					t.Fatalf("entry %d came back %d times", id, count)
				}
			}
			if s.Len() != 0 {
				t.Fatalf("Len = %d after draining", s.Len())
			}
		})
	}
}

// TestAllocs pins the request/reply hot path: a Put and the matching Take
// must not allocate. SyncMap is the exception — it stores its entry behind a
// pointer, which is the cost the bake-off weighs against its lock-free reads.
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			s := New[*call](k, Config{Capacity: 1024})
			c := newCall(1)
			var id ID
			cycle := func() {
				id++
				if id == 0 {
					id = 1
				}
				if !s.Put(id, 1<<40, c) {
					t.Fatalf("Put(%d) refused", id)
				}
				if _, ok := s.Take(id); !ok {
					t.Fatalf("Take(%d) missed", id)
				}
			}
			for range 1000 {
				cycle()
			}
			allocs := testing.AllocsPerRun(1000, cycle)
			t.Logf("%v: %.2f allocs per put/take", k, allocs)
			if allocs != 0 && k != SyncMap {
				t.Fatalf("%v: %.2f allocs per put/take", k, allocs)
			}
			// A sweep into a reused buffer allocates nothing, whatever it
			// finds: entries to expire, nothing at all, or entries that are
			// all still live.
			const n = 64
			calls := make([]*call, n)
			for i := range calls {
				calls[i] = newCall(ID(i + 1))
			}
			fill := func(deadline int64) {
				for i, c := range calls {
					if !s.Put(ID(i+1), deadline, c) {
						t.Fatalf("Put(%d) refused", i+1)
					}
				}
			}
			for _, sc := range []sweepCase{
				{what: "that expires 64 entries", prepare: func() { fill(10) }, expires: n},
				{what: "over an empty table", prepare: func() {}, expires: 0},
				{what: "over 64 entries none of which is due", setup: func() { fill(1000) }, prepare: func() {}, expires: 0},
			} {
				totals := sweepMallocs(t, s, sc, 100, 3)
				t.Logf("%v: a sweep %s: %v allocations over 100 sweeps, per attempt", k, sc.what, totals)
				if slices.Min(totals) != 0 {
					t.Fatalf("%v: a sweep %s into a reused buffer allocated %v times over 100 sweeps, per attempt: "+
						"in every one of them, so it is Expire's", k, sc.what, totals)
				}
			}
		})
	}
}

// sweepCase is one shape of table a sweep can find.
type sweepCase struct {
	what    string
	setup   func() // once, before the first sweep
	prepare func() // before every sweep, outside the counted window
	expires int    // how many entries every sweep must expire
}

// sweepMallocs counts the heap allocations Expire makes: the TOTAL over runs
// sweeps into one reused buffer, each counted from just before the sweep to
// just after it, so that an allocation on any one sweep fails the caller -- an
// average, which is what testing.AllocsPerRun reports, lets through a sweep
// that allocates on every other call. The table is prepared outside the
// counted windows, and a sweep that does not expire what its case says fails
// here, so every counted sweep did the work the assertion is about.
//
// The total is taken attempts times, every one of them returned, and the
// caller fails only when none is zero, because runtime.MemStats.Mallocs is
// process-wide: an allocation anything else in the process makes has to land
// in a counted window of every attempt to fail the test. What that gives up is
// an allocation Expire makes on SOME sweeps only -- one on the fiftieth call,
// or one every few hundred -- which can leave an attempt at zero and pass. An
// allocation on at least one sweep in every hundred fails, and so does one on
// every sweep, which is the shape a reused buffer exists to prevent.
// The per-attempt totals are logged either way, so a
// failure, or a pass with a non-zero attempt, shows whether the counts agree.
//
// It replaces AllocsPerRun(1, sweep), which measured nothing and failed
// anyway. AllocsPerRun calls its function once before it starts counting,
// and that warm-up sweep drained the 64 entries the test had placed, so the
// one counted sweep walked an empty table: an Expire that allocated on every
// entry it returned passed it. And one counted run is charged with every
// allocation anything in the process makes while it runs: GitHub run
// 35560561256 (go 1.26.x, a 2-core runner) failed it with "allocated 1.00"
// over that empty table, where lockfreeoa's Expire has no allocation to make
// (its only allocating calls, growslice and moveSliceNoCap, need an entry to
// append or a slice in its own frame). The put/take measurement above averages
// over 1000 runs, which divides such a stray to zero, and that is exactly what
// that run's log shows: "0.00 allocs per put/take", then the sweep's 1.00.
func sweepMallocs(t *testing.T, s Store[*call], sc sweepCase, runs, attempts int) []uint64 {
	t.Helper()
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	if sc.setup != nil {
		sc.setup()
	}
	// The first sweep grows the buffer to its working size, which is the one
	// allocation a reused buffer exists to pay once, and pays any first-use
	// cost the kind has.
	sc.prepare()
	dst := s.Expire(20, nil)
	var ms runtime.MemStats
	totals := make([]uint64, 0, attempts)
	for range attempts {
		var total uint64
		for range runs {
			sc.prepare()
			runtime.ReadMemStats(&ms)
			before := ms.Mallocs
			dst = s.Expire(20, dst[:0])
			runtime.ReadMemStats(&ms)
			total += ms.Mallocs - before
			if len(dst) != sc.expires {
				t.Fatalf("a sweep %s expired %d entries, want %d; the measurement is of a sweep that did not do "+
					"the work the case is about", sc.what, len(dst), sc.expires)
			}
		}
		totals = append(totals, total)
	}
	return totals
}

// TestAutoBindingMatchesADR checks that when an ADR binds the corr family
// (front-matter `auto: {corr: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "corr")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds corr yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds corr to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestLayering keeps the package on the standard library and off the root.
func TestLayering(t *testing.T) {
	testguard.NoRootImport(t, ".")
	testguard.StdlibOnly(t, ".")
}
