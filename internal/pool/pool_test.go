package pool

import (
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
	if len(Kinds()) != 5 {
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
		if New(k, Config[int]{}) == nil || NewBuffers(k, BufferConfig{}) == nil {
			t.Fatalf("New(%v) nil", k)
		}
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Valid() || AutoKind == Auto || Auto.String() != "auto" {
		t.Fatal("Auto binding")
	}
	if Kind(99).Valid() || Kind(99).Ident() != "" || Kind(99).String() != "kind(?)" {
		t.Fatal("invalid kind rendering")
	}
	if _, ok := ParseKind("x"); ok {
		t.Fatal("ParseKind junk")
	}
	if None.Retains() || !SyncPool.Retains() || !FreeList.Retains() || !Slab.Retains() {
		t.Fatal("Retains")
	}
	if !SyncPool.GCCleared() || FreeList.GCCleared() || None.GCCleared() {
		t.Fatal("GCCleared")
	}
	if !SizeClass.Classed() || !Slab.Classed() || FreeList.Classed() || SyncPool.Classed() || None.Classed() {
		t.Fatal("Classed")
	}
	if !Slab.Chunked() || SizeClass.Chunked() {
		t.Fatal("Chunked")
	}
	if !FreeList.Bounded() || !SizeClass.Bounded() || !Slab.Bounded() || SyncPool.Bounded() || None.Bounded() {
		t.Fatal("Bounded")
	}
	if Auto.Retains() != AutoKind.Retains() || Auto.Classed() != AutoKind.Classed() {
		t.Fatal("Auto forwards capability queries")
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
	expectPanic("objects", func() { New(Kind(99), Config[int]{}) })
	expectPanic("buffers", func() { NewBuffers(Kind(99), BufferConfig{}) })
	if New(Auto, Config[int]{}) == nil || NewBuffers(Auto, BufferConfig{}) == nil {
		t.Fatal("Auto")
	}
}

func TestHelpers(t *testing.T) {
	for n, want := range map[int]int{0: 1, 1: 1, 2: 2, 3: 4, 5: 8, 1000: 1024, 1024: 1024, 1025: 2048} {
		if got := pow2ceil(n); got != want {
			t.Errorf("pow2ceil(%d) = %d, want %d", n, got, want)
		}
	}
	for n, want := range map[int]int{0: 16, 1: 16, 16: 16, 17: 32, 100: 128, 65536: 65536} {
		if got := classOf(n); got != want {
			t.Errorf("classOf(%d) = %d, want %d", n, got, want)
		}
	}
	for c, want := range map[int]int{16: 0, 32: 1, 64: 2, 1024: 6, 65536: 12} {
		if got := classIndex(c); got != want {
			t.Errorf("classIndex(%d) = %d, want %d", c, got, want)
		}
	}
	for c, want := range map[int]int{0: 0, 15: 0, 16: 16, 17: 16, 31: 16, 32: 32, 100: 64, 65537: 65536} {
		if got := floorClass(c); got != want {
			t.Errorf("floorClass(%d) = %d, want %d", c, got, want)
		}
	}
	if n := stripeCount(); n < 1 || n > MaxStripes || n&(n-1) != 0 {
		t.Fatalf("stripeCount() = %d", n)
	}
	for _, maxIdle := range []int{1, 2, 3, 8, 17, 1000, 1024} {
		count, per := stripeLayout(maxIdle)
		if count < 1 || count*per > maxIdle && maxIdle >= count || per < 1 {
			t.Errorf("stripeLayout(%d) = %d×%d", maxIdle, count, per)
		}
	}
	b := make([]byte, 3, 8)
	if bufKey(b) != &b[:8][0] || bufKey(b[1:]) != &b[1] {
		t.Fatal("bufKey")
	}
}

func TestConfigDefaults(t *testing.T) {
	var c Config[int]
	if c.maxIdle() != DefaultMaxIdle || c.chunk() != DefaultChunk {
		t.Fatalf("Config defaults: %d %d", c.maxIdle(), c.chunk())
	}
	if (Config[int]{MaxIdle: 5, Chunk: 100}).chunk() != 5 {
		t.Fatal("chunk is capped at MaxIdle")
	}
	var bc BufferConfig
	if bc.maxIdle() != DefaultMaxIdle || bc.maxSize() != DefaultMaxSize || bc.chunk() != DefaultChunkBytes {
		t.Fatalf("BufferConfig defaults: %d %d %d", bc.maxIdle(), bc.maxSize(), bc.chunk())
	}
	if (BufferConfig{MaxSize: 1}).maxSize() != MinClass || (BufferConfig{MaxSize: 1000}).maxSize() != 1024 {
		t.Fatal("MaxSize rounding")
	}
}

// TestAllocs asserts that the steady-state Get/Put cycle of every retaining
// kind allocates nothing, for objects and for pooled buffer sizes, while
// None allocates exactly one object per Get. The debug canary allocates, so
// the test is meaningful in release builds only.
func TestAllocs(t *testing.T) {
	if Debug {
		t.Skip("iqmq_debug canaries allocate")
	}
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			want := 0.0
			if !k.Retains() {
				want = 1
			}
			objs := New(k, Config[item]{})
			cycle := func() { objs.Put(objs.Get()) }
			for range 100 {
				cycle()
			}
			if got := testing.AllocsPerRun(1000, cycle); got != want {
				t.Fatalf("%v objects: %.2f allocs/op, want %.0f", k, got, want)
			}
			bufs := NewBuffers(k, BufferConfig{})
			for _, n := range []int{64, 1024, DefaultMaxSize} {
				bcycle := func() { bufs.Put(bufs.Get(n)) }
				for range 100 {
					bcycle()
				}
				if got := testing.AllocsPerRun(1000, bcycle); got != want {
					t.Fatalf("%v buffers n=%d: %.2f allocs/op, want %.0f", k, n, got, want)
				}
			}
			big := func() { bufs.Put(bufs.Get(DefaultMaxSize + 1)) }
			if got := testing.AllocsPerRun(100, big); got != 1 {
				t.Fatalf("%v oversize buffers: %.2f allocs/op, want 1", k, got)
			}
		})
	}
}

// TestAutoBindingMatchesADR checks that when an ADR binds the pool family
// (front-matter `auto: {pool: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "pool")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds pool yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds pool to %q but AutoKind is %q", slug, AutoKind)
	}
}
