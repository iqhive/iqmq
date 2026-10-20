package registry

import (
	"strings"
	"testing"
	"unsafe"

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
	if len(Kinds()) != 4 {
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
		if mustNew[int](t, k) == nil {
			t.Fatalf("New(%v) nil", k)
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
	if !RWMutexMap.RangeSnapshot() || !COW.RangeSnapshot() || SyncMap.RangeSnapshot() || Sharded.RangeSnapshot() {
		t.Fatal("RangeSnapshot")
	}
	if RWMutexMap.LockFreeLoad() || !COW.LockFreeLoad() || !SyncMap.LockFreeLoad() || Sharded.LockFreeLoad() {
		t.Fatal("LockFreeLoad")
	}
	if !RWMutexMap.ExactLen() || !COW.ExactLen() || SyncMap.ExactLen() || Sharded.ExactLen() {
		t.Fatal("ExactLen")
	}
	if Auto.RangeSnapshot() != AutoKind.RangeSnapshot() || Auto.LockFreeLoad() != AutoKind.LockFreeLoad() ||
		Auto.ExactLen() != AutoKind.ExactLen() {
		t.Fatal("Auto properties must resolve")
	}
}

func TestNewRejectsInvalidKind(t *testing.T) {
	if _, err := New[int](Kind(99)); err == nil {
		t.Fatal("New(invalid) should fail")
	}
}

func mustNew[V any](tb testing.TB, k Kind) Registry[V] {
	tb.Helper()
	r, err := New[V](k)
	if err != nil {
		tb.Fatalf("New(%v): %v", k, err)
	}
	return r
}

func TestIntern(t *testing.T) {
	a, b := Intern("orders"), Intern("or"+"ders")
	if a != b || a.Value() != "orders" {
		t.Fatal("Intern must return equal handles for equal names")
	}
	if Intern("orders") == Intern("orders2") {
		t.Fatal("distinct names collide")
	}
	if hashKey(a) != hashKey(b) {
		t.Fatal("hashKey differs for equal keys")
	}
	// The hash must spread distinct keys over shards rather than
	// collapsing onto a few: with 1024 keys and 32 shards, no shard should
	// be empty and none should hold more than four times its fair share.
	counts := make([]int, DefaultShards)
	for i := range 1024 {
		counts[hashKey(Intern("topic-"+itoa(i)))&(DefaultShards-1)]++
	}
	for i, c := range counts {
		if c == 0 || c > 4*1024/DefaultShards {
			t.Fatalf("shard %d holds %d of 1024 keys: %v", i, c, counts)
		}
	}
}

func TestHelpers(t *testing.T) {
	for n, want := range map[int]int{0: 1, 1: 1, 2: 2, 3: 4, 4: 4, 5: 8, 31: 32, 32: 32, 33: 64} {
		if got := pow2ceil(n); got != want {
			t.Errorf("pow2ceil(%d) = %d, want %d", n, got, want)
		}
	}
	for _, n := range []int{0, 1, 3, 4, 5, 16} {
		s := newSharded[int](n)
		if len(s.shards) != pow2ceil(max(n, 1)) || int(s.mask) != len(s.shards)-1 {
			t.Errorf("newSharded(%d): %d shards mask %d", n, len(s.shards), s.mask)
		}
	}
	if unsafe.Sizeof(shard[int]{}) < cacheLine {
		t.Fatalf("shard is %d bytes; adjacent shards would share a cache line", unsafe.Sizeof(shard[int]{}))
	}
}

// TestAllocs pins the hot lookups: Load (hit and miss), LoadOrStore on a
// present key and Delete of an absent key allocate nothing for every kind,
// and hashing an interned key allocates nothing.
func TestAllocs(t *testing.T) {
	present, absent := Intern("present"), Intern("absent")
	if n := testing.AllocsPerRun(1000, func() { _ = hashKey(present) }); n != 0 {
		t.Fatalf("hashKey: %.2f allocs/op", n)
	}
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			r := mustNew[*int](t, k)
			v := new(int)
			r.LoadOrStore(present, v)
			check := func(name string, fn func()) {
				t.Helper()
				if n := testing.AllocsPerRun(1000, fn); n != 0 {
					t.Errorf("%v %s: %.2f allocs/op", k, name, n)
				}
			}
			check("Load hit", func() {
				if got, ok := r.Load(present); !ok || got != v {
					t.Fatal("Load hit")
				}
			})
			check("Load miss", func() {
				if _, ok := r.Load(absent); ok {
					t.Fatal("Load miss")
				}
			})
			check("LoadOrStore hit", func() {
				if got, loaded := r.LoadOrStore(present, nil); !loaded || got != v {
					t.Fatal("LoadOrStore hit")
				}
			})
			check("Delete miss", func() {
				if _, ok := r.Delete(absent); ok {
					t.Fatal("Delete miss")
				}
			})
			check("Len", func() {
				if r.Len() != 1 {
					t.Fatal("Len")
				}
			})
		})
	}
}

// TestAutoBindingMatchesADR checks that when an ADR binds the registry
// family (front-matter `auto: {registry: <slug>}`), AutoKind agrees.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "registry")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds registry yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds registry to %q but AutoKind is %q", slug, AutoKind)
	}
}

// itoa keeps tests free of strconv so they read like the package.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
