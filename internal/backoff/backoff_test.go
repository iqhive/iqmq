package backoff

import (
	"strings"
	"testing"
	"time"

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
		if New(k).Kind() != k {
			t.Fatalf("New(%v).Kind() = %v", k, New(k).Kind())
		}
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Valid() || AutoKind == Auto || Auto.String() != "auto" {
		t.Fatal("Auto binding")
	}
	if New(Auto).Kind() != AutoKind {
		t.Fatal("New(Auto) does not resolve")
	}
	if None.Jittered() || Constant.Jittered() || !Full.Jittered() || !Equal.Jittered() || !Decorrelated.Jittered() {
		t.Fatal("Jittered")
	}
	if !Decorrelated.UsesPrev() || Full.UsesPrev() || None.UsesPrev() {
		t.Fatal("UsesPrev")
	}
	if None.Exponential() || Constant.Exponential() || !Full.Exponential() {
		t.Fatal("Exponential")
	}
	if Kind(9).Valid() || Kind(9).Ident() != "" || Kind(9).String() != "kind(?)" {
		t.Fatal("invalid kind")
	}
	if _, ok := ParseKind("x"); ok {
		t.Fatal("ParseKind junk")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("NewWith(invalid) should panic")
			}
		}()
		NewWith(Kind(9), Config{})
	}()
}

func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "backoff")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds backoff yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds backoff to %q but AutoKind is %q", slug, AutoKind)
	}
}

var sink time.Duration

// TestAllocs: a retry decision allocates nothing, whichever kind is bound.
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			p := New(k)
			if a := testing.AllocsPerRun(200, func() { sink += p.Next(3, sink, 0x9E3779B97F4A7C15) }); a != 0 {
				t.Fatalf("Next: %.1f allocs/op", a)
			}
			if a := testing.AllocsPerRun(200, func() { sink += p.Interval(7) }); a != 0 {
				t.Fatalf("Interval: %.1f allocs/op", a)
			}
		})
	}
}

// TestSchedule pins the documented schedule of each kind against
// hand-computed values, so a change in the arithmetic is visible.
func TestSchedule(t *testing.T) {
	cfg := Config{Base: 100 * time.Millisecond, Max: time.Second, Growth: 2}
	want := map[Kind][]time.Duration{
		None:         {0, 0, 0, 0, 0, 0},
		Constant:     {100e6, 100e6, 100e6, 100e6, 100e6, 100e6},
		Full:         {100e6, 200e6, 400e6, 800e6, 1e9, 1e9},
		Equal:        {100e6, 200e6, 400e6, 800e6, 1e9, 1e9},
		Decorrelated: {100e6, 200e6, 400e6, 800e6, 1e9, 1e9},
	}
	for k, ivs := range want {
		p := NewWith(k, cfg)
		for i, w := range ivs {
			if got := p.Interval(i + 1); got != w {
				t.Fatalf("%v: Interval(%d) = %v, want %v", k, i+1, got, w)
			}
		}
	}
	// Full jitter with the extreme random words gives the envelope's ends.
	full := NewWith(Full, cfg)
	if got := full.Next(3, 0, 0); got != 0 {
		t.Fatalf("full jitter with word 0 = %v", got)
	}
	if got := full.Next(3, 0, uint64(400*time.Millisecond)); got != 400*time.Millisecond {
		t.Fatalf("full jitter at the top of the envelope = %v", got)
	}
	// Equal jitter's floor is half the interval.
	eq := NewWith(Equal, cfg)
	if got := eq.Next(3, 0, 0); got != 200*time.Millisecond {
		t.Fatalf("equal jitter with word 0 = %v", got)
	}
	// Decorrelated grows from prev and is capped.
	dec := NewWith(Decorrelated, cfg)
	if got := dec.Next(1, 0, 0); got != cfg.Base {
		t.Fatalf("decorrelated first attempt = %v", got)
	}
	if got := dec.Next(9, time.Hour, 0); got != cfg.Base {
		t.Fatalf("decorrelated floor = %v", got)
	}
	if got := dec.Next(9, time.Hour, ^uint64(0)>>1); got > cfg.Max {
		t.Fatalf("decorrelated exceeded the cap: %v", got)
	}
}

// TestStdlibOnly keeps the policies inside the layering rules.
func TestStdlibOnly(t *testing.T) {
	testguard.StdlibOnly(t, ".")
	testguard.NoRootImport(t, ".")
}
