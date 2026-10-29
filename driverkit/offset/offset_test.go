package offset

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/errcode"
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
		tr := mustNew(t, k, Config{})
		if tr.Kind() != k {
			t.Fatalf("New(%v).Kind() = %v", k, tr.Kind())
		}
		if tr.Watermark() != 0 || tr.Pending() != 0 || tr.Paused() || tr.Gen() != 1 {
			t.Fatalf("%v: fresh tracker wm=%d pending=%d paused=%v gen=%d", k, tr.Watermark(), tr.Pending(), tr.Paused(), tr.Gen())
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

func TestNewRejects(t *testing.T) {
	expectErr := func(name string, fn func() error) {
		t.Helper()
		if fn() == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
	expectErr("invalid kind", func() error { _, err := New(Kind(99), Config{}); return err })
	expectErr("negative base", func() error { _, err := New(MapScan, Config{Base: -1}); return err })
	expectErr("negative window", func() error { _, err := New(MapScan, Config{Window: -1}); return err })
	expectErr("pause above window", func() error { _, err := New(MapScan, Config{Window: 8, MaxPending: 9}); return err })
	if tr := mustNew(t, Auto, Config{}); tr.Kind() != AutoKind {
		t.Fatalf("New(Auto).Kind() = %v", tr.Kind())
	}
}

func TestConfig(t *testing.T) {
	n := Config{}.Normalize()
	if n.Window != DefaultWindow || n.MaxPending != DefaultWindow || n.MaxGap != DefaultWindow {
		t.Fatalf("zero config normalised to %+v", n)
	}
	n = Config{Base: 7, Window: 32, MaxPending: 4}.Normalize()
	if n.Base != 7 || n.Window != 32 || n.MaxPending != 4 || n.MaxGap != 32 {
		t.Fatalf("normalised to %+v", n)
	}
	for _, c := range []Config{{Base: -1}, {Window: -2}, {MaxPending: -1}, {MaxGap: -1}, {Window: 4, MaxGap: 5}} {
		if err := c.Validate(); !errors.Is(err, ErrInvalidConfig) || !errors.Is(err, errcode.New(errcode.InvalidConfig, "")) {
			t.Fatalf("%+v: Validate = %v", c, err)
		}
	}
	if err := (Config{Base: 10, Window: 64, MaxPending: 8, MaxGap: 16}).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

// overflowingWindow is the smallest window whose power of two does not fit in
// an int: 1<<62 + 1 on a 64-bit platform and 1<<30 + 1 on a 32-bit one, where a
// literal 1<<62 would not compile.
const overflowingWindow = math.MaxInt/2 + 2

// TestAWindowWiderThanTheBoundIsRefused: Validate has an upper bound, and New
// refuses a window above it for every kind.
//
// Without one, Kafka's TrackerWindow = math.MaxInt reached the bitset, whose
// ring size pow2ceil(Window) overflowed to a negative number that
// max(64, ...) turned into a 64-slot ring. Offsets 64 apart then shared a slot
// and the watermark passed a record still in flight, which a restart loses.
func TestAWindowWiderThanTheBoundIsRefused(t *testing.T) {
	for _, w := range []int{maxWindow + 1, overflowingWindow, math.MaxInt} {
		c := Config{Window: w}
		if err := c.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("Config{Window: %d}.Validate() = %v, want ErrInvalidConfig", w, err)
		}
		for _, k := range Kinds() {
			if _, err := New(k, c); !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("New(%v, Config{Window: %d}) = %v, want ErrInvalidConfig", k, w, err)
			}
		}
	}
	if err := (Config{Window: maxWindow}).Validate(); err != nil {
		t.Errorf("a window at the bound was refused: %v", err)
	}
}

// TestTheBitsetRingHoldsTheWholeWindow holds the ring arithmetic up to the
// bound: the ring is a power of two no smaller than the window it serves.
func TestTheBitsetRingHoldsTheWholeWindow(t *testing.T) {
	for _, n := range []int{1, 2, 3, 63, 64, 65, DefaultWindow, DefaultWindow + 1, maxWindow - 1, maxWindow} {
		p := pow2ceil(n)
		if p < n || p&(p-1) != 0 || (n > 1 && p >= 2*n) {
			t.Errorf("pow2ceil(%d) = %d, want the smallest power of two at or above it", n, p)
		}
	}
}

// TestTheBitsetNeverPassesAnOffsetInFlight is the loss itself, on a bitset
// built past Validate: whatever its ring holds, two offsets in flight never
// share a slot, so completing the later one cannot move the watermark over the
// earlier one.
func TestTheBitsetNeverPassesAnOffsetInFlight(t *testing.T) {
	for _, w := range []int{overflowingWindow, math.MaxInt} {
		tr := newBitset(Config{Window: w}.Normalize())
		gen := tr.Gen()
		if err := tr.Track(gen, 0); err != nil {
			t.Fatalf("Window %d: Track(0): %v", w, err)
		}
		ring := int64(tr.mask) + 1
		if err := tr.Track(gen, ring); err != nil {
			if !errors.Is(err, ErrWindowFull) {
				t.Fatalf("Window %d: Track(%d) = %v, want nil or ErrWindowFull", w, ring, err)
			}
			continue
		}
		if wm, _ := tr.Complete(gen, ring); wm != 0 {
			t.Fatalf("Window %d, a %d-slot ring: completing offset %d moved the watermark to %d past offset 0, "+
				"which is still in flight", w, ring, ring, wm)
		}
	}
}

// TestScenarios walks the specification rules for one kind at a time.
func TestScenarios(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			tr := mustNew(t, k, Config{Base: 100, Window: 64})
			gen := tr.Gen()
			// Nothing delivered: the watermark is the base.
			if wm, adv := tr.Complete(gen, 100); wm != 100 || adv {
				t.Fatalf("complete of an undelivered offset: %d %v", wm, adv)
			}
			// In-order delivery and completion advances offset by offset.
			for off := int64(100); off < 110; off++ {
				if err := tr.Track(gen, off); err != nil {
					t.Fatalf("track %d: %v", off, err)
				}
				if wm, adv := tr.Complete(gen, off); wm != off+1 || !adv {
					t.Fatalf("complete %d: wm %d adv %v", off, wm, adv)
				}
			}
			if tr.Pending() != 0 || tr.Watermark() != 110 {
				t.Fatalf("after ten records: pending %d wm %d", tr.Pending(), tr.Watermark())
			}
			// Out-of-order completion: the watermark waits for the lowest.
			for off := int64(110); off < 115; off++ {
				if err := tr.Track(gen, off); err != nil {
					t.Fatalf("track %d: %v", off, err)
				}
			}
			if tr.Pending() != 5 {
				t.Fatalf("pending = %d", tr.Pending())
			}
			for _, off := range []int64{114, 112, 111} {
				if wm, adv := tr.Complete(gen, off); wm != 110 || adv {
					t.Fatalf("complete %d moved the watermark to %d (%v)", off, wm, adv)
				}
			}
			// Completing the blocker jumps the watermark over the run.
			if wm, adv := tr.Complete(gen, 110); wm != 113 || !adv {
				t.Fatalf("complete 110: wm %d adv %v", wm, adv)
			}
			if wm, adv := tr.Complete(gen, 113); wm != 115 || !adv {
				t.Fatalf("complete 113: wm %d adv %v", wm, adv)
			}
			// A repeated completion changes nothing.
			if wm, adv := tr.Complete(gen, 113); wm != 115 || adv {
				t.Fatalf("repeat complete: wm %d adv %v", wm, adv)
			}
			// A gap in the delivered offsets never blocks the watermark: with
			// nothing in flight the watermark jumps to the new record, so the
			// offsets the broker never delivered are committed straight away.
			if err := tr.Track(gen, 130); err != nil {
				t.Fatalf("track over a gap: %v", err)
			}
			if tr.Watermark() != 130 {
				t.Fatalf("the watermark did not jump the gap: %d", tr.Watermark())
			}
			if wm, adv := tr.Complete(gen, 130); wm != 131 || !adv {
				t.Fatalf("complete over a gap: wm %d adv %v", wm, adv)
			}
			// Ordering and generation rules.
			if err := tr.Track(gen, 130); !errors.Is(err, ErrOutOfOrder) {
				t.Fatalf("re-track: %v", err)
			}
			if err := tr.Track(gen, 120); !errors.Is(err, ErrOutOfOrder) {
				t.Fatalf("backwards track: %v", err)
			}
			if err := tr.Track(gen+1, 140); !errors.Is(err, ErrStaleGeneration) {
				t.Fatalf("stale track: %v", err)
			}
			// The window bounds the span only while a record is in flight.
			if err := tr.Track(gen, 131); err != nil {
				t.Fatalf("track the blocker: %v", err)
			}
			if err := tr.Track(gen, 131+64); !errors.Is(err, ErrWindowFull) {
				t.Fatalf("track beyond the window: %v", err)
			}
			// Rebalance: a new generation, and every old handle is stale.
			if err := tr.Track(gen, 135); err != nil {
				t.Fatal(err)
			}
			next := tr.Reset(300)
			if next != gen+1 || tr.Gen() != next {
				t.Fatalf("Reset returned %d, Gen %d", next, tr.Gen())
			}
			if tr.Watermark() != 300 || tr.Pending() != 0 || tr.Paused() {
				t.Fatalf("after reset: wm %d pending %d paused %v", tr.Watermark(), tr.Pending(), tr.Paused())
			}
			if wm, adv := tr.Complete(gen, 131); wm != 300 || adv {
				t.Fatalf("stale completion: wm %d adv %v", wm, adv)
			}
			if err := tr.Track(gen, 300); !errors.Is(err, ErrStaleGeneration) {
				t.Fatalf("stale track after reset: %v", err)
			}
			if err := tr.Track(next, 300); err != nil {
				t.Fatalf("track under the new generation: %v", err)
			}
		})
	}
}

// TestPauseSignal checks the bounded window: the tracker asks for a pause
// before Track can fail, on either the in-flight count or the window span.
func TestPauseSignal(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			tr := mustNew(t, k, Config{Window: 32, MaxPending: 4, MaxGap: 16})
			gen := tr.Gen()
			for off := range int64(4) {
				if err := tr.Track(gen, off); err != nil {
					t.Fatal(err)
				}
				if want := off == 3; tr.Paused() != want {
					t.Fatalf("after %d in flight: Paused = %v", off+1, tr.Paused())
				}
			}
			for off := range int64(4) {
				tr.Complete(gen, off)
			}
			if tr.Paused() {
				t.Fatal("still paused after draining")
			}
			// A single slow record with a wide span pauses on the gap, well
			// before the window fills: the watermark is 4 and MaxGap is 16.
			if err := tr.Track(gen, 4); err != nil {
				t.Fatal(err)
			}
			if err := tr.Track(gen, 20); err != nil {
				t.Fatal(err)
			}
			if !tr.Paused() {
				t.Fatalf("span %d..%d did not pause", tr.Watermark(), 21)
			}
			if err := tr.Track(gen, 36); !errors.Is(err, ErrWindowFull) {
				t.Fatalf("beyond the window: %v", err)
			}
			if wm, adv := tr.Complete(gen, 4); wm != 20 || !adv {
				t.Fatalf("completing the blocker: wm %d adv %v", wm, adv)
			}
			if tr.Paused() {
				t.Fatalf("still paused with wm %d and %d pending", tr.Watermark(), tr.Pending())
			}
		})
	}
}

// TestAllocs pins the steady-state track-and-complete cycle at zero
// allocations for every kind.
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			tr := mustNew(t, k, Config{Window: 4096})
			gen := tr.Gen()
			var off int64
			// Warm up so every kind has grown its storage and, for the pooled
			// kinds, filled its free list.
			cycle := func() {
				for range 16 {
					if err := tr.Track(gen, off); err != nil {
						t.Fatal(err)
					}
					off++
				}
				for i := int64(16); i > 0; i-- {
					tr.Complete(gen, off-i)
				}
			}
			for range 64 {
				cycle()
			}
			if allocs := testing.AllocsPerRun(200, cycle); allocs != 0 {
				t.Fatalf("%v: %.2f allocs per 16 records", k, allocs)
			}
		})
	}
}

// TestAutoBindingMatchesADR checks that when an ADR binds the offset family
// (front-matter `auto: {offset: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "offset")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds offset yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds offset to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestLayering keeps the tracker on the standard library: the root module
// declares no third-party dependency and driverkit is part of it.
func TestLayering(t *testing.T) {
	testguard.StdlibOnly(t, ".")
}

func mustNew(tb testing.TB, k Kind, cfg Config) Tracker {
	tb.Helper()
	tr, err := New(k, cfg)
	if err != nil {
		tb.Fatalf("New(%v): %v", k, err)
	}
	return tr
}
