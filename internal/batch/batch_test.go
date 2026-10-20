package batch

import (
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
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
		b := mustNew[int](t, k, Config{Clock: clock.NewFake(modelStart)})
		if b.Kind() != k {
			t.Fatalf("New(%v).Kind() = %v", k, b.Kind())
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Valid() || AutoKind == Auto || Auto.String() != "auto" {
		t.Fatal("Auto binding")
	}
	b := mustNew[int](t, Auto, Config{Clock: clock.NewFake(modelStart)})
	if b.Kind() != AutoKind {
		t.Fatal("New(Auto) does not resolve")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if !PerBatchTimer.Timed() || !SharedTicker.Timed() || SizeOnly.Timed() {
		t.Fatal("Timed")
	}
	if !PerBatchTimer.Exact() || SharedTicker.Exact() || SizeOnly.Exact() {
		t.Fatal("Exact")
	}
	if PerBatchTimer.TimersPerBatch() != 1 || SharedTicker.TimersPerBatch() != 0 || SizeOnly.TimersPerBatch() != 0 {
		t.Fatal("TimersPerBatch")
	}
	if Kind(9).Valid() || Kind(9).Ident() != "" || Kind(9).String() != "kind(?)" {
		t.Fatal("invalid kind")
	}
	if _, ok := ParseKind("x"); ok {
		t.Fatal("ParseKind junk")
	}
	if _, err := New[int](Kind(9), Config{}); err == nil {
		t.Fatal("New(invalid) should fail")
	}
}

func mustNew[T any](tb testing.TB, k Kind, cfg Config) Batcher[T] {
	tb.Helper()
	b, err := New[T](k, cfg)
	if err != nil {
		tb.Fatalf("New(%v): %v", k, err)
	}
	return b
}

func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "batch")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds batch yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds batch to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestTimersPerBatch counts the timers each kind creates while filling a
// hundred batches: one per batch for PerBatchTimer, one for the whole run
// for SharedTicker (its ticker), none for SizeOnly. This is the
// timers/op figure the family exists to compare.
func TestTimersPerBatch(t *testing.T) {
	const batches = 100
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			f := clock.NewFake(modelStart)
			counting := clock.NewCounting(f)
			b := mustNew[int](t, k, Config{MaxMessages: 4, Linger: time.Millisecond, Clock: counting})
			defer func() { _ = b.Close() }()
			var out []int
			for i := range batches * 4 {
				ready, err := b.Add(i, 1)
				if err != nil {
					t.Fatal(err)
				}
				if ready {
					out, _ = b.TakeReady(f.Now(), out[:0])
				}
			}
			created := counting.Created()
			want := int64(batches * k.TimersPerBatch())
			if k == SharedTicker {
				want = 1 // the ticker's own timer, once for the whole run
			}
			if created != want {
				t.Fatalf("%d timers for %d batches, want %d", created, batches, want)
			}
			t.Logf("%v: %.3f timers/batch", k, float64(created)/batches)
		})
	}
}

// TestSharedTickerIsShared: many batchers on one ticker cost one timer in
// total, and each is woken on the tick.
func TestSharedTickerIsShared(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		counting := clock.NewCounting(clock.Real{})
		ticker := NewTicker(counting, 5*time.Millisecond)
		defer func() { _ = ticker.Close() }()
		const n = 16
		bs := make([]Batcher[int], n)
		for i := range bs {
			bs[i] = mustNew[int](t, SharedTicker, Config{MaxMessages: 100, Linger: 5 * time.Millisecond, Clock: counting, Ticker: ticker})
			if _, err := bs[i].Add(i, 1); err != nil {
				t.Fatal(err)
			}
		}
		if got := ticker.Waiters(); got != n {
			t.Fatalf("%d waiters, want %d", got, n)
		}
		if got := counting.Created(); got != 1 {
			t.Fatalf("%d timers for %d batchers, want 1", got, n)
		}
		for i, b := range bs {
			select {
			case <-b.Wait():
			case <-time.After(time.Second):
				t.Fatalf("batcher %d was not woken", i)
			}
			if !b.Ready(time.Now()) {
				t.Fatalf("batcher %d woken but not ready", i)
			}
			if out := b.Flush(nil); len(out) != 1 {
				t.Fatalf("batcher %d: %v", i, out)
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if got := ticker.Waiters(); got != 0 {
			t.Fatalf("%d waiters after closing every batcher", got)
		}
		// The ticker's period never changes, and re-closing is safe.
		if ticker.Period() != 5*time.Millisecond {
			t.Fatalf("period %v", ticker.Period())
		}
		if err := ticker.Close(); err != nil {
			t.Fatal(err)
		}
		if err := ticker.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	})
}

// TestTickerGrid: deadlines land on the ticker's grid, never early.
func TestTickerGrid(t *testing.T) {
	f := clock.NewFake(modelStart)
	ticker := NewTicker(f, 10*time.Millisecond)
	defer func() { _ = ticker.Close() }()
	for _, d := range []time.Duration{-time.Second, 0, time.Nanosecond, 9 * time.Millisecond, 10 * time.Millisecond, 11 * time.Millisecond, time.Second} {
		want := modelStart.Add(d)
		got := ticker.deadlineFor(want)
		if got.Before(want) {
			t.Fatalf("deadlineFor(%v) = %v, which is early", d, got.Sub(modelStart))
		}
		if d <= 0 {
			if !got.Equal(modelStart) {
				t.Fatalf("deadlineFor(%v) = %v, want the epoch", d, got.Sub(modelStart))
			}
			continue
		}
		if got.Sub(want) >= 10*time.Millisecond {
			t.Fatalf("deadlineFor(%v) = %v, more than one tick late", d, got.Sub(modelStart))
		}
		if got.Sub(modelStart)%(10*time.Millisecond) != 0 {
			t.Fatalf("deadlineFor(%v) = %v, off the grid", d, got.Sub(modelStart))
		}
	}
}

// TestZeroLinger: a zero linger makes a timed batch ready as soon as it
// holds an item, and never makes SizeOnly ready on time alone.
func TestZeroLinger(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			f := clock.NewFake(modelStart)
			b := mustNew[int](t, k, Config{MaxMessages: 1000, Clock: f})
			defer func() { _ = b.Close() }()
			if _, err := b.Add(1, 1); err != nil {
				t.Fatal(err)
			}
			if got := b.Ready(f.Now()); got != k.Timed() {
				t.Fatalf("Ready = %v, want %v", got, k.Timed())
			}
		})
	}
}

// TestNoThresholds: with no threshold at all a batch only leaves when the
// caller flushes it, and nothing is stranded.
func TestNoThresholds(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			f := clock.NewFake(modelStart)
			b := mustNew[int](t, k, Config{Linger: time.Hour, Clock: f})
			defer func() { _ = b.Close() }()
			for i := range 1000 {
				if ready, _ := b.Add(i, 1<<20); ready {
					t.Fatalf("ready at item %d with no thresholds", i)
				}
			}
			f.Advance(time.Hour)
			out := b.Flush(nil)
			if len(out) != 1000 {
				t.Fatalf("Flush returned %d items", len(out))
			}
		})
	}
}

var allocSink int

// TestAllocs: filling and taking a batch allocates nothing beyond the
// timer the kind arms — which is exactly the cost the family is measuring,
// so PerBatchTimer's allocations are reported rather than forbidden.
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			f := clock.NewFake(modelStart)
			b := mustNew[int](t, k, Config{MaxMessages: 64, Linger: time.Hour, Clock: f})
			defer func() { _ = b.Close() }()
			dst := make([]int, 0, 64)
			// Grow the batch's own storage first.
			for i := range 64 {
				if _, err := b.Add(i, 1); err != nil {
					t.Fatal(err)
				}
			}
			dst, _ = b.TakeReady(f.Now(), dst[:0])
			a := testing.AllocsPerRun(200, func() {
				for i := range 32 {
					if _, err := b.Add(i, 1); err != nil {
						t.Fatal(err)
					}
				}
				allocSink += len(b.Flush(dst[:0]))
			})
			switch {
			case k.TimersPerBatch() == 0 && a != 0:
				t.Fatalf("Add+Flush of 32 items: %.1f allocs/op, want 0", a)
			case k.TimersPerBatch() > 0 && a > 4:
				t.Fatalf("Add+Flush of 32 items: %.1f allocs/op, more than one timer's worth", a)
			}
			t.Logf("%v: %.1f allocs per batch (timers per batch: %d)", k, a, k.TimersPerBatch())
		})
	}
}

// TestStdlibOnly keeps the batchers inside the layering rules.
func TestStdlibOnly(t *testing.T) {
	testguard.StdlibOnly(t, ".")
	testguard.NoRootImport(t, ".")
}
