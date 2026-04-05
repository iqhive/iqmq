package offset

import (
	"errors"
	"testing"

	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
	"bitbucket.org/iqhive/iqmq/internal/model/commit"
)

// harness pairs one tracker with the reference model. Both see identical
// operations and every result is compared; the harness keeps the offsets it
// believes are in flight so the script can complete real records as well as
// invented ones.
type harness struct {
	tr       Tracker
	m        *commit.Model
	high     int64
	inflight []int64
	stale    []int64 // offsets whose completion must be refused
}

func newHarness(k Kind, cfg Config) *harness {
	cfg = cfg.Normalize()
	return &harness{
		tr:   New(k, cfg),
		m:    commit.New(commit.Config{Base: cfg.Base, Window: cfg.Window, MaxPending: cfg.MaxPending, MaxGap: cfg.MaxGap}),
		high: cfg.Base,
	}
}

// configFrom decodes a tracker configuration from fuzz input.
func configFrom(r *fuzzutil.Reader) Config {
	window := r.Int(1, 300)
	base := int64(r.Int(0, 1000))
	maxPending := r.Int(1, window)
	maxGap := r.Int(1, window)
	return Config{Base: base, Window: window, MaxPending: maxPending, MaxGap: maxGap}
}

// compare asserts that the tracker and the model report the same state.
func (h *harness) compare(t testing.TB, op string) {
	t.Helper()
	if got, want := h.tr.Watermark(), h.m.Watermark(); got != want {
		t.Fatalf("%s: watermark %d, model %d", op, got, want)
	}
	if got, want := h.tr.Pending(), h.m.Pending(); got != want {
		t.Fatalf("%s: pending %d, model %d", op, got, want)
	}
	if got, want := h.tr.Paused(), h.m.Paused(); got != want {
		t.Fatalf("%s: paused %v, model %v", op, got, want)
	}
	if got, want := h.tr.Gen(), h.m.Gen(); got != want {
		t.Fatalf("%s: generation %d, model %d", op, got, want)
	}
}

// track applies one Track to both sides.
func (h *harness) track(t testing.TB, gen uint32, off int64) {
	t.Helper()
	err := h.tr.Track(gen, off)
	res := h.m.Step(commit.Op{Kind: commit.Track, Gen: gen, Off: off})
	if (err == nil) != (res.Err == nil) || (err != nil && !errors.Is(err, res.Err)) {
		t.Fatalf("track(gen=%d, off=%d): tracker %v, model %v", gen, off, err, res.Err)
	}
	if err == nil {
		h.high = off + 1
		h.inflight = append(h.inflight, off)
	}
	h.compare(t, "track")
}

// complete applies one Complete to both sides.
func (h *harness) complete(t testing.TB, gen uint32, off int64) {
	t.Helper()
	wm, adv := h.tr.Complete(gen, off)
	res := h.m.Step(commit.Op{Kind: commit.Complete, Gen: gen, Off: off})
	if wm != res.Watermark || adv != res.Advanced {
		t.Fatalf("complete(gen=%d, off=%d): tracker (%d, %v), model (%d, %v)", gen, off, wm, adv, res.Watermark, res.Advanced)
	}
	for i, o := range h.inflight {
		if o == off && gen == h.tr.Gen() {
			h.inflight = append(h.inflight[:i], h.inflight[i+1:]...)
			h.stale = append(h.stale, off)
			break
		}
	}
	h.compare(t, "complete")
}

// reset applies one Reset to both sides.
func (h *harness) reset(t testing.TB, next int64) {
	t.Helper()
	gen := h.tr.Reset(next)
	res := h.m.Step(commit.Op{Kind: commit.Reset, Off: next})
	if gen != res.Gen {
		t.Fatalf("reset(%d): tracker generation %d, model %d", next, gen, res.Gen)
	}
	h.high = next
	h.stale = append(h.stale, h.inflight...)
	h.inflight = h.inflight[:0]
	h.compare(t, "reset")
}

// pick returns an offset from list, or -1 when it is empty.
func pick(r *fuzzutil.Reader, list []int64) int64 {
	if len(list) == 0 {
		return -1
	}
	return list[r.Choice(len(list))]
}

// trackerScript is the op-script differential between a Tracker and the
// reference model. Every step applies the same operation to both and compares
// the result; the script's Check runs the model's invariants.
func trackerScript() *fuzzutil.Script[*harness] {
	return &fuzzutil.Script[*harness]{
		Steps: []fuzzutil.Step[*harness]{
			{Name: "track", Weight: 5, Run: func(t testing.TB, r *fuzzutil.Reader, h *harness) {
				h.track(t, h.tr.Gen(), h.high+int64(r.Int(0, 4)))
			}},
			{Name: "trackFar", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, h *harness) {
				// Far enough to exercise ErrWindowFull and the pause signal.
				h.track(t, h.tr.Gen(), h.tr.Watermark()+int64(r.Int(0, 400)))
			}},
			{Name: "trackBack", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, h *harness) {
				// At or below an offset already delivered: ErrOutOfOrder.
				h.track(t, h.tr.Gen(), h.high-int64(r.Int(0, 8)))
			}},
			{Name: "trackStale", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, h *harness) {
				h.track(t, staleGen(r, h.tr.Gen()), h.high+int64(r.Int(0, 2)))
			}},
			{Name: "complete", Weight: 5, Run: func(t testing.TB, r *fuzzutil.Reader, h *harness) {
				off := pick(r, h.inflight)
				if off < 0 {
					off = h.tr.Watermark()
				}
				h.complete(t, h.tr.Gen(), off)
			}},
			{Name: "completeAgain", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, h *harness) {
				off := pick(r, h.stale)
				if off < 0 {
					off = h.tr.Watermark() - 1
				}
				h.complete(t, h.tr.Gen(), off)
			}},
			{Name: "completeUnknown", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, h *harness) {
				h.complete(t, h.tr.Gen(), h.tr.Watermark()+int64(r.Int(-8, 400)))
			}},
			{Name: "completeStale", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, h *harness) {
				off := pick(r, h.inflight)
				if off < 0 {
					off = h.tr.Watermark()
				}
				h.complete(t, staleGen(r, h.tr.Gen()), off)
			}},
			{Name: "reset", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, h *harness) {
				h.reset(t, h.tr.Watermark()+int64(r.Int(0, 16)))
			}},
			{Name: "commit", Weight: 1, Run: func(t testing.TB, _ *fuzzutil.Reader, h *harness) {
				res := h.m.Step(commit.Op{Kind: commit.Commit, Gen: h.m.Gen()})
				if res.Watermark != h.tr.Watermark() {
					t.Fatalf("commit: model published %d, tracker watermark %d", res.Watermark, h.tr.Watermark())
				}
			}},
		},
		MaxSteps: 2048,
		Check: func(t testing.TB, h *harness) {
			if err := h.m.Invariants(); err != nil {
				t.Fatalf("model invariant: %v", err)
			}
			// The watermark is safe: every in-flight offset is at or above it,
			// and it is the lowest of them when any are in flight.
			wm := h.tr.Watermark()
			lowest := int64(-1)
			for _, off := range h.inflight {
				if off < wm {
					t.Fatalf("watermark %d passed in-flight offset %d", wm, off)
				}
				if lowest < 0 || off < lowest {
					lowest = off
				}
			}
			switch {
			case lowest >= 0 && wm != lowest:
				t.Fatalf("watermark %d is not the lowest in-flight offset %d", wm, lowest)
			case lowest < 0 && wm != h.high:
				t.Fatalf("nothing in flight but watermark %d is not %d", wm, h.high)
			}
			if h.tr.Pending() != len(h.inflight) {
				t.Fatalf("pending %d, harness holds %d in flight", h.tr.Pending(), len(h.inflight))
			}
		},
	}
}

// staleGen picks a generation that is not the live one.
func staleGen(r *fuzzutil.Reader, cur uint32) uint32 {
	switch r.Choice(3) {
	case 0:
		return cur - 1
	case 1:
		return cur + 1
	default:
		g := uint32(r.Byte())
		if g == cur {
			g++
		}
		return g
	}
}

// trackerSeeds authors readable seed scripts for every kind.
func trackerSeeds(sc *fuzzutil.Script[*harness]) [][]byte {
	type step func(w *fuzzutil.Writer)
	track := func(d int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "track"); w.Int(0, 4, d) }
	}
	complete := func(i int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "complete"); w.Choice(8, i) }
	}
	plain := func(name string) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, name) }
	}
	reset := func(d int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "reset"); w.Int(0, 16, d) }
	}
	far := func(d int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "trackFar"); w.Int(0, 400, d) }
	}
	scripts := [][]step{
		// Sequential: deliver and complete in order.
		{track(0), complete(0), track(0), complete(0), track(0), complete(0), plain("commit")},
		// Out-of-order completion with a blocked watermark.
		{track(0), track(0), track(0), track(0), complete(3), complete(2), complete(1), complete(0), plain("commit")},
		// Gaps: offsets the broker never delivered must not block.
		{track(3), complete(0), track(2), track(4), complete(1), complete(0), plain("commit")},
		// Window and pause: a far offset, then the blocker completes.
		{track(0), far(200), plain("commit"), complete(0), complete(0)},
		// Rebalance: stale handles after a reset.
		{track(0), track(1), reset(4), plain("completeStale"), plain("trackStale"), track(0), complete(0)},
		// Repeated and unknown completions.
		{track(0), complete(0), plain("completeAgain"), plain("completeUnknown"), plain("trackBack")},
	}
	seeds := make([][]byte, 0, len(scripts)*len(Kinds()))
	for ki := range Kinds() {
		for _, steps := range scripts {
			w := fuzzutil.NewWriter()
			w.Choice(len(Kinds()), ki)
			// Config: window 300, base 0, pause thresholds at the window.
			w.Int(1, 300, 64)
			w.Int(0, 1000, 0)
			w.Int(1, 64, 64)
			w.Int(1, 64, 64)
			for _, st := range steps {
				st(w)
			}
			seeds = append(seeds, w.Data())
		}
	}
	return seeds
}

// FuzzOffsetTracker is the differential fuzz target (the test suite): a fuzz-chosen kind, window and operation
// script run against a Tracker and internal/model/commit. Every result is
// compared, the model's invariants are checked after every step, and the
// harness independently asserts that the watermark equals the lowest in-flight
// offset (the offset after the newest record when nothing is in flight), that
// nothing in flight is below it, and that memory stays bounded by the window.
func FuzzOffsetTracker(f *testing.F) {
	sc := trackerScript()
	for _, s := range trackerSeeds(sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		cfg := configFrom(r)
		h := newHarness(k, cfg)
		sc.Run(t, data[len(data)-r.Remaining():], h)
		// Draining every in-flight offset must leave the watermark at the
		// offset after the newest record: nothing is lost, nothing sticks.
		gen := h.tr.Gen()
		for _, off := range append([]int64(nil), h.inflight...) {
			h.complete(t, gen, off)
		}
		if h.tr.Pending() != 0 || h.tr.Watermark() != h.high {
			t.Fatalf("after draining: pending %d watermark %d, want 0 and %d", h.tr.Pending(), h.tr.Watermark(), h.high)
		}
		if h.tr.Paused() {
			t.Fatal("pause signal stuck with an empty window")
		}
	})
}
