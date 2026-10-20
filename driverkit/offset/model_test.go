package offset

import (
	"math/rand/v2"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/model/commit"
)

// TestModel runs every kind against the reference model
// (internal/model/commit) on the four delivery patterns the bake-off
// benchmarks, on a rebalance sequence, and on pseudo-random op scripts, so
// the ordinary test run covers the differential without the fuzzer.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("sequential", func(t *testing.T) { modelPattern(t, k, patternSequential) })
			t.Run("random", func(t *testing.T) { modelPattern(t, k, patternRandom) })
			t.Run("gaps", func(t *testing.T) { modelPattern(t, k, patternGaps) })
			t.Run("widewindow", func(t *testing.T) { modelPattern(t, k, patternWide) })
			t.Run("rebalance", func(t *testing.T) { modelRebalance(t, k) })
			t.Run("script", func(t *testing.T) { modelScript(t, k) })
		})
	}
}

// pattern is a delivery and completion pattern; it returns the number of
// records and, for step i, the offset to deliver and the index (into the
// records delivered so far) of the record to complete, or -1 for none.
type pattern int

const (
	patternSequential pattern = iota
	patternRandom
	patternGaps
	patternWide
)

// modelPattern drives one kind and the model through a pattern of 4000
// records, comparing the watermark, the pending count and the pause signal
// after every operation.
func modelPattern(t *testing.T, k Kind, p pattern) {
	const records = 4000
	cfg := Config{Base: 1 << 20, Window: 1024}
	if p == patternWide {
		cfg.Window = 8192
	}
	h := newHarness(t, k, cfg)
	gen := h.tr.Gen()
	rng := rand.New(rand.NewPCG(uint64(k), uint64(p)))
	for i := range records {
		switch p {
		case patternSequential:
			h.track(t, gen, h.high)
			h.complete(t, gen, h.high-1)
		case patternRandom:
			// Keep a window of 64 records in flight, completing a random one.
			h.track(t, gen, h.high)
			if len(h.inflight) >= 64 {
				h.complete(t, gen, h.inflight[rng.IntN(len(h.inflight))])
			}
		case patternGaps:
			// Every fourth offset is never delivered (compacted records and
			// transaction markers look like this).
			h.track(t, gen, h.high+int64(i%4))
			if len(h.inflight) >= 8 {
				h.complete(t, gen, h.inflight[rng.IntN(len(h.inflight))])
			}
		case patternWide:
			// One record stays in flight for a long time, so the window is
			// wide while few records are outstanding.
			h.track(t, gen, h.high)
			if i%1000 != 0 && len(h.inflight) > 1 {
				h.complete(t, gen, h.inflight[len(h.inflight)-1])
			}
		}
		if t.Failed() {
			t.Fatalf("diverged at record %d", i)
		}
	}
	// Draining leaves nothing behind.
	for _, off := range append([]int64(nil), h.inflight...) {
		h.complete(t, gen, off)
	}
	if h.tr.Pending() != 0 || h.tr.Watermark() != h.high || h.tr.Paused() {
		t.Fatalf("after draining: pending %d watermark %d (want %d) paused %v", h.tr.Pending(), h.tr.Watermark(), h.high, h.tr.Paused())
	}
	if err := h.m.Invariants(); err != nil {
		t.Fatalf("model invariant: %v", err)
	}
}

// modelRebalance checks the rebalance path: in-flight records are dropped, the
// generation advances, late completions from the old generation are refused
// and the watermark of the new assignment is the one the caller chose.
func modelRebalance(t *testing.T, k Kind) {
	h := newHarness(t, k, Config{Window: 256})
	gen := h.tr.Gen()
	for off := range int64(32) {
		h.track(t, gen, off)
	}
	for _, off := range []int64{0, 1, 2, 5} {
		h.complete(t, gen, off)
	}
	if h.tr.Watermark() != 3 {
		t.Fatalf("watermark %d before the rebalance", h.tr.Watermark())
	}
	// The driver commits the safe boundary and is then revoked.
	if res := h.m.Step(commit.Op{Kind: commit.Commit, Gen: h.m.Gen()}); res.Watermark != 3 {
		t.Fatalf("committed %d", res.Watermark)
	}
	next := h.tr.Watermark()
	h.reset(t, next)
	newGen := h.tr.Gen()
	if newGen == gen {
		t.Fatal("generation did not advance")
	}
	// Every handle from the old generation is stale, in both directions.
	for off := range int64(32) {
		h.complete(t, gen, off)
	}
	h.track(t, gen, 100)
	if h.tr.Watermark() != next || h.tr.Pending() != 0 {
		t.Fatalf("stale operations changed the state: wm %d pending %d", h.tr.Watermark(), h.tr.Pending())
	}
	// The new assignment re-delivers from the committed boundary.
	for off := next; off < next+8; off++ {
		h.track(t, newGen, off)
	}
	for off := next; off < next+8; off++ {
		h.complete(t, newGen, off)
	}
	if h.tr.Watermark() != next+8 {
		t.Fatalf("watermark %d after the new assignment", h.tr.Watermark())
	}
	if err := h.m.Invariants(); err != nil {
		t.Fatalf("model invariant: %v", err)
	}
}

// modelScript runs the differential op script of FuzzOffsetTracker with the
// authored seeds and with pseudo-random input.
func modelScript(t *testing.T, k Kind) {
	sc := trackerScript()
	for i, seed := range trackerSeeds(t, sc) {
		r := fuzzutil.NewReader(seed)
		if Kinds()[r.Choice(len(Kinds()))] != k {
			continue
		}
		h := newHarness(t, k, configFrom(r))
		if n := sc.Run(t, seed[len(seed)-r.Remaining():], h); n == 0 {
			t.Fatalf("seed %d executed no steps", i)
		}
		if t.Failed() {
			t.Fatalf("seed %d diverged", i)
		}
	}
	rng := rand.New(rand.NewPCG(uint64(k), 0x5eed))
	for run := range 200 {
		data := make([]byte, 512+rng.IntN(1536))
		for i := range data {
			data[i] = byte(rng.IntN(256))
		}
		r := fuzzutil.NewReader(data)
		cfg := configFrom(r)
		h := newHarness(t, k, cfg)
		sc.Run(t, data[len(data)-r.Remaining():], h)
		if t.Failed() {
			t.Fatalf("run %d diverged (config %+v)", run, cfg)
		}
	}
}

// TestKindsAgree runs every kind side by side on one operation stream: the
// kinds must be indistinguishable through the Tracker interface.
func TestKindsAgree(t *testing.T) {
	cfg := Config{Base: 500, Window: 512, MaxPending: 64, MaxGap: 256}
	trackers := make([]Tracker, 0, len(Kinds()))
	for _, k := range Kinds() {
		trackers = append(trackers, mustNew(t, k, cfg))
	}
	rng := rand.New(rand.NewPCG(3, 9))
	var inflight []int64
	high := cfg.Base
	gen := trackers[0].Gen()
	agree := func(op string) {
		t.Helper()
		for i, tr := range trackers[1:] {
			a, b := trackers[0], tr
			if a.Watermark() != b.Watermark() || a.Pending() != b.Pending() || a.Paused() != b.Paused() || a.Gen() != b.Gen() {
				t.Fatalf("%s: %v (wm %d pending %d paused %v gen %d) != %v (wm %d pending %d paused %v gen %d)",
					op, a.Kind(), a.Watermark(), a.Pending(), a.Paused(), a.Gen(),
					Kinds()[i+1], b.Watermark(), b.Pending(), b.Paused(), b.Gen())
			}
		}
	}
	for step := range 20000 {
		switch {
		case len(inflight) == 0 || rng.IntN(3) > 0:
			off := high + int64(rng.IntN(3))
			errs := make([]error, 0, len(trackers))
			for _, tr := range trackers {
				errs = append(errs, tr.Track(gen, off))
			}
			for i, err := range errs {
				if (err == nil) != (errs[0] == nil) {
					t.Fatalf("step %d: %v returned %v, %v returned %v", step, Kinds()[0], errs[0], Kinds()[i], err)
				}
			}
			if errs[0] == nil {
				high = off + 1
				inflight = append(inflight, off)
			}
			agree("track")
		case rng.IntN(20) == 0:
			next := trackers[0].Watermark() + int64(rng.IntN(4))
			gens := make([]uint32, 0, len(trackers))
			for _, tr := range trackers {
				gens = append(gens, tr.Reset(next))
			}
			for i, g := range gens {
				if g != gens[0] {
					t.Fatalf("step %d: %v generation %d, %v %d", step, Kinds()[0], gens[0], Kinds()[i], g)
				}
			}
			gen, high, inflight = gens[0], next, inflight[:0]
			agree("reset")
		default:
			j := rng.IntN(len(inflight))
			off := inflight[j]
			inflight = append(inflight[:j], inflight[j+1:]...)
			wms := make([]int64, 0, len(trackers))
			advs := make([]bool, 0, len(trackers))
			for _, tr := range trackers {
				wm, adv := tr.Complete(gen, off)
				wms, advs = append(wms, wm), append(advs, adv)
			}
			for i := range wms {
				if wms[i] != wms[0] || advs[i] != advs[0] {
					t.Fatalf("step %d: %v (%d, %v), %v (%d, %v)", step, Kinds()[0], wms[0], advs[0], Kinds()[i], wms[i], advs[i])
				}
			}
			agree("complete")
		}
	}
}
