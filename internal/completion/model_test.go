package completion

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// TestModel runs every kind against the model of what a completer owes: the
// authored seed scripts, pseudo-random scripts over every hand-over ring and
// wait strategy, and the two shapes a publisher produces — a stream of
// publishes completed in order, and one completed out of order with failures
// mixed in.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("seeds", func(t *testing.T) { modelSeeds(t, k) })
			t.Run("script", func(t *testing.T) { modelRandomScripts(t, k) })
			t.Run("stream", func(t *testing.T) { modelStream(t, k, false) })
			t.Run("outoforder", func(t *testing.T) { modelStream(t, k, true) })
		})
	}
}

func modelSeeds(t *testing.T, k Kind) {
	sc := compScript()
	for i, seed := range compSeeds(t, sc) {
		r := fuzzutil.NewReader(seed)
		if Kinds()[r.Choice(len(Kinds()))] != k {
			continue
		}
		if n := runCompScript(t, seed); n == 0 {
			t.Fatalf("seed %d executed no steps", i)
		}
		if t.Failed() {
			t.Fatalf("seed %d diverged", i)
		}
	}
}

func modelRandomScripts(t *testing.T, k Kind) {
	rng := rand.New(rand.NewPCG(uint64(k), 0x22a))
	for run := range 100 {
		data := make([]byte, 64+rng.IntN(256))
		for i := range data {
			data[i] = byte(rng.IntN(256))
		}
		// The first byte selects the kind: force it to this one so every
		// kind sees the same scripts.
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), int(k)-1)
		runCompScript(t, append(w.Data(), data...))
		if t.Failed() {
			t.Fatalf("run %d diverged", run)
		}
	}
}

// modelStream drives the shape an async publisher produces: a window of
// outstanding publishes that the broker confirms as it goes, in order or not,
// with a share of failures.
func modelStream(t *testing.T, k Kind, outOfOrder bool) {
	h := newCompHarness(t, k, Config{Capacity: 32})
	defer func() { _ = h.c.Close() }()
	rng := rand.New(rand.NewPCG(uint64(k), 7))
	for i := range 2000 {
		h.start(t, i%16 == 0, 1)
		if t.Failed() {
			t.FailNow()
		}
		if len(h.live) < 8 {
			continue
		}
		idx := 0
		if outOfOrder {
			idx = rng.IntN(len(h.live))
		}
		var err error
		if i%5 == 0 {
			err = errBroker
		}
		h.completeLive(t, idx, err)
		if i%64 == 0 {
			h.check(t)
		}
		if t.Failed() {
			t.FailNow()
		}
	}
	h.close(t)
	if err := h.c.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
}
