package mwchain

import (
	"math/rand/v2"
	"testing"

	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// TestModel runs every kind against the model composition: the authored seed
// chains, pseudo-random chains of every behaviour, and every chain length
// from empty to two past the generated maximum, which is where the fused kind
// falls back.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("seeds", func(t *testing.T) { modelSeeds(t, k) })
			t.Run("random", func(t *testing.T) { modelRandom(t, k) })
			t.Run("arities", func(t *testing.T) { modelArities(t, k) })
		})
	}
}

func modelSeeds(t *testing.T, k Kind) {
	for i, seed := range mwSeeds() {
		specs := specsFrom(fuzzutil.NewReader(seed))
		compareKind(t, k, middlewaresOf(specs), specs)
		if t.Failed() {
			t.Fatalf("seed %d diverged", i)
		}
	}
}

func modelRandom(t *testing.T, k Kind) {
	rng := rand.New(rand.NewPCG(uint64(k), 0x3011))
	for run := range 500 {
		data := make([]byte, 4+rng.IntN(32))
		for i := range data {
			data[i] = byte(rng.IntN(256))
		}
		specs := specsFrom(fuzzutil.NewReader(data))
		compareKind(t, k, middlewaresOf(specs), specs)
		if t.Failed() {
			t.Fatalf("run %d diverged", run)
		}
	}
}

// modelArities walks every chain length the compositions have to cover, with
// one behaviour of each kind mixed in, so that the generated arities and the
// fallback beyond them are all exercised.
func modelArities(t *testing.T, k Kind) {
	behaviours := []behaviour{bPass, bMutate, bSwallow, bPass, bRecover, bPass, bTwice, bPass, bPass, bPass, bPass}
	for n := range MaxFused + 3 {
		specs := make([]spec, 0, n)
		for i := range n {
			specs = append(specs, spec{name: string(rune('a' + i)), b: behaviours[i%len(behaviours)]})
		}
		compareKind(t, k, middlewaresOf(specs), specs)
		if t.Failed() {
			t.Fatalf("chain of %d diverged", n)
		}
	}
}
