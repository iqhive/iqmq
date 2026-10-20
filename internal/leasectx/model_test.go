package leasectx

import (
	"math/rand/v2"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// TestModel runs every kind against the model of what a lease owes: the
// authored seed scripts and pseudo-random scripts over every deadline
// scheduler, all inside a synctest bubble so the clock is exact.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("seeds", func(t *testing.T) { modelSeeds(t, k) })
			t.Run("script", func(t *testing.T) { modelRandomScripts(t, k) })
		})
	}
}

func modelSeeds(t *testing.T, k Kind) {
	sc := leaseScript()
	for i, seed := range leaseSeeds(t, sc) {
		r := fuzzutil.NewReader(seed)
		if Kinds()[r.Choice(len(Kinds()))] != k {
			continue
		}
		runLeaseScript(t, seed)
		if t.Failed() {
			t.Fatalf("seed %d diverged", i)
		}
	}
}

func modelRandomScripts(t *testing.T, k Kind) {
	rng := rand.New(rand.NewPCG(uint64(k), 0x25a))
	for run := range 30 {
		data := make([]byte, 64+rng.IntN(192))
		for i := range data {
			data[i] = byte(rng.IntN(256))
		}
		// The first byte selects the kind: force it to this one so every
		// kind sees the same scripts.
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), int(k)-1)
		runLeaseScript(t, append(w.Data(), data...))
		if t.Failed() {
			t.Fatalf("run %d diverged", run)
		}
	}
}
