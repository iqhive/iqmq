package settlement

import (
	"math/rand/v2"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/model/settle"
)

// TestModelStateNames pins the shared enumeration between the
// implementation and internal/model/settle.
func TestModelStateNames(t *testing.T) {
	for st := Idle; st < stateCount; st++ {
		if got := settle.State(st).String(); got != st.String() {
			t.Fatalf("state %d: implementation %q, model %q", st, st, got)
		}
	}
}

// TestModel runs the differential op script of FuzzSettlement with
// pseudo-random inputs, so the ordinary test run covers the model
// comparison without the fuzzer.
func TestModel(t *testing.T) {
	sc := settlementScript()
	for i, s := range settlementSeeds(sc) {
		r := fuzzutil.NewReader(s)
		h := newHarness(r.Int(1, 4))
		if n := sc.Run(t, s[len(s)-r.Remaining():], h); n == 0 {
			t.Fatalf("seed %d executed no steps", i)
		}
	}
	rng := rand.New(rand.NewPCG(7, 11))
	for run := range 300 {
		data := make([]byte, 512+rng.IntN(1536))
		for i := range data {
			data[i] = byte(rng.IntN(256))
		}
		h := newHarness(1 + run%4)
		sc.Run(t, data, h)
		if t.Failed() {
			t.Fatalf("run %d diverged", run)
		}
	}
}
