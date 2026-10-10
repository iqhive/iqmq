package competitors_test

import (
	"testing"

	"github.com/iqhive/iqmq/benchmarks/competitors"
	"github.com/iqhive/iqmq/benchmarks/opbench"
)

// TestSessionsRoundTrip is the correctness gate: every arm of every
// available family round-trips one message and a full batch at every canonical
// payload size, through the same opbench.CheckSession both arms of the
// overhead comparison run.
//
// It is what `hack/bench/run-compare.sh` runs before it measures anything, and
// it is also how the script waits for a broker to become usable. An arm that
// cannot do the work is not measured: a number from an arm that drops messages
// is worse than no number.
func TestSessionsRoundTrip(t *testing.T) {
	for _, f := range competitors.Families() {
		addr, ok := f.Addr()
		if !ok {
			t.Run(f.Name, func(t *testing.T) {
				t.Skipf("competitors: %s is unset, so the %s family was not gated", f.AddrEnv, f.Name)
			})
			continue
		}
		for _, arm := range competitors.ArmsFor(f.Name) {
			t.Run(arm.Name(), func(t *testing.T) {
				opbench.CheckSession(t, func(tb testing.TB, p opbench.Params) opbench.Session {
					return arm.Open(tb, addr, p)
				})
			})
		}
	}
}
