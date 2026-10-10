package nativebench

import (
	"os"
	"testing"

	"github.com/iqhive/iqmq/benchmarks/opbench"
)

// EnvEnable gates the baseline session tests. They start containers and take
// tens of seconds per client, so `go test ./...` skips them; the
// correctness gate in hack/native-baselines.sh sets the variable before it
// measures anything.
const EnvEnable = "IQMQ_NATIVE_BASELINES"

// SkipUnlessEnabled skips t unless EnvEnable is set to 1. Call it before
// starting a broker.
func SkipUnlessEnabled(tb testing.TB) {
	tb.Helper()
	if os.Getenv(EnvEnable) != "1" {
		tb.Skipf("native baseline session tests are off; set %s=1 (make bench-native runs them)", EnvEnable)
	}
}

// CheckSession is the correctness gate for one baseline. It is
// [opbench.CheckSession], which the driver arm runs too: a gate that differed
// between the two arms would let one of them be measured in a shape the other
// was never held to.
func CheckSession(t *testing.T, open Open) {
	t.Helper()
	opbench.CheckSession(t, open)
}
