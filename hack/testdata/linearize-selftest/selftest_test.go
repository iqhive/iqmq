// Package linearizeselftest is the negative control for
// `make linearize-campaign`.
//
// It holds one history that is genuinely NOT linearizable -- a write that
// completed before a read began, and a read that returned the value from
// before the write -- and a test and a fuzz target that fail on it. The
// campaign script points every one of its three stages at this package with
// `--selftest` and asserts that all three go red and that the checker's
// verdict reaches the output.
//
// It lives under `testdata` on purpose: the Go tool never expands `./...` into
// a testdata directory, so `go build ./...`, `go vet ./...`, `make
// fuzz-seeds`, `make ratchet` and `golangci-lint` do not see it. Only
// `hack/linearize-campaign.sh --selftest` compiles it, which is what keeps a
// deliberately failing test out of every other gate.
package linearizeselftest

import (
	"testing"

	"github.com/iqhive/iqmq/testkit/linear"
)

// register is the sequential specification: an integer cell that "write" sets
// and "read" observes.
func register() linear.Model[int] {
	return linear.Model[int]{
		Init: func(string) int { return 0 },
		Step: func(s int, op linear.Op) (int, bool) {
			switch op.Kind {
			case "write":
				return op.Arg.(int), true
			case "read":
				return s, op.Ret.(int) == s
			}
			return s, false
		},
		Key: func(s int) string { return string(rune('0' + s)) },
	}
}

// stale is the smallest non-linearizable history there is. Client 1's write of
// 1 returned at logical time 2; client 2's read began at 3 and returned 0. The
// two do not overlap, so every total order that respects real time puts the
// write first, and no such order lets the read return 0.
func stale() []linear.Op {
	return []linear.Op{
		{Client: 1, Kind: "write", Arg: 1, Ret: nil, Start: 1, End: 2},
		{Client: 2, Kind: "read", Arg: nil, Ret: 0, Start: 3, End: 4},
	}
}

// The two branches below carry DISTINCT markers, and the campaign's self-test
// asserts the first is present and the second absent.
//
// They used to be told apart by the words "not linearizable", which BOTH
// messages contain -- so a `linear.Check` mutated to accept every history
// still satisfied the assertion, and the one thing this negative control
// exists to detect was the one thing it could not see. A review demonstrated
// it with a vacuous `Check`.
const (
	rejected = "NEGATIVE-CONTROL-REJECTED"
	accepted = "NEGATIVE-CONTROL-ACCEPTED"
)

// verdict fails either way, which is what a negative control is for: it fails
// with the checker's rejection when the checker works, and with a louder
// message when it does not.
func verdict(t *testing.T) {
	t.Helper()
	res := linear.Check(register(), stale(), 0)
	if res.OK {
		t.Fatal(accepted + ": the checker ACCEPTED a history that is not linearizable -- " +
			"testkit/linear is broken, and every linearizability check in this repository is vacuous")
	}
	t.Fatalf("%s: the history is not linearizable (partition %q, %d model steps explored); "+
		"this package exists to fail, and hack/linearize-campaign.sh --selftest asserts that it does",
		rejected, res.Partition, res.Explored)
}

// TestLinearizeSelfTest is what the campaign's `engine` stage runs under
// --selftest.
func TestLinearizeSelfTest(t *testing.T) { verdict(t) }

// FuzzLinearizeSelfTest is what its `fuzz` stage runs. The input is ignored:
// the history is fixed, because what is under test is the campaign's reporting
// and not the checker's search.
func FuzzLinearizeSelfTest(f *testing.F) {
	f.Add([]byte{0})
	f.Fuzz(func(t *testing.T, _ []byte) { verdict(t) })
}
