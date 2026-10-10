package budgets

import (
	"math"
	"strings"
	"testing"
)

// TestThroughputLossIsTheRateNotTheDuration is the gate's half of the one
// piece of arithmetic every overhead figure rests on. Its twin is
// `TestLossIsTheRateNotTheDuration` beside benchmarks/cmd/overhead, which
// pins the same boundaries on the REPORTING side -- and they are held to each
// other by construction as well as by these two tables, because
// cmd/overhead's `loss` calls this function.
//
// The specification budgets a "throughput loss", and an arm that takes 10 % LONGER per
// operation does not move 10 % fewer messages a second -- it moves 9.09 %
// fewer. Reading the duration would overstate every row and in the wrong
// direction: it reports a breach on a row that meets the budget (watched:
// with the duration reading, the self-test fixture "5 % longer meets the 5 %
// budget" fails with exit=1 want=0).
func TestThroughputLossIsTheRateNotTheDuration(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		numeratorNs, denomNs, want float64
	}{
		{"identical arms lose nothing", 100, 100, 0},
		{"10 % longer per op is 9.09 % of the rate", 110, 100, 9.0909},
		{"5 % longer per op is 4.76 % of the rate", 105, 100, 4.7619},
		{"twice as slow is half the throughput", 200, 100, 50},
		{"faster than the raw client is a negative loss", 90, 100, -11.1111},
		{"a zero numerator has no ratio", 0, 100, 0},
	} {
		if got := ThroughputLossPct(tc.numeratorNs, tc.denomNs); math.Abs(got-tc.want) > 0.001 {
			t.Errorf("%s: ThroughputLossPct(%g, %g) = %.4f, want %.4f", tc.name, tc.numeratorNs, tc.denomNs, got, tc.want)
		}
	}
	// The two boundaries the budgets really permit.
	if got := ThroughputLossPct(105, 100); got > 5 {
		t.Errorf("a 5 %%-longer operation reports %.4f %% throughput loss, which would breach the 5 %% batched budget", got)
	}
	if got := ThroughputLossPct(105.264, 100); got < 5 {
		t.Errorf("a 5.264 %%-longer operation reports %.4f %%, which should be at the 5 %% budget", got)
	}
	if got := ThroughputLossPct(111.2, 100); got < 10 {
		t.Errorf("an 11.2 %%-longer operation reports %.4f %%, which should be over the 10 %% single-message budget", got)
	}
}

// TestARatioBudgetIsRefusedUnlessItCanBeDecided drives every rule
// `Config.validateRatio` holds a ratio row to. Each one is a way the row could
// otherwise report nothing and be indistinguishable from a budget that is met
// , or evaluate for ever in a gate set that cannot measure
// it.
func TestARatioBudgetIsRefusedUnlessItCanBeDecided(t *testing.T) {
	const head = `
gate_sets:
  overhead: ["BenchmarkDriver/*", "BenchmarkNative/*"]
  pr: ["BenchmarkMemory/*"]
budgets:
`
	good := `  - kind: ratio
    numerator: "BenchmarkDriver/x/Publish/64B"
    denominator: "BenchmarkNative/y/Publish/64B"
    max_pct: 10
    gates: ["overhead"]
    spec: " fixture"
`
	for _, tc := range []struct {
		name, doc, want string
	}{
		{"a well-formed ratio", good, ""},
		{
			"no denominator",
			`  - {kind: ratio, numerator: "BenchmarkDriver/x/Publish/64B", max_pct: 10, gates: ["overhead"], spec: "s"}` + "\n",
			"needs both a numerator: and a denominator:",
		},
		{
			"no max_pct",
			`  - {kind: ratio, numerator: "BenchmarkDriver/x/Publish/64B", denominator: "BenchmarkNative/y/Publish/64B", gates: ["overhead"], spec: "s"}` + "\n",
			"ceiling is max_pct",
		},
		{
			"no gate set, so it would be evaluated by every one of them",
			`  - {kind: ratio, numerator: "BenchmarkDriver/x/Publish/64B", denominator: "BenchmarkNative/y/Publish/64B", max_pct: 10, spec: "s"}` + "\n",
			"must name the gate set(s) it is evaluated in",
		},
		{
			"a gate set that does not exist",
			`  - {kind: ratio, numerator: "BenchmarkDriver/x/Publish/64B", denominator: "BenchmarkNative/y/Publish/64B", max_pct: 10, gates: ["nope"], spec: "s"}` + "\n",
			`gates names "nope", which is not a gate set`,
		},
		{
			"a gate set that does not select the arms",
			`  - {kind: ratio, numerator: "BenchmarkDriver/x/Publish/64B", denominator: "BenchmarkNative/y/Publish/64B", max_pct: 10, gates: ["pr"], spec: "s"}` + "\n",
			"does not select the",
		},
		{
			"no spec citation",
			`  - {kind: ratio, numerator: "BenchmarkDriver/x/Publish/64B", denominator: "BenchmarkNative/y/Publish/64B", max_pct: 10, gates: ["overhead"]}` + "\n",
			"must cite its specification row in spec:",
		},
		{
			"an absolute ceiling as well",
			`  - {kind: ratio, numerator: "BenchmarkDriver/x/Publish/64B", denominator: "BenchmarkNative/y/Publish/64B", max_pct: 10, ns_op_max: 5, gates: ["overhead"], spec: "s"}` + "\n",
			"takes no ns_op_max",
		},
		{
			"a match as well, which is a second identity",
			`  - {kind: ratio, match: "BenchmarkDriver/x/Publish/64B", numerator: "BenchmarkDriver/x/Publish/64B", denominator: "BenchmarkNative/y/Publish/64B", max_pct: 10, gates: ["overhead"], spec: "s"}` + "\n",
			"takes no match:",
		},
		{
			"the same pattern on both sides",
			`  - {kind: ratio, numerator: "BenchmarkDriver/x/Publish/64B", denominator: "BenchmarkDriver/x/Publish/64B", max_pct: 10, gates: ["overhead"], spec: "s"}` + "\n",
			"the ratio is 1 by construction",
		},
	} {
		cfg, err := Parse([]byte(head + tc.doc))
		if err != nil {
			t.Errorf("%s: Parse: %v", tc.name, err)
			continue
		}
		problems := cfg.Validate()
		if tc.want == "" {
			if len(problems) != 0 {
				t.Errorf("%s: want no problem, got %v", tc.name, problems)
			}
			continue
		}
		found := false
		for _, p := range problems {
			if strings.Contains(p, tc.want) {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: no problem mentions %q; got %v", tc.name, tc.want, problems)
		}
	}
}

// TestGatePackagesAreReadPerGateSet pins the half that is
// in this package: `gate_packages` is a map from gate set to packages, so a
// package added for one set is not compiled and run by every other one.
func TestGatePackagesAreReadPerGateSet(t *testing.T) {
	cfg, err := Parse([]byte(`
gate_sets:
  pr: ["BenchmarkA/*"]
  selftest: ["BenchmarkB/*"]
gate_packages:
  pr: ["./x", "./y"]
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.GatePackages["pr"]; len(got) != 2 {
		t.Errorf("pr gate packages = %v, want two", got)
	}
	if got := cfg.GatePackages["selftest"]; len(got) != 0 {
		t.Errorf("selftest gate packages = %v, want none: a set with no entry gets no extra packages", got)
	}
	if problems := cfg.Validate(); len(problems) != 0 {
		t.Errorf("Validate = %v", problems)
	}
	// And a key that names no gate set buys nothing, so it is refused.
	cfg, err = Parse([]byte(`
gate_sets:
  pr: ["BenchmarkA/*"]
gate_packages:
  typo: ["./x"]
`))
	if err != nil {
		t.Fatal(err)
	}
	problems := cfg.Validate()
	found := false
	for _, p := range problems {
		if strings.Contains(p, "which is not a gate set") {
			found = true
		}
	}
	if !found {
		t.Errorf("a gate_packages key naming no gate set must be refused; got %v", problems)
	}
}
