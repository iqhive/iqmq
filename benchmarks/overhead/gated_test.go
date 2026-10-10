package overhead_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/benchmarks/internal/budgets"
	"github.com/iqhive/iqmq/benchmarks/opbench"
	"github.com/iqhive/iqmq/benchmarks/overhead"
)

// comparedOps are the two operations the overhead budgets are
// written about, with the throughput loss each is allowed. `Receive` and
// `EndToEnd` are measured beside them and deliberately not compared: the specification's
// native-comparison rules require the same prefetch on both sides and these
// arms do not have it, measured in both directions and at two orders of
// magnitude. benchmarks/cmd/overhead's `budget` is the
// same table for the reporting side, and `TestBudgetsFollowTheSpecification`
// pins it there.
var comparedOps = map[string]float64{"Publish": 10, "PublishBatch": 5}

// TestEveryProfileWithADenominatorIsGated is the check that keeps the policy honest.
//
// The policy: a `kind: ratio` budget is declared only for the driver profiles
// that HAVE a raw-client denominator, so the gate is silent on fourteen of the
// nineteen driver profiles that exist. The alternative of declaring a row per
// profile would leave fourteen rows permanently inconclusive and every run of
// the gate red, and the alternative of covering the absence with a waiver
// would reuse a mechanism that means "measured and not met" to mean "not
// measurable yet" -- both rejected, both recorded.
//
// The cost of the policy is the silence, and this test is what stops the
// silence being permanent by accident: the moment a profile acquires a
// `Native` baseline -- which is exactly what closing one of
// docs/performance/overhead.md's "no denominator" rows means -- this test
// fails until the ratio rows for it are declared. A budget nobody declared is
// otherwise indistinguishable from a budget that is met ,
// and that is a failure this repository has found six times.
func TestEveryProfileWithADenominatorIsGated(t *testing.T) {
	cfg, err := budgets.Load("../budgets.yaml")
	if err != nil {
		t.Fatalf("loading budgets.yaml: %v", err)
	}
	declared := map[string]*budgets.Budget{}
	for i := range cfg.Budgets {
		b := &cfg.Budgets[i]
		if b.IsRatio() {
			declared[b.Numerator+" / "+b.Denominator] = b
		}
	}
	if len(declared) == 0 {
		t.Fatal("budgets.yaml declares no kind: ratio budget at all, so the first two budgets are ungated")
	}

	want := map[string]bool{}
	for _, p := range overhead.Profiles() {
		for _, op := range opbench.Ops() {
			for _, size := range opbench.Sizes() {
				num := strings.Join([]string{"BenchmarkDriver", p.Arm, op, size.Name}, "/")
				pct, compared := comparedOps[op]
				if p.Native == "" {
					// No denominator: there is nothing to divide by, so a row
					// for it would be inconclusive at every run. The profile
					// must NOT be declared, and docs/performance/overhead.md
					// is where the absence is explained.
					for key := range declared {
						if strings.HasPrefix(key, num+" /") {
							t.Errorf("%s has no raw-client baseline (Native is empty) and yet a ratio budget names it: the row can never be decided", num)
						}
					}
					continue
				}
				if !compared {
					for key := range declared {
						if strings.HasPrefix(key, num+" /") {
							t.Errorf("%s is a %s row, which the prefetch asymmetry forbids comparing, and yet a ratio budget names it", num, op)
						}
					}
					continue
				}
				den := strings.Join([]string{"BenchmarkNative", p.Native, op, size.Name}, "/")
				key := num + " / " + den
				want[key] = true
				b, ok := declared[key]
				if !ok {
					t.Errorf("no kind: ratio budget compares %s against %s, so the %s budget is unenforced for that row\n"+
						"    (add it to benchmarks/budgets.yaml; a profile with a denominator and no ratio row is a budget that reports nothing and passes for ever)", num, den, op)
					continue
				}
				if b.MaxPct != pct {
					t.Errorf("%s: max_pct is %g, want the %g for a %s row", key, b.MaxPct, pct, op)
				}
				if b.Kind != budgets.RatioKind || b.Spec == "" {
					t.Errorf("%s: kind=%q spec=%q; a ratio ceiling is a specification figure and cites its row", key, b.Kind, b.Spec)
				}
				if !b.InGate("overhead") {
					t.Errorf("%s: gates=%v, which does not include the set that measures both arms", key, b.Gates)
				}
			}
		}
	}
	for key := range declared {
		if !want[key] {
			t.Errorf("budgets.yaml declares a ratio %s that overhead.Profiles() does not pair: the gate would be enforcing a comparison nothing measures", key)
		}
	}
	t.Logf("%d ratio row(s) declared for %d of the profiles overhead.Profiles() measures", len(declared), countPaired())
}

func countPaired() int {
	n := 0
	for _, p := range overhead.Profiles() {
		if p.Native != "" {
			n++
		}
	}
	return n
}

// TestTheUnmeasuredProfilesAreNamedAndUngated states the other half of the cost in a form that fails when it stops being true:
// every profile the ratio gate says nothing about is named on
// docs/performance/overhead.md under one of its three obstacle kinds, so the
// silence is documented rather than merely absent.
func TestTheUnmeasuredProfilesAreNamedAndUngated(t *testing.T) {
	cfg, err := budgets.Load("../budgets.yaml")
	if err != nil {
		t.Fatalf("loading budgets.yaml: %v", err)
	}
	gated := map[string]bool{}
	for i := range cfg.Budgets {
		b := &cfg.Budgets[i]
		if !b.IsRatio() {
			continue
		}
		// `BenchmarkDriver/<arm...>/<op>/<size>` -> the arm.
		segs := strings.Split(b.Numerator, "/")
		if len(segs) < 4 {
			t.Errorf("ratio numerator %q is not BenchmarkDriver/<arm>/<op>/<size>", b.Numerator)
			continue
		}
		gated[strings.Join(segs[1:len(segs)-2], "/")] = true
	}
	for _, p := range overhead.Profiles() {
		if p.Native == "" {
			if gated[p.Arm] {
				t.Errorf("profile %s has no denominator and is gated", p.Arm)
			}
			continue
		}
		if !gated[p.Arm] {
			t.Errorf("profile %s has the denominator %q and no ratio budget", p.Arm, p.Native)
		}
	}
	if len(gated) == 0 {
		t.Fatal("no driver profile is gated by a ratio budget")
	}
	t.Logf("gated profiles: %s", strings.Join(sortedKeys(gated), ", "))
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
