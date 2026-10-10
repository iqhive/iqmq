package main

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/benchmarks/internal/budgets"
)

// repoRoot is the repository root from this package's directory, which is
// where a Go test runs. `budgets.Load` takes "../../budgets.yaml" because the
// file is in the `benchmarks` module; anything that reads hack/bakeoff or a
// package directory needs one level more.
const repoRoot = "../../.."

func TestGlobsToRegexp(t *testing.T) {
	re := regexp.MustCompile(GlobsToRegexp([]string{"BenchmarkMemory/PubSubAck/1P1C/*", "BenchmarkHeaders/*/Get/n=8", "BenchmarkWire/*"}))
	for name, want := range map[string]bool{
		"BenchmarkMemory/PubSubAck/1P1C/safe":   true,
		"BenchmarkMemory/PubSubAck/8P8C/safe":   false,
		"BenchmarkHeaders/inline/Get/n=8":       true,
		"BenchmarkHeaders/inline/Get/n=8/extra": true,
		"BenchmarkHeaders/inline/Get/n=80":      false,
		"BenchmarkWire/tlv/Encode":              true,
		"BenchmarkWireX/tlv":                    false,
	} {
		if got := re.MatchString(name); got != want {
			t.Errorf("%s: %v want %v (re %s)", name, got, want, re)
		}
	}
	if GlobsToRegexp([]string{"*"}) != "." {
		t.Error("wildcard set")
	}
}

// The 30 ns figure of §10.2 -- "middleware chain of 5 no-ops <= 30 ns/msg,
// 0 allocs" -- was measured for the PUBLIC middleware package by A33 and gated
// only for family A30's internal/mwchain. The reason was mechanical: a budget
// whose benchmark the gate never measures is a budget that reports nothing and
// passes for ever, and `gate_packages` did not name ./middleware, so adding the
// row alone would have been that.
//
// Two things therefore have to be true at once, and this test is here because
// either of them can be undone on its own and nothing else would notice:
//
//   - the `pr` gate set has to SELECT the benchmark, or it is never measured;
//   - `gate_packages` has to name the package that holds it, or `make gate`
//     never builds the test binary the benchmark is in.
//
// It reads the real the benchmark budgets rather than a fixture, because
// what it is asserting is a fact about the shipped configuration.
func TestPublicMiddlewareChainIsGated(t *testing.T) {
	const (
		bench = "BenchmarkMiddleware/chain/n=5"
		pkg   = "./middleware"
	)
	cfg, err := budgets.Load("../../budgets.yaml")
	if err != nil {
		t.Fatalf("loading budgets.yaml: %v", err)
	}
	set, ok := cfg.GateSets["pr"]
	if !ok {
		t.Fatal("budgets.yaml has no `pr` gate set")
	}
	re := regexp.MustCompile(GlobsToRegexp(set))
	if !re.MatchString(bench) {
		t.Errorf("the `pr` gate set does not select %s, so the gate never measures it and its budget can never fail", bench)
	}
	if !cfg.InGateSet("pr", bench) {
		t.Errorf("InGateSet reports %s is not in the `pr` set, so benchgate would not count it as a gate row", bench)
	}
	budgeted := false
	for i := range cfg.Budgets {
		if budgets.Match(cfg.Budgets[i].Match, bench) {
			budgeted = true
			break
		}
	}
	if !budgeted {
		t.Errorf("no budget in budgets.yaml matches %s, so §10.2's 30 ns ceiling is measured and not enforced", bench)
	}
	// repoRoot, not "../..": the test's working directory is this package's
	// own directory, so "../.." is the `benchmarks` module. The first version
	// passed that, which made `readBakeoffEnvs` glob a hack/bakeoff that does
	// not exist -- so the family-derived half of the list was silently empty
	// and only budgets.yaml's own `gate_packages` entries were ever checked.
	pkgs, err := gatePackages(repoRoot, set, cfg.GatePackages["pr"])
	if err != nil {
		t.Fatalf("gatePackages: %v", err)
	}
	if !slices.Contains(pkgs, pkg) {
		t.Errorf("the gate measures %v, which does not include %s: the budget above would report nothing at all", pkgs, pkg)
	}
}

// TestEveryGateSetsPackagesCanBeBuilt is the `make ci` net for the defect
// assumption B11.2 records: a gate set whose package list names a directory
// that does not exist, or a package in no module, is a gate that dies at its
// compile step.
//
// It was true of the `nightly` set -- the documented safety net for everything
// the `pr` set leaves out -- from the moment family A12 declared
// `MODDIR=benchmarks`, because `gate-pattern -packages` printed the family's
// MODDIR-relative `./redisclient` and `hack/benchgate.sh` compiled it from the
// repository root:
//
//	# ./redisclient
//	stat .../redisclient: directory not found
//	FAIL	./redisclient [setup failed]
//	!! gate: head does not build ./redisclient
//
// Nothing in `make ci` could see it, because nothing in `make ci` derived a
// gate set's package list. This does, for every set in the shipped
// budgets.yaml, and it checks the two properties `hack/benchgate.sh` needs:
// the directory is there, and it is inside a module something can build it
// from.
func TestEveryGateSetsPackagesCanBeBuilt(t *testing.T) {
	const root = repoRoot
	cfg, err := budgets.Load("../../budgets.yaml")
	if err != nil {
		t.Fatalf("loading budgets.yaml: %v", err)
	}
	if len(cfg.GateSets) == 0 {
		t.Fatal("budgets.yaml declares no gate set")
	}
	for _, name := range sortedSetNames(cfg.GateSets) {
		pkgs, err := gatePackages(root, cfg.GateSets[name], cfg.GatePackages[name])
		if err != nil {
			// gatePackages refuses a list it knows the gate cannot measure,
			// so this IS the failure this test exists for.
			t.Errorf("gate set %q: %v", name, err)
			continue
		}
		if len(pkgs) == 0 {
			t.Errorf("gate set %q names no package, so the gate would measure nothing", name)
			continue
		}
		for _, pkg := range pkgs {
			dir := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(pkg, "./")))
			if info, serr := os.Stat(dir); serr != nil || !info.IsDir() {
				t.Errorf("gate set %q: %s is not a directory in this repository", name, pkg)
				continue
			}
			mod, ok := moduleOf(root, pkg)
			if !ok {
				t.Errorf("gate set %q: %s is in no Go module", name, pkg)
				continue
			}
			t.Logf("gate set %s: %s (module %s)", name, pkg, mod)
		}
	}
}

// TestAFamilyInANestedModuleIsGatedByItsRepositoryRelativePath pins the half
// of assumption B11.2 that fixes the paths rather than the check: a
// declaration's PKG is relative to its MODDIR, and everything that consumes
// the gate's package list -- the compile step, and
// hack/gate-if-touched.sh's `git diff --name-only` match -- needs it relative
// to the repository.
func TestAFamilyInANestedModuleIsGatedByItsRepositoryRelativePath(t *testing.T) {
	for _, tc := range []struct {
		fam  bakeoffFamily
		want string
	}{
		{bakeoffFamily{pkg: "./internal/engine"}, "./internal/engine"},
		{bakeoffFamily{pkg: "."}, "."},
		{bakeoffFamily{moddir: "benchmarks", pkg: "./redisclient"}, "./benchmarks/redisclient"},
		{bakeoffFamily{moddir: "drivers/kafka/kafkago", pkg: "."}, "./drivers/kafka/kafkago"},
		{bakeoffFamily{moddir: "drivers/gcppubsub/v1compat", pkg: "./sub"}, "./drivers/gcppubsub/v1compat/sub"},
	} {
		if got := tc.fam.gatePkg(); got != tc.want {
			t.Errorf("gatePkg(moddir=%q pkg=%q) = %q, want %q", tc.fam.moddir, tc.fam.pkg, got, tc.want)
		}
	}
}

func sortedSetNames[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// TestTheObserverRowIsGated is §10.2's observer row held together the way
// TestPublicMiddlewareChainIsGated holds the middleware row: three edits that
// have to be true at once, any one of which can be undone on its own with
// nothing else noticing.
//
// It is also the first gated package in a NESTED module, which is what
// assumptions B11.2 and B11.6 made possible: the gate resolves a package's
// module rather than assuming the root, and it measures a package the base
// tree does not have on the candidate only. Without both, the edit that
// closes this budget's loop could not be landed.
func TestTheObserverRowIsGated(t *testing.T) {
	const (
		bench = "BenchmarkHooks/disabled"
		pkg   = "./benchmarks/hooks"
	)
	cfg, err := budgets.Load("../../budgets.yaml")
	if err != nil {
		t.Fatalf("loading budgets.yaml: %v", err)
	}
	set, ok := cfg.GateSets["pr"]
	if !ok {
		t.Fatal("budgets.yaml has no `pr` gate set")
	}
	if !cfg.InGateSet("pr", bench) {
		t.Errorf("the `pr` gate set does not select %s, so the gate never measures it and its budget can never fail", bench)
	}
	budgeted := false
	for i := range cfg.Budgets {
		b := &cfg.Budgets[i]
		if !budgets.Match(b.Match, bench) {
			continue
		}
		budgeted = true
		if !b.AllocsSet {
			t.Errorf("the budget matching %s sets no allocs/op ceiling; §10.2's observer row IS an allocation count", bench)
		}
	}
	if !budgeted {
		t.Errorf("no budget in budgets.yaml matches %s, so §10.2's observer row is measured and not enforced", bench)
	}
	pkgs, err := gatePackages(repoRoot, set, cfg.GatePackages["pr"])
	if err != nil {
		t.Fatalf("gatePackages: %v", err)
	}
	if !slices.Contains(pkgs, pkg) {
		t.Errorf("the gate measures %v, which does not include %s: the budget above would report nothing at all", pkgs, pkg)
	}
	// The half that is specific to this row: the package is in a nested
	// module, so a gate that assumed the root module could not build it.
	mod, ok := moduleOf(repoRoot, pkg)
	if !ok {
		t.Fatalf("%s is in no Go module", pkg)
	}
	if mod == "." {
		t.Errorf("%s resolved to the root module; this test exists because it is in a nested one", pkg)
	}
}
