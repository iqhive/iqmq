package main

import (
	"maps"
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

// TestGlobsToRegexpSelectsWhatMatchSelects: `make budgets-lint` and
// benchgate's InGateSet decide what a gate-set pattern names with
// budgets.Match, which expands a {a,b} alternation, and the gate measures with
// the -bench regular expression GlobsToRegexp builds. That quoted the braces
// literally, so `BenchmarkHeaders/{Get,Set}/keys=registry/n=8` in the `pr` set
// passed the lint, was counted as evaluating its two budgets, and selected no
// benchmark at all.
func TestGlobsToRegexpSelectsWhatMatchSelects(t *testing.T) {
	names := []string{
		"BenchmarkHeaders/Get/keys=registry/n=8",
		"BenchmarkHeaders/Set/keys=registry/n=8",
		"BenchmarkHeaders/Del/keys=registry/n=8",
		"BenchmarkHeaders/Get/keys=inline/n=8",
		"BenchmarkHeaders/Get/keys=registry/n=80",
		"BenchmarkDriver/redis/streams/Publish/64B",
		"BenchmarkDriver/redis/streams/PublishBatch/64B",
		"BenchmarkDriver/redis/streams/Fetch/64B",
		"BenchmarkNative/goredis/Publish/64B",
		"BenchmarkNative/rueidis/Publish/64B",
	}
	for _, pattern := range []string{
		"BenchmarkHeaders/Get/keys=registry/n=8",
		"BenchmarkHeaders/{Get,Set}/keys=registry/n=8",
		"BenchmarkHeaders/{Get,Set}/keys={registry,inline}/n=8",
		"BenchmarkDriver/redis/streams/{Publish,PublishBatch}/*",
		"BenchmarkNative/{goredis,nats}/Publish/*",
		// An alternative that spans a segment boundary.
		"Benchmark{Headers/Del,Native/rueidis}/*",
	} {
		re := regexp.MustCompile(GlobsToRegexp([]string{pattern}))
		for _, name := range names {
			if got, want := re.MatchString(name), budgets.Match(pattern, name); got != want {
				t.Errorf("pattern %q, %s: the gate's -bench %q selects it: %v; budgets.Match: %v", pattern, name, re, got, want)
			}
		}
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
// It reads the real benchmarks/budgets.yaml rather than a fixture, because
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

// TestGateNamesListsWhatTheGateAndTheCampaignMeasure drives the listing
// `make budgets-lint` checks budgets.yaml against, over a one-module tree with
// one family whose declared MATRIX=full adds a row the default grid lacks.
//
// The default pass lists the gate's rows. The full-grid row is listed, marked
// `campaign`, only because a §8.10 target names it: a budget that names it is
// not enough, because the gate never sets IQMQ_BENCH_MATRIX and could not
// measure the row. A row nothing produces is never listed, so a mistyped
// pattern still matches nothing, and the caller's IQMQ_ environment does not
// reach the listing.
func TestGateNamesListsWhatTheGateAndTheCampaignMeasure(t *testing.T) {
	const gateSets = "gate_sets:\n  pr: [\"BenchmarkThing/small\"]\n"
	for _, c := range []struct {
		name, budgets string
		env           map[string]string
		want          []string
	}{
		{
			name: "a target reaches the family's full grid",
			budgets: gateSets + "targets:\n  - {match: \"BenchmarkThing/large\", ns_op_target: 100}\n" +
				"  - {match: \"BenchmarkThing/lrage\", ns_op_target: 100}\n",
			want: []string{"BenchmarkThing/small pr", "campaign BenchmarkThing/large"},
		},
		{
			// The campaign listing runs `go test -bench` with GlobsToRegexp's
			// expression, so a target written with braces selected nothing
			// and was refused although the family measures it.
			name:    "a target written with braces reaches it too",
			budgets: gateSets + "targets:\n  - {match: \"BenchmarkThing/{large,huge}\", ns_op_target: 100}\n",
			want:    []string{"BenchmarkThing/small pr", "campaign BenchmarkThing/large"},
		},
		{
			name: "a budget does not",
			budgets: gateSets + "budgets:\n" +
				"  - {match: \"BenchmarkThing/large\", allocs_op_max: 0, kind: specification, spec: \"fixture\"}\n",
			want: []string{"BenchmarkThing/small pr"},
		},
		{
			name:    "the caller's IQMQ_BENCH_MATRIX does not widen the default pass",
			budgets: gateSets,
			env:     map[string]string{"IQMQ_BENCH_MATRIX": "full"},
			want:    []string{"BenchmarkThing/small pr"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			root := gateNamesTree(t, c.budgets, nil)
			var err error
			out := captureStdout(t, func() {
				err = gateNames([]string{"-budgets", filepath.Join(root, "budgets.yaml"), "-root", root})
			})
			if err != nil {
				t.Fatalf("gate-names: %v", err)
			}
			got := strings.Split(strings.TrimSpace(out), "\n")
			if !slices.Equal(got, c.want) {
				t.Errorf("gate-names listed %q, want %q", got, c.want)
			}
		})
	}
}

// TestGateNamesSaysWhichGateSetMeasuresEachName is tt6-4. gate-names printed
// the names of every in-process gate set's packages as one list, and
// `benchgate -lint` checked each set's patterns against it, so a `pr` pattern
// naming a benchmark that only a `nightly` package holds passed `make
// budgets-lint` while hack/benchgate.sh compiles and runs only the `pr` set's
// packages: the pattern, and any budget over its rows, was never measured.
// Each name now carries the gate sets whose packages report it.
func TestGateNamesSaysWhichGateSetMeasuresEachName(t *testing.T) {
	root := gateNamesTree(t,
		"gate_sets:\n  pr: [\"BenchmarkThing/small\"]\n  nightly: [\"*\"]\n  selftest: [\"BenchmarkOther/*\"]\n",
		map[string]string{
			"q/q_test.go": "package q\n\nimport \"testing\"\n\n" +
				"func BenchmarkOther(b *testing.B) {\n" +
				"\tb.Run(\"x\", func(b *testing.B) { for range b.N {} })\n}\n",
			"hack/bakeoff/F2.env": "PKG=./q\nBENCH=BenchmarkOther\n",
		})
	var err error
	out := captureStdout(t, func() {
		err = gateNames([]string{"-budgets", filepath.Join(root, "budgets.yaml"), "-root", root})
	})
	if err != nil {
		t.Fatalf("gate-names: %v", err)
	}
	got := strings.Split(strings.TrimSpace(out), "\n")
	want := []string{"BenchmarkOther/x nightly,selftest", "BenchmarkThing/small nightly,pr"}
	if !slices.Equal(got, want) {
		t.Errorf("gate-names listed %q, want %q", got, want)
	}
}

// gateNamesTree writes the one-module tree TestGateNamesListsWhatTheGateAndTheCampaignMeasure
// lists, with the given budgets document and any extra files.
func gateNamesTree(t *testing.T, budgetsDoc string, extra map[string]string) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"go.mod": "module example.com/fx\n\ngo 1.26\n",
		"p/p_test.go": "package p\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\n" +
			"func BenchmarkThing(b *testing.B) {\n" +
			"\tb.Run(\"small\", func(b *testing.B) { for range b.N {} })\n" +
			"\tif os.Getenv(\"IQMQ_BENCH_MATRIX\") == \"full\" {\n" +
			"\t\tb.Run(\"large\", func(b *testing.B) { for range b.N {} })\n" +
			"\t}\n}\n",
		"hack/bakeoff/F1.env": "PKG=./p\nBENCH=BenchmarkThing\nMATRIX=full\n",
		"budgets.yaml":        budgetsDoc,
	}
	maps.Copy(files, extra)
	for path, body := range files {
		p := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
