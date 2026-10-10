package main

import (
	"encoding/json"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestEveryDeclaredFamilyHasADecisionRule holds the rules table against the
// campaign: every family hack/bakeoff/*.env declares must have a §9.1/§9.2
// criterion `bakeoff decide` can apply, and no row may exist here for a
// family the campaign does not measure. Without this the table drifts and a
// family added to the campaign is refused mid-run instead of at `make ci`.
func TestEveryDeclaredFamilyHasADecisionRule(t *testing.T) {
	// The campaign's declarations are in-tree, three levels up from this
	// package -- the same path adr_test.go and lintenv_test.go read them
	// from. This was `..`/`..` for one release, which resolves to
	// benchmarks/hack/bakeoff, a directory that has never existed: the glob
	// came back empty, the test skipped, and the table drifted until family
	// A15 was refused in the middle of a bake-off by the very error this
	// test exists to raise at `make ci`. A missing directory is now a
	// failure, not a skip, so the check cannot pass by finding nothing.
	dir := filepath.Join("..", "..", "..", "hack", "bakeoff")
	paths, err := filepath.Glob(filepath.Join(dir, "*.env"))
	if err != nil {
		t.Fatalf("globbing %s: %v", dir, err)
	}
	if len(paths) == 0 {
		t.Fatalf("no %s/*.env files: this test decides nothing unless it reads the campaign's declarations", dir)
	}
	declared := map[string]bool{}
	for _, p := range paths {
		declared[strings.TrimSuffix(filepath.Base(p), ".env")] = true
	}
	var missing []string
	for fam := range declared {
		if _, ok := ruleFor(fam); !ok {
			missing = append(missing, fam)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("families the campaign measures with no decision rule: %s\n"+
			"`score` refuses them rather than substituting a rule of its own; add them to `rules`",
			strings.Join(missing, ", "))
	}
	var orphan []string
	for _, fam := range ruleFamilies() {
		if !declared[fam] {
			orphan = append(orphan, fam)
		}
	}
	if len(orphan) > 0 {
		t.Errorf("decision rules for families with no hack/bakeoff/<id>.env: %s", strings.Join(orphan, ", "))
	}
	// Every rule must say, in the specification's own words, what it applies.
	for _, fam := range ruleFamilies() {
		r, _ := ruleFor(fam)
		if strings.TrimSpace(r.Criterion) == "" {
			t.Errorf("%s: no criterion quoted; the scorecard and the ADR state the rule they were decided by", fam)
		}
		if r.Fixed && r.Incumbent == "" {
			t.Errorf("%s: Fixed with no Incumbent names no default", fam)
		}
		if r.Margin != 0 && r.Incumbent == "" {
			t.Errorf("%s: a margin with no incumbent has nothing to be measured against", fam)
		}
	}
}

// TestSelectsMatchesWholeSegments: a workload selector must not match a
// substring of a dimension. "16" selects family A2's 16-subscriber fan-out
// and must not select family A1's batch=16.
func TestSelectsMatchesWholeSegments(t *testing.T) {
	for _, tc := range []struct {
		row, sel string
		want     bool
	}{
		{"BenchmarkMemory/FanOut/16/policy=atomic", "16", true},
		{"BenchmarkMemory/Queue/MPMC/P=4/batch=16/occupancy=50", "16", false},
		{"BenchmarkMemory/Queue/MPMC/P=4/batch=16/occupancy=50", "MPMC", true},
		{"BenchmarkMemory/Queue/MPSC/P=4/batch=16/occupancy=50", "MPMC", false},
		{"BenchmarkStats/P=32/op=add", "P=8|P=32|P=64", true},
		{"BenchmarkStats/P=1/op=add", "P=8|P=32|P=64", false},
		{"BenchmarkMiddleware/n=5/enabled", "enabled", true},
		{"BenchmarkMiddleware/n=5/disabled", "enabled", false},
	} {
		if got := selects(tc.row, tc.sel); got != tc.want {
			t.Errorf("selects(%q, %q) = %v, want %v", tc.row, tc.sel, got, tc.want)
		}
	}
	if !selectsAll("BenchmarkMemory/Scheduler/insert/n=1000000/dist=uniform", []string{"insert|fire", "n=1000000"}) {
		t.Error("selectsAll rejected a row matching both selectors")
	}
	if selectsAll("BenchmarkMemory/Scheduler/cancel/n=1000000/dist=uniform", []string{"insert|fire", "n=1000000"}) {
		t.Error("selectsAll accepted a row matching only one selector")
	}
}

// TestRulesCoverTheSpecifiedMargins pins the three §9.1 rows whose criterion
// states a margin other than §0.3(2)'s 3%, because applying 3% to them is
// exactly the defect the table exists to fix.
func TestRulesCoverTheSpecifiedMargins(t *testing.T) {
	for fam, want := range map[string]struct {
		incumbent string
		margin    float64
	}{
		"A20": {"json", 0.30},      // "stdlib unless > 30% gain"
		"A26": {"interface", 0.05}, // "bind default via build tag if > 5% cost"
		"A6":  {"none", 0},         // "adopt only if measurably better"
		"A30": {"closure", 0},      // "closure chain unless beaten"
	} {
		r, ok := ruleFor(fam)
		if !ok {
			t.Fatalf("%s has no rule", fam)
		}
		if r.Incumbent != want.incumbent || r.Margin != want.margin {
			t.Errorf("%s: incumbent %q margin %v; want %q %v", fam, r.Incumbent, r.Margin, want.incumbent, want.margin)
		}
	}
	if r, _ := ruleFor("A10"); r.Undecidable == "" {
		t.Error("A10's arms are two builds and this tool compares kinds in one results file (assumption A24.30)")
	}
	// §9.1 and §10.5 put arms under a family's root that can never be its
	// default: a no-boundary floor, a partitioner, a range reduction. Each
	// was measured against the real benchmark names, and each would either
	// win by construction or make its family undecidable.
	for fam, want := range map[string][]string{
		"A4":  {"stdtimer"},
		"A5":  {"Lane", "KafkaPartition"},
		"A26": {"native"},
		"B6":  {"direct"},
	} {
		r, _ := ruleFor(fam)
		for _, slug := range want {
			if !r.isBaseline(slug) {
				t.Errorf("%s: %q is not declared a baseline", fam, slug)
			}
		}
	}
	for _, fam := range []string{"A7", "A29", "A37"} {
		if r, _ := ruleFor(fam); !r.Fixed {
			t.Errorf("%s's §9.1 row states its default outright; the numbers report overhead and do not choose", fam)
		}
	}
}

// TestRuleBaselinesAreNotCrowned: §9.1 and §10.5 mark some arms as
// baselines -- A4's one time.Timer per message, B6's driver-direct call --
// and reading one as a sibling would read the absence of a feature as a
// cheaper implementation of it.
func TestRuleBaselinesAreNotCrowned(t *testing.T) {
	const root = "BenchmarkMemory/Scheduler"
	in := rows(root+"/heap/insert/n=1000/dist=uniform", 8, 5, 100, 32, 1) +
		rows(root+"/heap/insert/n=1000/dist=skewed", 8, 5, 100, 32, 1) +
		rows(root+"/hwheel/insert/n=1000/dist=uniform", 8, 5, 90, 32, 1) +
		rows(root+"/hwheel/insert/n=1000/dist=skewed", 8, 5, 90, 32, 1) +
		rows(root+"/stdtimer/insert/n=1000/dist=uniform", 8, 5, 10, 0, 0)
	sc, err := score(decideOpts{family: "A4", benchRoot: root, ref: "heap"}, parseAll(t, in), nil)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Winner != "hwheel" {
		t.Errorf("winner = %s (%s); the stdtimer arm is §9.1 row A4's baseline", sc.Winner, sc.Rule)
	}
	for _, k := range sc.Kinds {
		if k.Kind == "stdtimer" && !strings.Contains(k.Disposition, "baseline") {
			t.Errorf("stdtimer disposition = %q", k.Disposition)
		}
	}
	// The baseline covers only dist=uniform, and it must not have narrowed
	// the decision to that row either.
	if len(sc.Common) != 2 {
		t.Errorf("decided on %d row(s): %v", len(sc.Common), sc.Common)
	}
}

// The optional part of a workload selector -- a shape only
// IQMQ_BENCH_MATRIX widens the benchmark to -- is dropped with a note rather
// than refused, and the note says the row's own criterion named a shape the
// run did not measure.
func TestOptionalWorkloadIsNotedNotRefused(t *testing.T) {
	const root = "BenchmarkMemory/Scheduler"
	in := rows(root+"/heap/insert/n=1000/dist=uniform", 8, 5, 100, 0, 0) +
		rows(root+"/hwheel/insert/n=1000/dist=uniform", 8, 5, 90, 0, 0)
	sc, err := score(decideOpts{family: "A4", benchRoot: root, ref: "heap"}, parseAll(t, in), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(sc.Notes, "\n"), "n=1000000") {
		t.Errorf("nothing records that the row's stated size was not measured: %v", sc.Notes)
	}
}

// A required selector that matches no measured row IS refused: a criterion
// naming a shape the run did not measure cannot be applied to it.
func TestRequiredWorkloadThatMatchesNothingIsRefused(t *testing.T) {
	const root = "BenchmarkWire"
	in := rows(root+"/fixed/Encode/1KiB/headers=0", 8, 5, 100, 0, 0) +
		rows(root+"/tlv/Encode/1KiB/headers=0", 8, 5, 90, 0, 0)
	_, err := score(decideOpts{family: "A18", benchRoot: root, ref: "fixed"}, parseAll(t, in), nil)
	if err == nil {
		t.Fatal("family A18 was decided on encode rows; §9.1 row A18 is decided on decode")
	}
	if !strings.Contains(err.Error(), "Decode") {
		t.Errorf("error = %v", err)
	}
}

// A row decided on a latency percentile is refused when the run recorded
// none, rather than decided on throughput instead.
func TestARowDecidedOnP99IsRefusedWithoutP99(t *testing.T) {
	const root = "BenchmarkMemory/Wait"
	in := rows(root+"/chan/occupancy=0", 8, 5, 100, 0, 0) +
		rows(root+"/hybrid/occupancy=0", 8, 5, 90, 0, 0)
	_, err := score(decideOpts{family: "A3", benchRoot: root, ref: "chan"}, parseAll(t, in), nil)
	if err == nil {
		t.Fatal("family A3 was decided without the percentile its §9.1 row names")
	}
	if !strings.Contains(err.Error(), "p99-ns") {
		t.Errorf("error = %v", err)
	}
}

// A row that caps a metric excludes the kinds over the cap: §9.1 row A3's
// "≤ 1% idle CPU".
func TestARowsMetricCeilingExcludes(t *testing.T) {
	const root = "BenchmarkMemory/Wait"
	var b strings.Builder
	for _, k := range []struct {
		slug   string
		ns, cp float64
	}{{"spin", 50, 96}, {"hybrid", 80, 0.4}, {"chan", 100, 0.2}} {
		for i := range 5 {
			// occupancy rows carry the percentile the row is decided on.
			b.WriteString(sprintfLine(root+"/"+k.slug+"/occupancy=0", 8, k.ns+float64(i), k.ns*2))
			b.WriteString(cpuLine(root+"/"+k.slug+"/IdleCPU", 8, k.cp))
		}
	}
	sc, err := score(decideOpts{family: "A3", benchRoot: root, ref: "chan"}, parseAll(t, b.String()), nil)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Winner != "hybrid" {
		t.Errorf("winner = %s (%s); spin burns 96%% of a processor while parked and §9.1 row A3 caps that at 1%%", sc.Winner, sc.Rule)
	}
	if !strings.Contains(strings.Join(sc.Notes, "\n"), "cpu-pct") {
		t.Errorf("nothing records the ceiling exclusion: %v", sc.Notes)
	}
}

// TestReportedWorkloadsAreNamed: §9.1 row A27 names two workloads, and a
// default chosen on the geomean over both must still show each one's own
// best kind.
func TestReportedWorkloadsAreNamed(t *testing.T) {
	const root = "BenchmarkRegistry"
	in := rows(root+"/rwmutexmap/readheavy/P=8", 8, 5, 200, 0, 0) +
		rows(root+"/rwmutexmap/churn/P=8", 8, 5, 200, 0, 0) +
		rows(root+"/sharded/readheavy/P=8", 8, 5, 90, 0, 0) +
		rows(root+"/sharded/churn/P=8", 8, 5, 150, 0, 0) +
		rows(root+"/syncmap/readheavy/P=8", 8, 5, 60, 0, 0) +
		rows(root+"/syncmap/churn/P=8", 8, 5, 400, 0, 0)
	sc, err := score(decideOpts{family: "A27", benchRoot: root, ref: "rwmutexmap"}, parseAll(t, in), nil)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(sc.Notes, "\n")
	for _, want := range []string{`best on the "readheavy" rows alone: syncmap`, `best on the "churn" rows alone: sharded`} {
		if !strings.Contains(joined, want) {
			t.Errorf("notes do not say %q: %v", want, sc.Notes)
		}
	}
}

// A family this tool cannot decide says why in the refusal itself: the row's
// own note travels with it, so an operator at the end of a campaign is not
// left with a coverage table and no explanation.
func TestCoverageRefusalCarriesTheRowsNote(t *testing.T) {
	const root = "BenchmarkPublish/Async"
	in := rows(root+"/inline/inflight=1", 8, 5, 100, 0, 0) +
		rows(root+"/deferred/inflight=1", 8, 5, 110, 0, 0) +
		rows(root+"/future/inline/inflight=1", 8, 5, 90, 0, 0) +
		rows(root+"/future/deferred/inflight=1", 8, 5, 95, 0, 0) +
		rows(root+"/channel/inline/inflight=1", 8, 5, 120, 0, 0) +
		rows(root+"/channel/deferred/inflight=1", 8, 5, 125, 0, 0)
	_, err := score(decideOpts{family: "A22", benchRoot: root, ref: "inline"}, parseAll(t, in), nil)
	if err == nil {
		t.Fatal("family A22's four kinds have disjoint rows and it was decided anyway")
	}
	for _, want := range []string{"inline covers", "Note:", "grid"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v; it must contain %q", err, want)
		}
	}
}

// A scorecard is what an ADR is rendered from, so two runs of `decide` over
// one raw.txt must produce the same scorecard -- the same winner, the same
// intervals and the same notes in the same order.
func TestScoreIsReproducible(t *testing.T) {
	const root = "BenchmarkStats"
	var b strings.Builder
	for _, k := range []struct {
		slug string
		ns   float64
	}{{"atomic", 100}, {"striped", 70}, {"perworker", 71}} {
		for _, p := range []int{1, 8} {
			for _, procs := range []int{1, 8} {
				b.WriteString(rows(root+"/"+k.slug+"/P="+itoa(p)+"/op=add", procs, 10, k.ns, 0, 0))
			}
		}
	}
	rs := parseAll(t, b.String())
	first, err := score(decideOpts{family: "A9", benchRoot: root, ref: "atomic"}, rs, nil)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		again, err := score(decideOpts{family: "A9", benchRoot: root, ref: "atomic"}, rs, nil)
		if err != nil {
			t.Fatal(err)
		}
		a, _ := json.Marshal(first)
		c, _ := json.Marshal(again)
		// DecidedAt is a timestamp and is expected to differ.
		as := strings.ReplaceAll(string(a), first.DecidedAt, "")
		cs := strings.ReplaceAll(string(c), again.DecidedAt, "")
		if as != cs {
			t.Fatalf("two runs over one raw.txt disagree:\n%s\n%s", as, cs)
		}
	}
}
