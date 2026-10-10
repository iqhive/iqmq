package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// rows renders count samples of one benchmark row at one GOMAXPROCS rung, in
// the format `go test -bench -benchmem` prints.
func rows(name string, procs, count int, ns, bop, allocs float64) string {
	var b strings.Builder
	for i := range count {
		fmt.Fprintf(&b, "%s-%d\t1000\t%.1f ns/op\t%.0f B/op\t%.0f allocs/op\n",
			name, procs, ns+float64(i), bop, allocs)
	}
	return b.String()
}

// sample renders one explicit sample list, so that a test can control the
// spread and not just the level.
func samplesOf(name string, procs int, ns []float64) string {
	var b strings.Builder
	for _, v := range ns {
		fmt.Fprintf(&b, "%s-%d\t1000\t%.1f ns/op\t0 B/op\t0 allocs/op\n", name, procs, v)
	}
	return b.String()
}

func pkgWith(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		mustWrite(t, filepath.Join(dir, name), body)
	}
	return dir
}

// Defect 1: the GOMAXPROCS ladder is measured and thrown away. benchfmt.Base
// strips the -N suffix and score() keys its samples by it, so the four rungs
// of the full profile pool into one list and one median. `narrow` is fast at
// one thread and collapses at 32; `wide` is flat. Pooled, `wide` has the
// lower median and wins; per rung, `narrow` wins the geomean and the two
// cross over, which is the whole subject of families A1, A2 and A9.
func TestScoreKeepsTheGOMAXPROCSRung(t *testing.T) {
	const root = "BenchmarkMemory/Queue"
	in := rows(root+"/narrow/MPMC/P=4", 1, 5, 100, 0, 0) +
		rows(root+"/narrow/MPMC/P=4", 32, 5, 300, 0, 0) +
		rows(root+"/wide/MPMC/P=4", 1, 5, 190, 0, 0) +
		rows(root+"/wide/MPMC/P=4", 32, 5, 190, 0, 0)
	sc, err := score(decideOpts{family: "A1", benchRoot: root, ref: "wide"}, parseAll(t, in), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range sc.Kinds {
		if got := len(k.Medians); got != 2 {
			t.Errorf("kind %s: %d median(s) %v; one row measured at two rungs is two measurements", k.Kind, got, k.Medians)
		}
	}
	if sc.Winner != "narrow" {
		t.Errorf("winner = %s (%s); the pooled median of a 1-thread and a 32-thread run describes no configuration", sc.Winner, sc.Rule)
	}
	var crossover bool
	for _, n := range sc.Notes {
		if strings.Contains(n, "GOMAXPROCS") {
			crossover = true
		}
	}
	if !crossover {
		t.Errorf("no note reports that the winner changes with the rung: %v", sc.Notes)
	}
}

// Defect 2: the common-benchmark intersection can reduce a family to a corner
// of itself. Family A1's SPSC kind is single-producer and cannot run the
// multi-producer shapes, so the intersection of "every kind has ns/op here"
// is the SPSC rows alone -- and the family is then decided on the one shape
// §9.1's criterion ("ns/op & p99 under MPMC contention") is not about.
func TestScoreRefusesToDecideAFamilyOnACornerOfItself(t *testing.T) {
	const root = "BenchmarkMemory/Queue"
	in := rows(root+"/mutexring/SPSC/P=1", 8, 5, 100, 0, 0) +
		rows(root+"/mutexring/MPMC/P=4", 8, 5, 200, 0, 0) +
		rows(root+"/seqmpmc/SPSC/P=1", 8, 5, 80, 0, 0) +
		rows(root+"/seqmpmc/MPMC/P=4", 8, 5, 90, 0, 0) +
		rows(root+"/spsc/SPSC/P=1", 8, 5, 50, 0, 0)
	sc, err := score(decideOpts{family: "A1", benchRoot: root, ref: "mutexring"}, parseAll(t, in), nil)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Winner == "spsc" {
		t.Errorf("winner = spsc: a single-producer queue was crowned on the 1 of 3 rows it can run (rule %q)", sc.Rule)
	}
	if sc.Winner != "seqmpmc" {
		t.Errorf("winner = %s; want seqmpmc, decided on the MPMC rows §9.1 row A1 names", sc.Winner)
	}
	for _, k := range sc.Kinds {
		if k.Kind == "spsc" && !strings.Contains(k.Disposition, "candidate") {
			t.Errorf("spsc disposition = %q; a kind that cannot run the deciding workload is a fact to report", k.Disposition)
		}
	}
}

// A workload no two kinds span cannot be decided at all: the intersection is
// one kind's own coverage, and the tool must refuse rather than crown it.
func TestScoreRefusesWhenNoTwoKindsSpanTheWorkload(t *testing.T) {
	const root = "BenchmarkStats"
	in := rows(root+"/atomic/n=8", 8, 5, 100, 0, 0) +
		rows(root+"/striped/n=32", 8, 5, 90, 0, 0) +
		rows(root+"/perworker/n=8", 8, 5, 80, 0, 0) +
		rows(root+"/perworker/n=32", 8, 5, 80, 0, 0)
	_, err := score(decideOpts{family: "A9", benchRoot: root, ref: "atomic"}, parseAll(t, in), nil)
	if err == nil {
		t.Fatal("a family no two kinds span was decided anyway")
	}
	if !strings.Contains(err.Error(), "covers") {
		t.Errorf("error = %v; it must name what each kind covers", err)
	}
}

// A kind that covers less than the family's workload is reported and dropped
// from candidacy, and the decision is taken over the whole workload the
// remaining candidates share -- not over the narrowest kind's corner.
func TestScoreDecidesOverTheWholeSharedWorkload(t *testing.T) {
	const root = "BenchmarkStats"
	in := rows(root+"/atomic/n=8", 8, 5, 100, 0, 0) +
		rows(root+"/atomic/n=32", 8, 5, 400, 0, 0) +
		rows(root+"/striped/n=8", 8, 5, 110, 0, 0) +
		rows(root+"/striped/n=32", 8, 5, 120, 0, 0) +
		rows(root+"/perworker/n=8", 8, 5, 90, 0, 0)
	sc, err := score(decideOpts{family: "A9", benchRoot: root, ref: "atomic"}, parseAll(t, in), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(sc.Common) != 2 {
		t.Errorf("decided on %d row(s) %v; atomic and striped share two", len(sc.Common), sc.Common)
	}
	if sc.Winner != "striped" {
		t.Errorf("winner = %s (%s); striped wins the shared workload", sc.Winner, sc.Rule)
	}
}

// Defect 3: §0.3(3)'s exclusion cannot fire. No benchmark emits a fairness
// metric, p99 is absent from most families, and B/op went through
// math.Max(..., 1), which reports a zero-allocation kind as a 1-byte one.
// The scorecard must say which of the three guards was evaluated and which
// had no data, so that no ADR claims a check that did not run.
func TestScoreRecordsWhichExclusionGuardsCouldRun(t *testing.T) {
	const root = "BenchmarkMemory/Queue"
	in := rows(root+"/mutexring/MPMC/P=4", 8, 5, 200, 0, 0) +
		rows(root+"/seqmpmc/MPMC/P=4", 8, 5, 100, 0, 0)
	sc, err := score(decideOpts{family: "A1", benchRoot: root, ref: "mutexring"}, parseAll(t, in), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range sc.Kinds {
		if k.BytesMean != 0 {
			t.Errorf("kind %s: footprint = %v B/op; it allocates nothing", k.Kind, k.BytesMean)
		}
	}
	joined := strings.Join(sc.Notes, "\n")
	for _, want := range []string{"fairness", "p99"} {
		if !strings.Contains(joined, want) {
			t.Errorf("nothing records that the %s guard of §0.3(3) had no data: %v", want, sc.Notes)
		}
	}
}

// Defect 4: §9.4 step 2 requires 95% confidence intervals and there are none,
// so §0.3(2)'s "within 3%" is applied to bare medians. These two kinds differ
// by about 4% in the median and overlap almost completely: the difference is
// noise, and the simpler kind must keep the default rather than losing to a
// 4% number no interval supports.
func TestScoreConsultsAConfidenceIntervalNotABareMedian(t *testing.T) {
	dir := pkgWith(t, map[string]string{
		"simple.go": "package q\n\nfunc a() {}\n",
		"fancy.go":  "package q\n\nfunc b() {}\nfunc c() {}\nfunc d() {}\nfunc e() {}\nfunc f() {}\n",
	})
	const root = "BenchmarkReqReply/Store"
	simple := []float64{100, 101, 102, 103, 104, 105, 106, 107, 108, 109}
	fancy := []float64{96, 97, 98, 99, 100, 101, 102, 103, 104, 105}
	in := samplesOf(root+"/simple/hot", 8, simple) + samplesOf(root+"/fancy/hot", 8, fancy)
	sc, err := score(decideOpts{family: "A31", benchRoot: root, ref: "simple", pkg: dir}, parseAll(t, in), nil)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Winner != "simple" {
		t.Errorf("winner = %s (%s); a 4%% median difference these samples cannot separate is not a win", sc.Winner, sc.Rule)
	}
	if !strings.Contains(sc.Rule, "95%") {
		t.Errorf("rule = %q; §9.4 step 2 decides on 95%% confidence intervals", sc.Rule)
	}
	for _, k := range sc.Kinds {
		if k.Kind == "fancy" && (k.RatioLo == 0 || k.RatioHi == 0) {
			t.Errorf("kind fancy carries no interval: %+v", k)
		}
	}
}

// Defect 5: §9.1's per-row decision rule never reaches the decision. Family
// A10's arms are two BUILDS of family A1's kinds, so `bakeoff decide`'s
// winner for it is a fact about A1  and must not be
// presented as A10's decision.
func TestScoreRefusesToDecideACrossBuildFamily(t *testing.T) {
	const root = "BenchmarkMemory/Queue"
	in := rows(root+"/mutexring/MPMC/P=4", 8, 5, 200, 0, 0) +
		rows(root+"/seqmpmc/MPMC/P=4", 8, 5, 100, 0, 0)
	sc, err := score(decideOpts{family: "A10", benchRoot: root, ref: "mutexring"}, parseAll(t, in), nil)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Winner != "" {
		t.Errorf("winner = %q; A10 compares two builds and this run measured one (assumption A24.30)", sc.Winner)
	}
	if !strings.Contains(strings.Join(sc.Notes, "\n"), "A1") {
		t.Errorf("notes do not say what A10's rule needs: %v", sc.Notes)
	}
}

// A row whose criterion is "stdlib unless > 30% gain" must not be decided by
// a 3% rule: a 15% win leaves the incumbent in place.
func TestScoreHonoursARowsOwnMargin(t *testing.T) {
	const root = "BenchmarkCodec/JSON"
	in := rows(root+"/json/small", 8, 5, 100, 0, 0) +
		rows(root+"/jsonv2/small", 8, 5, 85, 0, 0)
	sc, err := score(decideOpts{family: "A20", benchRoot: root, ref: "json"}, parseAll(t, in), nil)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Winner != "json" {
		t.Errorf("winner = %s (%s); §9.1 row A20 says stdlib unless > 30%% gain", sc.Winner, sc.Rule)
	}
}

// A family whose rule the scorer cannot express is refused, never given the
// scorer's own rule instead.
func TestScoreRefusesAFamilyWithNoRule(t *testing.T) {
	const root = "BenchmarkNothing"
	in := rows(root+"/a/x", 8, 5, 100, 0, 0) + rows(root+"/b/x", 8, 5, 90, 0, 0)
	_, err := score(decideOpts{family: "Z99", benchRoot: root, ref: "a"}, parseAll(t, in), nil)
	if err == nil {
		t.Fatal("a family with no §9.1 decision rule was decided anyway")
	}
	if !strings.Contains(err.Error(), "rule") {
		t.Errorf("error = %v; it must say the rule is what is missing", err)
	}
}

// Defect 6a: a sibling benchmark whose name merely EXTENDS the family's root
// is folded into the family's geomean. BenchmarkMiddlewareCompose is the
// once-per-Run composition cost and is not family A30's subject; a third of
// A30's rows come from it.
func TestScoreMatchesTheBenchmarkRootOnSegmentBoundaries(t *testing.T) {
	in := rows("BenchmarkMiddleware/closure/n=1/enabled", 8, 5, 30, 0, 0) +
		rows("BenchmarkMiddleware/fused/n=1/enabled", 8, 5, 25, 0, 0) +
		rows("BenchmarkMiddlewareCompose/closure/n=1", 8, 5, 500, 0, 0) +
		rows("BenchmarkMiddlewareCompose/fused/n=1", 8, 5, 900, 0, 0)
	sc, err := score(decideOpts{family: "A30", benchRoot: "BenchmarkMiddleware", ref: "closure"}, parseAll(t, in), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range sc.Common {
		if strings.Contains(r, "Compose") {
			t.Errorf("row %q is BenchmarkMiddlewareCompose's, not BenchmarkMiddleware's", r)
		}
	}
	if len(sc.Common) != 1 {
		t.Errorf("decided on %d row(s): %v", len(sc.Common), sc.Common)
	}
}

// Defect 6b: `decide` never checks that every kind the family announced
// produced samples, so a kind can vanish from the scorecard, and from the
// ADR, without a word.
func TestScoreRequiresEveryAnnouncedKind(t *testing.T) {
	const root = "BenchmarkMemory/Queue"
	in := rows(root+"/mutexring/MPMC/P=4", 8, 5, 200, 0, 0) +
		rows(root+"/seqmpmc/MPMC/P=4", 8, 5, 100, 0, 0)
	idents := map[string]string{"mutexring": "MutexRing", "seqmpmc": "SeqMPMC", "chunked": "Chunked"}
	_, err := score(decideOpts{family: "A1", benchRoot: root, ref: "mutexring"}, parseAll(t, in), idents)
	if err == nil {
		t.Fatal("a kind the family announced produced no samples and the scorecard said nothing")
	}
	if !strings.Contains(err.Error(), "chunked") {
		t.Errorf("error = %v; it must name the missing kind", err)
	}
}

// Defect 6c: the `quick` profile wrote the canonical scorecard, the published
// variant page and the generated auto.go from three 200 ms samples under a
// load ceiling of 64. Assumption A24.33 says only `full` decides.
func TestDecideRefusesCanonicalArtefactsFromANonDecidingProfile(t *testing.T) {
	dir := t.TempDir()
	pkg := filepath.Join(dir, "queue")
	run := filepath.Join(dir, "results", "20260910T000000Z-A1")
	cards := filepath.Join(dir, "scorecards")
	reports := filepath.Join(dir, "reports")
	for _, d := range []string{pkg, run} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	mustWrite(t, filepath.Join(pkg, "queue.go"), "package queue\n")
	mustWrite(t, filepath.Join(run, "raw.txt"),
		rows("BenchmarkMemory/Queue/mutexring/MPMC/P=4", 8, 5, 200, 0, 0)+
			rows("BenchmarkMemory/Queue/seqmpmc/MPMC/P=4", 8, 5, 100, 0, 0))
	err := decide([]string{
		"-family", "A1", "-results", filepath.Join(run, "raw.txt"), "-pkg", pkg,
		"-bench", "BenchmarkMemory/Queue", "-ref", "mutexring", "-auto-key", "queue",
		"-idents", "mutexring=MutexRing,seqmpmc=SeqMPMC", "-adr", "14",
		"-raw", run, "-scorecards", cards, "-out-report", reports,
		"-profile", "quick", "-auto-file", filepath.Join(pkg, "auto.go"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{
		filepath.Join(cards, "A1.json"),
		filepath.Join(reports, "A1.md"),
		filepath.Join(pkg, "auto.go"),
	} {
		if _, err := os.Stat(p); err == nil {
			t.Errorf("%s was written from a `quick` profile (assumption A24.33)", p)
		}
	}
	// The run's own copy is still written, and it says what it was measured
	// under and that it decides nothing.
	data, err := os.ReadFile(filepath.Join(run, "scorecard.json"))
	if err != nil {
		t.Fatal(err)
	}
	var sc scorecard
	if err := json.Unmarshal(data, &sc); err != nil {
		t.Fatal(err)
	}
	if sc.Deciding {
		t.Errorf("a quick-profile scorecard claims to decide: %+v", sc)
	}
}

// sprintfLine renders one ns/op sample carrying a p99-ns metric.
func sprintfLine(name string, procs int, ns, p99 float64) string {
	return fmt.Sprintf("%s-%d\t1000\t%.1f ns/op\t0 B/op\t0 allocs/op\t%.1f p99-ns\n", name, procs, ns, p99)
}

// cpuLine renders one sample of the idle-CPU row, whose metric is a
// percentage of a processor and not a time.
func cpuLine(name string, procs int, pct float64) string {
	return fmt.Sprintf("%s-%d\t1000\t1.0 ns/op\t0 B/op\t0 allocs/op\t%.2f cpu-pct\n", name, procs, pct)
}

// A guard is only "evaluated" where the comparison it makes could actually
// run. §0.3(3) compares a candidate against the reference, so a metric only
// the candidates report cannot fire -- and a run made without -benchmem has
// no footprint to compare at all. Counting "any candidate has it" would have
// made this record's own claim as vacuous as the guard it describes.
func TestGuardStatusNeedsTheReferenceToReportTheMetric(t *testing.T) {
	const root = "BenchmarkMemory/Queue"
	// The reference reports no percentile; one candidate does.
	in := rows(root+"/mutexring/MPMC/P=4", 8, 5, 200, 0, 0) +
		sprintfLine(root+"/seqmpmc/MPMC/P=4", 8, 100, 400) +
		sprintfLine(root+"/seqmpmc/MPMC/P=4", 8, 101, 401)
	sc, err := score(decideOpts{family: "A1", benchRoot: root, ref: "mutexring"}, parseAll(t, in), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range sc.Guards {
		if g.Metric == "p99 latency" {
			if g.Status != guardNoData || !strings.Contains(g.Detail, "mutexring") {
				t.Errorf("p99 guard = %+v; the reference reports no p99-ns, so the comparison cannot run", g)
			}
		}
	}
	// A run without -benchmem has no footprint at all, which is not the same
	// fact as every kind reporting zero.
	bare := "BenchmarkMemory/Queue/mutexring/MPMC/P=4-8\t100\t200 ns/op\n" +
		"BenchmarkMemory/Queue/seqmpmc/MPMC/P=4-8\t100\t100 ns/op\n"
	sc, err = score(decideOpts{family: "A1", benchRoot: root, ref: "mutexring"}, parseAll(t, bare), nil)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, g := range sc.Guards {
		if g.Metric == "memory footprint" {
			found = true
			if g.Status != guardNoData || !strings.Contains(g.Detail, "-benchmem") {
				t.Errorf("footprint guard = %+v", g)
			}
		}
	}
	if !found {
		t.Error("no footprint guard recorded")
	}
}

// An arm can be measured, scored and reported, and still be barred from being
// the default. Family A2's `logcursor` is the case: it shares one log per
// partition, so `Engine.CreateTopic` refuses the stream profile, wildcards,
// priority and per-subscription capacity , and binding it
// would take those away from `New(Config{})`. §0.3 puts the correctness gate
// before the numbers, so it is passed over with its numbers stated -- not
// quietly dropped, and not crowned because it is fastest.
func TestScoreReportsAnArmThatCannotBeTheDefault(t *testing.T) {
	const root = "BenchmarkMemory/FanOut"
	// logcursor is fastest on every row and must still lose.
	in := rows(root+"/arch=persubenvelopes/subs=16", 8, 5, 200, 0, 0) +
		rows(root+"/arch=logcursor/subs=16", 8, 5, 50, 0, 0) +
		rows(root+"/arch=fullcopy/subs=16", 8, 5, 400, 0, 0) +
		rows(root+"/arch=chanfanout/subs=16", 8, 5, 300, 0, 0)
	sc, err := score(decideOpts{family: "A2", benchRoot: root, ref: "persubenvelopes", kindDim: "arch"}, parseAll(t, in), nil)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Winner == "logcursor" {
		t.Errorf("winner = logcursor: an arm the row bars from being the default was crowned for being fastest (rule %q)", sc.Rule)
	}
	if sc.Winner != "persubenvelopes" {
		t.Errorf("winner = %s; want persubenvelopes, the only architecture §8.4 and assumption A15.4 leave eligible", sc.Winner)
	}
	var seen string
	for _, k := range sc.Kinds {
		if k.Kind == "logcursor" {
			seen = k.Disposition
		}
	}
	switch {
	case seen == "":
		t.Error("logcursor is absent from the scorecard: an arm that cannot be the default is still measured and still reported")
	case !strings.Contains(seen, "never the default"):
		t.Errorf("logcursor disposition = %q; it must say it cannot be the default", seen)
	case !strings.Contains(seen, "A15.4"):
		t.Errorf("logcursor disposition = %q; it must carry the reason, not just the verdict", seen)
	}
}

// One eligible arm is a decision when the others were excluded by the row's
// own declaration rather than by what they can run -- and the rule string must
// not then claim a contest. The ADR prints it verbatim.
func TestScoreDecidesASoleEligibleArm(t *testing.T) {
	const root = "BenchmarkMemory/FanOut"
	in := rows(root+"/arch=persubenvelopes/subs=16", 8, 5, 200, 0, 0) +
		rows(root+"/arch=logcursor/subs=16", 8, 5, 50, 0, 0) +
		rows(root+"/arch=fullcopy/subs=16", 8, 5, 400, 0, 0) +
		rows(root+"/arch=chanfanout/subs=16", 8, 5, 300, 0, 0)
	sc, err := score(decideOpts{family: "A2", benchRoot: root, ref: "persubenvelopes", kindDim: "arch"}, parseAll(t, in), nil)
	if err != nil {
		t.Fatalf("a family with one eligible arm was refused: %v", err)
	}
	if strings.Contains(sc.Rule, "geomean") {
		t.Errorf("rule = %q; there was one candidate, so naming a geomean contest is a claim the data does not support", sc.Rule)
	}
	if !strings.Contains(sc.Rule, "eligible") {
		t.Errorf("rule = %q; it must say the decision came from the row's own declaration", sc.Rule)
	}
}
