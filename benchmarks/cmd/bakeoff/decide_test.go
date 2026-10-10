package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/benchmarks/internal/benchfmt"
)

// fixture renders 5 samples of two benchmarks for one kind.
func fixture(kind string, ns, bytes, allocs, p99 float64) string {
	var b strings.Builder
	for _, bench := range []string{"MPMC/P=1", "MPMC/P=8"} {
		for i := range 5 {
			fmt.Fprintf(&b, "BenchmarkMemory/Queue/%s/%s-8\t1000\t%.1f ns/op\t%.0f B/op\t%.0f allocs/op\t%.0f p99-ns\n",
				kind, bench, ns+float64(i), bytes, allocs, p99)
		}
	}
	return b.String()
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func parseAll(t *testing.T, text string) []benchfmt.Result {
	t.Helper()
	rs, err := benchfmt.ParseBytes([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	return rs
}

func TestScoreFastestWins(t *testing.T) {
	rs := parseAll(t, fixture("mutexring", 100, 64, 0, 200)+fixture("mpmc", 50, 64, 0, 100)+fixture("chan", 80, 96, 1, 150))
	sc, err := score(decideOpts{family: "A1", benchRoot: "BenchmarkMemory/Queue", ref: "mutexring"}, rs, map[string]string{"mpmc": "SeqMPMC"})
	if err != nil {
		t.Fatal(err)
	}
	if sc.Winner != "mpmc" || !strings.Contains(sc.Rule, "fastest geomean") {
		t.Errorf("winner = %s (%s)", sc.Winner, sc.Rule)
	}
	if len(sc.Common) != 2 {
		t.Errorf("common = %v", sc.Common)
	}
	for _, k := range sc.Kinds {
		switch k.Kind {
		case "mpmc":
			if k.Disposition != "default" || k.Ident != "SeqMPMC" || k.RatioVsRef >= 1 {
				t.Errorf("mpmc = %+v", k)
			}
		case "mutexring":
			if k.Disposition != "reference (Safe)" || k.RatioVsRef != 1 {
				t.Errorf("mutexring = %+v", k)
			}
		case "chan":
			if k.AllocsMax != 1 || !strings.HasPrefix(k.Disposition, "profile: retained") {
				t.Errorf("chan = %+v", k)
			}
		}
	}
}

func TestScoreDoesNotSubstituteUnavailableRevision(t *testing.T) {
	if gitCommit(".") == "" {
		t.Skip("requires a Git checkout to exercise the current-revision fallback")
	}
	run := t.TempDir()
	mustWrite(t, filepath.Join(run, "sysinfo.json"), `{"git_commit_status":"unavailable","git_dirty":true}`)
	rs := parseAll(t, fixture("mutexring", 100, 64, 0, 200)+fixture("mpmc", 50, 64, 0, 100))
	sc, err := score(decideOpts{
		family: "A1", benchRoot: "BenchmarkMemory/Queue", ref: "mutexring", pkg: ".", rawPath: run,
	}, rs, map[string]string{"mpmc": "SeqMPMC"})
	if err != nil {
		t.Fatal(err)
	}
	if sc.Commit != "" || sc.CommitStatus != "unavailable" || !sc.CommitDirty {
		t.Fatalf("recorded revision changed: commit=%q status=%q dirty=%v", sc.Commit, sc.CommitStatus, sc.CommitDirty)
	}
}

func TestScoreTieBreaksOnComplexity(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "simple.go"), "package q\n\nfunc a() {}\n")
	mustWrite(t, filepath.Join(dir, "fancy.go"), "package q\n\nimport \"unsafe\"\n\nvar _ unsafe.Pointer\n\nfunc b() {}\nfunc c() {}\nfunc d() {}\n")
	rs := parseAll(t, fixture("simple", 100, 64, 0, 100)+fixture("fancy", 99, 64, 0, 100))
	sc, err := score(decideOpts{family: "A1", benchRoot: "BenchmarkMemory/Queue", ref: "simple", pkg: dir}, rs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Winner != "simple" || !strings.Contains(sc.Rule, "2 candidate(s) within 3%") {
		t.Errorf("winner = %s (%s)", sc.Winner, sc.Rule)
	}
	for _, k := range sc.Kinds {
		if k.Kind == "fancy" && (!k.Unsafe || k.Complexity < 1000) {
			t.Errorf("fancy complexity = %+v", k)
		}
	}
}

func TestScoreDisqualificationAndP99Rule(t *testing.T) {
	rs := parseAll(t, fixture("ref", 100, 64, 0, 100)+fixture("fast", 50, 64, 0, 300)+fixture("broken", 10, 64, 0, 10))
	sc, err := score(decideOpts{family: "A1", benchRoot: "BenchmarkMemory/Queue", ref: "ref", disqualified: "broken"}, rs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Winner != "ref" {
		t.Errorf("winner = %s (%s) notes=%v", sc.Winner, sc.Rule, sc.Notes)
	}
	var excluded bool
	for _, n := range sc.Notes {
		if strings.Contains(n, "fast excluded from default") && strings.Contains(n, "p99") {
			excluded = true
		}
	}
	if !excluded {
		t.Errorf("notes = %v", sc.Notes)
	}
	for _, k := range sc.Kinds {
		if k.Kind == "broken" && (!k.Disqualified || k.Disposition != "disqualified") {
			t.Errorf("broken = %+v", k)
		}
	}
	if _, err := score(decideOpts{family: "A1", benchRoot: "BenchmarkMemory/Queue", disqualified: "ref,fast,broken"}, rs, nil); err == nil {
		t.Error("all-disqualified did not error")
	}
	if _, err := score(decideOpts{family: "A1", benchRoot: "BenchmarkNope"}, rs, nil); err == nil {
		t.Error("no results did not error")
	}
}

func TestSplitKind(t *testing.T) {
	k, b, ok := splitKind("BenchmarkMemory/Queue/mpmc/MPMC/P=1", "BenchmarkMemory/Queue", "")
	if !ok || k != "mpmc" || b != "BenchmarkMemory/Queue/MPMC/P=1" {
		t.Errorf("segment kind = %q %q %v", k, b, ok)
	}
	k, b, ok = splitKind("BenchmarkMemory/FanOut/16/arch=logcursor/policy=atomic", "BenchmarkMemory/FanOut", "arch")
	if !ok || k != "logcursor" || b != "BenchmarkMemory/FanOut/16/policy=atomic" {
		t.Errorf("dim kind = %q %q %v", k, b, ok)
	}
	if _, _, ok := splitKind("BenchmarkMemory/Queue", "BenchmarkMemory/Queue", ""); ok {
		t.Error("root-only name accepted")
	}
	if _, _, ok := splitKind("BenchmarkMemory/Queue/x/y", "BenchmarkMemory/Queue", "missing"); ok {
		t.Error("missing dim accepted")
	}
}

func TestRenderAndAuto(t *testing.T) {
	dir := t.TempDir()
	pkg := filepath.Join(dir, "queue")
	if err := os.MkdirAll(pkg, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(pkg, "queue.go"), "package queue\n")
	rs := parseAll(t, fixture("mutexring", 100, 64, 0, 100)+fixture("mpmc", 50, 64, 0, 90))
	o := decideOpts{
		family: "A1", benchRoot: "BenchmarkMemory/Queue", ref: "mutexring", pkg: pkg, adrNum: 14,
		title: "Memory engine defaults", autoKey: "queue", autoFile: filepath.Join(pkg, "auto.go"), sysinfo: "s.json", rawPath: dir,
	}
	sc, err := score(o, rs, map[string]string{"mpmc": "SeqMPMC", "mutexring": "MutexRing"})
	if err != nil {
		t.Fatal(err)
	}
	repPath := filepath.Join(dir, "report.md")
	if err := render(reportTmpl, repPath, sc, o); err != nil {
		t.Fatal(err)
	}
	rep, _ := os.ReadFile(repPath)
	if !strings.Contains(string(rep), "# Bake-off A1") || !strings.Contains(string(rep), "MPMC/P=1") {
		t.Errorf("report = %s", rep)
	}
	if err := writeAuto(o.autoFile, o, sc); err != nil {
		t.Fatal(err)
	}
	auto, _ := os.ReadFile(o.autoFile)
	if !strings.Contains(string(auto), "package queue") || !strings.Contains(string(auto), "const AutoKind = SeqMPMC") || !strings.Contains(string(auto), "AutoProvisional = true") {
		t.Errorf("auto.go = %s", auto)
	}
	if err := writeAuto(o.autoFile, o, &scorecard{Winner: "nope"}); err == nil {
		t.Error("writeAuto without ident succeeded")
	}
	if slugify("Memory engine defaults (A1–A7)") != "memory-engine-defaults-a1-a7" {
		t.Errorf("slugify = %q", slugify("Memory engine defaults (A1–A7)"))
	}
}

// TestDecideWritesTheStableScorecard: an ADR is rendered from the per-family
// scorecard, not from a run directory named after a run, so `decide` must
// write that copy and it must carry the run id, the host and the commit
// the benchmark configuration step 5 requires the ADR to show.
func TestDecideWritesTheStableScorecard(t *testing.T) {
	dir := t.TempDir()
	pkg := filepath.Join(dir, "queue")
	run := filepath.Join(dir, "results", "20260910T000000Z-A1")
	cards := filepath.Join(dir, "scorecards")
	for _, d := range []string{pkg, run} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	mustWrite(t, filepath.Join(pkg, "queue.go"), "package queue\n")
	mustWrite(t, filepath.Join(run, "sysinfo.json"), `{"hostname":"bench-1","os":"linux","arch":"amd64","cpus":64,"kernel":"7.0.14","governor":"performance","go_version":"go1.27.1","git_commit":"abc123def456","git_dirty":true}`)
	mustWrite(t, filepath.Join(run, "raw.txt"), fixture("mutexring", 100, 64, 0, 100)+fixture("mpmc", 50, 64, 0, 90))
	if err := decide([]string{
		"-family", "A1", "-results", filepath.Join(run, "raw.txt"), "-pkg", pkg,
		"-bench", "BenchmarkMemory/Queue", "-ref", "mutexring", "-auto-key", "queue",
		"-idents", "mpmc=SeqMPMC,mutexring=MutexRing", "-adr", "14",
		"-title", "Memory lane queue kinds (A1)", "-raw", run, "-scorecards", cards,
		"-out-report", filepath.Join(dir, "reports"), "-profile", "full",
		"-methodology", "count=10 cpu=1,4,8,32 benchtime=1s",
		"-sysinfo", "benchmarks/reports/results/20260910T000000Z-A1/sysinfo.json",
		"-auto-file", filepath.Join(pkg, "auto.go"),
	}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(cards, "A1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var sc scorecard
	if err := json.Unmarshal(data, &sc); err != nil {
		t.Fatal(err)
	}
	if sc.Winner != "mpmc" || sc.ADR != 14 || sc.AutoKey != "queue" {
		t.Errorf("scorecard = %+v", sc)
	}
	if sc.RunID != "20260910T000000Z-A1" || sc.Commit != "abc123def456" || !sc.CommitDirty {
		t.Errorf("provenance = run %q commit %q dirty %v", sc.RunID, sc.Commit, sc.CommitDirty)
	}
	if sc.Host == nil || sc.Host.Hostname != "bench-1" || sc.Host.CPUs != 64 || sc.Host.Governor != "performance" {
		t.Errorf("host = %+v", sc.Host)
	}
	if sc.Profile != "full" || !sc.Deciding || !strings.Contains(sc.Methodology, "benchtime=1s") {
		t.Errorf("methodology = %q %q", sc.Profile, sc.Methodology)
	}
	// The run directory keeps its own copy, and `decide` renders no ADR: one
	// ADR number is shared by many families and `bakeoff adr` owns the file.
	if _, err := os.Stat(filepath.Join(run, "scorecard.json")); err != nil {
		t.Errorf("run scorecard: %v", err)
	}
	adrs, _ := filepath.Glob(filepath.Join(dir, "**", "ADR-*.md"))
	if len(adrs) != 0 {
		t.Errorf("decide rendered an ADR: %v", adrs)
	}
}

func TestMentions(t *testing.T) {
	names := []string{"BenchmarkMemory/Queue/mpmc/MPMC/P=1-8", "BenchmarkMemory/FanOut/16/arch=logcursor-8", "BenchmarkX/spin-park-8"}
	for slug, want := range map[string]bool{"mpmc": true, "logcursor": true, "spin-park": true, "MPMC": true, "chan": false, "16": true} {
		if got := mentions(names, slug); got != want {
			t.Errorf("mentions(%q) = %v", slug, got)
		}
	}
}

// TestComplexityAttributesAKindThatSharesAFile pins the declaration fallback:
// seven families of the campaign hold more than one kind in a file, and three
// hold every kind in one, so a count that only looks at file names reports
// zero for every arm of those families and the simplicity band then prefers
// whichever of them sorts first.
func TestComplexityAttributesAKindThatSharesAFile(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "kinds.go"), `package q

// Package-level lines belong to nobody and must not be charged to a kind.
import "strings"

var _ = strings.TrimSpace

type spinKind struct{ n int }

func (s *spinKind) wait() {
	for range s.n {
		_ = s
	}
}

type condKind struct{ n int }

func (c *condKind) wait() { _ = c }
`)
	both := []kindSeed{{Slug: "spin"}, {Slug: "cond"}}
	rep := attributeLOC(dir, both)
	loc, attributed, unsafe := rep.Lines["spin"], rep.attributed("spin"), rep.Unsafe["spin"]
	if !attributed || loc == 0 || unsafe {
		t.Errorf("spin = (loc %d, attributed %v, unsafe %v), want an attributed non-zero safe count", loc, attributed, unsafe)
	}
	cond, condAttributed := rep.Lines["cond"], rep.attributed("cond")
	if !condAttributed || cond == 0 {
		t.Errorf("cond = (loc %d, attributed %v), want an attributed non-zero count", cond, condAttributed)
	}
	if cond >= loc {
		t.Errorf("cond loc %d is not smaller than spin's %d, and its implementation is", cond, loc)
	}
	// "secondChance" carries "cond" as a substring and is not part of it.
	mustWrite(t, filepath.Join(dir, "other.go"), "package q\n\nfunc secondChance() { _ = 1 }\n")
	if after := attributeLOC(dir, both).Lines["cond"]; after != cond {
		t.Errorf("cond loc went from %d to %d when an unrelated declaration was added", cond, after)
	}
}

// TestComplexityReportsWhenItCanAttributeNothing pins the distinction the
// whole fix rests on: a kind whose lines are nowhere to be found reports
// "not attributed" rather than a zero that reads as the simplest arm.
func TestComplexityReportsWhenItCanAttributeNothing(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "misc.go"), "package q\n\nfunc helper() { _ = 1 }\n")
	rep := attributeLOC(dir, []kindSeed{{Slug: "absent"}})
	loc, attributed := rep.Lines["absent"], rep.attributed("absent")
	if attributed || loc != 0 {
		t.Errorf("absent = (loc %d, attributed %v), want (0, false)", loc, attributed)
	}
}

// TestScoreDoesNotClaimASimplicityItCannotMeasure is the rule half. With one
// band member's source unattributable, the record must not say the simplest
// arm was preferred -- it must say which rule was applied instead, and the
// faster arm must win rather than the unmeasured one.
func TestScoreDoesNotClaimASimplicityItCannotMeasure(t *testing.T) {
	dir := t.TempDir()
	// `small` has its own file and 3 lines. `hidden` has none, and nothing in
	// the package bears its name: before the fix it scored 0 and beat `small`
	// on being unmeasured.
	mustWrite(t, filepath.Join(dir, "small.go"), "package q\n\nfunc small() { _ = 1 }\n")
	mustWrite(t, filepath.Join(dir, "misc.go"), "package q\n\nfunc helper() { _ = 1 }\n")
	rs := parseAll(t, fixture("small", 100, 64, 0, 100)+fixture("hidden", 101, 64, 0, 100))
	sc, err := score(decideOpts{family: "A1", benchRoot: "BenchmarkMemory/Queue", ref: "small", pkg: dir}, rs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sc.Rule, "simplest") {
		t.Errorf("rule claims a simplicity tie-break: %q", sc.Rule)
	}
	if !strings.Contains(sc.Rule, "no source line attributable") {
		t.Errorf("rule does not say why simplicity was not applied: %q", sc.Rule)
	}
	if sc.Winner != "small" {
		t.Errorf("winner = %s, want small: the unattributed arm is slower and must not win on being unmeasured", sc.Winner)
	}
	for _, k := range sc.Kinds {
		switch k.Kind {
		case "hidden":
			if k.LOCAttributed || k.LOCText() != "n/a" {
				t.Errorf("hidden = %+v, LOCText %q", k, k.LOCText())
			}
		case "small":
			if !k.LOCAttributed || k.LOCText() == "n/a" {
				t.Errorf("small = %+v, LOCText %q", k, k.LOCText())
			}
		}
	}
}

// TestScoreOrdersUnsafeLastWithoutEveryLineCount pins that dropping the
// simplicity rule does not drop §0.3(2)'s other half: use of unsafe is read
// from a file's imports and build tag, not from a line count, so an unsafe arm
// still sorts behind a safe one in a band no line count could order.
func TestScoreOrdersUnsafeLastWithoutEveryLineCount(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "fancy.go"), "package q\n\nimport \"unsafe\"\n\nvar _ unsafe.Pointer\n\nfunc fancy() { _ = 1 }\n")
	mustWrite(t, filepath.Join(dir, "misc.go"), "package q\n\nfunc helper() { _ = 1 }\n")
	// `fancy` is the fastest arm of the three and uses unsafe; `hidden` has no
	// attributable source; `plain` is the reference.
	rs := parseAll(t, fixture("plain", 101, 64, 0, 100)+fixture("fancy", 100, 64, 0, 100)+fixture("hidden", 102, 64, 0, 100))
	sc, err := score(decideOpts{family: "A1", benchRoot: "BenchmarkMemory/Queue", ref: "plain", pkg: dir}, rs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Winner == "fancy" {
		t.Errorf("winner = fancy: an unsafe arm must not be preferred inside the band (%s)", sc.Rule)
	}
	for _, k := range sc.Kinds {
		if k.Kind == "fancy" && !k.Unsafe {
			t.Errorf("fancy = %+v, want Unsafe", k)
		}
	}
}

// TestReadResultsFindsTheCompressedRun pins that a re-decide can run on the
// evidence the repository actually holds: `raw.txt` is gitignored and
// `raw.txt.gz` is what a run commits, so `-results .../raw.txt` has to resolve
// to the compressed file when the plain one is absent.
func TestReadResultsFindsTheCompressedRun(t *testing.T) {
	dir := t.TempDir()
	text := fixture("mutexring", 100, 64, 0, 100) + fixture("mpmc", 50, 64, 0, 90)
	plain := filepath.Join(dir, "raw.txt")
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write([]byte(text)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, plain+".gz", buf.String())

	want := len(parseAll(t, text))
	for _, path := range []string{plain, plain + ".gz"} {
		rs, err := readResults(path)
		if err != nil {
			t.Fatalf("readResults(%q): %v", path, err)
		}
		if len(rs) != want {
			t.Errorf("readResults(%q) = %d result(s), want %d", path, len(rs), want)
		}
	}
	if _, err := readResults(filepath.Join(dir, "absent.txt")); err == nil {
		t.Error("a results file that is neither present nor compressed must be an error")
	}
}

// TestScoreDoesNotClaimASimplicityItOnlyPARTLYMeasured is assumption A26.14's
// defect one level along.
//
// A26.14 stopped a kind whose lines nobody could attribute from arriving in
// §0.3(2)'s simplicity band as a zero and winning on being unmeasured. Its
// guard is `LOCAttributed`, which answers "were any lines found". The rule
// needs "were THIS KIND's lines found", and `complexityByDecl` finding nine
// lines of a fifty-seven-line implementation satisfies the flag and defeats
// the guard exactly as a zero did.
//
// Family B9 is the live case: `x/apibench/b9` holds five kinds in one package
// and its `seqSession` and `iterSession` types carry neither the slug
// `seq2`/`iternext` nor the ident `Seq2`/`IterNext`, so each was charged nine
// lines against `value`'s 64 -- and a whole-tree re-decide moved a frozen §9.2
// API shape from `value` to `seq2` on the strength of two misnamed types. The
// fixture below is that arrangement in miniature.
func TestScoreDoesNotClaimASimplicityItOnlyPARTLYMeasured(t *testing.T) {
	dir := t.TempDir()
	// `small` is fully attributed at 3 lines. `partial` has ONE line bearing
	// its name and a large implementation the matcher cannot see, in a
	// declaration named after neither kind -- which is the shape of
	// seqSession. Both report LOCAttributed, so A26.14's guard passes and
	// only the remainder can tell that the comparison is not one.
	mustWrite(t, filepath.Join(dir, "small.go"), "package q\n\nfunc small() { _ = 1 }\n")
	var body strings.Builder
	body.WriteString("package q\n\nfunc partialTag() { _ = 1 }\n\nfunc shared() {\n")
	for range 60 {
		body.WriteString("\t_ = 1\n")
	}
	body.WriteString("}\n")
	mustWrite(t, filepath.Join(dir, "misc.go"), body.String())

	rs := parseAll(t, fixture("small", 101, 64, 0, 100)+fixture("partial", 100, 64, 0, 100))
	sc, err := score(decideOpts{family: "A1", benchRoot: "BenchmarkMemory/Queue", ref: "small", pkg: dir}, rs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sc.LOCUnattributed == 0 {
		t.Fatalf("the scorecard records no unattributed lines; loc_total=%d", sc.LOCTotal)
	}
	if strings.Contains(sc.Rule, "simplest") {
		t.Errorf("rule claims a simplicity tie-break over a partly attributed package: %q", sc.Rule)
	}
	if !strings.Contains(sc.Rule, "attributable to no kind") {
		t.Errorf("rule does not say why simplicity was not applied: %q", sc.Rule)
	}
	// And the guard must not fire when the remainder CANNOT reorder the band:
	// a package whose every line belongs to a kind still gets §0.3(2).
	clean := t.TempDir()
	mustWrite(t, filepath.Join(clean, "tiny.go"), "package q\n\nfunc tiny() { _ = 1 }\n")
	var big strings.Builder
	big.WriteString("package q\n\nfunc bulky() {\n")
	for range 40 {
		big.WriteString("\t_ = 1\n")
	}
	big.WriteString("}\n")
	mustWrite(t, filepath.Join(clean, "bulky.go"), big.String())
	rs2 := parseAll(t, fixture("tiny", 101, 64, 0, 100)+fixture("bulky", 100, 64, 0, 100))
	sc2, err := score(decideOpts{family: "A1", benchRoot: "BenchmarkMemory/Queue", ref: "tiny", pkg: clean}, rs2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sc2.LOCUnattributed != 0 {
		t.Errorf("a package whose every line is attributed reports %d unattributed", sc2.LOCUnattributed)
	}
	if !strings.Contains(sc2.Rule, "simplest") {
		t.Errorf("simplicity was dropped on a fully attributed package: %q", sc2.Rule)
	}
	if sc2.Winner != "tiny" {
		t.Errorf("winner = %s, want tiny: the simplest arm of a tied band", sc2.Winner)
	}
}

// TestLOCFollowsTheDispatchAndNotOnlyTheName is C01.7's remaining half: the
// attribution itself.
//
// Assumption C01.7 made the REMAINDER visible and stopped §0.3(2)'s
// simplicity band being ordered on a count that was only partly a count of
// the kind's own source. It deliberately did not repair the count, and said
// why: loosening the name matcher is how one kind starts stealing another's
// declarations. This is the repair it named instead -- attribute by what the
// family's own dispatch reaches, so that a kind whose implementation is named
// after the CONCEPT (`seqSession`) rather than after the SLUG (`seq2`) is
// charged its own lines.
//
// The fixture is family B9 in miniature and the numbers are the real ones'
// shape: `value` is named after its kind throughout, `seq2` is not named after
// it anywhere except in the Kind vocabulary, and `seq2`'s implementation is
// the LARGER of the two. A name-only attribution charges `seq2` the lines of
// the const block that merely mentions it -- four -- and `value` sixty, so the
// larger implementation reads as the simpler arm, which is the defect.
func TestLOCFollowsTheDispatchAndNotOnlyTheName(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "kinds.go"), `package q

type Kind uint8

const (
	Value Kind = iota
	Seq2
	kindCount
)

func New(k Kind) any {
	switch k {
	case Value:
		return newValueSession()
	case Seq2:
		return newSeqSession()
	}
	return nil
}
`)
	var body strings.Builder
	body.WriteString("package q\n\ntype valueSession struct{ n int }\n\nfunc newValueSession() any { return &valueSession{} }\n\nfunc (s *valueSession) next() int {\n")
	for range 8 {
		body.WriteString("\t_ = s.n\n")
	}
	body.WriteString("\treturn s.n\n}\n\ntype seqSession struct{ n int }\n\nfunc newSeqSession() any { return &seqSession{} }\n\nfunc (s *seqSession) seq() int {\n")
	for range 40 {
		body.WriteString("\t_ = s.n\n")
	}
	body.WriteString("\treturn s.n\n}\n")
	mustWrite(t, filepath.Join(dir, "shapes.go"), body.String())

	rep := attributeLOC(dir, []kindSeed{{Slug: "value", Ident: "Value"}, {Slug: "seq2", Ident: "Seq2"}})
	value, seq2 := rep.Lines["value"], rep.Lines["seq2"]
	if !rep.attributed("value") || !rep.attributed("seq2") {
		t.Fatalf("value = (%d, %v), seq2 = (%d, %v); both implementations are in the package",
			value, rep.attributed("value"), seq2, rep.attributed("seq2"))
	}
	if seq2 <= value {
		t.Errorf("seq2 charged %d lines and value %d; seq2's implementation is the larger of the two, so a simplicity band ordered on these counts prefers the wrong arm",
			seq2, value)
	}
	// The Kind vocabulary names every kind, so charging it to each of them
	// counts the same lines twice and inflates both arms equally -- which is
	// the mechanism that gave `seq2` and `iternext` nine lines apiece of
	// x/apibench/b9's const block.
	if sum := value + seq2; sum > rep.Total {
		t.Errorf("the per-kind counts sum to %d over a package of %d attributable lines, so some lines are charged to more than one kind",
			sum, rep.Total)
	}
}
