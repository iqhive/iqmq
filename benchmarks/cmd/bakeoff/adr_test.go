package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/testguard"
)

// adrTree is a throwaway repository shape: the env files that hold the
// family -> ADR map, the ADR registry, the per-family scorecards and the ADR
// directory.
type adrTree struct {
	root, envs, scorecards, adr, registry string
}

func newADRTree(t *testing.T) *adrTree {
	t.Helper()
	root := t.TempDir()
	tr := &adrTree{
		root:       root,
		envs:       filepath.Join(root, "envs"),
		scorecards: filepath.Join(root, "scorecards"),
		adr:        filepath.Join(root, "adr"),
		registry:   filepath.Join(root, "adrs.tsv"),
	}
	for _, d := range []string{tr.envs, tr.scorecards, tr.adr} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return tr
}

// env writes one family's env file. autoFile "-" is a family that binds no
// Auto constant and must therefore claim no auto: key.
func (tr *adrTree) env(t *testing.T, family string, adr int, autoKey, autoFile, title string) {
	t.Helper()
	var b strings.Builder
	b.WriteString("# Family " + family + "\n")
	b.WriteString("PKG=./internal/" + strings.ToLower(family) + "\n")
	b.WriteString("BENCH=Benchmark" + family + "\n")
	b.WriteString("REF=ref\n")
	b.WriteString("AUTO_KEY=" + autoKey + "\n")
	if autoFile != "" {
		b.WriteString("AUTO_FILE=" + autoFile + "\n")
	}
	b.WriteString("ADR=" + itoa(adr) + "\n")
	b.WriteString("TITLE=\"" + title + "\"\n")
	mustWrite(t, filepath.Join(tr.envs, family+".env"), b.String())
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var d []byte
	for ; n > 0; n /= 10 {
		d = append([]byte{byte('0' + n%10)}, d...)
	}
	return string(d)
}

// scorecard writes a per-family scorecard with one winning kind and one
// disqualified kind, exactly as `bakeoff decide` would.
func (tr *adrTree) scorecard(t *testing.T, family, winner, autoKey, autoFile string) {
	t.Helper()
	sc := scorecard{
		Family: family, Package: "./internal/" + strings.ToLower(family),
		BenchRoot: "Benchmark" + family, Reference: "ref", Winner: winner,
		Rule: "fastest geomean", Provisional: true, AutoKey: autoKey, AutoFile: autoFile,
		Title: "title of " + family, Sysinfo: "benchmarks/reports/results/run-" + family + "/sysinfo.json",
		RawResults: "benchmarks/reports/results/run-" + family + "/raw.txt.gz",
		RunID:      "run-" + family, Profile: "full", Methodology: "count=10 cpu=1,8 benchtime=1s",
		Commit:    "deadbeefcafe",
		Host:      &hostInfo{Hostname: "bench-1", OS: "linux", Arch: "amd64", CPUs: 64, GoVersion: "go1.27.1"},
		DecidedAt: "2026-09-1" + itoa(len(family)%9) + "T10:00:00Z",
		Common:    []string{"Benchmark" + family + "/case"},
		Kinds: []kindScore{
			{Kind: "ref", GeomeanNs: 100, RatioVsRef: 1, RatioVsBest: 2, Disposition: "reference (Safe)"},
			{Kind: winner, GeomeanNs: 50, RatioVsRef: 0.5, RatioVsBest: 1, Disposition: "default"},
			{Kind: "broken", GeomeanNs: 10, Disqualified: true, Reason: "failed the correctness gate", Disposition: "disqualified"},
		},
	}
	data, err := json.MarshalIndent(sc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(tr.scorecards, family+".json"), string(data)+"\n")
}

func (tr *adrTree) registryRows(t *testing.T, rows ...string) {
	t.Helper()
	mustWrite(t, tr.registry, "# number\tslug\textra-variants\ttitle\n"+strings.Join(rows, "\n")+"\n")
}

func (tr *adrTree) run(t *testing.T, extra ...string) {
	t.Helper()
	// -root is the tree the staleness cross-check reads. A throwaway
	// directory is not a git worktree, so the check reports "not verified"
	// rather than refusing every synthetic commit; the tests that exercise
	// the check build a real repository and pass their own -root.
	args := append([]string{
		"-envs", tr.envs, "-registry", tr.registry,
		"-scorecards", tr.scorecards, "-out-adr", tr.adr, "-root", tr.root,
	}, extra...)
	if err := adrCmd(args); err != nil {
		t.Fatalf("bakeoff adr: %v", err)
	}
}

func (tr *adrTree) read(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(tr.adr, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestADRAggregatesEveryFamilyOfOneNumber is the defect this command exists
// for: `decide` wrote an ADR per family, so the last family of a shared
// number overwrote the rest. One render must produce ONE file listing every
// family in `variants:` and one `auto:` key per family that binds.
func TestADRAggregatesEveryFamilyOfOneNumber(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "A1", 14, "queue", "", "Memory lane queue kinds (A1)")
	tr.env(t, "A2", 14, "arch", "./internal/engine/auto_arch.go", "Fan-out storage architecture (A2)")
	tr.env(t, "A26", 14, "dispatch", "-", "Variant dispatch mechanism (A26)")
	tr.env(t, "A14", 19, "offset", "", "Kafka offset tracker (A14)")
	tr.registryRows(t,
		"14\tmemory-engine-internals\tA10\tMemory engine internals (A1, A2, A26, A10)",
		"19\tkafka-offset-tracker\t-\tKafka offset tracker (A14)")
	tr.scorecard(t, "A1", "mpmc", "queue", "./internal/queue/auto.go")
	tr.scorecard(t, "A2", "persubenvelopes", "arch", "./internal/engine/auto_arch.go")
	tr.scorecard(t, "A26", "interface", "dispatch", "-")
	// A14 is deliberately unmeasured: ADR-019 must be skipped, not created
	// half empty.
	tr.run(t)

	if _, err := os.Stat(filepath.Join(tr.adr, "ADR-019-kafka-offset-tracker.md")); !os.IsNotExist(err) {
		t.Errorf("ADR-019 was created without a scorecard: %v", err)
	}
	doc := tr.read(t, "ADR-014-memory-engine-internals.md")
	// A10 has no env file of its own; the registry's extra-variants column
	// puts it in the row list all the same.
	for _, want := range []string{
		"status: Accepted (provisional)",
		"kind: measured",
		"date: 2026-09-13",
		"variants: [A1, A2, A10, A26]",
		"authoritative: no",
		"auto:\n  arch: persubenvelopes\n  queue: mpmc\n",
		"# ADR-014: Memory engine internals (A1, A2, A26, A10)",
		"### A1 -- Memory lane queue kinds (A1)",
		"### A2 -- Fan-out storage architecture (A2)",
		"### A26 -- Variant dispatch mechanism (A26)",
		"**A10**", // listed as not yet measured
		"failed the correctness gate",
		"`deadbeefcafe`",
		"count=10 cpu=1,8 benchtime=1s",
		"bench-1, linux/amd64, 64 CPUs, go1.27.1",
		beginMarker,
		endMarker,
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("ADR-014 lacks %q\n---\n%s", want, doc)
		}
	}
	// A26 binds nothing (AUTO_FILE=-), so it must not claim an auto: key even
	// though its env file names one.
	front, _, err := splitFront(doc)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(front, "dispatch:") {
		t.Errorf("a family with AUTO_FILE=- claimed an auto: key\n%s", front)
	}
	// The block form must be the one internal/testguard reads, because
	// TestAutoBindingMatchesADR in every family package depends on it.
	for family, want := range map[string]string{"queue": "mpmc", "arch": "persubenvelopes"} {
		if got, ok := testguard.ParseADRAutoBinding(doc, family); !ok || got != want {
			t.Errorf("ParseADRAutoBinding(%q) = %q, %v; want %q", family, got, ok, want)
		}
	}
	if _, ok := testguard.ParseADRAutoBinding(doc, "dispatch"); ok {
		t.Error("ParseADRAutoBinding found a binding for the non-binding family")
	}
}

// handWritten is a hand-written ADR of the shape adr/ADR-009 and ADR-011
// have: front matter with variants already listed, prose, and no room for a
// second file claiming the number.
const handWritten = `---
status: Accepted
kind: policy
date: 2026-09-05
variants: [C5, C6]
---

# ADR-011: Failure policy

## Context

Prose a human wrote.

## Decision

More prose.

## Consequences

Closing prose.
`

// TestADRAttachesToExistingFile: the material goes INTO the file that already
// claims the number, between markers, and never into a second file.
func TestADRAttachesToExistingFile(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "B10", 11, "b10", "", "Error construction (B10)")
	tr.registryRows(t, "11\tfailure-policy\t-\tFailure policy")
	tr.scorecard(t, "B10", "prealloccodes", "b10", "./x/apibench/b10/auto.go")
	mustWrite(t, filepath.Join(tr.adr, "ADR-011-failure-policy.md"), handWritten)
	tr.run(t)

	files, _ := filepath.Glob(filepath.Join(tr.adr, "ADR-011-*.md"))
	if len(files) != 1 {
		t.Fatalf("ADR-011 is claimed by %d files: %v", len(files), files)
	}
	doc := tr.read(t, "ADR-011-failure-policy.md")
	for _, want := range []string{
		"status: Accepted", // untouched: a human's curatorial fields stay
		"kind: policy",     // untouched
		"date: 2026-09-05", // untouched
		"variants: [B10, C5, C6]",
		"authoritative: no",
		"auto:\n  b10: prealloccodes\n",
		"# ADR-011: Failure policy",
		"Prose a human wrote.",
		"Closing prose.",
		"## Bake-off data (B10)",
		"### Context", // nested one level under the wrapper
		"#### B10 -- Error construction (B10)",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("attached ADR lacks %q\n---\n%s", want, doc)
		}
	}
	// The generated block must not duplicate the headings the human file
	// already has at the top level.
	for _, heading := range []string{"\n## Context", "\n## Decision", "\n## Consequences"} {
		if n := strings.Count(doc, heading); n != 1 {
			t.Errorf("%q appears %d times; the generated block duplicated a human heading", heading, n)
		}
	}
	// The human prose must precede the generated section, and the section
	// must be the tail of the file.
	if strings.Index(doc, "Closing prose.") > strings.Index(doc, beginMarker) {
		t.Error("the generated section was spliced above the prose")
	}
	if !strings.HasSuffix(doc, endMarker+"\n") {
		t.Errorf("file does not end with the end marker:\n%q", doc[max(0, len(doc)-120):])
	}
}

// TestADRRenderIsIdempotent: re-rendering must replace only the generated
// part, so two renders in a row produce the same bytes -- for a file the
// command created and for one it attached to.
func TestADRRenderIsIdempotent(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "A31", 20, "corr", "", "Correlation storage (A31)")
	tr.env(t, "B10", 11, "b10", "", "Error construction (B10)")
	tr.registryRows(t,
		"11\tfailure-policy\t-\tFailure policy",
		"20\trequest-reply-correlation-storage\t-\tRequest/reply correlation storage (A31)")
	tr.scorecard(t, "A31", "shardedmap", "corr", "./internal/corr/auto.go")
	tr.scorecard(t, "B10", "prealloccodes", "b10", "./x/apibench/b10/auto.go")
	mustWrite(t, filepath.Join(tr.adr, "ADR-011-failure-policy.md"), handWritten)

	tr.run(t)
	first := map[string]string{}
	for _, name := range []string{"ADR-011-failure-policy.md", "ADR-020-request-reply-correlation-storage.md"} {
		first[name] = tr.read(t, name)
	}
	tr.run(t)
	for name, want := range first {
		if got := tr.read(t, name); got != want {
			t.Errorf("%s changed on the second render:\nfirst:\n%s\nsecond:\n%s", name, want, got)
		}
	}
	// A human edit outside the markers survives a re-render; an edit inside
	// them does not, which is what the markers promise.
	path := filepath.Join(tr.adr, "ADR-020-request-reply-correlation-storage.md")
	doc := first["ADR-020-request-reply-correlation-storage.md"]
	mustWrite(t, path, doc+"\n## Review\n\nA human added this after the block.\n")
	tr.run(t)
	after := tr.read(t, "ADR-020-request-reply-correlation-storage.md")
	if !strings.Contains(after, "A human added this after the block.") {
		t.Errorf("a re-render dropped prose written after the end marker:\n%s", after)
	}
	if strings.Count(after, beginMarker) != 1 || strings.Count(after, endMarker) != 1 {
		t.Errorf("a re-render duplicated the markers:\n%s", after)
	}
	tr.run(t)
	if again := tr.read(t, "ADR-020-request-reply-correlation-storage.md"); again != after {
		t.Errorf("the render with human prose is not idempotent:\n%s\n---\n%s", after, again)
	}
}

// TestADRRefusesTwoFilesForOneNumber: the failure mode that made this command
// necessary must be reported, never resolved by guessing.
func TestADRRefusesTwoFilesForOneNumber(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "A31", 20, "corr", "", "Correlation storage (A31)")
	tr.registryRows(t, "20\trequest-reply-correlation-storage\t-\tCorrelation storage")
	tr.scorecard(t, "A31", "shardedmap", "corr", "./internal/corr/auto.go")
	mustWrite(t, filepath.Join(tr.adr, "ADR-020-one.md"), handWritten)
	mustWrite(t, filepath.Join(tr.adr, "ADR-020-two.md"), handWritten)
	err := adrCmd([]string{"-envs", tr.envs, "-registry", tr.registry, "-scorecards", tr.scorecards, "-out-adr", tr.adr, "-root", tr.root})
	if err == nil || !strings.Contains(err.Error(), "claimed by 2 files") {
		t.Fatalf("err = %v; want a duplicate-number refusal", err)
	}
}

// TestADRMapMustBeComplete: a family with no ADR number, or an ADR number
// with no registry row, is refused rather than silently dropped -- which is
// how fifteen families came to have no ADR at all.
func TestADRMapMustBeComplete(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "A31", 0, "corr", "", "Correlation storage (A31)")
	tr.registryRows(t, "20\trequest-reply-correlation-storage\t-\tCorrelation storage")
	err := adrCmd([]string{"-envs", tr.envs, "-registry", tr.registry, "-scorecards", tr.scorecards, "-out-adr", tr.adr, "-root", tr.root})
	if err == nil || !strings.Contains(err.Error(), "has no ADR number") {
		t.Fatalf("err = %v; want a missing-ADR refusal", err)
	}
	tr.env(t, "A31", 20, "corr", "", "Correlation storage (A31)")
	tr.registryRows(t, "19\tkafka-offset-tracker\t-\tKafka offset tracker")
	err = adrCmd([]string{"-envs", tr.envs, "-registry", tr.registry, "-scorecards", tr.scorecards, "-out-adr", tr.adr, "-root", tr.root})
	if err == nil || !strings.Contains(err.Error(), "no row in the ADR registry") {
		t.Fatalf("err = %v; want a missing-registry-row refusal", err)
	}
}

// TestRealRegistryAndEnvFilesAgree checks the repository's own map: every
// family has an ADR number, every number has a registry row, and no ADR
// number is claimed by two files in adr/.
func TestRealRegistryAndEnvFilesAgree(t *testing.T) {
	fams, err := loadEnvs("../../../hack/bakeoff")
	if err != nil {
		t.Fatal(err)
	}
	reg, err := loadRegistry("../../../hack/bakeoff/adrs.tsv")
	if err != nil {
		t.Fatal(err)
	}
	docs, err := plan(fams, reg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) < 8 {
		t.Fatalf("only %d ADR(s) collect families", len(docs))
	}
	seen := map[string]string{}
	for _, d := range docs {
		if _, _, err := adrPath("../../../adr", d); err != nil {
			t.Errorf("ADR-%03d: %v", d.Num, err)
		}
		for _, f := range d.Families {
			if prev, dup := seen[f.Row]; dup {
				t.Errorf("row %s is collected by ADR-%03d and by %s", f.Row, d.Num, prev)
			}
			seen[f.Row] = "ADR-" + itoa(d.Num)
		}
	}
	for _, row := range []string{"A1", "A10", "A20", "A29"} {
		if seen[row] == "" {
			t.Errorf("row %s is collected by no ADR", row)
		}
	}
}

// TestADRRefusesTwoFamiliesBindingOneAutoKey: the block form keeps one value
// per key, so two binding families sharing an AUTO_KEY would silently bind
// one family's constant to the other's winner.
func TestADRRefusesTwoFamiliesBindingOneAutoKey(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "A8", 17, "dispatch", "", "Runner lane assignment (A8)")
	tr.env(t, "A26", 17, "dispatch", "", "Variant dispatch mechanism (A26)")
	tr.registryRows(t, "17\tassumptions\t-\tAssumptions ledger")
	tr.scorecard(t, "A8", "hashlanes", "dispatch", "./internal/dispatch/auto.go")
	tr.scorecard(t, "A26", "interface", "dispatch", "./internal/engine/auto.go")
	err := adrCmd([]string{"-envs", tr.envs, "-registry", tr.registry, "-scorecards", tr.scorecards, "-out-adr", tr.adr, "-root", tr.root})
	if err == nil || !strings.Contains(err.Error(), "both bind auto.dispatch") {
		t.Fatalf("err = %v; want a duplicate auto-key refusal", err)
	}
	// AUTO_FILE=- takes the second family out of the binding, which is how
	// the repository's own A8/A26 pair is arranged.
	tr.env(t, "A26", 17, "dispatch", "-", "Variant dispatch mechanism (A26)")
	tr.run(t)
	doc := tr.read(t, "ADR-017-assumptions.md")
	if !strings.Contains(doc, "auto:\n  dispatch: hashlanes\n") {
		t.Errorf("ADR-017 auto block:\n%s", doc)
	}
}

// TestADRIgnoresAMarkerQuotedInProse: ADR-017 explains the markers in its own
// assumption ledger, so a marker counts only on a line of its own.
func TestADRIgnoresAMarkerQuotedInProse(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "A31", 20, "corr", "", "Correlation storage (A31)")
	tr.registryRows(t, "20\trequest-reply-correlation-storage\t-\tCorrelation storage")
	tr.scorecard(t, "A31", "shardedmap", "corr", "./internal/corr/auto.go")
	quoted := "---\nstatus: Accepted\nkind: policy\ndate: 2026-09-05\nvariants: []\n---\n\n" +
		"# ADR-020: Correlation storage\n\n## Context\n\n" +
		"| id | note |\n|---|---|\n| A24.13 | it writes between " + beginMarker + " and " + endMarker + " markers |\n"
	path := filepath.Join(tr.adr, "ADR-020-request-reply-correlation-storage.md")
	mustWrite(t, path, quoted)
	tr.run(t)
	doc := tr.read(t, "ADR-020-request-reply-correlation-storage.md")
	if !strings.Contains(doc, "| A24.13 | it writes between "+beginMarker+" and "+endMarker+" markers |") {
		t.Errorf("the quoted markers were spliced into:\n%s", doc)
	}
	first := doc
	tr.run(t)
	if again := tr.read(t, "ADR-020-request-reply-correlation-storage.md"); again != first {
		t.Errorf("not idempotent with markers quoted in prose:\n%s\n---\n%s", first, again)
	}
}

// TestGeneratedADRDropsARowThatMoved: the generator owns the variants list of
// a file it wrote, so a row reassigned to another ADR leaves that file --
// while a hand-written ADR keeps rows no env file names (ADR-011's C5, C6).
func TestGeneratedADRDropsARowThatMoved(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "A31", 20, "corr", "", "Correlation storage (A31)")
	tr.env(t, "A14", 20, "offset", "", "Kafka offset tracker (A14)")
	tr.registryRows(t, "20\trequest-reply-correlation-storage\t-\tCorrelation storage")
	tr.scorecard(t, "A31", "shardedmap", "corr", "./internal/corr/auto.go")
	tr.scorecard(t, "A14", "bitset", "offset", "./driverkit/offset/auto.go")
	tr.run(t)
	if doc := tr.read(t, "ADR-020-request-reply-correlation-storage.md"); !strings.Contains(doc, "variants: [A14, A31]") {
		t.Fatalf("first render:\n%s", doc)
	}
	// A14 moves to its own ADR; ADR-020 must stop claiming it.
	tr.env(t, "A14", 19, "offset", "", "Kafka offset tracker (A14)")
	tr.registryRows(t,
		"19\tkafka-offset-tracker\t-\tKafka offset tracker (A14)",
		"20\trequest-reply-correlation-storage\t-\tCorrelation storage")
	tr.run(t)
	doc := tr.read(t, "ADR-020-request-reply-correlation-storage.md")
	if !strings.Contains(doc, "variants: [A31]") || strings.Contains(doc, "offset: bitset") {
		t.Errorf("a moved row stayed behind:\n%s", doc)
	}
	if !strings.Contains(tr.read(t, "ADR-019-kafka-offset-tracker.md"), "variants: [A14]") {
		t.Error("ADR-019 did not gain the moved row")
	}
}

func TestRowLess(t *testing.T) {
	got := []string{"B10", "A10", "A2", "C1", "B2", "A1"}
	for i := range got {
		for j := i + 1; j < len(got); j++ {
			if rowLess(got[j], got[i]) {
				got[i], got[j] = got[j], got[i]
			}
		}
	}
	if want := "A1 A2 A10 B2 B10 C1"; strings.Join(got, " ") != want {
		t.Errorf("sorted = %q, want %q", strings.Join(got, " "), want)
	}
}

// markerBody wraps prose in a body with whatever markers the case names, so
// that each malformed shape is built the way a merge or a copy-paste builds
// it: real prose above and below, and the marker lines exactly as the tool
// writes them.
func markerBody(markers ...string) string {
	body := "\n# ADR-011: Failure policy\n\n## Context\n\nProse a human wrote.\n\n"
	for _, m := range markers {
		switch m {
		case "begin":
			body += beginMarker + "\n"
		case "end":
			body += endMarker + "\n"
		default:
			body += m + "\n"
		}
	}
	return body + "\n## Decision\n\nMore prose.\n\n## Consequences\n\nClosing prose.\n"
}

func handWrittenWith(body string) string {
	return "---\nstatus: Accepted\nkind: policy\ndate: 2026-09-05\nvariants: [C5, C6]\n---\n" + body
}

// TestADRRefusesMalformedMarkers is the finding this pass exists for. The
// splice used to take the first BEGIN and the first END and append whenever
// it could not find both, so a lone BEGIN with prose after it lost that prose
// on the second render (242 -> 3896 -> 3789 bytes, "## Decision" and
// "## Consequences" gone, exit 0), and a stray END above the BEGIN appended a
// whole fresh copy every time (3803 -> 7407 -> 11011). Every shape that is
// not zero or one well-formed pair must now be refused, by name, with the
// file left byte for byte as it was.
func TestADRRefusesMalformedMarkers(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    string
		want    string // substring of the refusal, "" when the render must succeed
		renders bool
	}{
		{
			name: "lone BEGIN",
			body: markerBody("begin"),
			want: "a BEGIN marker on line 8 with no END marker",
		},
		{
			name: "lone END",
			body: markerBody("end"),
			want: "an END marker on line 8 with no BEGIN marker",
		},
		{
			name: "END before BEGIN",
			body: markerBody("end", "begin"),
			want: "the END marker (line 8) comes before the BEGIN marker (line 9)",
		},
		{
			name: "two BEGINs",
			body: markerBody("begin", "end", "begin"),
			want: "2 BEGIN markers (lines 8, 10); a file holds at most one generated section",
		},
		{
			name: "two ENDs",
			body: markerBody("begin", "end", "end"),
			want: "2 END markers (lines 9, 10); a file holds at most one generated section",
		},
		{
			name:    "well-formed pair",
			body:    markerBody("begin", "old generated text", "end"),
			renders: true,
		},
		{
			name:    "no markers at all",
			body:    markerBody(),
			renders: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := newADRTree(t)
			tr.env(t, "B10", 11, "b10", "", "Error construction (B10)")
			tr.registryRows(t, "11\tfailure-policy\t-\tFailure policy")
			tr.scorecard(t, "B10", "prealloccodes", "b10", "./x/apibench/b10/auto.go")
			path := filepath.Join(tr.adr, "ADR-011-failure-policy.md")
			before := handWrittenWith(tc.body)
			mustWrite(t, path, before)

			err := adrCmd([]string{
				"-envs", tr.envs, "-registry", tr.registry,
				"-scorecards", tr.scorecards, "-out-adr", tr.adr, "-root", tr.root,
			})
			after, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if tc.renders {
				if err != nil {
					t.Fatalf("render refused a well-formed file: %v", err)
				}
				for _, want := range []string{"Prose a human wrote.", "Closing prose.", beginMarker, endMarker} {
					if !strings.Contains(string(after), want) {
						t.Errorf("rendered file lost %q:\n%s", want, after)
					}
				}
				if n := strings.Count(string(after), beginMarker); n != 1 {
					t.Errorf("%d BEGIN markers after the render", n)
				}
				return
			}
			if err == nil {
				t.Fatalf("malformed markers were accepted; file is now:\n%s", after)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v\nwant it to name %q", err, tc.want)
			}
			if !strings.Contains(err.Error(), "nothing written") || !strings.Contains(err.Error(), path) {
				t.Errorf("err = %v\nwant it to name the file and say nothing was written", err)
			}
			// The refusal must leave the human's file exactly as it was --
			// prose, markers, byte count and all -- and must not have written
			// any other ADR either.
			if string(after) != before {
				t.Errorf("a refused render changed the file (%d -> %d bytes):\n%s", len(before), len(after), after)
			}
			if others, _ := filepath.Glob(filepath.Join(tr.adr, "*")); len(others) != 1 {
				t.Errorf("a refused render left %d file(s) in adr/: %v", len(others), others)
			}
		})
	}
}

// TestADRRefusalIsAllOrNothing: one malformed file must not cost the run its
// other ADRs' output half way through a dozen writes. Nothing is written at
// all, so the set is never part old and part new.
func TestADRRefusalIsAllOrNothing(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "B10", 11, "b10", "", "Error construction (B10)")
	tr.env(t, "A31", 20, "corr", "", "Correlation storage (A31)")
	tr.registryRows(t,
		"11\tfailure-policy\t-\tFailure policy",
		"20\trequest-reply-correlation-storage\t-\tCorrelation storage")
	tr.scorecard(t, "B10", "prealloccodes", "b10", "./x/apibench/b10/auto.go")
	tr.scorecard(t, "A31", "shardedmap", "corr", "./internal/corr/auto.go")
	mustWrite(t, filepath.Join(tr.adr, "ADR-011-failure-policy.md"), handWrittenWith(markerBody("begin")))

	err := adrCmd([]string{"-envs", tr.envs, "-registry", tr.registry, "-scorecards", tr.scorecards, "-out-adr", tr.adr, "-root", tr.root})
	if err == nil {
		t.Fatal("the run was not refused")
	}
	// ADR-020 is well formed and would have been created; it must not exist.
	if _, statErr := os.Stat(filepath.Join(tr.adr, "ADR-020-request-reply-correlation-storage.md")); !os.IsNotExist(statErr) {
		t.Errorf("a refused run still created ADR-020: %v", statErr)
	}
	// Fix the file and the same command writes both.
	mustWrite(t, filepath.Join(tr.adr, "ADR-011-failure-policy.md"), handWrittenWith(markerBody()))
	tr.run(t)
	for _, name := range []string{"ADR-011-failure-policy.md", "ADR-020-request-reply-correlation-storage.md"} {
		if _, statErr := os.Stat(filepath.Join(tr.adr, name)); statErr != nil {
			t.Errorf("%s: %v", name, statErr)
		}
	}
}

// TestADRCommitIsAtomic: the file is replaced by a rename, so a render that
// is interrupted or fails at any point before the rename leaves the previous
// file exactly as it was and never a truncated one. os.WriteFile truncated in
// place, which put a hand-written record at the mercy of a crash between two
// of the dozen files one run writes.
func TestADRCommitIsAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ADR-011-failure-policy.md")
	original := "original content that must survive a failed write\n"
	mustWrite(t, path, original)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	w := &adrWrite{path: path, content: []byte("new content\n"), verb: "wrote", doc: &adrDoc{Num: 11}}

	// A commit that cannot create its temporary file must leave the target
	// untouched. A directory the process may read but not write is the
	// cheapest way to make every step before the rename fail.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	commitErr := w.commit()
	if chErr := os.Chmod(dir, 0o755); chErr != nil {
		t.Fatal(chErr)
	}
	if commitErr == nil {
		t.Error("commit into a read-only directory succeeded")
	}
	if got, _ := os.ReadFile(path); string(got) != original {
		t.Errorf("a failed commit changed the file: %q", got)
	}

	// The successful commit replaces the file by rename, which gives it a new
	// inode: the target never exists in a partially written state, because
	// the bytes are all in the temporary file before the name moves.
	if err := w.commit(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// A rename gives the replacement a different identity on disk; an
	// in-place write does not, which is how this tells them apart.
	if os.SameFile(before, after) {
		t.Error("commit wrote in place; an interrupted write could truncate the file")
	}
	if got, _ := os.ReadFile(path); string(got) != "new content\n" {
		t.Errorf("content = %q", got)
	}
	if perm := after.Mode().Perm(); perm != 0o644 {
		t.Errorf("mode = %v, want 0644", perm)
	}
	// No temporary file is left behind, and none of them can be mistaken for
	// an ADR by adrPath's glob.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "ADR-011-failure-policy.md" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("directory holds %v", names)
	}
}

// TestADRRecordsAFamilyThatBindsNothing: family A29's copy mode is chosen per
// publication (hack/bakeoff/A29.env sets AUTO_FILE=-) and ADR-009 carries no
// auto binding. The render must record the family, its winner and its kind
// table, and invent no `auto:` front matter at all -- not even an empty block
// -- because internal/testguard would then report a binding for a family that
// has no generated constant to check it against.
func TestADRRecordsAFamilyThatBindsNothing(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "A29", 9, "copymode", "-", "Payload ownership and copy modes (A29)")
	tr.registryRows(t, "9\tsafe-copy-default-ownership-modes\t-\tSafe-copy publication and safe receive are the defaults")
	tr.scorecard(t, "A29", "safeclone", "copymode", "-")
	mustWrite(t, filepath.Join(tr.adr, "ADR-009-safe-copy-default-ownership-modes.md"),
		"---\nstatus: Accepted\nkind: policy\ndate: 2026-09-05\nvariants: [A29]\n---\n"+
			"\n# ADR-009: Safe-copy publication\n\n## Context\n\nProse a human wrote.\n")
	tr.run(t)
	doc := tr.read(t, "ADR-009-safe-copy-default-ownership-modes.md")
	front, _, err := splitFront(doc)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(front, "auto") {
		t.Errorf("a family that binds nothing gained auto front matter:\n%s", front)
	}
	if _, ok := testguard.ParseADRAutoBinding(doc, "copymode"); ok {
		t.Error("testguard found a binding for a family with no generated constant")
	}
	// The row is still recorded, with its winner and its evidence.
	for _, want := range []string{
		"variants: [A29]",
		"authoritative: no",
		"#### A29 -- Payload ownership and copy modes (A29)",
		"Winner: **safeclone**",
		"Binds no `Auto` constant.",
		"Prose a human wrote.",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("ADR-009 lacks %q:\n%s", want, doc)
		}
	}
	// And the whole-file case: an ADR every one of whose families binds
	// nothing says so instead of showing an empty binding section.
	if !strings.Contains(doc, "No family collected here binds a generated `Auto` constant") {
		t.Errorf("ADR-009 does not say that nothing is bound:\n%s", doc)
	}
}

// TestGeneratedADRRefreshesItsTitle: the record the tool owns takes its title
// from hack/bakeoff/adrs.tsv on every render, so a title corrected after a
// campaign lands. The heading sits OUTSIDE the markers, and a splice that
// only replaced the marked section could never refresh it -- while the front
// matter around it was rewritten every time, which made
// `generated: bakeoff adr` promise more than it delivered.
func TestGeneratedADRRefreshesItsTitle(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "B8", 6, "b8", "", "Codec interface shape (B8)")
	tr.registryRows(t, "6\tapi-surface-shapes\t-\tAPI surface shapes")
	tr.scorecard(t, "B8", "hooks", "b8", "./x/apibench/b8/auto.go")
	tr.run(t)
	path := filepath.Join(tr.adr, "ADR-006-api-surface-shapes.md")
	first := tr.read(t, "ADR-006-api-surface-shapes.md")
	if !strings.Contains(first, "# ADR-006: API surface shapes\n") {
		t.Fatalf("first render:\n%s", first)
	}

	// A human adds prose to the generated file, and the title is corrected in
	// the registry. Both must be honoured: the prose survives (it is outside
	// the markers) and the title is refreshed (it is the tool's own line).
	mustWrite(t, path, first+"\n## A human wrote this into a GENERATED file\n\nKeep me.\n")
	tr.registryRows(t, "6\tapi-surface-shapes\t-\tCORRECTED TITLE FOR ADR-006")
	tr.run(t)
	after := tr.read(t, "ADR-006-api-surface-shapes.md")
	if !strings.Contains(after, "# ADR-006: CORRECTED TITLE FOR ADR-006\n") {
		t.Errorf("a corrected registry title did not land:\n%s", after)
	}
	if strings.Contains(after, "# ADR-006: API surface shapes\n") {
		t.Errorf("the old title survived:\n%s", after)
	}
	if !strings.Contains(after, "## A human wrote this into a GENERATED file") || !strings.Contains(after, "Keep me.") {
		t.Errorf("a re-render dropped prose a human added to a generated file:\n%s", after)
	}
	if n := strings.Count(after, "\n# ADR-006:"); n != 1 {
		t.Errorf("%d title headings after the re-render", n)
	}
	// Still idempotent with both the correction and the human prose in place.
	tr.run(t)
	if again := tr.read(t, "ADR-006-api-surface-shapes.md"); again != after {
		t.Errorf("not idempotent after a title correction:\n%s\n---\n%s", after, again)
	}
}

// TestAttachedADRKeepsItsOwnTitle: a hand-written record's heading is prose
// the human wrote, and the registry's title column only records what a fresh
// render would have used (adr/README.md). It must not be rewritten.
func TestAttachedADRKeepsItsOwnTitle(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "B10", 11, "b10", "", "Error construction (B10)")
	tr.registryRows(t, "11\tfailure-policy\t-\tA TITLE THE TOOL MUST NOT IMPOSE")
	tr.scorecard(t, "B10", "prealloccodes", "b10", "./x/apibench/b10/auto.go")
	mustWrite(t, filepath.Join(tr.adr, "ADR-011-failure-policy.md"), handWritten)
	tr.run(t)
	doc := tr.read(t, "ADR-011-failure-policy.md")
	if !strings.Contains(doc, "# ADR-011: Failure policy\n") {
		t.Errorf("the human's title was rewritten:\n%s", doc)
	}
	if strings.Contains(doc, "A TITLE THE TOOL MUST NOT IMPOSE") {
		t.Errorf("the registry title was imposed on a hand-written ADR:\n%s", doc)
	}
	for _, want := range []string{"status: Accepted", "kind: policy", "date: 2026-09-05", "Prose a human wrote.", "Closing prose."} {
		if !strings.Contains(doc, want) {
			t.Errorf("attached ADR lost %q", want)
		}
	}
}

// TestGeneratedADRReportsAnOlderFileName: a render refreshes the title but
// cannot rename the file -- two files claiming one number is what adrPath and
// `make adr-check` refuse -- so a corrected slug is reported with the move
// that applies it rather than ignored.
func TestGeneratedADRReportsAnOlderFileName(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "B8", 6, "b8", "", "Codec interface shape (B8)")
	tr.registryRows(t, "6\told-slug\t-\tAPI surface shapes")
	tr.scorecard(t, "B8", "hooks", "b8", "./x/apibench/b8/auto.go")
	tr.run(t)
	tr.registryRows(t, "6\tnew-slug\t-\tAPI surface shapes")
	out := captureStdout(t, func() { tr.run(t) })
	if !strings.Contains(out, "ADR-006-old-slug.md keeps an older file name") ||
		!strings.Contains(out, `the registry now spells the slug "new-slug"`) ||
		!strings.Contains(out, "git mv ") {
		t.Errorf("no notice about the stale file name:\n%s", out)
	}
	// The file is not renamed and no second file claims the number.
	files, _ := filepath.Glob(filepath.Join(tr.adr, "ADR-006-*.md"))
	if len(files) != 1 || filepath.Base(files[0]) != "ADR-006-old-slug.md" {
		t.Errorf("ADR-006 files = %v", files)
	}
}

// captureStdout runs f with os.Stdout redirected and returns what it printed.
func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var b strings.Builder
		_, _ = io.Copy(&b, r)
		done <- b.String()
	}()
	f()
	os.Stdout = saved
	_ = w.Close()
	out := <-done
	_ = r.Close()
	return out
}

// TestRenderVerbFollowsOwnership: the line the render prints must say what it
// did to the record. It used to read "attached to" for a tool-owned file on
// every render after the first, because the verb came from whether the file
// existed rather than from who owns it -- and "attached to" is the word for
// the one case where a human's prose is authoritative.
func TestRenderVerbFollowsOwnership(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "B8", 6, "b8", "", "Codec interface shape (B8)")
	tr.env(t, "B10", 11, "b10", "", "Error construction (B10)")
	tr.registryRows(t,
		"6\tapi-surface-shapes\t-\tAPI surface shapes",
		"11\tfailure-policy\t-\tFailure policy")
	tr.scorecard(t, "B8", "hooks", "b8", "./x/apibench/b8/auto.go")
	tr.scorecard(t, "B10", "prealloccodes", "b10", "./x/apibench/b10/auto.go")
	mustWrite(t, filepath.Join(tr.adr, "ADR-011-failure-policy.md"), handWritten)

	for _, round := range []string{"first", "second"} {
		out := captureStdout(t, func() { tr.run(t) })
		if !strings.Contains(out, "wrote "+filepath.Join(tr.adr, "ADR-006-api-surface-shapes.md")) {
			t.Errorf("%s render of the tool-owned record: %s", round, out)
		}
		if !strings.Contains(out, "attached to "+filepath.Join(tr.adr, "ADR-011-failure-policy.md")) {
			t.Errorf("%s render of the hand-written record: %s", round, out)
		}
	}
}
