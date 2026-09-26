package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// modules is the shape `run` collects while walking: module path -> the
// repository-relative directory that module's root sits in.
var modules = map[string]string{
	"github.com/iqhive/iqmq":                       ".",
	"github.com/iqhive/iqmq/benchmarks":            "benchmarks",
	"github.com/iqhive/iqmq/codec/jsonv2":          "codec/jsonv2",
	"github.com/iqhive/iqmq/benchmarks/nativebase": "benchmarks/native-baselines",
}

// TestNormalizePkg pins the reconciliation assumption A25.2 records. Every form
// `the fuzz runner` records has to land on the repository-relative directory the
// target list is keyed by; before this, `./internal/topicname` and
// `github.com/iqhive/iqmq/internal/topicname` both missed `internal/topicname`
// and every run result was dropped.
func TestNormalizePkg(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"./internal/topicname", "internal/topicname"},
		{"github.com/iqhive/iqmq/internal/topicname", "internal/topicname"},
		{"internal/topicname", "internal/topicname"},
		{"github.com/iqhive/iqmq/internal/topicname/", "internal/topicname"},
		{"github.com/iqhive/iqmq", "."},
		{".", "."},
		{"./", "."},
		{"github.com/iqhive/iqmq/benchmarks/internal/budgets", "benchmarks/internal/budgets"},
		{"github.com/iqhive/iqmq/codec/jsonv2", "codec/jsonv2"},
		// The longest matching module path wins, or a nested module's packages
		// would be filed under the parent's directory.
		{"github.com/iqhive/iqmq/benchmarks/nativebase/kafka", "benchmarks/native-baselines/kafka"},
		// A package of no known module is left alone rather than guessed at,
		// and then shows up as an orphan the caller has to explain.
		{"example.com/other/pkg", "example.com/other/pkg"},
	} {
		if got := normalizePkg(c.in, modules); got != c.want {
			t.Errorf("normalizePkg(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestNormalizePkgIsNotMapOrderDependent pins the answer for a package of a
// nested module whose directory is not its import path, which is the only
// shape the choice of module can be observed in. The selection used to be made
// against a pkg the loop had already rewritten, so the result depended on which
// of two matching modules Go's randomised map iteration reached first and this
// case came out wrong about half the time. One assertion
// cannot catch that; the retries can.
func TestNormalizePkgIsNotMapOrderDependent(t *testing.T) {
	const in, want = "github.com/iqhive/iqmq/benchmarks/nativebase/kafka", "benchmarks/native-baselines/kafka"
	for i := range 200 {
		if got := normalizePkg(in, modules); got != want {
			t.Fatalf("iteration %d: normalizePkg(%q) = %q, want %q", i, in, got, want)
		}
	}
}

// fakeTree writes a one-module tree with two fuzz targets and a results
// directory holding the given JSON lines.
func fakeTree(t *testing.T, lines ...string) (root, results string) {
	t.Helper()
	root = t.TempDir()
	write := func(path, content string) {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "go.mod"), "module example.com/m\n\ngo 1.26\n")
	write(filepath.Join(root, "p", "fuzz_test.go"),
		"package p\n\nimport \"testing\"\n\nfunc FuzzOne(f *testing.F) {}\n\nfunc FuzzTwo(f *testing.F) {}\n")
	write(filepath.Join(root, "p", "testdata", "fuzz", "FuzzOne", "seed"), "seed")
	results = t.TempDir()
	if len(lines) > 0 {
		write(filepath.Join(results, "run.jsonl"), strings.Join(lines, "\n")+"\n")
	}
	return root, results
}

// TestRunMergesResultsByNormalizedPackage is the end-to-end form: a result
// recorded as an import path has to reach the target and bring its duration,
// result, arch and Go version with it.
func TestRunMergesResultsByNormalizedPackage(t *testing.T) {
	root, results := fakeTree(t,
		`{"target":"FuzzOne","package":"example.com/m/p","duration_s":600,"result":"ok","arch":"amd64","go":"go1.27.1"}`,
		`{"target":"FuzzTwo","package":"./p","duration_s":600,"result":"crash","arch":"amd64","go":"go1.27.1"}`)
	out := filepath.Join(t.TempDir(), "m.json")
	if err := run(root, results, out, true); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if m.Total != 2 || m.Runs != 2 {
		t.Fatalf("manifest holds %d target(s) and %d run(s), want 2 and 2", m.Total, m.Runs)
	}
	if m.DurationS != 1200 {
		t.Errorf("total_duration_s = %v, want 1200", m.DurationS)
	}
	if !strings.Contains(m.Result, "crash") {
		t.Errorf("result = %q, want it to name the crash", m.Result)
	}
	for _, e := range m.Targets {
		if len(e.Runs) != 1 {
			t.Fatalf("%s.%s carries %d run(s), want 1", e.Package, e.Name, len(e.Runs))
		}
		if e.Runs[0].Go == "" || e.Runs[0].Arch == "" || e.Runs[0].DurationS == 0 {
			t.Errorf("%s.%s run is missing a §11.3 field: %+v", e.Package, e.Name, e.Runs[0])
		}
	}
	if m.Go == "" || m.Arch == "" {
		t.Error("manifest is missing its own go/arch")
	}
	// FuzzOne has a corpus and FuzzTwo does not: the hash is per target.
	for _, e := range m.Targets {
		if e.Name == "FuzzOne" && (e.Seeds != 1 || e.CorpusSHA256 == "") {
			t.Errorf("FuzzOne: seeds=%d hash=%q, want 1 seed and a hash", e.Seeds, e.CorpusSHA256)
		}
		if e.Name == "FuzzTwo" && e.CorpusSHA256 != "" {
			t.Errorf("FuzzTwo has no corpus but carries the hash %q", e.CorpusSHA256)
		}
	}
}

// TestRunRefusesAnOrphanResult holds the other half: a result the merge cannot
// place must fail rather than vanish, because a manifest missing every duration
// looks exactly like one whose targets were never run.
func TestRunRefusesAnOrphanResult(t *testing.T) {
	root, results := fakeTree(t,
		`{"target":"FuzzOne","package":"example.com/m/p","duration_s":600,"result":"ok"}`,
		`{"target":"FuzzGone","package":"example.com/m/q","duration_s":600,"result":"ok"}`)
	err := run(root, results, filepath.Join(t.TempDir(), "m.json"), false)
	if err == nil {
		t.Fatal("run accepted a result that matches no target")
	}
	if !strings.Contains(err.Error(), "q.FuzzGone") {
		t.Errorf("refusal does not name the orphan: %v", err)
	}
}

// TestRunRequireRuns fails when a target was not exercised, which is what
// `make fuzz-nightly` needs of the shard set it just ran.
func TestRunRequireRuns(t *testing.T) {
	root, results := fakeTree(t,
		`{"target":"FuzzOne","package":"example.com/m/p","duration_s":600,"result":"ok"}`)
	out := filepath.Join(t.TempDir(), "m.json")
	err := run(root, results, out, true)
	if err == nil {
		t.Fatal("-require-runs passed with one of two targets unrun")
	}
	if !strings.Contains(err.Error(), "1 of 2 runnable target(s) have no run result") {
		t.Errorf("unexpected refusal: %v", err)
	}
	// The refusal names the target, not only a count: "1 of 195" is not
	// something an operator can act on.
	if !strings.Contains(err.Error(), "p.FuzzTwo") {
		t.Errorf("refusal does not name the unrun target: %v", err)
	}
	// The manifest is still written, so the operator can see which target is
	// missing rather than only being told a count.
	if _, statErr := os.Stat(out); statErr != nil {
		t.Errorf("the manifest was not written beside the refusal: %v", statErr)
	}
	if err := run(root, results, out, false); err != nil {
		t.Errorf("without -require-runs the same tree is fine: %v", err)
	}
}

// TestRequireRunsIgnoresABuildConstrainedTarget is the case that would have
// made the nightly manifest red every night from the day it was wired in:
// `conformance/differential`'s three `FuzzDifferential_*` targets are
// `//go:build integration`, so a runner without a broker cannot build them,
// let alone run them -- and this AST walk sees them all the same. Measured on
// this tree, the campaign enumerates 192 targets and the walk 195, and the
// difference is exactly those three.
func TestRequireRunsIgnoresABuildConstrainedTarget(t *testing.T) {
	const brokerFile = "//go:build integration\n\npackage p\n\nimport \"testing\"\n\nfunc FuzzBroker(f *testing.F) {}\n"

	root, results := fakeTree(t,
		`{"target":"FuzzOne","package":"example.com/m/p","duration_s":600,"result":"ok"}`,
		`{"target":"FuzzTwo","package":"example.com/m/p","duration_s":600,"result":"ok"}`)
	if err := os.WriteFile(filepath.Join(root, "p", "broker_test.go"), []byte(brokerFile), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "m.json")
	if err := run(root, results, out, true); err != nil {
		t.Fatalf("-require-runs refused a campaign that ran every target it could build: %v", err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	// It is still in the manifest, with its corpus hash and the reason it was
	// not asked for a run. Dropping it would lose what §11.3 wants recorded.
	found := false
	for _, e := range m.Targets {
		switch {
		case e.Name == "FuzzBroker":
			found = true
			if e.Constraint != "integration" {
				t.Errorf("FuzzBroker constraint = %q, want %q", e.Constraint, "integration")
			}
		case e.Constraint != "":
			t.Errorf("%s carries constraint %q; it is in the default build", e.Name, e.Constraint)
		}
	}
	if !found {
		t.Error("a build-constrained target was dropped from the manifest instead of being marked")
	}
	if m.Total != 3 {
		t.Errorf("total_targets = %d, want 3: the constrained target is counted", m.Total)
	}

	// And the exemption is narrow: an unrun target that IS in the default
	// build still fails, or the check would have stopped checking anything.
	root2, results2 := fakeTree(t,
		`{"target":"FuzzOne","package":"example.com/m/p","duration_s":600,"result":"ok"}`)
	if err := os.WriteFile(filepath.Join(root2, "p", "broker_test.go"), []byte(brokerFile), 0o644); err != nil {
		t.Fatal(err)
	}
	err = run(root2, results2, filepath.Join(t.TempDir(), "m.json"), true)
	if err == nil {
		t.Fatal("-require-runs accepted an unrun target that is in the default build")
	}
	if !strings.Contains(err.Error(), "p.FuzzTwo") || strings.Contains(err.Error(), "FuzzBroker") {
		t.Errorf("refusal should name FuzzTwo and not FuzzBroker: %v", err)
	}
}

// exemptTargets is EVERY fuzz target this repository excuses from
// `-require-runs`, pinned by name.
//
// The mechanism is right and was unheld: `unmetConstraint` asks go/build
// whether the default build selects the file, so the exemption GROWS silently.
// Put a tag nothing satisfies on a `_test.go` file -- two letters of
// "integration" transposed, a copy-pasted `//go:build !ci`, a
// `_windows_test.go` rename -- and every fuzz target in that file stops being
// required by the nightly manifest with nothing going red. `make ratchet`
// cannot see it either: it counts targets, and the count does not change when
// one moves from "required" to "exempt". The only trace was an stderr line
// nothing asserted on.
//
// A name list was rejected in C08.2's Alternatives as "a second place to
// edit", and that objection is about an ALLOWLIST the tool consults at run
// time. This is a test over the real tree, which is one place and the usual
// one: a deliberately constrained target means adding a row here, and an
// accidental one means a red test that names the target.
var exemptTargets = map[string]string{
	"conformance/differential.FuzzDifferential_jetstream": "integration",
	"conformance/differential.FuzzDifferential_rabbitmq":  "integration",
	"conformance/differential.FuzzDifferential_redis":     "integration",
}

// constrainedTargets reads the manifest at path and returns target -> the
// build constraint that keeps it out of the default build.
func constrainedTargets(t *testing.T, path string) (manifest, map[string]string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, e := range m.Targets {
		if e.Constraint != "" {
			got[e.Package+"."+e.Name] = e.Constraint
		}
	}
	return m, got
}

// exemptionProblems compares what the tree exempts with what is recorded, in
// both directions. It is a function rather than a pair of loops inside the
// test so that TestAnUnrecordedExemptionIsReported can drive it over a tree it
// controls -- the negative control for this check used to be a build tag typed
// by hand into a throwaway worktree, which is a demonstration nothing reruns.
func exemptionProblems(got, want map[string]string) []string {
	var problems []string
	for key, w := range want {
		if got[key] != w {
			problems = append(problems, fmt.Sprintf(
				"%s is excused from -require-runs by %q, want %q; a target that stopped being "+
					"constrained is now required by the nightly, which is fine -- delete its row",
				key, got[key], w,
			))
		}
	}
	for key, constraint := range got {
		if _, ok := want[key]; !ok {
			problems = append(problems, fmt.Sprintf(
				"%s is NOT required by the nightly manifest, because %q keeps it out of the "+
					"default build, and nothing records that it should be. A stray or mistyped "+
					"build tag drops a target out of the check silently: record it deliberately, "+
					"or fix the tag.", key, constraint,
			))
		}
	}
	sort.Strings(problems)
	return problems
}

// TestExemptTargetsAreExactlyTheRecordedOnes walks the real repository.
func TestExemptTargetsAreExactlyTheRecordedOnes(t *testing.T) {
	out := filepath.Join(t.TempDir(), "m.json")
	// No results directory: the walk, the constraints and the counts are what
	// this is about, and `-require-runs` is off, so an unrun target is fine.
	if err := run("../..", "", out, false); err != nil {
		t.Fatal(err)
	}
	m, got := constrainedTargets(t, out)
	if m.Total < 100 {
		t.Fatalf("the walk found %d target(s); it did not reach the repository", m.Total)
	}
	for _, p := range exemptionProblems(got, exemptTargets) {
		t.Error(p)
	}
	t.Logf("%d target(s), %d excused from -require-runs", m.Total, len(got))
}

// TestAnUnrecordedExemptionIsReported is the negative control, over a tree
// this test writes: a fuzz target behind a tag nobody satisfies has to be
// reported rather than quietly excused, and a recorded target that is no
// longer constrained has to be reported too. Without both halves the check
// above would pass over a repository whose exemptions had all drifted.
func TestAnUnrecordedExemptionIsReported(t *testing.T) {
	root, _ := fakeTree(t)
	// `nosuchtag` is satisfied by no build of anything, which is what a
	// mistyped or copy-pasted tag amounts to.
	if err := os.WriteFile(filepath.Join(root, "p", "stray_test.go"),
		[]byte("//go:build nosuchtag\n\npackage p\n\nimport \"testing\"\n\nfunc FuzzStray(f *testing.F) {}\n"),
		0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "m.json")
	if err := run(root, "", out, false); err != nil {
		t.Fatal(err)
	}
	_, got := constrainedTargets(t, out)
	if got["p.FuzzStray"] != "nosuchtag" {
		t.Fatalf("the walk did not record the stray tag: %v", got)
	}

	problems := exemptionProblems(got, map[string]string{})
	if len(problems) != 1 || !strings.Contains(problems[0], "p.FuzzStray") {
		t.Fatalf("an unrecorded exemption was not reported: %v", problems)
	}
	if !strings.Contains(problems[0], "silently") {
		t.Errorf("the report does not say what the reader should do: %q", problems[0])
	}

	// The other direction: recorded, but no longer constrained.
	problems = exemptionProblems(got, map[string]string{
		"p.FuzzStray": "nosuchtag",
		"p.FuzzOne":   "integration",
	})
	if len(problems) != 1 || !strings.Contains(problems[0], "p.FuzzOne") {
		t.Fatalf("a recorded target that is no longer constrained was not reported: %v", problems)
	}

	// And the control: recorded exactly, no problems.
	if p := exemptionProblems(got, map[string]string{"p.FuzzStray": "nosuchtag"}); len(p) != 0 {
		t.Errorf("an exactly-recorded exemption reported %v", p)
	}
}
