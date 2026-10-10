package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// patchCard rewrites one field set of a scorecard on disk, so that a test can
// age or downgrade a card `bakeoff decide` would have written.
func patchCard(t *testing.T, tr *adrTree, family string, fn func(*scorecard)) {
	t.Helper()
	path := filepath.Join(tr.scorecards, family+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var sc scorecard
	if err := json.Unmarshal(data, &sc); err != nil {
		t.Fatal(err)
	}
	fn(&sc)
	out, err := json.MarshalIndent(sc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, path, string(out)+"\n")
}

// Defect 6d: `bakeoff adr` renders whatever scorecard is on disk with no
// staleness, profile or commit cross-check, so a family that failed mid
// campaign contributes its previous run's numbers to an ADR silently -- and a
// `quick` scorecard, which decides nothing , renders as a
// decision.
func TestADRRefusesANonDecidingScorecard(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "A1", 14, "queue", "", "Memory lane queue kinds (A1)")
	tr.registryRows(t, "14\tmemory-engine-internals\t-\tMemory engine internals (A1)")
	tr.scorecard(t, "A1", "mpmc", "queue", "./internal/queue/auto.go")
	patchCard(t, tr, "A1", func(sc *scorecard) { sc.Profile = "quick"; sc.Deciding = false })
	err := adrCmd([]string{
		"-envs", tr.envs, "-registry", tr.registry,
		"-scorecards", tr.scorecards, "-out-adr", tr.adr, "-root", tr.root,
	})
	if err == nil {
		t.Fatal("an ADR was rendered from a `quick` scorecard, which decides nothing (assumption A24.33)")
	}
	if !strings.Contains(err.Error(), "quick") {
		t.Errorf("error = %v; it must name the profile", err)
	}
	if _, statErr := os.Stat(filepath.Join(tr.adr, "ADR-014-memory-engine-internals.md")); statErr == nil {
		t.Error("the refused ADR was written anyway")
	}
}

// A scorecard measured on a tree whose family package has changed since is
// stale: the numbers describe code that is no longer there.
func TestADRRefusesAScorecardWhosePackageChangedSince(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	tr := newADRTree(t)
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", tr.root}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "main")
	if err := os.MkdirAll(filepath.Join(tr.root, "internal", "a1"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(tr.root, "internal", "a1", "a1.go"), "package a1\n")
	git("add", "-A")
	git("commit", "-qm", "one")
	measured := git("rev-parse", "HEAD")
	mustWrite(t, filepath.Join(tr.root, "internal", "a1", "a1.go"), "package a1\n\nfunc changed() {}\n")
	git("add", "-A")
	git("commit", "-qm", "two")

	tr.env(t, "A1", 14, "queue", "", "Memory lane queue kinds (A1)")
	tr.registryRows(t, "14\tmemory-engine-internals\t-\tMemory engine internals (A1)")
	tr.scorecard(t, "A1", "mpmc", "queue", "./internal/queue/auto.go")
	patchCard(t, tr, "A1", func(sc *scorecard) { sc.Commit = measured; sc.Package = "internal/a1" })
	err := adrCmd([]string{
		"-envs", tr.envs, "-registry", tr.registry,
		"-scorecards", tr.scorecards, "-out-adr", tr.adr, "-root", tr.root,
	})
	if err == nil {
		t.Fatal("an ADR was rendered from numbers measured before the package changed")
	}
	if !strings.Contains(err.Error(), "internal/a1") {
		t.Errorf("error = %v; it must name the package that moved under the numbers", err)
	}
}

// The generated Decision section must not claim a §0.3(3) check that had no
// data to run on. It says which of p99, footprint and fairness was evaluated
// from what the scorecard recorded.
func TestADRSaysWhichExclusionGuardsRan(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "A1", 14, "queue", "", "Memory lane queue kinds (A1)")
	tr.registryRows(t, "14\tmemory-engine-internals\t-\tMemory engine internals (A1)")
	tr.scorecard(t, "A1", "mpmc", "queue", "./internal/queue/auto.go")
	patchCard(t, tr, "A1", func(sc *scorecard) {
		sc.Guards = []guardCheck{
			{Metric: "p99 latency", Unit: "p99-ns", Status: guardNoData, Detail: "no kind reported p99-ns"},
			{Metric: "memory footprint", Unit: "B/op, allocs/op", Status: guardEvaluated},
			{Metric: "fairness", Unit: "fairness", Status: guardNoData, Detail: "no benchmark emits a fairness metric"},
		}
	})
	tr.run(t, "-root", tr.root)
	doc := tr.read(t, "ADR-014-memory-engine-internals.md")
	for _, want := range []string{
		"memory footprint",
		"no benchmark emits a fairness metric",
		"not evaluated",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("the ADR does not say %q; §0.3(3) must not be claimed where it could not run", want)
		}
	}
}

// -allow-stale is the deliberate re-render of old data: the record is written
// and says in its own text that it is stale, rather than the operator having
// to remember.
func TestADRAllowStaleStatesTheStalenessInTheRecord(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "A1", 14, "queue", "", "Memory lane queue kinds (A1)")
	tr.registryRows(t, "14\tmemory-engine-internals\t-\tMemory engine internals (A1)")
	tr.scorecard(t, "A1", "mpmc", "queue", "./internal/queue/auto.go")
	patchCard(t, tr, "A1", func(sc *scorecard) { sc.Profile = "quick"; sc.Deciding = false })
	tr.run(t, "-root", tr.root, "-allow-stale")
	doc := tr.read(t, "ADR-014-memory-engine-internals.md")
	if !strings.Contains(doc, "profile \"quick\", which decides nothing") {
		t.Errorf("the record does not say the numbers came from a profile that decides nothing:\n%s", doc)
	}
}

// The families of one ADR measured at different commits are what a campaign
// interrupted and restarted looks like from here, and the record says so.
func TestADRNamesAMixedCommitCampaign(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "A1", 14, "queue", "", "Memory lane queue kinds (A1)")
	tr.env(t, "A2", 14, "arch", "./internal/engine/auto_arch.go", "Fan-out storage architecture (A2)")
	tr.registryRows(t, "14\tmemory-engine-internals\t-\tMemory engine internals (A1, A2)")
	tr.scorecard(t, "A1", "mpmc", "queue", "./internal/queue/auto.go")
	tr.scorecard(t, "A2", "persubenvelopes", "arch", "./internal/engine/auto_arch.go")
	patchCard(t, tr, "A2", func(sc *scorecard) { sc.Commit = "0123456789abcdef" })
	tr.run(t, "-root", tr.root)
	doc := tr.read(t, "ADR-014-memory-engine-internals.md")
	if !strings.Contains(doc, "not all measured on one tree") {
		t.Errorf("the record does not name the commit spread:\n%s", doc)
	}
}

// The record cites §9.4, so it must say which parts of §9.4 the campaign did
// not implement: `authoritative: no` says the numbers are provisional, not
// which stages of the protocol were skipped.
func TestADRNamesTheProtocolItDidNotImplement(t *testing.T) {
	tr := newADRTree(t)
	tr.env(t, "A1", 14, "queue", "", "Memory lane queue kinds (A1)")
	tr.registryRows(t, "14\tmemory-engine-internals\t-\tMemory engine internals (A1)")
	tr.scorecard(t, "A1", "mpmc", "queue", "./internal/queue/auto.go")
	tr.run(t, "-root", tr.root)
	doc := tr.read(t, "ADR-014-memory-engine-internals.md")
	for _, want := range []string{
		"Protocol coverage",
		"one architecture, not two",
		"measured on amd64",
		"closed-loop only",
		"no stress stage",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("the record does not say %q", want)
		}
	}
}
