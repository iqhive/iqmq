package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestRawResultsPathIsRepositoryRelative pins the renderer half of assumption
// B11.3: `hack/bakeoff.sh` hands `decide` an absolute `-raw`, and the variant
// page's "Raw results" line used to carry it verbatim.
func TestRawResultsPathIsRepositoryRelative(t *testing.T) {
	root := t.TempDir()
	// A linked worktree's .git is a FILE, not a directory, which is why the
	// walk tests with os.Stat rather than looking for a directory.
	if err := os.WriteFile(filepath.Join(root, ".git"), []byte("gitdir: /elsewhere\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runDir := filepath.Join(root, "benchmarks", "reports", "results", "20260915T000000Z-B11")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatal(err)
	}

	got := repoRelative(runDir)
	const want = "benchmarks/reports/results/20260915T000000Z-B11"
	if got != want {
		t.Errorf("repoRelative(%q) = %q, want %q", runDir, got, want)
	}
	if filepath.IsAbs(got) {
		t.Errorf("the rendered path is absolute: %q", got)
	}
}

func TestRepoRelativeLeavesAPathWithNoRepositoryAboveItAlone(t *testing.T) {
	dir := t.TempDir() // no .git anywhere above a temp dir
	if got := repoRelative(dir); got != filepath.ToSlash(dir) {
		t.Errorf("repoRelative(%q) = %q, want it unchanged", dir, got)
	}
	if repoRelative("") != "" {
		t.Error("repoRelative(\"\") should stay empty")
	}
}
