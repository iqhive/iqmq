package main

import (
	"os"
	"path/filepath"
	"testing"
)

// writeTestFile writes body to root/rel, making its directories.
func writeTestFile(t *testing.T, root, rel, body string) {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestATestOnlyAnAgentWorktreeDefinesIsNotFound keeps a citation of a deleted
// test from resolving to a stale copy. An agent's worktree under
// .claude/worktrees is a whole second checkout, so the trace table would cite a
// renamed test and still pass for as long as an old worktree kept its copy.
func TestATestOnlyAnAgentWorktreeDefinesIsNotFound(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "internal/engine/fanout_test.go",
		"package engine\n\nimport \"testing\"\n\nfunc FuzzEngineFanoutRenamed(f *testing.F) {}\n")
	writeTestFile(t, root, ".claude/worktrees/stale-agent/internal/engine/fanout_test.go",
		"package engine\n\nimport \"testing\"\n\nfunc FuzzEngineFanout(f *testing.F) {}\n")

	got, err := testNames(root)
	if err != nil {
		t.Fatal(err)
	}
	if !got["FuzzEngineFanoutRenamed"] {
		t.Errorf("the tree's own FuzzEngineFanoutRenamed was not found; got %v", got)
	}
	if got["FuzzEngineFanout"] {
		t.Errorf("FuzzEngineFanout, defined only under .claude/worktrees, was found: a deleted test's citation would pass")
	}
}
