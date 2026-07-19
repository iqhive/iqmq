package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// writeFile creates path with content, making its directory first.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// names lists the file names in dir.
func names(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// fakeRepo builds a one-module tree holding one fuzz target and returns the
// root, the target's corpus directory and a GOCACHE whose fuzz cache holds
// cacheFiles inputs for it.
func fakeRepo(t *testing.T, committed, cacheFiles []string) (root, corpus, cache string) {
	t.Helper()
	root = t.TempDir()
	writeFile(t, filepath.Join(root, "go.mod"), "module example.com/m\n\ngo 1.26\n")
	writeFile(t, filepath.Join(root, "p", "fuzz_test.go"), "package p\n\nimport \"testing\"\n\nfunc FuzzThing(f *testing.F) {}\n")
	corpus = filepath.Join(root, "p", "testdata", "fuzz", "FuzzThing")
	for _, n := range committed {
		writeFile(t, filepath.Join(corpus, n), "committed "+n)
	}
	cache = t.TempDir()
	for _, n := range cacheFiles {
		writeFile(t, filepath.Join(cache, "fuzz", "example.com", "m", "p", "FuzzThing", n), "cached "+n)
	}
	return root, corpus, cache
}

// TestExportNeverRemovesAnExistingInput pins the rule assumption A25.1 records:
// export writes new inputs and removes nothing, because the committed corpus is
// the regression corpus the test suite requires a fix to commit its seed
// to. The tool used to copy the whole Go fuzz cache in and then trim the union
// to -max keeping the smallest, which deleted 36 committed inputs in this
// repository, among them internal/engine's FuzzEngineModel seeds.
func TestExportNeverRemovesAnExistingInput(t *testing.T) {
	root, corpus, cache := fakeRepo(t,
		[]string{"keep-a", "keep-b", "keep-c"},
		[]string{"new-1", "new-2"})
	t.Setenv("GOCACHE", cache)

	// -max 3 is already reached by the committed three, so nothing is copied
	// and — the point of the test — nothing is deleted either.
	if err := export(root, 3, false); err != nil {
		t.Fatal(err)
	}
	if got, want := len(names(t, corpus)), 3; got != want {
		t.Fatalf("corpus holds %d input(s) after a full export, want %d: %v", got, want, names(t, corpus))
	}
	for _, n := range []string{"keep-a", "keep-b", "keep-c"} {
		if _, err := os.Stat(filepath.Join(corpus, n)); err != nil {
			t.Errorf("export removed the committed input %s: %v", n, err)
		}
	}

	// With room for two more, both cache inputs arrive and the committed
	// three stay.
	if err := export(root, 5, false); err != nil {
		t.Fatal(err)
	}
	if got, want := len(names(t, corpus)), 5; got != want {
		t.Fatalf("corpus holds %d input(s), want %d: %v", got, want, names(t, corpus))
	}
}

// TestExportStopsAtMax holds export to the -max the plan's §11.3 notes give it
// even when the cache offers far more.
func TestExportStopsAtMax(t *testing.T) {
	cached := make([]string, 0, 40)
	for i := range 40 {
		cached = append(cached, string(rune('a'+i/10))+string(rune('0'+i%10)))
	}
	root, corpus, cache := fakeRepo(t, nil, cached)
	t.Setenv("GOCACHE", cache)
	if err := export(root, 7, false); err != nil {
		t.Fatal(err)
	}
	if got, want := len(names(t, corpus)), 7; got != want {
		t.Fatalf("export wrote %d input(s) under -max %d", got, want)
	}
}

// TestTrimRefusesToDeleteATrackedInput holds the opt-in -prune path to the same
// rule: it may delete what git does not track and nothing else.
func TestTrimRefusesToDeleteATrackedInput(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	dir := t.TempDir()
	for _, arg := range [][]string{{"init", "-q"}, {"config", "user.email", "t@example.com"}, {"config", "user.name", "t"}} {
		cmd := exec.Command("git", arg...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", arg, err, out)
		}
	}
	corpus := filepath.Join(dir, "testdata", "fuzz", "FuzzThing")
	// The tracked inputs are the large ones, which is the case that lost data:
	// a minimised crasher is often bigger than a random interesting input, and
	// the trim kept the smallest.
	for _, n := range []string{"tracked-1", "tracked-2"} {
		writeFile(t, filepath.Join(corpus, n), strings.Repeat("x", 512))
	}
	for _, arg := range [][]string{{"add", "-A"}, {"commit", "-qm", "corpus"}} {
		cmd := exec.Command("git", arg...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", arg, err, out)
		}
	}
	for _, n := range []string{"loose-1", "loose-2", "loose-3"} {
		writeFile(t, filepath.Join(corpus, n), "x")
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })

	removed, err := trim(filepath.Join("testdata", "fuzz", "FuzzThing"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Errorf("trim removed %d input(s), want the 2 untracked ones above -max", removed)
	}
	for _, n := range []string{"tracked-1", "tracked-2"} {
		if _, err := os.Stat(filepath.Join(corpus, n)); err != nil {
			t.Errorf("trim deleted the tracked input %s: %v", n, err)
		}
	}
}

// TestTrimRefusesWithoutGit pins the other half: when git cannot say what is
// committed, nothing may be deleted.
func TestTrimRefusesWithoutGit(t *testing.T) {
	dir := t.TempDir()
	corpus := filepath.Join(dir, "corpus")
	for _, n := range []string{"a", "b", "c"} {
		writeFile(t, filepath.Join(corpus, n), n)
	}
	t.Setenv("PATH", t.TempDir())
	if _, err := trim(corpus, 1); err == nil {
		t.Fatal("trim pruned a corpus with no git to ask which inputs are committed")
	}
	if got := len(names(t, corpus)); got != 3 {
		t.Fatalf("trim removed %d input(s) after refusing", 3-got)
	}
}
