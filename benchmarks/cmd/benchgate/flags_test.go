package main

// The first tests in this package, and they are about the ARGUMENT SURFACE
// rather than about any verdict: `benchgate`'s two path flags name two
// differently-typed things -- `-results` a stored campaign DIRECTORY, `-run`
// ONE run's raw benchmark output -- and until assumption C17.1 both were the
// same flag. Everything below exists because the split has two failure modes
// that no exit status of its own would show:
//
//   - a caller left passing the old flag. A caller may run
//     `benchgate -ratios` under `|| true` and read only the `Breaches:` line it
//     printed, so an undefined flag there would have lost the gate in silence.
//   - a path argument that is ACCEPTED and then goes nowhere, which is how a
//     gate becomes a check that passes by finding nothing.
//
// So the wrong-way-round forms are asserted to fail BY NAME, and the two
// guards this increment must not weaken -- `-ratios` over a gate set no ratio
// budget names  and `-lint` over a directory with no measured
// names -- are asserted to still fail through the new flag.

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The two arms of the self-test's batched ratio budget and of its
// single-message one, spelled as a Go benchmark spells them (with the
// GOMAXPROCS suffix `benchfmt` strips). `selfTestBudgets` declares both over
// the `ratio` gate set, so a file holding these four names is the ONE input
// shape a `kind: ratio` budget can be evaluated over.
const (
	testBatchNum = "BenchmarkDriver/fixture/PublishBatch/64B-16"
	testBatchDen = "BenchmarkNative/fixture/PublishBatch/64B-16"
	testPubNum   = "BenchmarkDriver/fixture/Publish/64B-16"
	testPubDen   = "BenchmarkNative/fixture/Publish/64B-16"
)

// bothArms is one run's raw.txt: both arms of both ratio rows, in one file.
// Identical arms lose no throughput, so both ceilings are met and the verdict
// is a PASS over a table that is not empty -- which is what makes it usable as
// the positive case for "did -run actually reach the reader".
func bothArms() string {
	return fixture(testBatchNum, 1000, 0) + fixture(testBatchDen, 1000, 0) +
		fixture(testPubNum, 1000, 0) + fixture(testPubDen, 1000, 0)
}

// budgetsFile writes the self-test's own budgets document to a temp file. It is
// the fixture the `-self-test` cases are judged against, so a test that uses it
// is asking about the flag wiring and not about `benchmarks/budgets.yaml`.
func budgetsFile(t *testing.T, dir string) string {
	t.Helper()
	p := filepath.Join(dir, "budgets.yaml")
	if err := os.WriteFile(p, []byte(selfTestBudgets), 0o644); err != nil {
		t.Fatalf("write budgets: %v", err)
	}
	return p
}

func writeFile(t *testing.T, path, body string) string {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func writeGzip(t *testing.T, path, body string) string {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	zw := gzip.NewWriter(f)
	if _, err := zw.Write([]byte(body)); err != nil {
		t.Fatalf("gzip %s: %v", path, err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
	return path
}

// invoke runs the whole of benchgate's argument handling and returns its exit
// status with everything it wrote. `lint` and `runRatios` print to os.Stdout
// and os.Stderr directly, so both are redirected for the duration -- to FILES
// rather than to pipes, because a pipe nobody is draining deadlocks at 64 KiB
// and a verdict table has no bound. The same file is handed to `run` as its
// error stream, so a message lands in one place wherever it was written.
func invoke(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	dir := t.TempDir()
	outF, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatalf("stdout file: %v", err)
	}
	errF, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatalf("stderr file: %v", err)
	}
	savedOut, savedErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = outF, errF
	func() {
		defer func() { os.Stdout, os.Stderr = savedOut, savedErr }()
		code = run("benchgate", args, errF)
	}()
	_ = outF.Close()
	_ = errF.Close()
	ob, err := os.ReadFile(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	eb, err := os.ReadFile(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	return code, string(ob), string(eb)
}

// TestRatiosReadsTheRunFile is the positive half, and it is the one that stops
// every other test here from being satisfied by a flag that is parsed and
// dropped: the value of `-run` has to reach `readResults`, and the verdict has
// to name the path it read.
func TestRatiosReadsTheRunFile(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(t *testing.T, path, body string) string
		file  string
	}{
		{"raw.txt", writeFile, "raw.txt"},
		// The compressed input format accepted by `make overhead-gate`.
		{"raw.txt.gz", writeGzip, "raw.txt.gz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			rawPath := tc.write(t, filepath.Join(dir, tc.file), bothArms())
			verdict := filepath.Join(dir, "verdict.md")
			code, _, errOut := invoke(t,
				"-ratios", "-gate", "ratio", "-budgets", budgetsFile(t, dir),
				"-run", rawPath, "-out", verdict)
			if code != 0 {
				t.Fatalf("exit %d, want 0 (identical arms meet both ceilings); stderr:\n%s", code, errOut)
			}
			md, err := os.ReadFile(verdict)
			if err != nil {
				t.Fatalf("read verdict: %v", err)
			}
			if !strings.Contains(string(md), "Results: `"+rawPath+"`") {
				t.Errorf("verdict does not name the -run path %s:\n%s", rawPath, md)
			}
			// Two ratio rows compared, not a PASS over an empty table.
			if !strings.Contains(string(md), "Ratio rows: 2") {
				t.Errorf("verdict does not report 2 ratio rows:\n%s", md)
			}
		})
	}
}

// TestRatiosRequiresRun: `-ratios` with no `-run` is refused. There is
// deliberately no default, because the only default available was the
// directory-shaped one `-lint` needs, and a `-ratios` that quietly fell back to
// a directory is precisely the overload being removed.
func TestRatiosRequiresRun(t *testing.T) {
	dir := t.TempDir()
	code, _, errOut := invoke(t, "-ratios", "-gate", "ratio", "-budgets", budgetsFile(t, dir))
	if code != 2 {
		t.Fatalf("exit %d, want 2; stderr:\n%s", code, errOut)
	}
	if !strings.Contains(errOut, "-run is required") {
		t.Errorf("stderr does not say -run is required:\n%s", errOut)
	}
}

// TestRatiosRefusesResults: the old spelling. Every caller in the tree moved in
// the same commit, and a caller that did not has to be told, not ignored --
// Callers can parse the output of this invocation.
func TestRatiosRefusesResults(t *testing.T) {
	dir := t.TempDir()
	raw := writeFile(t, filepath.Join(dir, "raw.txt"), bothArms())
	code, _, errOut := invoke(t,
		"-ratios", "-gate", "ratio", "-budgets", budgetsFile(t, dir), "-results", raw)
	if code != 2 {
		t.Fatalf("exit %d, want 2; stderr:\n%s", code, errOut)
	}
	if !strings.Contains(errOut, "-results is the stored campaign DIRECTORY") ||
		!strings.Contains(errOut, "as -run") {
		t.Errorf("stderr does not name both flags and which is which:\n%s", errOut)
	}
}

// TestRunRefusedOutsideRatios: the mirror image, and the one that would
// otherwise be silent. `-lint` does not read `-run`, so accepting it would mean
// linting the DEFAULT directory while the caller believed it had named the
// input.
func TestRunRefusedOutsideRatios(t *testing.T) {
	dir := t.TempDir()
	raw := writeFile(t, filepath.Join(dir, "raw.txt"), bothArms())
	for _, args := range [][]string{
		{"-lint", "-budgets", budgetsFile(t, dir), "-run", raw},
		{"-base", raw, "-head", raw, "-run", raw},
		{"-self-test", "-run", raw},
	} {
		code, _, errOut := invoke(t, args...)
		if code != 2 {
			t.Errorf("%v: exit %d, want 2; stderr:\n%s", args, code, errOut)
			continue
		}
		if !strings.Contains(errOut, "-run names ONE run's raw.txt") ||
			!strings.Contains(errOut, "is -results") {
			t.Errorf("%v: stderr does not name both flags and which is which:\n%s", args, errOut)
		}
	}
}

// TestLintResultsDefaultIsADirectory is the guard on the constraint that has no
// other witness: `make budgets-lint` runs `benchgate -lint` from `benchmarks/`
// with NO -results, so the default has to remain a directory that exists there.
// Repointing it at a file -- the obvious way to "split" the flag -- would leave
// `-lint` with nothing to inventory and break a step of `make ci`, and the
// break would be in a Makefile rather than in this package.
func TestLintResultsDefaultIsADirectory(t *testing.T) {
	if defaultRunFile != "" {
		t.Errorf("defaultRunFile = %q, want empty: -ratios names one run or is refused", defaultRunFile)
	}
	// This test runs in benchmarks/cmd/benchgate; `make budgets-lint` runs
	// benchgate in benchmarks/, which is ../.. from here.
	resolved := filepath.Join("..", "..", defaultResultsDir)
	st, err := os.Stat(resolved)
	if os.IsNotExist(err) {
		t.Log("results are generated locally when benchmarks run")
		return
	}
	if err != nil {
		t.Fatalf("the -results default %q does not exist relative to benchmarks/ (%s): %v", defaultResultsDir, resolved, err)
	}
	if !st.IsDir() {
		t.Fatalf("the -results default %q is not a directory (%s): make budgets-lint passes no -results", defaultResultsDir, resolved)
	}
}

// emptyDir is a directory that exists and holds nothing.
func emptyDir(t *testing.T, parent, name string) string {
	t.Helper()
	p := filepath.Join(parent, name)
	if err := os.Mkdir(p, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", p, err)
	}
	return p
}

// TestLintRefusesAFileAsResults: the other wrong-way-round form. It failed
// before the split and still fails, which is worth pinning because a `-lint`
// that accepted a file and found no run directories inside it would report
// nothing and pass.
func TestLintRefusesAFileAsResults(t *testing.T) {
	dir := t.TempDir()
	raw := writeFile(t, filepath.Join(dir, "raw.txt"), bothArms())
	code, _, errOut := invoke(t, "-lint", "-budgets", budgetsFile(t, dir),
		"-results", raw, "-scorecards", emptyDir(t, dir, "scorecards"))
	if code != 2 {
		t.Fatalf("exit %d, want 2; stderr:\n%s", code, errOut)
	}
	// Named, and named as the shape error it is: the inventory could not be
	// read at all, as against having been read and found empty.
	if !strings.Contains(errOut, "benchgate -lint:") || !strings.Contains(errOut, raw) ||
		!strings.Contains(errOut, "not a directory") {
		t.Errorf("exit 2 for the wrong reason:\n%s", errOut)
	}
}

// TestLintStillFailsOnAResultsDirectoryWithNoNames pins lint.go's guard
// through the flag this increment left alone: a results directory that holds no
// measured benchmark name is exit 2, because a pattern check against an empty
// inventory is a check that passes by finding nothing.
//
// -scorecards names an EMPTY DIRECTORY on purpose. Left at its default it does
// not exist from here, `nonDefaultArms` fails on it further down, and lint
// returns 2 for that reason instead -- which is how the first version of this
// test went on passing with the guard's `return 2` deleted. With the scorecards
// step satisfied, exit 2 can only come from the guard.
func TestLintStillFailsOnAResultsDirectoryWithNoNames(t *testing.T) {
	dir := t.TempDir()
	code, _, errOut := invoke(t, "-lint", "-budgets", budgetsFile(t, dir),
		"-results", emptyDir(t, dir, "results"), "-scorecards", emptyDir(t, dir, "scorecards"))
	if code != 2 {
		t.Fatalf("exit %d, want 2; stderr:\n%s", code, errOut)
	}
	if !strings.Contains(errOut, "holds no measured benchmark names") {
		t.Errorf("exit 2 for the wrong reason:\n%s", errOut)
	}
}

// TestRatiosStillFailsWhenNoRatioBudgetNamesTheGateSet pins ratio.go's guard
// through `-run`: the `pr` gate set of the self-test's budgets has no
// `kind: ratio` budget, so there is nothing to compare, and nothing to compare
// is exit 2 rather than a PASS over an empty table. Without
// this, a `-run` wired to a reader that returned no usable rows would look like
// a pass.
func TestRatiosStillFailsWhenNoRatioBudgetNamesTheGateSet(t *testing.T) {
	dir := t.TempDir()
	raw := writeFile(t, filepath.Join(dir, "raw.txt"), bothArms())
	code, _, errOut := invoke(t,
		"-ratios", "-gate", "pr", "-budgets", budgetsFile(t, dir), "-run", raw)
	if code != 2 {
		t.Fatalf("exit %d, want 2; stderr:\n%s", code, errOut)
	}
	if !strings.Contains(errOut, "no `kind: ratio` budget names gate set") {
		t.Errorf("exit 2 for the wrong reason:\n%s", errOut)
	}
}

// TestRatiosRunFileMustExist: a `-run` path nothing wrote is an error and not
// an empty comparison.
func TestRatiosRunFileMustExist(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "no-such-run", "raw.txt.gz")
	code, _, errOut := invoke(t,
		"-ratios", "-gate", "ratio", "-budgets", budgetsFile(t, dir), "-run", missing)
	if code != 2 {
		t.Fatalf("exit %d, want 2; stderr:\n%s", code, errOut)
	}
	if !strings.Contains(errOut, missing) {
		t.Errorf("stderr does not name the path it could not read:\n%s", errOut)
	}
}

// TestRatiosRefusesADirectoryAsRun: the slip the entry predicted -- a run
// DIRECTORY where its raw.txt.gz was meant. It has to keep failing loudly.
func TestRatiosRefusesADirectoryAsRun(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "20260912T000840Z-A3")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeFile(t, filepath.Join(runDir, "raw.txt"), bothArms())
	code, _, errOut := invoke(t,
		"-ratios", "-gate", "ratio", "-budgets", budgetsFile(t, dir), "-run", runDir)
	if code != 2 {
		t.Fatalf("exit %d, want 2; stderr:\n%s", code, errOut)
	}
	if !strings.Contains(errOut, runDir) {
		t.Errorf("stderr does not name the directory it was handed:\n%s", errOut)
	}
}

// TestUnknownFlagIsRefused checks that an unknown flag cannot be mistaken for
// a benchmark verdict by a caller parsing the command's output.
func TestUnknownFlagIsRefused(t *testing.T) {
	code, out, errOut := invoke(t, "-ratios", "-gate", "overhead", "-nosuchflag", "x")
	if code != 2 {
		t.Fatalf("exit %d, want 2; stderr:\n%s", code, errOut)
	}
	if strings.Contains(out+errOut, "Breaches:") {
		t.Errorf("an unknown flag printed a Breaches line, which a caller could read as a verdict:\n%s%s", out, errOut)
	}
	if !strings.Contains(errOut, "not defined") {
		t.Errorf("stderr does not report the undefined flag:\n%s", errOut)
	}
}
