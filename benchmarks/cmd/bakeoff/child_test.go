package main

import (
	"bytes"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
)

// These are the tests for the defect child.go describes: `make bench-lint`
// could be ended from outside and the outcome was indistinguishable from
// success -- in a log, always, and in the lint's own logic for one class of
// package.
//
// Every case runs a REAL child and reads a real *exec.ExitError, because the
// classification turns on `ProcessState.ExitCode() == -1`, which is the one
// part of this a hand-built error would not exercise.

// signalledChild runs a child that prints out and then kills itself, which is
// the closest deterministic stand-in for the SIGTERM that reproduced the
// defect.
func signalledChild(t *testing.T, print string) ([]byte, error) {
	t.Helper()
	out, err := exec.Command("sh", "-c", fmt.Sprintf("printf '%%s' %q; kill -TERM $$", print)).CombinedOutput()
	if err == nil {
		t.Fatalf("the child was expected to die by signal; it exited cleanly with %q", out)
	}
	if !signalled(err) {
		t.Fatalf("signalled(%v) = false, want true (exit code should be -1 for a signalled child)", err)
	}
	return out, err
}

func TestASignalledChildIsNotReadAsAPackageWithNoTests(t *testing.T) {
	// This is the defect exactly. `go test -run '^TestKindsList$' PKG` on a
	// package without that test has its test binary print the warning below
	// before `go test` prints anything of its own, so a kill landing after
	// that point leaves BOTH the "no tests to run" text and a signalled exit
	// -- and the old code asked about the text first and skipped the package
	// in silence.
	const warning = "testing: warning: no tests to run\n"
	out, err := signalledChild(t, warning)

	if !noSuchTest(out) {
		t.Fatalf("noSuchTest(%q) = false; the fixture no longer reproduces the overlap this test is about", out)
	}
	got := childFailure("github.com/iqhive/iqmq/internal/backoff", "TestKindsList", out, err)
	if got == nil {
		t.Fatal("childFailure returned nil for a signalled child: the check would have passed having proved nothing")
	}
	if !aborted(got) {
		t.Fatalf("childFailure classified a signalled child whose output says %q as an ordinary failure: %v", warning, got)
	}
	var ab *abortedError
	if !errors.As(got, &ab) {
		t.Fatalf("errors.As(%v, **abortedError) = false", got)
	}
	if ab.pkg != "github.com/iqhive/iqmq/internal/backoff" || ab.what != "TestKindsList" {
		t.Errorf("the diagnostic names %q/%q, not the package and the job", ab.pkg, ab.what)
	}
	if msg := got.Error(); !strings.Contains(msg, "ENDED BY A SIGNAL") {
		t.Errorf("the message does not say the child was ended by a signal: %q", msg)
	}
}

func TestAChildThatChoseItsOwnExitStatusIsAnOrdinaryFailure(t *testing.T) {
	out, err := exec.Command("sh", "-c", "printf 'FAIL\\n'; exit 1").CombinedOutput()
	if err == nil {
		t.Fatal("the child was expected to exit 1")
	}
	if signalled(err) {
		t.Fatalf("signalled(%v) = true for a child that exited 1", err)
	}
	got := childFailure("pkg", "TestModel", out, err)
	if aborted(got) {
		t.Fatalf("a child that exited 1 was classified as aborted: %v", got)
	}
	if !strings.Contains(got.Error(), "pkg: TestModel:") {
		t.Errorf("the wrapped message lost the package and the job: %q", got)
	}
}

func TestSignalledIsFalseForEverythingThatIsNotASignalledChild(t *testing.T) {
	for _, err := range []error{nil, errors.New("plain"), fmt.Errorf("wrapped: %w", errors.New("plain"))} {
		if signalled(err) {
			t.Errorf("signalled(%v) = true", err)
		}
	}
}

// TestReportMarksEveryFailureAndSeparatesAnAbortFromAFinding is the half of
// assumption B11.1 that is about the LOG. "Green" here means a `make ci` log
// with no `FAIL` and no `!!` line in it, so a failure that prints neither is a
// red target that reads as a green log -- which is what the reproduction in
// child.go produced.
func TestReportMarksEveryFailureAndSeparatesAnAbortFromAFinding(t *testing.T) {
	if got := report(&bytes.Buffer{}, nil); got != 0 {
		t.Errorf("report(nil) = %d, want 0", got)
	}

	var buf bytes.Buffer
	if got := report(&buf, errors.New("3 problem(s) in 2 famil(y/ies)")); got != 1 {
		t.Errorf("report(a lint finding) = %d, want 1", got)
	}
	if !strings.HasPrefix(buf.String(), "!! ") {
		t.Errorf("a lint finding printed no !! marker: %q", buf.String())
	}

	buf.Reset()
	_, err := signalledChild(t, "")
	ab := childFailure("pkg", "benchmarks", nil, err)
	if got := report(&buf, ab); got != 2 {
		t.Errorf("report(an aborted check) = %d, want 2 (nothing was checked, which is not a finding)", got)
	}
	if !strings.HasPrefix(buf.String(), "!! ") {
		t.Errorf("an aborted check printed no !! marker: %q", buf.String())
	}
	if !strings.Contains(buf.String(), "DID NOT RUN") {
		t.Errorf("an aborted check does not say it proved nothing: %q", buf.String())
	}
}
