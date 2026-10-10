package main

import (
	"bytes"
	"errors"
	"fmt"
	"os/exec"
)

// A lint check runs its work in child `go test` processes, and the thing that
// used to be indistinguishable from success was one of those children being
// ended from OUTSIDE this process.
//
// Watched: `hack/bench-lint` was started, a descendant `go test` was found by
// walking /proc/<pid>/task/*/children from the pid the shell started, and one
// SIGTERM produced exactly this and nothing else:
//
//	bakeoff: github.com/iqhive/iqmq/internal/backoff: TestKindsList: signal: terminated
//
//	exit status 1
//
// Two things are wrong with that, and the second is worse than the first.
//
// It carries no `FAIL` line and no `!!` line, so a reader (or a grep) scanning
// a `make ci` log for the two markers this repository uses sees a clean log for
// a check that did not run. `make` flattens every failure to its own exit
// status 2, so the status does not say which target died either.
//
// And `loadKinds` swallowed it outright for one class of package. Its error
// branch asked whether the output mentioned "no test files" or "no tests to
// run" BEFORE it asked what had happened to the child, and a `go test` whose
// test binary has already printed `testing: warning: no tests to run` -- which
// every package without the named test prints, before `go test` prints its own
// `ok` line -- was then read as "this package is not a variant family" and
// skipped in silence. A kill landing in that window removed a family from the
// check with no diagnostic at all. That is a check that cannot fail, in the
// worst direction: the more of the tree that is killed, the greener it looks.
//
// So a signalled child is its own kind of failure here: it is not an answer
// about the tree, it is the absence of an answer, and it is reported with the
// `!!` marker and exit status 2 (a setup error, the same status
// hack/benchgate.sh uses for "nothing could be measured") rather than with the
// exit status 1 that means "the lint found a problem". Recorded as assumption
// B11.1.

// abortedError reports that a child process did not finish because something
// outside this process ended it. It is deliberately NOT a lint problem: the
// check proved nothing and has to be run again.
type abortedError struct {
	pkg  string // the package the child was working on
	what string // the child's job, for the diagnostic ("TestModel", "benchmarks")
	err  error  // the child's own error, for errors.As / Unwrap
	out  []byte // whatever the child had printed before it died
}

func (e *abortedError) Error() string {
	msg := fmt.Sprintf("%s: %s: the child `go test` was ENDED BY A SIGNAL (%v), so this check did not run", e.pkg, e.what, e.err)
	if out := bytes.TrimSpace(e.out); len(out) > 0 {
		msg += "\n" + string(out)
	}
	return msg
}

func (e *abortedError) Unwrap() error { return e.err }

// signalled reports whether err is a child that a signal ended rather than a
// child that chose its own exit status. Go reports the first as exit code -1,
// which is not a status any process can exit with.
func signalled(err error) bool {
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		return false
	}
	ps := ee.ProcessState
	return ps != nil && ps.ExitCode() == -1
}

// noSuchTest reports whether a child's output says the package has no such
// test rather than that the test failed. `go test -run '^TestX$'` on a package
// without TestX exits 0 and prints "no tests to run"; a package with no test
// files at all prints "no test files".
func noSuchTest(out []byte) bool {
	return bytes.Contains(out, []byte("no test files")) || bytes.Contains(out, []byte("no tests to run"))
}

// childFailure turns a child's own failure into the error the caller reports.
// It never returns nil: a caller reaches it only when the child failed.
//
// The classification's ORDER is the fix. "Was this child killed" is asked
// before any question about what the child PRINTED, because the output of a
// child that was killed is a partial output, and the sentence a caller reads
// for "this package has no such test" is printed by the test binary early --
// long before `go test` decides anything.
func childFailure(pkg, what string, out []byte, err error) error {
	if signalled(err) {
		return &abortedError{pkg: pkg, what: what, err: err, out: out}
	}
	return fmt.Errorf("%s: %s: %w\n%s", pkg, what, err, out)
}

// aborted reports whether err is, or wraps, a child this process did not get
// an answer from.
func aborted(err error) bool {
	var ab *abortedError
	return errors.As(err, &ab)
}
