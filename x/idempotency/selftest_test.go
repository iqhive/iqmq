package idempotency_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"bitbucket.org/iqhive/iqmq/x/idempotency/storetest"
)

// defectEnv names the defect the child process injects. It is read by
// TestTheContractAgainstABrokenStore, which is the subject rather than a test.
const defectEnv = "IQMQ_STORETEST_DEFECT"

// TestTheStoreContractCanFail is the check on the check.
//
// x/idempotency/storetest is the instrument both stores are measured with, so
// it is part of the code under test: a suite that passed a wrong store would
// make the Redis store's green a statement about nothing. This runs the whole
// suite against a store carrying each defect in turn and requires it to FAIL,
// and against the correct one and requires it to pass.
//
// It works by re-executing this test binary, because storetest.Run drives
// *testing.T directly: a failure inside it fails the process, and there is no
// way to observe that from within the same process. os.Args[0] with
// -test.run is the standard shape (it is what os/exec's own tests do), and the
// child is the one place a deliberate failure is allowed.
func TestTheStoreContractCanFail(t *testing.T) {
	if os.Getenv(defectEnv) != "" {
		t.Skip("this process IS the child; TestTheContractAgainstABrokenStore is the subject")
	}
	for _, d := range storetest.Defects() {
		t.Run(string(d), func(t *testing.T) {
			out, err := runChild(t, string(d))
			passed := err == nil
			wantPass := d == storetest.DefectNone
			if passed != wantPass {
				verb := "FAILED"
				if passed {
					verb = "PASSED"
				}
				t.Fatalf("the contract %s against the store whose defect is %q, want the opposite.\n%s", verb, d, out)
			}
			if wantPass {
				return
			}
			// A failure is not enough: it has to be the RIGHT failure. A
			// suite that failed every defect because its harness could not
			// build a store would look identical here.
			if !strings.Contains(out, "--- FAIL: TestTheContractAgainstABrokenStore/"+string(d)+"/") {
				t.Fatalf("the run failed, but no case of the %q suite is named in the output, so the failure may not be the defect's:\n%s", d, out)
			}
		})
	}
}

// runChild runs this binary's TestTheContractAgainstABrokenStore with defect
// injected, and returns its output and exit status.
func runChild(t *testing.T, defect string) (string, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0],
		"-test.run=^TestTheContractAgainstABrokenStore$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), defectEnv+"="+defect)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestTheContractAgainstABrokenStore is the child process's whole job.
//
// It is skipped in an ordinary run — without the environment variable there is
// no defect to inject and a deliberately failing test in `make ci` would be a
// red build — and the skip is safe because TestTheStoreContractCanFail is what
// runs it, cannot be skipped, and fails if this one's verdict is wrong.
func TestTheContractAgainstABrokenStore(t *testing.T) {
	defect := os.Getenv(defectEnv)
	if defect == "" {
		t.Skip(defectEnv + " is unset: TestTheStoreContractCanFail is what runs this, in a child process, once per defect")
	}
	storetest.Run(t, storetest.BrokenHarness(storetest.Defect(defect)))
}
