//go:build integration

package suites

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"go.uber.org/goleak"
)

// conformanceLeakIgnores is the goroutine-leak allow-list every conformance
// suite uses.
//
// goleak is on the DRIVER: a subscription goroutine, a parked Push or a
// flusher that outlives Close fails it. Everything ignored here belongs to the
// test harness or to a dependency's package `init()`, and none of it is
// anything a driver started or a Close can reach.
//
// It is ONE list because fourteen suites had grown their own copy of the first
// four entries, and the fifth -- OpenCensus -- was added to this helper by and reached only the two suites that already called it. The other
// seventeen conformance jobs then failed a leak check none of them could pass:
//
//	panic.go:694: found unexpected goroutines:
//	  go.opencensus.io/stats/view.(*worker).start(0xc0000d8a80)
//	  created by go.opencensus.io/stats/view.init.0 in goroutine 1
//
// [TestEverySuiteUsesTheSharedLeakIgnores] is what stops the copies coming
// back.
//
// extra is for an ignore that is genuinely one driver's client library:
// `drivers/gcppubsub` and `drivers/pulsar` each have one, and a list that
// carried them for every suite would ignore a real leak in the other twelve.
func conformanceLeakIgnores(extra ...goleak.Option) []goleak.Option {
	opts := make([]goleak.Option, 0, 5+len(extra))
	opts = append(opts,
		// testcontainers keeps a connection to its reaper for the life of
		// the process, and net/http keeps its idle connections.
		goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("internal/poll.runtime_pollWait"),
		// OpenCensus starts a worker from a package `init()` -- it is
		// reachable through the Docker client the container harness uses --
		// and nothing stops it, ever. It is not this driver's goroutine and
		// no Close can reach it, so ignoring it is the honest answer;
		// counting it makes every container suite in this repository fail a
		// leak check it cannot pass.
		goleak.IgnoreAnyFunction("go.opencensus.io/stats/view.(*worker).start"),
	)
	return append(opts, extra...)
}

// verifyNoneRe finds a goleak.VerifyNone call and captures what it was given.
var verifyNoneRe = regexp.MustCompile(`goleak\.VerifyNone\(\s*t,\s*([A-Za-z_][A-Za-z0-9_]*)`)

// anyVerifyNoneRe finds the call however it was written, so a suite that
// passes its ignores some other way is still counted rather than missed.
var anyVerifyNoneRe = regexp.MustCompile(`goleak\.VerifyNone\(`)

// TestEverySuiteUsesTheSharedLeakIgnores keeps the allow-list in one place.
//
// It is a source scan rather than a convention, because the convention was
// already in force and did not hold: [conformanceLeakIgnores] existed, two
// suites called it, and fourteen carried a copy of four of its five entries.
// Nothing said so until the fifth entry was added to the helper alone and the
// integration workflow went red on seventeen drivers at once -- a divergence
// that costs nothing until the day it costs everything.
func TestEverySuiteUsesTheSharedLeakIgnores(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the suite directory: %v", err)
	}
	calls := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, "_test.go") || name == "leak_test.go" {
			continue
		}
		src, err := os.ReadFile(filepath.Join(".", name))
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		text := string(src)
		total := len(anyVerifyNoneRe.FindAllString(text, -1))
		shared := 0
		for _, m := range verifyNoneRe.FindAllStringSubmatch(text, -1) {
			if m[1] == "conformanceLeakIgnores" {
				shared++
			}
		}
		if total != shared {
			t.Errorf("%s: %d of %d goleak.VerifyNone call(s) do not pass conformanceLeakIgnores(). A "+
				"suite with its own list drifts silently: the OpenCensus ignore lived in the shared "+
				"helper for a whole increment while fourteen suites could not see it, and every one of "+
				"them failed a leak check for a goroutine no driver started", name, total-shared, total)
		}
		calls += total
	}
	if calls < 14 {
		t.Errorf("only %d goleak.VerifyNone call(s) were found across the suite files; this check reads "+
			"the source, so a rename or a move makes it pass by finding nothing", calls)
	}
}
