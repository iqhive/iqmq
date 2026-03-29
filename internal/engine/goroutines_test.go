package engine

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestMain fails a test run that leaves an engine behind.
//
// Engine.Close stops the scheduler and waits for it (lifecycle StopFetch ->
// scheduler.close -> <-s.exited), so a parked scheduler goroutine at the end
// of a run is an engine nobody closed rather than a close that did not
// finish. One such engine costs nothing in a single run -- the process exits
// -- and it is invisible at -count=1, which is why the one this check was
// written for survived every run of this suite: TestQueueKindsAreMPMC built
// the engine its assertion needed and discarded it. At -count=20 that is
// twenty live engines and twenty timers, and the goroutine dump of a race
// loop that reached its ceiling carried eighteen of them, aged 1, 5, 9 ...
// 59 minutes -- one per iteration, getting slower as it went.
//
// It counts scheduler goroutines rather than all of them, because the total
// includes the test binary's own and the race detector's, and because the
// scheduler is the one goroutine an unclosed engine always has. A closed
// engine's is gone before Close returns, so the only reason to look twice is
// a Close that another goroutine started as the run ended; that is what the
// short retry is for.
//
// It is skipped for a benchmark or a fuzzing run, whose rigs deliberately
// outlive the function that built them, and whose measurements are not the
// place to discover this.
func TestMain(m *testing.M) {
	code := m.Run()
	if code == 0 && !measuring() {
		if n, dump := liveSchedulers(); n > 0 {
			fmt.Fprintf(os.Stderr, "\n%d engine scheduler goroutine(s) outlived this run: "+
				"an engine some test built and never closed. Close it (or give it a t.Cleanup); "+
				"New starts the scheduler and only Close stops it.\n%s\n", n, dump)
			code = 1
		}
	}
	os.Exit(code)
}

// measuring reports whether this run is a benchmark or a fuzzing run.
func measuring() bool {
	for _, name := range []string{"test.bench", "test.fuzz"} {
		if f := flag.Lookup(name); f != nil && f.Value.String() != "" {
			return true
		}
	}
	return false
}

// liveSchedulers counts the engine scheduler goroutines still running, and
// returns the stacks of the first few for the failure message.
func liveSchedulers() (int, string) {
	for attempt := range 20 {
		if attempt > 0 {
			// A Close another goroutine started as the run ended is still
			// entitled to finish.
			time.Sleep(10 * time.Millisecond)
		}
		buf := make([]byte, 1<<16)
		for {
			n := runtime.Stack(buf, true)
			if n < len(buf) {
				buf = buf[:n]
				break
			}
			buf = make([]byte, 2*len(buf))
		}
		var kept []string
		for _, g := range strings.Split(string(buf), "\n\n") {
			if strings.Contains(g, "engine.(*scheduler") && strings.Contains(g, ").run(") {
				kept = append(kept, g)
			}
		}
		if len(kept) == 0 {
			return 0, ""
		}
		if attempt == 19 {
			if len(kept) > 3 {
				return len(kept), strings.Join(kept[:3], "\n\n") + "\n\n(and more)"
			}
			return len(kept), strings.Join(kept, "\n\n")
		}
	}
	return 0, ""
}
