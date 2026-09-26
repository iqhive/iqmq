package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/memory"
)

// harness is one CLI invocation's environment and the streams it wrote.
type harness struct {
	env *cliEnv
	out bytes.Buffer
	err bytes.Buffer
}

// stdout and stderr are what the invocation printed.
func (h *harness) stdout() string { return h.out.String() }
func (h *harness) stderr() string { return h.err.String() }

// newHarness builds an environment whose registry holds the memory driver
// alone, which is every test that needs a broker without needing a container.
func newHarness(t *testing.T) *harness {
	t.Helper()
	reg := dsn.NewRegistry()
	if err := memory.Register(reg); err != nil {
		t.Fatalf("registering the memory driver: %v", err)
	}
	return harnessWith(t, reg, nil)
}

// newFullHarness builds an environment with every driver this binary
// registers, for the tests that are about the driver table rather than about a
// broker.
func newFullHarness(t *testing.T) *harness {
	t.Helper()
	reg, err := newRegistry()
	if err != nil {
		t.Fatalf("newRegistry: %v", err)
	}
	return harnessWith(t, reg, nil)
}

// newEmptyHarness builds an environment with NO driver registered. It is what
// FuzzCLIArgs drives: every DSN then fails at the registry, so the fuzz target
// never performs network I/O however inventive the argument vector is.
func newEmptyHarness(t *testing.T) *harness {
	t.Helper()
	return harnessWith(t, dsn.NewRegistry(), nil)
}

// harnessWith is the common constructor.
func harnessWith(t *testing.T, reg *dsn.Registry, env map[string]string) *harness {
	t.Helper()
	h := &harness{}
	h.env = &cliEnv{
		stdout: &h.out, stderr: &h.err, stdin: strings.NewReader(""),
		registry: reg,
		getenv: func(k string) (string, bool) {
			v, ok := env[k]
			return v, ok
		},
		clock: realClock{},
		ctx:   context.Background(),
	}
	return h
}

// withEnv replaces the environment the invocation reads. No test mutates the
// process environment: the third place a credential can appear is an
// environment variable, and a test that set one for real would leak it into
// every other test in the binary.
func (h *harness) withEnv(env map[string]string) *harness {
	h.env.getenv = func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	}
	return h
}

// withStdin gives the invocation an input.
func (h *harness) withStdin(s string) *harness {
	h.env.stdin = strings.NewReader(s)
	return h
}

// withClock replaces the time source.
func (h *harness) withClock(c mq.Clock) *harness {
	h.env.clock = c
	return h
}

// run dispatches one argument vector.
func (h *harness) run(args ...string) int { return dispatch(h.env, args) }

// firedClock is a clock on which the timers a test NAMES have already fired,
// and no others.
//
// It is how the specification's "fake clocks instead of sleeps" reaches a CLI timeout
// test. A test for "--idle expired" needs the idle timer to fire after Receive
// has blocked, and a real clock makes that a race against a sleep; a timer
// whose channel is already loaded makes the expiry the first thing the select
// sees, so the test is deterministic and takes no time at all.
//
// The durations are named because firing EVERY timer made this double the
// cause of a load-sensitive failure rather than the cure for one. A command
// arms more timers than the one a test is about: `tail` opens its capability
// probe under `deadline(e.ctx, 10*time.Second)`, so a clock that fired every
// duration cancelled that context too, and whether `registry.Open` finished
// first was a scheduling race. On an idle host the in-process driver won it
// every time; at a load average of 44 it did not, and `make ci` reported
//
//	tail --subscription = 1: iqmq tail: opening shared://: context canceled
//
// which names neither the timer nor the double. A test
// that says which timer it means cannot cancel a context it was not thinking
// about, and the ten-second probe is simply left running for the length of
// the test, which is where a real clock would leave it.
type firedClock struct {
	now time.Time
	// fire is the set of durations whose timers are already loaded. It is
	// never nil for a clock built by firedFor, and NewTimer refuses a
	// zero value rather than guessing: "fire everything" is the behaviour
	// this field exists to remove.
	fire map[time.Duration]bool
}

// firedFor is a [firedClock] whose timers for ds have already fired. Every
// other duration gets a timer that never fires.
//
// A test names the duration it passed on the command line -- `--idle 1s` is
// `firedFor(now, time.Second)`.
func firedFor(now time.Time, ds ...time.Duration) firedClock {
	if len(ds) == 0 {
		panic("firedFor with no duration: name the timer the test is about, " +
			"or use a clock that fires nothing")
	}
	fire := make(map[time.Duration]bool, len(ds))
	for _, d := range ds {
		fire[d] = true
	}
	return firedClock{now: now, fire: fire}
}

// Now returns the fixed time.
func (c firedClock) Now() time.Time { return c.now }

// NewTimer returns a timer that has already fired if d was named, and one
// that never fires otherwise.
func (c firedClock) NewTimer(d time.Duration) mq.Timer {
	if c.fire == nil {
		panic("firedClock built as a literal: use firedFor, which names the " +
			"timers the test is about")
	}
	ch := make(chan time.Time, 1)
	if c.fire[d] {
		ch <- c.now
	}
	return firedTimer{ch: ch, fires: c.fire[d]}
}

// firedTimer is firedClock's timer. It has either already fired or will never
// fire, and it says which.
type firedTimer struct {
	ch    chan time.Time
	fires bool
}

// C returns the channel, loaded if this timer is one the test named.
func (t firedTimer) C() <-chan time.Time { return t.ch }

// Stop reports false for a timer that had already fired and true for one that
// had not, which is what time.Timer.Stop means.
func (t firedTimer) Stop() bool { return !t.fires }

// Reset reloads the channel for a timer the test named, so a second wait fires
// too, and leaves a timer it did not name alone.
func (t firedTimer) Reset(time.Duration) bool {
	if !t.fires {
		return true
	}
	select {
	case t.ch <- time.Time{}:
	default:
	}
	return false
}

// steppedClock is a clock that advances by a fixed step on every read, so a
// duration measured across two reads is exact and a test can assert on it.
// Its timers never fire.
type steppedClock struct {
	now  time.Time
	step time.Duration
}

// Now advances and returns.
func (c *steppedClock) Now() time.Time {
	c.now = c.now.Add(c.step)
	return c.now
}

// NewTimer returns a timer that never fires.
func (c *steppedClock) NewTimer(time.Duration) mq.Timer {
	return firedTimer{ch: make(chan time.Time, 1)}
}

// memDSN is the memory target every broker-backed test uses.
const memDSN = "memory://?autocreate=true"

// repoRoot is the repository root from this package, for the tests that hold
// the CLI to a generated artefact.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolving the repository root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("../.. is not the repository root: %v", err)
	}
	return root
}

// sharedDSN is the scheme newSharedHarness registers. It is not a driver's
// scheme: the whole point is that two invocations reach ONE broker, which
// memory:// deliberately cannot do.
const sharedDSN = "shared://"

// newSharedHarness registers a scheme whose opener hands out clients over one
// borrowed broker, so that a `pub` invocation and a `sub` invocation in the
// same test see the same messages.
//
// It exists because memory.Open creates a NEW broker per call -- the specification forbids
// global state, so there is nowhere for a second process's broker to be -- and
// therefore `iqmq pub memory:// t x` followed by `iqmq sub memory:// t s` sees
// nothing. That is correct behaviour and a real trap for a CLI user, which is
// why the commands print a note for a memory target; here it means a
// round-trip test has to share a broker explicitly, the way
// memory.OpenWithClient's own doc comment describes.
func newSharedHarness(t *testing.T) (*harness, *memory.Broker) {
	t.Helper()
	return sharedHarness(t, true)
}

// newSharedHarnessNoDeadLetter is newSharedHarness with no dead-letter
// destination, which is what makes a Reject refusal reachable.
func newSharedHarnessNoDeadLetter(t *testing.T) (*harness, *memory.Broker) {
	t.Helper()
	return sharedHarness(t, false)
}

// sharedHarness is the two constructors' body.
func sharedHarness(t *testing.T, deadLetter bool) (*harness, *memory.Broker) {
	t.Helper()
	// A STREAM topology with retention, because the round trip publishes
	// before the subscription exists. A queue or a topic fans out at
	// publication time, so a subscription created afterwards is empty and
	// --start earliest has nothing to be earliest OF -- which is a fact about
	// messaging and not a quirk of this driver, and is why the CLI's
	// --start flag offers only the two portable positions.
	broker, err := memory.NewBroker(memory.Config{
		AutoCreate: true,
		DefaultTopology: memory.TopologyConfig{
			Kind:      memory.TopologyStream,
			Retention: memory.Retention{MaxMessages: 1024},
			// Delivery.Reject is Nack(NackActionDeadLetter) -- the specification makes
			// "destroy this" explicit -- so `--settle reject` needs a
			// dead-letter destination and is ErrUnsupported without one.
			// TestSubSettlementRefusedAsUnsupported covers the other half.
			DeadLetterTopic: deadLetterTopic(deadLetter),
		},
	})
	if err != nil {
		t.Fatalf("memory.NewBroker: %v", err)
	}
	t.Cleanup(func() { _ = broker.Close() })

	// The dead-letter topic has to EXIST. memory's settler looks it up and
	// refuses when it does not, because "the engine never creates one behind
	// the operator's back" -- so AutoCreate does not conjure it and
	// `--settle reject` was ErrUnsupported with the topology naming it.
	// That is the driver being careful, and it is a thing a CLI user meets:
	// naming a dead-letter destination is not the same as having one.
	if deadLetter {
		setup, serr := memory.OpenWithClient(context.Background(), broker, memory.ClientBorrowed)
		if serr != nil {
			t.Fatalf("opening a client over the shared broker: %v", serr)
		}
		admin, aerr := setup.Admin()
		if aerr != nil {
			t.Fatalf("the memory broker has no admin surface: %v", aerr)
		}
		if eerr := admin.EnsureTopic(context.Background(), mq.TopicSpec{Name: "dead-letters"}); eerr != nil {
			t.Fatalf("creating the dead-letter topic: %v", eerr)
		}
		if cerr := setup.Close(); cerr != nil {
			t.Fatalf("closing the setup client: %v", cerr)
		}
	}

	reg := dsn.NewRegistry()
	err = reg.Register("shared", func(ctx context.Context, _ string) (*mq.Client, error) {
		return memory.OpenWithClient(ctx, broker, memory.ClientBorrowed)
	})
	if err != nil {
		t.Fatalf("registering the shared scheme: %v", err)
	}
	return harnessWith(t, reg, nil), broker
}

// deadLetterTopic names the dead-letter destination, or none.
func deadLetterTopic(want bool) string {
	if want {
		return "dead-letters"
	}
	return ""
}
