package main

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/memory"
)

// TestTailVerdictReadsTheCapabilities is the composition on its own: the specification has
// no "can be watched" feature, so the verdict is built from three, and each
// combination has to come out right.
func TestTailVerdictReadsTheCapabilities(t *testing.T) {
	caps := func(fanOut, explicit, atLeastOnce mq.Level) mq.Capabilities {
		var c mq.Capabilities
		for f, l := range map[mq.Feature]mq.Level{
			mq.FeatureFanOut:      fanOut,
			mq.FeatureExplicitAck: explicit,
			mq.FeatureAtLeastOnce: atLeastOnce,
		} {
			if err := c.Set(f, mq.Support{Level: l}); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
		return c
	}
	cases := []struct {
		name                  string
		fanOut, explicit, alo mq.Level
		wantOwnCopy           bool
		wantUnsett            bool
		wantNoSettlement      bool
	}{
		{"fan-out and at-least-once", mq.Native, mq.Native, mq.Native, true, true, false},
		{"no fan-out", mq.Unsupported, mq.Native, mq.Native, false, true, false},
		{"no settlement at all", mq.Native, mq.Unsupported, mq.Unsupported, true, true, true},
		{"at-most-once work queue", mq.Unsupported, mq.Native, mq.Unsupported, false, false, false},
		{"conditional fan-out still counts", mq.Conditional, mq.Native, mq.Native, true, true, false},
		{"advertises nothing", mq.Unsupported, mq.Unsupported, mq.Unsupported, false, true, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := verdictFor(caps(c.fanOut, c.explicit, c.alo))
			if v.ownCopy != c.wantOwnCopy {
				t.Errorf("ownCopy = %v, want %v", v.ownCopy, c.wantOwnCopy)
			}
			if v.leaveUnsettled != c.wantUnsett {
				t.Errorf("leaveUnsettled = %v, want %v", v.leaveUnsettled, c.wantUnsett)
			}
			// A driver with no settlement has no leases, so tail must not
			// say a delivery comes back when its lease expires. Against NATS
			// Core it did, which is what this field exists for.
			if v.noSettlement != c.wantNoSettlement {
				t.Errorf("noSettlement = %v, want %v", v.noSettlement, c.wantNoSettlement)
			}
			if len(v.why) != 3 {
				t.Errorf("the verdict records %d capability facts, want 3; a refusal has to "+
					"quote the levels it read rather than assert a conclusion", len(v.why))
			}
			for _, line := range v.why {
				if !strings.Contains(line, "=") {
					t.Errorf("the capability fact %q does not name a level", line)
				}
			}
		})
	}
}

// TestTailRefusesADriverThatCannotBeTailed is the refusal path and its exit
// code, driven through a registry whose driver advertises a work queue.
func TestTailRefusesADriverThatCannotBeTailed(t *testing.T) {
	h := workQueueHarness(t)
	code := h.run("tail", "workqueue://host", "orders")
	if code != exitUnsupported {
		t.Fatalf("tail against a driver with no fan-out = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			code, exitUnsupported, h.stdout(), h.stderr())
	}
	for _, want := range []string{
		"cannot be tailed without consuming",
		"FeatureFanOut",
		"property of the broker",
		"--consume",
		"iqmq sub",
	} {
		if !strings.Contains(h.stderr(), want) {
			t.Errorf("the refusal does not say %q. A capability fact stated as a tool "+
				"limitation is the defect exists to prevent:\n%s", want, h.stderr())
		}
	}
	// And the capability levels it read are on stdout, so the reader can check
	// the claim against the driver's own page.
	if !strings.Contains(h.stdout(), "FanOut=unsupported") {
		t.Errorf("tail did not print the capability level it decided on:\n%s", h.stdout())
	}
}

// TestTailConsumeOverridesTheRefusal: the refusal is a default, not a wall.
func TestTailConsumeOverridesTheRefusal(t *testing.T) {
	h := workQueueHarness(t)
	h.env.clock = firedFor(time.Unix(1700000000, 0), time.Second)
	code := h.run("tail", "workqueue://host", "orders", "--consume", "--idle", "1s")
	if code != exitOK {
		t.Fatalf("tail --consume = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
}

// TestTailSettlesNothing is the specification's own wording: tail settles nothing
// where the driver permits it, and a delivery in flight when the run ends is
// left exactly as unsettled as every other one.
func TestTailSettlesNothing(t *testing.T) {
	h, _ := newSharedHarness(t)
	if got := h.run("pub", sharedDSN, "orders", "one", "two"); got != exitOK {
		t.Fatalf("pub = %d: %s", got, h.stderr())
	}
	h.out.Reset()
	code := h.run("tail", sharedDSN, "orders", "watcher", "--from-earliest",
		"--max", "1", "--prefetch", "4")
	if code != exitOK {
		t.Fatalf("tail = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	out := h.stdout()
	if !strings.Contains(out, "abandoned") {
		t.Errorf("tail settled something. The specification is 'never settle where the driver "+
			"permits it':\n%s", out)
	}
	for _, forbidden := range []string{"acked ", "rejected "} {
		if strings.Contains(out, forbidden) {
			t.Errorf("tail's report counts %q, so it settled a delivery:\n%s", forbidden, out)
		}
	}
	if !strings.Contains(out, "settling nothing") {
		t.Errorf("tail did not say that it settles nothing:\n%s", out)
	}
}

// TestTailSubscriptionIsItsOwn covers the generated name: two tails must not
// share one subscription and split the stream between them.
func TestTailSubscriptionIsItsOwn(t *testing.T) {
	e := &cliEnv{clock: &steppedClock{now: time.Unix(1700000000, 0), step: time.Nanosecond}}
	first, second := e.tailSubscription(), e.tailSubscription()
	if first == second {
		t.Errorf("two tails would share the subscription %q and split the stream", first)
	}
	for _, s := range []string{first, second} {
		if !strings.HasPrefix(s, "iqmq-tail-") {
			t.Errorf("the generated subscription %q is not recognisable as tail's", s)
		}
		if err := mq.ValidateTopic(s); err != nil {
			t.Errorf("the generated subscription %q is not a usable name: %v", s, err)
		}
	}

	// An explicit name wins, both as a flag and as a positional.
	h, _ := newSharedHarness(t)
	h.env.clock = firedFor(time.Unix(1700000000, 0), time.Second)
	if got := h.run("tail", sharedDSN, "orders", "--subscription", "chosen", "--idle", "1s"); got != exitOK {
		t.Fatalf("tail --subscription = %d: %s", got, h.stderr())
	}
	if !strings.Contains(h.stdout(), "subscription chosen") {
		t.Errorf("--subscription was ignored:\n%s", h.stdout())
	}
}

// TestTailTooManyArguments: tail takes a DSN, a topic and at most a
// subscription.
func TestTailTooManyArguments(t *testing.T) {
	h := newHarness(t)
	if got := h.run("tail", memDSN, "orders", "sub", "extra"); got != exitUsage {
		t.Errorf("tail with four positionals = %d, want %d", got, exitUsage)
	}
}

// workQueueHarness builds an environment whose only driver advertises a work
// queue: explicit acknowledgement, no fan-out, no at-least-once. It is the
// shape several real drivers have, and it is what tail has to refuse.
//
// No shipped driver has that exact shape in-process -- the memory driver
// advertises fan-out -- so the capability set is narrowed over the memory
// broker's own backend. The refusal path is the one this increment must be
// able to watch working, and a path only a container can reach is a path
// nobody watches.
func workQueueHarness(t *testing.T) *harness {
	t.Helper()
	return harnessWith(t, narrowedRegistry(t), nil)
}

// narrowedRegistry returns a registry whose "workqueue" scheme opens a client
// over a backend advertising a work queue.
func narrowedRegistry(t *testing.T) *dsn.Registry {
	t.Helper()
	reg := dsn.NewRegistry()
	err := reg.Register("workqueue", func(_ context.Context, _ string) (*mq.Client, error) {
		b, err := memory.NewBroker(memory.Config{AutoCreate: true})
		if err != nil {
			return nil, err
		}
		return mq.NewClient(&workQueueBackend{Broker: b}), nil
	})
	if err != nil {
		t.Fatalf("registering: %v", err)
	}
	return reg
}

// workQueueBackend wraps the memory broker's backend and advertises a work
// queue: explicit acknowledgement, nothing else tail needs.
//
// It exists because no shipped driver has that exact shape in-process, and the
// refusal path is the one this increment must be able to watch working. The
// embedded broker supplies every other method, so this is a capability
// narrowing and not a second backend implementation.
type workQueueBackend struct {
	*memory.Broker
}

// Capabilities advertises explicit acknowledgement and no fan-out.
func (b *workQueueBackend) Capabilities() mq.Capabilities {
	var c mq.Capabilities
	for f, l := range map[mq.Feature]mq.Level{
		mq.FeatureExplicitAck:    mq.Native,
		mq.FeatureFanOut:         mq.Unsupported,
		mq.FeatureAtLeastOnce:    mq.Unsupported,
		mq.FeatureAtMostOnce:     mq.Native,
		mq.FeatureConsumerGroups: mq.Native,
	} {
		if err := c.Set(f, mq.Support{Level: l}); err != nil {
			panic(err)
		}
	}
	return c
}

// Info names this narrowing, so that a doctor run against it is not mistaken
// for the memory driver's own answer.
func (b *workQueueBackend) Info() mq.DriverInfo {
	return mq.DriverInfo{Name: "workqueue", Profile: "test-narrowing"}
}

// TestTailProbeTimerIsNotTheIdleTimer pins that the clock a test builds for
// its OWN timer does not cancel a context the command needed for something
// else.
//
// `tail` opens its capability probe under `deadline(e.ctx, 10*time.Second)`
// before it consumes, so a clock whose every timer had already fired
// cancelled that context as well as the idle one, and whether `registry.Open`
// finished first was a scheduling race. It failed one `make ci` at a load
// average of 44 with
//
//	tail --subscription = 1: iqmq tail: opening shared://: context canceled
//
// which names neither the probe nor the test double.
//
// Both arms are deterministic. The first asks the clock directly: the named
// timer has fired and the probe's has not, which is the whole invariant and
// cannot be raced. The second is the end-to-end statement, made decidable by
// an opener that waits: on a clock that fired everything the cancellation is
// already in flight and arrives at once, and on this one it never comes.
func TestTailProbeTimerIsNotTheIdleTimer(t *testing.T) {
	c := firedFor(time.Unix(1700000000, 0), time.Second)

	idle := c.NewTimer(time.Second)
	select {
	case <-idle.C():
	default:
		t.Error("the timer the test named had not fired; --idle would never expire and the run would " +
			"block until the test timed out")
	}
	if idle.Stop() {
		t.Error("Stop on the fired timer reported that it had stopped one that was still running")
	}

	probe := c.NewTimer(10 * time.Second)
	select {
	case <-probe.C():
		t.Error("tail's ten-second capability probe timer had ALREADY fired on a clock built for a " +
			"one-second idle timer; the probe's context is cancelled before the driver is dialled, and " +
			"whether the open wins is a scheduling race")
	default:
	}
	if !probe.Stop() {
		t.Error("Stop on a timer that never fired reported that it had already fired")
	}

	// End to end: an opener that waits makes the cancellation decidable.
	broker, err := memory.NewBroker(memory.Config{
		AutoCreate:      true,
		DefaultTopology: memory.TopologyConfig{Kind: memory.TopologyStream, Retention: memory.Retention{MaxMessages: 16}},
	})
	if err != nil {
		t.Fatalf("memory.NewBroker: %v", err)
	}
	t.Cleanup(func() { _ = broker.Close() })

	// tail opens TWICE -- once for the capability probe and once to consume
	// -- and it is the FIRST open, the probe's, whose context the stray
	// timer cancelled. Only that one waits, and only that one is recorded.
	var opens, probeCancelled atomic.Int64
	reg := dsn.NewRegistry()
	if err := reg.Register("probe", func(ctx context.Context, _ string) (*mq.Client, error) {
		if opens.Add(1) == 1 {
			// A hundred milliseconds is not a synchronisation: it is the
			// grace a cancellation that is ALREADY in flight needs in
			// order to arrive. `expiring` starts its goroutine before
			// `open` is called and the timer's channel is loaded, so on a
			// clock that fired everything this returns in microseconds;
			// on this one the grace runs out and the dial proceeds, which
			// is what a real ten-second deadline does.
			grace := time.NewTimer(100 * time.Millisecond)
			defer grace.Stop()
			select {
			case <-ctx.Done():
				probeCancelled.Store(1)
				return nil, ctx.Err()
			case <-grace.C:
			}
		}
		return memory.OpenWithClient(ctx, broker, memory.ClientBorrowed)
	}); err != nil {
		t.Fatalf("registering the probe scheme: %v", err)
	}

	h := harnessWith(t, reg, nil)
	h.env.clock = c
	if got := h.run("tail", "probe://host", "orders", "--idle", "1s"); got != exitOK {
		t.Fatalf("tail = %d: %s", got, h.stderr())
	}
	if opens.Load() < 2 {
		t.Fatalf("the driver was opened %d time(s); tail opens once to read the capabilities and once to "+
			"consume, so this test did not reach the path it is about", opens.Load())
	}
	if probeCancelled.Load() != 0 {
		t.Error("the capability probe's context was already cancelled when the driver was dialled: the " +
			"test's clock had fired a timer the test was not about")
	}
}
