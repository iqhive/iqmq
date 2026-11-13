package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
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

// TestSubAndTailPrintTheNewestDeliveryWhileTheyWait: with a credential in the
// DSN, the scrub held back the end of every Write, and nothing flushed it per
// delivery. So a live sub or tail on a quiet topic printed the newest delivery
// only when another one arrived or the run ended.
//
// stdout here stops the run as soon as the body reaches it, so a run that
// printed it while it waited for the next one reports that it was stopped. A
// run that held it back waits out --idle instead, which bounds the failure
// only.
func TestSubAndTailPrintTheNewestDeliveryWhileTheyWait(t *testing.T) {
	const target = "shared://watcher:" + credPassword + "@localhost/"
	for name, args := range map[string][]string{
		"sub":  {"sub", target, "orders", "probe", "--start", "earliest", "--settle", "none", "--idle", "10s"},
		"tail": {"tail", target, "orders", "--from-earliest", "--idle", "10s"},
	} {
		t.Run(name, func(t *testing.T) {
			h, _ := newSharedHarness(t)
			if got := h.run("pub", sharedDSN, "orders", "first-message-body"); got != exitOK {
				t.Fatalf("pub = %d: %s", got, h.stderr())
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out := &stopOnSight{want: "first-message-body\n", stop: cancel}
			h.env.ctx, h.env.stdout = ctx, out
			if code := h.run(args...); code != exitOK {
				t.Fatalf("%s = %d\nstdout:\n%s\nstderr:\n%s", name, code, out, h.stderr())
			}
			if !strings.Contains(out.String(), "stopped: the run was stopped") {
				t.Errorf("%s printed the delivery only after the run had ended:\n%s", name, out)
			}
		})
	}
}

// stopOnSight is a stdout that cancels the run the moment a given text has
// reached it.
type stopOnSight struct {
	mu   sync.Mutex
	b    strings.Builder
	want string
	stop context.CancelFunc
}

// Write records p and stops the run once the text is there.
func (w *stopOnSight) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.b.Write(p)
	if strings.Contains(w.b.String(), w.want) {
		w.stop()
	}
	return len(p), nil
}

// String is what reached stdout.
func (w *stopOnSight) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// TestTailScrubsItsOutputOnce: tail wrapped both streams in the scrub and then
// ran the consume loop sub uses, which wrapped them again. Each replacement was
// counted by one writer and its note by the other, so tail printed two notes,
// and the outer scrub redacted the credential in the inner note's example.
func TestTailScrubsItsOutputOnce(t *testing.T) {
	h, _ := newSharedHarness(t)
	if got := h.run("pub", sharedDSN, "orders", "artemis"); got != exitOK {
		t.Fatalf("pub = %d: %s", got, h.stderr())
	}
	h.out.Reset()
	if code := h.run("tail", "shared://u:artemis@localhost/", "orders", "--from-earliest", "--max", "1"); code != exitOK {
		t.Fatalf("tail = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	out := h.stdout()
	if n := strings.Count(out, "note: "); n != 1 {
		t.Errorf("tail printed %d scrub notes, want 1:\n%s", n, out)
	}
	if !strings.Contains(out, `a password of "artemis", say`) {
		t.Errorf("a second scrub redacted the note's own example:\n%s", out)
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

// TestTailRemovesTheSubscriptionItGenerated: tail with no subscription
// argument generates one, and nothing removed it when the run ended. On a
// broker whose subscriptions outlive their consumers each run left a
// subscription nobody would read again, holding a copy of every later
// message: on nsqd a channel that fills memory and then disk. tail now
// removes the subscription it generated once its consumer has closed, and
// leaves one the caller named alone.
func TestTailRemovesTheSubscriptionItGenerated(t *testing.T) {
	reg, broker := tailRegistry(t, "tailseam", nil)
	generated := make([]string, 0, 3)
	for i := range 3 {
		h := harnessWith(t, reg, nil)
		// A clock on which only the --idle timer has fired.
		h.env.clock = firedFor(time.Unix(1700000000+int64(i), 0), time.Second)
		if code := h.run("tail", "tailseam://", "orders", "--idle", "1s"); code != exitOK {
			t.Fatalf("tail = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
		}
		name := subscriptionLine(t, h.stdout())
		generated = append(generated, name)
		if !strings.Contains(h.stdout(), "removed the subscription "+name) {
			t.Errorf("tail did not say that it removed the subscription it generated\nstdout:\n%s\nstderr:\n%s",
				h.stdout(), h.stderr())
		}
	}
	named := harnessWith(t, reg, nil)
	named.env.clock = firedFor(time.Unix(1700000100, 0), time.Second)
	if code := named.run("tail", "tailseam://", "orders", "kept", "--idle", "1s"); code != exitOK {
		t.Fatalf("tail kept = %d\nstderr:\n%s", code, named.stderr())
	}
	if strings.Contains(named.stdout(), "removed") {
		t.Errorf("tail removed a subscription the caller named:\n%s", named.stdout())
	}

	// The runs are over, and the application publishes on.
	publish(t, broker, "orders", &mq.Message{Body: []byte("a")}, &mq.Message{Body: []byte("b")})

	for _, s := range generated {
		if subscriptionExists(t, broker, "orders", s) {
			t.Errorf("tail's generated subscription %q outlived the run and holds %d message(s) "+
				"published after it ended", s, depth(t, broker, "orders", s))
		}
	}
	if got := depth(t, broker, "orders", "kept"); got != 2 {
		t.Errorf("the subscription the caller named holds %d of the 2 later messages", got)
	}
	if got := depth(t, broker, "orders", "billing"); got != 2 {
		t.Errorf("the application's own subscription holds %d of the 2 later messages", got)
	}
}

// TestTailNamesTheSubscriptionItCouldNotRemove: a driver with durable
// subscriptions and no Admin surface (JetStream's, or ActiveMQ Classic's)
// cannot have tail's subscription removed for it. The run still succeeds,
// and tail names the subscription it left behind rather than leaving it
// there unmentioned.
func TestTailNamesTheSubscriptionItCouldNotRemove(t *testing.T) {
	reg, broker := tailRegistry(t, "noadmin", func(b *memory.Broker) mq.Backend {
		return noAdminBackend{Backend: b}
	})
	h := harnessWith(t, reg, nil)
	h.env.clock = firedFor(time.Unix(1700000000, 0), time.Second)
	if code := h.run("tail", "noadmin://", "orders", "--idle", "1s"); code != exitOK {
		t.Fatalf("tail = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	name := subscriptionLine(t, h.stdout())
	if !strings.Contains(h.stderr(), "could not remove the subscription "+name) {
		t.Errorf("tail left its subscription %q behind without naming it\nstdout:\n%s\nstderr:\n%s",
			name, h.stdout(), h.stderr())
	}
	if !subscriptionExists(t, broker, "orders", name) {
		t.Fatalf("the subscription %q is gone, so this test proves nothing", name)
	}
}

// TestTailRemovesItsSubscriptionAfterAStop: the removal runs on a context
// detached from the interrupt, because Ctrl-C is how a tail usually ends, and
// on the interrupted context the delete failed and left the subscription
// behind with a NOTE. The tests above end the run with --idle, when nothing
// has been interrupted, so they could not tell the two contexts apart.
// --timeout ends the run's own context and not the interrupt's; it is here so
// that both stops are held.
func TestTailRemovesItsSubscriptionAfterAStop(t *testing.T) {
	for _, c := range []struct {
		name      string
		args      []string
		interrupt bool
	}{
		{"Ctrl-C", nil, true},
		{"--timeout", []string{"--timeout", "2s"}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				reg, broker := tailRegistry(t, "tailseam", nil)
				h := harnessWith(t, reg, nil)
				ctx, interrupt := context.WithCancel(context.Background())
				defer interrupt()
				h.env.ctx = ctx
				done := make(chan int, 1)
				go func() {
					done <- h.run(append([]string{"tail", "tailseam://", "orders"}, c.args...)...)
				}()
				synctest.Wait()
				if c.interrupt {
					interrupt()
				} else {
					time.Sleep(3 * time.Second)
				}
				if code := <-done; code != exitOK {
					t.Errorf("tail = %d after %s, want %d", code, c.name, exitOK)
				}
				name := subscriptionLine(t, h.stdout())
				if subscriptionExists(t, broker, "orders", name) || !strings.Contains(h.stdout(), "removed the subscription "+name) {
					t.Errorf("tail stopped by %s left its generated subscription %q behind\nstdout:\n%s\nstderr:\n%s",
						c.name, name, h.stdout(), h.stderr())
				}
			})
		})
	}
}

// TestTailSubscriptionsGeneratedAtOneInstantDiffer: the generated name was
// "iqmq-tail-" and the clock's nanoseconds, so two tails that read the same
// clock value shared one subscription, and once tail removed its own the first
// to end removed it under the other, whose watch then failed. The name now
// ends in random bytes. In the bubble the clock does not move while the second
// tail starts, so both read one instant.
func TestTailSubscriptionsGeneratedAtOneInstantDiffer(t *testing.T) {
	e := &cliEnv{clock: newOnDemandClock(time.Second)} // one fixed Now
	first, second := e.tailSubscription(), e.tailSubscription()
	if first == second {
		t.Errorf("two tails at one clock reading generated the same subscription %q", first)
	}
	for _, s := range []string{first, second} {
		if !strings.HasPrefix(s, "iqmq-tail-1700000000000000000-") || len(s) > 64 {
			t.Errorf("the generated subscription %q is not the clock's nanoseconds and a random part, "+
				"in at most nsqd's 64 bytes", s)
		}
	}

	synctest.Test(t, func(t *testing.T) {
		reg, broker := tailRegistry(t, "tailseam", nil)
		long := harnessWith(t, reg, nil)
		ctx, interrupt := context.WithCancel(context.Background())
		defer interrupt()
		long.env.ctx = ctx
		done := make(chan int, 1)
		go func() { done <- long.run("tail", "tailseam://", "orders") }()
		synctest.Wait()
		short := harnessWith(t, reg, nil)
		if code := short.run("tail", "tailseam://", "orders", "--idle", "1s"); code != exitOK {
			t.Fatalf("the short tail = %d\nstderr:\n%s", code, short.stderr())
		}
		synctest.Wait()
		select {
		case code := <-done:
			t.Fatalf("the long tail ended with %d when the short one removed its subscription\nstdout:\n%s",
				code, long.stdout())
		default:
		}
		interrupt()
		if code := <-done; code != exitOK {
			t.Errorf("the long tail = %d, want %d\nstderr:\n%s", code, exitOK, long.stderr())
		}
		for _, h := range []*harness{short, long} {
			if name := subscriptionLine(t, h.stdout()); subscriptionExists(t, broker, "orders", name) {
				t.Errorf("the subscription %q outlived its tail", name)
			}
		}
	})
}

// TestTailTakesASubscriptionAlreadyGoneAsRemoved: a broker that drops a
// subscription when its last consumer leaves, as nsqd does an ephemeral
// channel, answers the removal with not found (nsqd's 404 CHANNEL_NOT_FOUND).
// tail printed a NOTE that the subscription was left behind and kept a copy
// of every later message, which nothing did.
func TestTailTakesASubscriptionAlreadyGoneAsRemoved(t *testing.T) {
	reg, _ := tailRegistry(t, "gone", func(b *memory.Broker) mq.Backend {
		return goneAdminBackend{Backend: b}
	})
	h := harnessWith(t, reg, nil)
	h.env.clock = firedFor(time.Unix(1700000000, 0), time.Second)
	if code := h.run("tail", "gone://", "orders", "--idle", "1s"); code != exitOK {
		t.Fatalf("tail = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	name := subscriptionLine(t, h.stdout())
	if strings.Contains(h.stderr(), "NOTE") || !strings.Contains(h.stdout(), "the subscription "+name+" was already gone") {
		t.Errorf("tail took a subscription the broker had already removed as left behind\nstdout:\n%s\nstderr:\n%s",
			h.stdout(), h.stderr())
	}
}

// tailRegistry registers scheme over a memory broker that has the topic
// "orders" and the application's subscription "billing" on it. wrap, when not
// nil, puts a narrowed backend in front of the broker.
func tailRegistry(t *testing.T, scheme string, wrap func(*memory.Broker) mq.Backend) (*dsn.Registry, *memory.Broker) {
	t.Helper()
	broker, err := memory.NewBroker(memory.Config{AutoCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	reg := dsn.NewRegistry()
	if err := reg.Register(scheme, func(ctx context.Context, _ string) (*mq.Client, error) {
		if wrap == nil {
			return memory.OpenWithClient(ctx, broker, memory.ClientBorrowed)
		}
		return mq.NewClient(wrap(broker)), nil
	}); err != nil {
		t.Fatal(err)
	}
	ensure(t, broker, "orders", "billing")
	return reg, broker
}

// goneAdminBackend is the memory broker with an Admin surface whose
// DeleteSubscription answers not found, as a broker does for a subscription
// it removed itself.
type goneAdminBackend struct {
	mq.Backend
}

// Close leaves the broker to the test.
func (goneAdminBackend) Close() error { return nil }

// Admin wraps the broker's own surface.
func (b goneAdminBackend) Admin() (mq.Admin, error) {
	inner, err := b.Backend.(mq.AdminBackend).Admin()
	if err != nil {
		return nil, err
	}
	return goneAdmin{Admin: inner}, nil
}

// goneAdmin answers every DeleteSubscription with not found.
type goneAdmin struct {
	mq.Admin
}

// DeleteSubscription reports that there is no such subscription.
func (goneAdmin) DeleteSubscription(context.Context, string, string) error {
	return &mq.OpError{Code: mq.CodeNotFound, Operation: mq.OpAdmin, Err: errors.New("CHANNEL_NOT_FOUND")}
}

// noAdminBackend is the memory broker with its Admin surface hidden: only
// the methods of mq.Backend are promoted, so it does not satisfy
// mq.AdminBackend and Client.Admin answers ErrUnsupported.
type noAdminBackend struct {
	mq.Backend
}

// Close leaves the broker to the test, as memory.ClientBorrowed does: tail
// closes the client of its capability probe before it consumes.
func (noAdminBackend) Close() error { return nil }

// subscriptionLine is the name tail printed on its "subscription" line.
func subscriptionLine(t *testing.T, stdout string) string {
	t.Helper()
	for line := range strings.SplitSeq(stdout, "\n") {
		if name, ok := strings.CutPrefix(line, "subscription "); ok {
			return name
		}
	}
	t.Fatalf("tail printed no subscription line:\n%s", stdout)
	return ""
}

// subscriptionExists reports whether the broker still has the subscription,
// read through its own Admin surface.
func subscriptionExists(t *testing.T, b *memory.Broker, topic, sub string) bool {
	t.Helper()
	admin, err := clientOver(t, b).Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	_, err = admin.Stats(context.Background(), topic, sub)
	switch {
	case err == nil:
		return true
	case mq.CodeOf(err) == mq.CodeNotFound:
		return false
	default:
		t.Fatalf("Stats(%q/%q): %v", topic, sub, err)
		return false
	}
}
