package engine

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/clock"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/lifecycle"
)

// A Publish.Release callback is the caller's own code. the message contract
// keeps it out of the engine's internal locks and the specification makes Close "prompt,
// idempotent … never waits indefinitely"; internal/lifecycle states the same
// rule for its own gate ("the section between Enter and Leave must not
// block"), because a phase barrier waits for every admitted ticket.
//
// The tests below drive every route on which the engine hands a payload back
// while it holds a ticket, and check the two things that follow: a callback
// may re-enter the engine — Close included — and a callback that takes its
// time never becomes something Close or Shutdown waits for. Before the fix
// each of them either deadlocked outright or waited out the callback.

// releaseRoute is one path on which a publication's Release callback runs
// while the engine holds a lifecycle ticket.
type releaseRoute struct {
	name string
	// build makes the engine and whatever state the route needs, and returns
	// the call that runs release on the caller's own goroutine.
	build func(t *testing.T, drain time.Duration, release func(int64)) (*Engine[int64], func())
}

// releaseRoutes are the ticketed sections that end in a caller's callback.
func releaseRoutes() []releaseRoute {
	stream := TopologyConfig{
		Kind: TopologyStream, Partitions: 1, CapacityMessages: 64,
		AckDeadline: time.Second, Retention: Retention{MaxMessages: 64},
	}
	aged := stream
	aged.Retention = Retention{MaxMessages: 64, MaxAge: time.Second}
	return []releaseRoute{{
		// Topic.commit's copying branch: every subscription observes its own
		// clone and the caller's payload goes straight back, inside the
		// OpPublish ticket publishPerSub holds.
		name: "commit",
		build: func(t *testing.T, drain time.Duration, release func(int64)) (*Engine[int64], func()) {
			eng, _ := newReleaseEngine(t, drain, TopologyConfig{})
			top := newReleaseTopic(t, eng, TopologyConfig{})
			if _, err := top.Subscribe("s", SubscriptionConfig{}); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			return eng, func() {
				_ = top.Publish(context.Background(), Publish[int64]{
					Payload: 1, Size: 1, Mode: CopySafe, Release: release,
				})
			}
		},
	}, {
		// Topic.publish's no-subscription arm on a stream: the log keeps a
		// clone and the caller's payload goes straight back, inside the
		// ticket retainOnlyGated takes. It is not a corner — this is the arm
		// ordinary subject-filtered traffic takes whenever no filter matches.
		name: "retain-only",
		build: func(t *testing.T, drain time.Duration, release func(int64)) (*Engine[int64], func()) {
			eng, _ := newReleaseEngine(t, drain, stream)
			top := newReleaseTopic(t, eng, stream)
			return eng, func() {
				_ = top.Publish(context.Background(), Publish[int64]{
					Payload: 1, Size: 1, Mode: CopySafe, Release: release,
				})
			}
		},
	}, {
		// Topic.commit's retention trim: the entries the age bound no longer
		// covers are given back by the append this publication makes, and
		// the last reference on an owned record runs the publisher's
		// callback.
		name: "commit-trim",
		build: func(t *testing.T, drain time.Duration, release func(int64)) (*Engine[int64], func()) {
			eng, fake := newReleaseEngine(t, drain, aged)
			top := newReleaseTopic(t, eng, aged)
			// Published before anyone subscribed, so the retained log holds
			// the only reference and the trim below is what releases it.
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: 1, Size: 1, Mode: CopyOwned, Release: release,
			}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if _, err := top.Subscribe("s", SubscriptionConfig{}); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			// The first record is now older than the age bound, so the next
			// publication's append trims it.
			fake.Advance(2 * time.Second)
			return eng, func() {
				_ = top.Publish(context.Background(), Publish[int64]{
					Payload: 2, Size: 1, Mode: CopyOwned, Release: func(int64) {},
				})
			}
		},
	}, {
		// lane.lease's expiry arm, inside the OpFetch ticket Consumer.take
		// holds: a record that outlived its time to live is never delivered,
		// and the last reference on an owned one is the publisher's payload.
		// The fetch route the increment first closed was the raced-close one
		// (Consumer.deliver); this one reaches the same drop through the
		// claim itself, with no close involved.
		name: "fetch-expiry",
		build: func(t *testing.T, drain time.Duration, release func(int64)) (*Engine[int64], func()) {
			eng, fake := newReleaseEngine(t, drain, TopologyConfig{})
			top := newReleaseTopic(t, eng, TopologyConfig{})
			sub, err := top.Subscribe("s", SubscriptionConfig{})
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: 1, Size: 1, Mode: CopyOwned,
				TTL: 10 * time.Millisecond, Release: release,
			}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			cons, err := sub.Attach(ConsumerConfig{})
			if err != nil {
				t.Fatalf("Attach: %v", err)
			}
			// A record that has outlived its time to live is also what the
			// engine's own TTL sweep gives up, and the advance below is what
			// would wake it. The sweep is stopped so that the fetch's own
			// claim is the only route to this record: that claim is the
			// section under test, and a callback the scheduler goroutine ran
			// would be testing the sweep instead. Arming a stopped scheduler
			// is harmless (scheduler.arm), and Engine.Close's own call to
			// scheduler.close is idempotent.
			eng.sched.close()
			fake.Advance(200 * time.Millisecond)
			return eng, func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				dst := make([]Claim[int64], 4)
				_, _ = cons.Fetch(ctx, dst)
			}
		},
	}, {
		// Topic.Subscribe: the replay it runs under a publish ticket starts
		// by applying the topic's age bound, which gives records back.
		name: "subscribe-trim",
		build: func(t *testing.T, drain time.Duration, release func(int64)) (*Engine[int64], func()) {
			eng, fake := newReleaseEngine(t, drain, aged)
			top := newReleaseTopic(t, eng, aged)
			for i := range 3 {
				if err := top.Publish(context.Background(), Publish[int64]{
					Payload: int64(i + 1), Size: 1, Mode: CopyOwned, Release: release,
				}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}
			fake.Advance(2 * time.Second)
			return eng, func() {
				// A callback that closed the engine makes the Subscribe that
				// called it refuse, which is the right answer and not a
				// failure of the route.
				if _, err := top.Subscribe("s", SubscriptionConfig{
					Start: StartPosition{Kind: StartEarliest},
				}); err != nil && !errors.Is(err, errcode.ErrClosed) {
					t.Errorf("Subscribe: %v", err)
				}
			}
		},
	}}
}

func newReleaseTopic(t *testing.T, eng *Engine[int64], cfg TopologyConfig) *Topic[int64] {
	t.Helper()
	top, err := eng.CreateTopic("t", cfg)
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	return top
}

// newReleaseEngine builds the engine for a route whose topic is cfg. The
// architecture follows the topology (archFor): a route whose topic is a stream
// cannot run on a shared partition log, and the routes whose topic is an
// ordinary one keep running on whatever the bake-off bound.
func newReleaseEngine(t *testing.T, drain time.Duration, cfg TopologyConfig) (*Engine[int64], *clock.Fake) {
	t.Helper()
	fake := clock.NewFake(time.Unix(0, 0))
	eng, err := New(Config[int64]{
		Kinds:        KindSet{Arch: archFor(cfg)},
		Clock:        fake,
		Clone:        func(v int64) int64 { return v },
		DrainTimeout: drain,
		DefaultTopology: TopologyConfig{
			Partitions: 1, CapacityMessages: 64, AckDeadline: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return eng, fake
}

// TestReleaseCallbackMayCloseTheEngine is the deadlock the ticket introduced:
// a Release callback that calls Engine.Close never returns, because the only
// outstanding publish ticket is held by the goroutine parked inside Close and
// the shutdown's StopAccept barrier is waiting for exactly that ticket.
//
// Nothing documents Release as forbidden from blocking or from re-entering
// the engine, and a permanent deadlock is not an acceptable answer to an
// undocumented call.
func TestReleaseCallbackMayCloseTheEngine(t *testing.T) {
	for _, rt := range releaseRoutes() {
		t.Run(rt.name, func(t *testing.T) {
			var (
				inRoute atomic.Bool
				fired   atomic.Bool
				eng     *Engine[int64]
				once    sync.Once
			)
			var call func()
			eng, call = rt.build(t, time.Second, func(int64) {
				// The engine's own settling drain runs this callback too, on
				// the goroutine performing the run; calling Close from there
				// is a re-entrant join, not the defect under test.
				if !inRoute.Load() {
					return
				}
				once.Do(func() {
					fired.Store(true)
					if err := eng.Close(); err != nil {
						t.Errorf("Close from a Release callback: %v", err)
					}
				})
			})
			done := make(chan struct{})
			go func() {
				defer close(done)
				inRoute.Store(true)
				call()
				inRoute.Store(false)
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				// The goroutine is parked for good; the test has to report
				// rather than hang until the package timeout.
				t.Fatal("a Release callback that called Close never returned")
			}
			if !fired.Load() {
				t.Fatal("no Release callback ran inside the route: the check proves nothing")
			}
			_ = eng.Close()
		})
	}
}

// TestCloseIsPromptWhileAReleaseCallbackRuns is the same defect without any
// re-entrancy at all: a Release callback that takes its time made Close take
// the same time, because Close was waiting for the ticket the callback ran
// under. Close is prompt, and how long a caller's own callback runs
// is not something the engine may wait for.
func TestCloseIsPromptWhileAReleaseCallbackRuns(t *testing.T) {
	for _, rt := range releaseRoutes() {
		t.Run(rt.name, func(t *testing.T) {
			eng, call, running, resume := blockedRelease(t, rt, time.Second)
			defer close(resume)
			done := make(chan struct{})
			go func() { defer close(done); call() }()
			select {
			case <-running:
			case <-time.After(10 * time.Second):
				t.Fatal("no Release callback ran inside the route: the check proves nothing")
			}
			closed := make(chan struct{})
			go func() { defer close(closed); _ = eng.Close() }()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("Close waited for a Release callback that had not returned")
			}
		})
	}
}

// TestShutdownHonoursItsBoundsAgainstAReleaseCallback is the graceful form,
// and the bound that was missing entirely: phases() enters StopAccept before
// any step runs, so a bound that governed only the steps governed nothing
// until the first one ran. Shutdown with an already-cancelled context and a
// 10 ms drain deadline used to wait out the whole callback and return nil.
func TestShutdownHonoursItsBoundsAgainstAReleaseCallback(t *testing.T) {
	for _, rt := range releaseRoutes() {
		t.Run(rt.name, func(t *testing.T) {
			eng, call, running, resume := blockedRelease(t, rt, 10*time.Millisecond)
			defer close(resume)
			done := make(chan struct{})
			go func() { defer close(done); call() }()
			select {
			case <-running:
			case <-time.After(10 * time.Second):
				t.Fatal("no Release callback ran inside the route: the check proves nothing")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			out := make(chan error, 1)
			go func() { out <- eng.Shutdown(ctx) }()
			select {
			case err := <-out:
				// Whichever bound ended the wait, what Shutdown must not do
				// is wait out the callback — and it must not report success
				// for a wait a bound ended.
				if err != nil && !errors.Is(err, context.Canceled) &&
					!errors.Is(err, lifecycle.ErrDrainTimeout) {
					t.Fatalf("Shutdown returned %v, want nil, the caller's error or ErrDrainTimeout", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Shutdown honoured neither its context nor its drain deadline")
			}
		})
	}
}

// blockedRelease builds one route whose first Release callback parks until
// resume is closed, and reports on running when it has parked.
func blockedRelease(t *testing.T, rt releaseRoute, drain time.Duration) (
	eng *Engine[int64], call func(), running, resume chan struct{},
) {
	t.Helper()
	running, resume = make(chan struct{}), make(chan struct{})
	var once sync.Once
	eng, call = rt.build(t, drain, func(int64) {
		once.Do(func() {
			close(running)
			<-resume
		})
	})
	return eng, call, running, resume
}

// TestFetchDoesNotHoldTheGateOverAReleaseCallback is the same property on the
// fetch side. Consumer.take holds an OpFetch ticket, and a fetch that a close
// raced drops the records it could not put back — which runs the publisher's
// Release callback. The shutdown's StopFetch barrier waits for that ticket,
// so a callback inside it is one Close waits for.
//
// The callback checks it directly rather than by timing: it runs during a
// shutdown, so if the ticket were still held the phase could never move past
// StopFetch however long it waited.
//
// The interleaving is forced, not raced. The test used to run a Close against
// a fetch two hundred times and fail as vacuous when no callback had landed
// inside the window, which the scheduler decides: it failed CI that way twice
// on two cores, then skipped below four CPUs , and at four
// CPUs with four test binaries sharing them it still failed 2 runs in 240.
// The window is the fetch between its claim and its hand-over, and the engine
// reads its injected clock at the top of that section (Consumer.drain), so a
// clock that parks that one read holds the fetch inside its ticket for as long
// as the test needs: the Close is started, observed at the StopFetch barrier
// the ticket is holding, and only then is the fetch let go. Every round takes
// the route, on every machine.
func TestFetchDoesNotHoldTheGateOverAReleaseCallback(t *testing.T) {
	const (
		rounds  = 20
		records = 8
	)
	for round := range rounds {
		trap := newClockTrap(clock.NewFake(time.Unix(0, 0)), "(*Consumer[", ").drain")
		eng, err := New(Config[int64]{
			Kinds:        KindSet{Arch: archFor(TopologyConfig{})},
			Clock:        trap,
			Clone:        func(v int64) int64 { return v },
			DrainTimeout: time.Second,
			DefaultTopology: TopologyConfig{
				Partitions: 1, CapacityMessages: 64, AckDeadline: time.Second,
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		top := newReleaseTopic(t, eng, TopologyConfig{})
		sub, err := top.Subscribe("s", SubscriptionConfig{})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		lc := eng.Lifecycle()
		var released, onFetch, stuck atomic.Int64
		release := func(int64) {
			released.Add(1)
			if !inFetch() {
				// The closer's own settling drain: nothing is waiting for
				// a fetch ticket there.
				return
			}
			onFetch.Add(1)
			if stuck.Load() > 0 {
				return // the round has already failed; do not wait again
			}
			deadline := time.Now().Add(2 * time.Second)
			for lc.Phase() <= lifecycle.StopFetch {
				if time.Now().After(deadline) {
					stuck.Add(1)
					return
				}
				runtime.Gosched()
			}
		}
		for i := range records {
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: int64(i + 1), Size: 1, Mode: CopyOwned, Release: release,
			}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}
		cons, err := sub.Attach(ConsumerConfig{})
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}

		trap.arm()
		fetched := make(chan error, 1)
		go func() {
			dst := make([]Claim[int64], records)
			_, err := cons.Fetch(context.Background(), dst)
			fetched <- err
		}()
		select {
		case <-trap.parked:
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: the fetch never read the clock in Consumer.drain; the trap no longer "+
				"reaches the section it is meant to hold", round)
		}
		closed := make(chan struct{})
		go func() { defer close(closed); _ = eng.Close() }()
		// The fetch is parked inside its ticket, so the StopFetch barrier
		// cannot pass it: the phase reaches StopFetch and stays there.
		for deadline := time.Now().Add(10 * time.Second); lc.Phase() < lifecycle.StopFetch; {
			if time.Now().After(deadline) {
				t.Fatalf("round %d: Close never reached StopFetch (phase %v)", round, lc.Phase())
			}
			runtime.Gosched()
		}
		if p := lc.Phase(); p != lifecycle.StopFetch {
			t.Fatalf("round %d: the phase moved to %v past a fetch ticket that is still held", round, p)
		}
		trap.let()

		if err := <-fetched; !errors.Is(err, errcode.ErrClosed) {
			t.Fatalf("round %d: the raced fetch returned %v, want %v", round, err, errcode.ErrClosed)
		}
		select {
		case <-closed:
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: Close never returned after the fetch let its ticket go", round)
		}
		if n := stuck.Load(); n > 0 {
			t.Fatalf("round %d: a Release callback ran with the fetch gate held; "+
				"the shutdown could never pass StopFetch", round)
		}
		// Every record the parked fetch claimed is one the close makes it
		// give up, so every callback is the fetch route's: one that ran
		// anywhere else means the forced interleaving no longer holds all of
		// them inside the ticket, and the test is proving less than it says.
		if got := onFetch.Load(); got != records {
			t.Fatalf("round %d: %d of %d Release callbacks ran on the fetch route: the forced "+
				"interleaving no longer drops everything the raced fetch claimed", round, got, records)
		}
		if got := released.Load(); got != records {
			t.Fatalf("round %d: %d Release callbacks for %d owned records", round, got, records)
		}
	}
}

// clockTrap is a clock that parks one read: the first one made, after arm,
// by a goroutine with the method recv...name on its stack. The engine reads
// its injected clock at the top of the sections a close races, so parking
// that read holds a goroutine at a known point for as long as a test needs:
// Consumer.drain is the top of a fetch's claim-to-hand-over section, inside
// its lifecycle ticket (TestFetchDoesNotHoldTheGateOverAReleaseCallback), and
// Subscription.leave reaches it in the revoke that follows the table it has
// already stored (TestConsumerCloseRacingARebalanceClosesItsLeases). Every
// other read -- the scheduler's, the lifecycle's, another goroutine's --
// passes straight through.
type clockTrap struct {
	clock.Clock
	recv, name string
	armed      atomic.Bool
	parked     chan struct{}
	release    chan struct{}
}

func newClockTrap(c clock.Clock, recv, name string) *clockTrap {
	return &clockTrap{Clock: c, recv: recv, name: name, parked: make(chan struct{}), release: make(chan struct{})}
}

// arm makes the next matching read park.
func (c *clockTrap) arm() { c.armed.Store(true) }

// let lets the parked read return.
func (c *clockTrap) let() { close(c.release) }

// Now parks the armed read until let.
func (c *clockTrap) Now() time.Time {
	if c.armed.Load() && calledFrom(c.recv, c.name) && c.armed.CompareAndSwap(true, false) {
		close(c.parked)
		<-c.release
	}
	return c.Clock.Now()
}

// inFetch reports whether the calling goroutine is inside Consumer.take,
// which is where the fetch route releases -- after the ticket, in
// fetchDrops.run, or inside it, which is the defect.
func inFetch() bool { return calledFrom("(*Consumer[", ").take") }

// calledFrom reports whether a frame of the calling goroutine's stack is a
// method of this package whose receiver starts with recv and whose name ends
// with name. It reads function names rather than a flag the test sets,
// because the engine runs callbacks and clock reads on goroutines of its own
// at the same time.
func calledFrom(recv, name string) bool {
	pcs := make([]uintptr, 64)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		f, more := frames.Next()
		fn := f.Function
		if strings.Contains(fn, "internal/engine."+recv) && strings.HasSuffix(fn, name) {
			return true
		}
		if !more {
			return false
		}
	}
}

// inPublication reports whether the calling goroutine is inside a live
// Topic.publish — the section that holds an OpPublish ticket — and is not one
// of the engine's own goroutines. It is read from the callback's own stack
// rather than from a flag the test sets around the call, because the engine
// runs Release callbacks on its own settling and scheduler goroutines at the
// same time: a shared counter cannot tell one goroutine's callback from
// another's, and a re-entrant Close from the goroutine performing the run is
// not the defect these tests are about.
func inPublication() bool {
	buf := make([]byte, 16384)
	s := string(buf[:runtime.Stack(buf, false)])
	for _, engineOwn := range []string{").Close(", ").Shutdown(", ").settle", ".drainAll", ".run("} {
		if strings.Contains(s, engineOwn) {
			return false
		}
	}
	return strings.Contains(s, ").publish(") || strings.Contains(s, ").Publish(")
}

// TestJoinCommitRunsNoCallerCallbackInsideItsTicket is releaseRoutes' property
// on the one route that cannot be written as a route: it needs a subscription
// joining a stream at the same time as a publication, so the call that runs
// the callback is not the caller's alone.
//
// The specification built a join staging buffer, in which a publication that found a
// subscription still replaying its join handed its record to that
// subscription (Topic.commit -> admitPublished -> stageJoin) and the buffer's
// own close check dropped what it had staged (dropStaged), running the
// publisher's Release inside the OpPublish ticket the publication was holding.
// A Release that called Engine.Close then waited for its own ticket for ever.
// The specification reverted the buffer in its final rounds (ADR-017) and this test
// is what says the route did not come back — by driving the join rather than
// by naming the functions, so that any other route to the same class is caught
// with it.
//
// Reverted onto the tree that carried the buffer it fails within a dozen
// rounds, the parked goroutine reading Topic.publish -> stageJoin ->
// dropStaged -> releasePayload with Close parked in lifecycle's gate.wait.
func TestJoinCommitRunsNoCallerCallbackInsideItsTicket(t *testing.T) {
	const (
		retain = 600
		rounds = 12
	)
	for round := range rounds {
		// The join's replay clones every entry it walks, so a slow Clone is
		// what makes the join wide enough for a publication to land inside it.
		var slow atomic.Bool
		fake := clock.NewFake(time.Unix(0, 0))
		eng, err := New(Config[int64]{
			// A stream: pinned, because a shared partition log has no way to
			// admit a replayed record.
			Kinds: KindSet{Arch: featureArch},
			Clock: fake,
			Clone: func(v int64) int64 {
				if slow.Load() {
					time.Sleep(150 * time.Microsecond)
				}
				return v
			},
			DrainTimeout: time.Second,
			DefaultTopology: TopologyConfig{
				Partitions: 1, CapacityMessages: 200000, AckDeadline: time.Second,
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		top, err := eng.CreateTopic("t", TopologyConfig{
			Kind: TopologyStream, Partitions: 1, CapacityMessages: 200000,
			AckDeadline: time.Second, Retention: Retention{MaxMessages: retain},
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		for i := range retain {
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: int64(i), Size: 1, Mode: CopySafe, Release: func(int64) {},
			}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}

		var once sync.Once
		release := func(int64) {
			if !inPublication() {
				return
			}
			once.Do(func() { _ = eng.Close() })
		}

		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _ = top.Subscribe("s", SubscriptionConfig{
				Start: StartPosition{Kind: StartEarliest},
			})
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Owned publications share one record with the retained log, so
			// once the log has trimmed them the subscription holds the last
			// reference — and the last reference is what runs the callback.
			for i := range 4 * retain {
				_ = top.Publish(context.Background(), Publish[int64]{
					Payload: int64(100000 + i), Size: 1, Mode: CopyOwned, Release: release,
				})
			}
		}()

		slow.Store(true)
		close(start)
		time.Sleep(time.Duration(300+round*400) * time.Microsecond)
		go func() { _ = eng.Close() }()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			buf := make([]byte, 1<<20)
			t.Logf("all goroutines:\n%s", string(buf[:runtime.Stack(buf, true)]))
			t.Fatalf("round %d: a publication never returned; a Release callback that called Close is parked inside its own ticket", round)
		}
		_ = eng.Close()
	}
}
