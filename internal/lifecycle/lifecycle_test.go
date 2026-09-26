package lifecycle

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
)

// recorder records the order the steps ran in.
type recorder struct {
	mu    sync.Mutex
	order []string
}

func (r *recorder) add(s string) {
	r.mu.Lock()
	r.order = append(r.order, s)
	r.mu.Unlock()
}

func (r *recorder) joined() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.order, ",")
}

// steps returns a Steps that records every call, plus the recorder.
func steps() (Steps, *recorder) {
	r := &recorder{}
	return Steps{
		StopAccept:  func() { r.add("stopaccept") },
		StopFetch:   func() { r.add("stopfetch") },
		Flush:       func(context.Context) error { r.add("flush"); return nil },
		Drain:       func(context.Context) error { r.add("drain"); return nil },
		Settle:      func() { r.add("settle") },
		Commit:      func() error { r.add("commit"); return nil },
		CloseNative: func() error { r.add("closenative"); return nil },
	}, r
}

func TestPhaseNames(t *testing.T) {
	if got := len(Phases()); got != int(phaseCount) {
		t.Fatalf("Phases() returned %d of %d", got, phaseCount)
	}
	for _, p := range Phases() {
		if p.String() == "" || p.Ident() == "" || !p.Valid() {
			t.Fatalf("phase %d has no name", p)
		}
	}
	if got := phaseCount.String(); got != "phase(8)" {
		t.Fatalf("out-of-range phase named %q", got)
	}
	if got := phaseCount.Ident(); got != "" {
		t.Fatalf("out-of-range phase has ident %q", got)
	}
	for _, o := range Ops() {
		if o.String() == "" || o.Ident() == "" {
			t.Fatalf("op %d has no name", o)
		}
		if b := o.Barrier(); b == Running || !b.Valid() {
			t.Fatalf("op %s has barrier %s", o, b)
		}
	}
	if got := opCount.String(); got != "op(3)" {
		t.Fatalf("out-of-range op named %q", got)
	}
	if got := opCount.Barrier(); got != Running {
		t.Fatalf("out-of-range op has barrier %s", got)
	}
	// The barriers must be in the order the message contract fixes:
	// publications stop before fetches, fetches before settlements.
	if OpPublish.Barrier() >= OpFetch.Barrier() || OpFetch.Barrier() >= OpSettle.Barrier() {
		t.Fatalf("barriers out of order: %s %s %s",
			OpPublish.Barrier(), OpFetch.Barrier(), OpSettle.Barrier())
	}
}

func TestShutdownRunsEveryPhaseInOrder(t *testing.T) {
	st, r := steps()
	l := New(Config{Steps: st})
	if err := l.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	const want = "stopaccept,stopfetch,flush,drain,settle,commit,closenative"
	if got := r.joined(); got != want {
		t.Fatalf("step order %q, want %q", got, want)
	}
	if l.Phase() != Closed {
		t.Fatalf("phase %s after Shutdown", l.Phase())
	}
}

func TestCloseSkipsTheWaitingSteps(t *testing.T) {
	st, r := steps()
	l := New(Config{Steps: st})
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	const want = "stopaccept,stopfetch,settle,commit,closenative"
	if got := r.joined(); got != want {
		t.Fatalf("step order %q, want %q", got, want)
	}
}

func TestCloseIsIdempotentUnderConcurrency(t *testing.T) {
	var runs atomic.Int64
	boom := errors.New("boom")
	l := New(Config{Steps: Steps{
		Settle:      func() { runs.Add(1) },
		CloseNative: func() error { return boom },
	}})
	const goroutines = 32
	errs := make([]error, goroutines)
	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				errs[i] = l.Close()
				return
			}
			errs[i] = l.Shutdown(context.Background())
		}()
	}
	wg.Wait()
	if got := runs.Load(); got != 1 {
		t.Fatalf("the settling step ran %d times", got)
	}
	for i, err := range errs {
		if !errors.Is(err, boom) {
			t.Fatalf("caller %d got %v, want the one run's error", i, err)
		}
	}
	// A later Close observes the same outcome rather than a fresh run.
	if err := l.Close(); !errors.Is(err, boom) {
		t.Fatalf("late Close returned %v", err)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("a late Close re-ran the steps (%d)", got)
	}
	select {
	case <-l.Done():
	default:
		t.Fatal("Done is not closed after Close")
	}
}

func TestEnterRefusedPastTheBarrier(t *testing.T) {
	l := New(Config{})
	for _, op := range Ops() {
		if tk := l.Enter(op); !tk.OK() {
			t.Fatalf("%s refused while running", op)
		} else {
			l.Leave(tk)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, op := range Ops() {
		tk := l.Enter(op)
		if tk.OK() {
			t.Fatalf("%s admitted after Close", op)
		}
		if l.Allow(op) {
			t.Fatalf("Allow(%s) after Close", op)
		}
		l.Leave(tk) // must be a no-op
	}
	if !l.Closing() {
		t.Fatal("Closing() is false after Close")
	}
	if tk := l.Enter(opCount); tk.OK() {
		t.Fatal("an undefined op was admitted")
	}
	if l.BeginDeadLetter() != nil {
		t.Fatal("a dead-letter publication was opened after Close")
	}
}

// TestPhaseWaitsForAdmittedOperation is the ordering the whole package exists
// for: a phase transition does not complete while an operation it bars is
// still inside its placement.
func TestPhaseWaitsForAdmittedOperation(t *testing.T) {
	var settled atomic.Bool
	placed := make(chan struct{})
	release := make(chan struct{})
	l := New(Config{Steps: Steps{Settle: func() { settled.Store(true) }}})

	tk := l.Enter(OpPublish)
	if !tk.OK() {
		t.Fatal("Enter refused while running")
	}
	go func() {
		close(placed)
		<-release
		l.Leave(tk)
	}()
	<-placed

	done := make(chan error, 1)
	go func() { done <- l.Close() }()
	select {
	case <-done:
		t.Fatal("Close completed with an operation still admitted")
	case <-time.After(20 * time.Millisecond):
	}
	if settled.Load() {
		t.Fatal("the settling step ran while an operation was admitted")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !settled.Load() {
		t.Fatal("the settling step never ran")
	}
}

// TestPlacementNeverEscapesTheDrain races placements against Close for
// thousands of rounds. It is the regression test for the engine's recurring
// defect class (hack/PLAYBOOK.md): a record placed into a store while the
// handle is closing, whose drain has already run, is never released.
//
// The check has teeth: replacing Enter/Leave with a bare read of Closing()
// before the placement — the protocol that shipped the bug three times — makes
// it fail within a few hundred rounds under -race.
func TestPlacementNeverEscapesTheDrain(t *testing.T) {
	rounds := 3000
	if testing.Short() {
		rounds = 200
	}
	for round := range rounds {
		var (
			mu       sync.Mutex
			store    []int
			drained  bool
			released int
			placed   atomic.Int64
			refused  atomic.Int64
		)
		l := New(Config{Steps: Steps{Settle: func() {
			mu.Lock()
			released += len(store)
			store = nil
			drained = true
			mu.Unlock()
		}}})

		const placers = 4
		var wg sync.WaitGroup
		start := make(chan struct{})
		for p := range placers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				tk := l.Enter(OpPublish)
				if !tk.OK() {
					refused.Add(1)
					return
				}
				mu.Lock()
				if drained {
					mu.Unlock()
					l.Leave(tk)
					t.Errorf("round %d: placer %d was admitted after the drain", round, p)
					return
				}
				store = append(store, p)
				mu.Unlock()
				placed.Add(1)
				l.Leave(tk)
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := l.Close(); err != nil {
				t.Errorf("round %d: Close: %v", round, err)
			}
		}()
		close(start)
		wg.Wait()

		mu.Lock()
		leaked := len(store)
		got := released
		mu.Unlock()
		if leaked != 0 {
			t.Fatalf("round %d: %d record(s) left in the store after Close", round, leaked)
		}
		if int64(got) != placed.Load() {
			t.Fatalf("round %d: %d placed but %d released", round, placed.Load(), got)
		}
		if placed.Load()+refused.Load() != placers {
			t.Fatalf("round %d: %d placed + %d refused != %d",
				round, placed.Load(), refused.Load(), placers)
		}
	}
}

func TestDeadLetterOrdering(t *testing.T) {
	l := New(Config{})
	p := l.BeginDeadLetter()
	if p == nil {
		t.Fatal("BeginDeadLetter refused while running")
	}
	if p.SettleAllowed() {
		t.Fatal("an open dead-letter publication allows the settlement")
	}
	if p.Resolved() {
		t.Fatal("an open dead-letter publication reports itself resolved")
	}
	if got := l.DeadLettersPending(); got != 1 {
		t.Fatalf("%d publications pending, want 1", got)
	}
	p.Commit()
	if !p.SettleAllowed() || !p.Resolved() {
		t.Fatal("a committed publication does not allow the settlement")
	}
	if got := l.DeadLettersPending(); got != 0 {
		t.Fatalf("%d publications pending after commit", got)
	}
	// Resolving twice, and resolving the other way, are both no-ops.
	p.Abort()
	if !p.SettleAllowed() {
		t.Fatal("a committed publication was aborted afterwards")
	}

	q := l.BeginDeadLetter()
	q.Abort()
	if q.SettleAllowed() {
		t.Fatal("an aborted publication allows the settlement")
	}
	if !q.Resolved() {
		t.Fatal("an aborted publication is not resolved")
	}
	if got := l.DeadLettersPending(); got != 0 {
		t.Fatalf("%d publications pending after abort", got)
	}
}

// TestSettlingWaitsForDeadLetters is the DLQ-before-ack ordering hook seen
// from the shutdown: the settling step, which is what drops what the lanes
// hold, does not run while a dead-letter publication is still in flight.
func TestSettlingWaitsForDeadLetters(t *testing.T) {
	var settledWith atomic.Int64
	var l *Lifecycle
	l = New(Config{Steps: Steps{Settle: func() {
		settledWith.Store(int64(l.DeadLettersPending()))
	}}})
	pending := l.BeginDeadLetter()

	done := make(chan error, 1)
	go func() { done <- l.Shutdown(context.Background()) }()
	select {
	case <-done:
		t.Fatal("Shutdown settled with a dead-letter publication in flight")
	case <-time.After(20 * time.Millisecond):
	}
	pending.Commit()
	if err := <-done; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := settledWith.Load(); got != 0 {
		t.Fatalf("the settling step ran with %d publications in flight", got)
	}
}

func TestPromptCloseDoesNotWaitForDeadLetters(t *testing.T) {
	l := New(Config{})
	p := l.BeginDeadLetter()
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := l.DeadLettersPending(); got != 1 {
		t.Fatalf("%d publications pending, want the abandoned one", got)
	}
	// The record's fate is still decided by the publication, never by the
	// close: an abandoned publication does not permit the settlement.
	if p.SettleAllowed() {
		t.Fatal("an abandoned dead-letter publication allows the settlement")
	}
}

func TestDrainDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stuck := make(chan struct{})
		l := New(Config{
			Clock:        clock.Real{},
			DrainTimeout: time.Second,
			Steps: Steps{
				Drain: func(ctx context.Context) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-stuck:
						return nil
					}
				},
			},
		})
		err := l.Shutdown(context.Background())
		if !errors.Is(err, ErrDrainTimeout) {
			t.Fatalf("Shutdown returned %v, want ErrDrainTimeout", err)
		}
		if got := errcode.CodeOf(err); got != errcode.DeadlineExceeded {
			t.Fatalf("ErrDrainTimeout has code %v", got)
		}
		if l.Phase() != Closed {
			t.Fatalf("phase %s: the deadline stopped the shutdown", l.Phase())
		}
		close(stuck)
	})
}

func TestDrainCancelledByCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := New(Config{DrainTimeout: time.Hour, Steps: Steps{
		Drain: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}})
	go cancel()
	err := l.Shutdown(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown returned %v, want the caller's cancellation", err)
	}
	if errors.Is(err, ErrDrainTimeout) {
		t.Fatal("a cancelled caller was reported as a drain timeout")
	}
	if l.Phase() != Closed {
		t.Fatalf("phase %s: cancellation stopped the shutdown", l.Phase())
	}
}

func TestUnboundedDrainTimeout(t *testing.T) {
	l := New(Config{DrainTimeout: -1, Steps: Steps{
		Drain: func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); ok {
				t.Error("a negative DrainTimeout still bounded the drain")
			}
			return nil
		},
	}})
	if err := l.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestPhaseMonotone checks that each step runs in its own phase and that the
// phase only ever moves forward.
func TestPhaseMonotone(t *testing.T) {
	var (
		l        *Lifecycle
		observed []Phase
	)
	note := func() { observed = append(observed, l.Phase()) }
	l = New(Config{Steps: Steps{
		StopAccept:  note,
		StopFetch:   note,
		Flush:       func(context.Context) error { note(); return nil },
		Drain:       func(context.Context) error { note(); return nil },
		Settle:      note,
		Commit:      func() error { note(); return nil },
		CloseNative: func() error { note(); return nil },
	}})
	if err := l.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	want := []Phase{StopAccept, StopFetch, Flushing, Draining, Settling, Committing, Closed}
	if len(observed) != len(want) {
		t.Fatalf("observed %v, want %v", observed, want)
	}
	for i := range want {
		if observed[i] != want[i] {
			t.Fatalf("step %d ran in phase %s, want %s", i, observed[i], want[i])
		}
		if i > 0 && observed[i] <= observed[i-1] {
			t.Fatalf("phase went from %s to %s", observed[i-1], observed[i])
		}
	}
}

// TestShutdownBarrierHonoursTheDrainDeadline pins the half of the API contract
// the specification's "never waits indefinitely" that the phase barriers themselves owe.
// enter(StopAccept) runs before any step does, so a bound that governed only
// the steps would govern nothing at all until the first one ran: a Shutdown
// with a 10 ms deadline would still wait out an operation that stayed
// admitted for a second.
//
// The run is not abandoned — that would let the placement it waits for land
// behind the drain — so the check is that the *caller* returns on its own
// deadline and that the run finishes properly once the operation leaves.
//
// It has teeth: making gate.wait park on a bare receive again makes Shutdown
// take the full hold and return nil.
func TestShutdownBarrierHonoursTheDrainDeadline(t *testing.T) {
	const hold = 500 * time.Millisecond
	l := New(Config{DrainTimeout: 10 * time.Millisecond})
	tk := l.Enter(OpPublish)
	if !tk.OK() {
		t.Fatal("Enter refused while running")
	}
	go func() {
		time.Sleep(hold)
		l.Leave(tk)
	}()
	start := time.Now()
	err := l.Shutdown(context.Background())
	took := time.Since(start)
	if took > hold/2 {
		t.Fatalf("Shutdown took %v with DrainTimeout=10ms and an operation held for %v", took, hold)
	}
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Shutdown returned %v, want ErrDrainTimeout for a barrier its deadline outlived", err)
	}
	if got := l.Phase(); got == Closed {
		t.Fatal("the run abandoned the barrier rather than outliving the caller")
	}
	select {
	case <-l.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the run never finished after the operation left")
	}
	if got := l.Phase(); got != Closed {
		t.Fatalf("phase %s after the run finished", got)
	}
}

// TestShutdownBarrierHonoursTheCallerContext is the same property for the
// other bound: a caller whose context has already been cancelled waits for no
// barrier at all, and is told its own error rather than a deadline that never
// passed.
func TestShutdownBarrierHonoursTheCallerContext(t *testing.T) {
	const hold = 500 * time.Millisecond
	l := New(Config{DrainTimeout: time.Hour})
	tk := l.Enter(OpPublish)
	if !tk.OK() {
		t.Fatal("Enter refused while running")
	}
	go func() {
		time.Sleep(hold)
		l.Leave(tk)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err := l.Shutdown(ctx)
	took := time.Since(start)
	if took > hold/2 {
		t.Fatalf("Shutdown took %v with a cancelled context and an operation held for %v", took, hold)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown returned %v, want the caller's cancellation", err)
	}
	if errors.Is(err, ErrDrainTimeout) {
		t.Fatal("a cancelled caller was reported as a drain timeout")
	}
	select {
	case <-l.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the run never finished after the operation left")
	}
}

// TestPromptCloseWaitsOutTheBarrier is the other side of the contract: a
// prompt Close has no deadline to honour, and the operations it waits for
// are bounded placements, so it waits for them rather than abandoning a
// placement half done.
func TestPromptCloseWaitsOutTheBarrier(t *testing.T) {
	l := New(Config{})
	tk := l.Enter(OpPublish)
	if !tk.OK() {
		t.Fatal("Enter refused while running")
	}
	done := make(chan error, 1)
	go func() { done <- l.Close() }()
	select {
	case <-done:
		t.Fatal("Close completed with an operation still admitted")
	case <-time.After(20 * time.Millisecond):
	}
	l.Leave(tk)
	if err := <-done; err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestCallerCancelDuringDeadLetterWait pins the error contract Shutdown
// documents: the caller's cancellation is reported as the caller's, not as a
// drain deadline that had not passed. The dead-letter wait was the one wait
// not routed through timeout.
func TestCallerCancelDuringDeadLetterWait(t *testing.T) {
	reached := make(chan struct{})
	l := New(Config{DrainTimeout: time.Hour, Steps: Steps{
		StopFetch: func() { close(reached) },
	}})
	p := l.BeginDeadLetter()
	if p == nil {
		t.Fatal("BeginDeadLetter returned nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-reached
		cancel()
	}()
	err := l.Shutdown(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown returned %v, want the caller's cancellation", err)
	}
	if errors.Is(err, ErrDrainTimeout) {
		t.Fatal("a cancelled caller was reported as a drain deadline that had not passed")
	}
	p.Abort()
}

// TestASecondShutdownWaitsOnItsOwnBound pins whose deadline a graceful caller
// is answering. The barrier's report is a one-shot for the life of the
// handle: it says a barrier outlived the bound the *run* was given, and a
// caller that arrives afterwards with a context of its own and a drain
// deadline it has not begun to spend has earned nothing by it. Before this,
// the second Shutdown below returned ErrDrainTimeout in under a microsecond,
// having waited for nothing at all.
func TestASecondShutdownWaitsOnItsOwnBound(t *testing.T) {
	l := New(Config{DrainTimeout: time.Minute})
	tk := l.Enter(OpPublish)
	if !tk.OK() {
		t.Fatal("Enter refused while running")
	}
	// The first caller ends its own wait by cancelling its own context, so
	// the barrier reports itself stuck while the run goes on waiting.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("first Shutdown returned %v, want the caller's cancellation", err)
	}
	out := make(chan error, 1)
	go func() { out <- l.Shutdown(context.Background()) }()
	select {
	case err := <-out:
		t.Fatalf("a second Shutdown returned %v before its own bound or the run", err)
	case <-time.After(50 * time.Millisecond):
	}
	// The run can finish now, and that is what the second caller is waiting
	// for: its own bound is a minute away and its context is live.
	l.Leave(tk)
	select {
	case err := <-out:
		if err != nil {
			t.Fatalf("a second Shutdown returned %v, want the run's outcome", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a second Shutdown did not return when the run finished")
	}
}

// TestJoiningAFinishedRunReportsItsOutcome pins the ordering between the two
// things a graceful caller waits on, and it does so without a race of its
// own: the ticket is held across the first Shutdown, so the run cannot finish
// while that Shutdown waits and the caller's own bound is the only thing that
// can end it. The second Shutdown then arrives with both cases ready — the
// run has finished and its context is already cancelled — which is the state
// select decides at random. A finished run wins: there is nothing left to
// wait for, so the caller is told the outcome rather than a deadline.
func TestJoiningAFinishedRunReportsItsOutcome(t *testing.T) {
	for range 200 {
		l := New(Config{DrainTimeout: time.Millisecond})
		tk := l.Enter(OpPublish)
		if !tk.OK() {
			t.Fatal("Enter refused while running")
		}
		if err := l.Shutdown(context.Background()); !errors.Is(err, ErrDrainTimeout) {
			t.Fatalf("first Shutdown returned %v, want ErrDrainTimeout", err)
		}
		// Only now can the run complete: the barrier it is parked on has
		// been waiting for this ticket the whole time.
		l.Leave(tk)
		<-l.Done()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := l.Shutdown(ctx); err != nil {
			t.Fatalf("joining a finished run returned %v, want its outcome", err)
		}
		if err := l.Close(); err != nil {
			t.Fatalf("Close after the run finished: %v", err)
		}
	}
}
