package lifecycle

import (
	"context"
	"errors"
	"fmt"
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
	case err := <-done:
		t.Fatalf("Close completed (%v) with an operation still admitted", err)
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
// defect class: a record placed into a store while the
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
	case err := <-done:
		t.Fatalf("Shutdown settled (%v) with a dead-letter publication in flight", err)
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

// TestDrainTimeoutOfIsReadAsShutdownBegins is the bound of a handle that
// chooses it after New: what DrainTimeoutOf answers when Shutdown begins,
// not what it answered when the handle was built, and DefaultDrainTimeout
// for an answer of zero, as DrainTimeout reads it.
func TestDrainTimeoutOfIsReadAsShutdownBegins(t *testing.T) {
	for _, tc := range []struct {
		name   string
		answer time.Duration
		want   time.Duration
	}{
		{"chosen", time.Minute, time.Minute},
		{"zero", 0, DefaultDrainTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				answer := time.Second
				l := New(Config{
					DrainTimeoutOf: func() time.Duration { return answer },
					Steps: Steps{
						Drain: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
					},
				})
				answer = tc.answer
				start := time.Now()
				err := l.Shutdown(context.Background())
				if elapsed := time.Since(start); !errors.Is(err, ErrDrainTimeout) || elapsed != tc.want {
					t.Fatalf("Shutdown = %v after %v, want ErrDrainTimeout at %v", err, elapsed, tc.want)
				}
			})
		})
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
	case err := <-done:
		t.Fatalf("Close completed (%v) with an operation still admitted", err)
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
	// for: its own bound is a minute away and its context is live. The run
	// was cut short by the first caller's cancellation, which is not this
	// caller's to hear: its own answer is nil, as nothing of its own ended.
	l.Leave(tk)
	select {
	case err := <-out:
		if err != nil {
			t.Fatalf("a second Shutdown returned %v, want nil: the first caller's cancellation is not its own", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a second Shutdown did not return when the run finished")
	}
}

// TestTheRunNamesTheBoundThatCutIt is the outcome of a graceful run whose
// bound cut a barrier short: the bound that passed at that moment, and not
// whatever the caller's context says by the time the admitted operation
// leaves. When the drain deadline cut it, the outcome is ErrDrainTimeout even
// though the caller cancels its context afterwards, as a deferred cancel does;
// the run used to read the context only at its end and report a cancellation
// nobody made while it waited. When the caller's own cancellation cut it, the
// outcome is that cancellation.
func TestTheRunNamesTheBoundThatCutIt(t *testing.T) {
	t.Run("DrainTimeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := New(Config{DrainTimeout: time.Second})
			tk := l.Enter(OpSettle)
			if !tk.OK() {
				t.Fatal("the settlement was not admitted")
			}
			ctx, cancel := context.WithCancel(context.Background())
			if err := l.Shutdown(ctx); !errors.Is(err, ErrDrainTimeout) {
				t.Fatalf("Shutdown = %v, want ErrDrainTimeout", err)
			}
			cancel()
			time.Sleep(time.Second)
			l.Leave(tk)
			<-l.Done()
			if err := l.Close(); !errors.Is(err, ErrDrainTimeout) || errors.Is(err, context.Canceled) {
				t.Fatalf("Close after the run = %v, want ErrDrainTimeout: the caller cancelled nothing while the run waited", err)
			}
		})
	})
	t.Run("CallersCancellation", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := New(Config{DrainTimeout: time.Minute})
			tk := l.Enter(OpSettle)
			if !tk.OK() {
				t.Fatal("the settlement was not admitted")
			}
			ctx, cancel := context.WithCancel(context.Background())
			out := make(chan error, 1)
			go func() { out <- l.Shutdown(ctx) }()
			time.Sleep(time.Second)
			cancel()
			if err := <-out; !errors.Is(err, context.Canceled) {
				t.Fatalf("Shutdown = %v, want context.Canceled", err)
			}
			l.Leave(tk)
			<-l.Done()
			if err := l.Close(); !errors.Is(err, context.Canceled) {
				t.Fatalf("Close after the run = %v, want the caller's cancellation, which cut it", err)
			}
		})
	})
}

// TestAJoinerIsNotToldTheBeginnersCancellationByAStep is the joiner's rule
// for a step's error. Shutdown A begins the run and cancels its context while
// a waiting step (Flush or Drain) waits; Shutdown B joined with a live
// context. The step returns A's cancellation, and B was told it: outcome
// stripped the beginner's cancellation only from a barrier's cut, and a step
// ended by the beginner's context put its error with the other step errors,
// where every caller heard it. B hears nothing of its own while its context
// is live; A, which cancelled, hears its cancellation, and a step error the
// beginner's context did not cause still reaches both.
func TestAJoinerIsNotToldTheBeginnersCancellationByAStep(t *testing.T) {
	broken := errors.New("the sink is broken")
	for _, step := range []string{"Flush", "Drain"} {
		t.Run(step, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan struct{})
				wait := func(ctx context.Context) error {
					close(entered)
					<-ctx.Done()
					return ctx.Err()
				}
				steps := Steps{CloseNative: func() error { return broken }}
				switch step {
				case "Flush":
					steps.Flush = wait
				case "Drain":
					steps.Drain = wait
				}
				l := New(Config{DrainTimeout: time.Hour, Steps: steps})
				ctxA, cancelA := context.WithCancel(context.Background())
				began := make(chan error, 1)
				go func() { began <- l.Shutdown(ctxA) }()
				<-entered
				joined := make(chan error, 1)
				go func() { joined <- l.Shutdown(context.Background()) }()
				synctest.Wait()
				time.Sleep(time.Second)
				cancelA()
				errA, errB := <-began, <-joined
				if !errors.Is(errA, context.Canceled) || !errors.Is(errA, broken) {
					t.Errorf("the beginning Shutdown = %v, want its own cancellation and the close's error", errA)
				}
				if errors.Is(errB, context.Canceled) {
					t.Errorf("the joining Shutdown, whose context is live, = %v: it was told the beginner's cancellation", errB)
				}
				if !errors.Is(errB, broken) {
					t.Errorf("the joining Shutdown = %v, want the close's error, which is every caller's", errB)
				}
				ctxC, cancelC := context.WithCancel(context.Background())
				cancelC()
				if err := l.Shutdown(ctxC); !errors.Is(err, context.Canceled) || !errors.Is(err, broken) {
					t.Errorf("a cancelled Shutdown joining the finished run = %v, want its own cancellation in the beginner's place", err)
				}
			})
		})
	}
}

// TestAJoinerStillHearsAStepsOwnDeadlineError is the other side of
// TestAJoinerIsNotToldTheBeginnersCancellationByAStep: only the beginner's
// context's own error is kept from a joiner. A step that returns an error of
// its own with the deadline code, a driver's timeout say, after the
// beginner's context passed its deadline, was dropped from the joiner's
// answer too, because errors.Is matches iqmq errors by code: an *mq.OpError
// carrying CodeDeadlineExceeded is context.DeadlineExceeded to errors.Is.
// driverTimeout is that matching rule without the root package.
func TestAJoinerStillHearsAStepsOwnDeadlineError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var timedOut error = driverTimeout{}
		entered := make(chan struct{})
		l := New(Config{DrainTimeout: time.Hour, Steps: Steps{Drain: func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			return timedOut
		}}})
		ctxA, cancelA := context.WithTimeout(context.Background(), time.Minute)
		defer cancelA()
		began := make(chan error, 1)
		go func() { began <- l.Shutdown(ctxA) }()
		<-entered
		joined := make(chan error, 1)
		go func() { joined <- l.Shutdown(context.Background()) }()
		if err := <-began; !errors.Is(err, timedOut) {
			t.Errorf("the beginning Shutdown = %v, want the step's own error", err)
		}
		if err := <-joined; !errors.Is(err, timedOut) {
			t.Errorf("the joining Shutdown = %v, want the step's own error, which is not the beginner's context's", err)
		}
	})
}

// TestWrapsComparesTheChainByIdentity pins which step errors count as the
// beginner's context's: the context's own error, bare or wrapped (a driver's
// *mq.OpError around it, say), and a multi-error under a wrapper only when
// every branch is. A driver's own timeout, which matches the context's error
// by code and wraps nothing, does not count.
func TestWrapsComparesTheChainByIdentity(t *testing.T) {
	other := errors.New("the sink is broken")
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"bare", context.Canceled, true},
		{"wrapped", fmt.Errorf("the driver: %w", context.Canceled), true},
		{"wrapped twice", fmt.Errorf("outer: %w", fmt.Errorf("the driver: %w", context.Canceled)), true},
		{"wrapping a join of it", fmt.Errorf("outer: %w", errors.Join(context.Canceled, fmt.Errorf("x: %w", context.Canceled))), true},
		{"wrapping a join with something else", fmt.Errorf("outer: %w", errors.Join(context.Canceled, other)), false},
		{"another error", other, false},
		{"matched by code only", driverTimeout{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cerr := context.Canceled
			if tc.name == "matched by code only" {
				cerr = context.DeadlineExceeded
			}
			if got := wraps(tc.err, cerr); got != tc.want {
				t.Errorf("wraps(%v, %v) = %v, want %v", tc.err, cerr, got, tc.want)
			}
		})
	}
}

// driverTimeout is a driver's own timeout error that matches
// context.DeadlineExceeded by code, as an *mq.OpError does.
type driverTimeout struct{}

func (driverTimeout) Error() string { return "the broker did not answer in time" }

func (driverTimeout) Is(target error) bool { return target == context.DeadlineExceeded }

// TestJoiningAFinishedRunReportsItsOutcome pins the ordering between the two
// things a graceful caller waits on, and it does so without a race of its
// own: the ticket is held across the first Shutdown, so the run cannot finish
// while that Shutdown waits and the caller's own bound is the only thing that
// can end it. The second Shutdown then arrives with both cases ready — the
// run has finished and its context is already cancelled — which is the state
// select decides at random. A finished run wins: there is nothing left to
// wait for, so the caller is told the outcome rather than a deadline. The
// outcome is the drain deadline that cut the run's barrier short, and never
// the second caller's own cancellation.
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
		if err := l.Shutdown(ctx); !errors.Is(err, ErrDrainTimeout) || errors.Is(err, context.Canceled) {
			t.Fatalf("joining a finished run returned %v, want its outcome, ErrDrainTimeout", err)
		}
		if err := l.Close(); !errors.Is(err, ErrDrainTimeout) {
			t.Fatalf("Close after the run finished = %v, want its outcome, ErrDrainTimeout", err)
		}
	}
}

// waitForCtx is a waiting step that holds the run until its bound ends it.
func waitForCtx(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestAJoiningShutdownReturnsAtItsOwnBound is a graceful Shutdown that joins
// a run whose bound is further off than its own. It waited for that run
// whatever its own bound said, because only a barrier that outlived the run's
// bound released anyone early, and a run waiting in a step, or a prompt
// Close's run, never reports that. A Shutdown with a second, joining one with
// an hour, waited the hour. It returns at its own bound now, with that
// bound's error, and the run goes on.
func TestAJoiningShutdownReturnsAtItsOwnBound(t *testing.T) {
	t.Run("its context", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := New(Config{DrainTimeout: time.Hour, Steps: Steps{Drain: waitForCtx}})
			ctxA, cancelA := context.WithCancel(context.Background())
			began := make(chan error, 1)
			go func() { began <- l.Shutdown(ctxA) }()
			synctest.Wait()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			start := time.Now()
			err := l.Shutdown(ctx)
			if took := time.Since(start); took != time.Second || !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("the joining Shutdown = %v after %v, want its own deadline at 1s", err, took)
			}
			select {
			case <-l.Done():
				t.Error("the run finished with the joiner: it was cut short by a bound that was not its own")
			default:
			}
			cancelA()
			if err := <-began; !errors.Is(err, context.Canceled) {
				t.Errorf("the beginning Shutdown = %v, want its own cancellation", err)
			}
		})
	})
	t.Run("its drain deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			answer := time.Hour
			l := New(Config{DrainTimeoutOf: func() time.Duration { return answer }, Steps: Steps{Drain: waitForCtx}})
			began := make(chan error, 1)
			go func() { began <- l.Shutdown(context.Background()) }()
			synctest.Wait()
			answer = time.Second
			start := time.Now()
			err := l.Shutdown(context.Background())
			if took := time.Since(start); took != time.Second || !errors.Is(err, ErrDrainTimeout) {
				t.Errorf("the joining Shutdown = %v after %v, want its own drain deadline at 1s", err, took)
			}
			if err := <-began; !errors.Is(err, ErrDrainTimeout) {
				t.Errorf("the beginning Shutdown = %v, want its drain deadline", err)
			}
		})
	})
	t.Run("a prompt Close", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			release := make(chan struct{})
			l := New(Config{Steps: Steps{CloseNative: func() error { <-release; return nil }}})
			closed := make(chan error, 1)
			go func() { closed <- l.Close() }()
			synctest.Wait()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			start := time.Now()
			err := l.Shutdown(ctx)
			if took := time.Since(start); took != time.Second || !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("a Shutdown joining a Close = %v after %v, want its own deadline at 1s", err, took)
			}
			close(release)
			if err := <-closed; err != nil {
				t.Errorf("Close = %v, want nil", err)
			}
		})
	})
}

// TestAJoinerSharingTheRunsBoundWaitsForTheClose is the other side of
// TestAJoiningShutdownReturnsAtItsOwnBound: a joiner whose bound passes with
// the run's waits for the rest of the run, the settling, the commits and the
// close of the natives, as the caller that began it does. A client begins
// each consumer's shutdown under its own context and then joins it, so its
// bound and the consumer run's fall at the same instant; one released there
// would close the backend under a source still closing.
func TestAJoinerSharingTheRunsBoundWaitsForTheClose(t *testing.T) {
	for _, tc := range []struct {
		name  string
		drain time.Duration
		ctx   func() (context.Context, context.CancelFunc)
		want  error
	}{
		{"context", time.Hour, func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), time.Second)
		}, context.DeadlineExceeded},
		{"drain deadline", time.Second, func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		}, ErrDrainTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				closedAt := make(chan time.Duration, 1)
				var start time.Time
				l := New(Config{DrainTimeout: tc.drain, Steps: Steps{
					Drain: waitForCtx,
					CloseNative: func() error {
						time.Sleep(time.Minute)
						closedAt <- time.Since(start)
						return nil
					},
				}})
				ctx, cancel := tc.ctx()
				defer cancel()
				start = time.Now()
				if err := l.StartShutdown(ctx); err != nil {
					t.Fatalf("StartShutdown = %v", err)
				}
				err := l.Shutdown(ctx)
				took := time.Since(start)
				if !errors.Is(err, tc.want) {
					t.Errorf("the joining Shutdown = %v, want %v", err, tc.want)
				}
				if at := <-closedAt; took < at {
					t.Errorf("the joining Shutdown returned after %v, before the natives closed at %v", took, at)
				}
			})
		})
	}
}

// TestAwaitReportsTheBound is a step waiting for something the native client
// holds past the shutdown's bound, as a publisher's close waits for its
// flusher's send. Await never gives up on the wait, but past the bound it
// reports it as a barrier does: a graceful Shutdown returns at its bound with
// the run still going, the Interrupt step runs once, and the run's outcome
// names the bound. A prompt Close runs Interrupt at once and waits.
func TestAwaitReportsTheBound(t *testing.T) {
	t.Run("Shutdown", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			held := make(chan struct{})
			var interrupts atomic.Int32
			l := New(Config{DrainTimeout: time.Second, Steps: Steps{Interrupt: func() { interrupts.Add(1) }}})
			l.steps.CloseNative = func() error { l.Await(held, nil); return nil }
			start := time.Now()
			err := l.Shutdown(context.Background())
			if took := time.Since(start); took != time.Second || !errors.Is(err, ErrDrainTimeout) {
				t.Errorf("Shutdown = %v after %v, want the drain deadline at 1s", err, took)
			}
			select {
			case <-l.Done():
				t.Error("the run finished while the step still waited")
			default:
			}
			if n := interrupts.Load(); n != 1 {
				t.Errorf("Interrupt ran %d times, want once", n)
			}
			time.Sleep(time.Minute)
			close(held)
			<-l.Done()
			if err := l.Close(); !errors.Is(err, ErrDrainTimeout) {
				t.Errorf("the run's outcome = %v, want the drain deadline that cut its wait", err)
			}
		})
	})
	t.Run("Shutdown within its bound", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			held := make(chan struct{})
			l := New(Config{DrainTimeout: time.Minute})
			l.steps.CloseNative = func() error { l.Await(held, nil); return nil }
			go func() {
				time.Sleep(time.Second)
				close(held)
			}()
			start := time.Now()
			if err := l.Shutdown(context.Background()); err != nil {
				t.Errorf("Shutdown = %v, want nil", err)
			}
			if took := time.Since(start); took != time.Second {
				t.Errorf("Shutdown returned after %v, want once the wait ended at 1s", took)
			}
		})
	})
	t.Run("Close", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			held := make(chan struct{})
			l := New(Config{Steps: Steps{Interrupt: func() { close(held) }}})
			l.steps.CloseNative = func() error { l.Await(held, nil); return nil }
			if err := l.Close(); err != nil {
				t.Errorf("Close = %v, want nil once Interrupt ended the wait", err)
			}
		})
	})
	t.Run("enough", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			held, enough := make(chan struct{}), make(chan struct{})
			defer close(held)
			l := New(Config{DrainTimeout: time.Minute})
			l.steps.CloseNative = func() error { l.Await(held, enough); return nil }
			go func() {
				time.Sleep(time.Second)
				close(enough)
			}()
			start := time.Now()
			if err := l.Shutdown(context.Background()); err != nil {
				t.Errorf("Shutdown = %v, want nil: enough ended the wait within the bound", err)
			}
			if took := time.Since(start); took != time.Second {
				t.Errorf("Shutdown returned after %v, want once enough was closed at 1s", took)
			}
			select {
			case <-l.Stuck():
				t.Error("the run reports itself stuck, and nothing outlived its bound")
			default:
			}
		})
	})
}

// TestStuckSaysABarrierOutlivedTheBound is what a client's close reads to
// stop waiting for a handle: Stuck is closed once a barrier of the handle's
// own run outlives that run's bound, and not while the run is only waiting
// within it.
func TestStuckSaysABarrierOutlivedTheBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := New(Config{DrainTimeout: time.Minute})
		tk := l.Enter(OpPublish)
		if !tk.OK() {
			t.Fatal("Enter refused while running")
		}
		out := make(chan error, 1)
		go func() { out <- l.Shutdown(context.Background()) }()
		time.Sleep(59 * time.Second)
		synctest.Wait()
		select {
		case <-l.Stuck():
			t.Fatal("Stuck is closed within the run's bound")
		default:
		}
		time.Sleep(time.Second)
		synctest.Wait()
		select {
		case <-l.Stuck():
		default:
			t.Fatal("Stuck is still open with the barrier waiting past the run's bound")
		}
		if err := <-out; !errors.Is(err, ErrDrainTimeout) {
			t.Errorf("Shutdown = %v, want ErrDrainTimeout", err)
		}
		l.Leave(tk)
		<-l.Done()
	})
}
