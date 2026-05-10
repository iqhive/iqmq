package inbox_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/x/inbox"
)

// TestDifferentKeysDoNotWaitForEachOther is the reason MemoryStore locks per
// key.
//
// A database with a unique index on the inbox's key column blocks a second
// transaction inserting the SAME key until the first commits or rolls back --
// TestASecondWorkerWaitsWhileFnRuns holds that half -- and lets a transaction
// inserting a DIFFERENT key straight through. The store used to hold one mutex
// across fn, so worker B on key "b" waited for worker A's handler on key "a":
// a slow handler serialised every delivery in the process, and a test written
// against the store never saw two handlers interleave, which is the one thing
// production is guaranteed to do.
func TestDifferentKeysDoNotWaitForEachOther(t *testing.T) {
	s := memStore(t, inbox.MemoryOptions{})
	ctx := context.Background()
	inFn := make(chan struct{})
	release := make(chan struct{})
	aDone := make(chan error, 1)
	go func() {
		_, err := s.Once(ctx, "a", func(context.Context) error {
			close(inFn)
			<-release
			return nil
		})
		aDone <- err
	}()
	<-inFn
	defer func() {
		close(release)
		if err := <-aDone; err != nil {
			t.Errorf("worker A = %v", err)
		}
	}()

	bDone := make(chan struct {
		ran bool
		err error
	}, 1)
	go func() {
		ran, err := s.Once(ctx, "b", func(context.Context) error { return nil })
		bDone <- struct {
			ran bool
			err error
		}{ran, err}
	}()
	select {
	case got := <-bDone:
		if !got.ran || got.err != nil {
			t.Fatalf("worker B on another key = (%v, %v), want (true, nil)", got.ran, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker B's Once on key \"b\" did not return while worker A's handler held key \"a\": " +
			"one slow handler serialises every delivery in the process")
	}
}

// TestAPanickingHandlerIsRolledBack: a handler that panics has not done its
// work, so its key must not be recorded -- a database rolls back a transaction
// whose owner never committed it, and the store stands in for one.
//
// The panic is recovered HERE, the way a runner's recover middleware would
// recover it. If the key survived the unwind as recorded, the redelivery that
// follows would be acked as a duplicate without the handler ever having
// completed: the one failure this package exists to prevent.
func TestAPanickingHandlerIsRolledBack(t *testing.T) {
	s := memStore(t, inbox.MemoryOptions{})
	ctx := context.Background()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the handler's panic did not propagate out of Once")
			}
		}()
		_, _ = s.Once(ctx, "k", func(context.Context) error { panic("handler bug") })
	}()
	if s.Has("k") {
		t.Fatal("the key of a handler that panicked is recorded; its redelivery would be acked as a duplicate of work that never completed")
	}
	ran, err := s.Once(ctx, "k", func(context.Context) error { return nil })
	if !ran || err != nil {
		t.Fatalf("the redelivery after a panic = (%v, %v), want (true, nil): it must run the handler", ran, err)
	}
}

// holdKey starts a Once on key whose fn blocks until the returned release is
// called, and returns once fn is running. fail makes that fn roll back.
func holdKey(t *testing.T, s *inbox.MemoryStore, key string, fail bool) (release func() (bool, error)) {
	t.Helper()
	inFn := make(chan struct{})
	gate := make(chan struct{})
	type result struct {
		ran bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		ran, err := s.Once(context.Background(), key, func(context.Context) error {
			close(inFn)
			<-gate
			if fail {
				return errors.New("worker A's handler failed")
			}
			return nil
		})
		done <- result{ran, err}
	}()
	<-inFn
	return func() (bool, error) {
		close(gate)
		r := <-done
		return r.ran, r.err
	}
}

// TestAWaiterRunsWhenTheHolderRollsBack: the second worker on a key is not
// told "duplicate" merely because somebody held the key -- when the holder's
// transaction rolls back the key is free, and the waiter must take it and run
// its own handler, or the message is acked with its work undone.
func TestAWaiterRunsWhenTheHolderRollsBack(t *testing.T) {
	s := memStore(t, inbox.MemoryOptions{})
	release := holdKey(t, s, "k", true)
	type result struct {
		ran bool
		err error
	}
	b := make(chan result, 1)
	go func() {
		ran, err := s.Once(context.Background(), "k", func(context.Context) error { return nil })
		b <- result{ran, err}
	}()
	if ran, err := release(); ran || err == nil {
		t.Fatalf("worker A = (%v, %v), want its handler's error", ran, err)
	}
	select {
	case got := <-b:
		if !got.ran || got.err != nil {
			t.Fatalf("worker B after A rolled back = (%v, %v), want (true, nil): the key was free", got.ran, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker B never returned after worker A rolled back")
	}
	if !s.Has("k") || s.Len() != 1 {
		t.Fatalf("after B committed: Has = %v, Len = %d", s.Has("k"), s.Len())
	}
}

// TestAWaiterGivesUpWithItsContext: a second worker waiting on a held key
// stops waiting when its own context ends, without running its fn. A
// database's lock timeout is the same thing, and the middleware reads a store
// that returned an error without calling fn as a store that could not answer,
// so the delivery is retried rather than acked.
func TestAWaiterGivesUpWithItsContext(t *testing.T) {
	s := memStore(t, inbox.MemoryOptions{})
	release := holdKey(t, s, "k", false)
	defer func() { _, _ = release() }()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	ran, err := s.Once(ctx, "k", func(context.Context) error {
		t.Error("a waiter whose context ended ran its fn")
		return nil
	})
	if ran || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Once with an expired context behind a held key = (%v, %v), want (false, DeadlineExceeded)", ran, err)
	}
}

// TestTheStoreIsReadableFromInsideFn: Has and Len on any key, and Once on a
// DIFFERENT key, return from inside fn. Only a second Once on fn's own key
// waits for itself, and the Store contract says so.
func TestTheStoreIsReadableFromInsideFn(t *testing.T) {
	s := memStore(t, inbox.MemoryOptions{})
	done := make(chan error, 1)
	go func() {
		_, err := s.Once(context.Background(), "outer", func(ctx context.Context) error {
			if s.Has("outer") {
				return errors.New("the uncommitted key reads as recorded from inside its own transaction")
			}
			if s.Len() != 0 {
				return errors.New("Len counts a transaction that has not committed")
			}
			ran, err := s.Once(ctx, "inner", func(context.Context) error { return nil })
			if !ran || err != nil {
				return errors.New("a Once on another key from inside fn did not commit")
			}
			return nil
		})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reading the store from inside fn deadlocked")
	}
	if s.Len() != 2 {
		t.Fatalf("Len = %d after two commits", s.Len())
	}
}

// TestCloseWaitsForTransactionsInFlight: Close returns only once no fn is
// running inside the store, and a worker waiting on a held key is refused
// rather than admitted after the store closed.
func TestCloseWaitsForTransactionsInFlight(t *testing.T) {
	s := memStore(t, inbox.MemoryOptions{})
	release := holdKey(t, s, "k", false)
	waiter := make(chan error, 1)
	go func() {
		_, err := s.Once(context.Background(), "k", func(context.Context) error { return nil })
		waiter <- err
	}()
	closed := make(chan struct{})
	go func() {
		_ = s.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while a transaction was still in flight")
	case <-time.After(100 * time.Millisecond):
	}
	if ran, err := release(); !ran || err != nil {
		t.Fatalf("the in-flight transaction = (%v, %v), want it to commit", ran, err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close never returned after the last transaction ended")
	}
	select {
	case err := <-waiter:
		if !errors.Is(err, inbox.ErrClosed) {
			t.Fatalf("the waiter = %v, want ErrClosed: it must not be admitted after Close", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter never returned after Close")
	}
}

// TestOnceRacesClose is the playbook's recurring defect class applied to this
// store: a transaction started while the store is closing.
//
// The property: every Once either is refused with ErrClosed without running
// fn, or runs fn to completion; and once Close has returned, no fn is running
// and none starts. Thousands of rounds, because the known instances of this
// class appeared at five to twenty failures per two or three thousand rounds.
func TestOnceRacesClose(t *testing.T) {
	const rounds = 3000
	for i := range rounds {
		s, err := inbox.NewMemoryStore(inbox.MemoryOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var running, afterClose atomic.Int32
		var closedFlag atomic.Bool
		var wg sync.WaitGroup
		for j := range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				key := string(rune('a' + j%2)) // two keys, so some workers wait
				_, err := s.Once(context.Background(), key, func(context.Context) error {
					if closedFlag.Load() {
						afterClose.Add(1)
					}
					running.Add(1)
					defer running.Add(-1)
					// Stay inside fn for a few scheduling points, so that
					// a Close which did not wait is caught with a fn
					// running rather than only occasionally.
					for range 4 {
						runtime.Gosched()
					}
					return nil
				})
				if err != nil && !errors.Is(err, inbox.ErrClosed) {
					t.Errorf("round %d: Once = %v, want nil or ErrClosed", i, err)
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Close()
			closedFlag.Store(true)
			if n := running.Load(); n != 0 {
				t.Errorf("round %d: Close returned with %d fn still running", i, n)
			}
		}()
		wg.Wait()
		if n := afterClose.Load(); n != 0 {
			t.Fatalf("round %d: %d fn started after Close returned", i, n)
		}
	}
}
