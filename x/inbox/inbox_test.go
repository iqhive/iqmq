package inbox_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/x/inbox"
)

const keyHeader = "operation-id"

func delivery(key string) *mq.Delivery {
	d := &mq.Delivery{}
	if key != "" {
		d.Headers.Set(keyHeader, []byte(key))
	}
	return d
}

// guarded wraps a handler in an inbox middleware over a fresh memory store.
func guarded(tb testing.TB, cfg inbox.Config, h mq.Handler) (mq.Handler, *inbox.Middleware) {
	tb.Helper()
	if cfg.Key == nil {
		cfg.Key = inbox.KeyFromHeader(keyHeader)
	}
	m, err := inbox.New(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	return m.Wrap()(h), m
}

func memStore(tb testing.TB, opt inbox.MemoryOptions) *inbox.MemoryStore {
	tb.Helper()
	s, err := inbox.NewMemoryStore(opt)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = s.Close() })
	return s
}

// TestADuplicateIsAckedWithoutRunningTheHandler is the whole point: this is
// what turns at-least-once delivery into effectively-once processing.
func TestADuplicateIsAckedWithoutRunningTheHandler(t *testing.T) {
	s := memStore(t, inbox.MemoryOptions{})
	runs := 0
	h, m := guarded(t, inbox.Config{Store: s}, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		runs++
		return nil
	}))
	// Ten deliveries of one message, which is what a lease expiry, a
	// rebalance and an operator replaying a day all look like.
	for i := range 10 {
		if err := h.Handle(context.Background(), delivery("op-1")); err != nil {
			t.Fatalf("delivery %d = %v, want nil (acked)", i+1, err)
		}
	}
	if runs != 1 {
		t.Errorf("the handler ran %d times for one key, want 1", runs)
	}
	c := m.Counts()
	if c.Processed != 1 || c.Duplicate != 9 {
		t.Errorf("Counts = %+v, want Processed 1 and Duplicate 9", c)
	}
	if !s.Has("op-1") || s.Len() != 1 {
		t.Errorf("the store holds %d key(s) and Has(op-1)=%v", s.Len(), s.Has("op-1"))
	}
}

// TestAFailedHandlerIsNotRecorded is the property that makes a retry work: a
// handler that failed must be retryable, which means its record must not
// survive the rollback.
func TestAFailedHandlerIsNotRecorded(t *testing.T) {
	s := memStore(t, inbox.MemoryOptions{})
	fail := errors.New("the side effect failed")
	attempts := 0
	h, m := guarded(t, inbox.Config{Store: s}, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		attempts++
		if attempts < 3 {
			return fail
		}
		return nil
	}))
	for i := range 2 {
		if err := h.Handle(context.Background(), delivery("op-1")); !errors.Is(err, fail) {
			t.Errorf("attempt %d = %v, want the handler's error so the delivery is retried", i+1, err)
		}
		if s.Has("op-1") {
			t.Fatalf("after a failed attempt the key is recorded; the redelivery would be acked without the work ever having been done")
		}
	}
	if err := h.Handle(context.Background(), delivery("op-1")); err != nil {
		t.Fatalf("the third attempt = %v, want nil", err)
	}
	if !s.Has("op-1") {
		t.Error("after a successful attempt the key is not recorded; every redelivery would run the handler again")
	}
	if c := m.Counts(); c.Failed != 2 || c.Processed != 1 {
		t.Errorf("Counts = %+v, want Failed 2 and Processed 1", c)
	}
}

// TestAPostponementBehindTheInboxIsNotCountedAsAFailure is a rate limit or an
// open circuit behind the inbox turning the delivery away. Nothing ran, so
// nothing is recorded, the postponement reaches the runner unchanged, and
// the delivery is not counted as a failed handler.
func TestAPostponementBehindTheInboxIsNotCountedAsAFailure(t *testing.T) {
	s := memStore(t, inbox.MemoryOptions{})
	refused := true
	h, m := guarded(t, inbox.Config{Store: s}, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		if refused {
			return mq.Postpone(errors.New("rate limited"), time.Second)
		}
		return nil
	}))
	err := h.Handle(context.Background(), delivery("op-1"))
	if dir, _ := mq.DirectiveOf(err); dir != mq.DirectivePostpone {
		t.Fatalf("the guarded handler reported %v, want the postponement passed on", err)
	}
	if s.Has("op-1") {
		t.Fatal("a postponed delivery's key is recorded; the next offer would be acked without the work ever having been done")
	}
	if c := m.Counts(); c != (inbox.Counts{}) {
		t.Fatalf("Counts = %+v after a postponement, want nothing counted: the handler did not run", c)
	}
	refused = false
	if err := h.Handle(context.Background(), delivery("op-1")); err != nil {
		t.Fatalf("the next offer = %v, want nil", err)
	}
	if c := m.Counts(); c != (inbox.Counts{Processed: 1}) {
		t.Fatalf("Counts = %+v, want Processed 1 and nothing else", c)
	}
}

// TestAHandlerFailureIsNotMistakenForAStoreFailure is the defect this
// increment found in its own first draft.
//
// The classification used to be code -- CodeUnavailable,
// CodeClosed... -- and a HANDLER whose downstream service is down returns
// CodeUnavailable too. Under OnUnavailableProcess that handler would then be
// run a SECOND time, immediately and unguarded, for having failed. The fix is
// structural: whether the store called fn is a fact this middleware owns.
func TestAHandlerFailureIsNotMistakenForAStoreFailure(t *testing.T) {
	for _, code := range []errcode.Code{errcode.Unavailable, errcode.Closed, errcode.DeadlineExceeded, errcode.ResourceExhausted} {
		t.Run(code.String(), func(t *testing.T) {
			s := memStore(t, inbox.MemoryOptions{})
			handlerErr := errcode.New(code, "the handler's downstream is down")
			runs := 0
			h, m := guarded(t, inbox.Config{Store: s, OnUnavailable: inbox.OnUnavailableProcess},
				mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
					runs++
					return handlerErr
				}))
			err := h.Handle(context.Background(), delivery("op-1"))
			if !errors.Is(err, handlerErr) {
				t.Errorf("Handle = %v, want the handler's own error", err)
			}
			if runs != 1 {
				t.Errorf("the handler ran %d times; a handler that failed with %v was re-run as though the STORE had failed", runs, code)
			}
			c := m.Counts()
			if c.Failed != 1 || c.Unavailable != 0 {
				t.Errorf("Counts = %+v, want Failed 1 and Unavailable 0: the handler failed, the store did not", c)
			}
		})
	}
}

func TestAnUnreachableStorePolicy(t *testing.T) {
	// A store that always reports a retryable failure without calling fn.
	down := &downStore{}
	t.Run("retry is the default", func(t *testing.T) {
		runs := 0
		h, m := guarded(t, inbox.Config{Store: down}, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			runs++
			return nil
		}))
		err := h.Handle(context.Background(), delivery("op-1"))
		if err == nil {
			t.Error("Handle = nil against an unreachable store; the delivery would be acked although nothing was recorded or done")
		}
		if runs != 0 {
			t.Errorf("the handler ran %d times under OnUnavailableRetry, want 0", runs)
		}
		if c := m.Counts(); c.Unavailable != 1 {
			t.Errorf("Counts = %+v, want Unavailable 1", c)
		}
	})
	t.Run("process is opt-in", func(t *testing.T) {
		runs := 0
		h, m := guarded(t, inbox.Config{Store: down, OnUnavailable: inbox.OnUnavailableProcess},
			mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
				runs++
				return nil
			}))
		if err := h.Handle(context.Background(), delivery("op-1")); err != nil {
			t.Errorf("Handle = %v, want nil", err)
		}
		if runs != 1 {
			t.Errorf("the handler ran %d times under OnUnavailableProcess, want 1", runs)
		}
		if c := m.Counts(); c.Unavailable != 1 {
			t.Errorf("Counts = %+v, want Unavailable 1", c)
		}
	})
}

func TestMissingKeyPolicy(t *testing.T) {
	for _, c := range []struct {
		name    string
		policy  inbox.MissingKey
		runs    int
		wantErr error
	}{
		{"fail is the default", inbox.MissingKeyFail, 0, inbox.ErrMissingKey},
		{"process is opt-in", inbox.MissingKeyProcess, 1, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := memStore(t, inbox.MemoryOptions{})
			runs := 0
			h, m := guarded(t, inbox.Config{Store: s, OnMissingKey: c.policy},
				mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
					runs++
					return nil
				}))
			err := h.Handle(context.Background(), delivery(""))
			if c.wantErr == nil && err != nil {
				t.Errorf("Handle = %v, want nil", err)
			}
			if c.wantErr != nil && !errors.Is(err, c.wantErr) {
				t.Errorf("Handle = %v, want %v", err, c.wantErr)
			}
			if runs != c.runs {
				t.Errorf("the handler ran %d times, want %d", runs, c.runs)
			}
			if got := m.Counts().NoKey; got != 1 {
				t.Errorf("Counts.NoKey = %d, want 1", got)
			}
			// An unkeyed delivery is never recorded under any policy: there
			// is no key to record it under.
			if s.Len() != 0 {
				t.Errorf("the store recorded %d key(s) for an unkeyed delivery", s.Len())
			}
		})
	}
}

// TestTheHandlerSeesTheStoresContext is the mechanism the whole pattern rests
// on: fn runs inside the transaction, so a handler that writes through a
// transaction-carrying context must see the store's context and not the
// caller's.
func TestTheHandlerSeesTheStoresContext(t *testing.T) {
	type ctxKey struct{}
	tagging := &taggingStore{inner: memStore(t, inbox.MemoryOptions{}), key: ctxKey{}, value: "tx-1"}
	var seen any
	h, _ := guarded(t, inbox.Config{Store: tagging}, mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
		seen = ctx.Value(ctxKey{})
		return nil
	}))
	if err := h.Handle(context.Background(), delivery("op-1")); err != nil {
		t.Fatal(err)
	}
	if seen != "tx-1" {
		t.Errorf("the handler saw context value %v, want tx-1: it was given the caller's context instead of the store's transaction", seen)
	}
}

func TestConcurrentDeliveriesOfOneKeyRunTheHandlerOnce(t *testing.T) {
	s := memStore(t, inbox.MemoryOptions{})
	var mu sync.Mutex
	runs := 0
	h, m := guarded(t, inbox.Config{Store: s}, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		mu.Lock()
		runs++
		mu.Unlock()
		return nil
	}))
	const workers = 16
	var wg sync.WaitGroup
	errs := make([]error, workers)
	wg.Add(workers)
	for i := range errs {
		go func(i int) {
			defer wg.Done()
			errs[i] = h.Handle(context.Background(), delivery("op-1"))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("worker %d = %v, want nil", i, err)
		}
	}
	if runs != 1 {
		t.Errorf("the handler ran %d times across %d concurrent deliveries of one key, want 1", runs, workers)
	}
	if c := m.Counts(); c.Processed != 1 || c.Duplicate != workers-1 {
		t.Errorf("Counts = %+v, want Processed 1 and Duplicate %d", c, workers-1)
	}
}

func TestMemoryStore(t *testing.T) {
	t.Run("the bound is refused not evicted", func(t *testing.T) {
		s := memStore(t, inbox.MemoryOptions{MaxEntries: 2})
		ctx := context.Background()
		for _, k := range []string{"a", "b"} {
			if ran, err := s.Once(ctx, k, func(context.Context) error { return nil }); err != nil || !ran {
				t.Fatalf("Once(%s) = (%v, %v)", k, ran, err)
			}
		}
		ran, err := s.Once(ctx, "c", func(context.Context) error { return nil })
		if !errors.Is(err, inbox.ErrFull) {
			t.Errorf("Once at the bound = (%v, %v), want ErrFull: an eviction would silently turn a memory bound into a duplicate", ran, err)
		}
		if ran {
			t.Error("Once reported that fn ran while refusing")
		}
		// The evicted-key hazard, stated: "a" is still known, so the
		// message that was processed is still deduplicated.
		if !s.Has("a") {
			t.Error("the oldest key was evicted")
		}
	})
	t.Run("empty key", func(t *testing.T) {
		s := memStore(t, inbox.MemoryOptions{})
		if _, err := s.Once(context.Background(), "", func(context.Context) error { return nil }); !errors.Is(err, inbox.ErrEmptyKey) {
			t.Error("an empty key was accepted, which would make every keyless message a duplicate of the first")
		}
	})
	t.Run("negative MaxEntries", func(t *testing.T) {
		if _, err := inbox.NewMemoryStore(inbox.MemoryOptions{MaxEntries: -1}); err == nil {
			t.Error("a negative MaxEntries was accepted")
		}
	})
	t.Run("close is idempotent", func(t *testing.T) {
		s, err := inbox.NewMemoryStore(inbox.MemoryOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range 3 {
			if err := s.Close(); err != nil {
				t.Errorf("Close #%d = %v", i+1, err)
			}
		}
		if _, err := s.Once(context.Background(), "k", func(context.Context) error { return nil }); !errors.Is(err, inbox.ErrClosed) {
			t.Errorf("Once after Close = %v, want ErrClosed", err)
		}
	})
}

// TestASecondWorkerWaitsWhileFnRuns is the INSERT-then-work order, asserted
// the only way it safely can be.
//
// The first version of this test read the store from INSIDE fn -- s.Has(key)
// -- and deadlocked, because the store then held one mutex across fn. The
// store now locks per key , so a read from inside fn
// returns; what still waits is a second Once on the SAME key, which is the
// pattern's own shape: a real SQL store blocks a second transaction inserting
// an uncommitted row too.
//
// So the property is asserted from OUTSIDE: while worker A is inside fn,
// worker B's Once on the same key must not return. When A finishes, B must
// see a duplicate rather than run fn a second time.
func TestASecondWorkerWaitsWhileFnRuns(t *testing.T) {
	s := memStore(t, inbox.MemoryOptions{})
	ctx := context.Background()
	inFn := make(chan struct{})
	releaseFn := make(chan struct{})
	bDone := make(chan struct {
		ran bool
		err error
	}, 1)

	// Worker A's results come back through a channel and not through
	// captured variables. The first version of this test wrote them from
	// A's goroutine and read them from the test's, which -race caught:
	// "Read at 0x... by goroutine 46 / Previous write ... by goroutine 47".
	// The synchronisation the test was relying on -- bDone having been
	// received -- says nothing about A.
	aDone := make(chan struct {
		ran bool
		err error
	}, 1)
	go func() {
		ran, err := s.Once(ctx, "k", func(context.Context) error {
			close(inFn)
			<-releaseFn
			return nil
		})
		aDone <- struct {
			ran bool
			err error
		}{ran, err}
	}()
	<-inFn

	go func() {
		ran, err := s.Once(ctx, "k", func(context.Context) error {
			return errors.New("worker B's fn must never run")
		})
		bDone <- struct {
			ran bool
			err error
		}{ran, err}
	}()

	// B must still be waiting: A has not committed.
	select {
	case got := <-bDone:
		t.Fatalf("worker B's Once returned (%v, %v) while worker A was still inside fn; a second worker got into the transaction", got.ran, got.err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseFn)
	select {
	case got := <-bDone:
		if got.err != nil {
			t.Errorf("worker B = (%v, %v), want (false, nil): a duplicate", got.ran, got.err)
		}
		if got.ran {
			t.Error("worker B ran fn for a key worker A had already recorded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker B's Once never returned after worker A committed")
	}
	select {
	case a := <-aDone:
		if !a.ran || a.err != nil {
			t.Errorf("worker A = (%v, %v), want (true, nil)", a.ran, a.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker A's Once never returned")
	}
	if !s.Has("k") {
		t.Error("the key is not recorded after a committed Once")
	}
}

func TestConfigRefusals(t *testing.T) {
	s := memStore(t, inbox.MemoryOptions{})
	for name, tc := range map[string]struct {
		cfg  inbox.Config
		want error
	}{
		"no store": {inbox.Config{Key: inbox.KeyFromID()}, inbox.ErrNoStore},
		"no key":   {inbox.Config{Store: s}, inbox.ErrNoKeyFunc},
	} {
		t.Run(name, func(t *testing.T) {
			if err := tc.cfg.Validate(); !errors.Is(err, tc.want) {
				t.Errorf("Validate = %v, want %v", err, tc.want)
			}
			if _, err := inbox.New(tc.cfg); !errors.Is(err, tc.want) {
				t.Errorf("New = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestKeyFuncs(t *testing.T) {
	t.Run("KeyFromHeader", func(t *testing.T) {
		fn := inbox.KeyFromHeader(keyHeader)
		if got, ok := fn(delivery("op-9")); !ok || got != "op-9" {
			t.Errorf("KeyFromHeader = (%q, %v)", got, ok)
		}
		if _, ok := fn(delivery("")); ok {
			t.Error("KeyFromHeader found a key on a delivery with no header")
		}
	})
	t.Run("KeyFromID", func(t *testing.T) {
		fn := inbox.KeyFromID()
		d := &mq.Delivery{Message: mq.Message{ID: "msg-1"}}
		if got, ok := fn(d); !ok || got != "msg-1" {
			t.Errorf("KeyFromID = (%q, %v)", got, ok)
		}
		if _, ok := fn(&mq.Delivery{}); ok {
			t.Error("KeyFromID found a key on a delivery with no ID")
		}
	})
}

func TestErrorsCarryTheirCode(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want mq.ErrorCode
	}{
		{"ErrNoStore", inbox.ErrNoStore, mq.CodeInvalidConfig},
		{"ErrNoKeyFunc", inbox.ErrNoKeyFunc, mq.CodeInvalidConfig},
		{"ErrEmptyKey", inbox.ErrEmptyKey, mq.CodeInvalidArgument},
		{"ErrMissingKey", inbox.ErrMissingKey, mq.CodeInvalidArgument},
		{"ErrFull", inbox.ErrFull, mq.CodeResourceExhausted},
		{"ErrClosed", inbox.ErrClosed, mq.CodeClosed},
	} {
		if got := mq.CodeOf(c.err); got != c.want {
			t.Errorf("CodeOf(%s) = %v, want %v", c.name, got, c.want)
		}
		if !strings.Contains(c.err.Error(), "iqmq/x/inbox") {
			t.Errorf("%s does not name its package: %q", c.name, c.err.Error())
		}
	}
}

// downStore is a Store that never reaches its database: it reports a
// retryable failure WITHOUT calling fn, which is exactly the shape the
// OnUnavailable policy is about.
type downStore struct{}

func (*downStore) Once(context.Context, string, func(context.Context) error) (bool, error) {
	return false, errcode.New(errcode.Unavailable, "the database is not answering")
}

func (*downStore) Close() error { return nil }

// taggingStore puts a value in the context it hands fn, standing in for a
// transaction handle.
type taggingStore struct {
	inner inbox.Store
	key   any
	value any
}

func (s *taggingStore) Once(ctx context.Context, key string, fn func(context.Context) error) (bool, error) {
	return s.inner.Once(ctx, key, func(inner context.Context) error {
		return fn(context.WithValue(inner, s.key, s.value))
	})
}

func (s *taggingStore) Close() error { return s.inner.Close() }
