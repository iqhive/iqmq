package idempotency_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/idempotency"
)

// keyHeader is the header the tests derive a key from. It is not under the specification's
// reserved prefix, which TestAReservedHeaderIsUnwritable explains.
const keyHeader = "idempotency-key"

// delivery builds a Delivery a caller owns: no settlement right and no
// metadata, which is all the middleware reads. The specification makes that
// explicit — a caller-built Delivery's Info() is the zero value — and the
// middleware never settles, so this is the whole fixture.
func delivery(key, body string) *mq.Delivery {
	d := &mq.Delivery{Message: mq.Message{Body: []byte(body)}}
	if key != "" {
		d.Headers.Set(keyHeader, []byte(key))
	}
	return d
}

// counting is a handler that records every body it was given.
type counting struct {
	bodies []string
	err    error
}

func (c *counting) Handle(_ context.Context, d *mq.Delivery) error {
	c.bodies = append(c.bodies, string(d.Body))
	return c.err
}

// newGuard wires an interceptor over a fresh in-process store.
func newGuard(t *testing.T, mutate ...func(*idempotency.Config)) (*idempotency.Interceptor, *idempotency.MemoryStore) {
	t.Helper()
	store, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{Clock: newTestClock()})
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	cfg := idempotency.Config{
		Store:  store,
		Key:    idempotency.KeyFromHeader(keyHeader),
		Window: time.Minute,
	}
	for _, m := range mutate {
		m(&cfg)
	}
	in, err := idempotency.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return in, store
}

// TestADuplicateSkipsTheHandlerAndIsAcked is the middleware's whole purpose.
func TestADuplicateSkipsTheHandlerAndIsAcked(t *testing.T) {
	in, _ := newGuard(t)
	h := &counting{}
	guarded := in.Middleware()(h)
	for range 3 {
		if err := guarded.Handle(t.Context(), delivery("k1", "payload")); err != nil {
			t.Fatalf("the guarded handler reported %v; a duplicate must be ACKED, not retried", err)
		}
	}
	if len(h.bodies) != 1 {
		t.Fatalf("the handler was called %d times for one key, want 1 (bodies: %v)", len(h.bodies), h.bodies)
	}
	c := in.Counts()
	if c.Seen != 3 || c.Processed != 1 || c.Skipped != 2 {
		t.Errorf("counts were seen=%d processed=%d skipped=%d, want 3/1/2", c.Seen, c.Processed, c.Skipped)
	}
}

// TestAnOpenClaimIsRedeliveredAndNotSkipped is the sharpest rule in the
// package, as a test rather than a paragraph.
//
// A key another worker holds an open claim on is not a key whose work is done.
// The guarded handler must report an error so the runner redelivers, because
// returning nil would ack a message nobody has processed — and if the holder
// then dies, that message is gone.
func TestAnOpenClaimIsRedeliveredAndNotSkipped(t *testing.T) {
	in, store := newGuard(t)
	// The claim the "other worker" holds: taken directly on the store, and
	// never committed.
	if _, err := store.Claim(t.Context(), "k1", time.Minute); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	h := &counting{}
	err := in.Middleware()(h).Handle(t.Context(), delivery("k1", "payload"))
	if err == nil {
		t.Fatal("the guarded handler ACKED a delivery whose key another worker holds an open claim on; the work may never happen and the message is gone")
	}
	if !errors.Is(err, idempotency.ErrInFlight) {
		t.Fatalf("the guarded handler reported %v, want ErrInFlight", err)
	}
	if dir, _ := mq.DirectiveOf(err); dir == mq.DirectiveDiscard || dir == mq.DirectiveDeadLetter {
		t.Fatalf("an in-flight collision carries directive %v; it must be a retry", dir)
	}
	if len(h.bodies) != 0 {
		t.Errorf("the handler ran %d times, want 0", len(h.bodies))
	}
	if got := in.Counts().Deferred; got != 1 {
		t.Errorf("Counts().Deferred = %d, want 1", got)
	}
}

// TestInFlightRetryAfterCarriesTheDelay says the configurable delay reaches
// the directive rather than being dropped.
func TestInFlightRetryAfterCarriesTheDelay(t *testing.T) {
	const delay = 250 * time.Millisecond
	in, store := newGuard(t, func(c *idempotency.Config) { c.InFlightRetryAfter = delay })
	if _, err := store.Claim(t.Context(), "k1", time.Minute); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	err := in.Middleware()(&counting{}).Handle(t.Context(), delivery("k1", "x"))
	dir, d := mq.DirectiveOf(err)
	if dir != mq.DirectiveRetryAfter {
		t.Fatalf("the directive is %v, want DirectiveRetryAfter", dir)
	}
	if d != delay {
		t.Errorf("the delay is %v, want %v", d, delay)
	}
}

// TestAFailedHandlerReleasesTheClaim holds the two-phase claim to its reason
// for existing: a handler that failed has NOT done the work, so the
// redelivery must be processed rather than skipped.
func TestAFailedHandlerReleasesTheClaim(t *testing.T) {
	in, _ := newGuard(t)
	boom := errors.New("the handler failed")
	h := &counting{err: boom}
	guarded := in.Middleware()(h)
	if err := guarded.Handle(t.Context(), delivery("k1", "first")); !errors.Is(err, boom) {
		t.Fatalf("the guarded handler reported %v, want the handler's own error", err)
	}
	// The redelivery. The claim was aborted, so this is StatusFirst again.
	h.err = nil
	if err := guarded.Handle(t.Context(), delivery("k1", "second")); err != nil {
		t.Fatalf("the redelivery reported %v", err)
	}
	if len(h.bodies) != 2 {
		t.Fatalf("the handler was called %d times, want 2: a failed handler's claim must be released or its redelivery is skipped for a whole window (bodies: %v)", len(h.bodies), h.bodies)
	}
	c := in.Counts()
	if c.Failed != 1 || c.Processed != 2 {
		t.Errorf("counts were failed=%d processed=%d, want 1/2", c.Failed, c.Processed)
	}
}

// brokenStore is a Store that cannot answer. It is the outage, not a fake
// Redis: what the middleware does about an unreachable store is decided by
// Config.OnUnavailable and nothing else.
type brokenStore struct{ err error }

func (b brokenStore) Claim(context.Context, string, time.Duration) (idempotency.Claim, error) {
	return idempotency.Claim{}, b.err
}
func (b brokenStore) Commit(context.Context, idempotency.Claim) error { return b.err }
func (b brokenStore) Abort(context.Context, idempotency.Claim) error  { return b.err }
func (b brokenStore) Close() error                                    { return nil }

// TestAnUnreachableStoreFailsOpenAndSaysSo is the default, with
// both halves asserted: the handler runs, and the delivery is counted as
// UNGUARDED rather than as processed.
func TestAnUnreachableStoreFailsOpenAndSaysSo(t *testing.T) {
	down := errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")
	in, err := idempotency.New(idempotency.Config{
		Store: brokenStore{err: down}, Key: idempotency.KeyFromHeader(keyHeader), Window: time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := &counting{}
	if err := in.Middleware()(h).Handle(t.Context(), delivery("k1", "payload")); err != nil {
		t.Fatalf("the default policy reported %v; OnUnavailableProcess runs the handler", err)
	}
	if len(h.bodies) != 1 {
		t.Fatalf("the handler ran %d times, want 1", len(h.bodies))
	}
	c := in.Counts()
	if c.Unguarded != 1 {
		t.Errorf("Counts().Unguarded = %d, want 1: a delivery processed without a claim has to be visible", c.Unguarded)
	}
	if c.Processed != 0 {
		t.Errorf("Counts().Processed = %d, want 0: an unguarded delivery is not a claimed one", c.Processed)
	}
}

// TestOnUnavailableRetryRefuses is the other policy, and its cost.
func TestOnUnavailableRetryRefuses(t *testing.T) {
	down := errors.New("i/o timeout")
	in, err := idempotency.New(idempotency.Config{
		Store: brokenStore{err: down}, Key: idempotency.KeyFromHeader(keyHeader),
		Window: time.Minute, OnUnavailable: idempotency.OnUnavailableRetry,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := &counting{}
	got := in.Middleware()(h).Handle(t.Context(), delivery("k1", "payload"))
	if !errors.Is(got, down) {
		t.Fatalf("the guarded handler reported %v, want the store's own error so that a log line says WHY nothing was processed", got)
	}
	if len(h.bodies) != 0 {
		t.Errorf("the handler ran %d times under OnUnavailableRetry, want 0", len(h.bodies))
	}
	if in.Counts().Refused != 1 {
		t.Errorf("Counts().Refused = %d, want 1", in.Counts().Refused)
	}
}

// lostCommitStore claims normally and loses every commit, which is the state
// The specification is about.
type lostCommitStore struct{ *idempotency.MemoryStore }

func (lostCommitStore) Commit(context.Context, idempotency.Claim) error {
	return idempotency.ErrClaimLost
}

// TestALostCommitStillAcks pins.
//
// The handler SUCCEEDED. Nacking would repeat work that provably completed, so
// the delivery is acked and the loss is counted: each one is a future
// duplicate this store will not catch, and Counts().CommitLost is the only
// place that says so.
func TestALostCommitStillAcks(t *testing.T) {
	mem, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{Clock: newTestClock()})
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	defer func() { _ = mem.Close() }()
	in, err := idempotency.New(idempotency.Config{
		Store: lostCommitStore{MemoryStore: mem}, Key: idempotency.KeyFromHeader(keyHeader), Window: time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := &counting{}
	if err := in.Middleware()(h).Handle(t.Context(), delivery("k1", "payload")); err != nil {
		t.Fatalf("a successful handler whose commit was lost reported %v, want nil: nacking would repeat work that succeeded", err)
	}
	if got := in.Counts().CommitLost; got != 1 {
		t.Errorf("Counts().CommitLost = %d, want 1", got)
	}
}

// TestTheMissingKeyPolicies covers both.
func TestTheMissingKeyPolicies(t *testing.T) {
	t.Run("process", func(t *testing.T) {
		in, _ := newGuard(t)
		h := &counting{}
		if err := in.Middleware()(h).Handle(t.Context(), delivery("", "payload")); err != nil {
			t.Fatalf("the default policy reported %v", err)
		}
		if len(h.bodies) != 1 {
			t.Errorf("the handler ran %d times, want 1", len(h.bodies))
		}
		if in.Counts().NoKey != 1 {
			t.Errorf("Counts().NoKey = %d, want 1", in.Counts().NoKey)
		}
	})
	t.Run("fail", func(t *testing.T) {
		in, _ := newGuard(t, func(c *idempotency.Config) { c.OnMissingKey = idempotency.MissingKeyFail })
		h := &counting{}
		err := in.Middleware()(h).Handle(t.Context(), delivery("", "payload"))
		if !errors.Is(err, idempotency.ErrMissingKey) {
			t.Fatalf("MissingKeyFail reported %v, want ErrMissingKey", err)
		}
		if len(h.bodies) != 0 {
			t.Errorf("the handler ran %d times, want 0", len(h.bodies))
		}
	})
}

// TestConfigRefusesWhatItCannotDo holds the three required fields, and the
// absent default for Key in particular.
func TestConfigRefusesWhatItCannotDo(t *testing.T) {
	store, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{})
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	cases := []struct {
		name string
		cfg  idempotency.Config
		want error
	}{
		{"no store", idempotency.Config{Key: idempotency.KeyFromID(), Window: time.Minute}, idempotency.ErrNoStore},
		{"no key func", idempotency.Config{Store: store, Window: time.Minute}, idempotency.ErrNoKeyFunc},
		{"no window", idempotency.Config{Store: store, Key: idempotency.KeyFromID()}, idempotency.ErrBadWindow},
		{"negative lease", idempotency.Config{Store: store, Key: idempotency.KeyFromID(), Window: time.Minute, InFlightLease: -time.Second}, idempotency.ErrBadInFlightLease},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := idempotency.New(c.cfg); !errors.Is(err, c.want) {
				t.Fatalf("New reported %v, want %v", err, c.want)
			}
		})
	}
}

// TestKeyFromIDAndKeyFromHeader covers the two shipped derivations, including
// the empty cases both must report as "no key" rather than as the empty key
// (which the store refuses, and which would otherwise make every keyless
// message a duplicate of the first).
func TestKeyFromIDAndKeyFromHeader(t *testing.T) {
	fromID := idempotency.KeyFromID()
	if k, ok := fromID(&mq.Delivery{Message: mq.Message{ID: "ord-1"}}); !ok || k != "ord-1" {
		t.Errorf("KeyFromID = %q, %v; want \"ord-1\", true", k, ok)
	}
	if _, ok := fromID(&mq.Delivery{}); ok {
		t.Error("KeyFromID found a key on a delivery with no ID")
	}
	fromHeader := idempotency.KeyFromHeader(keyHeader)
	if k, ok := fromHeader(delivery("k1", "b")); !ok || k != "k1" {
		t.Errorf("KeyFromHeader = %q, %v; want \"k1\", true", k, ok)
	}
	if _, ok := fromHeader(&mq.Delivery{}); ok {
		t.Error("KeyFromHeader found a key on a delivery with no headers")
	}
	// A present-but-empty header is "no key" and not the empty key.
	empty := &mq.Delivery{}
	empty.Headers.Set(keyHeader, []byte{})
	if _, ok := fromHeader(empty); ok {
		t.Error("KeyFromHeader read an empty header value as a key; the store would refuse it and every keyless message would collide")
	}
}

// TestAReservedHeaderIsUnwritable is why KeyFromHeader's documentation refuses
// the reserved prefix instead of the code doing it.
//
// A KeyFunc has no way to report a configuration error, so nothing stops
// KeyFromHeader("iqmq-key"). What stops it being useful is one layer up: the specification
// reserves the prefix for iqmq's own headers and Headers.Validate refuses a
// caller's, so a publisher cannot write one and the derived key would never be
// found — a middleware that silently guards nothing.
func TestAReservedHeaderIsUnwritable(t *testing.T) {
	var h mq.Headers
	h.Set(mq.ReservedHeaderPrefix+"key", []byte("k1"))
	if err := h.Validate(mq.Limits{}, true); err == nil {
		t.Fatal("a caller's header under the reserved prefix validated; KeyFromHeader's documented refusal rests on this refusal")
	}
}

// TestAPanickingHandlerReleasesItsClaim is the worst case, and it is a test because the consequence is silent.
//
// The specification recovers a panicking handler unless the caller asked it not to. Without
// the deferred abort the panic unwinds past the release, the claim is held for
// the whole window, and the redelivery then reads StatusInFlight, is deferred,
// and consumes attempts until a max-attempts policy dead-letters a message
// nothing has processed. A panic is exactly when the claim should be released.
//
// The panic itself has to carry on, or the runner's own recovery — and
// mq.WithoutRecovery — would no longer be in charge of what a panic means.
func TestAPanickingHandlerReleasesItsClaim(t *testing.T) {
	in, store := newGuard(t)
	boom := "the handler panicked"
	panicking := mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		panic(boom)
	})
	guarded := in.Middleware()(panicking)

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("the middleware swallowed the handler's panic; the recovery and mq.WithoutRecovery decide what a panic means, not this package")
			}
			if s, ok := r.(string); !ok || s != boom {
				t.Fatalf("the middleware re-panicked with %v, want the handler's own value %q", r, boom)
			}
		}()
		_ = guarded.Handle(t.Context(), delivery("k1", "payload"))
	}()

	// The claim must be gone, so a redelivery is PROCESSED rather than
	// deferred for a whole window.
	c, err := store.Claim(t.Context(), "k1", time.Minute)
	if err != nil {
		t.Fatalf("Claim after the panic: %v", err)
	}
	if c.Status != idempotency.StatusFirst {
		t.Fatalf("after a panicking handler the key reads %v, want %v: the claim was not released and the redelivery would be deferred for a whole window",
			c.Status, idempotency.StatusFirst)
	}
	if got := in.Counts().Failed; got != 1 {
		t.Errorf("Counts().Failed = %d after a panic, want 1: a panic is a failed delivery", got)
	}
}

// TestADeadHoldersClaimLapsesAfterTheLease is the crash case: a worker that
// died mid-handler leaves an open claim, and its redelivery must be processed
// once the in-flight lease lapses, not deferred for the whole Window until a
// max-attempts policy dead-letters it. The committed claim still lasts the
// whole Window.
func TestADeadHoldersClaimLapsesAfterTheLease(t *testing.T) {
	clk := newTestClock()
	store, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{Clock: clk})
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	in, err := idempotency.New(idempotency.Config{
		Store: store, Key: idempotency.KeyFromHeader(keyHeader),
		Window: 24 * time.Hour, InFlightLease: time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := &counting{}
	guarded := in.Middleware()(h)
	// The dead worker: it took the claim and never settled it.
	var redelivered, afterLease error
	dying := in.Middleware()(mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		redelivered = guarded.Handle(t.Context(), delivery("k1", "redelivery"))
		clk.Advance(time.Minute + time.Second)
		afterLease = guarded.Handle(t.Context(), delivery("k1", "after the lease"))
		return nil
	}))
	_ = dying.Handle(t.Context(), delivery("k1", "first"))
	if !errors.Is(redelivered, idempotency.ErrInFlight) {
		t.Fatalf("a redelivery inside the lease reported %v, want ErrInFlight", redelivered)
	}
	if afterLease != nil || len(h.bodies) != 1 {
		t.Fatalf("a redelivery after the lease reported %v and ran the handler %d time(s), want nil and 1: the open claim outlived its lease", afterLease, len(h.bodies))
	}
	clk.Advance(time.Hour)
	if err := guarded.Handle(t.Context(), delivery("k1", "duplicate")); err != nil || len(h.bodies) != 1 {
		t.Fatalf("a duplicate an hour after the commit reported %v and ran the handler %d time(s), want nil and 1: the committed claim took the lease, not the Window", err, len(h.bodies))
	}
}
