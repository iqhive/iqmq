package main

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// wantBody is what the example prints after the store's own name line.
//
// The store line differs between the arms -- that is the only difference, and
// it is the point: the protocol above the store is identical, and the SQL is
// the part only one arm executes.
const wantBody = "atomic: 3 order(s) and 3 outbox row(s) committed together; the rolled-back transaction left neither\n" +
	"relay: pass 1 claimed 3 row(s), published 3 and marked 2 sent — the last row's relay died before its mark\n" +
	"relay: pass 2 re-claimed that row once its 150ms lock lapsed and published it AGAIN, on attempt 2 — an outbox is at-least-once by construction\n" +
	"consumer: 4 publication(s) became 3 side effect(s); x/idempotency skipped 1 duplicate(s)\n" +
	"outbox: 0 row(s) left unsent\n"

// TestRunInProcess is the arm that never skips.
//
// The store is the in-process stand-in, which models the three properties the
// relay rests on and executes no SQL. That limit is stated rather than
// implied: TestRunAgainstPostgres is what runs the schema, the partial index
// and the FOR UPDATE SKIP LOCKED claim.
func TestRunInProcess(t *testing.T) {
	store := newMemStore()
	defer func() { _ = store.Close() }()
	reportArm(t, armInProcess, statusRan, "in-process store (no SQL)")
	runExample(t, store, "in-memory (no SQL)")
}

// runExample holds the example to its output. One body for both arms.
func runExample(t *testing.T, store Store, wantStore string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	var out bytes.Buffer
	c, err := run(ctx, &out, store)
	if err != nil {
		t.Fatalf("run: %v\noutput so far:\n%s", err, out.String())
	}
	want := "store: " + wantStore + "\n" + wantBody
	if got := out.String(); got != want {
		t.Errorf("the example printed\n%s\nwant\n%s", got, want)
	}
	// The numbers, asserted beside the prose so that a reworded line cannot
	// quietly change what was measured.
	if c.published != 4 || c.handled != 3 || c.skipped != 1 {
		t.Errorf("published=%d handled=%d skipped=%d, want 4/3/1", c.published, c.handled, c.skipped)
	}
	if c.claims.Seen != 4 || c.claims.Processed != 3 || c.claims.Skipped != 1 {
		t.Errorf("the guard saw=%d processed=%d skipped=%d, want 4/3/1", c.claims.Seen, c.claims.Processed, c.claims.Skipped)
	}
	if c.claims.CommitLost != 0 || c.claims.Failed != 0 {
		t.Errorf("the guard reported commit-lost=%d failed=%d, want none", c.claims.CommitLost, c.claims.Failed)
	}
}

// TestTwoRelaysClaimDisjointRows is the atomicity of the claim: eight relays
// claiming at once must partition the rows between them, never share one.
//
// What it catches, and this is stated narrowly because three plausible
// defects were tried against it and only one was caught :
//
//   - on the IN-PROCESS store it catches a claim that does not hold its lock
//     across the read and the write. Watched failing with memStore.Claim's
//     mutex removed, with and without -race.
//   - on POSTGRES it catches nothing that was tried. Removing `SKIP LOCKED`
//     passes. Removing `FOR UPDATE` passes. Splitting the claim into a SELECT
//     and a separate UPDATE passes. Eight statements against forty rows
//     serialise faster than eight goroutines can interleave: the first takes
//     every row and the rest then see locked_until already set, so the
//     contention the test is named for never happens.
//
// So the SQL property is asserted somewhere else, by an observation that does
// not depend on winning a race:
// TestPostgresClaimSkipsARowAnotherTransactionHolds holds a row with a
// transaction of its own, and a claim without SKIP LOCKED then BLOCKS instead
// of stepping over it.
func TestTwoRelaysClaimDisjointRows(t *testing.T) {
	runDisjointClaims(t, newMemStore())
}

// runDisjointClaims is the store contract both arms run.
func runDisjointClaims(t *testing.T, store Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	const rows = 40
	for i := range rows {
		key := "k" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		if err := store.Place(ctx, key, []Outgoing{{Key: key, Body: key}}, false); err != nil {
			t.Fatalf("Place: %v", err)
		}
	}
	const relays = 8
	var (
		mu    sync.Mutex
		seen  = map[int64]int{}
		total int
		wg    sync.WaitGroup
	)
	start := make(chan struct{})
	for range relays {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got, err := store.Claim(ctx, rows)
			if err != nil {
				mu.Lock()
				t.Errorf("Claim: %v", err)
				mu.Unlock()
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, r := range got {
				seen[r.ID]++
				total++
			}
		}()
	}
	close(start)
	wg.Wait()
	for id, n := range seen {
		if n != 1 {
			t.Errorf("row %d was claimed by %d relays at once; a row claimed twice is published twice", id, n)
		}
	}
	if total != rows {
		t.Errorf("%d relays claimed %d row(s) between them, want %d: every unsent row must be claimable by exactly one of them", relays, total, rows)
	}
}

// TestARolledBackTransactionWritesNeither is the atomicity property on its
// own, so that a failure names it rather than failing the whole example.
func TestARolledBackTransactionWritesNeither(t *testing.T) {
	runRollback(t, newMemStore())
}

func runRollback(t *testing.T, store Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := store.Place(ctx, "ord-ok", []Outgoing{{Key: "ord-ok", Body: "ord-ok"}}, false); err != nil {
		t.Fatalf("Place: %v", err)
	}
	err := store.Place(ctx, "ord-bad", []Outgoing{{Key: "ord-bad", Body: "ord-bad"}}, true)
	if err == nil {
		t.Fatal("a rolled-back Place reported no error")
	}
	got, err := store.Orders(ctx)
	if err != nil {
		t.Fatalf("Orders: %v", err)
	}
	if len(got) != 1 || got[0] != "ord-ok" {
		t.Errorf("the orders table holds %v, want [ord-ok]: the rolled-back transaction wrote its order row", got)
	}
	unsent, err := store.Unsent(ctx)
	if err != nil {
		t.Fatalf("Unsent: %v", err)
	}
	if unsent != 1 {
		t.Errorf("the outbox holds %d unsent row(s), want 1: the rolled-back transaction wrote its outbox row", unsent)
	}
}

// TestTheLockIsShorterThanTheWaitForIt holds the example's own arithmetic to
// something a reader can check.
//
// A lock that outlasted pastTheLock would make the second relay pass find
// nothing, and the duplicate the whole example is about would never happen --
// so the run would fail on a timing constant rather than on a property.
func TestTheLockIsShorterThanTheWaitForIt(t *testing.T) {
	if lockFor >= pastTheLock {
		t.Fatalf("lockFor is %v and pastTheLock is %v: the second relay pass would find nothing", lockFor, pastTheLock)
	}
	if !strings.Contains(wantBody, lockFor.String()) {
		t.Errorf("the expected output does not mention the lock duration %v, so a change to it would not be visible", lockFor)
	}
}

// TestTheKeyIsTheOperationAndNotThePublication is the rule that makes the
// whole thing work, checked where it is decided.
//
// A relay that generated a key per publication would make every republication
// a new operation: the consumer could not tell a duplicate from a new order,
// and x/idempotency would answer StatusFirst for both -- the specification's
// false first, produced on purpose. So the key is the outbox row's, and the
// outbox row's is the order's.
func TestTheKeyIsTheOperationAndNotThePublication(t *testing.T) {
	ctx := t.Context()
	store := newMemStore()
	defer func() { _ = store.Close() }()
	if err := place(ctx, store, "ord-1", false); err != nil {
		t.Fatalf("place: %v", err)
	}
	one, err := store.Claim(ctx, 1)
	if err != nil || len(one) != 1 {
		t.Fatalf("Claim = %v, %v", one, err)
	}
	if one[0].Key != "ord-1" {
		t.Errorf("the outbox row's key is %q, want the order's own identifier %q", one[0].Key, "ord-1")
	}
	// And the re-claim carries the SAME key, which is the property.
	if err := sleepPast(ctx, pastTheLock); err != nil {
		t.Fatal(err)
	}
	again, err := store.Claim(ctx, 1)
	if err != nil || len(again) != 1 {
		t.Fatalf("the re-Claim = %v, %v", again, err)
	}
	if again[0].Key != one[0].Key {
		t.Errorf("the re-claimed row carries key %q, the first claim carried %q: a republished row has to be recognisable as the same operation",
			again[0].Key, one[0].Key)
	}
	if again[0].ID != one[0].ID {
		t.Errorf("the re-claim produced row %d, want the same row %d", again[0].ID, one[0].ID)
	}
}
