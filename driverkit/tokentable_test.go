package driverkit_test

import (
	"errors"
	"sync"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// TestTokenTableLifecycle pins the state machine as a driver sees it: one
// terminal settlement wins, a second is refused, a race is refused, and an
// unknown outcome is sticky.
func TestTokenTableLifecycle(t *testing.T) {
	tbl, err := driverkit.NewTokenTable(4)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	tok, ok := tbl.Claim("receipt-1")
	if !ok {
		t.Fatal("Claim on an empty table failed")
	}
	if tbl.Live() != 1 || tbl.Cap() != 4 {
		t.Fatalf("Live is %d, Cap is %d", tbl.Live(), tbl.Cap())
	}
	if ref, rerr := tbl.Resolve(tok); rerr != nil || ref != "receipt-1" {
		t.Fatalf("Resolve: %v, %v", ref, rerr)
	}
	if st, serr := tbl.State(tok); serr != nil || st != driverkit.StateActive {
		t.Fatalf("State: %v, %v", st, serr)
	}
	ref, err := tbl.Settle(tok)
	if err != nil || ref != "receipt-1" {
		t.Fatalf("Settle: %v, %v", ref, err)
	}
	if _, err := tbl.Settle(tok); !errors.Is(err, mq.ErrSettlementInProgress) {
		t.Fatalf("a concurrent settlement: %v", err)
	}
	if err := tbl.Complete(tok, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if _, err := tbl.Settle(tok); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a settlement of a released slot: %v", err)
	}
	if tbl.Live() != 0 {
		t.Errorf("Live is %d after the slot was freed", tbl.Live())
	}
}

// TestTokenTableRefusalLeavesItSettleable is the rule that a definite refusal
// does not settle: the caller may try again.
func TestTokenTableRefusalLeavesItSettleable(t *testing.T) {
	tbl, err := driverkit.NewTokenTable(2)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	tok, _ := tbl.Claim(nil)
	if _, err := tbl.Settle(tok); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if err := tbl.Complete(tok, errors.New("broker said no")); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if st, _ := tbl.State(tok); st != driverkit.StateActive {
		t.Fatalf("a refused settlement left the delivery %v, want active", st)
	}
	if _, err := tbl.Settle(tok); err != nil {
		t.Fatalf("a second attempt after a refusal: %v", err)
	}
}

// TestTokenTableUnknownIsSticky is the hardest rule: an ambiguous settlement
// stays unknown for ever, and its slot is never reused — a slot handed out again
// under a token whose fate nobody knows is how an unknown settlement quietly
// becomes a successful one.
func TestTokenTableUnknownIsSticky(t *testing.T) {
	tbl, err := driverkit.NewTokenTable(2)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	tok, _ := tbl.Claim("r")
	if _, err := tbl.Settle(tok); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	ambiguous := errcode.New(errcode.SettlementUnknown, "the round trip timed out")
	if err := tbl.Complete(tok, ambiguous); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if st, _ := tbl.State(tok); st != driverkit.StateUnknown {
		t.Fatalf("state is %v, want unknown", st)
	}
	if _, err := tbl.Settle(tok); !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("a settlement of an unknown delivery: %v", err)
	}
	if tbl.Live() != 1 {
		t.Errorf("an unknown settlement released its slot: Live is %d", tbl.Live())
	}
}

// TestTokenTableInvalidateAll is the stale-settlement rule: after a rebalance
// or a reconnection every token handed out before the bump is worthless, and a
// settlement carrying one reports ErrLeaseLost rather than landing on the
// delivery that took the slot.
func TestTokenTableInvalidateAll(t *testing.T) {
	tbl, err := driverkit.NewTokenTable(2)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	old, _ := tbl.Claim("before")
	gen := tbl.Generation()
	if n := tbl.InvalidateAll(); n != 1 {
		t.Fatalf("InvalidateAll reported %d live deliveries, want 1", n)
	}
	if tbl.Generation() == gen {
		t.Fatal("InvalidateAll did not bump the generation")
	}
	fresh, ok := tbl.Claim("after")
	if !ok {
		t.Fatal("Claim after InvalidateAll failed")
	}
	if fresh == old {
		t.Fatal("a token from before the invalidation was handed out again: a stale settlement would land on a live delivery")
	}
	if _, err := tbl.Settle(old); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a stale settlement: %v", err)
	}
	if ref, _ := tbl.Resolve(fresh); ref != "after" {
		t.Errorf("the fresh slot resolves to %v", ref)
	}
}

// TestTokenTableFullAndRefusals pins the bounds.
func TestTokenTableFullAndRefusals(t *testing.T) {
	if _, err := driverkit.NewTokenTable(0); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("NewTokenTable(0): %v", err)
	}
	tbl, err := driverkit.NewTokenTable(2)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	for range 2 {
		if _, ok := tbl.Claim(nil); !ok {
			t.Fatal("Claim failed with room left")
		}
	}
	if _, ok := tbl.Claim(nil); ok {
		t.Fatal("Claim succeeded past the table's capacity: the prefetch bound is not a bound")
	}
	if _, err := tbl.Resolve(1 << 40); !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("a token out of range: %v", err)
	}
	tok, _ := tbl.Claim(nil)
	_ = tok
	if err := tbl.Complete(1<<40, nil); !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("Complete of an out-of-range token: %v", err)
	}
}

// TestTokenTableConcurrent races claims, settlements and an invalidation, which
// is a rebalance arriving while a consumer is settling.
func TestTokenTableConcurrent(t *testing.T) {
	tbl, err := driverkit.NewTokenTable(64)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 500 {
				tok, ok := tbl.Claim("r")
				if !ok {
					continue
				}
				if _, serr := tbl.Settle(tok); serr != nil {
					continue
				}
				_ = tbl.Complete(tok, nil)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 200 {
			tbl.InvalidateAll()
		}
	}()
	wg.Wait()
	if tbl.Live() < 0 || tbl.Live() > tbl.Cap() {
		t.Fatalf("Live is %d, outside [0, %d]", tbl.Live(), tbl.Cap())
	}
}

// TestAllocsTokenTable is the rule for the settlement path: resolving and
// settling a delivery allocates nothing. A table that boxed per settlement would
// put an allocation on every ack of every driver built on it.
func TestAllocsTokenTable(t *testing.T) {
	tbl, err := driverkit.NewTokenTable(8)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	ref := new(int) // the driver's own handle, boxed once by the caller
	if n := testing.AllocsPerRun(200, func() {
		tok, ok := tbl.Claim(ref)
		if !ok {
			t.Fatal("Claim failed")
		}
		if _, rerr := tbl.Resolve(tok); rerr != nil {
			t.Fatalf("Resolve: %v", rerr)
		}
		if _, serr := tbl.Settle(tok); serr != nil {
			t.Fatalf("Settle: %v", serr)
		}
		if cerr := tbl.Complete(tok, nil); cerr != nil {
			t.Fatalf("Complete: %v", cerr)
		}
	}); n != 0 {
		t.Fatalf("a claim, a resolve and a settlement allocated %.1f times, want 0", n)
	}
}

// TestTokenTableReleaseMakesItsTokenStale is the per-delivery half, and
// it is the one that used not to hold.
//
// [driverkit.TokenTable.Release] is documented as returning a slot "under a new
// generation, so that a token naming it is stale from here on", and it is what a
// driver calls for a delivery the broker has taken back — a lease expiry, a
// redelivery. Claim used to reset the slot's generation to the TABLE's, which
// undid the bump: the released token resolved to whatever delivery took the slot
// next. On a broker whose settlement is a reply subject the server still honours,
// that is an acknowledgement of the record another delivery owns.
func TestTokenTableReleaseMakesItsTokenStale(t *testing.T) {
	tbl, err := driverkit.NewTokenTable(1)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	old, ok := tbl.Claim("first")
	if !ok {
		t.Fatal("Claim")
	}
	if err := tbl.Release(old); err != nil {
		t.Fatalf("Release: %v", err)
	}
	fresh, ok := tbl.Claim("second")
	if !ok {
		t.Fatal("Claim after Release")
	}
	if fresh == old {
		t.Fatal("the released token was handed out again")
	}
	if ref, err := tbl.Resolve(old); err == nil {
		t.Fatalf("a released token still resolves, to %v: a settlement carrying it would land on the delivery that took the slot", ref)
	} else if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a released token resolved with %v, want ErrLeaseLost", err)
	}
	if _, err := tbl.Settle(old); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("settling a released token reported %v, want ErrLeaseLost", err)
	}
	if ref, err := tbl.Resolve(fresh); err != nil || ref != any("second") {
		t.Fatalf("the fresh token resolves to %v (%v)", ref, err)
	}
}

// TestTokenTableCompletedTokenIsStale is the same rule for the terminal
// settlement path: Complete frees the slot, so the settled delivery's token must
// not name the next delivery either.
func TestTokenTableCompletedTokenIsStale(t *testing.T) {
	tbl, err := driverkit.NewTokenTable(1)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	old, _ := tbl.Claim("first")
	if _, err := tbl.Settle(old); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if err := tbl.Complete(old, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	fresh, ok := tbl.Claim("second")
	if !ok {
		t.Fatal("Claim after Complete")
	}
	if fresh == old {
		t.Fatal("a settled delivery's token was handed out again")
	}
	if ref, err := tbl.Resolve(old); err == nil {
		t.Fatalf("a settled token still resolves, to %v", ref)
	}
}

// TestStatsCountsTheSlotsAPrefetchHasLost is the number a driver could not
// read.
//
// The specification makes an ambiguous settlement permanent and the table keeps the slot,
// correctly — so a source whose settlements are often ambiguous loses its
// prefetch a slot at a time, and until now nothing said how close it was.
// drivers/amqp10 had to count unknown slots itself to make the shrinking
// window visible.
func TestStatsCountsTheSlotsAPrefetchHasLost(t *testing.T) {
	tbl, err := driverkit.NewTokenTable(4)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	if got := tbl.Stats(); got.Cap != 4 || got.Live != 0 || got.Unknown != 0 || got.Settling != 0 {
		t.Fatalf("a fresh table reports %+v", got)
	}

	tokens := make([]uint64, 0, 3)
	for i := range 3 {
		tok, ok := tbl.Claim(i)
		if !ok {
			t.Fatalf("Claim %d was refused on a table of 4", i)
		}
		tokens = append(tokens, tok)
	}
	if got := tbl.Stats(); got.Live != 3 || got.Unknown != 0 {
		t.Fatalf("after three claims the table reports %+v, want Live 3 and no unknown", got)
	}

	// One settlement in flight, one whose outcome nobody knows.
	if _, err := tbl.Settle(tokens[0]); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if got := tbl.Stats(); got.Settling != 1 {
		t.Errorf("a settlement in flight is not counted: %+v", got)
	}
	// An error carrying CodeSettlementUnknown is what makes the outcome
	// sticky: the slot is not freed, because a slot reused under a token
	// whose fate nobody knows is how an unknown settlement becomes a
	// successful one by accident.
	ambiguous := mq.ErrSettlementUnknown
	if err := tbl.Complete(tokens[0], ambiguous); err != nil {
		t.Fatalf("Complete(ambiguous): %v", err)
	}
	got := tbl.Stats()
	if got.Unknown != 1 {
		t.Fatalf("an ambiguous settlement left Unknown at %d, want 1: this is the slot the prefetch has "+
			"lost for good, and the number a driver reports to say how close it is to having none", got.Unknown)
	}
	if got.Settling != 0 {
		t.Errorf("the completed settlement is still counted as in flight: %+v", got)
	}
	if got.Live == 0 {
		t.Errorf("the unknown slot is not live, so nothing holds the prefetch it consumed: %+v", got)
	}
}
