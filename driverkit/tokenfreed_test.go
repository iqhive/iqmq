package driverkit_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The release signal and.
//
// A full table is a condition a driver WAITS on rather than a bug: a settlement
// writes its frame before it frees its slot and the broker answers that frame
// with the next delivery, so a healthy consumer meets a full table. Two drivers answered it by polling the table at sub-millisecond
// intervals, because there was nothing to wait on.

// fullTable is a table with every slot claimed, and the tokens it handed out.
func fullTable(t *testing.T, size int) (*driverkit.TokenTable, []uint64) {
	t.Helper()
	tbl, err := driverkit.NewTokenTable(size)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	tokens := make([]uint64, 0, size)
	for i := range size {
		token, ok := tbl.Claim(i)
		if !ok {
			t.Fatalf("claiming slot %d of %d: the table refused", i, size)
		}
		tokens = append(tokens, token)
	}
	if _, ok := tbl.Claim("one too many"); ok {
		t.Fatal("a table with every slot claimed handed out another")
	}
	return tbl, tokens
}

// park is the channel a waiter on a FULL table is handed, and it asserts that
// the table really is full -- a helper that silently claimed a slot would make
// every assertion below vacuous.
func park(t *testing.T, tbl *driverkit.TokenTable) <-chan struct{} {
	t.Helper()
	token, ok, p := tbl.ParkForTest("waiter")
	if ok {
		t.Fatalf("ClaimOrWait handed out token %d on a table that should be full", token)
	}
	if p == nil {
		t.Fatal("ClaimOrWait refused the claim and had no broadcast to wait on")
	}
	return p
}

// closed reports whether a broadcast channel has already been closed, without
// blocking. A closed channel is the release the waiter must not miss.
func closed(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// TestTokenTableClaimOrWaitWakesOnARelease is the signal's whole purpose: a waiter
// parks and a release wakes it, rather than a timer deciding when to look.
func TestTokenTableClaimOrWaitWakesOnARelease(t *testing.T) {
	tbl, tokens := fullTable(t, 2)
	freed := park(t, tbl)
	if closed(freed) {
		t.Fatal("the broadcast is already closed although nothing has been released")
	}
	go func() {
		// The release the waiter is waiting for. It happens on another
		// goroutine because that is where it happens in a driver: the
		// settlement that frees the slot is a caller's, and the waiter is the
		// pump.
		_ = tbl.Release(tokens[0])
	}()
	select {
	case <-freed:
	case <-time.After(10 * time.Second):
		t.Fatal("a waiter parked in ClaimOrWait was not woken by a Release: without the wake the only thing that " +
			"ends the wait is a poll, which is what removes")
	}
	if _, ok := tbl.Claim("woken"); !ok {
		t.Fatal("the slot the release freed could not be claimed after the wake")
	}
}

// TestTokenTableClaimOrWaitHandsBackTheChannelTheNextReleaseCloses is the
// lost-wakeup question, stated as the contract rather than as an ordering.
//
// The channel a waiter is holding has to be the one the next release closes.
// If ClaimOrWait parked on a channel that a release passing by in the same
// instant had already replaced, the waiter would park for ever on a table with
// a free slot in it -- and without the poll this replaced, for ever means the
// delivery is lost rather than delayed by 200 microseconds.
func TestTokenTableClaimOrWaitHandsBackTheChannelTheNextReleaseCloses(t *testing.T) {
	tbl, tokens := fullTable(t, 1)
	freed := park(t, tbl)
	// The release happens entirely after the waiter has its channel.
	if err := tbl.Release(tokens[0]); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !closed(freed) {
		t.Fatal("the broadcast ClaimOrWait parks on is still open after a release: a waiter holding it would " +
			"park and never be woken, which is the lost wakeup the one-critical-section rule exists to prevent")
	}
}

// TestTokenTableClaimOrWaitWakesEveryWaiter pins that the signal is a BROADCAST. A
// prefetch-sized table can have several deliveries waiting, and a wake-up that
// reached one of them would leave the rest on nothing.
func TestTokenTableClaimOrWaitWakesEveryWaiter(t *testing.T) {
	tbl, tokens := fullTable(t, 4)
	waiters := make([]<-chan struct{}, 3)
	for i := range waiters {
		waiters[i] = park(t, tbl)
	}
	if err := tbl.Release(tokens[0]); err != nil {
		t.Fatalf("Release: %v", err)
	}
	for i, w := range waiters {
		if !closed(w) {
			t.Fatalf("waiter %d was not woken: ClaimOrWait parks on a broadcast, and several waiters on one table is the "+
				"ordinary case for a prefetch-sized table", i)
		}
	}
}

// TestTokenTableClaimOrWaitWakesOnACompletedSettlement covers the release a driver
// actually makes most of the time: `Complete` with a nil error, which settles
// the delivery and returns the slot.
func TestTokenTableClaimOrWaitWakesOnACompletedSettlement(t *testing.T) {
	tbl, tokens := fullTable(t, 2)
	if _, err := tbl.Settle(tokens[1]); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	freed := park(t, tbl)
	if err := tbl.Complete(tokens[1], nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if !closed(freed) {
		t.Fatal("a settlement the broker took did not wake a waiter, although it freed the slot: it is the " +
			"release a driver makes on every acknowledgement")
	}
}

// TestTokenTableClaimOrWaitIsNotWokenByAStickyUnknown is the other half of the same
// rule: an ambiguous settlement keeps its slot for ever, so it frees
// nothing and must not pretend to.
//
// Waking on it would be worse than useless: the waiter would find the table
// still full, loop, and spin on a signal that fires without a slot behind it.
func TestTokenTableClaimOrWaitIsNotWokenByAStickyUnknown(t *testing.T) {
	tbl, tokens := fullTable(t, 2)
	if _, err := tbl.Settle(tokens[0]); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	freed := park(t, tbl)
	unknown := errcode.New(errcode.SettlementUnknown, "driverkit: the broker never answered")
	if err := tbl.Complete(tokens[0], unknown); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if closed(freed) {
		t.Fatal("an ambiguous settlement woke a waiter although the specification keeps its slot for ever: the waiter would " +
			"find the table full and spin")
	}
	if _, ok := tbl.Claim("should not fit"); ok {
		t.Fatal("the unknown slot was handed out again, which the specification forbids")
	}
}

// TestTokenTableClaimOrWaitWakesOnInvalidateAll is the exit a driver's TEARDOWN
// depends on. A reconnection, a rebalance or a close invalidates every token,
// and a wait that ended only on a per-slot release would outlive the
// subscription it belongs to.
func TestTokenTableClaimOrWaitWakesOnInvalidateAll(t *testing.T) {
	tbl, _ := fullTable(t, 3)
	freed := park(t, tbl)
	if n := tbl.InvalidateAll(); n != 3 {
		t.Fatalf("InvalidateAll reported %d live deliveries, want 3", n)
	}
	if !closed(freed) {
		t.Fatal("InvalidateAll did not wake a waiter although it freed every slot")
	}
}

// TestTokenTableClaimOrWaitAfterAWakeIsAFreshChannel pins that a waiter which
// loops gets a channel for the NEXT release rather than the one it has already
// been woken by. Without it the second round of a wait returns instantly, for
// ever, and the wait becomes the busy spin it replaced.
func TestTokenTableClaimOrWaitAfterAWakeIsAFreshChannel(t *testing.T) {
	tbl, tokens := fullTable(t, 2)
	first := park(t, tbl)
	if err := tbl.Release(tokens[0]); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !closed(first) {
		t.Fatal("the release did not wake the waiter")
	}
	// The woken waiter takes the slot, which fills the table again -- this is
	// the loop a real waiter runs, and the point is what it is handed NEXT.
	retaken, ok, again := tbl.ParkForTest("woken")
	if !ok {
		t.Fatal("the woken waiter could not claim the slot the release freed")
	}
	if again != nil {
		t.Fatal("a successful claim also handed back a channel to wait on")
	}
	second := park(t, tbl)
	if closed(second) {
		t.Fatal("the channel handed out after a wake is already closed: a waiter that looped would spin " +
			"instead of parking")
	}
	if err := tbl.Release(tokens[1]); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !closed(second) {
		t.Fatal("the next release did not wake the waiter that looped")
	}
	if _, err := tbl.Settle(retaken); err != nil {
		t.Fatalf("settling the delivery the woken waiter took: %v", err)
	}
}

// TestTokenTableClaimOrWaitWakeIsNotAClaim says what the signal does NOT promise. Two
// waiters wake on one release and only one of them gets the slot, so a caller
// has to loop rather than assume; a driver that read a wake as a claim would
// hand out a token the table never issued.
func TestTokenTableClaimOrWaitWakeIsNotAClaim(t *testing.T) {
	tbl, tokens := fullTable(t, 2)
	freed := park(t, tbl)
	if err := tbl.Release(tokens[0]); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !closed(freed) {
		t.Fatal("the release did not wake the waiter")
	}
	if _, ok := tbl.Claim("first"); !ok {
		t.Fatal("the first waiter could not claim the freed slot")
	}
	if _, ok := tbl.Claim("second"); ok {
		t.Fatal("a second waiter woken by the same release also claimed a slot: one release frees one slot, " +
			"and the wake is a wake rather than a reservation")
	}
	// And the table is still the same table: the tokens it handed out before
	// are unaffected by anybody's wake.
	if _, err := tbl.Settle(tokens[1]); err != nil {
		t.Fatalf("settling an untouched delivery after a wake: %v", err)
	}
	if _, err := tbl.Settle(tokens[0]); !errorsIsLeaseLost(err) {
		t.Fatalf("settling the released delivery reported %v, want ErrLeaseLost", err)
	}
}

// errorsIsLeaseLost is the one comparison the test above makes on an error.
func errorsIsLeaseLost(err error) bool { return err != nil && mq.CodeOf(err) == mq.CodeLeaseLost }

// TestClaimOrWaitNeverLeavesAWaiterParkedWithASlotFree is the property the
// lock exists for, hammered rather than reasoned about.
//
// Many waiters and many releasers run against a table too small for them. A
// waiter that is handed a channel which is never closed while a slot is free
// is a delivery nobody will ever hand out; before the release signal that cost
// 200 microseconds of polling, and now it would cost the message. Each waiter
// therefore has a bounded patience, and exceeding it is the failure.
func TestClaimOrWaitNeverLeavesAWaiterParkedWithASlotFree(t *testing.T) {
	const (
		size    = 4
		waiters = 8
		rounds  = 200
	)
	tbl, err := driverkit.NewTokenTable(size)
	if err != nil {
		t.Fatalf("NewTokenTable: %v", err)
	}
	var wg sync.WaitGroup
	fail := make(chan string, waiters)
	for w := range waiters {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range rounds {
				var token uint64
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				for {
					tok, ok, werr := tbl.ClaimOrWait(ctx, id)
					if ok {
						token = tok
						break
					}
					if werr != nil {
						cancel()
						fail <- "a waiter was parked for ten seconds on a table other waiters kept releasing: " +
							"its wait was ended by no release, which is the lost wakeup"
						return
					}
				}
				cancel()
				// Hold the slot for a moment, then give it back: this is the
				// settle-then-release the table is sized for.
				if _, serr := tbl.Settle(token); serr != nil {
					fail <- "Settle on a token this goroutine just claimed: " + serr.Error()
					return
				}
				if cerr := tbl.Complete(token, nil); cerr != nil {
					fail <- "Complete on a settlement this goroutine just started: " + cerr.Error()
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(fail)
	if msg, ok := <-fail; ok {
		t.Fatal(msg)
	}
	if n := tbl.Live(); n != 0 {
		t.Fatalf("%d deliveries are still live after every waiter completed its settlement, want 0", n)
	}
}

// TestTokenTableClaimOrWaitAnswers holds the three answers a waiting caller
// branches on: a claim when a slot is free, a wake that is not a claim when a
// release lands, and the context's error when the context ends first.
func TestTokenTableClaimOrWaitAnswers(t *testing.T) {
	tbl, tokens := fullTable(t, 1)
	ctx := context.Background()

	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, ok, err := tbl.ClaimOrWait(short, "late"); ok || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ClaimOrWait on a full table whose context ended = (%v, %v), want (false, DeadlineExceeded)", ok, err)
	}

	type answer struct {
		ok  bool
		err error
	}
	woke := make(chan answer, 1)
	go func() {
		_, ok, err := tbl.ClaimOrWait(ctx, "waiter")
		woke <- answer{ok, err}
	}()
	// The waiter may not have parked yet, and either way the release must
	// end its wait: a parked waiter is woken, and one that has not parked
	// finds the slot and claims it.
	if err := tbl.Release(tokens[0]); err != nil {
		t.Fatalf("Release: %v", err)
	}
	select {
	case a := <-woke:
		if a.err != nil {
			t.Fatalf("a wait ended by a release reported %v, want nil", a.err)
		}
		if !a.ok {
			if _, ok, err := tbl.ClaimOrWait(ctx, "waiter"); !ok || err != nil {
				t.Fatalf("the claim after a wake = (%v, %v), want (true, nil)", ok, err)
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a release did not end the wait")
	}
	if n := tbl.Live(); n != 1 {
		t.Fatalf("%d deliveries are live, want the one the waiter claimed", n)
	}
}
