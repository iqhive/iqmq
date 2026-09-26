package fanout

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// step applies op and asserts the invariants, which is what the differential
// harness does after every operation.
func step(t *testing.T, m *Model, op Op) Result {
	t.Helper()
	res := m.Step(op)
	if err := m.Invariants(); err != nil {
		t.Fatalf("after %v: %v", op.Kind, err)
	}
	return res
}

func TestPolicyNames(t *testing.T) {
	if len(Policies()) != 4 {
		t.Fatalf("Policies() = %v", Policies())
	}
	seen := map[string]bool{}
	for _, p := range Policies() {
		if seen[p.String()] {
			t.Fatalf("duplicate policy name %q", p)
		}
		seen[p.String()] = true
	}
	if Policy(9).String() != "policy(?)" || Kind(9).String() != "op(?)" {
		t.Fatal("undefined names")
	}
	for _, k := range []Kind{Publish, Claim, Settle, Close, Subscribe} {
		if k.String() == "op(?)" {
			t.Fatalf("op %d has no name", k)
		}
	}
}

// TestAtomicAllOrNothing: one full subscription refuses for every
// subscription, and every one of them counts the rejection.
func TestAtomicAllOrNothing(t *testing.T) {
	m := New(Config{Policy: Atomic, Subs: 3, Capacity: 1})
	if res := step(t, m, Op{Kind: Publish, Size: 1}); res.Err != nil || len(res.Reached) != 3 {
		t.Fatalf("first publication: %+v", res)
	}
	// Subscription 1 makes room; the other two are still full.
	step(t, m, Op{Kind: Claim, Sub: 1})
	step(t, m, Op{Kind: Settle, Sub: 1, Seq: 1})
	res := step(t, m, Op{Kind: Publish, Size: 1})
	if !errors.Is(res.Err, errcode.ErrQueueFull) || len(res.Reached) != 0 {
		t.Fatalf("atomic publication into a full subscription: %+v", res)
	}
	for i := range 3 {
		if _, _, rejected, _ := m.Counters(i); rejected != 1 {
			t.Fatalf("subscription %d counted %d rejections", i, rejected)
		}
	}
	if m.Depth(1) != 0 {
		t.Fatalf("the subscription with room took the record anyway: %d", m.Depth(1))
	}
}

// TestIndependentAndBestEffortDeliverPartially: the subscriptions with room
// take the record; only what the publisher is told differs. Independent says
// ErrPartial, which is not ErrQueueFull: the record was admitted, so the
// publication is neither retryable nor one whose payload the caller still
// owns.
func TestIndependentAndBestEffortDeliverPartially(t *testing.T) {
	for _, tc := range []struct {
		policy  Policy
		wantErr error
	}{{Independent, errcode.ErrPartial}, {BestEffort, nil}} {
		t.Run(tc.policy.String(), func(t *testing.T) {
			m := New(Config{Policy: tc.policy, Subs: 2, Capacity: 1})
			step(t, m, Op{Kind: Publish, Size: 1})
			step(t, m, Op{Kind: Claim, Sub: 1})
			step(t, m, Op{Kind: Settle, Sub: 1, Seq: 1})
			res := step(t, m, Op{Kind: Publish, Size: 1})
			if !errors.Is(res.Err, tc.wantErr) {
				t.Fatalf("publisher was told %v, want %v", res.Err, tc.wantErr)
			}
			if errors.Is(res.Err, errcode.ErrQueueFull) {
				t.Fatalf("a partial admission must not look like a refusal: %v", res.Err)
			}
			if len(res.Reached) != 1 || res.Reached[0] != 1 {
				t.Fatalf("reached %v, want the subscription with room", res.Reached)
			}
			if _, _, rejected, _ := m.Counters(0); rejected != 1 {
				t.Fatalf("the full subscription counted %d rejections", rejected)
			}
		})
	}
}

// TestDropSlowDropsAndAdmits: a full subscription loses its oldest record
// rather than its publisher, and cannot when everything it holds is claimed.
func TestDropSlowDropsAndAdmits(t *testing.T) {
	m := New(Config{Policy: DropSlow, Subs: 2, Capacity: 1})
	step(t, m, Op{Kind: Publish, Size: 1})
	res := step(t, m, Op{Kind: Publish, Size: 1})
	if res.Err != nil || len(res.Reached) != 2 || len(res.Dropped) != 2 {
		t.Fatalf("dropslow publication: %+v", res)
	}
	for i := range 2 {
		if _, dropped, _, _ := m.Counters(i); dropped != 1 {
			t.Fatalf("subscription %d dropped %d records", i, dropped)
		}
	}
	// With the only record claimed there is nothing to drop.
	step(t, m, Op{Kind: Claim, Sub: 0})
	step(t, m, Op{Kind: Claim, Sub: 1})
	if res := step(t, m, Op{Kind: Publish, Size: 1}); !errors.Is(res.Err, errcode.ErrQueueFull) {
		t.Fatalf("dropslow with everything claimed: %+v", res)
	}
}

// TestSharedLogCapacityIsShared: with one log per partition a subscription
// that never settles holds the room the others need, and the slot is
// reclaimed only when the last of them lets go.
func TestSharedLogCapacityIsShared(t *testing.T) {
	m := New(Config{Policy: Atomic, Subs: 2, Capacity: 1, Shared: true})
	step(t, m, Op{Kind: Publish, Size: 1})
	step(t, m, Op{Kind: Claim, Sub: 0})
	step(t, m, Op{Kind: Settle, Sub: 0, Seq: 1})
	// Subscription 0 has settled; subscription 1 has not, so the slot stays.
	if res := step(t, m, Op{Kind: Publish, Size: 1}); !errors.Is(res.Err, errcode.ErrQueueFull) {
		t.Fatalf("the log took a record the slowest subscription still holds: %+v", res)
	}
	step(t, m, Op{Kind: Claim, Sub: 1})
	step(t, m, Op{Kind: Settle, Sub: 1, Seq: 1})
	if res := step(t, m, Op{Kind: Publish, Size: 1}); res.Err != nil {
		t.Fatalf("the log did not reclaim the slot: %+v", res)
	}
	snap := m.Snapshot()
	if snap.LogSlots != 1 || snap.Reserved != snap.Enqueued+snap.Released {
		t.Fatalf("%+v", snap)
	}
}

// TestLateSubscriptionAndClose: a subscription created after a publication
// does not receive it, and a closed one releases everything it held.
func TestLateSubscriptionAndClose(t *testing.T) {
	m := New(Config{Policy: BestEffort, Subs: 1, Capacity: 4})
	step(t, m, Op{Kind: Publish, Size: 1})
	step(t, m, Op{Kind: Subscribe})
	step(t, m, Op{Kind: Publish, Size: 1})
	if got := m.Held(1); len(got) != 1 || got[0] != 2 {
		t.Fatalf("the late subscription holds %v", got)
	}
	step(t, m, Op{Kind: Close, Sub: 0})
	if m.Depth(0) != 0 || !m.Closed(0) {
		t.Fatalf("closed subscription holds %d records", m.Depth(0))
	}
	if _, dropped, _, _ := m.Counters(0); dropped != 2 {
		t.Fatalf("close dropped %d records", dropped)
	}
	// A closed subscription is not a target any more.
	res := step(t, m, Op{Kind: Publish, Size: 1})
	if len(res.Reached) != 1 || res.Reached[0] != 1 {
		t.Fatalf("publication after a close reached %v", res.Reached)
	}
	if res := m.Step(Op{Kind: Settle, Sub: 1, Seq: 99}); !errors.Is(res.Err, errcode.ErrAlreadySettled) {
		t.Fatalf("settling an unknown record: %v", res.Err)
	}
	if m.Subs() != 2 || m.Depth(9) != 0 || m.Held(9) != nil || m.Closed(9) {
		t.Fatal("out-of-range accessors")
	}
}

// TestUndefinedOpPanics guards the harness contract.
func TestUndefinedOpPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("an undefined operation did not panic")
		}
	}()
	New(Config{Subs: 1}).Step(Op{Kind: kindCount})
}

// TestAdvanceIsTimeless pins the declared deviation from the specification D: the
// model has the Advance(dt) every reference model has, and moving its clock
// changes nothing, because admission does not depend on time.
func TestAdvanceIsTimeless(t *testing.T) {
	m := New(Config{Policy: Atomic, Subs: 2, Capacity: 2})
	step(t, m, Op{Kind: Publish})
	before := fmt.Sprintf("%+v", m.Snapshot())
	m.Advance(time.Hour)
	if got := fmt.Sprintf("%+v", m.Snapshot()); got != before {
		t.Fatalf("Advance changed the model:\n before %s\n after  %s", before, got)
	}
	if err := m.Invariants(); err != nil {
		t.Fatalf("after Advance: %v", err)
	}
	// Time never becomes a step: Steps counts operations, and Advance is not
	// one of them.
	if m.Steps() != 1 {
		t.Fatalf("Steps() = %d after one publication and one Advance", m.Steps())
	}
}
