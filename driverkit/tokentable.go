package driverkit

import (
	"context"
	"sync"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// SettleState is where one delivery stands in a [TokenTable].
type SettleState uint8

// The settlement states, as a table sees them.
const (
	// StateFree is a slot nothing is using.
	StateFree SettleState = iota
	// StateActive is a delivery that has been handed out and not settled.
	StateActive
	// StateSettling is a settlement in flight at the broker.
	StateSettling
	// StateSettled is a delivery whose settlement the broker took.
	StateSettled
	// StateUnknown is a delivery whose settlement was ambiguous. It is
	// sticky: nothing later turns it into a success.
	StateUnknown

	settleStateCount
)

var settleStateNames = [settleStateCount]string{"free", "active", "settling", "settled", "unknown"}

// String returns the state's lower-case name.
func (s SettleState) String() string {
	if s < settleStateCount {
		return settleStateNames[s]
	}
	return "SettleState(?)"
}

// Valid reports whether s names a state.
func (s SettleState) Valid() bool { return s < settleStateCount }

// TokenTable is the prefetch-sized slot table a driver resolves a settlement
// through.
//
// A delivery's native handle — an SQS receipt, a Redis entry id, a
// *nats.Msg — has to survive from the fetch that produced it to the settlement
// that consumes it, and the SPI carries it as a packed integer and an optional
// boxed reference. A token here is a slot index and a generation:
//
//	token = generation<<32 | slot
//
// The generation is what makes a stale settlement detectable. A rebalance, a
// reconnection or a lease expiry calls [TokenTable.InvalidateAll], which bumps
// the table's generation and frees every slot; a settlement carrying a token
// from before the bump names a generation the slot no longer has and is refused
// with [github.com/iqhive/iqmq.ErrLeaseLost] rather than applied to whatever
// delivery took the slot afterwards. Without it, a late ack from a revoked
// partition acknowledges a different message.
//
// Claim, Resolve, Settle and Complete allocate nothing. Every method is safe
// for concurrent use.
type TokenTable struct {
	mu    sync.Mutex
	slots []tokenSlot
	free  []uint32
	gen   uint32
	live  int
	// freed is the release broadcast [TokenTable.ClaimOrWait] parks on, or
	// nil when nothing is waiting. It is CLOSED, never sent to, which
	// wakes every waiter at once and costs one channel per WAIT rather than
	// one per waiter — the idiom [PushAdapter] and the RabbitMQ sink's
	// publication window already use, and for the same reason: a sync.Cond
	// cannot be selected against a context.
	//
	// It is nil until a waiter asks, and nil again as soon as a release
	// closes it, because Claim, Resolve, Settle and Complete allocate nothing
	// and a release that always made a fresh channel would have made Complete
	// allocate one (`TestAllocsTokenTable`). A table nobody waits on pays for
	// this in one nil check.
	//
	// Only [TokenTable.ClaimOrWait] reads it, and it does so under the same
	// lock as the claim it has just failed, so no caller can be left parked
	// on a channel a release has already passed by. It never leaves the
	// table: a caller sees a blocking call, not the channel.
	freed chan struct{}
}

// tokenSlot is one delivery's bookkeeping.
type tokenSlot struct {
	ref   any
	gen   uint32
	state SettleState
}

// NewTokenTable returns a table with size slots, which a driver sizes from its
// prefetch: a source may not hold more unsettled deliveries than it asked the
// broker for.
func NewTokenTable(size int) (*TokenTable, error) {
	if size <= 0 {
		return nil, errTableSize
	}
	t := &TokenTable{
		slots: make([]tokenSlot, size),
		free:  make([]uint32, size),
		gen:   1,
	}
	for i := range t.slots {
		t.slots[i].gen = 1
		t.free[i] = uint32(size - 1 - i)
	}
	return t, nil
}

// Claim takes a free slot for a delivery and returns its token. It reports
// false when the table is full, which is a driver having fetched more than its
// prefetch and is a bug in the driver rather than a condition to wait on.
func (t *TokenTable) Claim(ref any) (uint64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.free) == 0 {
		return 0, false
	}
	slot := t.free[len(t.free)-1]
	t.free = t.free[:len(t.free)-1]
	s := &t.slots[slot]
	s.ref = ref
	s.state = StateActive
	t.live++
	return uint64(s.gen)<<32 | uint64(slot), true
}

// Resolve returns the native reference a token names, without changing the
// slot's state. It is what a driver reads to build an extension or a metadata
// answer.
func (t *TokenTable) Resolve(token uint64) (any, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, err := t.slotOf(token)
	if err != nil {
		return nil, err
	}
	return s.ref, nil
}

// State returns the state of the delivery a token names.
func (t *TokenTable) State(token uint64) (SettleState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, err := t.slotOf(token)
	if err != nil {
		return StateFree, err
	}
	return s.state, nil
}

// Settle moves a delivery from active to settling and returns its native
// reference, which is the call a driver makes before the round trip.
//
// A delivery that is already settling reports
// [github.com/iqhive/iqmq.ErrSettlementInProgress], one that is settled
// [github.com/iqhive/iqmq.ErrAlreadySettled], one whose outcome is unknown
// [github.com/iqhive/iqmq.ErrSettlementUnknown], and one whose generation is
// stale [github.com/iqhive/iqmq.ErrLeaseLost].
//
// The core already refuses a second settlement above the SPI. This is the
// driver's own half of the same rule, and it is what catches the settlement
// that raced a redelivery.
func (t *TokenTable) Settle(token uint64) (any, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, err := t.slotOf(token)
	if err != nil {
		return nil, err
	}
	switch s.state {
	case StateActive:
		s.state = StateSettling
		return s.ref, nil
	case StateSettling:
		return nil, mq.ErrSettlementInProgress
	case StateSettled:
		return nil, mq.ErrAlreadySettled
	case StateUnknown:
		return nil, mq.ErrSettlementUnknown
	case StateFree, settleStateCount:
	}
	return nil, mq.ErrLeaseLost
}

// Complete records the outcome of a settlement in flight and frees the slot
// when the outcome is terminal.
//
// A nil err settles the delivery and returns the slot to the free list. An
// ambiguous err — one [github.com/iqhive/iqmq.IsAmbiguous] reports, so
// CodeSettlementUnknown, CodeAmbiguous or an *OpError with its Ambiguous flag
// set — leaves the delivery unknown for ever and does NOT free the slot: an
// unknown outcome is sticky, and a slot reused under a token whose fate nobody
// knows is how an unknown settlement becomes a successful one by accident. Any
// other error is a definite refusal, which leaves the delivery settleable — the
// caller may try again.
//
// The flag matters as much as the codes. MapNet with sentAlready keeps the
// transport's code — Unavailable for an EOF after the frame was written — and
// says the outcome is unknown through the flag alone; reading only the code
// put such a delivery back to active, and a second settlement of it could
// reach a broker that had already taken the first.
func (t *TokenTable) Complete(token uint64, err error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, serr := t.slotOf(token)
	if serr != nil {
		return serr
	}
	if s.state != StateSettling {
		return errNotSettling
	}
	switch {
	case err == nil:
		s.state = StateSettled
		t.releaseLocked(token, s)
	case mq.IsAmbiguous(err):
		s.state = StateUnknown
		s.ref = nil
	default:
		s.state = StateActive
	}
	return nil
}

// Release frees a slot outright, whatever its state, which is what a driver
// does for a delivery the broker has taken back — a lease expiry, a
// redelivery — so that the slot can serve the next delivery under a new
// generation.
func (t *TokenTable) Release(token uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, err := t.slotOf(token)
	if err != nil {
		return err
	}
	t.releaseLocked(token, s)
	return nil
}

// releaseLocked returns a slot to the free list under a new generation, so that
// a token naming it is stale from here on.
func (t *TokenTable) releaseLocked(token uint64, s *tokenSlot) {
	s.ref = nil
	s.state = StateFree
	s.gen++
	t.live--
	t.free = append(t.free, uint32(token))
	t.signalFreedLocked()
}

// signalFreedLocked wakes everything parked in [TokenTable.ClaimOrWait].
//
// It closes the broadcast channel under the table's own lock, so that a waiter
// which read the channel before it found the table full cannot miss the
// release that happened in between: the channel it holds is the one this call
// closes. A nil channel is nobody waiting, and costs a branch.
func (t *TokenTable) signalFreedLocked() {
	if t.freed == nil {
		return
	}
	close(t.freed)
	t.freed = nil
}

// ClaimOrWait takes a free slot for a delivery, or waits until a slot is
// released or ctx ends.
//
// It is [TokenTable.Claim] for the caller that means to WAIT, and it exists so
// that the waiting cannot be got wrong. A full table is a condition a driver
// waits on rather than a bug: the prefetch bounds what the broker lends, but
// a settlement writes its frame before it frees its slot and the broker
// answers that frame with the next delivery, so a pump meets a full table on a
// healthy consumer. Both drivers that waited polled it at
// sub-millisecond intervals, because there was nothing to wait on.
//
// It reports (token, true, nil) for a claim, (0, false, nil) when a release
// woke it, and (0, false, ctx.Err()) when ctx ended first. A wake is NOT a
// claim: several waiters wake on one release and only one of them takes the
// slot, so a caller loops -- and the loop is where a driver re-checks its own
// reasons to stop waiting, such as a superseded subscription, before it
// claims again. A driver whose other reasons are edges rather than state ends
// the wait through ctx.
//
// THE FAILED CLAIM AND THE START OF THE WAIT ARE ONE CRITICAL SECTION, and
// that is the whole point rather than an implementation detail. It is
// guaranteed by construction -- the claim and the read of the release
// broadcast sit under one `t.mu.Lock()` -- and NOT by a test: a probe placed
// at the point a second acquisition would open lives inside the very function
// a mutation rewrites, so it moves with the defect and reports nothing. What
// the tests pin is the contract that follows from it. A caller that asked the
// two separately -- "is there a slot?", then "wait for a release" -- has a
// window between them in which a release can land, and then waits for a
// release that has already happened, on a table with a free slot in it. The
// poll this replaced made that window cost 200 microseconds; without a poll it
// costs the delivery. Releasing takes the same lock, so here there is no
// window at all and no ordering for a caller to observe.
//
// The broadcast is CLOSED, never sent to, so one release wakes every waiter.
// InvalidateAll wakes them too, which is the exit a driver's teardown leans on.
func (t *TokenTable) ClaimOrWait(ctx context.Context, ref any) (uint64, bool, error) {
	token, ok, freed := t.claimOrPark(ref)
	if ok {
		return token, true, nil
	}
	select {
	case <-freed:
		return 0, false, nil
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
}

// claimOrPark is ClaimOrWait's critical section: the claim, or the
// broadcast the next release will close.
func (t *TokenTable) claimOrPark(ref any) (uint64, bool, <-chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.free) > 0 {
		slot := t.free[len(t.free)-1]
		t.free = t.free[:len(t.free)-1]
		s := &t.slots[slot]
		s.ref = ref
		s.state = StateActive
		t.live++
		return uint64(s.gen)<<32 | uint64(slot), true, nil
	}
	if t.freed == nil {
		t.freed = make(chan struct{})
	}
	return 0, false, t.freed
}

// InvalidateAll bumps the table's generation and frees every slot, so that every
// token handed out before the bump is stale and every settlement carrying one
// reports ErrLeaseLost. It returns how many live deliveries it invalidated.
//
// It is what a driver calls on a rebalance, a reconnection or a subscription
// restart: the broker has taken back everything it lent, and a settlement for
// any of it must not land on the delivery that takes the slot next.
func (t *TokenTable) InvalidateAll() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.live
	t.gen++
	t.free = t.free[:0]
	for i := range t.slots {
		t.slots[i].ref = nil
		t.slots[i].state = StateFree
		// A slot's generation only ever rises, which is what keeps a token
		// from being handed out twice: a slot that has been released more
		// often than the table has been invalidated is already past the
		// table's generation, and resetting it down to the table's would
		// eventually reissue a generation an old token still names.
		t.slots[i].gen++
		if t.slots[i].gen < t.gen {
			t.slots[i].gen = t.gen
		}
		t.free = append(t.free, uint32(len(t.slots)-1-i))
	}
	t.live = 0
	// Every slot is free, so every waiter is woken. It is the release signal's
	// other half and the one a driver's teardown depends on: a wait that ended
	// only on a per-slot release would outlive the subscription it belongs to.
	t.signalFreedLocked()
	return n
}

// Live is how many deliveries the table has handed out and not released.
func (t *TokenTable) Live() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.live
}

// Cap is how many deliveries the table can hold at once.
func (t *TokenTable) Cap() int { return len(t.slots) }

// TableStats is what a table can say about itself, for a driver that reports
// its own health.
type TableStats struct {
	// Cap is how many deliveries the table can hold at once.
	Cap int
	// Live is how many slots are held by a delivery that is not free.
	Live int
	// Unknown is how many slots hold a delivery whose settlement outcome was
	// ambiguous. The specification makes that permanent, so this number never falls on
	// its own: it is the part of the prefetch the source has lost for good,
	// and a source whose settlements are ambiguous often enough runs out.
	Unknown int
	// Settling is how many settlements are in flight at the broker.
	Settling int
}

// Stats counts the table's slots by state.
//
// It exists because the number that matters most could not be read: the specification makes
// an ambiguous settlement permanent and the table keeps the slot, correctly,
// so a source whose settlements are often ambiguous loses its prefetch a slot
// at a time and NOTHING said how close it was. `drivers/amqp10` had to count
// unknown slots itself to make the shrinking window visible; this is that
// count, for every driver.
//
// Deliberately NOT a sweep. Reclaiming an unknown slot is seizing a delivery
// whose outcome nobody knows, which is the one thing the specification forbids — the
// choice between that and reopening the source belongs to whoever operates
// the deployment, not to the kit, and it needs a per-slot age this table does
// not keep.
func (t *TokenTable) Stats() TableStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := TableStats{Cap: len(t.slots), Live: t.live}
	for i := range t.slots {
		switch t.slots[i].state {
		case StateUnknown:
			st.Unknown++
		case StateSettling:
			st.Settling++
		case StateFree, StateActive, StateSettled, settleStateCount:
			// Counted by Live, or not a state a slot holds. Named rather
			// than defaulted so that a new state has to be classified here
			// instead of silently joining the ones nobody reports.
		}
	}
	return st
}

// Generation is the table's current generation.
func (t *TokenTable) Generation() uint32 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.gen
}

// slotOf returns the slot a token names, or ErrLeaseLost when the token is out
// of range or names a generation the slot no longer has.
func (t *TokenTable) slotOf(token uint64) (*tokenSlot, error) {
	idx := uint32(token)
	if int(idx) >= len(t.slots) {
		return nil, mq.ErrLeaseLost
	}
	s := &t.slots[idx]
	if uint64(s.gen) != token>>32 {
		return nil, mq.ErrLeaseLost
	}
	return s, nil
}

// The table's failures.
var (
	errTableSize   = errcode.New(errcode.InvalidConfig, "iqmq/driverkit: a TokenTable needs a positive size")
	errNotSettling = errcode.New(errcode.InvalidArgument, "iqmq/driverkit: Complete was given a token with no settlement in flight")
)
