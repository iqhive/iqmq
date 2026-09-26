package engine

import (
	"slices"
	"sort"

	"github.com/iqhive/iqmq/internal/settlement"
)

// assignment is the versioned sticky partition assignment of one
// subscription (the memory engine). owner[i] is the consumer that owns lane
// i, or 0 when the lane is unassigned; version is the rebalance generation,
// which every join and every leave increments.
//
// It is replaced wholesale on a rebalance and read without a lock, so a fetch
// sees one consistent table rather than a partly rewritten one. The table is
// authority over which lanes a consumer may claim from; a claim
// re-validates it under the lane mutex, which is the mutex a revocation takes
// too, so a claim that races a rebalance is either revoked by it or refused
// by the new table and never both.
//
// members is the membership the table was dealt from, in id order. A
// revocation reads it from the same table as the owner it compares against,
// and reads that table under the lane mutex, so that it can tell a holder
// that is still a member -- whose lease a reassignment takes, and who is told
// so with ErrLeaseLost -- from one that has left, which only a Consumer.Close
// does and whose lease is closed (revoke). A table read
// before the lane mutex can predate the join of a holder that claimed since,
// and would call an open consumer closed.
type assignment struct {
	version uint64
	owner   []uint32
	members []uint32
}

// ownerOf returns the consumer assigned to lane i.
func (a *assignment) ownerOf(i int32) uint32 {
	if a == nil || int(i) >= len(a.owner) {
		return 0
	}
	return a.owner[i]
}

// member reports whether id was a member when the table was dealt.
func (a *assignment) member(id uint32) bool {
	if a == nil {
		return false
	}
	_, found := slices.BinarySearch(a.members, id)
	return found
}

// owner returns the consumer currently assigned to the lane.
func (ln *lane[P]) owner() uint32 { return ln.sub.assign.Load().ownerOf(ln.idx) }

// assignmentVersion returns the current rebalance generation.
func (s *Subscription[P]) assignmentVersion() uint64 {
	if a := s.assign.Load(); a != nil {
		return a.version
	}
	return 0
}

// Assignment returns the rebalance generation and, for a subscription with
// sticky assignment, the consumer that owns each lane. The slice is a copy.
func (s *Subscription[P]) Assignment() (version uint64, owners []uint32) {
	a := s.assign.Load()
	if a == nil {
		return 0, nil
	}
	out := make([]uint32, len(a.owner))
	copy(out, a.owner)
	return a.version, out
}

// Sticky reports whether the subscription assigns each lane to one consumer
// exclusively (the memory engine).
func (s *Subscription[P]) Sticky() bool { return s.sticky }

// join adds a consumer to the membership and rebalances.
func (s *Subscription[P]) join(c *Consumer[P]) {
	id := c.id
	s.cmu.Lock()
	if s.cons == nil {
		s.cons = map[uint32]*Consumer[P]{}
	}
	s.cons[id] = c
	s.members = append(s.members, id)
	sort.Slice(s.members, func(i, j int) bool { return s.members[i] < s.members[j] })
	s.rebalanceLocked()
	s.cmu.Unlock()
	s.revoke(0)
}

// leave removes a consumer from the membership and rebalances.
func (s *Subscription[P]) leave(id uint32) {
	s.cmu.Lock()
	delete(s.cons, id)
	for i, m := range s.members {
		if m == id {
			s.members = append(s.members[:i], s.members[i+1:]...)
			break
		}
	}
	s.rebalanceLocked()
	s.cmu.Unlock()
	s.revoke(id)
}

// Rebalance recomputes the sticky assignment, increments the rebalance
// generation and revokes every lease the new table no longer allows. It
// returns the new generation.
//
// It is what a consumer joining or leaving does on its own; calling it
// directly is how a test drives a rebalance without changing the membership
// (the memory engine "simulate reconnect generations").
func (s *Subscription[P]) Rebalance() uint64 {
	if s.closed.Load() || s.eng.closing() {
		// After a close nothing is admitted and there is no lease left to
		// revoke, so the table is what it was: moving lanes nobody can claim
		// from and bumping a generation nothing reads would make Rebalance
		// the one control operation a close does not stop.
		return s.assign.Load().version
	}
	s.cmu.Lock()
	v := s.rebalanceLocked()
	s.cmu.Unlock()
	s.revoke(0)
	return v
}

// rebalanceLocked recomputes the table under the membership mutex. Lanes are
// dealt out to the members in id order, so an assignment is a function of the
// membership alone: the same members always produce the same table, which is
// what makes it sticky — a join or a leave moves only the lanes it has to.
func (s *Subscription[P]) rebalanceLocked() uint64 {
	old := s.assign.Load()
	next := &assignment{version: 1, owner: make([]uint32, len(s.lanes)), members: slices.Clone(s.members)}
	if old != nil {
		next.version = old.version + 1
	}
	if s.sticky && len(s.members) > 0 {
		for i := range next.owner {
			next.owner[i] = s.members[i%len(s.members)]
		}
	}
	s.assign.Store(next)
	return next.version
}

// revoke reclaims the leases the current assignment no longer allows: those
// of the departed consumer, and — under a sticky assignment — those held on a
// lane by anybody but its new owner. The records go back to their lanes for
// the consumer that now owns them.
//
// It is also where a rebalance announces itself, which is the last thing
// every rebalance does: join, leave and Rebalance all store the new table
// under the membership mutex and then call this with the mutex released. The
// announcement is unconditional, for the reason Consumer.Close gives
// (adr/ADR-017-assumptions.md): the table decides which
// lanes a consumer may claim from (lane.assigned), so storing a new one
// changes the condition every parked fetch waits on, and a fetch can only
// leave on a notification, on its context or on a close. Announcing it only
// when a lease had been requeued left the one case that has no lease to
// revoke — a lane holding *ready* records handed to a consumer already
// parked in Fetch — with no wake at all, and that consumer waited for its
// context while a record it now owned sat ready in its lane.
//
// A subscription without a sticky assignment has nothing to announce and
// nothing to revoke: every consumer competes on every lane whatever the
// table says, so a rebalance of one cannot change what a parked fetch would
// find.
//
// A revocation is not a failed delivery, so it never consults MaxAttempts: a
// rebalance that dead-lettered records nothing had failed on would be the
// double-retry accounting the message contract forbids
// (adr/ADR-017-assumptions.md).
func (s *Subscription[P]) revoke(gone uint32) {
	if gone == 0 && !s.sticky {
		return
	}
	// Announced however the walk below turns out, and after it: a consumer
	// woken by it re-reads the table and its lanes, which is the order a
	// requeue needs too.
	defer s.waiters.NotifyAll()
	var back []*Envelope[P]
	for _, ln := range s.lanes {
		// The clock is read per lane: dropAll below runs the publishers'
		// Release callbacks between one lane and the next, for as long as
		// they take, so a time read before the walk would judge a later
		// lane's records against an instant that has passed.
		now := s.eng.now()
		requeued := 0
		ln.mu.Lock()
		// One table for both questions -- who owns the lane now, and whether
		// the holder is still a member of the membership that dealt it --
		// and read under the lane mutex, never before it. Every lease on the
		// lane was placed under this mutex by a consumer whose join had
		// already stored a table naming it (Attach joins before it returns,
		// and a claim re-validates the table here), so a table read now is
		// at least as new as the join of every holder the walk can find: a
		// holder it does not name has left. Read before the lock, a consumer
		// that joined and claimed in between was missing from it, and its
		// lease was closed under a consumer nobody had closed.
		table := s.assign.Load()
		want := table.ownerOf(ln.idx)
		for e := ln.live.head; e != nil; {
			next := e.next
			holder := e.owner.Load()
			if holder == gone || (s.sticky && holder != want) {
				_, g := e.cell.Load()
				// Two withdrawals, two resting states. The departing
				// consumer's own records are CLOSED -- the consumer really
				// did close, and a settlement that arrives afterwards is
				// told so -- while a record the sticky reassignment takes
				// from a consumer that is still open is REVOKED, and a
				// settlement of it is told its lease is gone. Using Close
				// for both made a rebalanced delivery report ErrClosed from
				// a consumer nobody had closed
				// (adr/ADR-017-assumptions.md).
				//
				// "Departing" is read from the table, not from gone alone.
				// Consumer.Close stores a table without the consumer and
				// releases the membership mutex before its own revoke runs,
				// so a join or a Rebalance landing in between walks the
				// lanes first, with gone == 0, and used to Revoke the closing
				// consumer's leases: a settlement after Close returned was
				// told ErrLeaseLost, which no order of the two operations
				// produces.
				closing := holder == gone || !table.member(holder)
				if withdrawLease(e, g, closing) {
					ln.live.remove(e)
					if ln.pushReady(e, now, &back) {
						requeued++
					}
				}
			}
			e = next
		}
		ln.mu.Unlock()
		ln.dropAll(back, false)
		clear(back)
		back = back[:0]
		if requeued > 0 {
			s.st.requeued.Add(int64(requeued))
			s.waiters.NotifyAll()
			s.unleased()
		}
		ln.rearm()
	}
}

// withdrawLease takes a lease away from its holder, in the way the reason
// calls for: a consumer that closed takes internal/settlement's Close, whose
// stale settlement is told the consumer closed, and a lane a reassignment
// moved takes Revoke, whose stale settlement is told its lease is gone. It
// reports whether the caller has claimed the record and must requeue it
// (adr/ADR-017-assumptions.md).
func withdrawLease[P any](e *Envelope[P], g settlement.Gen, closing bool) bool {
	if closing {
		return e.cell.Close(g)
	}
	return e.cell.Revoke(g)
}

// assigned reports whether c may claim from the lane. Without a sticky
// assignment every consumer competes on every lane, which is the queue
// semantics of the memory engine's second sentence.
func (ln *lane[P]) assigned(c *Consumer[P]) bool {
	if !ln.sub.sticky {
		return true
	}
	return ln.owner() == c.id
}
