package memq

import "sort"

// rebalance recomputes a subscription's assignment, incrementing its
// rebalance generation.
func (m *Model) rebalance(s *sub) {
	s.version++
	if s.owner == nil {
		s.owner = make([]uint32, len(s.lanes))
	}
	clear(s.owner)
	if m.cfg.Sticky && len(s.members) > 0 {
		for i := range s.owner {
			s.owner[i] = s.members[i%len(s.members)]
		}
	}
}

// join adds a consumer to the membership, rebalances and revokes what the new
// table no longer allows.
func (m *Model) join(s *sub, id uint32) {
	s.consumers[id] = true
	s.members = append(s.members, id)
	sort.Slice(s.members, func(i, j int) bool { return s.members[i] < s.members[j] })
	m.rebalance(s)
	m.revoke(s, 0)
}

// leave removes a consumer from the membership, rebalances and revokes both
// its own leases and any the new table moved.
func (m *Model) leave(s *sub, id uint32) {
	delete(s.consumers, id)
	for i, got := range s.members {
		if got == id {
			s.members = append(s.members[:i], s.members[i+1:]...)
			break
		}
	}
	m.rebalance(s)
	m.revoke(s, id)
}

// revoke reclaims the leases the current assignment no longer allows: those
// of the departed consumer, and — under a sticky assignment — those held on a
// lane by anybody but its new owner.
//
// A revocation is not a failed delivery, so it never consults MaxAttempts:
// dead-lettering a record nothing failed on would be the double-retry
// accounting the message contract forbids. The two withdrawals count it
// differently. A record a reassignment takes from a consumer that is still
// open gives back the attempt its lease counted, so its redelivery reports the
// attempt the revoked one would have (engine Subscription.revoke and
// revokeAll). The departed consumer's own records keep theirs, which is the
// close policy, and so may be delivered again past MaxAttempts
// (rec.overBudget).
//
// The two withdrawals leave different resting states, as the engine's do
// (engine withdrawLease): the departed consumer's own
// records are Closed, and a settlement of one is told the consumer closed
// ("settlement after Consumer.Close returns ErrClosed"), while a record
// the sticky reassignment takes from a consumer that is still open is Idle,
// and a settlement of it is told its lease is gone ("a stale delivery
// generation returns ErrLeaseLost", "rebalance generations invalidate
// stale deliveries"). Closing both made the model expect ErrClosed from a
// consumer nobody had closed.
//
// Which consumer departed is read from the membership, not from gone alone,
// because that is how the engine reads it: its revoke asks whether the holder
// is a member of the assignment table it reads under the lane mutex -- a
// table at least as new as the join of any holder on that lane -- so that a
// rebalance overtaking a Consumer.Close closes the closing consumer's leases
// instead of revoking them. The model is sequential: its
// membership is always current and leave removes the consumer before it
// revokes, so the two tests agree here on every step, and the membership
// form is kept so that the model states the engine's rule.
func (m *Model) revoke(s *sub, gone uint32) {
	if gone == 0 && !m.cfg.Sticky {
		return
	}
	for li := range s.lanes {
		ln := &s.lanes[li]
		want := s.ownerOf(li)
		kept := ln.live[:0]
		var back []uint64
		for _, seq := range ln.live {
			r := s.recs[seq]
			if r.cell == cellActive && (r.owner == gone || (m.cfg.Sticky && r.owner != want)) {
				if r.owner == gone || !s.consumers[r.owner] {
					r.cell = cellClosed
				} else {
					r.cell = cellIdle
					// A reassignment takes the lease of a consumer that is
					// still open: the delivery is not a failure and does not
					// count against the record's attempts.
					if r.attempts > 0 {
						r.attempts--
					}
				}
				back = append(back, seq)
				continue
			}
			kept = append(kept, seq)
		}
		ln.live = kept
		for _, seq := range back {
			s.recs[seq].overBudget = true
			m.requeue(s, ln, seq, 0)
		}
		s.st.requeued += int64(len(back))
	}
}

// ownerOf returns the consumer assigned to lane i, or 0 when the lane is
// unassigned.
func (s *sub) ownerOf(i int) uint32 {
	if i >= len(s.owner) {
		return 0
	}
	return s.owner[i]
}

// assigned reports whether the consumer may claim from lane i. Without a
// sticky assignment every consumer competes on every lane.
func (m *Model) assigned(s *sub, i int, id uint32) bool {
	return !m.cfg.Sticky || s.ownerOf(i) == id
}
