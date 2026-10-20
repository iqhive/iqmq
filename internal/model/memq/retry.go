package memq

import "time"

// sealing reports whether a redelivery blocks its lane under the configured
// placement and ordering mode.
func (m *Model) sealing() bool { return m.cfg.Retry.Seals(m.cfg.Strict) }

// requeue returns a delivered record to its lane for another attempt, at the
// time at (0 for immediately). It mirrors lane.requeue: the seal the record
// was holding is given back first, and a delayed redelivery goes to the
// lane's delayed structure only when the placement does not seal — a sealing
// placement keeps it in the retry queue, where it holds the lane while it
// waits.
func (m *Model) requeue(s *sub, ln *lane, seq uint64, at time.Duration) {
	r := s.recs[seq]
	m.releaseSeal(ln, r)
	if at > m.now {
		r.reordered = true
	}
	r.dueAt = at
	if at > m.now {
		r.state = Delayed
	} else {
		r.state = Ready
	}
	ent := retryRec{seq: seq, dueAt: at}
	switch {
	case m.sealing():
		// Publication order: nothing may overtake a redelivery.
		i := len(ln.retry)
		for i > 0 && ln.retry[i-1].seq > seq {
			i--
		}
		ln.retry = append(ln.retry, retryRec{})
		copy(ln.retry[i+1:], ln.retry[i:])
		ln.retry[i] = ent
	case m.cfg.Retry == RetryFront:
		ln.retry = append(ln.retry, retryRec{})
		copy(ln.retry[1:], ln.retry)
		ln.retry[0] = ent
	default:
		ln.retry = append(ln.retry, ent)
	}
}

// releaseSeal gives the lane's seal back when the record holding it leaves
// the lane or is placed again.
func (m *Model) releaseSeal(ln *lane, r *rec) {
	if !r.sealed {
		return
	}
	r.sealed = false
	ln.out--
}

// retryReady reports the redeliveries a consumer could take now and the ones
// still waiting for their due time.
func (m *Model) retryDepth(ln *lane) (ready, delayed int) {
	for _, ent := range ln.retry {
		if ent.dueAt > m.now {
			delayed++
			continue
		}
		ready++
	}
	return ready, delayed
}

// claimRetry serves the lane's redelivery queue and reports whether the ready
// queue may be served as well. It mirrors retryQueue.claim: a sealed lane
// hands out one record at a time and opens the ready queue only once it holds
// no redelivery at all. It returns claims with the redeliveries it leased
// added, including when it fails part way.
func (m *Model) claimRetry(s *sub, ln *lane, li int, op Op, claims []Claim) ([]Claim, bool, error) {
	if m.sealing() {
		if ln.out > 0 {
			return claims, false, nil
		}
		if len(ln.retry) == 0 {
			return claims, true, nil
		}
		if ln.retry[0].dueAt > m.now {
			return claims, false, nil
		}
		seq := ln.retry[0].seq
		ln.retry = ln.retry[1:]
		leased, err := m.leaseRetry(s, ln, li, seq, op, claims, true)
		return leased, false, err
	}
	// A lane that is not sealed serves every redelivery that has come due,
	// in queue order, and steps over the ones still waiting for theirs.
	kept := ln.retry[:0]
	var take []uint64
	for _, ent := range ln.retry {
		if len(claims)+len(take) < op.Max && ent.dueAt <= m.now {
			take = append(take, ent.seq)
			continue
		}
		kept = append(kept, ent)
	}
	ln.retry = kept
	for _, seq := range take {
		var err error
		if claims, err = m.leaseRetry(s, ln, li, seq, op, claims, false); err != nil {
			return claims, true, err
		}
	}
	return claims, true, nil
}

// leaseRetry turns a redelivery into a delivery of the fetching consumer and
// returns claims with it added.
func (m *Model) leaseRetry(s *sub, ln *lane, li int, seq uint64, op Op, claims []Claim, seal bool) ([]Claim, error) {
	r := s.recs[seq]
	if op.Observed != nil {
		i := len(claims)
		if i >= len(op.Observed) || op.Observed[i] != r.origin {
			return claims, errMismatch
		}
	}
	if seal {
		r.sealed = true
		ln.out++
	}
	return append(claims, m.lease(s, ln, li, seq, op.Consumer)), nil
}

// lease is the delivery bookkeeping shared by the retry queue and the ready
// queue.
func (m *Model) lease(s *sub, ln *lane, li int, seq uint64, consumer uint32) Claim {
	r := s.recs[seq]
	r.gen++
	if r.gen == 0 {
		r.gen = 1
	}
	r.attempts++
	r.owner = consumer
	r.state = InFlight
	r.cell = cellActive
	r.deadline = maxDuration
	if m.cfg.AckDeadline > 0 {
		r.deadline = m.now + m.cfg.AckDeadline
	}
	pushLive(ln, s, seq)
	s.st.attempts++
	return Claim{
		Seq: seq, Origin: r.origin, Gen: r.gen, Attempts: r.attempts,
		Partition: li, Size: r.size,
	}
}

// exhausted reports whether a failed delivery has used up the record's
// attempts (engine Subscription.exhausted).
func (m *Model) exhausted(r *rec) bool {
	return m.cfg.MaxAttempts > 0 && r.attempts >= m.cfg.MaxAttempts
}

// deadLetter routes a record away from its subscription because it has used
// up its attempts, and reports whether it left. It mirrors lane.deadLetter:
// a subscription with no dead-letter topic discards the record, an armed
// dead-letter fault fails the publication and leaves the record for the
// caller to put back, and a committed publication settles it.
//
// The model's dead-letter topic is a sink, so a published record leaves the
// engine at once: its payload reference is dropped and the publication is
// counted as one made to a topic with no subscription, which is what the
// differential harness configures (Config.DeadLetter).
func (m *Model) deadLetter(s *sub, ln *lane, r *rec) bool {
	m.releaseSeal(ln, r)
	if !m.cfg.DeadLetter {
		m.dropRecord(s, ln, r)
		return true
	}
	if m.dlqFail {
		s.st.dlqFailed++
		r.overBudget = true
		return false
	}
	r.state = Acked
	s.st.deadLettered++
	m.orphans++
	m.logSettle(r)
	m.unreserveLane(ln, r.size)
	m.releaseTotal(1, r.size)
	m.dropRef(r.origin)
	return true
}

// Held returns the records a subscription holds that a consumer could be
// given now, in publication order, and the subset of them currently leased.
// A record still waiting for its due time is in neither: a delayed
// publication or a delayed redelivery is not deliverable, and strict ordering
// says nothing about a record the publisher asked to hold back.
//
// A differential harness uses the two to check strict ordering without going
// through the model's own placement rules: whatever a fetch returns must be
// the first deliverable records of the subscription.
func (m *Model) Held(name string) (held, leased []uint64) {
	_, s := m.find(name)
	if s == nil {
		return nil, nil
	}
	for li := range s.lanes {
		m.promote(s, &s.lanes[li])
	}
	for _, seq := range s.order {
		r := s.recs[seq]
		if r == nil || r.state.Terminal() || r.state == Delayed || r.reordered {
			continue
		}
		// The publication sequence, not the model's own identity: it is the
		// one both sides can name (Claim.Origin).
		held = append(held, r.origin)
		if r.state == InFlight {
			leased = append(leased, r.origin)
		}
	}
	return held, leased
}
