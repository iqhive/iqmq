package memq

import "fmt"

// Snapshot is a copy of the whole model.
type Snapshot struct {
	// Now is the model clock.
	Now int64
	// Steps is the number of operations applied.
	Steps int
	// Subs holds one entry per subscription, in creation order.
	Subs []SubSnapshot
	// Messages and Bytes are the engine-wide accounting.
	Messages, Bytes int64
	// Orphans counts publications discarded for want of a subscription.
	Orphans int64
	// Closed reports whether the engine has been closed.
	Closed bool
}

// SubSnapshot mirrors engine.Snapshot for one subscription.
type SubSnapshot struct {
	// Name is the subscription name.
	Name string
	// Ready, InFlight and Delayed are the lane gauges.
	Ready, InFlight, Delayed int
	// Bytes is the accounted size of every record the lanes hold.
	Bytes int64
	// The counters mirror engine.Snapshot field for field.
	Published, Acked, Nacked, Requeued, Expired, Dropped, Rejected, Attempts int64
	// DeadLettered, Overflowed and DeadLetterFailures mirror the
	// counters of engine.Snapshot.
	DeadLettered, Overflowed, DeadLetterFailures int64
	// ActiveConsumers counts attached consumers.
	ActiveConsumers int
	// Retained counts the records the topic keeps for replay, and
	// Deduplicated the publications its dedup window discarded; both are
	// properties of the topic, so every subscription reports the same
	// number (engine Snapshot).
	Retained     int
	Deduplicated int64
	// Assignment is the subscription's rebalance generation.
	Assignment uint64
}

// Conserved reports whether every admitted record is accounted for.
func (s SubSnapshot) Conserved() bool {
	return s.Published == s.Acked+int64(s.Ready)+int64(s.InFlight)+int64(s.Delayed)+
		s.DeadLettered+s.Dropped
}

// Snapshot returns a copy of the model state. Like the engine's Inspect it
// first promotes every delayed record that has come due, so that the two
// gauges are comparable.
func (m *Model) Snapshot() Snapshot {
	snap := Snapshot{
		Now:      int64(m.now),
		Steps:    m.steps,
		Messages: m.msgs,
		Bytes:    m.bytes,
		Orphans:  m.orphans,
		Closed:   m.closed,
		Subs:     make([]SubSnapshot, 0, len(m.subs)),
	}
	for _, s := range m.subs {
		snap.Subs = append(snap.Subs, m.subSnapshot(s))
	}
	return snap
}

// Sub returns the snapshot of one subscription.
func (m *Model) Sub(name string) (SubSnapshot, bool) {
	_, s := m.find(name)
	if s == nil {
		return SubSnapshot{}, false
	}
	return m.subSnapshot(s), true
}

func (m *Model) subSnapshot(s *sub) SubSnapshot {
	out := SubSnapshot{
		Name:               s.name,
		DeadLettered:       s.st.deadLettered,
		Overflowed:         s.st.overflowed,
		DeadLetterFailures: s.st.dlqFailed,

		Published:       s.st.published,
		Acked:           s.st.acked,
		Nacked:          s.st.nacked,
		Requeued:        s.st.requeued,
		Expired:         s.st.expired,
		Dropped:         s.st.dropped,
		Rejected:        s.st.rejected,
		Attempts:        s.st.attempts,
		ActiveConsumers: len(s.consumers),
		Assignment:      s.version,
		Deduplicated:    m.deduped,
	}
	out.Retained, _ = m.Retained()
	for li := range s.lanes {
		ln := &s.lanes[li]
		m.promote(s, ln)
		rready, rdelayed := m.retryDepth(ln)
		out.Ready += ln.ready.len() + rready
		out.InFlight += len(ln.live)
		out.Delayed += len(ln.delayed) + rdelayed
		out.Bytes += ln.bytes
	}
	return out
}

// Invariants checks the record-lifecycle invariants of the memory engine
// after every step. Their names are the ones models/tla/MemoryLease.tla
// carries, so that tools/modelcheck can pair them:
//
//   - TypeOK: every record has a defined state and a defined settlement cell,
//     and every counter is non-negative.
//   - OneState: a record appears in exactly one lane structure, and only when
//     its state says so.
//   - LeaseUnique: an in-flight record has one owner and an active cell;
//     nothing else does.
//   - AttemptsMonotone: attempts are positive exactly for records that have
//     been delivered, and never exceed the subscription's attempt total.
//   - CapacityNonNegative: lane depth and byte accounting never go negative
//     and never exceed the configured capacity.
//   - Conservation: published == acked + ready + in-flight + delayed +
//     dropped for every subscription.
//   - ReleaseOnce: a record's payload is released exactly once, after the
//     last subscription has settled or dropped it.
//   - NoEarlyDelivery: a delayed record is never delivered before its due
//     time, and a record that is in flight reached that state at or after
//     its due time — the second half is what makes an early delivery
//     observable, since the record is no longer delayed once it has one.
//   - ClosedTerminal: a closed subscription holds no record; every record it
//     had is terminal.
//   - StrictBlocksLane: a lane that seals its redeliveries — BlockLane, or
//     any placement under strict ordering — has at most one of them
//     outstanding, and the seal it accounts for is held by exactly the
//     deliveries that say so. That nothing overtakes a sealed redelivery is a
//     property of the transitions rather than of one state: the model
//     enforces it in claimRetry and the differential run holds the engine to
//     it fetch by fetch.
//   - MaxAttemptsRespected: no record is delivered more often than
//     Config.MaxAttempts allows, unless something other than a failed
//     delivery put it back in its lane — a dead-letter publication that
//     failed, or a consumer that closed.
//   - DeadLetterBeforeSettle: a record is DeadLettered only when the
//     subscription has a dead-letter topic and the publication committed; a
//     failed one leaves the record in a lane, where the conservation sum
//     still counts it.
//   - OverflowAccounted: a record is only discarded by an overflow policy
//     that says it may be — DropNewest before admission, counted in
//     Overflowed, and DropOldest after it, counted in Dropped.
//   - NoUnadmittedSettlement: every record that leaves a subscription was
//     admitted to it first: the settled, dead-lettered and dropped counters
//     never outrun the admitted one. It is the inequality half of
//     Conservation's sum and is checked before it, so that a settlement of
//     something never admitted reports this name rather than the sum's.
func (m *Model) Invariants() error {
	total := 0
	for _, s := range m.subs {
		if err := m.subInvariants(s); err != nil {
			return err
		}
		total += len(s.recs)
	}
	for seq, n := range m.refs {
		if n < 0 {
			return fmt.Errorf("memq: ReleaseOnce: record %d has %d outstanding references", seq, n)
		}
		want := 0
		if n == 0 || m.cfg.SafeCopy {
			want = 1
		}
		if m.release[seq] != want {
			return fmt.Errorf("memq: ReleaseOnce: record %d released %d times, want %d", seq, m.release[seq], want)
		}
	}
	if m.msgs < 0 || m.bytes < 0 {
		return fmt.Errorf("memq: CapacityNonNegative: engine totals are %d messages, %d bytes", m.msgs, m.bytes)
	}
	if m.cfg.MaxTotalMessages > 0 && m.msgs > int64(m.cfg.MaxTotalMessages) {
		return fmt.Errorf("memq: CapacityNonNegative: %d messages exceed the engine limit", m.msgs)
	}
	if m.cfg.MaxTotalBytes > 0 && m.bytes > m.cfg.MaxTotalBytes {
		return fmt.Errorf("memq: CapacityNonNegative: %d bytes exceed the engine limit", m.bytes)
	}
	return nil
}

func (m *Model) subInvariants(s *sub) error {
	seen := map[uint64]int{}
	count := func(seqs []uint64, want State) error {
		for _, seq := range seqs {
			r := s.recs[seq]
			if r == nil {
				return fmt.Errorf("memq: OneState: %s holds unknown record %d", s.name, seq)
			}
			seen[seq]++
			if r.state != want {
				return fmt.Errorf("memq: OneState: %s record %d is %v but sits in %v", s.name, seq, r.state, want)
			}
		}
		return nil
	}
	var ready, live, delayed int
	for li := range s.lanes {
		ln := &s.lanes[li]
		if err := count(ln.ready.seqs, Ready); err != nil {
			return err
		}
		// A redelivery is ready when its due time has come and delayed
		// until then; either way it is in exactly one structure.
		rready, rdelayed := 0, 0
		out := 0
		for _, ent := range ln.retry {
			want := Ready
			if ent.dueAt > m.now {
				want = Delayed
				rdelayed++
			} else {
				rready++
			}
			if err := count([]uint64{ent.seq}, want); err != nil {
				return err
			}
		}
		for _, seq := range ln.live {
			if s.recs[seq].sealed {
				out++
			}
		}
		if out != ln.out {
			return fmt.Errorf("memq: StrictBlocksLane: %s lane %d counts %d sealed deliveries but holds %d",
				s.name, li, ln.out, out)
		}
		if ln.out > 1 {
			return fmt.Errorf("memq: StrictBlocksLane: %s lane %d has %d redeliveries outstanding at once",
				s.name, li, ln.out)
		}
		ready += rready
		delayed += rdelayed
		if err := count(ln.live, InFlight); err != nil {
			return err
		}
		if err := count(ln.delayed, Delayed); err != nil {
			return err
		}
		ready += ln.ready.len()
		live += len(ln.live)
		delayed += len(ln.delayed)
		if ln.depth < 0 || ln.bytes < 0 {
			return fmt.Errorf("memq: CapacityNonNegative: %s lane %d holds %d records, %d bytes", s.name, li, ln.depth, ln.bytes)
		}
		if held := ln.ready.len() + len(ln.live) + len(ln.delayed) + len(ln.retry); ln.depth != held {
			return fmt.Errorf("memq: CapacityNonNegative: %s lane %d accounts %d records but holds %d",
				s.name, li, ln.depth, held)
		}
		if m.cfg.CapacityMessages > 0 && ln.depth > m.cfg.CapacityMessages {
			return fmt.Errorf("memq: CapacityNonNegative: %s lane %d exceeds its capacity", s.name, li)
		}
		prev := int64(-1 << 62)
		for _, seq := range ln.live {
			d := int64(s.recs[seq].deadline)
			if d < prev {
				return fmt.Errorf("memq: TypeOK: %s lane %d in-flight list is not deadline-ordered", s.name, li)
			}
			prev = d
		}
	}
	for seq, r := range s.recs {
		if r.state >= stateCount {
			return fmt.Errorf("memq: TypeOK: %s record %d has state %d", s.name, seq, r.state)
		}
		if r.state.Terminal() {
			if seen[seq] != 0 {
				return fmt.Errorf("memq: OneState: terminal record %d is still queued in %s", seq, s.name)
			}
			continue
		}
		if seen[seq] != 1 {
			return fmt.Errorf("memq: OneState: record %d appears %d times in %s", seq, seen[seq], s.name)
		}
		if (r.state == InFlight) != (r.cell == cellActive) {
			return fmt.Errorf("memq: LeaseUnique: %s record %d is %v with cell %d", s.name, seq, r.state, r.cell)
		}
		if r.state == InFlight && r.attempts == 0 {
			return fmt.Errorf("memq: AttemptsMonotone: %s record %d is in flight with no attempt", s.name, seq)
		}
		if r.state == Delayed && r.dueAt <= m.now && r.attempts == 0 && len(s.lanes[r.lane].delayed) == 0 {
			return fmt.Errorf("memq: NoEarlyDelivery: %s record %d is delayed past its due time", s.name, seq)
		}
		// The other half of the same invariant: a record that left the
		// waiting states left them on time. Without it the check could not
		// observe a delivery before the due time at all — the record is no
		// longer delayed once it has been delivered.
		if r.state == InFlight && r.dueAt > m.now {
			return fmt.Errorf("memq: NoEarlyDelivery: %s record %d is in flight %v before its due time",
				s.name, seq, r.dueAt-m.now)
		}
	}
	snap := m.subSnapshotNoPromote(s, ready, live, delayed)
	// Nothing leaves a subscription that was never admitted to it. It is
	// the inequality half of Conservation's sum, checked first and on its
	// own name: a settlement the subscription never admitted breaks this
	// one, and a record that left a lane without being counted breaks the
	// sum below.
	if settled := snap.Acked + snap.DeadLettered + snap.Dropped; settled > snap.Published {
		return fmt.Errorf("memq: NoUnadmittedSettlement: %s settled %d records but admitted %d",
			s.name, settled, snap.Published)
	}
	if !snap.Conserved() {
		return fmt.Errorf("memq: Conservation: %s published %d but accounts %d+%d+%d+%d+%d",
			s.name, snap.Published, snap.Acked, snap.Ready, snap.InFlight, snap.Delayed, snap.Dropped)
	}
	if s.st.attempts < 0 || s.st.acked < 0 || s.st.published < 0 {
		return fmt.Errorf("memq: TypeOK: %s has a negative counter", s.name)
	}
	if m.cfg.MaxAttempts > 0 {
		// A record is delivered no more often than its budget allows. Two
		// things put a record back in its lane without a delivery having
		// failed, and both are allowed past it: a dead-letter publication
		// that failed, which ADR-011 puts above the budget, and a consumer
		// that closed, which is a rebalance rather than a failure.
		for seq, r := range s.recs {
			if r.attempts > m.cfg.MaxAttempts && !r.overBudget {
				return fmt.Errorf("memq: MaxAttemptsRespected: %s record %d has %d attempts, limit %d",
					s.name, seq, r.attempts, m.cfg.MaxAttempts)
			}
		}
	}
	if s.st.deadLettered > 0 && !m.cfg.DeadLetter {
		return fmt.Errorf("memq: DeadLetterBeforeSettle: %s dead-lettered %d records with no dead-letter topic",
			s.name, s.st.deadLettered)
	}
	if s.st.deadLettered > 0 && m.cfg.MaxAttempts == 0 {
		return fmt.Errorf("memq: MaxAttemptsRespected: %s dead-lettered %d records with no attempt limit",
			s.name, s.st.deadLettered)
	}
	if s.st.overflowed > 0 && m.cfg.Overflow != OverflowDropNewest {
		return fmt.Errorf("memq: OverflowAccounted: %s counted %d overflows under %v",
			s.name, s.st.overflowed, m.cfg.Overflow)
	}
	if s.st.displaced > 0 && m.cfg.Overflow != OverflowDropOldest {
		return fmt.Errorf("memq: OverflowAccounted: %s displaced %d records under %v",
			s.name, s.st.displaced, m.cfg.Overflow)
	}
	if s.closed && (ready+live+delayed) != 0 {
		return fmt.Errorf("memq: ClosedTerminal: %s is closed but holds %d records", s.name, ready+live+delayed)
	}
	return nil
}

// subSnapshotNoPromote builds the counters view without moving the clock's
// work forward, so that Invariants observes the state the last step left.
func (m *Model) subSnapshotNoPromote(s *sub, ready, live, delayed int) SubSnapshot {
	return SubSnapshot{
		Name: s.name, Ready: ready, InFlight: live, Delayed: delayed,
		Published: s.st.published, Acked: s.st.acked, Nacked: s.st.nacked,
		Requeued: s.st.requeued, Expired: s.st.expired, Dropped: s.st.dropped,
		Rejected: s.st.rejected, Attempts: s.st.attempts,
		DeadLettered: s.st.deadLettered, Overflowed: s.st.overflowed,
		DeadLetterFailures: s.st.dlqFailed,
		ActiveConsumers:    len(s.consumers),
	}
}
