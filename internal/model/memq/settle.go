package memq

import (
	"errors"
	"sort"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// find returns the index and the subscription with the given name.
func (m *Model) find(name string) (int, *sub) {
	for i, s := range m.subs {
		if s.name == name {
			return i, s
		}
	}
	return -1, nil
}

// sortByDue orders a batch of promoted records by due time, which is the
// order an ordered scheduler kind fires them in.
func sortByDue(s *sub, seqs []uint64) {
	sort.SliceStable(seqs, func(i, j int) bool {
		return s.recs[seqs[i]].dueAt < s.recs[seqs[j]].dueAt
	})
}

// beginErr is the settlement error the engine's cell produces for a handle
// presenting gen, or nil when the handle wins.
func beginErr(r *rec, gen uint32) error {
	if r.gen != gen {
		return errcode.ErrLeaseLost
	}
	switch r.cell {
	case cellActive:
		return nil
	case cellSettled:
		return errcode.ErrAlreadySettled
	case cellClosed:
		return errcode.ErrClosed
	case cellSettling:
		return errcode.ErrSettlementInProgress
	default:
		return errcode.ErrLeaseLost
	}
}

// begin finds the delivery a settlement presents and begins its settlement:
// it returns the subscription and the record, or the Result the engine must
// produce for a handle that cannot settle.
func (m *Model) begin(op Op) (*sub, *rec, Result) {
	_, s := m.find(op.Sub)
	if s == nil {
		return nil, nil, Result{Err: errcode.ErrLeaseLost, Stale: true}
	}
	r := s.recs[op.Seq]
	if r == nil {
		return nil, nil, Result{Err: errcode.ErrNotSettleable, Stale: true}
	}
	if r.owner != op.Consumer {
		return nil, nil, Result{Err: errcode.ErrLeaseLost, Stale: true}
	}
	if err := beginErr(r, op.Gen); err != nil {
		return nil, nil, Result{Err: err, Stale: r.state.Terminal()}
	}
	return s, r, Result{}
}

// settle applies Ack (terminal) or Nack (requeue).
func (m *Model) settle(op Op, terminal bool) Result {
	s, r, res := m.begin(op)
	if r == nil {
		return res
	}
	ln := &s.lanes[r.lane]
	removeLive(ln, op.Seq)
	r.cell = cellSettled
	m.releaseSeal(ln, r)
	if terminal {
		r.state = Acked
		s.st.acked++
		m.logSettle(r)
		m.unreserveLane(ln, r.size)
		m.releaseTotal(1, r.size)
		m.dropRef(r.origin)
		return Result{}
	}
	s.st.nacked++
	if m.exhausted(r) && m.deadLetter(s, ln, r) {
		// The record used up its attempts and left the subscription, which
		// only a committed dead-letter publication does.
		return Result{}
	}
	at := time.Duration(0)
	if op.Delay > 0 {
		at = m.now + op.Delay
	}
	m.requeue(s, ln, op.Seq, at)
	s.st.requeued++
	return Result{}
}

// deadLetterBegin begins the explicit dead-letter move of a delivery (engine
// Consumer.DeadLetter). Its errors are those of Ack; once it has begun, the
// delivery is Settling until deadLetterEnd, and the operations that withdraw
// a lease record their withdrawal on it instead (expire, revoke, drain).
func (m *Model) deadLetterBegin(op Op) Result {
	_, r, res := m.begin(op)
	if r == nil {
		return res
	}
	r.cell = cellSettling
	r.pending, r.closed = withdrawNone, false
	return Result{}
}

// deadLetterEnd ends the move deadLetterBegin started, the way engine
// Consumer.DeadLetter and lane.refused do.
//
// A copy the dead-letter topic took settles the record, counted in Acked as
// the explicit route counts it, whatever withdrawal landed meanwhile: the
// settlement wins. The copy goes to a topic with no subscription, which is
// what the differential harness moves to, so it is a discarded publication.
//
// A refused copy settles nothing. The lease survives it and the refusal is
// returned, unless a withdrawal landed while the copy was in flight: the
// record then goes where that withdrawal would have put it, and the move
// reports ErrLeaseLost, or ErrClosed after a close or a drain. An expiry is
// counted in Expired and routes a record that has used its attempts to the
// attempt-limit route; a reassignment gives back the attempt the lease
// counted; a close keeps it and puts the record back; a drain drops it.
func (m *Model) deadLetterEnd(op Op) Result {
	_, s := m.find(op.Sub)
	var r *rec
	if s != nil {
		r = s.recs[op.Seq]
	}
	if r == nil || r.cell != cellSettling || r.gen != op.Gen {
		return Result{Err: errors.New("memq: deadletterend of a delivery with no move in flight")}
	}
	ln := &s.lanes[r.lane]
	why, closed := r.pending, r.closed
	r.pending, r.closed = withdrawNone, false
	if !op.Refuse {
		removeLive(ln, op.Seq)
		r.cell = cellSettled
		m.releaseSeal(ln, r)
		r.state = Acked
		s.st.acked++
		m.orphans++
		m.logSettle(r)
		m.unreserveLane(ln, r.size)
		m.releaseTotal(1, r.size)
		m.dropRef(r.origin)
		return Result{}
	}
	if why == withdrawNone && !closed {
		r.cell = cellActive
		return Result{Err: errcode.ErrMessageTooLarge}
	}
	removeLive(ln, op.Seq)
	switch {
	case why == withdrawDrained:
		r.cell = cellClosed
		m.dropRecord(s, ln, r)
		return Result{Err: errcode.ErrClosed}
	case closed:
		r.cell = cellClosed
		r.overBudget = true
		m.requeue(s, ln, op.Seq, 0)
		s.st.requeued++
		return Result{Err: errcode.ErrClosed}
	case why == withdrawMoved:
		r.cell = cellIdle
		if r.attempts > 0 {
			r.attempts--
		}
		r.overBudget = true
		m.requeue(s, ln, op.Seq, 0)
		s.st.requeued++
		return Result{Err: errcode.ErrLeaseLost}
	default:
		r.cell = cellIdle
		s.st.expired++
		if !m.exhausted(r) || !m.deadLetter(s, ln, r) {
			m.requeue(s, ln, op.Seq, 0)
			s.st.requeued++
		}
		return Result{Err: errcode.ErrLeaseLost}
	}
}

// extend moves a lease deadline to at least op.Dur from now. It never brings
// a deadline forward, and a lease that never expires keeps never expiring
// (engine Consumer.Extend).
func (m *Model) extend(op Op) Result {
	_, s := m.find(op.Sub)
	if s == nil {
		return Result{Err: errcode.ErrLeaseLost, Stale: true}
	}
	r := s.recs[op.Seq]
	if r == nil {
		return Result{Err: errcode.ErrNotSettleable, Stale: true}
	}
	if r.owner != op.Consumer {
		return Result{Err: errcode.ErrLeaseLost, Stale: true}
	}
	if r.gen != op.Gen {
		return Result{Err: errcode.ErrLeaseLost, Stale: true}
	}
	if r.cell != cellActive {
		return Result{Err: beginErr(r, op.Gen), Stale: r.state.Terminal()}
	}
	d := op.Dur
	if d <= 0 {
		d = m.cfg.AckDeadline
	}
	ln := &s.lanes[r.lane]
	removeLive(ln, op.Seq)
	at := maxDuration
	if d > 0 {
		at = m.now + d
	}
	r.deadline = max(r.deadline, at)
	pushLive(ln, s, op.Seq)
	return Result{}
}

// expire reclaims every lease whose deadline has passed.
func (m *Model) expire() int {
	total := 0
	for _, s := range m.subs {
		if s.closed {
			continue
		}
		for li := range s.lanes {
			ln := &s.lanes[li]
			kept := make([]uint64, 0, len(ln.live))
			var due []uint64
			stop := false
			for _, seq := range ln.live {
				r := s.recs[seq]
				if stop || r.deadline > m.now {
					stop = true
					kept = append(kept, seq)
					continue
				}
				if r.cell == cellSettling {
					// The move in flight decides; a refusal of its copy
					// carries this expiry out (deadLetterEnd).
					if r.pending == withdrawNone {
						r.pending = withdrawExpired
					}
					kept = append(kept, seq)
					continue
				}
				r.cell = cellIdle
				due = append(due, seq)
			}
			ln.live = kept
			n, routed := 0, 0
			for _, seq := range due {
				r := s.recs[seq]
				if m.exhausted(r) && m.deadLetter(s, ln, r) {
					routed++
					continue
				}
				m.requeue(s, ln, seq, 0)
				n++
			}
			s.st.expired += int64(n + routed)
			s.st.requeued += int64(n)
			total += n + routed
		}
	}
	return total
}

// logSettle drops one subscription's reference to a record's slot in the
// shared partition log, which is what lets the log reclaim it once every
// subscription has done the same.
func (m *Model) logSettle(r *rec) {
	if m.cfg.SharedLog {
		m.logs[r.lane].settle(r.origin)
	}
}

// dropRef drops one subscription's reference to a record and records the
// release when it was the last.
func (m *Model) dropRef(seq uint64) {
	m.refs[seq]--
	if m.refs[seq] == 0 && !m.cfg.SafeCopy {
		m.release[seq]++
	}
}

// closeSub detaches a subscription and drops everything it holds.
func (m *Model) closeSub(op Op) Result {
	_, s := m.find(op.Sub)
	if s == nil || s.closed {
		return Result{}
	}
	s.closed = true
	m.drain(s)
	return Result{}
}

// closeEngine shuts the model down.
func (m *Model) closeEngine() {
	if m.closed {
		return
	}
	m.closed = true
	for _, s := range m.subs {
		if s.closed {
			continue
		}
		s.closed = true
		m.drain(s)
	}
	// The retained log holds a reference of its own on every record it
	// keeps, which the engine's close releases too (Topic.dropAllRetained).
	m.dropAllRetained()
}

// drain drops every record a subscription holds: in flight, then delayed,
// then ready, exactly as the engine walks its lanes.
func (m *Model) drain(s *sub) {
	for li := range s.lanes {
		ln := &s.lanes[li]
		var kept []uint64
		for _, seq := range ln.live {
			r := s.recs[seq]
			if r.cell == cellSettling {
				// A move in flight keeps the record and its seal; a refusal
				// of its copy drops it (deadLetterEnd, engine
				// Subscription.drain).
				r.pending, r.closed = withdrawDrained, true
				kept = append(kept, seq)
				continue
			}
			if r.cell == cellActive {
				r.cell = cellClosed
			}
			m.dropRecord(s, ln, r)
		}
		ln.live = kept
		for _, seq := range ln.delayed {
			m.dropRecord(s, ln, s.recs[seq])
		}
		ln.delayed = nil
		for _, ent := range ln.retry {
			m.dropRecord(s, ln, s.recs[ent.seq])
		}
		ln.retry = nil
		ln.out = 0
		for _, seq := range kept {
			if s.recs[seq].sealed {
				ln.out++
			}
		}
		for _, seq := range ln.ready.seqs {
			m.dropRecord(s, ln, s.recs[seq])
		}
		ln.ready.reset()
	}
}

// dropRecord discards one record without settling it.
func (m *Model) dropRecord(s *sub, ln *lane, r *rec) {
	m.releaseSeal(ln, r)
	r.state = Dropped
	s.st.dropped++
	m.logSettle(r)
	m.unreserveLane(ln, r.size)
	m.releaseTotal(1, r.size)
	m.dropRef(r.origin)
}
