package memq

import (
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
	default:
		return errcode.ErrLeaseLost
	}
}

// settle applies Ack (terminal) or Nack (requeue).
func (m *Model) settle(op Op, terminal bool) Result {
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
	if err := beginErr(r, op.Gen); err != nil {
		return Result{Err: err, Stale: r.state.Terminal()}
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

// extend moves a lease deadline.
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
	if d > 0 {
		r.deadline = m.now + d
	} else {
		r.deadline = maxDuration
	}
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
		for _, seq := range ln.live {
			r := s.recs[seq]
			if r.cell == cellActive {
				r.cell = cellClosed
			}
			m.dropRecord(s, ln, r)
		}
		ln.live = nil
		for _, seq := range ln.delayed {
			m.dropRecord(s, ln, s.recs[seq])
		}
		ln.delayed = nil
		for _, ent := range ln.retry {
			m.dropRecord(s, ln, s.recs[ent.seq])
		}
		ln.retry = nil
		ln.out = 0
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
