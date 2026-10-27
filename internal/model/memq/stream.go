package memq

import (
	"github.com/iqhive/iqmq/internal/errcode"
)

//
// That distinction is why rec has both seq and origin. The engine hands a
// replayed record the same Info.Seq as the original — it is the same
// publication — while the subscription now holds a second, independently
// settled copy of it. The model gives each admission its own identity and
// carries the publication sequence alongside, so both sides can name the same
// delivery without the model having to hold two records under one key.

// ErrNoRetention is what a Seek on a topology that retains nothing returns.
// It mirrors engine.ErrInvalidConfig.
var ErrNoRetention = errcode.New(errcode.InvalidConfig, "memq: the topology retains nothing")

// ErrNoWildcards is what a subscription filter on a topology without
// wildcards returns. It mirrors engine.ErrInvalidConfig.
var ErrNoWildcards = errcode.New(errcode.InvalidConfig, "memq: the topology has no wildcards")

// retains reports whether the model keeps a retained log.
func (m *Model) retains() bool { return m.retained != nil }

// retain appends a publication to its partition's retained log and trims the
// log back within its bounds.
func (m *Model) retain(part int, seq uint64, size int64, prio uint8, subj string) {
	lg := &m.retained[part]
	lg.ents = append(lg.ents, retSlot{subj: subj, seq: seq, size: size, at: m.now, prio: prio})
	lg.bytes += size
	m.refs[seq]++
	m.trimLog(lg)
}

// retainOnly puts a publication no subscription took into the retained log
// (engine Topic.retainOnly). It consumes a publication sequence, so the
// engine and the model agree on what the log holds and on what a later
// StartAtSeq replays.
func (m *Model) retainOnly(part int, op Op, size int64) {
	m.seq++
	origin := m.seq
	m.refs[origin] = 0
	if m.cfg.SafeCopy {
		// The publisher's payload comes back as soon as the log's own copy
		// is taken; the copy itself is engine-owned.
		m.release[origin]++
	}
	m.retain(part, origin, size, m.prioOf(op.Priority), op.Subject)
}

// trimLog removes the entries the retention bounds no longer cover.
func (m *Model) trimLog(lg *retLog) {
	r := m.cfg.Retention
	for len(lg.ents) > 0 {
		e := lg.ents[0]
		over := (r.MaxMessages > 0 && len(lg.ents) > r.MaxMessages) ||
			(r.MaxBytes > 0 && lg.bytes > r.MaxBytes) ||
			(r.MaxAge > 0 && e.at+r.MaxAge <= m.now)
		if !over {
			return
		}
		lg.bytes -= e.size
		lg.ents = lg.ents[1:]
		m.dropRef(e.seq)
	}
}

// trimRetention applies the age bound to every partition, which the engine
// does whenever a retained log is read or written.
func (m *Model) trimRetention() {
	for i := range m.retained {
		m.trimLog(&m.retained[i])
	}
}

// dropAllRetained empties every retained log, which the engine's close does.
func (m *Model) dropAllRetained() {
	for i := range m.retained {
		lg := &m.retained[i]
		for _, e := range lg.ents {
			m.dropRef(e.seq)
		}
		lg.ents = nil
		lg.bytes = 0
	}
}

// seek replays the retained log into a subscription from a position: every
// record it holds is discarded and the retained records at or after the
// position are admitted again, in publication order, as far as the
// subscription's capacity allows.
func (m *Model) seek(op Op) Result {
	_, s := m.find(op.Sub)
	if !m.retains() {
		return Result{Err: ErrNoRetention}
	}
	if m.closed || s == nil || s.closed {
		return Result{Err: errcode.ErrClosed}
	}
	m.drain(s)
	n := m.replay(s, op.Start, 0)
	return Result{N: n}
}

// replay admits the retained records at or after pos, ignoring anything
// published after upto when upto is not zero.
func (m *Model) replay(s *sub, pos StartPosition, upto uint64) int {
	if pos.Kind == StartLatest || !m.retains() {
		return 0
	}
	m.trimRetention()
	n := 0
	for part := range m.retained {
		ln := &s.lanes[part]
		for _, e := range m.retained[part].ents {
			if upto > 0 && e.seq > upto {
				break
			}
			if !m.deliverable(s, e.subj) {
				// A replay is an admission like any other: a subscription
				// is never handed a retained record its filter excludes.
				continue
			}
			switch pos.Kind {
			case StartAtSeq:
				if e.seq < pos.Seq {
					continue
				}
			case StartAtTime:
				if e.at < pos.At {
					continue
				}
			case StartLatest, StartEarliest:
				// StartLatest replays nothing and returned above; the
				// earliest position takes everything the log still holds.
			}
			if !m.reserveTotal(1, e.size) {
				break
			}
			if !m.reserveLane(ln, e.size) {
				m.releaseTotal(1, e.size)
				break
			}
			m.rid++
			id := m.rid
			r := &rec{seq: id, origin: e.seq, size: e.size, lane: part, state: Ready, prio: e.prio}
			s.recs[id] = r
			s.order = append(s.order, id)
			ln.ready.push(id, e.prio)
			// A replayed record is one more holder of the publication the
			// retained entry keeps alive, so the release ledger stays keyed
			// by publication (dropRef takes rec.origin).
			m.refs[e.seq]++
			s.st.published++
			n++
		}
	}
	return n
}

// dedupAdmit reports whether a publication carrying key is admitted. A second
// publication of the same key inside the window is refused and does not push
// the window forward.
func (m *Model) dedupAdmit(key string) bool {
	if m.dedup == nil || key == "" {
		return true
	}
	if at, ok := m.dedup[key]; ok && at > m.now {
		return false
	}
	m.dedup[key] = m.now + m.cfg.DedupWindow
	return true
}

// dedupForget gives a key back to the window, which the engine does for a
// publication it refused: the caller still owns the payload and the retry the
// error asks for must not be discarded as a duplicate (engine Topic.dedupSettle).
func (m *Model) dedupForget(key string) {
	if m.dedup == nil || key == "" {
		return
	}
	delete(m.dedup, key)
}

// deliverable reports whether a subscription takes a publication with the
// given subject.
func (m *Model) deliverable(s *sub, subj string) bool {
	return !m.cfg.Wildcards || s.filter == nil || s.filter.Match(subj)
}

// prioOf clamps a publication's priority to the configured level count.
func (m *Model) prioOf(p uint8) uint8 {
	if !m.cfg.Priority {
		return 0
	}
	if int(p) >= m.cfg.Levels {
		return uint8(m.cfg.Levels - 1)
	}
	return p
}
