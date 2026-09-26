package engine

import (
	"cmp"
	"slices"

	"github.com/iqhive/iqmq/internal/timer"
)

// partitionStore is where the records of one subscription-partition wait
// between admission and delivery: the ready set a consumer claims from and
// the delayed set that is still holding records back until their due time
// (the specification A).
//
// What the Arch family varies is only this. The in-flight list, the
// settlement cells, the capacity accounting, the statistics and the wait
// strategy live in the lane and are the same for every architecture, so a new
// architecture is one file and cannot change the meaning of an
// acknowledgement.
//
// # Locking
//
// A store guards its own structures. The lane mutex may be held across a call
// into the store — the expiry and close paths do exactly that — so a store
// must never call back into its lane while holding a lock of its own.
type partitionStore[P any] interface {
	// Publish admits rec: ready when at is not after now, delayed until at
	// otherwise. Capacity is already reserved.
	Publish(rec *record[P], at, now int64)
	// Claim removes up to len(dst) records that are due at now, writes an
	// envelope for each into dst and returns how many it wrote. The
	// envelopes are not leased: the lane does that.
	Claim(now int64, dst []*Envelope[P]) int
	// Requeue returns a claimed envelope to the store, ready when at is not
	// after now and delayed until at otherwise.
	Requeue(e *Envelope[P], at, now int64)
	// Settle releases what the store holds for an envelope that has reached
	// a terminal state, which for a shared architecture is the reference
	// that keeps the record's slot alive. It is called once per envelope
	// that leaves through Claim.
	Settle(e *Envelope[P])
	// Depth reports the records the store holds, split into the ones a
	// consumer could claim now and the ones waiting for their due time. It
	// is exact while the lane is quiescent, which is the condition every
	// invariant check runs under.
	Depth(now int64) (ready, delayed int)
	// DropOldest removes the oldest record the store holds — ready before
	// delayed — and returns its envelope for the lane to drop. It reports
	// false when the store holds nothing, which is what makes the DropSlow
	// policy fail for a subscription whose records are all in flight.
	DropOldest() (*Envelope[P], bool)
	// Drain removes every record the store holds and appends an envelope for
	// each to dst.
	Drain(dst []*Envelope[P]) []*Envelope[P]
	// Promote moves every record whose due time has arrived into the ready
	// set and returns how many. The scheduler calls it when a lane's time
	// comes; a claim does it on its own.
	Promote(now int64) int
	// Next returns the earliest time at which the store would release a
	// record it cannot release now, or 0 when it holds nothing delayed. The
	// engine scheduler wakes on it.
	Next() int64
	// ExpireTTL removes every record the store holds whose time to live has
	// run out at now, appends an envelope for each to dst and returns the
	// earliest expiry still to come, or 0. The lane calls it only when it
	// knows an expiry is due.
	ExpireTTL(now int64, dst []*Envelope[P]) ([]*Envelope[P], int64)
}

// sortFired orders a promoted batch by due time and, for the records that
// came due together, by publication sequence.
//
// A scheduler kind promises no order within one firing — a wheel fires a
// whole bucket — so without this the order a lane serves its delayed records
// in, and the record a full lane displaces under OverflowDropOldest, would
// depend on the kind rather than on the topology. The cost is
// paid only by the lanes that hold delayed records, and only on the batch
// that came due.
//
// It is called after every Fire whose result order a caller can observe --
// the promotion, the drain a close or a time-to-live sweep takes, and the
// records such a sweep puts back -- which is what makes KindSet.QueueOrdered
// independent and the message contract's strict ordering
// available under every scheduler kind.
func sortFired[P any](fired []*timer.Entry[*Envelope[P]]) {
	if len(fired) < 2 {
		return
	}
	slices.SortFunc(fired, func(a, b *timer.Entry[*Envelope[P]]) int {
		if c := cmp.Compare(a.Value.deadline, b.Value.deadline); c != 0 {
			return c
		}
		return cmp.Compare(a.Value.seq, b.Value.seq)
	})
}

// oldestDelayed empties sched and returns the envelope with the earliest due
// time, together with a scheduler holding the rest, rebuilt at now.
//
// A scheduler kind cannot name an entry without firing it, and firing moves a
// wheel's own clock forward — Scheduler.Fire says now must not decrease — so
// a record armed after a drop that fired at a future time would be due at
// once. The structure is therefore emptied and rebuilt at now from the
// survivors, each at the time its envelope carries, which is exactly where it
// was.
func oldestDelayed[P any](sched timer.Scheduler[*Envelope[P]], k timer.Kind, now int64,
	scratch []*timer.Entry[*Envelope[P]],
) (timer.Scheduler[*Envelope[P]], []*timer.Entry[*Envelope[P]], *Envelope[P], bool) {
	scratch = sched.Fire(neverExpires, scratch[:0])
	if len(scratch) == 0 {
		return sched, scratch, nil, false
	}
	// The earliest due time, and the earliest publication among the records
	// that share it: a tie broken by the scheduler's own order would make
	// which record a full lane displaces depend on the kind.
	oldest := scratch[0]
	for _, ent := range scratch[1:] {
		switch {
		case ent.Value.deadline < oldest.Value.deadline:
			oldest = ent
		case ent.Value.deadline == oldest.Value.deadline && ent.Value.seq < oldest.Value.seq:
			oldest = ent
		}
	}
	next := timer.New[*Envelope[P]](k, now)
	for _, ent := range scratch {
		if ent != oldest {
			next.Add(ent, ent.Value.deadline)
		}
	}
	return next, scratch, oldest.Value, true
}

// expireByDrain implements ExpireTTL for a store whose records can be taken
// out and put back: everything is drained, the expired records are collected
// and the survivors are put back in the order they came out, each at the time
// its envelope carries — 0 for a ready record, its due time for a delayed
// one.
func expireByDrain[P any](s partitionStore[P], now int64, dst []*Envelope[P]) ([]*Envelope[P], int64) {
	base := len(dst)
	dst = s.Drain(dst)
	next := int64(0)
	kept := dst[:base]
	for _, e := range dst[base:] {
		if expired(e, now) {
			kept = append(kept, e)
			continue
		}
		if at := e.rec.expireAt; at > 0 && (next == 0 || at < next) {
			next = at
		}
		s.Requeue(e, e.deadline, now)
	}
	return kept, next
}

// newStore builds the store of one lane: the architecture's own structure,
// wrapped in one ring per priority level when the topic asks for priorities
// (the memory engine).
func newStore[P any](ln *lane[P], now int64) partitionStore[P] {
	if ln.sub.topic.cfg.Priority {
		return newPriorityStore(ln, now)
	}
	return newBaseStore(ln, now)
}

// newBaseStore builds the store of one lane for the engine's architecture.
func newBaseStore[P any](ln *lane[P], now int64) partitionStore[P] {
	switch ln.sub.eng.kinds.Arch {
	case ChanFanout:
		return newChanStore(ln)
	case LogCursor:
		return newLogStore(ln)
	default:
		// PerSubEnvelopes and FullCopy share the storage: they differ in
		// what a publication puts in it, one shared record or one copy per
		// subscription, which Topic.commit decides.
		return newPersubStore(ln, now)
	}
}
