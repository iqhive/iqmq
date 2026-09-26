package engine

import (
	"sync/atomic"

	"github.com/iqhive/iqmq/internal/settlement"
	"github.com/iqhive/iqmq/internal/timer"
)

// record is the immutable payload record shared by every subscription of one
// publication (the memory engine). It is reference-counted: the publisher's
// Release callback runs exactly once, when the last subscription reaches a
// terminal state or the engine drops the record.
type record[P any] struct {
	v       P
	key     []byte
	release func(P)
	// subj is the subject the record was published on, and "" on a topic
	// without wildcards. It is what a replay filters on and what a delivery
	// reports as the resolved name (the API).
	subj string
	seq  uint64
	// expireAt is the time the record's time to live runs out, or 0 when it
	// has none. It is a property of the publication, so every subscription
	// discards the record at the same instant.
	expireAt int64
	// slot is the index of the record in its partition's shared log. It is
	// meaningful only under the LogCursor architecture, where one slot
	// carries the record for every subscription at once.
	slot uint64
	size int32
	part int32
	refs atomic.Int32
	// mode is the ownership contract the record was published under (family
	// The specification). It decides whether the engine may scrub the payload as it hands
	// it back: a borrowed payload is the caller's storage.
	mode CopyMode
	// prio is the priority ring the record belongs to on a topic with
	// priorities enabled, and 0 on every other.
	prio uint8
	// released records that the payload has been handed back, so that a
	// second release is a panic rather than a corrupted free list. It is
	// written only on the release path, which the reference count makes
	// single-threaded, and only read there.
	released bool
}

// retain adds n references. It is called once, before the record is visible.
func (r *record[P]) retain(n int32) { r.refs.Store(n) }

// addRef takes one more reference on a record that is already visible, which
// the dead-letter path does so that the payload outlives the settlement of
// the delivery it is routing away (lane.deadLetter).
func (r *record[P]) addRef() { r.refs.Add(1) }

// tryAddRef takes one more reference on a record that may already have been
// released, which is what a replay racing a retention trim can find. It
// reports false when the record is gone, and never resurrects one.
func (r *record[P]) tryAddRef() bool {
	for {
		n := r.refs.Load()
		if n <= 0 {
			return false
		}
		if r.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// unref gives back a reference taken by addRef without releasing anything:
// the caller still holds the reference of the delivery it is settling, so the
// count cannot reach zero here.
func (r *record[P]) unref() { r.refs.Add(-1) }

// releaseOne drops one reference and reports whether it was the last.
func (r *record[P]) releaseOne() bool { return r.refs.Add(-1) == 0 }

// Envelope is the per-subscription delivery record of one publication
// (the memory engine). Consumers receive a pointer to it in a Claim and
// present it, with the generation from the same Claim, to Ack, Nack and
// Extend. It is pooled: the engine recycles it after a terminal settlement,
// which is safe because its settlement cell is never reset, only re-leased,
// so a handle from an earlier delivery can never match a later one.
//
// A caller must treat an Envelope as opaque and must not retain it after the
// delivery it identifies has been settled.
type Envelope[P any] struct {
	rec  *record[P]
	lane *lane[P] // immutable: envelopes are pooled per lane
	next *Envelope[P]
	prev *Envelope[P]
	// tnode is the intrusive scheduler entry used while the envelope waits
	// in the lane's delayed structure. Value points back at the envelope.
	tnode timer.Entry[*Envelope[P]]

	seq uint64
	// deadline is the lane's time for this envelope. A record is either
	// waiting for a consumer or leased to one, never both, so one field
	// carries both times: the due time while the envelope is delayed (0
	// while it is ready) and the lease deadline while it is in flight.
	deadline int64
	cell     settlement.Cell
	// inList reports whether the envelope is linked into its lane's
	// in-flight list. It is written under the lane mutex and makes unlinking
	// idempotent, so a settlement that races the engine's close can never
	// unlink an envelope twice and corrupt the list.
	inList bool
	// sealed reports that the delivery came out of its lane's redelivery
	// queue while the lane was sealed — a BlockLane placement, or any
	// placement under strict ordering — so the lane serves nothing else
	// until it is settled. It is written under the redelivery queue's mutex.
	sealed   bool
	attempts uint32
	// owner is the id of the consumer holding the current lease. It is
	// atomic because a settling consumer reads it while the engine may be
	// redelivering the record to another consumer.
	owner atomic.Uint32
}

// Gen returns the current delivery generation, or 0 when the envelope holds
// no lease. It is a diagnostic: a settling caller uses the generation handed
// to it in its Claim.
func (e *Envelope[P]) Gen() uint32 {
	_, g := e.cell.Load()
	return uint32(g)
}

// Attempts returns the number of deliveries of this record to its
// subscription so far.
func (e *Envelope[P]) Attempts() uint32 { return e.attempts }

// Seq returns the publication sequence number of the record within its topic.
func (e *Envelope[P]) Seq() uint64 { return e.seq }

// resetEnvelope is the pool reset hook. It clears everything the next
// delivery overwrites and deliberately leaves the settlement cell, the lane
// back-pointer and the scheduler entry alone: the cell must keep counting
// generations across recycles, and the lane never changes for a pooled
// envelope.
func resetEnvelope[P any](e *Envelope[P]) {
	e.rec = nil
	e.next = nil
	e.prev = nil
	e.seq = 0
	e.deadline = 0
	e.attempts = 0
	e.sealed = false
	// owner is deliberately not cleared: it is overwritten by the next
	// delivery, and leaving it lets a handle to a settled delivery reach the
	// settlement cell, which names the precise reason it failed.
}

// resetRecord is the record pool's reset hook.
func resetRecord[P any](r *record[P]) {
	var zero P
	r.v = zero
	r.key = nil
	r.release = nil
	r.subj = ""
	r.seq = 0
	r.expireAt = 0
	r.slot = 0
	r.size = 0
	r.part = 0
	r.mode = CopyDefault
	r.prio = 0
	r.released = false
	r.refs.Store(0)
}

// inflight is the per-lane list of leased envelopes, kept sorted by lease
// deadline so that expiry is an O(1) head check (the memory engine). It is
// guarded by the lane mutex.
type inflight[P any] struct {
	head *Envelope[P]
	tail *Envelope[P]
	n    int
}

// push inserts e in deadline order, scanning back from the tail: deliveries
// are normally issued in deadline order, so the scan stops immediately.
func (l *inflight[P]) push(e *Envelope[P]) {
	l.n++
	e.inList = true
	at := l.tail
	for at != nil && at.deadline > e.deadline {
		at = at.prev
	}
	if at == nil {
		e.prev, e.next = nil, l.head
		if l.head != nil {
			l.head.prev = e
		}
		l.head = e
		if l.tail == nil {
			l.tail = e
		}
		return
	}
	e.prev, e.next = at, at.next
	if at.next != nil {
		at.next.prev = e
	} else {
		l.tail = e
	}
	at.next = e
}

// remove unlinks e. It is a no-op when e is not linked.
func (l *inflight[P]) remove(e *Envelope[P]) {
	if !e.inList {
		return
	}
	e.inList = false
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		l.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		l.tail = e.prev
	}
	e.next, e.prev = nil, nil
	l.n--
}

// Len returns the number of leased envelopes.
func (l *inflight[P]) Len() int { return l.n }
