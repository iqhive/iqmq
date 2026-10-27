package iqmq

import "github.com/iqhive/iqmq/internal/settlement"

// Acked reports whether this delivery reached a successful terminal
// settlement that was an Ack.
//
// It is Settled narrowed to the positive verb: false for a delivery that was
// nacked, rejected or dead-lettered, however successfully, and false wherever
// Settled is false — for a settlement that was refused, for one whose outcome
// is unknown, and for a delivery that carries no settlement right at all. It
// is the question a middleware that records a delivery as done has to ask
// once the handler returns: a handler that settled its own delivery with a
// Nack or a Reject and returned nil has asked for the message again, and
// Settled cannot tell that from an Ack.
//
// Under ConsumerConfig.BufferReuse the answer is this delivery's for as long
// as the runner holds it — through the handler, its middleware and the
// runner's own settlement after them — and, for a caller of Receive, until
// the consumer's next fetch, which may give the record to another delivery.
// After that it may report false for a delivery that was acked; it never
// reports the answer of the delivery the record carries next.
//
// Acked allocates nothing.
func (d *Delivery) Acked() bool {
	if d.handle == nil {
		return false
	}
	return d.handle.acked(d.gen)
}

// acked is settled narrowed to an Ack, for generation gen.
//
// The verb is read after settled has seen the commit or the release, and
// noteVerb wrote it before either, so the word read here is the winning
// settlement's or a later generation's — never an earlier attempt's — and the
// generation it carries is what keeps a later delivery's verb from answering
// for this one.
func (r *settlementRecord) acked(gen uint32) bool {
	if !r.settled(gen) {
		return false
	}
	g, op := unpackSlot(r.verb.Load())
	return g == gen && settleOp(op) == settleAck
}

// noteVerb records the verb of the settlement of generation g that has just
// won the cell, packed as packSlot packs a slot.
//
// It is written by the one goroutine Begin admitted, before the native call
// and so before the commit and the release. A settlement that is refused and
// tried again overwrites it with the retry's verb, so the last verb written
// for a generation is the one that succeeded, if any did. The release cannot
// carry it instead: releaseSettled is every successful verb's reason, and
// what a released record answers with (releaseError) must not change with the
// verb.
func (r *settlementRecord) noteVerb(g settlement.Gen, op settleOp) {
	r.verb.Store(packSlot(uint32(g), uint32(op)))
}

// hold keeps the record behind d out of the free list while the runner holds
// d, so that Settled and Acked still answer for d when the middleware and the
// runner read them after the handler.
//
// Under ConsumerConfig.BufferReuse a release returns the record to the free
// list at once, and the fetch loop runs beside the handlers: a delivery the
// handler settled itself could be re-filled by the next fetch, settled and
// released again before the handler had even returned, and the record then
// answered for that delivery's release and not for d's. The runner overruled
// a delivery its handler had acked — a poison halt, a dead-letter copy of an
// acked message, a settlement reported failed — and a middleware committing
// a key on Acked could not tell a nacked delivery from one it must record.
// Pinned, a release parks the record (recyclable) and unhold returns it,
// before the runner recycles the admission slot, so the fetch that slot
// admits next still finds it in the free list and a steady-state run
// allocates nothing.
func (r *runner) hold(d *Delivery) {
	if r.c.reuse && d.handle != nil {
		d.handle.pin()
	}
}

// unhold is hold's other half. It must run before the slot holding d is
// recycled, which clears d.
func (r *runner) unhold(d *Delivery) {
	if r.c.reuse && d.handle != nil {
		d.handle.unpin()
	}
}
