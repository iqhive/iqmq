package iqmq

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/lifecycle"
	"github.com/iqhive/iqmq/internal/settlement"
)

// Delivery is one received message and the right to settle it.
//
// It embeds its Message by value, which is the winner (ADR-005,
// DelivValue): a received message is not a separate allocation, a batch of
// deliveries lives in one backing array, and d.Body reaches the payload with
// one indirection fewer than a pointer shape. The price is that the right to
// settle travels with the value — any copy of a Delivery can settle it and no
// copy can tell that it is one — which is why the specification's "exactly one settlement
// wins" is enforced below the API rather than by the caller's discipline.
//
// A Delivery is valid until it is settled. In the default safe mode nothing
// is recycled, so a settled Delivery keeps answering (Settled reports true,
// a second settlement reports ErrAlreadySettled); under
// ConsumerConfig.BufferReuse its storage returns to the consumer after
// settlement and it must not be retained.
//
// A zero Delivery, and one a caller built itself, carries no settlement
// right: Ack, Nack and Reject report ErrNotSettleable and Settled reports
// false.
type Delivery struct {
	// Message is the received message, embedded by value.
	Message
	// Attempt is the delivery attempt, 1 on the first delivery. Zero means
	// the broker does not count attempts.
	//
	// It is a convenience copy of Info().Attempt, which is the authoritative
	// value: both come from the one DeliveryInfo the driver handed over, in
	// the one assignment that builds the Delivery, so they cannot disagree
	// for a delivery the core produced — this one is here so that the common
	// question costs no accessor call and no int conversion. A Delivery a
	// caller built itself carries no metadata at all: Info() is the zero
	// value and this field is whatever the caller wrote.
	Attempt int
	// handle is the settlement object (ADR-005, Object): a small
	// per-delivery record the consumer owns, reached through an interface so
	// that one settlement costs one indirect call.
	handle settlementHandle
	// gen is the generation of the delivery handle was claimed for. Under
	// BufferReuse the record behind handle carries the next delivery once
	// this one is settled, and gen is what keeps this Delivery from
	// settling, reporting on or extending that one.
	gen uint32
}

// Ack settles the delivery positively: the application completed processing
// and the driver takes the strongest configured native settlement action
//
// Exactly one settlement of a delivery wins. A second reports
// ErrAlreadySettled rather than being ignored, because a program that settles
// twice has a bug and the specification says so; a settlement racing another reports
// ErrSettlementInProgress; one whose delivery is gone reports ErrLeaseLost;
// one after the consumer closed reports ErrClosed; a delivery from a backend
// with no settlement semantics reports ErrNotSettleable; and an ambiguous
// native outcome reports ErrSettlementUnknown and stays unknown for ever —
// nothing later turns it into success.
func (d *Delivery) Ack() error {
	if d.handle == nil {
		return ErrNotSettleable
	}
	return d.handle.settle(d.gen, settleAck, NackOptions{})
}

// Nack settles the delivery negatively. The default action requeues
// the message for redelivery; the options change that.
//
// A driver that cannot honour the action asked for — a delayed requeue or a
// dead-letter route it has neither natively nor by an enabled emulation —
// reports ErrUnsupported and settles nothing, so the delivery is still
// settleable. Nack answers the same failures as Ack.
func (d *Delivery) Nack(opts ...NackOption) error {
	if d.handle == nil {
		return ErrNotSettleable
	}
	var o NackOptions
	if err := o.Apply(opts...); err != nil {
		return err
	}
	return d.handle.settle(d.gen, settleNack, o)
}

// Reject settles the delivery negatively and terminally: this subscription
// will not process the message, and does not ask for it to be redelivered.
//
// It is Nack(NackDeadLetter()). The specification names the verb but not its action, and
// dead-lettering is the reading that cannot lose a message: a backend with no
// dead-letter destination and no enabled emulation reports ErrUnsupported and
// settles nothing, where a drop would have destroyed the message.
// A caller who means "destroy this" writes
// Nack(NackDrop()), which the specification requires to be explicit.
func (d *Delivery) Reject() error {
	if d.handle == nil {
		return ErrNotSettleable
	}
	return d.handle.settle(d.gen, settleNack, NackOptions{Action: NackActionDeadLetter})
}

// Settled reports whether this delivery reached a successful terminal
// settlement.
//
// It is false for a delivery whose settlement was refused, for one whose
// outcome is unknown — an unknown settlement is not a settlement — and for a
// delivery that carries no settlement right at all.
func (d *Delivery) Settled() bool {
	if d.handle == nil {
		return false
	}
	return d.handle.settled(d.gen)
}

// Info is the delivery's metadata: the resolved topic, the
// subscription it came from, the attempt, whether the broker calls it a
// redelivery, when it was received, when its lease expires, and the
// partition, offset or sequence the broker gave it — each of the optional
// three paired with a Has flag, because a partition of zero is a real
// partition.
//
// It returns a value and not an error, so it answers for every state a
// delivery can be in. A zero Delivery, and one a caller built itself, carry
// no delivery: the answer is the zero DeliveryInfo. For a delivery the core
// handed over it is what the driver handed over with it, and it stays that
// after the delivery is settled, lost or closed — a settled delivery still
// names its own topic — because the metadata is written once, before the
// delivery reaches the caller, and never again. That is
// also what makes Info safe to call from any goroutine at any time, which a
// value that a concurrent settlement rewrote could not be.
//
// Under ConsumerConfig.BufferReuse the record returns to the consumer when
// the settlement returns and the next fetch re-fills it, so the metadata is
// borrowed exactly as Body and Headers are: read it before the
// settlement, or keep the part you need. CloneMessage is the escape for the
// message itself.
//
// Info allocates nothing.
func (d *Delivery) Info() DeliveryInfo {
	if d.handle == nil {
		return DeliveryInfo{}
	}
	return d.handle.info()
}

// Native is the driver's own delivery object, for the caller who needs the
// escape hatch: a *kgo.Record, a types.Message, a *nats.Msg.
//
// It is nil for a delivery that carries none, which includes a zero or
// hand-built Delivery and a driver that surfaces nothing. What it returns is
// the driver's, not iqmq's: settling through it, or holding it past the
// delivery's settlement, is outside every contract the core states. Its
// lifetime and its concurrency rules are Info's.
//
// Native allocates nothing: the driver's value is already an interface in
// the delivery the driver handed over.
func (d *Delivery) Native() any {
	if d.handle == nil {
		return nil
	}
	return d.handle.native()
}

// NativeAs is Native with the type assertion every use of the escape hatch
// ends in:
//
//	rec, ok := iqmq.NativeAs[*kgo.Record](&d)
//
// It reports false, and T's zero value, when the delivery has no native
// object or has one of another type — a driver's object is documented on its
// page and may change with the driver, so the two-result form is the only
// one a caller can write safely. A type assertion is not an allocation.
//
// It is a function and not a method because Go has no type parameters on
// methods.
func NativeAs[T any](d *Delivery) (T, bool) {
	v, ok := d.Native().(T)
	return v, ok
}

// Extend moves the delivery's lease, visibility timeout or redelivery
// deadline at least by further into the future, so that a handler
// which needs longer than the deadline keeps the delivery instead of having
// it redelivered underneath it.
//
// It is not a settlement. It takes no generation from the state machine,
// so it neither wins nor loses the "exactly one settlement wins" race, and a
// successful extension leaves the delivery exactly as settleable as it was.
// It does take the same lifecycle ticket a settlement takes, because it
// reaches the driver: a consumer that is closing bars the class and waits for
// every admitted operation to leave before the source is closed, so an
// extension can never arrive at a driver whose Source has already been
// closed.
//
// The failures are the specification, in the order the settlements answer them in: a
// delivery that was settled reports ErrAlreadySettled, one whose lease is
// gone ErrLeaseLost, one whose consumer closed ErrClosed, a zero or
// hand-built Delivery and a source with no settler ErrNotSettleable, and a
// driver whose settler is not a DeadlineExtender reports ErrUnsupported —
// which is the answer for every broker with no lease to extend, and is
// decided once when the source is opened rather than per call. A by that is
// not positive is CodeInvalidArgument: "extend by nothing" is a mistake, not
// a request.
//
// A failed extension changes nothing else about the delivery. In particular
// it does not take one whose lease the broker has withdrawn out of
// circulation: the settlement that follows is what discovers that, through
// the driver, and releases the delivery with the accounting intact.
//
// The specification forbids a timer per delivery, so a handler's automatic extension is
// the consumer's shared lease scheduler; this is the call a caller driving
// Receive itself makes.
func (d *Delivery) Extend(by time.Duration) error {
	if d.handle == nil {
		return ErrNotSettleable
	}
	return d.handle.extend(d.gen, by)
}

// CloneMessage returns an independent deep copy of the delivery's message
// a *Message whose Key, Body and header storage are its own, and
// which therefore outlives the delivery.
//
// It is the escape from borrowed storage and the one call whose result is
// safe to keep past the settlement under ConsumerConfig.BufferReuse, where
// everything the Delivery itself points at may be recycled the moment the
// settlement returns. Call it before settling; keep the copy for as long as
// you like.
//
// It works on any Delivery, including one a caller built itself, because it
// copies the embedded Message rather than anything the consumer owns. It is
// the one accessor here that allocates: a deep copy is what was asked for.
func (d *Delivery) CloneMessage() *Message {
	// The embedded Message promotes its own Clone, so d.Clone() is this
	// same call; CloneMessage is the name give it, and the
	// one a caller looking for the escape from borrowed storage will search
	// for.
	return d.Clone()
}

// settleOp is a settlement verb. One verb parameter rather than a method per
// verb keeps a settlement to exactly one indirect call, which is what the specification measured.
type settleOp uint8

const (
	// settleAck is the terminal positive settlement.
	settleAck settleOp = iota
	// settleNack is the negative settlement, whose action is in NackOptions.
	settleNack
)

// settlementHandle is the unexported settlement object. A Delivery
// carries one; a caller cannot construct one, which is what makes a
// hand-built Delivery ErrNotSettleable rather than a way into the state
// machine.
type settlementHandle interface {
	// settle applies one verb to the delivery of generation gen,
	// enforcing the rules whatever the driver does.
	settle(gen uint32, op settleOp, o NackOptions) error
	// settled reports a successful terminal settlement of generation gen.
	settled(gen uint32) bool
	// info is the delivery's metadata, as the driver handed it over.
	info() DeliveryInfo
	// native is the driver's own delivery object, or nil.
	native() any
	// extend moves the lease of generation gen, without settling it.
	extend(gen uint32, by time.Duration) error
	// active reports that generation gen is still leased and that no
	// settlement of it is in flight or has resolved: what a definite
	// refusal leaves behind.
	active(gen uint32) bool
	// abandon gives up the lease of generation gen without telling the
	// driver, when the delivery is active and nothing settles it.
	abandon(gen uint32)
}

// The release reasons a settlement record can carry once it is released.
// They are what a handle kept past its delivery is answered with, so the
// rule the specification states for each case survives the record leaving circulation.
const (
	// releaseLive means the record is not released: it names a delivery.
	releaseLive uint32 = iota
	// releaseSettled means the delivery was settled successfully.
	releaseSettled
	// releaseClosed means the consumer closed while the delivery was live.
	releaseClosed
	// releaseLost means the delivery's lease is gone: it expired, it was
	// revoked, or the driver reported the generation stale.
	releaseLost
)

// packSlot packs a delivery generation and a release reason into one word.
// A live record carries a non-zero generation and releaseLive; a released
// one carries generation 0 — which settlement.Cell never leases — and the
// reason it was released for.
func packSlot(gen, reason uint32) uint64 {
	return uint64(gen)<<32 | uint64(reason)
}

// unpackSlot is packSlot's inverse.
func unpackSlot(w uint64) (gen, reason uint32) {
	return uint32(w >> 32), uint32(w)
}

// slotOf is the record's state as the delivery of generation gen sees it:
// releaseLive while the record still carries gen, and otherwise the reason
// that delivery was released for. Under BufferReuse a released record is
// claimed again under a later generation, and the slot then describes a
// delivery that is not the caller's.
func (r *settlementRecord) slotOf(gen uint32) uint32 {
	g, _ := unpackSlot(r.slot.Load())
	if g == gen {
		return releaseLive
	}
	// release writes last before slot, so a slot that has moved on from
	// gen is never read ahead of the release that moved it.
	lg, reason := unpackSlot(r.last.Load())
	if lg == gen {
		return reason
	}
	// The record has been released again since, and this delivery's own
	// reason with it; all that is certain is that the delivery is gone.
	return releaseLost
}

// releaseError is the failure a settlement of a released record reports.
func releaseError(reason uint32) error {
	switch reason {
	case releaseSettled:
		return ErrAlreadySettled
	case releaseClosed:
		return ErrClosed
	default:
		return ErrLeaseLost
	}
}

// settlementRecord is the settlement object: one small record per
// delivery, holding what a settlement needs and nothing else.
//
// It is the prototype's object shape (the specification's `object`, removed at the M6 disposition review, ADR-025) with the
// prototype's broker replaced by the driver's Settler and the prototype's
// live flag replaced by slot, because the root has to answer the rules
// itself: a driver may not have a settlement state machine, and the API's
// rules hold whatever the driver does.
//
// slot and cell are atomic, so a handle a caller kept past its delivery is
// answered rather than racing the fetch that recycled it. Every other field
// is written before the record is handed out and read afterwards, which the
// hand-off of the Delivery itself orders — and dd is the field that has to be
// read the way it is written, because the claim that "every other field" is
// written once was false for it: release used to clear it and a recycling
// fetch re-fills it. Both readers it has (the event a settlement reports and
// the accessors a caller calls) obey the rules on dd below.
type settlementRecord struct {
	// slot packs the generation and the release reason (packSlot).
	slot atomic.Uint64
	// last packs the generation and reason of the record's latest release,
	// which is what answers a Delivery whose record has been claimed again
	// (slotOf).
	last atomic.Uint64
	// cell is the settlement state machine for this delivery. It is never
	// reset, only re-leased, so a recycled record cannot forget what
	// happened to the delivery before it.
	cell settlement.Cell
	// owner is the consumer the delivery came from. It is set once, when the
	// record is allocated, and never changes.
	owner *ConsumerHandle
	// st is the source's settler, also set once. A nil settler is the specification's
	// AckNone model.
	st Settler
	// dd is the driver's delivery, copied out of the fetch buffer so that
	// the driver may reuse the buffer and still be handed its own record
	// when the delivery is settled. It is also what Delivery.Info,
	// Native and Extend answer from.
	//
	// It is written by claim, once per delivery, before the Delivery that
	// reaches it exists — and by nothing else, which is what makes those
	// accessors safe from any goroutine and why release no longer clears
	// it (consumer.go's release). Under BufferReuse a
	// later claim re-fills it, so a caller that retains a Delivery past its
	// settlement reads a delivery that is not its own: the rule for
	// Body and Headers, applied to the metadata.
	//
	// A settlement's own reads are narrower still: the event fields are
	// captured inside apply, after the cell has admitted the settlement,
	// because at that point nothing may recycle the record.
	dd DriverDelivery
	// next links the record into the consumer's free list under
	// BufferReuse.
	next *settlementRecord
	// pins counts the extensions reading dd, with pinParked set when the
	// record was released while one was. An extension takes no generation
	// from the cell, so nothing else orders it against a settlement that
	// releases the record and a fetch that re-fills dd under BufferReuse.
	pins atomic.Uint32
}

// pinParked marks a released record that an extension still holds: the last
// extension to leave returns it to the free list (unpin).
const pinParked = 1 << 31

// pin keeps the record out of the free list until unpin.
func (r *settlementRecord) pin() { r.pins.Add(1) }

// unpin drops an extension's pin and recycles the record when a release
// parked it meanwhile. The swap is what decides which of two leaving
// extensions recycles it: an extension that pinned after the count reached
// zero makes the swap fail, and its own unpin recycles the record instead.
func (r *settlementRecord) unpin() {
	if r.pins.Add(^uint32(0)) == pinParked && r.pins.CompareAndSwap(pinParked, 0) {
		r.owner.free.push(r)
	}
}

// recyclable reports that no extension holds the record, and parks it for
// the last one to recycle otherwise. release stores the slot first, so an
// extension that pins after this check sees the delivery gone and never
// reads dd.
func (r *settlementRecord) recyclable() bool {
	for {
		old := r.pins.Load()
		if old == 0 {
			return true
		}
		if r.pins.CompareAndSwap(old, old|pinParked) {
			return false
		}
	}
}

// settleFields is the part of a SettleEvent that comes from the driver's
// delivery, captured where the delivery is known to be this goroutine's.
//
// It exists because the two requirements pull opposite ways. A successful
// settlement releases the record, which under BufferReuse returns it to the
// consumer, so the fields cannot be read after the verb is applied; and the
// record is shared, so they cannot be read before the settlement is admitted
// either — a settler that lost the race would be reading a record the winner
// has already released and a fetch may already have re-filled, which is the
// data race a hook and two settlers used to produce.
// What is left is the moment in between: inside apply, after the cell's Begin
// has made this goroutine the one settlement of this generation, when nothing
// may release the record and nothing may recycle it.
type settleFields struct {
	topic        string
	subscription string
	attempt      uint32
	// set reports that the fields were captured, which only a settlement
	// the cell admitted is. A settlement refused before that never saw the
	// delivery's own metadata.
	set bool
}

// capture reads the event's fields out of the record. The caller must hold
// the delivery's generation: see settleFields.
func (f *settleFields) capture(dd *DriverDelivery) {
	f.topic, f.subscription, f.attempt = dd.Info.Topic, dd.Info.Subscription, dd.Info.Attempt
	f.set = true
}

// settle applies one verb to this delivery, counts the outcome and reports
// it to the settlement hook.
//
// The event is built here and filled in by apply, which is the only place
// the delivery's own metadata may be read from (settleFields). A settlement
// that never reached the delivery — a released record, a source that cannot
// settle, a closed consumer, a lost race — reports the consumer's own topic
// and subscription instead, as ReceiveEvent does, so that no event
// turns into a metric carries an empty label.
//
// Nothing is read from the clock, and no event exists at all, when no hook
// is watching (the disabled-observer budget).
func (r *settlementRecord) settle(gen uint32, op settleOp, o NackOptions) error {
	// the settlement takes no context -- Delivery.Ack() has none to give
	// -- so a manual settlement observes with Background. A settlement made
	// by the RUNNER passes the delivery's own context through settleCtx, and
	// that is the one an integration needs: it is what makes the settle
	// span a child of the handler's rather than a root.
	return r.settleCtx(context.Background(), gen, op, o)
}

// settleCtx is [settlementRecord.settle] with the context of whoever settled.
func (r *settlementRecord) settleCtx(ctx context.Context, gen uint32, op settleOp, o NackOptions) error {
	c := r.owner
	if c.hooks.OnSettle == nil {
		err := r.apply(gen, op, o, nil)
		if err != nil {
			c.counters.settleFailures.Add(1)
		}
		return err
	}
	e := SettleEvent{
		Topic:        c.cfg.Topic,
		Subscription: c.cfg.Subscription,
		Kind:         SettleAck,
	}
	if op == settleNack {
		e.Kind, e.Action = SettleNack, o.Action
	}
	var f settleFields
	start := c.clock.Now()
	err := r.apply(gen, op, o, &f)
	if err != nil {
		c.counters.settleFailures.Add(1)
	}
	e.Duration = c.clock.Now().Sub(start)
	e.Err = err
	f.fill(&e)
	c.hooks.OnSettle(ctx, e)
	return err
}

// fill copies a captured delivery's metadata into the event, leaving the
// consumer's own values where nothing was captured.
func (f *settleFields) fill(e *SettleEvent) {
	if !f.set {
		return
	}
	e.Topic, e.Subscription, e.Attempt = f.topic, f.subscription, int(f.attempt)
}

// apply is settle's state machine.
//
// The order of the checks is the order state the rules in: a
// released record answers for the reason it was released; a source with no
// settler is not settleable; a closed consumer refuses everything; and only
// then does the delivery's own state machine decide, which is what makes
// exactly one settlement win.
func (r *settlementRecord) apply(gen uint32, op settleOp, o NackOptions, f *settleFields) error {
	if reason := r.slotOf(gen); reason != releaseLive {
		return releaseError(reason)
	}
	if r.st == nil {
		return ErrNotSettleable
	}
	c := r.owner
	// The ticket is what keeps a settlement from reaching a driver whose
	// Source has already been closed: the Committing phase bars this class
	// and waits for every admitted settlement to leave before the Closed
	// phase closes the source. Checking a flag instead would leave the
	// window the playbook's recurring defect class lives in.
	t := c.life.Enter(lifecycle.OpSettle)
	if !t.OK() {
		return ErrClosed
	}
	defer c.life.Leave(t)
	g := settlement.Gen(gen)
	if err := r.cell.Begin(g); err != nil {
		return err
	}
	// Past Begin this goroutine owns the delivery: nothing else may resolve
	// its cell, release the record or recycle it, so this is where the
	// event's fields can be read (settleFields).
	if f != nil {
		f.capture(&r.dd)
	}
	var err error
	switch op {
	case settleNack:
		err = r.st.Nack(c.settleCtx, &r.dd, o)
	case settleAck:
		err = r.st.Ack(c.settleCtx, &r.dd)
	default:
		err = errNoSettleOp
	}
	return r.resolve(g, op, o, err)
}

// resolve records the native outcome in the cell and releases the record when
// the delivery can never be settled again.
//
// Three outcomes, and the difference between them is the whole of the specification's
// state machine: a success commits and releases; an ambiguous result leaves
// the delivery Unknown for ever, which is why it is not released — nothing
// may re-lease that cell, and nothing may report the delivery settled; a
// definite refusal returns the delivery to its caller, still settleable,
// unless the driver said the lease itself is gone.
func (r *settlementRecord) resolve(g settlement.Gen, op settleOp, o NackOptions, err error) error {
	c := r.owner
	if err == nil {
		r.cell.Commit(g)
		c.recordSettled(op, o)
		c.release(r, uint32(g), releaseSettled)
		return nil
	}
	if isAmbiguousSettlement(err) {
		r.cell.Ambiguous(g)
		return err
	}
	r.cell.Reject(g)
	switch CodeOf(err) {
	case CodeLeaseLost:
		// The driver holds the generation the broker knows about; when it
		// says the lease is stale, no later attempt can succeed either.
		r.cell.Revoke(g)
		c.release(r, uint32(g), releaseLost)
	case CodeClosed:
		r.cell.Close(g)
		c.release(r, uint32(g), releaseClosed)
	default:
		// A definite refusal the driver may still accept on a retry. The
		// cell is back in Active and the delivery is still in flight.
	}
	return err
}

// settled reports a successful terminal settlement of generation gen, for a
// live record and for one that has been released.
func (r *settlementRecord) settled(gen uint32) bool {
	if reason := r.slotOf(gen); reason != releaseLive {
		return reason == releaseSettled
	}
	st, g := r.cell.Load()
	if g != settlement.Gen(gen) {
		// Released and claimed again between the two loads.
		return r.slotOf(gen) == releaseSettled
	}
	return st == settlement.Settled
}

// info is the metadata the driver handed the delivery over with.
//
// It is read without a ticket and without the cell, which is safe because
// nothing writes r.dd after the delivery is handed out: see the field's own
// comment and. A released record still answers, which is
// what makes a settled delivery still name its topic.
func (r *settlementRecord) info() DeliveryInfo { return r.dd.Info }

// native is the driver's own delivery object. It is read under info's rules.
func (r *settlementRecord) native() any { return r.dd.Native }

// active reports that generation gen is leased and settleable.
func (r *settlementRecord) active(gen uint32) bool {
	st, g := r.cell.Load()
	return g == settlement.Gen(gen) && st == settlement.Active && r.slotOf(gen) == releaseLive
}

// abandon releases the record as lost when generation gen is still active.
// The broker keeps the message and redelivers it when its lease or
// connection ends, which is what a lost lease means; Revoke is what makes
// this lose to a settlement that began first.
func (r *settlementRecord) abandon(gen uint32) {
	if r.cell.Revoke(settlement.Gen(gen)) {
		r.owner.release(r, gen, releaseLost)
	}
}

// extend moves this delivery's lease and reports the outcome to the
// settlement hook, under the same rules settle reports one: the event exists
// only when a hook is watching, and its kind is SettleExtend — the third
// kind the iqmq_settle_duration_seconds{kind} names and the one nothing
// used to fire.
//
// It counts the specification's own metric for lease extension,
// iqmq_lease_extensions_total{outcome} — ConsumerStats.LeaseExtensions and
// LeaseExtensionFailures — and never ConsumerStats.SettleFailures, which
// counts settlements; an extension is not one. This is
// the single writer of that counter, so the runner's shared scheduler
// (lease.go) and a caller's own Delivery.Extend are counted alike.
func (r *settlementRecord) extend(gen uint32, by time.Duration) error {
	// An extension asked for by a caller has no context either; the
	// scheduler's own extensions pass theirs through extendCtx.
	return r.extendCtx(context.Background(), gen, by)
}

// extendCtx is [settlementRecord.extend] with the context of whoever asked.
func (r *settlementRecord) extendCtx(ctx context.Context, gen uint32, by time.Duration) error {
	c := r.owner
	if c.hooks.OnSettle == nil {
		err := r.applyExtend(gen, by, nil)
		c.counters.recordExtension(err)
		return err
	}
	e := SettleEvent{
		Topic:        c.cfg.Topic,
		Subscription: c.cfg.Subscription,
		Kind:         SettleExtend,
	}
	var f settleFields
	start := c.clock.Now()
	err := r.applyExtend(gen, by, &f)
	c.counters.recordExtension(err)
	e.Duration = c.clock.Now().Sub(start)
	e.Err = err
	f.fill(&e)
	c.hooks.OnSettle(ctx, e)
	return err
}

// applyExtend is the extension's state machine.
//
// The order of the checks is apply, because answers an extension with
// the same rules it answers a settlement with: a released record answers for
// the reason it was released, a source with no settler is not settleable, a
// closing consumer refuses everything — and only then does it matter whether
// the driver can extend a lease at all. The cell is not touched: an
// extension takes no generation, so a delivery that is settling, or whose
// outcome is unknown, can still have its lease extended, and a successful
// extension leaves the delivery settleable.
func (r *settlementRecord) applyExtend(gen uint32, by time.Duration, f *settleFields) error {
	if by <= 0 {
		return errExtendRange
	}
	// Pinned before the slot is read, so that a settlement releasing the
	// record from here on cannot hand it to a fetch while the driver reads
	// dd (recyclable).
	r.pin()
	defer r.unpin()
	if reason := r.slotOf(gen); reason != releaseLive {
		return releaseError(reason)
	}
	if r.st == nil {
		return ErrNotSettleable
	}
	c := r.owner
	t := c.life.Enter(lifecycle.OpSettle)
	if !t.OK() {
		return ErrClosed
	}
	defer c.life.Leave(t)
	if c.extender == nil {
		return ErrUnsupported
	}
	if f != nil {
		f.capture(&r.dd)
	}
	return c.extender.Extend(c.settleCtx, &r.dd, by)
}

// isAmbiguousSettlement reports whether a native settlement result means
// "the broker may or may not have taken it", which the specification keeps explicitly
// unknown rather than resolving either way.
func isAmbiguousSettlement(err error) bool {
	switch CodeOf(err) {
	case CodeSettlementUnknown, CodeAmbiguous:
		return true
	default:
		return IsAmbiguous(err)
	}
}

// NackAction is what a negative settlement asks the broker to do with the
// message.
type NackAction uint8

// The nack actions. They carry the NackAction prefix because the specification
// gives the unprefixed names to the value tokens below — NackDrop is a
// function, so the action it sets cannot also be called NackDrop
// (and the same collision resolved for the error codes).
const (
	// NackActionRequeue is the default: the message becomes deliverable
	// again, immediately or after NackOptions.Delay.
	NackActionRequeue NackAction = iota
	// NackActionDrop destroys the message. It is destructive and therefore
	// never a default.
	NackActionDrop
	// NackActionDeadLetter routes the message to the dead-letter
	// destination. A backend with none, and no enabled emulation, reports
	// ErrUnsupported.
	NackActionDeadLetter

	nackActionCount
)

var nackActionNames = [nackActionCount]string{"requeue", "drop", "deadletter"}

// String returns the action's lower-case name.
func (a NackAction) String() string {
	if a < nackActionCount {
		return nackActionNames[a]
	}
	return "NackAction(?)"
}

// Valid reports whether a names an action.
func (a NackAction) Valid() bool { return a < nackActionCount }

// NackOptions is a negative settlement's parameters, as a struct so that a
// batched settlement can carry one set for a whole batch.
type NackOptions struct {
	// Action is what the broker should do with the message. The zero value
	// is NackActionRequeue.
	Action NackAction
	// Delay holds a requeued message back for this long before it becomes
	// deliverable again. A driver without native delayed redelivery holds
	// the message in process, unacked at the broker, under the default
	// emulation policy.
	Delay time.Duration
	// Reason is local diagnostics unless the driver documents that it
	// transports it. It is never a payload: the specification forbids payloads in
	// anything iqmq logs or reports.
	Reason string
}

// nackField names the parameter a NackOption sets. The zero value names
// none, which is what makes a stray NackOption{} an error rather than a
// silent requeue.
type nackField uint8

const (
	nackFieldNone nackField = iota
	nackFieldAction
	nackFieldDelay
	nackFieldReason

	nackFieldCount
)

// NackOption is one negative-settlement parameter.
//
// It is a value token and not a closure, for the reason the specification decided for
// PublishOption (ADR-006, Value): d.Nack(NackDelay(time.Second)) allocates
// nothing at the call site.
//
// The zero NackOption names no parameter and is reported as invalid rather
// than applied.
type NackOption struct {
	field nackField
	num   int64
	str   string
}

// NackDelay holds the requeued message back for d before it becomes
// deliverable again. A negative delay is invalid.
func NackDelay(d time.Duration) NackOption {
	return NackOption{field: nackFieldDelay, num: int64(d)}
}

// NackRequeue asks for the message to be redelivered, which is the default.
// It is here so that a call site can say so.
func NackRequeue() NackOption {
	return NackOption{field: nackFieldAction, num: int64(NackActionRequeue)}
}

// NackDrop asks for the message to be destroyed. It is destructive
// and has to be written.
func NackDrop() NackOption {
	return NackOption{field: nackFieldAction, num: int64(NackActionDrop)}
}

// NackDeadLetter asks for the message to be routed to the dead-letter
// destination.
func NackDeadLetter() NackOption {
	return NackOption{field: nackFieldAction, num: int64(NackActionDeadLetter)}
}

// NackReason records why the delivery was refused, for local diagnostics.
func NackReason(s string) NackOption {
	return NackOption{field: nackFieldReason, str: s}
}

// Apply folds the tokens into o, left to right, and validates the result. A
// later token for the same parameter replaces an earlier one.
//
// It reports CodeInvalidArgument for a zero NackOption, for an action outside
// the enumeration and for a negative delay, and it leaves o unchanged when it
// reports an error, so a rejected option set is never half applied.
//
// Apply allocates nothing, and neither does a call site that hands it tokens
// built in the call.
func (o *NackOptions) Apply(opts ...NackOption) error {
	next := *o
	for _, opt := range opts {
		switch opt.field {
		case nackFieldAction:
			a := NackAction(opt.num)
			if !a.Valid() {
				return errNackOptionRange
			}
			next.Action = a
		case nackFieldDelay:
			if opt.num < 0 {
				return errNackOptionRange
			}
			next.Delay = time.Duration(opt.num)
		case nackFieldReason:
			next.Reason = opt.str
		case nackFieldNone, nackFieldCount:
			return errNoNackOption
		default:
			return errNoNackOption
		}
	}
	*o = next
	return nil
}

// The settlement failures with no sentinel of their own. The list names
// no sentinel for a malformed option or an unknown verb, and errors.Is
// matches by code, so a caller compares CodeOf(err) against
// CodeInvalidArgument (the rule, applied to the settlement surface).
var (
	errNoNackOption    = errcode.New(errcode.InvalidArgument, "iqmq: nack option names no parameter")
	errNackOptionRange = errcode.New(errcode.InvalidArgument, "iqmq: nack option value out of range")
	errNoSettleOp      = errcode.New(errcode.InvalidArgument, "iqmq: unknown settlement verb")
	errExtendRange     = errcode.New(errcode.InvalidArgument, "iqmq: lease extension is not a positive duration")
)
