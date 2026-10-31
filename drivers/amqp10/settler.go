package amqp10

import (
	"context"
	"errors"
	"maps"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/amqp10/internal/client"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The compile-time proof of the settler's surfaces: the mandatory two and
// nothing else.
//
// There is no [github.com/iqhive/iqmq.BatchSettler] and no
// [github.com/iqhive/iqmq.DeadlineExtender], and both absences are the
// protocol's or the library's rather than this driver's taste. A disposition
// frame can cover a RANGE of delivery ids, which is exactly what a batched
// settlement wants, and go-amqp's range form is unexported: only the
// per-message wrappers are public, so a batch settler here would be a loop
// wearing a name that promises a saving. And AMQP 1.0 grants no lease at all —
// an unsettled delivery is this receiver's for as long as the link lives — so
// there is no deadline to extend.
//
// Claiming either would be worse than not having it. The core asserts these
// once, when the source is opened, and records the answer in the capability
// table; a settler that satisfied BatchSettler and then looped would make
// FeatureBatchSettle advertise a round trip it does not save.
var _ mq.Settler = (*settler)(nil)

// settler settles one source's deliveries.
//
// Every verb goes through the token table and the generation guard FIRST, and
// that is the whole design: the answers — already settled, settlement in
// progress, lease lost, outcome unknown — come from the driver's own
// bookkeeping and not from the broker, because the broker cannot answer them.
//
// An AMQP 1.0 peer takes a disposition for any delivery id its link still has
// unsettled, and a delivery whose link is gone has already gone back to the
// node, where another consumer may hold it. So a disposition sent on a
// reconnected link either fails (the link is new and does not know the id) or,
// on a peer that reuses delivery ids across an attach, lands on a delivery this
// process never saw. The specification makes the second a disqualifier, and every driver
// that has landed in this repository needed its own version of the guard.
type settler struct{ src *Source }

// Ack settles a delivery positively: `accepted`, which tells the node the
// receiver processed the message and it may be forgotten.
//
// The boundary is [Config.ReceiverSettle]. Under the default `first` the
// delivery is settled the moment the disposition frame is written, so Ack
// returns without a round trip and a link that died immediately afterwards
// leaves the message redeliverable — which is at-least-once behaving as
// advertised. Under `second` the call returns only once the peer has confirmed
// the settlement, at the cost of a round trip per acknowledgement, and a failure
// then leaves the outcome UNKNOWN rather than lost.
func (s *settler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	h, err := s.begin(d)
	if err != nil {
		return err
	}
	return s.finish(ctx, d, h, mq.OpAck, client.Disposition{Outcome: client.OutcomeAccepted})
}

// Nack settles a delivery negatively under o.
//
// The three actions are three different AMQP 1.0 outcomes and none of them is
// the others:
//
//   - Requeue is `modified{delivery-failed=true}`: the node makes the message
//     available again and counts the attempt, so the redelivery arrives with a
//     higher delivery count. It is not `released`, which does not count it; see
//     [settler.requeue] for why. With a delay it
//     is `modified` carrying the profile's own annotation, because AMQP 1.0 has
//     no standard spelling for "hold this back" — a profile that has none
//     refuses a non-zero delay with ErrUnsupported and settles nothing, which is the rule and leaves the delivery settleable.
//   - Drop is `rejected`. AMQP 1.0 has no "destroy" outcome: `rejected` says the
//     message is unprocessable, and a broker with a dead-letter address routes
//     it there rather than destroying it. That is the safe direction and the
//     driver page says so — a caller that wrote Nack(NackDrop()) on a broker
//     with a dead-letter policy gets a message in the dead-letter queue, not a
//     message that is gone.
//   - DeadLetter is `rejected` too on a broker whose own policy routes it, and
//     a durable publication followed by `rejected` when [Profile.DeadLetterAddress]
//     supplies a route. See [Source.deadLetter] for why the order is the
//     contract.
//
// A delivery whose settlement this driver cannot perform is REFUSED before the
// token table is touched, so that the delivery is still settleable: the specification says a
// driver that cannot honour the action asked for "returns ErrUnsupported and
// settles nothing", and a driver that had already moved the slot to settling
// would leave it unsettleable for ever.
func (s *settler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	if !o.Action.Valid() {
		return errBadNackAction
	}
	if o.Action == mq.NackActionRequeue && o.Delay > 0 {
		p := s.src.b.profile
		if !p.NackDelay.carries() {
			return errNoNackDelay
		}
		if p.NackDelay.Max > 0 && o.Delay > p.NackDelay.Max {
			return errNackDelayTooLong
		}
	}
	route := s.src.b.profile.DeadLetterAddress
	if o.Action == mq.NackActionDeadLetter && route == nil && !s.src.b.caps.Has(mq.FeatureDeadLetter) {
		// The specification: a backend with no dead-letter destination reports
		// ErrUnsupported and settles NOTHING. The profile is the only thing
		// that knows whether its broker has a dead-letter policy, which is
		// what FeatureDeadLetter claims; answering before the table is touched
		// is what keeps the delivery settleable.
		return errNoDeadLetter
	}
	h, err := s.begin(d)
	if err != nil {
		return err
	}
	switch o.Action {
	case mq.NackActionRequeue:
		return s.finish(ctx, d, h, mq.OpNack, s.requeue(o))
	case mq.NackActionDrop:
		return s.finish(ctx, d, h, mq.OpNack, client.Disposition{
			Outcome: client.OutcomeRejected,
			Error:   rejectionError(dropCondition, o.Reason),
		})
	case mq.NackActionDeadLetter:
		if route != nil {
			return s.src.deadLetter(ctx, d, h, o)
		}
		return s.finish(ctx, d, h, mq.OpNack, client.Disposition{
			Outcome: client.OutcomeRejected,
			Error:   rejectionError(deadLetterCondition, o.Reason),
		})
	case nackActionCount:
	}
	// Unreachable: the action was validated above. Completing the slot rather
	// than leaving it settling is what keeps a future action from stranding a
	// delivery.
	_ = s.src.table.Complete(d.Token, mq.ErrUnsupported)
	return mq.ErrUnsupported
}

// nackActionCount is the sentinel the exhaustive switch above needs. The specification's
// enumeration has no exported count, so the switch names the three actions and
// this stands for "one the root package added since".
const nackActionCount = mq.NackActionDeadLetter + 1

// requeue is the disposition a negative settlement that asks for redelivery
// becomes.
//
// A plain requeue is `modified` with `delivery-failed` set, and the choice
// between that and `released` is the one decision in this file.
//
// `released` means "I did not act on this" and explicitly does NOT increment
// the delivery count. That is the right disposition for a delivery the DRIVER
// gives back — what a closing source does with what it never handed out, which
// is `Source.release` — and it is the wrong one for Nack: a caller that
// nacks has TRIED and failed, and Attempt has to rise or MaxAttempts is
// compared against a number that never moves and a poison message is retried
// for ever. `modified{delivery-failed=true}` is the protocol's own way to say
// "count this as a failed attempt", and it is what makes the redelivery arrive
// with a higher `delivery-count`.
//
// The first version used `released` here and the conformance kit caught it
// against a real broker: `nack/immediate-redelivery` reported "the redelivery
// reports attempt 1 after a first delivery at 1". The two dispositions are kept
// apart rather than merged, which is why the close path has a release of its
// own.
//
// A DELAYED requeue is `modified` with the profile's annotation as well, and
// `delivery-failed` there is the profile's decision: a broker whose delay is a
// redelivery policy counts the attempt, and one whose delay is a scheduler does
// not.
func (s *settler) requeue(o mq.NackOptions) client.Disposition {
	if o.Delay <= 0 {
		return client.Disposition{Outcome: client.OutcomeModified, DeliveryFailed: true}
	}
	rule := s.src.b.profile.NackDelay
	annos := amqp.Annotations{rule.Key: rule.encode(o.Delay)}
	return client.Disposition{
		Outcome:           client.OutcomeModified,
		DeliveryFailed:    rule.DeliveryFailed,
		UndeliverableHere: rule.UndeliverableHere,
		Annotations:       annos,
	}
}

// The conditions a rejection carries.
//
// A `rejected` outcome may carry an error, and a broker's dead-letter queue that
// records it is the one place the reason is worth the bytes. The specification makes
// NackOptions.Reason local diagnostics "unless the driver documents that it
// transports it": this driver documents that it transports it here, on the
// rejection only, because a dead-lettered message with no reason in it is a
// message somebody has to guess about.
const (
	// dropCondition says the receiver was told to destroy the message. It is
	// `amqp:not-allowed` rather than an invented symbol so that a broker's
	// own tooling recognises it.
	dropCondition = amqp.ErrCondNotAllowed
	// deadLetterCondition says the receiver refused the message terminally.
	deadLetterCondition = amqp.ErrCondNotAllowed
)

// rejectionError builds the rejection's condition and description.
//
// The description carries the caller's reason and NOTHING else: the specification forbids a
// payload, a credential or an address in anything iqmq reports, and a
// description that interpolated the message would put the payload on the wire
// and into the broker's log.
func rejectionError(cond amqp.ErrCond, reason string) *amqp.Error {
	e := &amqp.Error{Condition: cond, Description: "iqmq: the consumer refused the delivery"}
	if reason != "" {
		e.Description += ": " + reason
	}
	return e
}

// begin takes the delivery's slot from active to settling and returns what the
// settlement needs.
//
// This is the stale-disposition guard, and it is mandatory: the specification names "lets a
// stale ack settle a later redelivery" a disqualifier. Three things have to be
// true before a disposition may be written, and they are checked in this order
// because each is cheaper and more specific than the next:
//
//  1. the token table accepts the token, which refuses a second settlement
//     (ErrAlreadySettled), one racing another (ErrSettlementInProgress), one
//     whose outcome is unknown (ErrSettlementUnknown) and one whose slot has
//     been reused (ErrLeaseLost);
//  2. the slot really holds a delivery of this source;
//  3. the delivery's link belongs to the CURRENT connection generation. A
//     delivery from an older one has gone back to the node — detaching a link
//     returns everything unsettled on it — so a disposition for it must not be
//     written at all, and the delivery it names may already be another
//     consumer's.
//
// The third check is not made redundant by the invalidation that reconnect.go
// performs. The invalidation is what makes the table refuse such a token, and
// this is what makes the refusal true even if a future change to the close or
// the reconnect path forgets to invalidate — which is the shape of the defect
// The specification exists for, and one a test can drive directly.
func (s *settler) begin(d *mq.DriverDelivery) (*held, error) {
	if d == nil {
		return nil, errNilDelivery
	}
	if s.src.isClosing() {
		return nil, s.src.errClosed
	}
	ref, err := s.src.table.Settle(d.Token)
	if err != nil {
		s.countStale(err)
		return nil, err
	}
	h, ok := ref.(*held)
	if !ok || h == nil {
		_ = s.src.table.Release(d.Token)
		return nil, mq.ErrLeaseLost
	}
	if h.gen != s.src.b.Generation() || d.Gen != h.gen {
		// The link this delivery came from is not the link this source has
		// now. Releasing the slot rather than completing it is the right
		// bookkeeping: the delivery is gone, so the slot is free and its
		// generation rises, and the token that named it is stale from here on.
		_ = s.src.table.Release(d.Token)
		s.src.forget(h)
		s.src.window.freed(h.epoch)
		s.src.stale.Add(1)
		s.src.b.stale.Add(1)
		return nil, mq.ErrLeaseLost
	}
	return h, nil
}

// finish writes the disposition and records its outcome.
//
// Every failure is mapped through
// [github.com/iqhive/iqmq/driverkit.ErrorMapper.MapNet] with sentAlready set,
// because by the time a disposition fails the frame has been handed to the
// library and may have left. The one exception is a context that ended under
// `first`, which go-amqp reports only for a frame it never wrote, so that
// failure is definite and the delivery stays settleable. What the MODE decides is which side that
// lands on, and errors.go says why: under `first` the settlement is the frame,
// so a failure means the node still holds the delivery and will redeliver it —
// a lost lease; under `second` the peer confirms, so a failure may be a
// settlement the peer took and a confirmation nobody heard — an unknown
// outcome, sticky for ever.
//
// A definite refusal leaves the delivery settleable, which is what
// [github.com/iqhive/iqmq/driverkit.TokenTable.Complete] does with an error that
// is neither unknown nor a lost lease; an unknown one keeps the slot for ever;
// and a lost lease frees it, because the delivery is not this source's any more.
//
// A disposition that failed because its link went is never a definite refusal,
// whatever condition the detach carried ([goneWithItsLink]), and it drops the
// link ([Source.receiverLost]): every delivery the link carried went back to
// the node with it, an unknown one included, so the source invalidates them all
// and the next fetch attaches a new link.
func (s *settler) finish(ctx context.Context, d *mq.DriverDelivery, h *held, op string, disp client.Disposition) error {
	dctx, cancel := s.src.settleBound(ctx)
	defer cancel()
	if err := dctx.Err(); err != nil {
		// Nothing is written under a context that has already ended, so the
		// failure is definite and the delivery stays settleable.
		mapped := s.src.b.errs.MapNet(op, s.src.topic, s.src.subscription, err, false)
		_ = s.src.table.Complete(d.Token, mapped)
		return mapped
	}
	err := h.rcv.Dispose(dctx, h.msg, disp)
	if err == nil {
		_ = s.src.table.Complete(d.Token, nil)
		s.src.forget(h)
		s.src.window.freed(h.epoch)
		return nil
	}
	ctxEnded := errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
	// Under `first` go-amqp returns the context's error only when its
	// connection writer found the context done BEFORE it wrote the frame
	// (a write that starts and fails kills the connection and comes back as
	// a *amqp.ConnError), and there is no confirmation to wait for. So the
	// disposition never left, the delivery is still unsettled on a live
	// link, and the failure is definite: mapped as sent, it kept the slot as
	// an unknown outcome for good and the message was never redelivered.
	sent := !ctxEnded || s.src.b.cfg.ReceiverSettle.Resolve() == ReceiverSettleSecond
	mapped := s.src.b.errs.MapNet(op, s.src.topic, s.src.subscription, err, sent)
	if s.src.b.cfg.ReceiverSettle.Resolve() == ReceiverSettleSecond && ctxEnded {
		// Under `second` the library writes the disposition and then waits
		// for the peer's confirmation, returning the context's error when
		// that does not come in time. The peer may have settled the
		// delivery, so the outcome is unknown rather than a deadline the
		// caller may retry past: a retry would write a second disposition.
		unknownSettlement(mapped)
	}
	if isTransportGone(err) && mq.CodeOf(mapped) != mq.CodeLeaseLost {
		goneWithItsLink(mapped, s.src.b.cfg.ReceiverSettle.Resolve())
	}
	if isLinkGone(err) && s.src.receiverLost(ctx, h.rcv) {
		// Dropping the link invalidated every delivery from it and reset the
		// credit window, which freed this slot with the rest and woke a
		// fetch parked on the window into attaching a new link. The
		// bookkeeping below would count this slot a second time, and
		// freeing the slot before the link was dropped could wake that
		// fetch onto the dead link instead.
		return mapped
	}
	switch {
	case mq.CodeOf(mapped) == mq.CodeLeaseLost:
		_ = s.src.table.Release(d.Token)
		s.src.forget(h)
		s.src.window.freed(h.epoch)
		s.src.stale.Add(1)
		s.src.b.stale.Add(1)
	case mq.IsAmbiguous(mapped):
		// Sticky for ever. The slot stays taken and the credit window
		// records it, so the condition has a name and a number rather than
		// being an unexplained loss of throughput.
		//
		// The question is the one TokenTable.Complete asks, and not the
		// code alone: MapNet with sentAlready keeps the transport's code
		// (Unavailable for an EOF after the frame left) and says the outcome
		// is unknown through the Ambiguous flag. The table keeps such a slot
		// as unknown, and a window that read only CodeSettlementUnknown never
		// counted it, so Unknown understated the slots that will never come
		// back and a source whose every slot was stuck that way parked its
		// fetches instead of reporting the exhaustion.
		_ = s.src.table.Complete(d.Token, mapped)
		s.src.window.stuck(h.epoch)
	default:
		// A definite refusal: the delivery goes back to active and the caller
		// may try again.
		_ = s.src.table.Complete(d.Token, mapped)
	}
	return mapped
}

// unknownSettlement marks a mapped settlement failure as an unknown outcome.
// The native error stays underneath, so the context's error is still
// discoverable with errors.Is.
func unknownSettlement(mapped error) {
	var oe *mq.OpError
	if errors.As(mapped, &oe) {
		oe.Code, oe.Ambiguous, oe.Retryable = mq.CodeSettlementUnknown, true, false
	}
}

// goneWithItsLink reclassifies the failure of a disposition whose link, session
// or connection has gone as the settle mode reads any transport failure: a lost
// lease under `first` and an unknown outcome under `second`.
//
// The rule table classifies a condition before the transport it arrived in, so
// a peer that detached the link with `amqp:resource-deleted`, `amqp:not-found`
// or `amqp:unauthorized-access` made a failed disposition a definite NotFound or
// Unauthorized. finish then kept the slot for a second attempt the dead link
// could never take, though the delivery had gone back to the node with the
// link, and a source whose every slot was held that way parked its fetches on
// the credit window for good. The condition says why the link went, not what
// became of the disposition: the same distinction [markAmbiguous] draws for a
// publication. It stays underneath for [Condition] to read. A lost lease, which
// a stolen link is under either mode, is already the answer and is left alone.
//
// MapNet builds a fresh error, so changing it changes nothing anybody else
// holds.
func goneWithItsLink(mapped error, mode ReceiverSettleMode) {
	if mode == ReceiverSettleSecond {
		unknownSettlement(mapped)
		return
	}
	// Ambiguous is cleared rather than left as MapNet set it: a lost lease is a
	// delivery the node took back, which is a definite answer, and a profile
	// rule or classifier that marked a link error ambiguous must not make it
	// read as an unknown outcome as well.
	var oe *mq.OpError
	if errors.As(mapped, &oe) {
		oe.Code, oe.Ambiguous, oe.Retryable = mq.CodeLeaseLost, false, false
	}
}

// countStale records a settlement the guard refused, so that a deployment can
// see how often its handlers outlive their links.
func (s *settler) countStale(err error) {
	if mq.CodeOf(err) == mq.CodeLeaseLost {
		s.src.stale.Add(1)
		s.src.b.stale.Add(1)
	}
}

// deadLetter routes a rejected delivery to the profile's dead-letter address and
// only then rejects the original.
//
// **The order is the contract.** The specification says "a failed or ambiguous dead-letter
// publish never acknowledges the original", so the publication goes first and it
// waits for the peer's `accepted` disposition — not for the frame to be written.
// Only once that disposition is in hand does the rejection go out.
//
// A publication that fails leaves the delivery settleable and settles nothing,
// so the message is still the broker's and still redeliverable. A publication
// that succeeded and a rejection that then failed is the one outcome that can
// duplicate: the copy exists and the original will come back. That is the right
// way round — the specification chooses a duplicate over a loss — and it is reported with the
// rejection's own error rather than as a success.
func (s *Source) deadLetter(ctx context.Context, d *mq.DriverDelivery, h *held, o mq.NackOptions) error {
	addr, err := s.b.profile.DeadLetterAddress(s.topic, s.subscription)
	if err != nil {
		_ = s.table.Complete(d.Token, err)
		return s.b.errs.Map(mq.OpNack, s.topic, s.subscription, err)
	}
	link, err := s.deadLetterLink(ctx, addr)
	if err != nil {
		return s.deadLetterFailed(d, s.b.errs.Map(mq.OpNack, s.topic, s.subscription, err))
	}
	// The copy is a publication and is bounded like one: the caller's
	// context may carry no deadline, and a peer that never answers must not
	// hold the delivery in settling for ever.
	sctx, cancel := s.b.bound(ctx)
	defer cancel()
	copyMsg := deadLetterCopy(h.msg, o.Reason, s.b.profile.Dedup, s.b.profile.Delay, s.b.profile.Attempt)
	rec, err := link.snd.SendWithReceipt(sctx, copyMsg, nil)
	if err != nil {
		s.dropDeadLetterLink(ctx, link, err)
		return s.deadLetterFailed(d, s.b.errs.MapNet(mq.OpNack, s.topic, s.subscription, err, sentDuringSend(err)))
	}
	state, werr := rec.Wait(sctx)
	if werr != nil {
		// The copy may exist and may not. Nothing is destroyed and the
		// delivery stays settleable, so the message is still in the broker
		// either way — which is the outcome the specification wants when the copy's fate is
		// unknown.
		s.dropDeadLetterLink(ctx, link, werr)
		return s.deadLetterFailed(d, s.b.errs.MapNet(mq.OpNack, s.topic, s.subscription, werr, true))
	}
	if oerr := outcomeError(state); oerr != nil {
		return s.deadLetterFailed(d, s.b.errs.Map(mq.OpNack, s.topic, s.subscription, oerr))
	}
	return s.set.finish(ctx, d, h, mq.OpNack, client.Disposition{
		Outcome: client.OutcomeRejected,
		Error:   rejectionError(deadLetterCondition, o.Reason),
	})
}

// deadLetterFailed records a dead-letter route that failed before the original
// was settled, and leaves the delivery settleable.
//
// Mapped under OpNack, a link that died reads as LeaseLost or
// SettlementUnknown, and both are wrong here: no disposition was written, so
// the delivery is still this source's whatever became of the copy. A lost lease
// makes the core forget a delivery the driver still holds and an unknown outcome
// keeps it for ever, and either way its credit slot is never freed. The answer
// is a definite, retryable refusal, so the caller can settle it another way.
func (s *Source) deadLetterFailed(d *mq.DriverDelivery, mapped error) error {
	var oe *mq.OpError
	if errors.As(mapped, &oe) {
		oe.Ambiguous = false
		if oe.Code == mq.CodeLeaseLost || oe.Code == mq.CodeSettlementUnknown {
			oe.Code, oe.Retryable = mq.CodeUnavailable, true
		}
	}
	_ = s.table.Complete(d.Token, mapped)
	return mapped
}

// dropDeadLetterLink forgets a dead-letter link whose transfer failed because
// the link, its session or its connection went away, so that the next
// rejection attaches a fresh one instead of failing on the dead one.
func (s *Source) dropDeadLetterLink(ctx context.Context, link *senderLink, err error) {
	if !isTransportGone(err) {
		return
	}
	s.dlmu.Lock()
	if s.dlLink != link {
		s.dlmu.Unlock()
		return
	}
	s.dlLink = nil
	s.dlmu.Unlock()
	dctx, cancel := s.b.bound(ctx)
	defer cancel()
	_ = detachSender(dctx, link)
}

// deadLetterLink attaches this source's dead-letter sending link, or returns the
// one it has.
//
// The link lives on the source's own session and is attached lazily, on the
// first rejection: a source that attached one at open would hold a link on every
// subscription whether or not anything is ever rejected, and on a broker with a
// per-connection link ceiling that is a cost every consumer pays for a path most
// of them never take.
func (s *Source) deadLetterLink(ctx context.Context, addr string) (*senderLink, error) {
	s.dlmu.Lock()
	defer s.dlmu.Unlock()
	_, gen, err := s.b.connection()
	if err != nil {
		return nil, err
	}
	if s.dlLink != nil && s.dlLink.gen == gen {
		return s.dlLink, nil
	}
	old := s.dlLink
	s.dlLink = nil
	actx, cancel := s.b.bound(ctx)
	defer cancel()
	if old != nil {
		_ = detachSender(actx, old)
	}
	// The copy is a publication like any other, so it asks for the same kind
	// of target node a sink does. Without it a profile that routes by node
	// kind -- Artemis's `queue` capability for a JMS queue -- has its
	// dead-letter copy routed by the address's own default, which on an
	// auto-created or mixed address is multicast with no queue behind it: the
	// copy is accepted and dropped, and the original is rejected as if it had
	// landed. Only the node kind is taken from the publisher's options; its
	// link name would collide with the sink's on the same connection.
	link, err := s.b.attachSender(actx, addr, &amqp.SenderOptions{
		SettlementMode:              deadLetterSettle(s.b.cfg.SenderSettle).amqpMode(),
		RequestedReceiverSettleMode: s.b.cfg.ReceiverSettle.amqpMode(),
		TargetCapabilities:          s.b.cfg.Publisher.TargetCapabilities,
	})
	if err != nil {
		return nil, err
	}
	s.dlLink = link
	return link, nil
}

// deadLetterSettle is the settlement mode a source's dead-letter link attaches
// with.
//
// The copy waits for its disposition before the original is rejected, so the
// link has to be able to send an unsettled transfer. Under
// SenderSettleSettled it cannot: go-amqp refuses SendWithReceipt on a settled
// link before writing anything, and every dead-letter Nack failed. That mode
// is a sink's choice not to wait for an answer, so the dead-letter link takes
// the default, mixed, instead. Every other mode can carry the copy and is
// kept.
func deadLetterSettle(m SenderSettleMode) SenderSettleMode {
	if m.Resolve() == SenderSettleSettled {
		return SenderSettleMixed
	}
	return m
}

// deadLetterCopy is the message that goes to the dead-letter address.
//
// It carries the original's body, properties and application properties
// unchanged, plus two annotations that say where it came from and why: a
// dead-letter destination whose messages do not name their origin and their
// reason is a queue somebody has to guess about. The delivery annotations are
// NOT copied — they are hop-by-hop and describe the transfer this message is
// leaving, not the one it is joining.
//
// The header section is a fresh one that keeps the original's durability,
// priority and time to live. The original's delivery-count is the source's
// count of failed attempts, and a broker hands a message's header on as it was
// published until a delivery of it fails: AMQP 1.0 raises the count only on
// such an outcome, and Artemis sends the encoding it was given while its own
// count is one or less. So a copy that kept the count arrived at the
// dead-letter address already counted as redelivered, and a consumer there
// with WithMaxAttempts at or below that count treated every copy as exhausted
// on arrival. The copy is a new message, so its count starts at zero and
// nobody has acquired it; the attempt it failed on is in
// AnnotationDeadLetterAttempt.
//
// A profile that reads the count from a broker annotation
// (AttemptFromAnnotation) has the same problem with that annotation, so the
// copy drops it, and AnnotationDeadLetterAttempt is read from it rather than
// from a header section such a broker does not count in.
//
// The source's own AnnotationDeadLetterAttempt and AnnotationDeadLetterReason
// are dropped too, which matters when the source is itself a dead-letter copy
// that failed again: the copy carries the attempt and the reason of this
// dead-lettering, each absent when there is none, and not the first copy's.
//
// Two keys of the original publication are dropped as well, because the
// broker acts on them again when the copy is published: the profile's
// de-duplication key and its delay key, wherever the profile puts them. A
// broker delivers both as they were published (Artemis sends a message's
// application properties and annotations back byte for byte), so a copy that
// kept them was a new publication of the same key and the same delay. Artemis
// keeps its de-duplication cache per address, and two subscriptions
// dead-lettering one publication to one dead-letter address had the second
// copy accepted and dropped as a duplicate; the delay held the copy back at
// the dead-letter address for as long again as it held the original.
func deadLetterCopy(src *amqp.Message, reason string, dedup SlotRule, delay DelayRule, attempt AttemptRule) *amqp.Message {
	drop := [...]SlotRule{
		dedup,
		{Slot: delay.Slot, Key: delay.Key},
		attemptKey(attempt),
		{Slot: SlotMessageAnnotation, Key: AnnotationDeadLetterAttempt},
		{Slot: SlotMessageAnnotation, Key: AnnotationDeadLetterReason},
	}
	out := &amqp.Message{
		Header:                freshHeader(src.Header),
		Properties:            src.Properties,
		ApplicationProperties: withoutKeys(src.ApplicationProperties, SlotApplicationProperty, drop[:]),
		Data:                  src.Data,
		Value:                 src.Value,
		Sequence:              src.Sequence,
	}
	annos := make(amqp.Annotations, len(src.Annotations)+2)
	for k, v := range src.Annotations {
		if !droppedAnnotation(k, drop[:]) {
			annos[k] = v
		}
	}
	if n, ok := failedAttempt(src, attempt); ok {
		annos[AnnotationDeadLetterAttempt] = n
	}
	if reason != "" {
		annos[AnnotationDeadLetterReason] = reason
	}
	out.Annotations = annos
	return out
}

// attemptKey is the key a profile that reads the attempt count from a broker
// annotation keeps it under, which the copy drops. A profile that reads the
// header, or no count, names none.
func attemptKey(r AttemptRule) SlotRule {
	if r.Source != AttemptFromAnnotation {
		return SlotRule{}
	}
	return SlotRule{Slot: r.Slot, Key: r.Key}
}

// failedAttempt is the attempt src failed on, for the copy's
// AnnotationDeadLetterAttempt, read where r says the broker counts. It reports
// false when src carries no count there.
func failedAttempt(src *amqp.Message, r AttemptRule) (uint32, bool) {
	if r.Source == AttemptFromAnnotation {
		n := annotatedAttempt(src, r)
		return n, n > 0
	}
	if src.Header == nil {
		return 0, false
	}
	return src.Header.DeliveryCount + 1, true
}

// freshHeader is the header section of a new message that keeps h's
// durability, priority and time to live, or nil for a message with none.
func freshHeader(h *amqp.MessageHeader) *amqp.MessageHeader {
	if h == nil {
		return nil
	}
	return &amqp.MessageHeader{Durable: h.Durable, Priority: h.Priority, TTL: h.TTL}
}

// withoutKeys is props without the keys that drop names in slot. It is props
// itself, shared and not copied, when there is nothing to drop: the copy's
// sections are the original's wherever they can be.
func withoutKeys(props map[string]any, slot Slot, drop []SlotRule) map[string]any {
	var out map[string]any
	for _, r := range drop {
		if r.Slot != slot || r.Key == "" {
			continue
		}
		if _, ok := props[r.Key]; !ok {
			continue
		}
		if out == nil {
			out = maps.Clone(props)
		}
		delete(out, r.Key)
	}
	if out == nil {
		return props
	}
	return out
}

// droppedAnnotation reports whether a message-annotation key is one that drop
// names. A decoded key is a string; one a caller built may be an amqp.Symbol.
func droppedAnnotation(key any, drop []SlotRule) bool {
	var name string
	switch k := key.(type) {
	case string:
		name = k
	case amqp.Symbol:
		name = string(k)
	default:
		return false
	}
	for _, r := range drop {
		if r.Slot == SlotMessageAnnotation && r.Key != "" && r.Key == name {
			return true
		}
	}
	return false
}

// The annotations a dead-lettered copy carries.
const (
	// AnnotationDeadLetterAttempt is the attempt count the original had
	// reached, as a uint32: its header's delivery-count plus one, or, for a
	// profile with [AttemptFromAnnotation], the count the broker annotation
	// names.
	AnnotationDeadLetterAttempt = "x-iqmq-dead-letter-attempt"
	// AnnotationDeadLetterReason is [github.com/iqhive/iqmq.NackOptions.Reason],
	// which this driver documents that it transports on this route only.
	AnnotationDeadLetterReason = "x-iqmq-dead-letter-reason"
)

// The settler's refusals.
var (
	errNilDelivery = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: the settlement names no delivery")
	errBadNackAction = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: the nack action is not one of requeue, drop or deadletter")
	errNoNackDelay = errcode.New(errcode.Unsupported,
		"iqmq/amqp10: this profile has no way to hold a redelivery back; AMQP 1.0's `modified` outcome makes a message available again at once, so a delayed requeue needs a broker annotation the profile names (Profile.NackDelay)")
	errNackDelayTooLong = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: the redelivery delay is longer than this broker accepts (Profile.NackDelay.Max)")
	errNoDeadLetter = errcode.New(errcode.Unsupported,
		"iqmq/amqp10: this profile has no dead-letter destination, so Reject has nowhere to route the message; set Profile.DeadLetterAddress, or claim FeatureDeadLetter when the broker's own policy routes a `rejected` outcome (the message contract)")
)
