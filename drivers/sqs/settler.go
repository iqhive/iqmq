package sqs

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/driverkit/flush"
)

// The settler implements the mandatory settlement surface, the optional
// batched one and the optional lease extension.
//
// The batched surface is the one that earns its keep here: a settlement on
// this broker is an HTTP request the service charges for, so deleting a
// hundred messages one at a time is a hundred requests and deleting them ten
// at a time is ten. [driverkit.BatchSettler] is what coalesces them and
// [github.com/iqhive/iqmq/driverkit/flush] (the specification) is what decides when a coalesced batch leaves.
var (
	_ mq.Settler          = (*settler)(nil)
	_ mq.BatchSettler     = (*settler)(nil)
	_ mq.DeadlineExtender = (*settler)(nil)
)

// settler settles one source's deliveries.
type settler struct {
	src    *source
	policy flush.Policy

	once  sync.Once
	batch *driverkit.BatchSettler
	wrap  mq.Settler
}

// newSettler builds the settler and binds the policy.
func newSettler(src *source) (*settler, error) {
	cfg := src.b.cfg.resolve()
	policy, err := flush.New(cfg.Settle, flush.Config{
		MaxItems: cfg.SettleMaxItems,
		Interval: cfg.SettleInterval,
	})
	if err != nil {
		return nil, err
	}
	return &settler{src: src, policy: policy}, nil
}

// settler returns the value Source.Settler hands the core: this settler
// wrapped in driverkit's coalescing one, with the native batch surface still
// reachable.
func (s *settler) settler() mq.Settler {
	s.once.Do(func() {
		cfg := s.src.b.cfg.resolve()
		b, err := driverkit.NewBatchSettler(s, driverkit.BatchConfig{
			Batching: cfg.Settle.Batching(),
			MaxItems: s.policy.MaxItems(),
			Interval: intervalOf(s.policy),
			Clock:    s.src.b.clock,
		})
		if err != nil {
			// NewBatchSettler refuses only a nil settler and an invalid
			// configuration, neither of which is reachable from here
			// (Config.Validate has already run); using the bare settler
			// rather than panicking keeps a future configuration mistake
			// from taking the process down.
			s.wrap = driverkit.WrapSettler(s, s)
			return
		}
		s.batch = b
		// The kit's own value is what goes to the core. This driver used to
		// wrap it in a facade because driverkit.BatchSettler.Settler()
		// returned something that implemented Settler and DeadlineExtender
		// and NOT mq.BatchSettler — the omission the specification records —
		// so a driver whose settler had a native batch could not expose it
		// through the kit. WrapSettler now preserves every optional
		// settlement surface the specification defines, with a batched settlement going
		// straight to the native batch and a single one through the
		// coalescing queue.
		s.wrap = b.Settler()
	})
	return s.wrap
}

// intervalOf is the ticker period the policy asked for, and zero for a policy
// that needs none. The kit reads zero as its own default and starts no ticker
// for the two strategies that do not use one, so the two agree.
func intervalOf(p flush.Policy) time.Duration {
	if d, ok := p.Ticker(); ok {
		return d
	}
	return 0
}

// close releases the coalescing settler's goroutine and sends whatever it was
// holding.
func (s *settler) close() error {
	if s.batch != nil {
		return s.batch.Close()
	}
	return nil
}

// Ack settles d positively: the message is deleted.
func (s *settler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	return s.AckBatch(ctx, []*mq.DriverDelivery{d})
}

// AckBatch deletes every delivery in ds, reporting per-delivery outcomes
// through a *BatchError whose Errs are index-aligned with ds.
//
// The batch is split on the service's ten entries, and each chunk's entry
// identifier is the delivery's index in ds — which is what makes a partial
// response correlate exactly. A DeleteMessageBatch can and does fail for some
// entries and not others, and the ones that failed must stay UNSETTLED: the
// core hands the caller the error at their index and the message is still in
// the queue, so the next delivery of it is a redelivery rather than a loss.
func (s *settler) AckBatch(ctx context.Context, ds []*mq.DriverDelivery) error {
	return collapse(s.ackResults(ctx, ds))
}

// ackResults is AckBatch with the per-item results exposed, which is what the
// driver's own tests assert on.
func (s *settler) ackResults(ctx context.Context, ds []*mq.DriverDelivery) []error {
	errs := make([]error, len(ds))
	refs := make([]*delivery, len(ds))
	for i, d := range ds {
		ref, err := s.claim(d)
		if err != nil {
			errs[i] = err
			continue
		}
		refs[i] = ref
	}
	s.eachChunk(refs, errs, func(entries []int) {
		s.deleteChunk(ctx, refs, entries, errs)
	})
	for i, ref := range refs {
		if ref != nil {
			s.complete(ref, errs[i])
		}
	}
	return errs
}

// eachChunk calls fn with the indexes of each chunk of at most
// MaxBatchEntries claimable deliveries.
func (s *settler) eachChunk(refs []*delivery, errs []error, fn func(entries []int)) {
	chunk := make([]int, 0, MaxBatchEntries)
	for i := range refs {
		if refs[i] == nil || errs[i] != nil {
			continue
		}
		chunk = append(chunk, i)
		if len(chunk) == MaxBatchEntries {
			fn(chunk)
			chunk = chunk[:0]
		}
	}
	if len(chunk) > 0 {
		fn(chunk)
	}
}

// deleteChunk performs one DeleteMessageBatch and writes each entry's outcome
// to its own index.
func (s *settler) deleteChunk(ctx context.Context, refs []*delivery, at []int, errs []error) {
	entries := make([]types.DeleteMessageBatchRequestEntry, 0, len(at))
	for _, i := range at {
		entries = append(entries, types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(entryID(i)),
			ReceiptHandle: aws.String(refs[i].handle),
		})
	}
	callCtx, cancel := s.src.b.call(ctx)
	defer cancel()
	out, err := s.src.b.api.DeleteMessageBatch(callCtx, &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(s.src.url),
		Entries:  entries,
	})
	if err != nil {
		mapped := s.mapSettle(mq.OpAck, err)
		for _, i := range at {
			errs[i] = mapped
		}
		return
	}
	for _, i := range at {
		// Seeded with a refusal: an entry the service named in neither list
		// must not read as a success.
		errs[i] = errNoEntryResult
	}
	for i := range out.Successful {
		if idx, ok := entryIndex(aws.ToString(out.Successful[i].Id), len(refs)); ok {
			errs[idx] = nil
		}
	}
	for i := range out.Failed {
		e := &out.Failed[i]
		idx, ok := entryIndex(aws.ToString(e.Id), len(refs))
		if !ok {
			continue
		}
		errs[idx] = batchEntryError(mq.OpAck, s.src.cfg.Topic,
			aws.ToString(e.Code), aws.ToString(e.Message), e.SenderFault)
	}
}

// Nack settles d negatively under o.
func (s *settler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	errs := s.nackResults(ctx, []*mq.DriverDelivery{d}, o)
	return errs[0]
}

// NackBatch settles every delivery in ds negatively under o, with the same
// index-aligned result contract.
func (s *settler) NackBatch(ctx context.Context, ds []*mq.DriverDelivery, o mq.NackOptions) error {
	return collapse(s.nackResults(ctx, ds, o))
}

// nackResults performs one negative settlement for every delivery.
//
// The three actions map to three different things, and only one of them
// is a batch:
//
//   - Requeue is ChangeMessageVisibilityBatch with the delay, or with zero for
//     an immediate redelivery. It is native, it is batched, and the delay
//     survives a process death because the queue holds it.
//   - DeadLetter sends the message to Config.DeadLetterQueue and deletes the
//     original only once that send is confirmed, which is the rule that a
//     failed or ambiguous dead-letter publication never acknowledges the
//     original. It is one round trip per message because a send and a delete
//     cannot be interleaved across messages safely.
//   - Drop deletes the message, which is the one action that destroys it, and
//     is only ever reached from an explicit NackActionDrop.
func (s *settler) nackResults(ctx context.Context, ds []*mq.DriverDelivery, o mq.NackOptions) []error {
	errs := make([]error, len(ds))
	refs := make([]*delivery, len(ds))
	for i, d := range ds {
		ref, err := s.claim(d)
		if err != nil {
			errs[i] = err
			continue
		}
		refs[i] = ref
	}
	switch o.Action {
	case mq.NackActionRequeue:
		if o.Delay > MaxVisibility {
			for i := range refs {
				if refs[i] != nil {
					errs[i] = errDelayTooLongForLease
				}
			}
			break
		}
		s.eachChunk(refs, errs, func(at []int) {
			s.visibilityChunk(ctx, refs, at, errs, visibilitySeconds(o.Delay))
		})
	case mq.NackActionDrop:
		s.eachChunk(refs, errs, func(at []int) {
			s.deleteChunk(ctx, refs, at, errs)
		})
	case mq.NackActionDeadLetter:
		for i := range refs {
			if refs[i] != nil {
				errs[i] = s.deadLetter(ctx, refs[i], o)
			}
		}
	default:
		for i := range refs {
			if refs[i] != nil {
				errs[i] = errNackAction
			}
		}
	}
	for i, ref := range refs {
		if ref == nil {
			continue
		}
		if o.Action == mq.NackActionRequeue && errs[i] == nil {
			// A requeued delivery is not settled: it is back in the queue
			// and will be delivered again. Marking it lost rather than
			// settled is what makes a second settlement for the SAME
			// delivery report ErrLeaseLost instead of touching the
			// redelivery.
			ref.lose()
			s.src.forget(ref)
			continue
		}
		s.complete(ref, errs[i])
	}
	return errs
}

// visibilityChunk performs one ChangeMessageVisibilityBatch.
func (s *settler) visibilityChunk(ctx context.Context, refs []*delivery, at []int, errs []error, seconds int32) {
	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, 0, len(at))
	for _, i := range at {
		entries = append(entries, types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(entryID(i)),
			ReceiptHandle:     aws.String(refs[i].handle),
			VisibilityTimeout: seconds,
		})
	}
	callCtx, cancel := s.src.b.call(ctx)
	defer cancel()
	out, err := s.src.b.api.ChangeMessageVisibilityBatch(callCtx, &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(s.src.url),
		Entries:  entries,
	})
	if err != nil {
		mapped := s.mapSettle(mq.OpNack, err)
		for _, i := range at {
			errs[i] = mapped
		}
		return
	}
	for _, i := range at {
		errs[i] = errNoEntryResult
	}
	for i := range out.Successful {
		if idx, ok := entryIndex(aws.ToString(out.Successful[i].Id), len(refs)); ok {
			errs[idx] = nil
		}
	}
	for i := range out.Failed {
		e := &out.Failed[i]
		idx, ok := entryIndex(aws.ToString(e.Id), len(refs))
		if !ok {
			continue
		}
		errs[idx] = batchEntryError(mq.OpNack, s.src.cfg.Topic,
			aws.ToString(e.Code), aws.ToString(e.Message), e.SenderFault)
	}
}

// deadLetter routes one message off the queue, the way round.
//
// The order is the whole point: the message is SENT to the dead-letter queue
// first and the original is deleted only once the send is confirmed. A driver
// that deleted first, or that deleted after an AMBIGUOUS send, would
// acknowledge a message it had written nowhere — which the specification forbids outright
// and which is silent data loss on the one path that exists to make a poison
// message safe.
func (s *settler) deadLetter(ctx context.Context, ref *delivery, o mq.NackOptions) error {
	cfg := s.src.b.cfg.resolve()
	if cfg.DeadLetterQueue == "" {
		// The specification: only an explicit destination authorises a route off the
		// queue. Refusing leaves the delivery settleable, so the caller may
		// requeue it instead.
		return mq.ErrUnsupported
	}
	callCtx, cancel := s.src.b.call(ctx)
	defer cancel()
	url, err := s.src.b.res.URLOfName(callCtx, cfg.DeadLetterQueue)
	if err != nil {
		return s.mapSettle(mq.OpNack, err)
	}
	native := ref.native
	if native == nil {
		return mq.ErrLeaseLost
	}
	attrs := make(map[string]types.MessageAttributeValue, len(native.MessageAttributes)+2)
	for k, v := range native.MessageAttributes {
		attrs[k] = v
	}
	// the provenance: where it came from and why. Both are diagnostics
	// and neither is a payload.
	attrs[attrDLQOrigin] = stringAttr(QueueNameOfURL(ref.url))
	if o.Reason != "" {
		attrs[attrDLQReason] = stringAttr(reasonText(o.Reason))
	}
	if len(attrs) > MaxAttributes {
		// The provenance cannot displace the message's own metadata, so a
		// message that already fills the budget travels without the
		// origin rather than losing an attribute it was published with.
		delete(attrs, attrDLQReason)
		if len(attrs) > MaxAttributes {
			delete(attrs, attrDLQOrigin)
		}
	}
	in := &sqs.SendMessageInput{
		QueueUrl:          aws.String(url),
		MessageBody:       native.Body,
		MessageAttributes: attrs,
	}
	if s.src.b.cfg.FIFO {
		in.MessageGroupId = aws.String(orDefaultGroup(ref.group))
		in.MessageDeduplicationId = aws.String("dl-" + ref.id)
	}
	if _, err := s.src.b.api.SendMessage(callCtx, in); err != nil {
		// The send failed or is ambiguous. Either way the original stays,
		// which is the specification's "a failed or ambiguous DLQ publish never acks the
		// original".
		return s.mapSettle(mq.OpNack, err)
	}
	_, err = s.src.b.api.DeleteMessage(callCtx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(ref.url),
		ReceiptHandle: aws.String(ref.handle),
	})
	if err != nil {
		// The message is on the dead-letter queue and still on the source
		// queue: it will be delivered again, which is a duplicate and not a
		// loss. Reporting the failure is what lets the caller know.
		return s.mapSettle(mq.OpNack, err)
	}
	return nil
}

// reasonText is a dead-letter reason the service will carry.
//
// A String attribute is held to the body's character set (the pinned SDK's
// MessageAttributeValue documentation), and the reason is the caller's text.
// One carrying a control character or invalid UTF-8, which an error's text
// can, was sent as it was, the service refused the whole SendMessage with
// InvalidMessageContents, and the message could not be dead-lettered at all,
// by a Reject or by the undecodable route, for as long as the reason said the
// same thing. Each character the service would refuse becomes U+FFFD, so the
// rest of the diagnosis still arrives; a reason with nothing to replace is
// returned as it is, without a copy.
func reasonText(reason string) string {
	return strings.Map(func(r rune) rune {
		if sqsSafeRune(r) {
			return r
		}
		return utf8.RuneError
	}, reason)
}

// orDefaultGroup is the group a dead-lettered message is sent in.
func orDefaultGroup(group string) string {
	if group == "" {
		return DefaultGroupID
	}
	return group
}

// The dead-letter provenance attributes.
const (
	attrDLQOrigin = "iqmq-dlq-origin"
	attrDLQReason = "iqmq-dlq-reason"
)

// Extend moves d's lease deadline at least by further into the future.
//
// The service's ChangeMessageVisibility sets the window from the moment of the
// CALL, not from the delivery's deadline, so the timeout that moves the
// deadline `by` further is whatever is LEFT of the window plus by. Sending
// `by` as the timeout — which this driver did until the specification — sets
// the window to `by` from now, so an extension shorter than the remaining
// window SHORTENS the lease: the message becomes visible to another consumer
// early and the next settlement reports ErrLeaseLost, which is the opposite of
// what the caller asked for and the worst direction for the defect to point.
//
// The remaining window is read from the delivery's own local view, which is
// the one the driver already keeps and the one its lease check already trusts
//
//	— no second clock reading is stored per delivery, because
//
// a "received at" beside the deadline is a second copy of one fact and the two
// can disagree. What the local view is measured against is the INJECTED clock,
// so a clock behind the service's overestimates what is left and asks for a
// longer window than needed, which is the safe direction: the specification promises at
// least, and the local view is what refuses a settlement in any case.
//
// Five things are refused rather than sent. A non-positive `by`, because a
// timeout of zero makes the message immediately visible, which is what a Nack
// means. A delivery that is settling, settled or unknown, because the
// settlement owns the window from there. A `by` longer than the service's
// twelve hours. A `by` whose sum with the remaining window is longer than
// twelve hours: no visibility timeout grants that deadline, and clamping to
// the ceiling would move the deadline less than `by` while reporting success.
// And a deadline more than twelve hours after the receive, which is where the
// service stops extending a message whatever timeout a call asks for.
func (s *settler) Extend(ctx context.Context, d *mq.DriverDelivery, by time.Duration) error {
	if by <= 0 {
		return errExtendZero
	}
	// The lease check here is the window itself and not the window less
	// Config.LeaseGuard. The guard is a margin for the DELETE, whose crossing
	// of the window's close is silent and takes another consumer's message;
	// an extension is the call that stops the window closing. Refusing it
	// inside the guard marked a delivery whose window was still open lost,
	// and a lease scheduler whose lead is shorter than the guard had every
	// extension it sent refused that way. The deadline an extension records
	// is measured from before the call, so the settlement that follows is
	// still checked, guard and all, against a window the service granted.
	ref, err := s.peekWithin(d, 0)
	if err != nil {
		return err
	}
	// A settlement in flight owns the visibility window: a requeue Nack's
	// ChangeMessageVisibilityBatch sets it to the redelivery delay, and an
	// extension sent beside it would undo that delay.
	if err := stateError(ref.state.Load()); err != nil {
		return err
	}
	if by > MaxVisibility {
		return errExtendTooLong
	}
	// peekWithin has already refused a delivery whose window has closed, so
	// what is left is positive; the guard is what makes the arithmetic safe
	// if the clock moves between the two readings.
	now := s.src.b.now()
	window := by
	if remaining := ref.expiresAt().Sub(now); remaining > 0 {
		window = remaining + by
	}
	if window > MaxVisibility {
		return errExtendPastCeiling
	}
	// The whole seconds the service will actually grant, rounded UP: the local
	// view has to be what was granted rather than what was asked for, which is the trap in the one place it can make a lease SHORTER
	// than the caller was promised.
	seconds := visibilitySeconds(window)
	if pastReceiveCeiling(d.Info.ReceivedAt, now.Add(time.Duration(seconds)*time.Second)) {
		return errExtendPastCeiling
	}
	callCtx, cancel := s.src.b.call(ctx)
	defer cancel()
	_, err = s.src.b.api.ChangeMessageVisibility(callCtx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(ref.url),
		ReceiptHandle:     aws.String(ref.handle),
		VisibilityTimeout: seconds,
	})
	if err != nil {
		return s.mapSettle(mq.OpExtend, err)
	}
	// The window really has moved, so the local lease check has to move with
	// it or the next settlement would be refused against a deadline that is
	// no longer the service's. It is measured from the clock reading the
	// request was computed from, which is BEFORE the service saw it — so the
	// local deadline is never later than the service's and the refusal stays
	// on the safe side of it.
	ref.extend(now.Add(time.Duration(seconds) * time.Second))
	return nil
}

// pastReceiveCeiling reports whether deadline is later than the service's
// twelve hours allow. The service measures that ceiling from the receive, not
// from the call, so a delivery extended again and again runs into it however
// short each extension is.
func pastReceiveCeiling(received, deadline time.Time) bool {
	return !received.IsZero() && deadline.After(received.Add(MaxVisibility))
}

// claim takes a delivery out of the active state, applying the local lease
// check before anything is sent.
//
// The order matters. The generation and the supersession are checked first,
// because a delivery this source no longer vouches for must report
// ErrLeaseLost whatever its window says; then the window, because a delete for
// an expired handle would succeed at the service and take somebody else's
// message; and only then the state machine, because a second settlement of a
// delivery that IS still ours is ErrAlreadySettled and not a lost
// lease.
func (s *settler) claim(d *mq.DriverDelivery) (*delivery, error) {
	ref, err := s.peek(d)
	if err != nil {
		return nil, err
	}
	if !ref.state.CompareAndSwap(stateActive, stateSettling) {
		return nil, stateError(ref.state.Load())
	}
	return ref, nil
}

// peek recovers a delivery's bookkeeping and applies the lease check, with
// Config.LeaseGuard, without changing its state.
func (s *settler) peek(d *mq.DriverDelivery) (*delivery, error) {
	return s.peekWithin(d, s.src.b.cfg.LeaseGuard)
}

// peekWithin is [settler.peek] with the guard named by the caller, which is
// what an extension needs: see [settler.Extend] for why it passes zero.
func (s *settler) peekWithin(d *mq.DriverDelivery, guard time.Duration) (*delivery, error) {
	if d == nil {
		return nil, mq.ErrNotSettleable
	}
	ref, ok := d.Ref.(*delivery)
	if !ok || ref == nil || ref.src != s.src {
		// A delivery whose Ref is not one of this source's is not
		// settleable: it came from another source, or from a zero
		// DriverDelivery. The specification makes that ErrNotSettleable rather than a
		// panic.
		return nil, mq.ErrNotSettleable
	}
	if ref.gen != d.Gen {
		// The core hands back its own copy of the struct, so the generation
		// in the Ref and the one in the delivery are the same value written
		// once. A mismatch is a delivery that was tampered with, and
		// applying a settlement from it would settle a message this source
		// cannot vouch for.
		return nil, mq.ErrLeaseLost
	}
	if state := ref.state.Load(); state == stateLost {
		return nil, mq.ErrLeaseLost
	}
	if ref.expired(s.src.b.now(), guard) {
		ref.lose()
		s.src.forget(ref)
		return nil, mq.ErrLeaseLost
	}
	return ref, nil
}

// complete records the outcome of a settlement in flight.
//
// A nil error settles the delivery. An AMBIGUOUS one leaves it unknown for
// ever, which the specification makes sticky: nothing later turns an unknown settlement
// into a successful one. Any other error is a definite refusal, which leaves
// the delivery settleable — the caller may try again, and for a partial delete
// batch that is exactly what has to happen.
func (s *settler) complete(ref *delivery, err error) {
	switch {
	case err == nil:
		ref.state.Store(stateSettled)
		s.src.forget(ref)
	case mq.IsAmbiguous(err) || mq.CodeOf(err) == mq.CodeSettlementUnknown:
		ref.state.Store(stateUnknown)
	case errors.Is(err, mq.ErrLeaseLost):
		ref.state.Store(stateLost)
		s.src.forget(ref)
	default:
		ref.state.Store(stateActive)
	}
}

// mapSettle turns a native failure into the model.
//
// Every failure here is post-write — the request has left the socket — so
// sentAlready is true and the rule table plus driverkit.ClassifyNet decide the
// rest. ErrLeaseLost and ErrUnsupported pass through unchanged: they are
// already the answers ask for, and wrapping them in an *OpError
// whose code says something else is how a caller comes to retry a settlement
// that can never succeed.
func (s *settler) mapSettle(op string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, mq.ErrLeaseLost), errors.Is(err, mq.ErrUnsupported),
		errors.Is(err, mq.ErrSettlementInProgress), errors.Is(err, mq.ErrAlreadySettled):
		return err
	}
	return s.src.b.errs.MapNet(op, s.src.cfg.Topic, s.src.cfg.Subscription, err, true)
}
