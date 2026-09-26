package pulsar

import (
	"context"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The compile-time proof of what this settler is.
//
// All three surfaces are here, and the batched one is a real Pulsar verb rather
// than a loop: Consumer.AckIDList sends one frame for many identifiers. The
// NEGATIVE half of the batch is a loop, because Pulsar has no batched negative
// acknowledgement at all, and the driver page says so rather than presenting a
// loop as a round trip.
var (
	_ mq.Settler          = (*settler)(nil)
	_ mq.BatchSettler     = (*settler)(nil)
	_ mq.DeadlineExtender = (*settler)(nil)
)

// settler settles one source's deliveries.
//
// # The settlement boundary
//
// With [Config.AckWithResponse] off — the default, and pulsar-client-go's —
// an acknowledgement is put in the client's grouping tracker and sent within
// AckGroupingMaxTime, and this settler returns as soon as the client has taken
// it. With AckWithResponse on, every acknowledgement is a round trip the broker
// answers and this settler returns when the answer arrives. Both are stated on
// the driver page, because the difference is what "the message will not be
// redelivered" means.
//
// # Why the local checks come first
//
// Pulsar redelivers a message under the SAME (ledger, entry, batch index), and
// the broker accepts an acknowledgement for it from any consumer of the
// subscription that holds it. So an acknowledgement written for a delivery a
// later one has superseded would settle the work that replaced it, which is the specification's "lets a stale ack settle a later redelivery" — a disqualifier and
// not a rough edge. There is no verb that asks "do I still hold this?", so the
// refusal happens here, before anything is written.
//
// The second local check is [AckRules]: one message of a BATCHED entry can only
// be settled on its own when the identifier still carries the client's batch
// bookkeeping and the broker honours an acknowledgement set. When it cannot,
// this settler refuses rather than acknowledging an entry whose other messages
// nobody has finished with.
type settler struct{ src *source }

// Ack settles d positively.
func (s *settler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ref, err := s.claim(d)
	if err != nil {
		return err
	}
	return s.finish(ref, s.src.cons.AckID(ref.id), mq.OpAck)
}

// Nack settles d negatively under o.
//
//   - NackActionRequeue with no delay is Consumer.NackID: the client's negative
//     acknowledgement tracker asks the broker to redeliver after
//     Config.NackRedeliveryDelay, which is a per-CONSUMER setting and the only
//     delay Pulsar's negative acknowledgement has.
//   - NackActionRequeue with a delay is Consumer.ReconsumeLater when
//     Config.RetryEnable is on — a republication to the subscription's retry
//     topic with the delay as a DeliverAfter, which is durable and carries the
//     attempt count in Pulsar's own RECONSUMETIMES property — and an in-process
//     HOLD when it is not, which is the volatile emulation and is declared
//     as one in the capability table.
//   - NackActionDrop is an acknowledgement. The specification calls it destructive and it
//     is: Pulsar's only way to make a message go away without a destination is
//     to mark it consumed.
//   - NackActionDeadLetter is the client's dead-letter route, reached by
//     pushing the message's reconsume count past Config.MaxAttempts — which is
//     exactly what pulsar-client-go's own policy does when a message has been
//     delivered that many times. It is refused when no destination is
//     configured, because the specification forbids a dead-letter settlement that silently
//     destroys the message instead.
func (s *settler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.Delay < 0 {
		return errNegativeNackDelay
	}
	if o.Delay > MaxDelay {
		return errNackDelayTooLong
	}
	switch o.Action {
	case mq.NackActionDeadLetter:
		return s.deadLetter(d)
	case mq.NackActionDrop:
		ref, err := s.claim(d)
		if err != nil {
			return err
		}
		return s.finish(ref, s.src.cons.AckID(ref.id), mq.OpNack)
	case mq.NackActionRequeue:
	default:
		return errNackAction
	}
	if o.Delay > 0 {
		return s.delayedRequeue(d, o.Delay)
	}
	ref, err := s.claim(d)
	if err != nil {
		return err
	}
	// NackID and not Nack(msg): the two differ only in whether the client
	// consults a per-message backoff policy this driver does not configure,
	// and the identifier is what the driver holds for the delivery's life.
	s.src.cons.NackID(ref.id)
	s.src.forget(ref)
	return nil
}

// delayedRequeue is a negative settlement that must not come back for a while.
func (s *settler) delayedRequeue(d *mq.DriverDelivery, delay time.Duration) error {
	if s.src.b.cfg.RetryEnable {
		ref, err := s.claim(d)
		if err != nil {
			return err
		}
		// The republication and the acknowledgement of the original are
		// the client's retry router's, and it acknowledges the original
		// only after the republication has succeeded — so a failed
		// republication never loses the message.
		s.src.cons.ReconsumeLater(ref.msg, delay)
		s.src.forget(ref)
		return nil
	}
	// the volatile emulation: the delivery stays unsettled at the broker
	// and this process remembers when to give it back. The capability table
	// says EmulatedVolatile and says what is lost when the process is.
	ref, err := s.peek(d)
	if err != nil {
		return err
	}
	// The delivery is TERMINAL from the point of view the moment this
	// returns nil: the caller's settlement succeeded, so a second one has to
	// be refused with ErrAlreadySettled rather than reaching the broker
	// beside the hold. What is still OWED is the FRAME, which is what `held`
	// says — nackNow writes one for a delivery that is settled but held.
	//
	// Leaving the delivery active was the shape every other settlement route
	// here avoided and this one had: a direct caller could negatively settle
	// with a delay and then acknowledge the same delivery, and the broker
	// would see both.
	if !ref.claim() {
		return stateError(ref.state.Load())
	}
	if !ref.held.CompareAndSwap(false, true) {
		return mq.ErrSettlementInProgress
	}
	s.src.hold(ref, delay)
	return nil
}

// deadLetter routes the delivery to the configured dead-letter topic.
//
// The route is pulsar-client-go's own: ReconsumeLater with a reconsume count
// already past MaxDeliveries goes straight to the dead-letter router, which
// publishes the message to Config.DeadLetterTopic and acknowledges the original
// only when that publication succeeded. Forcing the count is what makes an
// EXPLICIT Reject reach the same route a message exhausted by redelivery would.
func (s *settler) deadLetter(d *mq.DriverDelivery) error {
	if !s.src.b.cfg.HasDeadLetter() {
		return errNoDeadLetter
	}
	ref, err := s.claim(d)
	if err != nil {
		return err
	}
	s.src.cons.ReconsumeLaterWithCustomProperties(ref.msg, map[string]string{
		pulsar.SysPropertyReconsumeTimes: strconv.FormatUint(uint64(s.src.b.cfg.MaxAttempts), 10),
	}, 0)
	s.src.b.deadLetter.Add(1)
	s.src.forget(ref)
	return nil
}

// Extend moves d's lease deadline.
//
// The lease is this driver's own — see [Config.AckTimeout] — so an extension is
// a store to a deadline in this process and never a frame. That is why the
// capability table advertises FeatureExtendLease as EmulatedVolatile: the
// extension buys the handler time against the DRIVER's sweeper and nothing
// else, and it is lost with the process.
//
// An extension for a delivery this source no longer holds is refused with
// ErrLeaseLost and nothing is written. It is not a formality: the caller would
// otherwise keep working on a message a later delivery now owns, and its
// eventual acknowledgement would be refused after the work was done.
func (s *settler) Extend(ctx context.Context, d *mq.DriverDelivery, by time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.src.lease <= 0 {
		return errNoLease
	}
	if by <= 0 {
		// An extension of zero or less is not an extension.
		return errExtendZero
	}
	ref, err := s.peek(d)
	if err != nil {
		return err
	}
	ref.extend(s.src.b.now().Add(by))
	return nil
}

// AckBatch settles every delivery in ds positively, in one frame.
//
// Consumer.AckIDList is the native verb, and the *BatchError this returns is
// index-aligned with ds: a result aligned with anything else — the failures
// only, or the identifiers the client accepted — reports one delivery's outcome
// for another.
func (s *settler) AckBatch(ctx context.Context, ds []*mq.DriverDelivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ds) == 0 {
		return nil
	}
	errs := make([]error, len(ds))
	refs := make([]*delivery, len(ds))
	ids := make([]pulsar.MessageID, 0, len(ds))
	for i, d := range ds {
		ref, err := s.claim(d)
		if err != nil {
			errs[i] = err
			continue
		}
		refs[i] = ref
		ids = append(ids, ref.id)
	}
	if len(ids) == 0 {
		return batchResult(errs)
	}
	err := s.src.cons.AckIDList(ids)
	var perID pulsar.AckError
	if ae, ok := err.(pulsar.AckError); ok { //nolint:errorlint // AckError is a map type the library returns by value, not a wrapped error
		perID = ae
	}
	for i, ref := range refs {
		if ref == nil {
			continue
		}
		one := err
		if perID != nil {
			one = perID[ref.id]
		}
		errs[i] = s.finish(ref, one, mq.OpAck)
	}
	return batchResult(errs)
}

// NackBatch settles every delivery in ds negatively under o.
//
// It is a LOOP, and saying so is the point: Pulsar has no batched negative
// acknowledgement. The core asks for one surface and gets the per-delivery
// verbs behind it, with the same index-aligned result, rather than a coalescing
// layer that would advertise a round trip this broker does not have.
func (s *settler) NackBatch(ctx context.Context, ds []*mq.DriverDelivery, o mq.NackOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ds) == 0 {
		return nil
	}
	errs := make([]error, len(ds))
	for i, d := range ds {
		errs[i] = s.Nack(ctx, d, o)
	}
	return batchResult(errs)
}

// finish records the outcome of a settlement that has been claimed.
//
// the three answers, and the middle one is the one a driver gets wrong: a
// DEFINITE refusal leaves the delivery settleable, so the state goes back to
// active and the caller may try again; an AMBIGUOUS outcome is sticky, because
// the broker may have taken it and a second attempt could settle a later
// redelivery.
func (s *settler) finish(ref *delivery, err error, op string) error {
	if err == nil {
		s.src.forget(ref)
		return nil
	}
	mapped := s.src.b.errs.MapNet(op, s.src.iqTopic, s.src.sub, err, true)
	if mq.IsAmbiguous(mapped) || mq.CodeOf(mapped) == mq.CodeSettlementUnknown {
		ref.state.Store(stateUnknown)
		return mapped
	}
	ref.state.CompareAndSwap(stateSettled, stateActive)
	return mapped
}

// claim takes a delivery terminal, applying every local check before anything
// is written.
//
// The order is the order of the reasons. The generation and the supersession
// come first, because a delivery this source no longer vouches for must report
// ErrLeaseLost whatever its deadline says; then the deadline; then the batch
// rules, because an acknowledgement that would settle a sibling must be refused
// before the state machine has taken the delivery; and only then the state
// machine, because a second settlement of a delivery that IS still ours is ErrAlreadySettled and not a lost lease.
func (s *settler) claim(d *mq.DriverDelivery) (*delivery, error) {
	ref, err := s.peek(d)
	if err != nil {
		return nil, err
	}
	if err := s.src.rules.Check(ref.pos); err != nil {
		return nil, err
	}
	if !ref.claim() {
		return nil, stateError(ref.state.Load())
	}
	return ref, nil
}

// peek recovers a delivery's bookkeeping and applies the lease check without
// changing its state, which is what an extension and a delayed hold need.
func (s *settler) peek(d *mq.DriverDelivery) (*delivery, error) {
	if d == nil {
		return nil, mq.ErrNotSettleable
	}
	ref, ok := d.Ref.(*delivery)
	if !ok || ref == nil || ref.src != s.src {
		// A delivery whose Ref is not one of this source's came from
		// another source or from a zero DriverDelivery. The specification makes that
		// ErrNotSettleable rather than a panic — and it is also the
		// structural half of the batch guard: a caller cannot hand this
		// settler an identifier of its own making.
		return nil, mq.ErrNotSettleable
	}
	if ref.gen != d.Gen {
		// The core hands back its own copy of the struct, so the
		// generation in the Ref and the one in the delivery are the same
		// value written once. A mismatch is a delivery that was tampered
		// with, and applying a settlement from it would settle a message
		// this source cannot vouch for.
		return nil, mq.ErrLeaseLost
	}
	if state := ref.state.Load(); state != stateActive {
		return nil, stateError(state)
	}
	if ref.pastGuard(s.src.b.now(), s.src.guard) {
		// Marked and refused, and nothing is written: the local deadline
		// has passed, so the sweeper has given the message back or is
		// about to, and a frame now would land on whatever the message
		// becomes next.
		if !ref.lose() {
			// A racer settled it between the state check above and
			// here, which is ErrAlreadySettled and not a lost
			// lease.
			return nil, stateError(ref.state.Load())
		}
		s.src.b.stale.Add(1)
		return nil, mq.ErrLeaseLost
	}
	return ref, nil
}

// The settler's refusals.
var (
	errNoDeadLetter = errcode.New(errcode.Unsupported,
		"iqmq/pulsar: no Config.DeadLetterTopic is configured, so there is nowhere for a rejected message to go; the specification forbids a dead-letter settlement that destroys the message instead. Configure one, or use mq.WithDeadLetter, which publishes to a topic and then acknowledges")
	errNegativeNackDelay = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: a negative-settlement delay cannot be negative")
	errNackDelayTooLong  = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: the negative-settlement delay is longer than pulsar.MaxDelay")
	errNackAction        = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: the negative-settlement action is not one of the three")
	errNoLease           = errcode.New(errcode.Unsupported,
		"iqmq/pulsar: this client grants no delivery lease, because Config.DisableAckTimeout was set or the consumer asked for leases that never expire; Pulsar's broker redelivers an unsettled message when the consumer disconnects and on no timer, so there is no deadline to extend")
	errExtendZero = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: an extension of zero or less is not an extension")
)
