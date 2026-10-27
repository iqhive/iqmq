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
// All three surfaces are here, and both halves of the batch are loops. The
// positive half cannot use Consumer.AckIDList, which drops failures (see
// [settler.AckBatch]), and Pulsar has no batched negative acknowledgement at
// all. The driver page says so rather than presenting a loop as a round trip.
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
//     HOLD when it is not, or when the subscription is Exclusive or Failover
//     and would deliver the retry topic's message at once, which is the
//     volatile emulation and is declared as one in the capability table.
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
		return s.deadLetter(ctx, d)
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
	// The retry topic's delay is a DeliverAfter, which only the Shared and
	// Key_Shared dispatchers honour, so an Exclusive or Failover
	// subscription takes the hold below whether or not the retry topic is
	// enabled. It used to take the retry topic, whose message then came
	// straight back, and a handler asking for a delay ran through every
	// attempt at once and on to the dead-letter topic.
	if s.src.b.cfg.RetryEnable && !s.src.b.cfg.SubscriptionType.Exclusive() {
		ref, err := s.claim(d)
		if err != nil {
			return err
		}
		// The republication and the acknowledgement of the original are
		// the client's retry router's, and it acknowledges the original
		// only after the republication has succeeded — so a failed
		// republication never loses the message.
		s.src.cons.ReconsumeLaterWithCustomProperties(ref.msg, carriedAttempts(ref.msg), delay)
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

// carriedAttempts is the reconsume count a delayed requeue hands the retry
// router, so that the deliveries the broker counted survive the hop.
//
// The router stores one more than the count it finds, and the broker's
// redelivery count starts again at zero on the retry topic, so the broker's
// count of the message being requeued was dropped: a message nacked once and
// then requeued with a delay reported attempts 1, 2 and then 2 again. Handing
// the router this delivery's attempt less one makes the next delivery's attempt
// one more than this one's. With no broker redelivery that is the count the
// router reads anyway, so nil leaves it alone.
//
// The router records the original topic and identifier only when it starts the
// count itself, and a count handed to it stops it doing so, so this records
// them the way the router would. REAL_TOPIC is what keeps the retry delivery
// reporting the topic the message was published to.
//
// The same count decides the dead-letter route, so every delivery now counts
// towards Config.MaxAttempts there, and not the hops alone.
func carriedAttempts(msg pulsar.Message) map[string]string {
	if msg.RedeliveryCount() == 0 {
		return nil
	}
	props := map[string]string{
		pulsar.SysPropertyReconsumeTimes: strconv.FormatUint(uint64(AttemptOf(msg))-1, 10),
	}
	if _, ok := msg.Properties()[pulsar.SysPropertyReconsumeTimes]; !ok {
		id := msg.ID().String()
		props[pulsar.SysPropertyRealTopic] = msg.Topic()
		props[pulsar.SysPropertyOriginMessageID] = id
		props[pulsar.PropertyOriginMessageID] = id
	}
	return props
}

// deadLetter routes the delivery to the configured dead-letter topic.
//
// The route is pulsar-client-go's own: ReconsumeLater with a reconsume count
// already past MaxDeliveries goes straight to the dead-letter router, which
// publishes the message to Config.DeadLetterTopic and acknowledges the original
// only when that publication succeeded. Forcing the count is what makes an
// EXPLICIT Reject reach the same route a message exhausted by redelivery would.
//
// The call is made on a goroutine of its own and waited for under ctx and the
// source's Close, because it can block for ever: it is a send on the router's
// unbuffered channel, the router retries a dead-letter producer it cannot
// create without end and takes nothing meanwhile, and a router that a
// concurrent Close has stopped takes nothing at all. Giving up reports
// ErrSettlementUnknown and leaves the delivery unknown, because the router may
// still take the message. The goroutine is the one leak the library forces: a
// send that never completes cannot be called back.
func (s *settler) deadLetter(ctx context.Context, d *mq.DriverDelivery) error {
	if !s.src.b.cfg.HasDeadLetter() {
		return errNoDeadLetter
	}
	ref, err := s.claim(d)
	if err != nil {
		return err
	}
	props := map[string]string{
		pulsar.SysPropertyReconsumeTimes: strconv.FormatUint(uint64(s.src.b.cfg.MaxAttempts), 10),
	}
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		s.src.cons.ReconsumeLaterWithCustomProperties(ref.msg, props, 0)
	}()
	select {
	case <-sent:
	case <-ctx.Done():
		ref.state.Store(stateUnknown)
		return errDeadLetterAbandoned
	case <-s.src.done:
		ref.state.Store(stateUnknown)
		return errDeadLetterClosed
	}
	s.src.b.deadLetter.Add(1)
	s.src.forget(ref)
	return nil
}

// Extend moves d's lease deadline to by from now, and never earlier than it
// already was.
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

// AckBatch settles every delivery in ds positively.
//
// It is a LOOP over Consumer.AckID and not Consumer.AckIDList, because
// pulsar-client-go v0.21's AckIDList cannot report a failure: it splits the
// identifiers by partition, keeps only the per-identifier AckError a partition
// consumer returns, discards every other error, and only logs an identifier
// whose partition consumer it cannot find. A batch that failed answered nil and
// every delivery in it was marked settled. Without AckWithResponse the client's
// grouping tracker still coalesces the frames, so the loop costs no round
// trips; with it, each acknowledgement is one.
func (s *settler) AckBatch(ctx context.Context, ds []*mq.DriverDelivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ds) == 0 {
		return nil
	}
	errs := make([]error, len(ds))
	for i, d := range ds {
		errs[i] = s.Ack(ctx, d)
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
		// Refused, and the message is given back here: the local deadline
		// has passed, and the sweeper gives back only a delivery that is
		// still active, so the one that marks it lost owes the broker the
		// negative acknowledgement. Without it nothing redelivers the
		// message until the consumer disconnects.
		if !ref.lose() {
			// A racer settled it between the state check above and
			// here, which is ErrAlreadySettled and not a lost
			// lease.
			return nil, stateError(ref.state.Load())
		}
		s.src.b.stale.Add(1)
		s.src.cons.NackID(ref.id)
		s.src.forget(ref)
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
	errDeadLetterAbandoned = errcode.New(errcode.SettlementUnknown,
		"iqmq/pulsar: the context ended before pulsar-client-go's dead-letter router took the message; the router may still publish it to Config.DeadLetterTopic and acknowledge the original, so the settlement's outcome is unknown")
	errDeadLetterClosed = errcode.New(errcode.SettlementUnknown,
		"iqmq/pulsar: the source closed before pulsar-client-go's dead-letter router took the message; the message may or may not have reached Config.DeadLetterTopic, so the settlement's outcome is unknown")
	errExtendZero = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: an extension of zero or less is not an extension")
)
