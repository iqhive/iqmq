package toydriver

import (
	"context"
	"errors"
	"sync"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
)

// The source implements the mandatory consumption surface, and its settler the
// mandatory settlement surface plus the optional lease extension.
//
// It is not a BatchSettler: this broker settles one record under one mutex, so
// a batch method would be the loop the core already writes and offering one
// would make the core skip its own adapter for no saving.
var (
	_ mq.Source           = (*source)(nil)
	_ mq.Settler          = (*settler)(nil)
	_ mq.DeadlineExtender = (*settler)(nil)
)

// source is one open subscription.
type source struct {
	b     *Broker
	topic string
	sub   *subState
	spec  TopicSpec
	st    *settler

	// lease is how long a delivery from this source lives before the
	// broker takes it back, resolved at open. Zero means leases never
	// expire, which the specification spells as a zero LeaseExpires.
	lease time.Duration

	// msgs is the per-delivery message storage, allocated once at open and
	// reused for ever, which is what makes a fetch allocate nothing per
	// message. It matters for a second reason too: driverkit's envelope
	// source writes the DECODED message back through
	// DriverDelivery.Message, so a driver that pointed that field at its
	// own stored record would have the decoder overwrite the broker's copy.
	msgs []mq.Message

	once   sync.Once
	closed chan struct{}
}

// OpenSource opens a subscription.
//
// The subscription is created if it does not exist and is never deleted here:
// a subscription outlives its consumers, which is what makes a consumer group
// a group, and deleting it belongs to an admin surface this driver does not
// have.
//
// Three of the four start positions are refused, and that is the honest
// answer for a broker with no retention: there is no history to start at the
// beginning of, no message to seek to by time and no sequence to seek to. The
// conformance kit checks the refusal in both directions — a driver that
// accepted StartEarliest and then delivered nothing would look like a broker
// that had lost the history rather than one that never had it.
func (b *Broker) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if cfg.Subscription == "" {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, "", errNoSubscription)
	}
	switch cfg.Start.Resolve().Kind() {
	case mq.StartKindLatest:
	case mq.StartKindEarliest, mq.StartKindTime, mq.StartKindSequence, mq.StartKindUnset:
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, errNoRetention)
	}
	env, err := b.envelope(cfg.Metadata)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	t, ok := b.topics[cfg.Topic]
	if !ok {
		b.mu.Unlock()
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, errNoSuchTopic)
	}
	s, ok := t.subs[cfg.Subscription]
	if !ok {
		s = newSub(cfg.Subscription)
		t.subs[cfg.Subscription] = s
	}
	spec := t.spec
	b.mu.Unlock()

	lease := cfg.AckDeadline
	if lease == 0 {
		lease = spec.AckDeadline
	}
	if lease < 0 {
		lease = 0
	}
	prefetch := max(cfg.Prefetch, 1)
	src := &source{
		b:      b,
		topic:  cfg.Topic,
		sub:    s,
		spec:   spec,
		lease:  lease,
		msgs:   make([]mq.Message, prefetch),
		closed: make(chan struct{}),
	}
	src.st = &settler{src: src}
	if env.Mode() == mq.MetadataNativeStrict || env.Mode() == mq.MetadataLenientDrop {
		return src, nil
	}
	// The envelope wrapper decodes each delivery on the way out of Fetch and
	// passes the settler through unchanged, extension included.
	return env.WrapSource(src), nil
}

// Fetch blocks until at least one delivery is available, ctx is done or the
// source is closed, and fills dst(n].
//
// The wait is a closed-channel broadcast and not a buffered signal, because
// several consumers of one subscription may be parked and a signal that wakes
// one of them loses the record another was waiting for. That is a lost
// wake-up, and it looks like a slow broker rather than a broken one.
func (src *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	n := min(len(dst), len(src.msgs))
	if n == 0 {
		return 0, nil
	}
	for {
		src.b.mu.Lock()
		if src.b.closed {
			src.b.mu.Unlock()
			return 0, mq.ErrClosed
		}
		// A fetch reclaims expired leases too, so that a broker whose
		// clock is only moved by Advance still hands a record back to
		// whoever asks for it after the move.
		src.b.sweepLocked()
		got := src.claimLocked(dst[:n])
		wake := src.sub.wake
		src.b.mu.Unlock()
		if got > 0 {
			return got, nil
		}
		select {
		case <-wake:
		case <-src.closed:
			return 0, mq.ErrClosed
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// claimLocked moves up to len(dst) ready records into flight and fills dst.
// The caller holds the broker's mutex.
func (src *source) claimLocked(dst []mq.DriverDelivery) int {
	now := src.b.now()
	var expires time.Time
	if src.lease > 0 {
		expires = now.Add(src.lease)
	}
	got := 0
	for got < len(dst) && len(src.sub.ready) > 0 {
		rec := src.sub.ready[0]
		src.sub.ready = src.sub.ready[1:]
		rec.attempts++
		// A new generation for every delivery and redelivery, which is
		// what makes a stale settlement detectable AT THE DRIVER: a
		// settlement carrying the old generation is answered with
		// ErrLeaseLost rather than applied to the delivery that took its
		// place.
		rec.gen++
		rec.lease = expires
		rec.holder = src
		src.sub.inflight[rec.id] = rec

		src.msgs[got] = *rec.msg
		dst[got] = mq.DriverDelivery{
			Message: &src.msgs[got],
			Info: mq.DeliveryInfo{
				Topic:        src.topic,
				Subscription: src.sub.name,
				ReceivedAt:   now,
				LeaseExpires: expires,
				Attempt:      rec.attempts,
				Redelivered:  rec.attempts > 1,
				Sequence:     rec.seq,
				HasSequence:  true,
			},
			// The handle a settlement is resolved through. A pointer in
			// an interface value is stored directly, so it costs no
			// allocation, and the core hands this struct back untouched.
			Ref: rec,
			Gen: rec.gen,
		}
		got++
	}
	if len(src.sub.ready) > 0 {
		// Another consumer of this group may still be parked on a record
		// this fetch did not take.
		src.sub.broadcastLocked()
	}
	return got
}

// Settler settles this source's deliveries. There is one per source
// and it may be called concurrently with Fetch.
func (src *source) Settler() mq.Settler { return src.st }

// Close releases the source, returns every delivery it handed out and never
// had settled, and unblocks a parked Fetch. It is idempotent.
func (src *source) Close() error {
	src.once.Do(func() {
		close(src.closed)
		src.b.mu.Lock()
		defer src.b.mu.Unlock()
		requeued := false
		for id, rec := range src.sub.inflight {
			if rec.holder != src {
				continue
			}
			delete(src.sub.inflight, id)
			rec.lease, rec.holder = time.Time{}, nil
			src.sub.ready = append([]*record{rec}, src.sub.ready...)
			requeued = true
		}
		if requeued {
			src.sub.broadcastLocked()
		}
	})
	return nil
}

// settler settles the deliveries of one source.
//
// It answers for the native action only: the core has already decided that
// this settlement is the one that may proceed, so nothing here keeps a second
// settlement state machine — which the specification forbids a driver from owning.
type settler struct{ src *source }

// Ack settles d positively: the record leaves the subscription.
func (st *settler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	return st.src.terminal(ctx, d, mq.OpAck)
}

// Nack settles d negatively under o.
//
// Three of the four actions are honoured and the fourth is refused, which is
// the contract: a driver that cannot honour o's action returns ErrUnsupported
// and settles NOTHING. A delayed requeue is the one refused here, and it is
// refused rather than silently requeued immediately, because "back in a
// minute" and "back at once" are different promises and only one of them is
// what the caller asked for.
func (st *settler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	switch o.Action {
	case mq.NackActionRequeue:
		if o.Delay > 0 {
			return st.src.b.errs.Map(mq.OpNack, st.src.topic, st.src.sub.name, errNoDelay)
		}
		return st.src.requeue(ctx, d)
	case mq.NackActionDrop:
		// The same native action an acknowledgement takes: the record
		// leaves the subscription and is never redelivered. The
		// difference between a drop and an ack is iqmq's own accounting.
		return st.src.terminal(ctx, d, mq.OpNack)
	case mq.NackActionDeadLetter:
		return st.src.deadLetter(ctx, d)
	default:
		return st.src.b.errs.Map(mq.OpNack, st.src.topic, st.src.sub.name, mq.ErrUnsupported)
	}
}

// Extend moves d's lease deadline to at least by from now.
func (st *settler) Extend(ctx context.Context, d *mq.DriverDelivery, by time.Duration) error {
	src := st.src
	if err := ctx.Err(); err != nil {
		return src.b.errs.Map(mq.OpExtend, src.topic, src.sub.name, err)
	}
	src.b.mu.Lock()
	defer src.b.mu.Unlock()
	rec, err := src.holdLocked(d)
	if err != nil {
		return src.b.errs.Map(mq.OpExtend, src.topic, src.sub.name, err)
	}
	if rec.lease.IsZero() {
		// There is no lease to extend, which is not a failure: the
		// delivery already lives until it is settled.
		return nil
	}
	rec.lease = src.b.now().Add(by)
	return nil
}

// terminal removes the record from the subscription for good.
func (src *source) terminal(ctx context.Context, d *mq.DriverDelivery, op string) error {
	if err := ctx.Err(); err != nil {
		return src.b.errs.Map(op, src.topic, src.sub.name, err)
	}
	src.b.mu.Lock()
	defer src.b.mu.Unlock()
	rec, err := src.holdLocked(d)
	if err != nil {
		return src.b.errs.Map(op, src.topic, src.sub.name, err)
	}
	delete(src.sub.inflight, rec.id)
	rec.holder = nil
	return nil
}

// requeue returns the record to the front of its subscription's queue.
func (src *source) requeue(ctx context.Context, d *mq.DriverDelivery) error {
	if err := ctx.Err(); err != nil {
		return src.b.errs.Map(mq.OpNack, src.topic, src.sub.name, err)
	}
	src.b.mu.Lock()
	defer src.b.mu.Unlock()
	rec, err := src.holdLocked(d)
	if err != nil {
		return src.b.errs.Map(mq.OpNack, src.topic, src.sub.name, err)
	}
	delete(src.sub.inflight, rec.id)
	rec.lease, rec.holder = time.Time{}, nil
	src.sub.ready = append([]*record{rec}, src.sub.ready...)
	src.sub.broadcastLocked()
	return nil
}

// deadLetter moves the record to the topic's dead-letter destination and then
// settles it.
//
// The order is what makes it safe to lose the process between the two steps: a
// move that failed leaves the delivery unsettled and redeliverable, which is the specification's "a failed or ambiguous dead-letter publish never acks". A topic that
// names no destination is refused with ErrUnsupported and NOTHING is settled,
// because destroying the message would be the one outcome the specification forbids.
func (src *source) deadLetter(ctx context.Context, d *mq.DriverDelivery) error {
	if err := ctx.Err(); err != nil {
		return src.b.errs.Map(mq.OpNack, src.topic, src.sub.name, err)
	}
	if src.spec.DeadLetterTopic == "" {
		return src.b.errs.Map(mq.OpNack, src.topic, src.sub.name, errNoDeadLetter)
	}
	src.b.mu.Lock()
	rec, err := src.holdLocked(d)
	if err != nil {
		src.b.mu.Unlock()
		return src.b.errs.Map(mq.OpNack, src.topic, src.sub.name, err)
	}
	msg := rec.msg
	src.b.mu.Unlock()

	if err := src.b.publish(ctx, src.spec.DeadLetterTopic, msg); err != nil {
		return err
	}
	return src.terminal(ctx, d, mq.OpNack)
}

// holdLocked recovers the record a delivery names and refuses a stale one. The
// caller holds the broker's mutex.
//
// A delivery whose generation is not the record's current one is a settlement
// of a delivery the broker has already taken back and redelivered, and
// applying it would acknowledge a message somebody else now owns. A
// delivery this driver never handed out is a different answer again: the specification
// gives the caller NotSettleable for a delivery that cannot be settled at all.
func (src *source) holdLocked(d *mq.DriverDelivery) (*record, error) {
	if d == nil {
		return nil, errNoHandle
	}
	rec, ok := d.Ref.(*record)
	if !ok || rec == nil {
		return nil, errNoHandle
	}
	held, inflight := src.sub.inflight[rec.id]
	if !inflight || held != rec || rec.gen != d.Gen {
		return nil, errStaleDelivery
	}
	return rec, nil
}

// The source's own native errors.
var (
	errNoSubscription      = errors.New("toy: a source needs a subscription name")
	errUnrepresentable     = errors.New("toy: this broker's headers are single-valued and UTF-8, so it cannot represent this message natively")
	errUnrepresentableSize = errors.New("toy: a field of the message exceeds this broker's limits")
)
