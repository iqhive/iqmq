package amqp10

import (
	"context"
	"sync"
	"sync/atomic"

	amqp "github.com/Azure/go-amqp"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/drivers/amqp10/internal/client"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// The compile-time proof of the source's surfaces. It hands out a settler and
// nothing else: there is no seek, because AMQP 1.0 has no position to seek to.
var _ mq.Source = (*Source)(nil)

// tokenHeadroom is how much bigger the token table is than the prefetch.
//
// The credit window bounds the deliveries this source HOLDS at the prefetch, so
// a table of exactly that size is nearly right. What makes it not quite right is the sticky unknown: a delivery whose settlement was ambiguous keeps its slot
// for ever, because a slot reused under a token whose fate nobody knows is how an
// unknown settlement becomes a successful one by accident. The headroom is what
// keeps a handful of those from making the table itself the bound, so that the
// condition is reported by the credit window — which has a name for it and a
// counter — rather than by a table that is simply full.
const tokenHeadroom = 4

// Source is one open subscription: the receiving link, the credit window that
// makes the prefetch real, the token table a settlement resolves through, and
// the per-slot storage that makes a fetch allocate nothing.
//
// It is exported for the reason [Sink] is: a broker-specific module has to be
// able to name it, and the critical tests live in another package.
type Source struct {
	linkHolder
	b            *Backend
	topic        string
	subscription string
	address      string
	mapper       *Mapper
	table        *driverkit.TokenTable
	window       *creditWindow
	set          *settler

	link *receiverLink

	// msgs is one iqmq message per token slot.
	//
	// A fetch fills dst with pointers into this array, which is what makes
	// Fetch allocate nothing per call: the core copies the Message VALUE out
	// before it reuses dst, and the Body and the header storage the value
	// points at belong to the delivery and stay valid until it is settled
	// The slot is not reused until its delivery is released, which is
	// exactly the token table's own lifetime — so the two share one index and
	// cannot disagree.
	msgs []mq.Message

	// live is the held delivery currently occupying each token slot, and it
	// is what makes a close able to hand back what the source is holding.
	//
	// [bitbucket.org/iqhive/iqmq/driverkit.TokenTable] offers no iterator over
	// its live slots and no accessor for a slot's generation, so a token
	// cannot be reconstructed from a slot index alone — and a close that
	// guessed would settle the wrong delivery or none. Keeping the tokens
	// here is the same shape drivers/jetstream needed for its stream-sequence
	// index; an iterator or a Sweep on the table would retire both, and that
	// is recorded as this increment's finding about the kit.
	hmu  sync.Mutex
	live []*held

	// dlLink is the sending link a dead-letter route uses, attached lazily on
	// the first rejection. It has a lock of its own so that a rejection does
	// not contend with a fetch's attach.
	dlmu   sync.Mutex
	dlLink *senderLink

	// fetchMu makes one Fetch at a time, which is the specification's "one active Receive
	// per consumer unless the driver documents otherwise". Settlement is NOT
	// under it: the specification requires a settler to be callable concurrently with a
	// fetch, and a settlement is what frees the room a parked fetch is
	// waiting for.
	fetchMu sync.Mutex

	closing chan struct{}

	// The per-source diagnostics. Each counts something the portable API
	// cannot see.
	stale       atomic.Uint64
	undecodable atomic.Uint64

	// The hot-path errors the specification asks to be preallocated per handle.
	errClosed *mq.OpError
}

// held is what a delivery needs in order to be settled later.
//
// It is the [bitbucket.org/iqhive/iqmq.DriverDelivery.Ref] of every delivery this
// source hands out, and it is allocated once per delivery — which is what the
// SPI's boxed Ref exists for and is unavoidable here, because an AMQP
// disposition needs the library's own *amqp.Message and the receiver it arrived
// on.
type held struct {
	msg *amqp.Message
	rcv client.Receiver
	// gen is the connection generation the delivery's link belonged to. It is
	// what makes a disposition for a detached or reconnected link detectable
	gen   uint32
	token uint64
}

// newSource creates the source for one subscription.
//
// The link is NOT attached here. An attach needs the caller's context and a
// source that attached at open would have to decide what to do about a broker
// that is down at that instant — where the answer the specification wants is that the first
// Fetch reports it, retryably, and the next one succeeds.
func (b *Backend) newSource(cfg mq.SourceConfig) (*Source, error) {
	if cfg.Prefetch <= 0 {
		return nil, errUnresolvedPrefetch
	}
	switch cfg.Start.Resolve().Kind() {
	case mq.StartKindLatest, mq.StartKindEarliest:
		// A receiving link attaches to an address and reads what is there. A
		// queue has no history and no position, so "latest" and "earliest"
		// are the same place — which is honest rather than a silent
		// approximation, because the alternative would be to refuse the
		// default position every consumer gets by not choosing.
	case mq.StartKindTime, mq.StartKindSequence:
		return nil, errNoSeek
	default:
		return nil, errBadStartPosition
	}
	address, err := b.profile.SourceAddress(cfg.Topic, cfg.Subscription)
	if err != nil {
		return nil, err
	}
	mapper, err := NewMapper(b.profile, cfg.Metadata, b.caps, !b.cfg.NonDurable)
	if err != nil {
		return nil, err
	}
	credit := cfg.Prefetch
	if b.cfg.MaxCredit > 0 && credit > b.cfg.MaxCredit {
		credit = b.cfg.MaxCredit
	}
	window, err := newCreditWindow(credit)
	if err != nil {
		return nil, err
	}
	table, err := driverkit.NewTokenTable(credit + tokenHeadroom)
	if err != nil {
		return nil, err
	}
	s := &Source{
		b:            b,
		topic:        cfg.Topic,
		subscription: cfg.Subscription,
		address:      address,
		mapper:       mapper,
		table:        table,
		window:       window,
		msgs:         make([]mq.Message, table.Cap()),
		live:         make([]*held, table.Cap()),
		closing:      make(chan struct{}),
		errClosed: &mq.OpError{
			Driver: b.profile.Driver, Operation: mq.OpFetch,
			Topic: cfg.Topic, Subscription: cfg.Subscription,
			Code: mq.CodeClosed, Err: mq.ErrClosed,
		},
	}
	s.set = &settler{src: s}
	return s, nil
}

// Address is the AMQP address this source's link attaches its source to, which
// is what the profile's naming produced — and on most brokers the one place the
// subscription's name appears.
func (s *Source) Address() string { return s.address }

// CreditStats is this source's credit accounting.
func (s *Source) CreditStats() CreditStats { return s.window.stats() }

// StaleSettlements is how many settlements this source's guard refused with
// ErrLeaseLost.
func (s *Source) StaleSettlements() uint64 { return s.stale.Load() }

// Undecodable is how many deliveries this source released back to the broker
// because it could not represent them as an iqmq message.
func (s *Source) Undecodable() uint64 { return s.undecodable.Load() }

// Fetch blocks until at least one delivery is available, ctx is done or the
// source is closed, and fills dst(n].
//
// It is one blocking wait and a non-blocking drain: the first delivery comes
// from the link's own Receive and the rest from whatever the library has already
// buffered, so a fetch of a batch of a hundred costs one wait rather than a
// hundred. It allocates nothing per fetch — the messages come from per-slot
// storage and the deliveries go into the caller's dst.
//
// # Three things it will not do
//
// It will not hand out more deliveries than the credit window allows, because
// the credit it issues is the room it has; a window with no room WAITS for a
// settlement rather than blocking on a link that has been given no credit,
// which is what keeps a full window from being a deadlock (credit.go).
//
// It will not hand out a delivery it could not decode. Such a delivery is
// released back to the broker with `modified` — the attempt counted, this link
// marked as one not to redeliver to — and the fetch carries on, because a
// message iqmq cannot represent is one a caller can neither read nor settle, and
// handing it over would make it unsettleable.
//
// It will not report a lost connection as a closed source. A link that died
// returns its own error, mapped to CodeUnavailable and retryable, and the next
// Fetch attaches a new link under the new generation.
func (s *Source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	s.fetchMu.Lock()
	defer s.fetchMu.Unlock()
	for {
		if err := s.checkFetch(ctx); err != nil {
			return 0, err
		}
		if err := s.window.waitForRoom(ctx, s.closing); err != nil {
			return 0, s.fetchErr(err)
		}
		link, err := s.receiver(ctx)
		if err != nil {
			return 0, s.fetchErr(err)
		}
		if err := s.window.topUp(link.rcv); err != nil {
			if isLinkGone(err) {
				s.linkLost(err)
			}
			return 0, s.fetchErr(err)
		}
		m, err := link.rcv.Receive(ctx, nil)
		if err != nil {
			if s.isClosing() {
				// The specification: a Close unblocks a parked operation, and what it
				// unblocks it with is ErrClosed rather than the link error the
				// close itself caused.
				return 0, s.errClosed
			}
			// A link the peer detached is dropped here and re-attached by
			// the next fetch. Without this the generation check in
			// [Source.receiver] hands back the dead link for ever, because a
			// link detach does not bump the connection generation.
			if isLinkGone(err) {
				s.linkLost(err)
			}
			return 0, s.fetchErr(err)
		}
		n := 0
		if d, ok := s.take(ctx, link, m); ok {
			dst[0] = d
			n = 1
		}
		for n < len(dst) {
			if s.window.full() {
				break
			}
			m := link.rcv.Prefetched()
			if m == nil {
				break
			}
			if d, ok := s.take(ctx, link, m); ok {
				dst[n] = d
				n++
			}
		}
		if n > 0 {
			return n, nil
		}
		// Everything this wait produced was undecodable and has been released.
		// The loop is bounded by the peer's credit, and the next iteration
		// blocks, so it cannot spin.
	}
}

// warm attaches the receiving link and issues its credit without fetching.
//
// It takes `fetchMu` like Fetch does, so it cannot race a fetch, and it does
// the first two of Fetch's four steps and neither of the last two: the peer is
// told how much it may send and nothing is read. See [WarmForTest] for the one
// caller and why it exists.
func (s *Source) warm(ctx context.Context) error {
	s.fetchMu.Lock()
	defer s.fetchMu.Unlock()
	if err := s.checkFetch(ctx); err != nil {
		return err
	}
	link, err := s.receiver(ctx)
	if err != nil {
		return s.fetchErr(err)
	}
	if err := s.window.topUp(link.rcv); err != nil {
		if isLinkGone(err) {
			s.linkLost(err)
		}
		return s.fetchErr(err)
	}
	return nil
}

// take claims a slot for one native message and builds the delivery.
//
// It reports false for a message this source will not hand over, having already
// released it: the credit window was full (a peer that sent more than its
// credit), the token table was full, or the message could not be decoded.
func (s *Source) take(ctx context.Context, link *receiverLink, m *amqp.Message) (mq.DriverDelivery, bool) {
	if !s.window.took() {
		// The peer exceeded its credit. Trusting it would mean handing out a
		// delivery with no slot to settle it through, so it goes straight
		// back.
		s.release(ctx, link.rcv, m, false)
		return mq.DriverDelivery{}, false
	}
	h := &held{msg: m, rcv: link.rcv, gen: link.gen}
	token, ok := s.table.Claim(h)
	if !ok {
		s.window.freed()
		s.release(ctx, link.rcv, m, false)
		return mq.DriverDelivery{}, false
	}
	h.token = token
	slot := uint32(token)
	s.remember(h, slot)
	msg := &s.msgs[slot]
	msg.Topic = s.topic
	lossy, err := s.mapper.Decode(msg, m)
	if err != nil {
		// A message this driver cannot represent is released with the attempt
		// COUNTED and this link marked as one not to redeliver to. Releasing
		// it plainly would have the broker hand it straight back, and this
		// source would spin on it for ever; rejecting it would destroy or
		// dead-letter a message whose only fault is a body shape iqmq has no
		// field for. `modified` is the protocol's own answer to "I did not
		// process this, do not give it to me again".
		_ = s.table.Release(token)
		s.forget(h)
		s.window.freed()
		s.undecodable.Add(1)
		s.b.undecodable.Add(1)
		s.release(ctx, link.rcv, m, true)
		return mq.DriverDelivery{}, false
	}
	if lossy > 0 {
		s.b.lossyFields.Add(uint64(lossy))
	}
	return mq.DriverDelivery{
		Message: msg,
		Info:    s.deliveryInfo(m),
		Token:   token,
		Ref:     h,
		Native:  m,
		// The CONNECTION generation and not the slot's: the slot's is already
		// in the token, and what a settlement has to be checked against is the
		// link the delivery came from (links.go).
		Gen: link.gen,
	}, true
}

// deliveryInfo is the delivery metadata.
//
// LeaseExpires is the zero time, which the specification makes "the broker grants no lease":
// AMQP 1.0 really does not grant one — an unsettled delivery is this receiver's
// for as long as the link lives — so a driver that reported a deadline would be
// inventing one, and a handler that trusted it would be told its delivery had
// expired when it had not.
func (s *Source) deliveryInfo(m *amqp.Message) mq.DeliveryInfo {
	attempt := s.attemptOf(m)
	return mq.DeliveryInfo{
		Topic:        s.topic,
		Source:       s.address,
		Subscription: s.subscription,
		ReceivedAt:   s.b.now(),
		Attempt:      attempt,
		Redelivered:  attempt > 1,
	}
}

// attemptOf is the delivery attempt, from wherever the profile says the
// broker keeps it.
//
// The AMQP header's `delivery-count` counts REDELIVERIES and is zero on the
// first transfer, where Attempt is 1 on the first delivery — so the
// header's value plus one is the answer, and a driver that reported the header
// raw would say "attempt 0" for every first delivery, which the specification makes "the
// broker does not count attempts".
func (s *Source) attemptOf(m *amqp.Message) uint32 {
	switch s.b.profile.Attempt.Source {
	case AttemptNone:
		return 0
	case AttemptFromAnnotation:
		v, ok := annotationValue(m, s.b.profile.Attempt.Slot, s.b.profile.Attempt.Key)
		if !ok {
			return 0
		}
		n, ok := uintOf(v)
		if !ok {
			return 0
		}
		if !s.b.profile.Attempt.FirstIsOne {
			n++
		}
		return clampAttempt(n)
	case AttemptFromHeader, attemptSourceCount:
	}
	if m.Header == nil {
		// No header section at all means no redelivery count, which for a
		// first delivery is what the protocol's defaults say.
		return 1
	}
	return clampAttempt(uint64(m.Header.DeliveryCount) + 1)
}

// clampAttempt keeps an attempt count inside the uint32 without wrapping a
// peer's absurd value round to a small one.
func clampAttempt(n uint64) uint32 {
	if n > 1<<31 {
		return 1 << 31
	}
	return uint32(n)
}

// annotationValue reads one value out of the section a profile rule names.
func annotationValue(m *amqp.Message, slot Slot, key string) (any, bool) {
	switch slot {
	case SlotMessageAnnotation:
		v, ok := m.Annotations[key]
		if !ok {
			v, ok = m.Annotations[amqp.Symbol(key)]
		}
		return v, ok
	case SlotDeliveryAnnotation:
		v, ok := m.DeliveryAnnotations[key]
		if !ok {
			v, ok = m.DeliveryAnnotations[amqp.Symbol(key)]
		}
		return v, ok
	case SlotApplicationProperty:
		v, ok := m.ApplicationProperties[key]
		return v, ok
	case SlotNone, slotCount:
	}
	return nil, false
}

// uintOf reads an AMQP integer value as an unsigned count.
func uintOf(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint8:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint32:
		return uint64(x), true
	case uint64:
		return x, true
	case int8:
		return nonNegative(int64(x))
	case int16:
		return nonNegative(int64(x))
	case int32:
		return nonNegative(int64(x))
	case int64:
		return nonNegative(x)
	case int:
		return nonNegative(int64(x))
	default:
		return 0, false
	}
}

// nonNegative rejects a negative count rather than wrapping it.
func nonNegative(n int64) (uint64, bool) {
	if n < 0 {
		return 0, false
	}
	return uint64(n), true
}

// release hands a delivery back to the broker without a caller ever having seen
// it.
//
// modified says "count the attempt and do not give it to me again", which is
// what an undecodable message needs; released says "I did not act on this",
// which is what everything else needs. Both are best effort: a disposition that
// fails is a delivery the broker holds unsettled, and detaching the link returns
// it anyway.
func (s *Source) release(ctx context.Context, rcv client.Receiver, m *amqp.Message, undeliverable bool) {
	d := client.Disposition{Outcome: client.OutcomeReleased}
	if undeliverable {
		d = client.Disposition{
			Outcome:           client.OutcomeModified,
			DeliveryFailed:    true,
			UndeliverableHere: true,
		}
	}
	dctx, cancel := s.b.bound(ctx)
	defer cancel()
	_ = rcv.Dispose(dctx, m, d)
}

// Settler settles this source's deliveries.
func (s *Source) Settler() mq.Settler { return s.set }

// Close releases the source, hands back everything it was holding, unblocks a
// parked Fetch and detaches the link. It is idempotent.
//
// The order is deliberate and each step depends on the one before it:
//
//  1. the closing flag and the credit window's close, so that a parked Fetch
//     and a parked wait for room both wake;
//  2. a `released` for every delivery the token table still calls outstanding,
//     on the LIVE link — this is the specification's "requeues or abandons everything it has
//     handed out", and it is what turns "when the link goes away" into "now";
//  3. whatever the library still holds prefetched, released too, because those
//     are deliveries the peer has spent credit on and nobody has seen;
//  4. the detach, which wakes the Fetch parked in Receive and, in AMQP 1.0,
//     returns every remaining unsettled delivery to the node — so the sweep
//     above is a promptness measure and the detach is the correctness backstop;
//  5. [bitbucket.org/iqhive/iqmq/driverkit.TokenTable.InvalidateAll], so that a
//     settlement arriving after all this reports ErrLeaseLost rather than being
//     applied to nothing.
//
// The sweep goes through [bitbucket.org/iqhive/iqmq/driverkit.TokenTable.Settle]
// and not through the table's contents alone, because that is the one call that
// can tell an unsettled delivery from one whose settlement is in flight: a
// release racing a caller's Ack would send the peer both and let the timing
// decide. A slot the call refuses is a slot somebody else owns, and it is left
// alone (the shape drivers/jetstream recorded as).
func (s *Source) Close() error {
	if !s.closeHolder() {
		return nil
	}
	close(s.closing)
	s.window.close()

	ctx, cancel := s.b.bound(context.Background())
	defer cancel()

	s.mu.Lock()
	link := s.link
	s.link = nil
	s.mu.Unlock()

	if link != nil {
		s.releaseOutstanding(ctx, link.rcv)
		for {
			m := link.rcv.Prefetched()
			if m == nil {
				break
			}
			s.release(ctx, link.rcv, m, false)
		}
	}
	err := detachReceiver(ctx, link)
	s.dlmu.Lock()
	dl := s.dlLink
	s.dlLink = nil
	s.dlmu.Unlock()
	if derr := detachSender(ctx, dl); derr != nil && err == nil {
		err = derr
	}
	s.table.InvalidateAll()
	s.b.forget(s, nil)
	if err != nil && !isTransportGone(err) {
		return s.b.errs.Map(mq.OpClose, s.topic, s.subscription, err)
	}
	return nil
}

// remember records which delivery occupies a slot.
func (s *Source) remember(h *held, slot uint32) {
	s.hmu.Lock()
	s.live[slot] = h
	s.hmu.Unlock()
}

// forget drops a delivery's slot record, for one that has been settled or
// handed back.
func (s *Source) forget(h *held) {
	if h == nil {
		return
	}
	slot := uint32(h.token)
	s.hmu.Lock()
	if int(slot) < len(s.live) && s.live[slot] == h {
		s.live[slot] = nil
	}
	s.hmu.Unlock()
}

// outstanding is every delivery this source has handed out and nobody settled.
func (s *Source) outstanding() []*held {
	s.hmu.Lock()
	defer s.hmu.Unlock()
	out := make([]*held, 0, len(s.live))
	for i, h := range s.live {
		if h != nil {
			out = append(out, h)
			s.live[i] = nil
		}
	}
	return out
}

// releaseOutstanding hands back every delivery this source has handed out and
// nobody settled.
//
// The sweep goes through [bitbucket.org/iqhive/iqmq/driverkit.TokenTable.Settle]
// rather than releasing what it finds, because that is the one call that can
// tell an unsettled delivery from one whose settlement is in flight: a release
// racing a caller's Ack would send the peer both and let the timing decide. A
// slot the call refuses is a slot somebody else owns, and it is left alone.
func (s *Source) releaseOutstanding(ctx context.Context, rcv client.Receiver) {
	for _, h := range s.outstanding() {
		ref, err := s.table.Settle(h.token)
		if err != nil {
			continue
		}
		if got, ok := ref.(*held); ok && got == h {
			s.release(ctx, rcv, h.msg, false)
		}
		_ = s.table.Complete(h.token, nil)
		s.window.freed()
	}
}

// connectionLost invalidates everything this source was holding.
//
// It is step 2 of the sequence in reconnect.go and the ONLY step that has to
// happen before anything else: from the instant the connection died, every
// delivery this source handed out is stale, and a settlement carrying one must
// be refused rather than applied to whatever takes its place. The
// invalidation is what makes that true, and it is why the generation is bumped
// first.
//
// The credit window is reset with the table, because credit belongs to a link
// and the link is gone: keeping the old numbers would make the new link's
// window smaller by however much the old one was holding.
func (s *Source) connectionLost(cause error) {
	// The invalidation takes no lock of this source's, and that ordering is
	// the point: an attach in flight holds s.mu for as long as the peer takes
	// to answer, and a guard that waited for it would leave a window in which
	// a settlement for a dead link could still be written.
	n := s.table.InvalidateAll()
	s.window.reset()
	s.clearLive()
	if n > 0 {
		s.stale.Add(uint64(n))
		s.b.stale.Add(uint64(n))
	}
	s.mu.Lock()
	s.link = nil
	s.mu.Unlock()
	s.dlmu.Lock()
	s.dlLink = nil
	s.dlmu.Unlock()
	// The cause is the supervisor's own record, and the generation is the
	// backend's: a source reads the current one when it settles rather than
	// keeping a copy that could go stale.
	_ = cause
}

// linkLost is [Source.connectionLost] for a link that died on a connection
// that did not.
//
// The work is identical -- every delivery this source handed out came from the
// link that is gone, so every one of them is stale and its settlement must be
// refused, and the credit window belongs to the link and has to start
// again -- and the CAUSE is different, which is why it has a name of its own:
// nothing bumps the connection generation here, because the generation is the
// connection's and bumping it would invalidate every other link's deliveries
// as well. What makes the refusal work without a generation bump is that a
// held delivery carries its own receiver, so a disposition for a dead link's
// delivery is written to the dead link and refused there rather than landing
// on whatever delivery the reattached link now holds.
func (s *Source) linkLost(cause error) { s.connectionLost(cause) }

// clearLive drops every slot record, for a connection whose deliveries have all
// gone back to the node.
func (s *Source) clearLive() {
	s.hmu.Lock()
	for i := range s.live {
		s.live[i] = nil
	}
	s.hmu.Unlock()
}

// receiver returns this source's link, attaching one when there is none or when
// the one it has belongs to an older connection.
//
// The generation check and the attach happen under ONE lock, for the reason
// links.go gives: two callers racing on a reconnected backend would each attach
// a link and orphan one, and an orphaned RECEIVING link is worse than an
// orphaned sending one — the peer fills it with deliveries nobody will ever
// settle.
func (s *Source) receiver(ctx context.Context) (*receiverLink, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, s.errClosed
	}
	_, gen, err := s.b.connection()
	if err != nil {
		return nil, err
	}
	if s.link != nil && s.link.gen == gen {
		return s.link, nil
	}
	old := s.link
	s.link = nil
	actx, cancel := s.b.bound(ctx)
	defer cancel()
	if old != nil {
		_ = detachReceiver(actx, old)
	}
	link, err := s.b.attachReceiver(actx, s.address, s.receiverOptions())
	if err != nil {
		return nil, err
	}
	s.link = link
	return link, nil
}

// receiverOptions are the attach's own.
//
// Credit is -1, which is go-amqp's MANUAL credit: the library issues nothing
// and this driver's credit window issues exactly the prefetch. credit.go says
// why that is not a preference.
//
// Filters come from the driver's own [ConsumerOptions], which is the only way a
// selector can reach the attach: the portable API's ConsumerConfig has no field
// for one, and a filter is the broker's own syntax.
//
// What it does NOT pass is [Config.SenderSettle]. That field is about the mode
// this driver's own SENDING links offer, and using it here to request a mode of
// the BROKER's sending link conflates two different things — a receiving link
// asks the peer how it will settle, and the answer this driver needs is always
// the same one.
//
// `unsettled` is that answer, and requesting nothing else is what makes the
// attach work at all. The explicit settlement requires every transfer to
// arrive unsettled: a PRE-SETTLED transfer cannot be acknowledged, because the
// sender has already forgotten it, so a `mixed` request lets the broker hand
// over deliveries this driver cannot settle. RabbitMQ 4 makes the point
// bluntly by refusing the negotiation outright — go-amqp reports
//
//	amqp: sender settlement mode "mixed" requested, received "unsettled" from server
//
// and the attach fails, which failed 39 of 156 conformance scenarios for this
// one cause. Requesting the mode the driver actually needs fixes the
// negotiation and the semantics together.
func (s *Source) receiverOptions() *amqp.ReceiverOptions {
	opts := &amqp.ReceiverOptions{
		Credit:                    -1,
		SettlementMode:            s.b.cfg.ReceiverSettle.amqpMode(),
		RequestedSenderSettleMode: SenderSettleUnsettled.amqpMode(),
	}
	s.b.cfg.Consumer.apply(opts)
	return opts
}

// settleBound bounds a disposition's round trip, and only when there is one.
//
// Under the default [ReceiverSettleFirst] a disposition is a single frame: it is
// settled the moment it is written and the peer answers nothing, so there is no
// round trip for a deadline to bound. Wrapping the caller's context anyway would
// be a per-message context.WithTimeout, which rule 4 forbids outright —
// and it is not free: it was three of the thirteen allocations
// TestSourceFetchAllocatesNothing measured before this function existed, on the
// settlement path of every delivery.
//
// Under [ReceiverSettleSecond] the peer confirms the settlement, so there IS a
// round trip and it needs a bound of its own when the caller gave none: a
// settlement that waited for ever on a link that is up and silent is a handler
// that never returns. A caller who has a deadline keeps it either way.
func (s *Source) settleBound(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.b.cfg.ReceiverSettle.Resolve() != ReceiverSettleSecond {
		return ctx, func() {}
	}
	return s.b.bound(ctx)
}

// checkFetch is the guard every fetch starts with.
func (s *Source) checkFetch(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosing() {
		return s.errClosed
	}
	return nil
}

// fetchErr maps a fetch failure.
//
// sentAlready is false throughout: a fetch sends a flow frame and reads, so
// nothing a fetch does can have been accepted by the broker in a way a retry
// would duplicate. That is the other half of the distinction, and reporting
// a fetch as ambiguous would make a caller reconcile a read.
func (s *Source) fetchErr(err error) error {
	if mq.CodeOf(err) == mq.CodeClosed {
		return s.errClosed
	}
	return s.b.errs.MapNet(mq.OpFetch, s.topic, s.subscription, err, false)
}

// isClosing reports whether Close has begun.
func (s *Source) isClosing() bool {
	select {
	case <-s.closing:
		return true
	default:
		return false
	}
}

// The source's refusals.
var (
	errNoSeek = errcode.New(errcode.Unsupported,
		"iqmq/amqp10: AMQP 1.0 has no position to seek to; a receiving link attaches to an address and reads what is there, so a start position of a time or a sequence cannot be honoured")
	errBadStartPosition = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: the start position is not one of the four")
)
