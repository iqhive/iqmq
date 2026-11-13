package jetstream

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	natsgo "github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// The source implements the mandatory consumption surface and hands out a
// settler, which is the whole difference between this driver and the Core one.
var _ mq.Source = (*source)(nil)

// tokenHeadroom is how much bigger the token table is than the prefetch.
//
// The prefetch bounds the deliveries the SERVER will leave unacked
// (MaxAckPending), so a table of exactly that size is nearly right. What makes
// it not quite right is the sticky unknown: a delivery whose settlement was
// ambiguous keeps its slot for ever, because a slot reused under a token whose
// fate nobody knows is how an unknown settlement becomes a successful one by
// accident. The headroom is what keeps a handful of those from stalling a
// consumer, and a table that does fill holds up the consume callback until a
// slot is free ([source.claimSlot]) rather than handing out a token that cannot
// be told from another delivery's.
const tokenHeadroom = 4

// source is one open subscription: the durable pull consumer, the bounded ring
// the consume callback pushes into, the token table a settlement resolves
// through, and the stream-sequence index that makes a stale ack detectable.
type source struct {
	b            *Backend
	consumer     js.Consumer
	consume      js.ConsumeContext
	push         *driverkit.PushAdapter
	table        *driverkit.TokenTable
	set          *settler
	topic        string
	subscription string
	filter       string
	durable      string
	prefix       string
	meta         metadataMode
	ackWait      time.Duration

	// backoff is the durable's BackOff, which the server uses in place of
	// AckWait when it has one (see [source.leaseOf]). It is empty for a
	// durable without one, which is every durable this driver creates.
	backoff []time.Duration

	// batch is how many messages one pull asks for (PullMaxMessages): the
	// prefetch, or the durable's MaxRequestBatch when that is smaller.
	// expiry is how long one pull waits (PullExpiry): zero for the client
	// library's default, or the durable's MaxRequestExpires when that is
	// shorter. Both are set before the consume starts ([source.bindDurable]).
	batch  int
	expiry time.Duration

	// arrivals is the consume callback's account of how long a message may
	// have waited in the client library. Only the callback touches it, and
	// nats.go calls the callback from one goroutine. Its prefetch is the
	// pull's batch, because that is what the library asks for.
	arrivals arrivals

	// reconnected is the connection's reconnection count when the source
	// opened, and repulls is how many times the consume loop has reported
	// pulling a whole prefetch again ([pullsAgain]). Together they are how
	// often the client library may have asked for more than its own count
	// allows ([source.pulledAgain]).
	reconnected uint64
	repulls     atomic.Uint64

	// smu guards seqs, the index from a record's STREAM SEQUENCE to the token
	// of the delivery that currently owns it.
	//
	// This index is the stale-ack guard. JetStream acknowledges by reply
	// subject and the server's AckExplicit path deletes its pending entry by
	// stream sequence without looking at which delivery the reply came from,
	// so the reply subject of an expired delivery still works — and acking it
	// acknowledges the record a LATER delivery now owns. When a record
	// arrives again, the previous delivery's slot is released here, which
	// bumps its generation, and the settlement carrying the old token is
	// answered ErrLeaseLost instead of reaching the wire.
	smu  sync.Mutex
	seqs map[uint64]uint64

	// started and finished run the two halves of Close once each
	// ([source.startClose], [source.finishClose]).
	started, finished sync.Once

	// closing ends when Close begins. It is a context because the consume
	// callback's wait for a token slot ([source.claimSlot]) has to end with
	// it, and that wait takes one.
	closing context.Context
	stop    context.CancelFunc
}

// held is what a delivery needs in order to be settled later: the native
// message, the record's stream sequence, the token that names its slot and
// the local view of its lease.
//
// It is the [github.com/iqhive/iqmq.DriverDelivery.Ref] of every delivery this
// source hands out, and it is allocated once per delivery — which is what the
// SPI's boxed Ref exists for and is unavoidable here, because a JetStream ack is
// a reply subject rather than an integer.
type held struct {
	msg       js.Msg
	seq       uint64
	delivered uint64
	token     uint64

	// lease is how long the server leaves the record with this delivery
	// before it redelivers it, counted from the delivery's arrival and again
	// from every `+WPI`: the durable's AckWait, or its BackOff step for this
	// delivery ([source.leaseOf]).
	lease time.Duration

	// expires is when this source expects the server to redeliver the
	// record, as Unix nanoseconds: the lease from the delivery's arrival,
	// moved by every `+WPI`. It is atomic because Extend writes it from the
	// lease scheduler's goroutine while a settlement reads it from the
	// handler's.
	expires atomic.Int64
}

// newSource creates or binds the durable pull consumer and wires its callback
// into the ring.
//
// The consumer's shape is the plan's: FilterSubjects is the mapped subject or
// pattern, AckPolicy is explicit, and MaxAckPending is the prefetch — which is
// what makes the prefetch a real bound rather than a client-side buffer. The
// server will not deliver an unacked message past it, the ring holds at most the
// prefetch, and blocking in the callback therefore stops the server rather than
// filling memory.
func (b *Backend) newSource(ctx context.Context, cfg mq.SourceConfig, filter string, meta metadataMode) (*source, error) {
	policy, seq, at, err := deliverPolicy(cfg.Start)
	if err != nil {
		return nil, err
	}
	durable, err := Durable(b.cfg.ConsumerPrefix, cfg.Subscription, filter)
	if err != nil {
		return nil, err
	}
	ackWait := cfg.AckDeadline
	if ackWait <= 0 {
		ackWait = b.cfg.AckWait
	}
	src := &source{
		b:            b,
		topic:        cfg.Topic,
		subscription: cfg.Subscription,
		filter:       filter,
		durable:      durable,
		prefix:       b.cfg.SubjectPrefix,
		meta:         meta,
		ackWait:      ackWait,
		batch:        cfg.Prefetch,
		arrivals:     arrivals{prefetch: cfg.Prefetch},
		reconnected:  b.nc.Stats().Reconnects,
		seqs:         make(map[uint64]uint64, cfg.Prefetch),
	}
	// Not the caller's ctx: it bounds the open, and the source outlives it.
	src.closing, src.stop = context.WithCancel(context.WithoutCancel(ctx))
	if src.table, err = driverkit.NewTokenTable(cfg.Prefetch + tokenHeadroom); err != nil {
		return nil, errPrefetchTooLarge
	}
	if src.push, err = driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: cfg.Prefetch}); err != nil {
		return nil, err
	}
	src.set = &settler{src: src}

	cctx, cancel := b.bound(ctx)
	defer cancel()
	consumer, err := b.createOrBindConsumer(cctx, js.ConsumerConfig{
		Durable:           durable,
		Description:       "iqmq " + cfg.Subscription,
		FilterSubjects:    []string{filter},
		AckPolicy:         js.AckExplicitPolicy,
		AckWait:           ackWait,
		MaxAckPending:     cfg.Prefetch,
		MaxDeliver:        b.cfg.MaxDeliver,
		InactiveThreshold: b.cfg.ConsumerInactiveThreshold,
		DeliverPolicy:     policy,
		OptStartSeq:       seq,
		OptStartTime:      at,
		ReplayPolicy:      js.ReplayInstantPolicy,
	})
	if err != nil {
		_ = src.push.Close()
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	if err := src.bindDurable(consumer.CachedInfo(), cfg.Prefetch); err != nil {
		_ = src.push.Close()
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	src.consumer = consumer
	pull := []js.PullConsumeOpt{js.PullMaxMessages(src.batch), js.ConsumeErrHandler(b.consumeErrHandler(src))}
	if src.expiry > 0 {
		pull = append(pull, js.PullExpiry(src.expiry))
	}
	before, overLimit := b.nc.LastError(), b.overLimit.Load()
	consume, err := consumer.Consume(src.onMessage, pull...)
	if err != nil {
		_ = src.push.Close()
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	src.consume = consume
	if err := b.confirmConsume(ctx, src, before, overLimit); err != nil {
		_ = src.Close()
		return nil, err
	}
	return src, nil
}

// confirmConsume waits for the server to read the subscription and the first
// pull request that consumer.Consume has just written for src. It refuses the
// source with a retryable CodeResourceExhausted when the server refused the
// subscription because the connection holds as many as it allows, and with a
// definite CodeForbidden when it refused the subscription or the pull for
// permissions. before is the connection's last error, and overLimit the
// backend's count of the first kind of refusal, from before Consume.
//
// The server refuses either with an asynchronous -ERR and keeps the
// connection, and nats.go keeps it only as the connection's last error, so a
// consumer used to open on a subscription the server never made, or whose
// every pull it refused, and receive nothing. The -ERR comes before the PONG
// of the flush that follows. This is drivers/nats' confirmation of a
// subscription, with its limits ([Backend.refusedOverLimit]), and it costs a
// round trip while the connection is up and none while it is not: what
// Consume wrote then is sent when the connection comes back, and a refusal of
// it reaches health alone. A flush that fails leaves the consumer as it is.
//
// The subscription a permissions refusal is read against is any inbox of one
// token under the connection's inbox prefix, the shape nats.go gives a
// consume inbox (the replies to the driver's requests come to one of two),
// and the pull is any pull for this durable. Neither names the consumer, so a
// refusal of another consumer's inbox or pull made on the connection at the
// same moment is read as this one's, which permissions that refuse one inbox
// or one durable's pulls refuse it in any case.
func (b *Backend) confirmConsume(ctx context.Context, src *source, before error, overLimit uint64) error {
	if !b.nc.IsConnected() || b.flush(ctx) != nil {
		return nil
	}
	now := b.nc.LastError()
	if b.refusedOverLimit(ctx, now, before, overLimit) {
		return b.errs.Map(mq.OpOpen, src.topic, src.subscription, now)
	}
	if !errors.Is(now, natsgo.ErrPermissionViolation) || now == before { //nolint:errorlint // identity is what says the refusal is new
		return nil
	}
	refused := refusesSubscription(now, b.inboxes())
	for _, pattern := range b.apiPatterns("CONSUMER.MSG.NEXT." + b.cfg.Stream + "." + src.durable) {
		refused = refused || refuses(now, pattern)
	}
	if !refused {
		return nil
	}
	return b.errs.Map(mq.OpOpen, src.topic, src.subscription, now)
}

// refusedOverLimit reports whether the server refused a subscription because
// the connection held as many as it allows, after before and overLimit were
// read ([Backend.confirmConsume]); now is the connection's last error once
// the flush after the subscription has returned.
//
// The refusal names no subscription, and nats.go keeps the sentinel
// ErrMaxSubscriptionsExceeded itself as the last error, so a second refusal
// reads exactly as the first did. A last error that is new says so, and
// otherwise the count of these refusals the driver's error handler has heard
// does ([Backend.onAsyncError]). nats.go queues the handler's call before it
// reads the PONG but runs it on a goroutine of its own, so a count that has
// not moved yet is read again after a second round trip. A refusal of another
// subscription made at the same time is read as this one's, and on a
// borrowed connection whose error handler the caller has replaced, only a
// last error that is new says anything.
func (b *Backend) refusedOverLimit(ctx context.Context, now, before error, overLimit uint64) bool {
	if !errors.Is(now, natsgo.ErrMaxSubscriptionsExceeded) {
		return false
	}
	if now != before || b.overLimit.Load() != overLimit { //nolint:errorlint // identity is what says the refusal is new
		return true
	}
	return b.flush(ctx) == nil && b.overLimit.Load() != overLimit
}

// inboxes is the pattern of the inboxes nats.go subscribes a consume to:
// one token under the connection's inbox prefix, as nc.NewInbox makes them.
func (b *Backend) inboxes() string {
	if prefix := b.nc.Opts.InboxPrefix; prefix != "" {
		return prefix + ".*"
	}
	return natsgo.InboxPrefix + "*"
}

// createOrBindConsumer creates the durable this subscription maps to, or binds
// to the one that is already there and leaves it as it is.
//
// The existing durable's own configuration wins, which is what a durable
// subscription means, and a caller who needs different settings deletes the
// consumer or names another subscription. So the create is a create and never
// an update: the server refuses it for a durable that exists with any other
// configuration, and the refusal becomes a lookup. The create used to be a
// create-or-update, which the server takes whenever the deliver policy is the
// durable's own, so every open replaced an operator's BackOff, MaxDeliver,
// AckWait, MaxAckPending and InactiveThreshold with this driver's settings,
// and two deployments of one subscription rewrote the durable at each other's
// every open. Only a second consumer that asked for another start position
// was bound to the durable as it stood, because a deliver policy cannot
// change.
//
// A request the server refuses for permissions ends when the refusal arrives
// ([Backend.apiRequest]), and each request waits on its own, so a refused
// create still leaves time for the lookup: a user the server lets read a
// durable but not create one binds to the durable an operator made. The
// refused create waited out the whole bound and left the lookup none. A
// lookup that fails reports the create's refusal, except after a create the
// server refused because the durable exists: then the lookup's failure is
// the answer, a timeout or a lost connection that a retry can get past. A
// push durable under the name is refused with CodeInvalidConfig: this source
// pulls, and the lookup refuses to hand a push consumer to a puller.
func (b *Backend) createOrBindConsumer(ctx context.Context, cfg js.ConsumerConfig) (js.Consumer, error) {
	api := b.apiPatterns("CONSUMER.*." + b.cfg.Stream + "." + cfg.Durable)
	var consumer js.Consumer
	err := b.apiRequest(ctx, api, func(ctx context.Context) error {
		var err error
		consumer, err = b.js.CreateConsumer(ctx, b.cfg.Stream, cfg)
		return err
	})
	if err == nil {
		return consumer, nil
	}
	lookupErr := b.apiRequest(ctx, api, func(ctx context.Context) error {
		var err error
		consumer, err = b.js.Consumer(ctx, b.cfg.Stream, cfg.Durable)
		return err
	})
	switch {
	case lookupErr == nil:
		return consumer, nil
	case errors.Is(lookupErr, js.ErrNotPullConsumer):
		return nil, errDurablePush
	case errors.Is(err, js.ErrConsumerExists):
		return nil, lookupErr
	default:
		return nil, err
	}
}

// bindDurable takes what this source relies on from the durable the server
// holds, which is not what the source asked for when the durable was already
// there.
//
// The server redelivers at the durable's AckWait, or at its BackOff step for
// the delivery when it has a BackOff, so that is the lease a delivery
// reports, the most an Extend can grant and the end of the local lease a
// settlement is checked against. A source that kept its own reported a lease
// the server would not honour, and its guard let through a late ack the
// server would read as settling another delivery. The server leaves the
// durable's MaxAckPending unacknowledged deliveries outstanding, so the token
// table needs a slot for each: a table sized for a smaller prefetch filled up,
// naked every delivery past it, and the server sent each one straight back.
// An unlimited MaxAckPending bounds nothing on the server, so the table keeps
// the prefetch's size and is the bound itself: a delivery that finds it full
// holds up the consume callback until a slot is free ([source.claimSlot]).
// The durable's MaxDeliver needs nothing here, because only the server counts
// deliveries against it.
//
// The server refuses a pull that asks for more messages than the durable's
// MaxRequestBatch, or that waits longer than its MaxRequestExpires, and the
// consume loop would then pull and be refused for ever while the source
// received nothing. So a pull asks for no more than MaxRequestBatch and waits
// no longer than MaxRequestExpires. The ring and the token table keep their
// sizes; only each pull is smaller or shorter. A pull asks for no byte limit,
// which MaxRequestMaxBytes never refuses.
//
// A durable this source cannot work to is refused with CodeInvalidConfig,
// and left as it is:
//   - an AckPolicy other than explicit, because nothing this source does could
//     make it safe. Under AckPolicy all one ack settles every delivery before
//     it as well, the ones a handler is still working on and the ones another
//     source holds, and under none the server counts a message settled as
//     soon as it sends it;
//   - a filter other than the source's one subject. The durable's name hashes
//     the filter, so only a durable someone else made under that name can
//     have another one, and bound as it stood it handed this topic's handlers
//     another topic's messages;
//   - HeadersOnly, under which every message would reach its handler without
//     its body and be acknowledged as handled;
//   - priority groups, under which the server refuses every pull that names
//     no group, and this source's name none;
//   - a MaxRequestExpires below one second, the shortest pull the client
//     library makes.
//
// A push durable under the name is refused too, before this is reached
// ([Backend.createOrBindConsumer]).
func (s *source) bindDurable(info *js.ConsumerInfo, prefetch int) error {
	if info == nil {
		return nil
	}
	c := info.Config
	switch {
	case c.AckPolicy != js.AckExplicitPolicy:
		return errDurableAckPolicy
	case !filtersOnly(c, s.filter):
		return fmt.Errorf("%w (the durable filters %q and %q, the source %q)", errDurableFilter, c.FilterSubject, c.FilterSubjects, s.filter)
	case c.HeadersOnly:
		return errDurableHeadersOnly
	case len(c.PriorityGroups) > 0 || c.PriorityPolicy != js.PriorityPolicyNone:
		return errDurablePriority
	case c.MaxRequestExpires > 0 && c.MaxRequestExpires < time.Second:
		return errDurablePullExpires
	}
	if c.AckWait > 0 {
		s.ackWait = c.AckWait
	}
	s.backoff = slices.Clone(c.BackOff)
	if pending := c.MaxAckPending; pending > prefetch {
		table, err := driverkit.NewTokenTable(pending + tokenHeadroom)
		if err != nil {
			return errPrefetchTooLarge
		}
		s.table = table
	}
	if c.MaxRequestBatch > 0 && c.MaxRequestBatch < s.batch {
		s.batch = c.MaxRequestBatch
		s.arrivals.prefetch = s.batch
	}
	if c.MaxRequestExpires > 0 && c.MaxRequestExpires < js.DefaultExpires {
		s.expiry = c.MaxRequestExpires
	}
	return nil
}

// filtersOnly reports whether a durable's configuration filters exactly the
// one subject filter, in either of the two forms the server keeps.
func filtersOnly(c js.ConsumerConfig, filter string) bool {
	if c.FilterSubject != "" {
		return c.FilterSubject == filter && len(c.FilterSubjects) == 0
	}
	return len(c.FilterSubjects) == 1 && c.FilterSubjects[0] == filter
}

// leaseOf is the lease the server gives a record's delivered-th delivery.
//
// A durable with a BackOff is timed by it and not by AckWait, which the server
// sets to the first step: it redelivers delivery n BackOff[n-1] after sending
// it, or after its last `+WPI`, and every delivery past the end of the list
// gets the last step. A source that gave every delivery AckWait refused a
// valid ack on a redelivery whose step was longer, and an Extend of the lease
// it really had.
func (s *source) leaseOf(delivered uint64) time.Duration {
	if len(s.backoff) == 0 {
		return s.ackWait
	}
	return s.backoff[min(max(delivered, 1), uint64(len(s.backoff)))-1]
}

// onMessage is the consume callback. It blocks while the ring is full of live
// deliveries, which is what makes the ring a bound rather than a buffer, and
// on this broker the block reaches the server, because MaxAckPending is the
// same number. It blocks while the token table is full as well
// ([source.claimSlot]). The time it takes is what [arrivals] counts against
// the leases of the messages that waited behind it.
func (s *source) onMessage(m js.Msg) {
	start := s.b.now()
	s.handle(m, earliestArrival(&s.arrivals, start))
	countCallback(&s.arrivals, s.pulledAgain(), start, s.b.now())
}

// pulledAgain is how many times the client library may have asked for a
// whole prefetch without counting what it already held, since the source
// opened: once at every reconnection, and once at every re-pull its consume
// loop reports.
//
// The reconnections are the connection's own count and not the backend's
// reconnect handler's. A connection handed to [OpenWithClient] carries none of
// this driver's handlers, and the handler runs on the library's callback
// goroutine, which can fall behind the consume loop's re-pull. nats.go counts
// a reconnection under the connection's lock before it reports the connection
// back, so the count has moved before the re-pull is asked for. Reading it
// takes that lock once a callback, and while a reconnection is dialling, the
// callback waits for it: the time counts as callback time, which shortens the
// leases after it rather than lengthening them.
func (s *source) pulledAgain() uint64 {
	return s.b.nc.Stats().Reconnects - s.reconnected + s.repulls.Load()
}

// handle is one message of the consume callback, which may have reached the
// client library as early as since.
func (s *source) handle(m js.Msg, since time.Time) {
	if s.isClosing() {
		// A source that is closing requeues what it is handed rather than
		// dropping it: the nak makes the message deliverable again at once
		// instead of after the lease expires, which is the specification's "Close requeues
		// or abandons" done the way that loses the least time. One that
		// waited out its lease in the client library is dropped instead,
		// for [source.live]'s reason.
		if meta, err := m.Metadata(); err == nil && s.ranOut(m, meta, since) {
			s.b.superseded.Add(1)
			return
		}
		s.nak(m)
		return
	}
	meta, err := m.Metadata()
	if err != nil {
		// Not a JetStream message, or one with no reply subject: there is
		// nothing to settle it with and nothing the caller could do with it.
		// The specification makes it a background failure, so it goes to health; the
		// server redelivers it when its lease expires.
		s.b.onConsumeError(s, err)
		return
	}
	msg, err := s.decode(m)
	if err != nil {
		s.undecodable(m, meta, err, since)
		return
	}
	d, err := s.delivery(m, meta, msg, since)
	switch {
	case errors.Is(err, mq.ErrClosed):
		// Close began while it waited for a token slot, so it is requeued
		// as one arriving after Close is.
		s.nak(m)
		return
	case err != nil:
		// Its lease reached the guard while it waited for a token slot. It
		// is not naked, for the reason below.
		s.b.superseded.Add(1)
		return
	}
	if h := d.Ref.(*held); s.pastGuard(h, s.b.now()) {
		// It waited out its lease in the client library. It is not naked
		// for [source.live]'s reason: the server may already have handed
		// the record to another consumer.
		s.release(d.Token, h.seq)
		s.b.superseded.Add(1)
		return
	}
	if err := s.admit(d); err != nil {
		// The ring closed while it waited for room, which can have taken
		// the rest of its lease.
		s.release(d.Token, meta.Sequence.Stream)
		if s.pastGuard(d.Ref.(*held), s.b.now()) {
			s.b.superseded.Add(1)
			return
		}
		s.nak(m)
	}
}

// ranOut reports whether the lease of a message that can have reached the
// client library as early as since has reached the guard.
func (s *source) ranOut(m js.Msg, meta *js.MsgMetadata, since time.Time) bool {
	h := &held{msg: m, lease: s.leaseOf(meta.NumDelivered)}
	h.expires.Store(since.Add(h.lease).UnixNano())
	return s.pastGuard(h, s.b.now())
}

// admit puts a delivery in the ring, and waits for room only while the ring is
// full of deliveries that are still live.
//
// A ring full of dead ones is how a redelivery used to find it when nobody had
// fetched for a lease: the server sent every record again, the first
// redelivery superseded its own copy in the ring and then blocked behind it,
// and the rest waited in the client library with their leases running. So the
// dead deliveries are dropped first, as [source.compact] would have dropped
// them at the next Fetch.
func (s *source) admit(d mq.DriverDelivery) error {
	if ok, err := s.push.TryPush(d); ok || err != nil {
		return err
	}
	if s.evict() {
		if ok, err := s.push.TryPush(d); ok || err != nil {
			return err
		}
	}
	// context.Background is deliberate: the callback has no context of its
	// own, and Close wakes a parked Push with ErrClosed, which is what the specification
	// requires and what keeps this goroutine from outliving the source.
	return s.push.Push(context.Background(), d)
}

// evict drops the dead deliveries from the ring and reports whether there were
// any.
//
// The ring cannot take one delivery out of its middle, so it is emptied and
// the live ones go back in their order. Only the callback puts deliveries in,
// so they fit, and a Fetch that finds the ring empty meanwhile waits for them.
// One that cannot go back because the ring closed is given up the way a
// refused Push is.
func (s *source) evict() bool {
	now := s.b.now()
	dropped := false
	s.push.Drain(func(d *mq.DriverDelivery) {
		if !s.live(d.Token, now) {
			s.b.superseded.Add(1)
			dropped = true
			return
		}
		if ok, _ := s.push.TryPush(*d); !ok {
			h := d.Ref.(*held)
			s.release(d.Token, h.seq)
			s.nak(h.msg)
		}
	})
	return dropped
}

// decode maps one native message onto the message the caller is handed, under
// the subscription's metadata mode. It fails only when an envelope the mode
// requires cannot be unwrapped.
func (s *source) decode(m js.Msg) (*mq.Message, error) {
	msg := &mq.Message{}
	body, hdr := m.Data(), m.Headers()
	// No reply subject: a JetStream delivery's m.Reply() is the server's
	// $JS.ACK subject, the settler's address and not the application's. The
	// stream keeps no publisher reply-to, so the only one a delivery has is the
	// one the envelope carried.
	switch s.meta {
	case metaEnvelope:
		var raw mq.Message
		Decode(&raw, m.Subject(), "", hdr, body, s.prefix)
		if err := s.b.env.Unwrap(msg, &raw); err != nil {
			return nil, err
		}
		msg.Topic = raw.Topic
	case metaAdaptive:
		var raw mq.Message
		Decode(&raw, m.Subject(), "", hdr, body, s.prefix)
		// The marker and never the payload: a native body that happens to
		// be a valid envelope is a payload, and decoding it as metadata
		// would hand the application bytes nobody published.
		if Enveloped(hdr) {
			if err := s.b.env.Unwrap(msg, &raw); err != nil {
				return nil, err
			}
			msg.Topic = raw.Topic
		} else {
			*msg = raw
		}
	case metaStrict, metaDrop:
		Decode(msg, m.Subject(), "", hdr, body, s.prefix)
	}
	return msg, nil
}

// delivery wraps one decoded message in a DriverDelivery, claiming the token
// slot that makes it settleable, or waiting for one ([source.claimSlot]).
//
// Its lease runs from since, the earliest moment the message can have reached
// the client library ([arrivals]), which is also what it reports as received:
// the server's lease started when it sent the message, and the time it waited
// for the callback was part of it.
func (s *source) delivery(m js.Msg, meta *js.MsgMetadata, msg *mq.Message, since time.Time) (mq.DriverDelivery, error) {
	// One reading for every time: the lease scheduler extends by
	// LeaseExpires-ReceivedAt, and two readings made that a little longer
	// than the lease, which Extend refuses. The local lease is the one the
	// delivery reports.
	lease := s.leaseOf(meta.NumDelivered)
	h := &held{msg: m, seq: meta.Sequence.Stream, delivered: meta.NumDelivered, lease: lease}
	h.expires.Store(since.Add(lease).UnixNano())
	token, err := s.claimSlot(h)
	if err != nil {
		return mq.DriverDelivery{}, err
	}
	return mq.DriverDelivery{
		Message: msg,
		Info: mq.DeliveryInfo{
			Topic:        msg.Topic,
			Source:       meta.Stream,
			Subscription: s.subscription,
			ReceivedAt:   since,
			LeaseExpires: since.Add(lease),
			// NumDelivered is the server's own attempt count and it starts
			// at 1, which is exactly Attempt. A driver that counted
			// its own would be the second owner of a number the specification gives to
			// one layer.
			Attempt:     uint32(min(meta.NumDelivered, 1<<31)),
			Redelivered: meta.NumDelivered > 1,
			Sequence:    meta.Sequence.Stream,
			HasSequence: true,
		},
		Token:  token,
		Ref:    h,
		Native: m,
		Gen:    uint32(token >> 32),
	}, nil
}

// claim takes the token slot for one delivery, releasing the slot of the
// PREVIOUS delivery of the same record first.
//
// That release is the stale-ack guard, and the order is the whole of it: the old
// slot is freed — which bumps its generation — before the new one is claimed, so
// from this moment a settlement carrying the old token reports ErrLeaseLost
// rather than acknowledging the record this delivery now owns.
func (s *source) claim(h *held) (uint64, error) {
	s.smu.Lock()
	defer s.smu.Unlock()
	if old, ok := s.seqs[h.seq]; ok {
		_ = s.table.Release(old)
		delete(s.seqs, h.seq)
		s.b.redeliveries.Add(1)
	}
	token, ok := s.table.Claim(h)
	if !ok {
		return 0, errTableFull
	}
	h.token = token
	s.seqs[h.seq] = token
	return token, nil
}

// claimSlot is [source.claim] for the consume callback, which waits for a slot
// when the table is full instead of giving the message back.
//
// Giving it back was a nak, and on a durable with an unlimited MaxAckPending
// it spun: nothing on the server bounds what such a durable sends, so once
// the caller held the prefetch and the headroom was used, the server sent the
// naked message straight back and the source naked it again, tens of
// thousands of times a second. A callback that waits is the bound the server
// does not keep. The client library counts a message off only once the
// callback has returned from it, so it stops pulling a prefetch past the
// message that waits, unless it reconnects meanwhile or its consume loop
// pulls again ([arrivals]).
//
// A redelivery of a record this source holds never waits, because
// [source.claim] frees the previous delivery's slot first. The wait ends with
// a slot, with Close (ErrClosed), or when the message's lease reaches its guard
// (ErrLeaseLost): past the guard the delivery could not be settled, and the
// messages queued behind it include the redeliveries that free slots held by
// deliveries whose leases ran out. The time left is read on the driver's clock
// and waited out on the wall clock, which outside a test is the same one.
func (s *source) claimSlot(h *held) (uint64, error) {
	token, err := s.claim(h)
	if !errors.Is(err, errTableFull) {
		return token, err
	}
	for {
		// The lease first, so that a message Close finds past its guard is
		// not naked.
		now := s.b.now()
		if s.pastGuard(h, now) {
			return 0, mq.ErrLeaseLost
		}
		if s.isClosing() {
			return 0, mq.ErrClosed
		}
		ctx, cancel := context.WithTimeout(s.closing, leaseEnds(h).Add(-leaseGuard(h.lease)).Sub(now))
		token, ok, _ := s.table.ClaimOrWait(ctx, h)
		cancel()
		if ok {
			s.smu.Lock()
			h.token = token
			s.seqs[h.seq] = token
			s.smu.Unlock()
			return token, nil
		}
		// A release woke it, which is not a claim, or the wait's context
		// ended: the loop says which.
	}
}

// release frees a slot and drops its index entry, for a delivery that never
// reached the caller.
func (s *source) release(token, seq uint64) {
	s.smu.Lock()
	defer s.smu.Unlock()
	if cur, ok := s.seqs[seq]; ok && cur == token {
		delete(s.seqs, seq)
	}
	_ = s.table.Release(token)
}

// leaseGuard is how long before a delivery's local lease ends this source
// stops writing a settlement for it.
//
// The index catches a redelivery only when the server hands it back to this
// source. One it hands to another consumer of the same durable never arrives
// here, so the token is still live when this source's late ack comes, and the
// server reads that ack as settling the other consumer's delivery: if that
// one is then requeued, the message is gone. The lease is what refuses it.
// The local lease starts no later than the message can have reached the client
// library ([arrivals]), a little after the server started its own, and a
// settlement takes a little longer again to reach the server, so the guard is
// the slack for both trips. Refusing early costs a redelivery; accepting late
// costs the message. It is a fifth of the delivery's lease, and never more
// than [maxLeaseGuard].
func leaseGuard(lease time.Duration) time.Duration { return min(maxLeaseGuard, lease/5) }

// maxLeaseGuard bounds [leaseGuard] on a long lease, where a fifth would
// refuse seconds of settlements the server would still take.
const maxLeaseGuard = 500 * time.Millisecond

// leaseEnds is the local view of when the server redelivers h's record.
func leaseEnds(h *held) time.Time { return time.Unix(0, h.expires.Load()) }

// pastGuard reports whether h's lease ends within the guard of now, from
// which point no settlement of it is written.
func (s *source) pastGuard(h *held, now time.Time) bool {
	return !now.Add(leaseGuard(h.lease)).Before(leaseEnds(h))
}

// arrivals is the consume callback's account of how long the message it is
// handed may have waited in the client library first.
//
// nats.go hands a subscription's messages to the callback one at a time, and
// the callback blocks while the ring is full of live deliveries, or for an
// undecodable message's dead-letter PubAck. What arrives meanwhile queues in
// the library with the server's lease already running, so a lease started
// when the callback is handed the message ends after the server's, by as long
// as it waited, and the guard covers only a trip's worth of that. A late ack
// from this source then settled the copy another consumer of the durable
// held. So a message's lease starts at the earliest moment it can have
// reached the library: when the callback is handed it, less the time the
// callback spent on the messages that can have been ahead of it.
//
// Which messages can have been ahead of it is nats.go v1.55.0's pull
// accounting. The library asks for at most a prefetch (PullMaxMessages) more
// than it has counted off, and counts a message off only once the callback
// has returned from it. So a message arriving is one of at most a prefetch
// still counted, and at most a prefetch less one are ahead of it: none at a
// prefetch of one, where the next message is not asked for until the
// callback returns. Counting more cut the lease of a message pulled after a
// long callback, so a valid ack was refused, or the message was dropped
// without a nak when the callback had waited to the guard. The library also
// asks for a whole prefetch again without counting what it holds when it
// reconnects, and when its consume loop resets ([pullsAgain]). A reconnection
// re-arms its heartbeat monitor too, which pulls again every two heartbeats a
// blocked callback keeps it from reading. What each of those asked for past
// the count is never counted off, so the window is a prefetch less one, plus
// a prefetch for each ([source.pulledAgain]), and it never shrinks: nothing
// tells the driver when the extra requests are used up. So after the first
// such event a long callback cuts the lease of the fresh message after it
// again, and on a durable whose callback waits for token slots the cut grows
// with every event, until fresh messages are dropped at the guard.
// docs/drivers/jetstream.md sets out the cost.
//
// The window is kept exactly, as the last callbacks' durations in a ring and
// their sum. A gap of [idleGap] between one callback returning and the next
// starting is the library waiting for a message rather than handing over one
// it held, so nothing before the gap was ahead of anything after it and the
// sum starts again.
type arrivals struct {
	prefetch int
	last     time.Time
	took     []time.Duration
	n, next  int
	sum      time.Duration
}

// idleGap is the longest the library takes to hand the callback a message it
// already holds. Handing one over is a list pop under a mutex, which takes
// microseconds, so this allows for the goroutine waiting its turn for a
// processor, and it is still short enough that a stall stops counting against
// the messages that arrive once the library has caught up.
const idleGap = 50 * time.Millisecond

// earliestArrival is the earliest moment the message the callback is handed
// at now can have reached the client library.
func earliestArrival(a *arrivals, now time.Time) time.Time {
	if a.last.IsZero() || now.Sub(a.last) >= idleGap {
		a.n, a.next, a.sum = 0, 0, 0
	}
	return now.Add(-a.sum)
}

// widen makes room in a for the last size callbacks, keeping the ones it
// holds in their order.
func widen(a *arrivals, size int) {
	took := make([]time.Duration, size)
	for i := range a.n {
		took[i] = a.took[(a.next-a.n+i+len(a.took))%len(a.took)]
	}
	a.took, a.next = took, a.n
}

// countCallback records one callback, from start to now, by a client library
// that has pulled outside its count pulled times.
//
// The window widens here rather than when the next callback starts, because
// the messages a pull made during this callback queued behind it: with a
// prefetch of one there was no window to record it in until then.
func countCallback(a *arrivals, pulled uint64, start, now time.Time) {
	a.last = now
	if size := a.prefetch*(1+int(pulled)) - 1; size > len(a.took) {
		widen(a, size)
	}
	if len(a.took) == 0 {
		return
	}
	took := now.Sub(start)
	if a.n == len(a.took) {
		a.sum -= a.took[a.next]
	} else {
		a.n++
	}
	a.took[a.next] = took
	a.sum += took
	a.next = (a.next + 1) % len(a.took)
}

// pastLease reports whether h's lease has ended, from which point not even a
// `+WPI` is written for it.
func pastLease(h *held, now time.Time) bool { return !now.Before(leaseEnds(h)) }

// forget drops a settled delivery's index entry, so that a later delivery of the
// same record does not release a slot that is already free.
func (s *source) forget(token, seq uint64) {
	s.smu.Lock()
	defer s.smu.Unlock()
	if cur, ok := s.seqs[seq]; ok && cur == token {
		delete(s.seqs, seq)
	}
}

// nak asks the server to redeliver a message this source will not hand out.
//
// It is fire and forget on purpose: there is no caller to report to, and a
// message whose nak was lost is redelivered when its lease expires anyway. The
// nak is what turns "when the lease expires" into "now", which is why a closing
// source uses it.
func (s *source) nak(m js.Msg) {
	if err := m.Nak(); err != nil && !isGone(err) {
		s.b.onConsumeError(s, err)
	}
}

// undecodable answers a message this source cannot decode. It is never handed
// out, and it is never acknowledged in place, which would destroy it.
//
// Naking it at once was this driver's first answer, and it spun: the server
// makes a naked message deliverable again immediately and MaxDeliver is
// unlimited unless a deployment sets it, so the same message came straight
// back to this callback thousands of times a second for as long as it
// existed. Nor does a nak free anything on this broker. A naked message stays
// in the consumer's pending set, with or without a delay, so it holds one of
// the MaxAckPending slots the prefetch is made of, and under a prefetch of one
// nothing behind it was ever delivered.
//
// So with a dead-letter subject configured the message goes where the
// settler's own Nack(NackDeadLetter()) would send it, which is the one answer
// that frees the slot without destroying anything. Without one, or when the
// copy could not be stored, it is naked with a delay ([source.nakLater]),
// unless its lease has reached the guard, and reported to health: a message
// this subscription can never read, coming back every lease, is something
// only an operator can fix. A message that was
// dead-lettered is not reported. It is where an operator looks for it, and a
// Degraded left behind for it would stay until the next reconnection.
func (s *source) undecodable(m js.Msg, meta *js.MsgMetadata, cause error, since time.Time) {
	// A message read off the dead-letter subject itself is not copied there
	// again. A consumer whose pattern reaches that subject would read the
	// copy back, fail to decode it the same way and copy it again, at the
	// speed of a PubAck, which is the loop this exists to stop.
	report := true
	if s.b.facts.deadLetter != "" && m.Subject() != s.b.facts.deadLetter {
		err := s.deadLetterUndecodable(m, meta, cause, since)
		if err == nil {
			return
		}
		// The publication's failure is the reason reported, because it is
		// the one to fix: the decode error is the same on every pass, and
		// the message leaves only once the dead-letter route works. A copy
		// Close ended, or came before, is not this message's failure.
		cause = err
		// The two sentinels come back unwrapped, and errors.Is would match
		// any error of their codes, a connection cut among them.
		report = err != errDeadLetterUnknownAtClose && err != mq.ErrClosed //nolint:errorlint // identity, as above
	}
	if report {
		s.b.onConsumeError(s, cause)
	}
	if s.ranOut(m, meta, since) {
		// It waited out its lease in the client library, or behind the
		// failed copy. The nak would land on whichever delivery owns the
		// record now, so it is not written, as a late settlement is not,
		// and the message comes back when the server's lease ends.
		s.b.stale.Add(1)
		return
	}
	s.nakLater(m)
}

// deadLetterUndecodable moves an undecodable message to the dead-letter subject
// in [settler.deadLetter]'s order and for its reason: a durable copy first,
// and `+TERM` only once its PubAck is in hand, so a publication that fails
// leaves the message in the stream. The copy is the settler's own, built from
// a held that is never claimed: the message was never a delivery, so there is
// no slot to settle.
//
// It runs on the consume callback rather than on a goroutine of its own. The
// callback already blocks whenever the ring is full, by design, so one PubAck
// round trip there holds up only this subscription's deliveries, and for no
// longer than [Config.PublishTimeout], or than the message's lease has left
// before its guard when that is sooner. A refusal of the dead-letter subject
// and Close end the wait at once ([Backend.publishCopy]); a refusal used to
// hold up every message behind this one for the whole wait, at every
// redelivery of it. A goroutine would free nothing sooner,
// because the message holds its slot until the Term either way, and it would
// be one more thing Close had to wait for. The messages that queue behind the
// round trip have their leases started from before it ([arrivals]).
//
// The message has a lease of its own all the same, from when it can have
// reached the client library, and it is held to it as [settler.deadLetter]
// holds a delivery: one whose lease is over is not copied, and a publication
// that outlasted it writes no Term, because the Term would land on whichever
// delivery owns the record now. The copy stays and the original comes back.
// So the wait for the PubAck ends at the guard: past it the answer changes
// nothing but the report, and the messages queued behind the callback would
// wait out their leases for it. A copy that was not confirmed by then is
// reported as one that could not be stored, although it may have been, and
// the original is copied again when it comes back.
func (s *source) deadLetterUndecodable(m js.Msg, meta *js.MsgMetadata, cause error, since time.Time) error {
	h := &held{msg: m, seq: meta.Sequence.Stream, delivered: meta.NumDelivered, lease: s.leaseOf(meta.NumDelivered)}
	h.expires.Store(since.Add(h.lease).UnixNano())
	now := s.b.now()
	if s.pastGuard(h, now) {
		s.b.stale.Add(1)
		return nil
	}
	o := mq.NackOptions{Action: mq.NackActionDeadLetter, Reason: "undecodable: " + cause.Error()}
	// context.Background for onMessage's reason. The time left before the
	// guard is read on the driver's clock and waited out on the wall clock,
	// as claimSlot's is.
	wait := leaseEnds(h).Add(-leaseGuard(h.lease)).Sub(now)
	if pt := s.b.cfg.PublishTimeout; pt > 0 && pt < wait {
		wait = pt
	}
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if _, err := s.b.publishCopy(ctx, s.set.deadLetterMessage(h, o)); err != nil {
		return err
	}
	if s.pastGuard(h, s.b.now()) {
		s.b.stale.Add(1)
		return nil
	}
	// A Term that fails leaves a durable copy and an original that comes
	// back after its lease, to be copied again: the duplicate the
	// specification prefers to a loss. The failure is the link's, so it is
	// reported the way a failed nak is.
	err := m.Term()
	s.b.deadLettered.Add(1)
	if err != nil && !isGone(err) {
		s.b.onConsumeError(s, err)
	}
	return nil
}

// nakLater is [source.nak] for a message that would come straight back to a
// consumer that cannot use it: the server makes it deliverable again one lease
// from now rather than at once.
//
// The lease is the delay because it is the period this subscription already
// chose for "a delivery nobody settled comes back", and a message nobody can
// decode is exactly that. It turns thousands of redeliveries a second into one
// per lease. The lease is the durable's AckWait, read back when the source
// opened, so the cadence is the subscription's whichever client set it.
func (s *source) nakLater(m js.Msg) {
	if err := m.NakWithDelay(s.ackWait); err != nil && !isGone(err) {
		s.b.onConsumeError(s, err)
	}
}

// Fetch blocks until at least one delivery is available, ctx is done or the
// source is closed, and fills dst(n].
//
// It is the ring drain plus one filter, and the filter is not optional on this
// broker. MaxAckPending counts unacknowledged MESSAGES, so a record whose lease
// keeps expiring is delivered again and again while nobody settles it, and every
// one of those deliveries lands in the ring behind the first. Handing a caller
// the oldest of them would be handing it a delivery the driver already knows is
// dead: its settlement is refused with ErrLeaseLost, so the handler's work is
// thrown away and the message is processed again from the next copy.
//
// So a delivery whose token went stale while it waited in the ring is dropped
// here rather than handed out, and the caller sees the delivery that really owns
// the record. So is one whose lease ran out while it waited, which the settler
// would refuse for the same reason. It allocates nothing per fetch.
func (s *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	for {
		n, err := s.push.Fetch(ctx, dst)
		if err != nil {
			return 0, err
		}
		if n = s.compact(dst[:n]); n > 0 {
			return n, nil
		}
		// Everything this take produced had been superseded. The ring is
		// finite and the next wait blocks, so the loop terminates.
	}
}

// compact moves the still-live deliveries to the front of ds and returns how
// many there are.
//
// "Live" is the token table's answer and not a guess: a delivery whose slot was
// released is one the source has seen again, and the redelivery is somewhere
// behind it in the same ring.
func (s *source) compact(ds []mq.DriverDelivery) int {
	now := s.b.now()
	out := 0
	for i := range ds {
		if !s.live(ds[i].Token, now) {
			s.b.superseded.Add(1)
			ds[i] = mq.DriverDelivery{}
			continue
		}
		if out != i {
			ds[out] = ds[i]
			ds[i] = mq.DriverDelivery{}
		}
		out++
	}
	return out
}

// live reports whether a delivery in the ring is still worth handing out: its
// slot is still its own and its lease leaves room to settle it.
//
// One whose lease ran out while it waited has its slot released, so the token
// is stale for every later call, and it is not naked: the server may already
// have handed the record to another consumer, and it redelivers it anyway once
// its own AckWait ends.
func (s *source) live(token uint64, now time.Time) bool {
	ref, err := s.table.Resolve(token)
	if err != nil {
		return false
	}
	if h, ok := ref.(*held); ok && s.pastGuard(h, now) {
		s.release(h.token, h.seq)
		return false
	}
	return true
}

// Settler settles this source's deliveries.
func (s *source) Settler() mq.Settler { return s.set }

// Close stops the consumer, naks everything it was still holding, wakes a parked
// Fetch and drops the ring. It is idempotent.
//
// The order is deliberate and each step depends on the one before it:
//
//  1. the closing flag, so that a message arriving from here on is naked rather
//     than pushed, and one waiting for a token slot is naked too;
//  2. the ring's close, which wakes a callback parked in Push — it will nak what
//     it was holding;
//  3. the consumer's DRAIN, so that what the client library had already buffered
//     reaches the callback and is naked too, rather than sitting unacked until
//     its lease runs out. It is waited for up to [Config.DrainTimeout], or
//     once the client has begun to close up to the time [Backend.BeginClose]
//     set for every source;
//  4. the ring's drain, which empties it;
//  5. a nak for every delivery the token table still calls active — which is the
//     ring's leftovers AND everything this source handed the core that was never
//     settled.
//
// No step naks a message whose lease has reached the guard ([leaseGuard]). The
// server may already have handed the record to another consumer, and the nak
// would make it deliverable again under that consumer's handler, so it comes
// back when the server's lease ends instead.
//
// Step 5 is what the specification's "requeue or abandon everything it has HANDED OUT" means,
// and the ring alone does not cover it: a delivery the core has fetched is no
// longer in the ring, so a Close that only drained the ring left it unacked
// until its lease ran out. The token table is the record of what is really
// outstanding, and Settle is what makes the sweep safe — a settlement already in
// flight is refused there rather than raced.
//
// Abandoning instead would not lose anything — the server redelivers after
// AckWait — but on a thirty-second default it is the difference between a
// restart that resumes and one that appears to hang.
func (s *source) Close() error {
	s.startClose()
	s.finishClose(s.b.drainBy())
	return nil
}

// startClose takes Close's first three steps, which end with the consumer's
// drain started and not waited for. [Backend.teardown] starts every source's
// drain before it waits for any, and so does [Backend.BeginClose], before
// the client closes its consumers one after another.
func (s *source) startClose() {
	s.started.Do(func() {
		s.stop()
		_ = s.push.Close()
		if s.consume != nil {
			s.consume.Drain()
		}
	})
}

// finishClose waits for the drain until drained and takes Close's last two
// steps. A second call waits for the first.
func (s *source) finishClose(drained time.Time) {
	s.finished.Do(func() {
		if s.consume != nil {
			timer := time.NewTimer(time.Until(drained))
			select {
			case <-s.consume.Closed():
			case <-timer.C:
				// The drain did not finish inside its bound, and Close
				// stops waiting for it. Stop does nothing here in nats.go
				// v1.55.0, because Drain has already marked the consumer
				// closed, so the drain carries on in the client library
				// until its own flush and buffer are done, and on an owned
				// connection until the connection closes. What it hands
				// the callback then is naked as a closing source naks
				// anything; what is left is unacked at the server and
				// comes back when its lease expires. The call stays for a
				// client library whose Stop ends a drain.
				s.consume.Stop()
			}
			timer.Stop()
		}
		s.push.Drain(nil)
		s.nakOutstanding()
		s.b.forget(s, nil)
	})
}

// nakOutstanding naks every delivery this source has handed out and nobody
// settled.
//
// The sweep goes through [github.com/iqhive/iqmq/driverkit.TokenTable.Settle]
// and not through the index alone, because that is the one call that can tell an
// unsettled delivery from one whose settlement is in flight: a nak racing a
// caller's Ack would send the server both and let the timing decide. A slot the
// call refuses is a slot somebody else owns, and it is left alone. A delivery
// whose lease has reached the guard is not naked, for the reason a late
// settlement is refused.
func (s *source) nakOutstanding() {
	s.smu.Lock()
	tokens := make([]uint64, 0, len(s.seqs))
	for _, token := range s.seqs {
		tokens = append(tokens, token)
	}
	s.seqs = map[uint64]uint64{}
	s.smu.Unlock()
	for _, token := range tokens {
		ref, err := s.table.Settle(token)
		if err != nil {
			continue
		}
		h, ok := ref.(*held)
		if ok && s.pastGuard(h, s.b.now()) {
			// The server may already have handed the record to another
			// consumer, and a nak now would make it deliverable again
			// under that consumer's handler. It is refused as a late
			// settlement is, and comes back when the server's lease ends.
			s.release(token, h.seq)
			s.b.stale.Add(1)
			continue
		}
		if ok {
			s.nak(h.msg)
		}
		_ = s.table.Complete(token, nil)
	}
}

// isClosing reports whether Close has begun.
func (s *source) isClosing() bool { return s.closing.Err() != nil }

// Durable is the durable consumer name this source's subscription maps to, which
// is what a caller looks for in `nats consumer ls`.
func (s *source) Durable() string { return s.durable }
