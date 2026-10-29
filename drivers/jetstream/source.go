package jetstream

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

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
// consumer, and a table that does fill reports ResourceExhausted and naks rather
// than handing out a token that cannot be told from another delivery's.
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

	// arrivals is the consume callback's account of how long a message may
	// have waited in the client library. Only the callback touches it, and
	// nats.go calls the callback from one goroutine.
	arrivals arrivals

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
	// answered ErrLeaseLost instead of reaching the wire (
	// ).
	smu  sync.Mutex
	seqs map[uint64]uint64

	once    sync.Once
	closing chan struct{}
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
		arrivals:     arrivals{window: 2 * cfg.Prefetch},
		seqs:         make(map[uint64]uint64, cfg.Prefetch),
		closing:      make(chan struct{}),
	}
	if src.table, err = driverkit.NewTokenTable(cfg.Prefetch + tokenHeadroom); err != nil {
		return nil, errPrefetchTooLarge
	}
	if src.push, err = driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: cfg.Prefetch}); err != nil {
		return nil, err
	}
	src.set = &settler{src: src}

	cctx, cancel := b.bound(ctx)
	defer cancel()
	consumer, err := b.upsertConsumer(cctx, js.ConsumerConfig{
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
		return nil, err
	}
	src.consumer = consumer
	consume, err := consumer.Consume(src.onMessage,
		js.PullMaxMessages(cfg.Prefetch),
		js.ConsumeErrHandler(b.consumeErrHandler(src)),
	)
	if err != nil {
		_ = src.push.Close()
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	src.consume = consume
	return src, nil
}

// upsertConsumer creates the durable this subscription maps to, or binds to the
// one that is already there.
//
// The fallback is not laziness. A durable consumer's DeliverPolicy cannot be
// changed once it exists — the server refuses the update — and a second consumer
// of the same subscription asking for a different start position must NOT be the
// reason the subscription stops working. So an update that the server refuses
// becomes a lookup: the existing durable's own configuration wins, which is what
// a durable subscription means, and a caller who needs different settings deletes
// the consumer or names another subscription.
func (b *Backend) upsertConsumer(ctx context.Context, cfg js.ConsumerConfig) (js.Consumer, error) {
	consumer, err := b.js.CreateOrUpdateConsumer(ctx, b.cfg.Stream, cfg)
	if err == nil {
		return consumer, nil
	}
	existing, lookupErr := b.js.Consumer(ctx, b.cfg.Stream, cfg.Durable)
	if lookupErr != nil {
		return nil, err
	}
	return existing, nil
}

// bindDurable takes what this source relies on from the durable the server
// holds, which is not what the source asked for when the server refused its
// update.
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
// An unlimited MaxAckPending leaves the prefetch as the only bound there is.
// The durable's MaxDeliver needs nothing here, because only the server counts
// deliveries against it.
func (s *source) bindDurable(info *js.ConsumerInfo, prefetch int) error {
	if info == nil {
		return nil
	}
	if info.Config.AckWait > 0 {
		s.ackWait = info.Config.AckWait
	}
	s.backoff = slices.Clone(info.Config.BackOff)
	if pending := info.Config.MaxAckPending; pending > prefetch {
		table, err := driverkit.NewTokenTable(pending + tokenHeadroom)
		if err != nil {
			return errPrefetchTooLarge
		}
		s.table = table
	}
	return nil
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
// same number. The time it takes is what [arrivals] counts against the
// leases of the messages that waited behind it.
func (s *source) onMessage(m js.Msg) {
	start := s.b.now()
	s.handle(m, earliestArrival(&s.arrivals, start))
	countCallback(&s.arrivals, start, s.b.now())
}

// handle is one message of the consume callback, which may have reached the
// client library as early as since.
func (s *source) handle(m js.Msg, since time.Time) {
	if s.isClosing() {
		// A source that is closing requeues what it is handed rather than
		// dropping it: the nak makes the message deliverable again at once
		// instead of after the lease expires, which is the specification's "Close requeues
		// or abandons" done the way that loses the least time.
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
	if err != nil {
		s.b.onConsumeError(s, err)
		s.nak(m)
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
		s.release(d.Token, meta.Sequence.Stream)
		s.nak(m)
	}
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
// slot that makes it settleable.
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
	token, err := s.claim(h)
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
// rather than acknowledging the record this delivery now owns (
// ).
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
// The library pulls a prefetch at a time and counts a message off only once
// the callback has returned from it, so no more than a prefetch of messages
// is ahead of one arriving, and no more than two across a reconnection, after
// which it pulls a whole prefetch again. The window is therefore twice the
// prefetch, kept as two halves, the one filling and the one before, whose sum
// covers at least the last window's worth of callbacks without a time kept
// for each. A gap of [idleGap] between one callback returning and the next
// starting is the library waiting for a message rather than handing over one
// it held, so nothing before the gap was ahead of anything after it and the
// sum starts again.
type arrivals struct {
	window int
	last   time.Time
	n      int
	cur    time.Duration
	prev   time.Duration
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
		a.n, a.cur, a.prev = 0, 0, 0
	}
	return now.Add(-(a.cur + a.prev))
}

// countCallback records one callback, from start to now.
func countCallback(a *arrivals, start, now time.Time) {
	a.last = now
	a.cur += now.Sub(start)
	if a.n++; a.n >= a.window {
		a.n, a.cur, a.prev = 0, 0, a.cur
	}
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
// copy could not be stored, it is naked with a delay ([source.nakLater]) and
// reported to health: a message this subscription can never read, coming
// back every lease, is something only an operator can fix. A message that was
// dead-lettered is not reported. It is where an operator looks for it, and a
// Degraded left behind for it would stay until the next reconnection.
func (s *source) undecodable(m js.Msg, meta *js.MsgMetadata, cause error, since time.Time) {
	// A message read off the dead-letter subject itself is not copied there
	// again. A consumer whose pattern reaches that subject would read the
	// copy back, fail to decode it the same way and copy it again, at the
	// speed of a PubAck, which is the loop this exists to stop.
	if s.b.facts.deadLetter != "" && m.Subject() != s.b.facts.deadLetter {
		err := s.deadLetterUndecodable(m, meta, cause, since)
		if err == nil {
			return
		}
		// The publication's failure is the reason reported, because it is
		// the one to fix: the decode error is the same on every pass, and
		// the message leaves only once the dead-letter route works.
		cause = err
	}
	s.b.onConsumeError(s, cause)
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
// longer than [Config.PublishTimeout]. A goroutine would free nothing sooner,
// because the message holds its slot until the Term either way, and it would
// be one more thing Close had to wait for. The messages that queue behind the
// round trip have their leases started from before it ([arrivals]).
//
// The message has a lease of its own all the same, from when it can have
// reached the client library, and it is held to it as [settler.deadLetter]
// holds a delivery: one whose lease is over is not copied, and a publication
// that outlasted it writes no Term, because the Term would land on whichever
// delivery owns the record now. The copy stays and the original comes back.
func (s *source) deadLetterUndecodable(m js.Msg, meta *js.MsgMetadata, cause error, since time.Time) error {
	h := &held{msg: m, seq: meta.Sequence.Stream, delivered: meta.NumDelivered, lease: s.leaseOf(meta.NumDelivered)}
	h.expires.Store(since.Add(h.lease).UnixNano())
	if s.pastGuard(h, s.b.now()) {
		s.b.stale.Add(1)
		return nil
	}
	o := mq.NackOptions{Action: mq.NackActionDeadLetter, Reason: "undecodable: " + cause.Error()}
	// context.Background for onMessage's reason; bound gives it the
	// backend's PublishTimeout.
	ctx, cancel := s.b.bound(context.Background())
	defer cancel()
	if _, err := s.b.js.PublishMsg(ctx, s.set.deadLetterMessage(h, o)); err != nil {
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
//     than pushed;
//  2. the ring's close, which wakes a callback parked in Push — it will nak what
//     it was holding;
//  3. the consumer's DRAIN, so that what the client library had already buffered
//     reaches the callback and is naked too, rather than sitting unacked until
//     its lease runs out;
//  4. the ring's drain, which empties it;
//  5. a nak for every delivery the token table still calls active — which is the
//     ring's leftovers AND everything this source handed the core that was never
//     settled.
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
	var err error
	s.once.Do(func() {
		close(s.closing)
		_ = s.push.Close()
		if s.consume != nil {
			s.consume.Drain()
			timer := time.NewTimer(s.b.cfg.DrainTimeout)
			select {
			case <-s.consume.Closed():
			case <-timer.C:
				// The drain did not finish inside its bound. Stop is the
				// hard form: whatever is left is unacked at the server and
				// comes back when its lease expires.
				s.consume.Stop()
			}
			timer.Stop()
		}
		s.push.Drain(nil)
		s.nakOutstanding()
		s.b.forget(s, nil)
	})
	return err
}

// nakOutstanding naks every delivery this source has handed out and nobody
// settled.
//
// The sweep goes through [github.com/iqhive/iqmq/driverkit.TokenTable.Settle]
// and not through the index alone, because that is the one call that can tell an
// unsettled delivery from one whose settlement is in flight: a nak racing a
// caller's Ack would send the server both and let the timing decide. A slot the
// call refuses is a slot somebody else owns, and it is left alone.
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
		if h, ok := ref.(*held); ok {
			s.nak(h.msg)
		}
		_ = s.table.Complete(token, nil)
	}
}

// isClosing reports whether Close has begun.
func (s *source) isClosing() bool {
	select {
	case <-s.closing:
		return true
	default:
		return false
	}
}

// Durable is the durable consumer name this source's subscription maps to, which
// is what a caller looks for in `nats consumer ls`.
func (s *source) Durable() string { return s.durable }
