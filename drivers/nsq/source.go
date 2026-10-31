package nsq

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	gonsq "github.com/nsqio/go-nsq"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/nsq/internal/nsqapi"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The compile-time proof of what this source is.
var _ mq.Source = (*source)(nil)

// maxHandlerConcurrency bounds the handler pool a source starts.
//
// The pool does not need to be the prefetch. nsqd's own RDY accounting already
// bounds how many messages are out at once, the ring is sized to hold all of
// them, so a push never blocks in steady state and a handler goroutine is only
// ever moving a delivery from go-nsq's channel into the ring. What the pool
// bounds is how many deliveries are held in a BLOCKED push when the ring is
// unexpectedly full — and each of those is a message whose nsqd timeout is
// running down while nothing is working on it, so fewer is safer, not slower.
// A push does not wait behind a delivery that has gone dead in the ring, which
// is what kept the ring full while nsqd kept sending (see source.admit).
const maxHandlerConcurrency = 8

// minSweepInterval bounds how often the lease sweeper runs, so that a very
// short lease guard does not turn into a busy loop.
const minSweepInterval = 50 * time.Millisecond

// The settlement states, which are the as one source sees them.
const (
	stateActive  uint32 = iota // handed out, not settled
	stateSettled               // FIN or REQ written
	stateLost                  // this source no longer vouches for it
)

// source is one open subscription: the nsqd channel, the bounded ring its
// handler pushes into, and the fetch that drains it.
type source struct {
	b        *Backend
	cons     nsqapi.Consumer
	push     *driverkit.PushAdapter
	topic    string
	channel  string
	iqTopic  string
	deadline time.Duration
	meta     mq.MetadataMode
	env      *driverkit.Envelope
	handlers int

	// ctx is cancelled by Close, which is what wakes a handler waiting for
	// room in the ring. A push that could not be cancelled would be a
	// goroutine a closing client cannot get rid of, and go-nsq's handler loop
	// is not ours to interrupt.
	ctx    context.Context
	cancel context.CancelFunc

	// pushMu serialises every insertion into the ring with the eviction of
	// dead deliveries from it (see source.admit), and guards room, which a
	// handler waiting for space waits on. It is closed and cleared when
	// space may have appeared: a Fetch took deliveries out, the sweeper gave
	// some back, or an eviction freed more than the evicting push took.
	pushMu sync.Mutex
	room   chan struct{}

	gen atomic.Uint32

	// mu guards inflight, which maps an nsqd and a message identifier to the
	// delivery this source currently vouches for.
	//
	// It is the half of the stale-settlement defence the lease deadline
	// cannot provide. A message this source receives TWICE, because its
	// timeout expired or because it was requeued, arrives as two
	// `*nsq.Message` values with the same identifier, and nsqd will accept a
	// FIN through either. So the older delivery is marked lost the moment the
	// newer one arrives, and a settlement for it reports ErrLeaseLost rather
	// than finishing a message this process is working on under a different
	// delivery.
	//
	// The nsqd is part of the key because an identifier is unique only within
	// the nsqd that issued it (see deliveryKey).
	mu       sync.Mutex
	inflight map[deliveryKey]*delivery

	// connMu guards connecting and closing, which keep a Close away from
	// go-nsq while OpenSource is still connecting. go-nsq's Stop counts only
	// the connections that have finished their handshake: with none finished
	// it closes the channel its handler pool reads from, and a connection
	// that finishes afterwards sends RDY and then puts nsqd's first message
	// on the closed channel, a panic on a go-nsq goroutine. So a Close that
	// arrives during connect only marks the source closing, and connect's
	// caller, OpenSource, closes it once go-nsq has returned.
	connMu     sync.Mutex
	connecting bool
	closing    bool

	once     sync.Once
	wg       sync.WaitGroup
	settleFn *settler
}

// newSourceFor builds the subscription without connecting it.
//
// Construction and connection are separate because go-nsq panics if handlers
// are added after a connection exists, and because OpenSource has to register
// the source with the backend before it can start receiving — a delivery that
// arrived for a source the backend did not yet know about would be one Close
// could not requeue.
func (b *Backend) newSourceFor(cfg mq.SourceConfig, topic, channel string, deadline time.Duration, env *driverkit.Envelope) (*source, error) {
	push, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: cfg.Prefetch})
	if err != nil {
		return nil, err
	}
	cons, err := b.newConsumer(topic, channel, cfg.Prefetch, deadline)
	if err != nil {
		_ = push.Close()
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	s := &source{
		b:        b,
		cons:     cons,
		push:     push,
		topic:    topic,
		channel:  channel,
		iqTopic:  cfg.Topic,
		deadline: deadline,
		meta:     cfg.Metadata,
		env:      env,
		handlers: min(cfg.Prefetch, maxHandlerConcurrency),
		inflight: map[deliveryKey]*delivery{},
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.settleFn = &settler{src: s}
	// The handler pool is added HERE and not in connect, because go-nsq's
	// consumer cannot be stopped without one. Its Stop with no connections
	// closes the incoming-message channel and waits for the handler count to
	// fall to zero — and with no handlers that count is already zero, so
	// nothing ever calls the exit that closes the stop channel and the RDY
	// loop runs for ever. A source that is built and then closed without ever
	// connecting is not hypothetical: OpenSource closes one when the backend
	// closed underneath it.
	s.cons.AddConcurrentHandlers(nsqapi.Handler(gonsq.HandlerFunc(s.handle)), s.handlers)
	return s, nil
}

// connect connects the subscription and starts its lease sweeper.
//
// The handler pool is already running — see newSourceFor — because go-nsq
// panics if handlers are added after a connection exists and cannot be stopped
// if they were never added at all.
//
// Discovery prefers nsqlookupd when it is configured, and the preference is
// not a tie-break: a consumer connected directly to the nsqd addresses never
// learns about an nsqd that joins the cluster later, so on a cluster a direct
// connection silently consumes a subset of the topic. With one nsqd the two
// are equivalent and the direct form is one fewer moving part.
//
// It returns ErrClosed when the source was closed before or during the
// connect, and the caller then closes it (see source.connMu).
func (s *source) connect() error {
	s.connMu.Lock()
	if s.closing {
		s.connMu.Unlock()
		return mq.ErrClosed
	}
	s.connecting = true
	s.connMu.Unlock()
	var err error
	if len(s.b.cfg.LookupdAddrs) > 0 {
		err = s.cons.ConnectToNSQLookupds(s.b.cfg.LookupdAddrs)
	} else {
		err = s.cons.ConnectToNSQDs(s.b.cfg.Addrs)
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	s.connecting = false
	if s.closing {
		return mq.ErrClosed
	}
	if err != nil {
		return s.b.errs.Map(mq.OpOpen, s.iqTopic, s.channel, err)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.sweep()
	}()
	return nil
}

// handle is go-nsq's callback, and the one place this driver decides what a
// delivery is.
//
// It always returns nil. go-nsq's handler loop reads the error and would
// requeue the message with a backoff of its own, which is the second owner of
// a redelivery policy the specification gives to one layer — so the decision is made here
// and the loop is told nothing. DisableAutoResponse is the other half of the
// same rule: without it, the loop would FIN the message the moment this
// function returned, before the application had seen it.
func (s *source) handle(msg *nsqapi.Message) error {
	// First, before anything can return: the loop must not settle this.
	msg.DisableAutoResponse()

	var m mq.Message
	if err := s.env.Unwrap(&m, &mq.Message{Topic: s.iqTopic, Body: msg.Body}); err != nil {
		// There is no settlement that means "this is not a message I can
		// read": nsqd's verbs are finish, requeue and touch. Finishing it
		// would discard a message somebody published, so it goes back with
		// a delay and the failure surfaces as a count. It is not a health
		// transition: the connection that carried the record is fine, and
		// marking it degraded made the next successful Ping report a
		// reconnection that never happened.
		s.b.undecodable.Add(1)
		msg.RequeueWithoutBackoff(s.requeueDelay())
		return nil
	}
	m.Topic = s.iqTopic
	now := s.b.now()
	d := &delivery{msg: msg, key: deliveryKey{nsqd: msg.NSQDAddress, id: msg.ID}, src: s, gen: s.gen.Add(1), received: now}
	d.expires.Store(now.Add(s.deadline).UnixNano())
	s.supersede(d)

	if err := s.admit(mq.DriverDelivery{
		Message: &m,
		Info: mq.DeliveryInfo{
			Topic:        s.iqTopic,
			Subscription: s.channel,
			ReceivedAt:   now,
			LeaseExpires: now.Add(s.deadline),
			Attempt:      uint32(msg.Attempts),
			Redelivered:  msg.Attempts > 1,
		},
		Ref:    d,
		Native: msg,
		Gen:    d.gen,
	}); err != nil {
		// The only errors admit reports are the source closing and its
		// context being cancelled, which are the same event here. The
		// message goes straight back so that nothing waits for its timeout.
		s.requeue(d, 0)
	}
	return nil
}

// admit puts a delivery into the ring, waiting while the ring is full of
// deliveries that are still live, and returns ErrClosed or the source's
// context error once the source is closing.
//
// It never waits behind a delivery that has gone dead in the ring. The ring
// holds as many deliveries as nsqd will have in flight to this consumer, but a
// delivery the sweeper gives back, or a later delivery supersedes, while it
// waits there is no longer in flight at nsqd, and nsqd sends another message in
// its place. A push that waited for that place waited for a Fetch, which an
// application too busy to fetch does not make, and once every handler was
// waiting go-nsq held the next message itself: nsqd's timeout for it was
// running and its lease here had not started, so a later Fetch handed it out
// as live after nsqd had already taken it back. So a push that finds the ring
// full first evicts the dead deliveries from it, which is what Fetch would
// have dropped anyway, and waits only when every delivery there is live.
func (s *source) admit(dd mq.DriverDelivery) error {
	for {
		// Before every insert, as PushAdapter.Push does: a closing source
		// admits nothing more.
		if err := s.ctx.Err(); err != nil {
			return err
		}
		s.pushMu.Lock()
		ok, err := s.push.TryPush(dd)
		if err == nil && !ok && s.evictDeadLocked() {
			ok, err = s.push.TryPush(dd)
		}
		if err != nil || ok {
			s.pushMu.Unlock()
			return err
		}
		if s.room == nil {
			s.room = make(chan struct{})
		}
		room := s.room
		s.pushMu.Unlock()
		select {
		case <-room:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

// evictDeadLocked takes the deliveries that have gone dead out of the ring,
// keeps the live ones in their order, and reports whether it took any. It is
// called with pushMu held, so no other insertion can take the room the live
// ones go back into.
//
// What goes is what Fetch would drop (see source.Fetch), and it is dropped the
// same way: marked, counted, and left in the source's map for the sweeper or a
// redelivery to answer.
func (s *source) evictDeadLocked() bool {
	now := s.b.now()
	evicted := false
	var live []mq.DriverDelivery
	s.push.Drain(func(d *mq.DriverDelivery) {
		if ref, ok := d.Ref.(*delivery); ok && ref != nil && ref.dead(now, s.b.cfg.LeaseGuard) {
			if ref.lose() {
				s.b.stale.Add(1)
			}
			evicted = true
			return
		}
		live = append(live, *d)
	})
	for i := range live {
		// The ring was emptied under pushMu, so a live delivery always fits
		// back unless the ring has closed, and then Close gives it back
		// from the source's map, where it still is.
		_, _ = s.push.TryPush(live[i])
	}
	if evicted && s.room != nil {
		// More than one place may have come free, and the caller takes
		// only one: every handler already waiting tries again.
		close(s.room)
		s.room = nil
	}
	return evicted
}

// signalRoom wakes every handler waiting in admit for space in the ring.
func (s *source) signalRoom() {
	s.pushMu.Lock()
	defer s.pushMu.Unlock()
	if s.room != nil {
		close(s.room)
		s.room = nil
	}
}

// requeueDelay is how long an undecodable delivery is held back, bounded by
// nsqd's own maximum. It is the source's ack deadline rather than zero so that
// a record nothing can read does not spin the connection.
func (s *source) requeueDelay() time.Duration {
	d := s.deadline
	if maxD := s.b.cfg.MaxRequeueDelay; maxD > 0 && d > maxD {
		return maxD
	}
	return d
}

// supersede records d as the delivery this source vouches for and returns any
// older delivery of the same message, which nothing can settle any more.
//
// The old one is RETURNED rather than dealt with here, because dealing with it
// writes a frame to the connection and that must not happen under this mutex:
// go-nsq's response channel is unbuffered, so a frame written under a lock the
// settlement path also takes is a deadlock waiting for a slow broker.
func (s *source) supersede(d *delivery) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, ok := s.inflight[d.key]
	s.inflight[d.key] = d
	if !ok || prev == d {
		return
	}
	// Counted only when this supersession is what took it out of
	// circulation: a delivery the core had already settled went nowhere
	// stale (see delivery.lose).
	if prev.lose() {
		s.b.stale.Add(1)
	}
	if prev.msg.HasResponded() {
		// The frame was written, so nothing is owed.
		return
	}
	// The superseded delivery is CHAINED off the new one rather than
	// answered now, and the reason is. Go-nsq's connection is owed one
	// response per delivery and will not close without them — but nsqd looks
	// a settlement up by message IDENTIFIER, so a frame written now would
	// land on the delivery that has just arrived: a requeue of a message
	// somebody is working on, or worse a finish of it. The safe moment is
	// AFTER the live delivery has been answered, when nsqd no longer holds
	// the identifier in flight and the frame is refused with E_REQ_FAILED
	// and changes nothing. So the debt travels with the delivery that caused
	// it, and every path that answers a delivery pays it.
	d.prev = prev
}

// takeOwed removes and returns the superseded predecessors d still owes a
// response for, so that the caller can write their frames outside the mutex.
func (s *source) takeOwed(d *delivery) []*delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d.prev == nil {
		// The common case allocates nothing: a delivery nothing superseded
		// owes nothing.
		return nil
	}
	var out []*delivery
	for p := d.prev; p != nil; p = p.prev {
		out = append(out, p)
	}
	d.prev = nil
	return out
}

// payOwed writes the frames for d's superseded predecessors, after d itself has
// been answered.
//
// REQ and never FIN. By now nsqd has no in-flight entry for the identifier, so
// each frame is refused and changes nothing; and in the case where that
// reasoning is wrong, a requeue costs a duplicate where a finish would cost the
// message.
func (s *source) payOwed(owed []*delivery) {
	for _, p := range owed {
		p.msg.RequeueWithoutBackoff(0)
	}
}

// forget drops a delivery this source has finished with.
func (s *source) forget(d *delivery) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.inflight[d.key]; ok && cur == d {
		delete(s.inflight, d.key)
	}
}

// requeue gives a delivery back to nsqd and takes it out of circulation.
//
// It is called for every delivery this source stops vouching for, and calling
// it is not optional: go-nsq's connection counts ONE RESPONSE PER DELIVERY and
// [github.com/nsqio/go-nsq.Conn.Close] does not close the read side until that
// count reaches zero — so a delivery the driver never answered keeps the
// connection, its read and write loops and the whole handler pool alive for
// ever. goleak found exactly that, through `lease/expiry-redelivery`: the first
// delivery's lease expired, nsqd redelivered, the driver marked the first
// SUPERSEDED and answered nothing, and eight handler goroutines outlived the
// client by the length of the test run.
//
// The frame is written even for a delivery already marked lost, and that is
// safe. nsqd's requeue looks the message up by identifier AND checks that the
// client sending the frame owns it, so a frame for a delivery another consumer
// has taken over is refused with E_REQ_FAILED and changes nothing — and one
// for a redelivery this same client now holds requeues a message this call was
// giving back anyway. What is NOT safe is a stale FIN, which would destroy
// somebody else's work, and this driver never writes one.
//
// go-nsq's Message carries its own responded flag and sets it with a
// compare-and-swap, so a second call writes nothing: a delivery the core has
// already settled is not requeued by a later Close.
//
// RequeueWithoutBackoff rather than Requeue, because go-nsq's Requeue sets the
// consumer's RDY to zero for the whole subscription while its own backoff
// runs — one failed message stopping every other one. The delay is the core's
// and the client library is not a second owner of it.
func (s *source) requeue(d *delivery, delay time.Duration) {
	if !d.state.CompareAndSwap(stateActive, stateLost) {
		// Already terminal, and in every case writing now would be wrong.
		// A delivery the core settled has had its frame written, and one
		// the lease check took is the sweeper's to answer (see
		// delivery.lostAndDue). A delivery a LATER one superseded has not
		// — but the response it owes is chained onto that later delivery
		// (see source.supersede), because nsqd looks a settlement up by
		// identifier and a frame written now would requeue the delivery
		// somebody is working on. That is what made an expired lease loop:
		// the drop wrote a frame, nsqd requeued the live copy, the
		// redelivery superseded it, and the driver dropped that one too —
		// sixty-five attempts of one message, which
		// TestFetchDropsADeliveryThatWentDeadInTheRing found.
		return
	}
	s.answer(d, delay)
}

// answer writes the REQ a delivery this source has just taken out of
// circulation owes, then pays its superseded predecessors in the safe order,
// and forgets it.
func (s *source) answer(d *delivery, delay time.Duration) {
	owed := s.takeOwed(d)
	d.msg.RequeueWithoutBackoff(delay)
	s.payOwed(owed)
	s.forget(d)
}

// sweep gives back every delivery whose lease is about to run out, and answers
// every one the lease check has already taken.
//
// It is what makes the local lease check a PREVENTION rather than a diagnosis.
// Without it the normal course of a slow handler is: the lease expires, nsqd
// redelivers to this same client, the driver refuses the caller's settlement
// with ErrLeaseLost — correctly — and is left holding a delivery it cannot
// settle and can answer only blind, with a REQ that may land on the
// redelivery (see delivery.lostAndDue). That is a possible duplicate per
// expiry, where a give-back before the deadline costs none.
//
// Giving the delivery back one guard early costs nothing that expiry would not
// have cost — nsqd would have requeued it a moment later — and it buys a
// subscription in which a delivery is superseded only by a redelivery this
// driver did not see coming. The specification's "one scheduler per consumer, never one
// timer per delivery" is honoured: this is one ticker per source over a map
// bounded by the prefetch.
func (s *source) sweep() {
	interval := max(s.b.cfg.LeaseGuard/2, minSweepInterval)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		now := s.b.now()
		s.mu.Lock()
		var due, lost []*delivery
		for _, d := range s.inflight {
			switch {
			case d.expiringSoon(now, s.b.cfg.LeaseGuard):
				due = append(due, d)
			case d.lostAndDue(now):
				lost = append(lost, d)
			}
		}
		s.mu.Unlock()
		// Outside the mutex: each of these writes a frame, and go-nsq's
		// response channel is unbuffered.
		for _, d := range due {
			// [source.requeue] with the count inside it. Counted only when
			// this give-back is what took the delivery out of circulation:
			// between the scan and here a settlement can claim it, or the
			// lease check take it and count it itself (see delivery.lose).
			// The deadline is read again under the lock an extension moves
			// it under, because an Extend can land between the scan and
			// here too: one that did has moved the deadline and been told
			// so, and giving the delivery back on the deadline the scan read
			// would take back the lease that call was granted.
			if s.loseIfDue(d, now) {
				s.b.stale.Add(1)
				s.answer(d, 0)
			}
		}
		for _, d := range lost {
			s.answer(d, 0)
		}
		if len(due) > 0 {
			// A delivery given back may still be in the ring, where a
			// handler waiting for space can now evict it.
			s.signalRoom()
		}
	}
}

// loseIfDue takes d out of circulation if it is still due for a give-back at
// now, reporting whether it did. It holds the source's lock, which
// [settler.Extend] moves a deadline under.
func (s *source) loseIfDue(d *delivery, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return d.expiringSoon(now, s.b.cfg.LeaseGuard) && d.lose()
}

// Fetch fills dst(n] with the deliveries the ring holds.
//
// Deliveries that have gone dead while they sat in the ring are DROPPED rather
// than handed out: a delivery whose lease has expired, or which a later
// delivery of the same message has superseded, is one no settlement can
// succeed on, and handing it to a handler would make the handler's work
// certainly wasted and its acknowledgement certainly refused. nsqd will
// redeliver the message, because that is what an expired timeout means.
func (s *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	for {
		n, err := s.push.Fetch(ctx, dst)
		if n == 0 {
			return 0, err
		}
		s.signalRoom()
		live := 0
		// Over dst[:n] rather than n, so that gosec can see the bound.
		for i := range dst[:n] {
			ref, ok := dst[i].Ref.(*delivery)
			if !ok || ref == nil {
				continue
			}
			if ref.dead(s.b.now(), s.b.cfg.LeaseGuard) {
				// Marked and dropped, and NO frame is written here. The
				// local deadline has passed, so nsqd may already have
				// redelivered, and a frame now would land on that
				// redelivery if it is still on its way in. The delivery
				// stays in the source's map, and the response it owes is
				// paid by whichever redelivery supersedes it or, failing
				// that, by the lease sweeper a sweep later (see
				// delivery.lostAndDue). It is not forgotten here, because
				// forgetting it is how it would be neither.
				if ref.lose() {
					s.b.stale.Add(1)
				}
				continue
			}
			dst[live] = dst[i]
			live++
		}
		for i := live; i < n; i++ {
			dst[i] = mq.DriverDelivery{}
		}
		if live > 0 {
			return live, err
		}
		if err != nil {
			return 0, err
		}
		// Everything in this batch was dead. Wait for the next one rather
		// than returning zero with no error, which the SPI does not allow.
	}
}

// Settler settles this source's deliveries.
func (s *source) Settler() mq.Settler { return s.settleFn }

// Close releases the source, requeues everything it has handed out that was
// never settled, and unblocks a parked Fetch.
//
// The order matters and each step is load-bearing:
//
//  1. set the consumer's in-flight limit to zero, so that nsqd sends nothing
//     more and no connection asks it to, including one go-nsq finishes
//     making after its Stop (see below);
//  2. cancel the context, which wakes a handler waiting for room in the ring;
//  3. close the ring, which unparks a blocked Fetch with ErrClosed;
//  4. requeue what the ring still holds: those deliveries never reached a
//     caller, so putting them back loses nothing and costs no timeout;
//  5. Requeue what WAS handed out and never settled, which is the specification's "requeues
//     or abandons everything it has handed out". It is also what lets
//     go-nsq's own teardown finish: a connection with messages still in
//     flight drains for up to thirty seconds waiting for their responses.
//  6. only then stop the consumer, because every step above writes to the
//     connection the stop is about to take away.
//
// The first step is what keeps a redial from crashing the process. go-nsq
// redials a dropped connection on its own goroutine, and its Stop counts only
// the connections that have finished their handshake: a Close during a redial
// therefore closes the channel go-nsq's handler pool reads from while the
// redial is still waiting for nsqd's answer to IDENTIFY. The redial does not
// look at the stop again; it subscribes and sends RDY, and nsqd's first
// message on it would be put on the closed channel. With the limit at zero
// that RDY is zero, nsqd sends nothing, and the connection stays idle until
// nsqd or the network closes it.
//
// A Close while OpenSource is still connecting only marks the source closing
// and returns: OpenSource closes it once go-nsq has finished (see
// source.connMu). Nothing else can hold the source by then.
func (s *source) Close() error {
	s.connMu.Lock()
	s.closing = true
	connecting := s.connecting
	s.connMu.Unlock()
	if connecting {
		return nil
	}
	s.once.Do(func() {
		s.cons.ChangeMaxInFlight(0)
		s.cancel()
		s.wg.Wait()
		_ = s.push.Close()
		s.push.Drain(func(d *mq.DriverDelivery) {
			if ref, ok := d.Ref.(*delivery); ok && ref != nil {
				_ = ref.lose()
			}
		})
		// Everything this source still holds, live deliveries first and each
		// one's superseded predecessors after it, and a REQ for every one of
		// them.
		//
		// go-nsq's Message carries its own responded flag and sets it with a
		// compare-and-swap, so a delivery the core already settled is not
		// requeued a second time — and a delivery it did NOT settle is
		// answered here, which is what lets the connection close at all
		// (see source.requeue).
		//
		// A live delivery the settler has already claimed is the exception.
		// Its FIN or REQ is about to be written by the settlement that
		// claimed it, which then pays its predecessors in the safe order, so
		// a REQ from here would win go-nsq's flag and that settlement would
		// report success for a message that is coming back. Taking the
		// others to Lost under the mutex is what makes a settlement that
		// arrives afterwards report ErrLeaseLost.
		s.mu.Lock()
		held := make([]*delivery, 0, len(s.inflight))
		for _, d := range s.inflight {
			if claimedBySettler(d) {
				continue
			}
			held = append(held, d)
			for p := d.prev; p != nil; p = p.prev {
				held = append(held, p)
			}
			d.prev = nil
		}
		s.inflight = map[deliveryKey]*delivery{}
		s.mu.Unlock()
		for _, d := range held {
			d.msg.RequeueWithoutBackoff(0)
		}
		s.cons.Stop()
		// A bounded wait: the specification forbids a Close that waits indefinitely,
		// and go-nsq's own stop gives up on a draining connection after
		// thirty seconds of its own.
		stopCtx, cancel := context.WithTimeout(context.Background(), s.b.cfg.APITimeout)
		// A stop that outlasts the bound is abandoned rather than reported:
		// Close has already given every delivery back, and go-nsq finishes
		// the stop on its own.
		_ = s.cons.WaitStopped(stopCtx)
		cancel()
		s.b.forget(s, nil)
	})
	return nil
}

// claimedBySettler takes an active delivery to Lost and reports whether it was
// not active but settled instead, which only a settlement still writing its
// frame can have done.
func claimedBySettler(d *delivery) bool {
	return !d.lose() && d.state.Load() == stateSettled
}

// deliveryKey names a message the way nsqd can tell it apart: by the nsqd that
// delivered it and its identifier there.
//
// The identifier alone is not enough. nsqd builds one from a timestamp, its node
// ID and a sequence, so it is unique within one nsqd and not across them: two
// nsqds whose node IDs collide, and the default is a hash of the host name,
// issue the same identifier for different messages published in the same tick.
// Keyed by the identifier alone, a message from the second superseded a message
// from the first, whose Ack was then refused and whose work was done again.
//
// The nsqd is go-nsq's name for the connection's address, which a reconnection
// to the same nsqd keeps, so a redelivery after one still supersedes. Two
// addresses for one nsqd make two keys; nsqd itself refuses a settlement from
// a connection that does not hold the message, so a delivery on the other
// connection is out of the stale one's reach.
type deliveryKey struct {
	nsqd string
	id   gonsq.MessageID
}

// delivery is one delivery's bookkeeping: the native message, the local view
// of its lease, and the state machine answers a second settlement from.
type delivery struct {
	msg *nsqapi.Message
	key deliveryKey
	src *source
	gen uint32

	// received is when this delivery arrived. nsqd stops a TOUCH at
	// --max-msg-timeout from the delivery, so it is where an extension's
	// ceiling is measured from. It is written once, before the delivery is
	// handed out.
	received time.Time

	// prev is the delivery of the same message this one superseded, and the
	// response this source still owes for it. It is guarded by source.mu.
	prev *delivery

	// sweptLost records that a sweep has already seen this delivery lost to
	// its lease and unanswered, so that the next sweep answers it. It is
	// guarded by source.mu.
	sweptLost bool

	// expires is the local view of the per-message timeout, as Unix
	// nanoseconds. It is atomic because the lease check reads it from
	// whichever goroutine is settling while a Touch writes it from another,
	// and because a time.Time is two words and cannot be read atomically.
	expires atomic.Int64
	state   atomic.Uint32
}

// expiresAt is the local view of the lease deadline.
func (d *delivery) expiresAt() time.Time { return time.Unix(0, d.expires.Load()) }

// extend moves the local view of the lease, which a Touch has to do or the
// next settlement would be refused against a deadline nsqd has already moved.
func (d *delivery) extend(at time.Time) { d.expires.Store(at.UnixNano()) }

// lose marks an ACTIVE delivery one this source no longer vouches for,
// reporting whether it did.
//
// It is a compare-and-swap and not a store, and the difference is a defect
// TestConcurrentSettlementsSerialise found under -race. A store clobbers a
// terminal state: the settled delivery whose requeue had already produced a
// SECOND delivery of the same message was marked lost by the supersession that
// second delivery performed, and every concurrent settlement then reported
// ErrLeaseLost where the specification requires ErrAlreadySettled. One says "your work was
// never accounted for and the message is coming back"; the other says "it was
// accounted for once". A caller that retried on the first would do the work
// twice for no reason.
func (d *delivery) lose() bool { return d.state.CompareAndSwap(stateActive, stateLost) }

// claim takes an active delivery terminal, reporting whether it was still
// active. A second claim is refused, which is what makes a settlement happen
// at most once.
func (d *delivery) claim() bool { return d.state.CompareAndSwap(stateActive, stateSettled) }

// claimLost takes an active delivery out of circulation, reporting whether it
// was still active.
//
// It is what a CLOSE requeues with, and the state it leaves behind is Lost
// rather than Settled on purpose. A caller whose settlement arrives afterwards
// has to be told ErrLeaseLost and not ErrAlreadySettled: the message really is
// back in the channel and really will be delivered again, and "already
// settled" would read as "your work was accounted for" when it was not.

// dead reports whether the delivery is past use: lost, already settled, or
// inside guard of its lease expiring.
func (d *delivery) dead(now time.Time, guard time.Duration) bool {
	if d.state.Load() != stateActive {
		return true
	}
	return d.pastGuard(now, guard)
}

// pastGuard reports whether the delivery is inside guard of its lease expiring,
// and asks nothing about its state.
//
// The two predicates are separate because folding the state into the deadline
// question turned a concurrent settlement into a lost lease. [settler.peek]
// checks the state first — a second settlement of a delivery that IS still
// ours is ErrAlreadySettled — and then asked `dead`, which reads the
// state AGAIN: a racer that won in between made the second read say "not
// active", `dead` say "true", and the loser get ErrLeaseLost where the specification
// requires ErrAlreadySettled. One says "your work was never accounted for and
// the message is coming back"; the other says "it was accounted for once".
// TestConcurrentSettlementsSerialise found it at one failure in about thirty
// runs.
func (d *delivery) pastGuard(now time.Time, guard time.Duration) bool {
	return !now.Add(guard).Before(d.expiresAt())
}

// expiringSoon reports whether the delivery is active, inside guard of its
// lease expiring, and NOT yet past it.
//
// The upper bound is the load-bearing half. A delivery whose lease has already
// passed may have been redelivered by nsqd, and a frame written for it would
// land on that redelivery — so the sweeper gives back only what it can still
// give back safely, and a lease that got away is left to be superseded, or to
// be answered a sweep after the lease check takes it (see
// delivery.lostAndDue). Without the bound the sweeper became the
// cross-settlement it exists to prevent:
// TestFetchDropsADeliveryThatWentDeadInTheRing saw CrossSettled rise to one and
// the live delivery requeued underneath its own acknowledgement.
func (d *delivery) expiringSoon(now time.Time, guard time.Duration) bool {
	if d.state.Load() != stateActive {
		return false
	}
	expires := d.expiresAt()
	return !now.Add(guard).Before(expires) && now.Before(expires)
}

// lostAndDue reports whether d is a delivery the lease check took out of
// circulation that nothing has answered yet, and whether this sweep is the one
// to answer it with REQ(0). It is called with source.mu held, and a lost
// delivery still in the source's map is exactly that: a settlement or a Fetch
// found its lease gone, and neither a redelivery nor Close has taken it over.
//
// The answer is not optional. go-nsq counts one response per delivery on the
// connection that carried it, will not finish closing that connection until
// the count is zero, and does not redial its nsqd until the close has
// finished. Leaving the debt to a redelivery or to Close alone therefore waited
// for ever on a connection that had dropped: the redelivery that was to pay it
// needed the very connection the unpaid debt was blocking, and the
// subscription never received from that nsqd again, which
// TestADeliveryWhoseLeasePassedDoesNotHoldADroppedConnectionOpen holds. On a
// live connection the same delivery stayed counted until Close whenever nsqd
// gave the redelivery to another consumer.
//
// The safe moment would be when the connection is closing, because a frame
// then cannot reach a redelivery; go-nsq shows a consumer neither its
// connections nor their state, so the driver cannot wait for it. It answers
// blind instead, with REQ and never FIN because of what the frame can land on.
// nsqd takes a REQ only from the client that holds the message, so another
// consumer's delivery is out of reach, and on a connection that has gone the
// frame is written nowhere and still counted by go-nsq as the answer. What a
// REQ CAN land on is a redelivery to this same connection that nsqd has sent
// and the driver has not yet seen supersede this delivery: nsqd requeues it,
// the driver hands it out as live, and its acknowledgement later finds nothing
// in flight. That is one more delivery of the message, a duplicate, which
// at-least-once allows; a FIN in the same place would finish the message
// whether or not anybody had done its work.
//
// The sweep it waits for is what keeps that duplicate rare. Just after a
// deadline is exactly when nsqd's redelivery is likely to be on its way, and a
// redelivery that arrives supersedes this delivery and carries its debt to the
// safe moment instead (see source.supersede). Answering at the moment of loss
// made TestFetchDropsADeliveryThatWentDeadInTheRing refuse its live delivery's
// acknowledgement about one run in five, because the REQ had requeued it. A
// delivery still short of its deadline is answered on the first sweep, as the
// sweeper would have given it back anyway.
func (d *delivery) lostAndDue(now time.Time) bool {
	if d.state.Load() != stateLost {
		return false
	}
	if now.Before(d.expiresAt()) || d.sweptLost {
		return true
	}
	d.sweptLost = true
	return false
}

// stateError maps a delivery's current state to the answer the specification gives a second
// settlement.
func stateError(state uint32) error {
	switch state {
	case stateSettled:
		return mq.ErrAlreadySettled
	case stateLost:
		return mq.ErrLeaseLost
	case stateActive:
		return nil
	default:
		return mq.ErrLeaseLost
	}
}

// The source's refusals.
var (
	errUnresolvedPrefetch = errcode.New(errcode.InvalidConfig, "iqmq/nsq: SourceConfig.Prefetch is not positive; the core resolves it before opening a source")
	errNoStartPosition    = errcode.New(errcode.Unsupported, "iqmq/nsq: an NSQ channel has no cursor, so only StartLatest can be honoured; nsqd keeps nothing it has finished")

	errSourceGuardPastDeadline = errcode.New(errcode.InvalidConfig,
		"iqmq/nsq: the source's AckDeadline is no longer than LeaseGuard, so every delivery would be inside its guard on arrival and none would ever be handed out")
)
