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

	// ctx is cancelled by Close, which is what unparks a handler blocked in
	// Push. A push that could not be cancelled would be a goroutine a
	// closing client cannot get rid of, and go-nsq's handler loop is not
	// ours to interrupt.
	ctx    context.Context
	cancel context.CancelFunc

	gen atomic.Uint32

	// mu guards inflight, which maps an nsqd message identifier to the
	// delivery this source currently vouches for.
	//
	// It is the half of the stale-settlement defence the lease deadline
	// cannot provide. A message this source receives TWICE — because its
	// timeout expired, or because the connection that held it went away and
	// nsqd made it available again — arrives as two `*nsq.Message` values
	// with the same identifier, and nsqd will accept a FIN through either.
	// So the older delivery is marked lost the moment the newer one
	// arrives, and a settlement for it reports ErrLeaseLost rather than
	// finishing a message this process is working on under a different
	// delivery.
	mu       sync.Mutex
	inflight map[gonsq.MessageID]*delivery

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
		inflight: map[gonsq.MessageID]*delivery{},
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
func (s *source) connect() error {
	var err error
	if len(s.b.cfg.LookupdAddrs) > 0 {
		err = s.cons.ConnectToNSQLookupds(s.b.cfg.LookupdAddrs)
	} else {
		err = s.cons.ConnectToNSQDs(s.b.cfg.Addrs)
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
		// a delay and the failure surfaces as a count and a health
		// transition, which is the answer for a driver's background
		// failure.
		s.b.undecodable.Add(1)
		s.b.degrade(s.b.errs.Map(mq.OpFetch, s.iqTopic, s.channel, err))
		msg.RequeueWithoutBackoff(s.requeueDelay())
		return nil
	}
	m.Topic = s.iqTopic
	now := s.b.now()
	d := &delivery{msg: msg, id: msg.ID, src: s, gen: s.gen.Add(1)}
	d.expires.Store(now.Add(s.deadline).UnixNano())
	s.supersede(d)

	if err := s.push.Push(s.ctx, mq.DriverDelivery{
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
		// The only errors Push reports are the source closing and its
		// context being cancelled, which are the same event here. The
		// message goes straight back so that nothing waits for its timeout.
		s.requeue(d, 0)
	}
	return nil
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
	prev, ok := s.inflight[d.id]
	s.inflight[d.id] = d
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
	if cur, ok := s.inflight[d.id]; ok && cur == d {
		delete(s.inflight, d.id)
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
		// Already terminal, and in both cases writing now would be wrong.
		// A delivery the core settled has had its frame written. A delivery
		// a LATER one superseded has not — but the response it owes is
		// chained onto that later delivery (see source.supersede), because
		// nsqd looks a settlement up by identifier and a frame written now
		// would requeue the delivery somebody is working on. That is what
		// made an expired lease loop: the drop wrote a frame, nsqd requeued
		// the live copy, the redelivery superseded it, and the driver
		// dropped that one too — sixty-five attempts of one message, which
		// TestFetchDropsADeliveryThatWentDeadInTheRing found.
		return
	}
	owed := s.takeOwed(d)
	d.msg.RequeueWithoutBackoff(delay)
	s.payOwed(owed)
	s.forget(d)
}

// sweep gives back every delivery whose lease is about to run out.
//
// It is what makes the local lease check a PREVENTION rather than a diagnosis.
// Without it the normal course of a slow handler is: the lease expires, nsqd
// redelivers to this same client, the driver refuses the caller's settlement
// with ErrLeaseLost — correctly — and is left holding a delivery it can neither
// settle nor answer, because a frame for it would land on the redelivery. That
// is one unanswerable delivery per expiry, and go-nsq's connection needs an
// answer for each of them.
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
		var due []*delivery
		for _, d := range s.inflight {
			if d.expiringSoon(now, s.b.cfg.LeaseGuard) {
				due = append(due, d)
			}
		}
		s.mu.Unlock()
		// Outside the mutex: each of these writes a frame, and go-nsq's
		// response channel is unbuffered.
		for _, d := range due {
			s.b.stale.Add(1)
			s.requeue(d, 0)
		}
	}
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
		live := 0
		for i := range n {
			ref, ok := dst[i].Ref.(*delivery)
			if !ok || ref == nil {
				continue
			}
			if ref.dead(s.b.now(), s.b.cfg.LeaseGuard) {
				// Marked and dropped, and NO frame is written. The local
				// deadline has passed, so nsqd may already have
				// redelivered — and a frame now would land on that
				// redelivery, which is exactly the cross-settlement the
				// lease check exists to prevent. The response this
				// delivery owes is paid by whichever redelivery
				// supersedes it, or by Close; it is not forgotten here,
				// because forgetting it is how it would be neither.
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
//  1. cancel the context, which unparks a handler blocked in Push;
//  2. close the ring, which unparks a blocked Fetch with ErrClosed;
//  3. requeue what the ring still holds — those deliveries never reached a
//     caller, so putting them back loses nothing and costs no timeout;
//  4. Requeue what WAS handed out and never settled, which is the specification's "requeues
//     or abandons everything it has handed out". It is also what lets
//     go-nsq's own teardown finish: a connection with messages still in
//     flight drains for up to thirty seconds waiting for their responses.
//  5. only then stop the consumer, because every step above writes to the
//     connection the stop is about to take away.
func (s *source) Close() error {
	s.once.Do(func() {
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
		s.mu.Lock()
		held := make([]*delivery, 0, len(s.inflight))
		for _, d := range s.inflight {
			held = append(held, d)
			for p := d.prev; p != nil; p = p.prev {
				held = append(held, p)
			}
			d.prev = nil
		}
		s.inflight = map[gonsq.MessageID]*delivery{}
		s.mu.Unlock()
		for _, d := range held {
			_ = d.lose()
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

// delivery is one delivery's bookkeeping: the native message, the local view
// of its lease, and the state machine answers a second settlement from.
type delivery struct {
	msg *nsqapi.Message
	id  gonsq.MessageID
	src *source
	gen uint32

	// prev is the delivery of the same message this one superseded, and the
	// response this source still owes for it. It is guarded by source.mu.
	prev *delivery

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
// give back safely, and a lease that got away is left to be superseded. Without
// the bound the sweeper became the cross-settlement it exists to prevent:
// TestFetchDropsADeliveryThatWentDeadInTheRing saw CrossSettled rise to one and
// the live delivery requeued underneath its own acknowledgement.
func (d *delivery) expiringSoon(now time.Time, guard time.Duration) bool {
	if d.state.Load() != stateActive {
		return false
	}
	expires := d.expiresAt()
	return !now.Add(guard).Before(expires) && now.Before(expires)
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
)
