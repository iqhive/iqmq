package gcppubsub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// The source implements the mandatory consumption surface; its settler
// implements the optional batched settlement and the optional lease
// extension.
var _ mq.Source = (*source)(nil)

// source is one open subscription: the client library's streaming pull feeding
// one bounded ring, and the bookkeeping that makes a stale settlement
// detectable.
//
// # Push to pull
//
// The library delivers by CALLBACK — Subscriber.Receive calls a function per
// message — and the SPI asks the driver to be called instead.
// [driverkit.PushAdapter] is what makes the two meet, and its bound is what
// makes the meeting safe: the callback blocks in Push while the ring is full,
// and the library's flow control releases a message's slot when the CALLBACK
// RETURNS. So a blocked callback is a slot the library cannot reuse, which is
// exactly the backpressure a streaming pull has and the only one it has.
//
// The two bounds are therefore both the prefetch, and the driver holds at most
// twice it: `prefetch` deliveries sitting in the ring, and up to `prefetch`
// callbacks parked on Push. That is stated rather than hidden because it is
// the number an operator sizing memory needs, and because the alternative —
// making the callback wait for the SETTLEMENT — would hold one library
// goroutine per unsettled message for as long as a handler runs.
//
// # NumGoroutines is one
//
// One streaming pull stream, always. A second stream is a second source of
// out-of-order delivery for an ordering key, and the ring behind the callback
// is what bounds concurrency instead. The specification says so for this driver and this is
// where it is done.
type source struct {
	b     *Backend
	cfg   mq.SourceConfig
	enc   *encoder
	topic string
	sub   string
	info  psapi.SubscriptionInfo
	recv  psapi.Subscription
	ring  *driverkit.PushAdapter
	set   *settler

	// deadline is the acknowledgement deadline the subscription grants, and
	// budget is the lease a delivery has: the longer of that deadline and
	// Config.MaxExtension, bounded by MaxExtensionCeiling.
	//
	// The budget is also what the client library is told to extend for, and
	// that is what makes it the lease. Streaming pull sets its own
	// acknowledgement deadline per stream, which overrides the
	// subscription's, and the library stops extending a message once
	// MaxExtension has passed since it arrived. So a subscription deadline
	// longer than MaxExtension never reached the service: the library let go
	// at MaxExtension, the message went to another consumer, and this source
	// went on reporting a lease that had already gone, and accepting
	// settlements for it.
	deadline time.Duration
	budget   time.Duration

	// gen is the delivery generation, one per delivery and redelivery
	// It is what makes a settlement carrying a delivery the driver
	// no longer vouches for detectable.
	gen atomic.Uint32

	// mu guards inflight and holds.
	//
	// inflight maps a message identifier to the delivery this source
	// currently vouches for, which is the stale-settlement defence and the
	// half the acknowledgement deadline cannot provide. A message this
	// source receives TWICE — because its deadline expired — has two
	// acknowledgement identifiers and the service will accept an
	// acknowledgement for either of them, so the older delivery is marked
	// superseded the moment the newer one arrives (the shape `drivers/sqs`
	// recorded).
	//
	// holds are the delayed negative settlements in flight, and the
	// undecodable messages waiting out a deadline before their nack
	// ([source.holdUndecodable]). They are here rather than in the settler
	// because Close has to end them: a hold that outlived its source would be
	// a goroutine nobody can stop and a message nobody nacks.
	mu       sync.Mutex
	inflight map[string]*delivery
	holds    map[*delivery]mq.Timer
	closing  bool
	// dlq is the dead-letter publisher, created on the first Reject.
	dlq psapi.Publisher

	// stale counts the superseded or expired deliveries Fetch and the
	// callback have dropped.
	stale atomic.Uint64

	// arrivals is the callback's account of how long a message can have
	// waited in the client library before it was handed over, which is what
	// a delivery's lease is started from.
	arrivals arrivals

	wg     sync.WaitGroup
	holdWG sync.WaitGroup
	cancel context.CancelFunc
	stop   chan struct{}
	once   sync.Once
	closed atomic.Bool
}

// newSource builds the source and its settler, without starting the receiver.
func (b *Backend) newSource(
	cfg mq.SourceConfig, enc *encoder, topic, subID string, info psapi.SubscriptionInfo,
) (*source, error) {
	deadline := cfg.AckDeadline
	if deadline <= 0 {
		deadline = info.AckDeadline
	}
	if deadline <= 0 {
		deadline = b.cfg.AckDeadline
	}
	// The service's acknowledgement deadline is a whole number of seconds,
	// so the deadline this source measures its own lease against is rounded
	// UP to what the service will actually grant. Rounding down is what makes
	// a sub-second deadline into no lease at all, which `drivers/sqs`
	// recorded after two scenarios failed on it.
	deadline = time.Duration(wholeSeconds(deadline)) * time.Second
	prefetch := cfg.Prefetch
	if prefetch <= 0 {
		prefetch = b.cfg.Prefetch
	}
	ring, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: prefetch})
	if err != nil {
		return nil, err
	}
	outstandingBytes := int64(b.cfg.MaxOutstandingBytes)
	if outstandingBytes == 0 {
		outstandingBytes = libraryOutstandingBytes
	}
	src := &source{
		b: b, cfg: cfg, enc: enc, topic: topic, sub: subID, info: info,
		ring:     ring,
		deadline: deadline,
		budget:   min(max(deadline, b.cfg.MaxExtension), MaxExtensionCeiling),
		inflight: make(map[string]*delivery, prefetch),
		holds:    make(map[*delivery]mq.Timer, 4),
		stop:     make(chan struct{}),
		arrivals: arrivals{window: 2 * prefetch, slots: prefetch, bytes: outstandingBytes},
	}
	src.gen.Store(1)
	recv, err := b.api.Subscription(psapi.SubscriptionConfig{
		Subscription:           subID,
		MaxOutstandingMessages: prefetch,
		MaxOutstandingBytes:    b.cfg.MaxOutstandingBytes,
		NumGoroutines:          1,
		MaxExtension:           src.budget,
	})
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	src.recv = recv
	src.set = newSettler(src)
	return src, nil
}

// start launches the receiver.
//
// It deliberately does NOT take the opening context. It outlives the call that
// started it — a source lives until Close — so a receiver bound to the open's
// context would stop the moment a caller who used a bounded context for
// construction let it expire, and a subscription whose receiver has stopped is
// one that has silently stopped consuming.
func (s *source) start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(1)
	go s.receive(ctx)
}

// receiveBackoff is how long the receiver waits after a failed streaming pull.
//
// It exists so that a subscription that has been deleted, or a credential that
// has expired, costs one attempt per interval rather than a spin. The
// streaming pull itself is the pacing for the successful path, so there is
// nothing to back off from there.
const receiveBackoff = 500 * time.Millisecond

// receive runs the streaming pull, restarting it after a failure.
func (s *source) receive(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			return
		case <-ctx.Done():
			return
		default:
		}
		err := s.recv.Receive(ctx, s.admit)
		if err == nil || s.closed.Load() || ctx.Err() != nil {
			return
		}
		// A failed streaming pull is a background failure with nowhere to
		// surface but Health. It is not fatal: the next attempt
		// retries, which is what a service whose stream is torn down every
		// few minutes by design needs.
		s.b.degraded(s.b.errs.Map(mq.OpFetch, s.cfg.Topic, s.cfg.Subscription, err))
		if !s.wait(receiveBackoff) {
			return
		}
	}
}

// wait sleeps for d unless the source is closing, and reports whether the
// receiver should continue.
func (s *source) wait(d time.Duration) bool {
	t := s.b.newTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return true
	case <-s.stop:
		return false
	}
}

// admit turns one received message into a delivery and pushes it into the
// ring, blocking while the ring is full.
//
// Blocking is the contract: the library releases the message's flow-control
// slot when this function returns, so a callback that waits is a stream that
// stops asking for more.
func (s *source) admit(ctx context.Context, in *psapi.Incoming) {
	// A delivery is proof that the streaming pull is working, which is the
	// only proof Ready transition can have on a consumer. It costs
	// one atomic load unless a failure was reported.
	s.b.recovered()
	now := s.b.now()
	// The lease runs from the earliest moment the message can have reached
	// the client library, which is when the library started its budget, and
	// that is what the delivery reports as received too ([arrivals]).
	size := int64(len(in.Data))
	since := s.arrivals.enter(now, size)
	defer s.left(size)
	attempt, hasAttempt := attemptOf(in)
	ref := &delivery{
		id:     in.ID,
		settle: in.Settle,
		native: in.Native,
		gen:    s.gen.Add(1),
		src:    s,
		kept:   now.Add(s.budget).UnixNano(),
	}
	ref.received.Store(since.UnixNano())
	ref.expires.Store(since.Add(s.budget).UnixNano())
	msg := &mq.Message{}
	if err := s.enc.decode(in, s.cfg.Topic, msg); err != nil {
		// A message this driver cannot decode is not something to
		// acknowledge, which would destroy it; see [source.undecodable] for
		// what it gets instead. The failure is reported through Health,
		// because there is no delivery to attach it to.
		s.b.degraded(err)
		s.undecodable(ref, hasAttempt)
		return
	}
	d := mq.DriverDelivery{
		Message: msg,
		Info: mq.DeliveryInfo{
			Topic:        s.cfg.Topic,
			Source:       s.sub,
			Subscription: s.cfg.Subscription,
			ReceivedAt:   since,
			LeaseExpires: ref.expiresAt(),
			Attempt:      attempt,
		},
		Ref:    ref,
		Native: in.Native,
		Gen:    ref.gen,
	}
	d.Info.Redelivered = hasAttempt && attempt > 1
	s.supersede(ref)
	if ref.expired(now) {
		// The message waited in the library for the whole of its budget, so
		// the library has stopped extending it, or is about to, and the
		// service may have given it to another consumer already. Handed
		// out, its settlement would be refused; pushed, it would hold up
		// the messages behind it for nothing.
		s.stale.Add(1)
		s.drop(ref, now)
		return
	}
	if err := s.push(ctx, d, s.info.Ordering && in.OrderingKey != ""); err != nil {
		// The ring is closed, so the source is closing. The message is made
		// deliverable again rather than held: a delivery nobody will ever
		// fetch is one whose acknowledgement deadline expires for no reason.
		s.forget(ref)
		if ref.claimLost() {
			in.Settle.Nack()
		}
	}
}

// push puts a delivery into the ring, blocking while the ring is full.
//
// A delivery with an ordering key the subscription orders by tells [arrivals]
// while it is blocked: the client library holds the later messages of that key
// back behind it, in flow-control slots this source cannot see. The try that
// finds room does not look at the context, so the context is checked first, as
// [driverkit.PushAdapter.Push] checks it before every insert.
func (s *source) push(ctx context.Context, d mq.DriverDelivery, keyed bool) error {
	if !keyed {
		return s.ring.Push(ctx, d)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if ok, err := s.ring.TryPush(d); ok || err != nil {
		return err
	}
	s.arrivals.park()
	err := s.ring.Push(ctx, d)
	s.arrivals.unpark(s.b.now())
	return err
}

// left records a callback returning, which is when the client library takes
// its message's flow-control slot back.
func (s *source) left(size int64) { s.arrivals.leave(s.b.now(), size) }

// arrivals is the streaming pull callback's account of how long the message it
// is handed may have waited in the client library first.
//
// The library starts a message's extension budget when the streaming pull
// returns it, and stops extending the message once the budget has passed. It
// hands the message to the callback only when its flow control has room, which
// is a prefetch of messages and MaxOutstandingBytes of data, held by callbacks
// that have not returned. The callback here returns at once unless it is
// blocked on a full ring, so once blocked callbacks fill the flow control the
// library holds pulled messages back with their budgets running. A lease
// started when the callback is handed one of them ends after the library has
// let it go, by as long as it waited, and the service may then have given the
// message to another consumer, whose work this delivery's Ack acknowledges.
//
// So a delivery's lease starts at the earliest moment its message can have
// reached the library. The library hands over one pull at a time and in order,
// so a message waited no longer than the gaps between the callbacks ahead of
// it since its pull arrived. The service sends a stream no more than its flow
// control allows, which is the prefetch, so no more than a prefetch of messages
// is ahead of one in its pull, and no more than two across a stream the service
// restarted while this source still held messages. The gaps of the last
// window, twice the prefetch, are therefore charged, kept as two halves, the
// one filling and the one before, whose sum covers at least a window of
// callbacks without a time kept for each.
//
// A gap is charged only while the flow control is full, or within [idleGap] of
// it last having room: a library nothing has held back for that long handed
// its last message over straight from the pull, so nothing before that moment
// was ahead of anything after it, and the sum starts again. The flow control
// is counted as the library counts it, by the callbacks that have not returned
// and their data, and it is taken as full while a callback with an ordering
// key is blocked on the ring, because the messages of that key held back
// behind it hold slots of their own. A ring that is full while the flow
// control has room charges nothing: nothing is waiting in the library then.
type arrivals struct {
	mu sync.Mutex
	// window is twice the prefetch. slots and bytes are the library's flow
	// control: the prefetch, and MaxOutstandingBytes as the library resolves
	// it.
	window int
	slots  int
	bytes  int64
	// inflight and data are the callbacks that have not returned and the
	// data of their messages; keyed is the callbacks with an ordering key
	// blocked on the ring.
	inflight int
	data     int64
	keyed    int
	// freed is when the flow control last went from full to having room.
	freed time.Time
	last  time.Time
	n     int
	cur   time.Duration
	prev  time.Duration
}

// libraryOutstandingBytes is the client library's MaxOutstandingBytes when
// Config.MaxOutstandingBytes leaves it at zero, in both generations.
const libraryOutstandingBytes = 1e9

// idleGap is the longest the client library takes to hand the callback a
// message it already holds once its flow control has room. That is a
// semaphore and a goroutine, which take microseconds, so this allows for the
// goroutines waiting their turn for a processor, and it is still short enough
// that a stall stops counting against the messages pulled after it.
const idleGap = 50 * time.Millisecond

// enter records a callback handed a message of size bytes at now, and returns
// the earliest moment that message can have reached the client library.
func (a *arrivals) enter(now time.Time, size int64) time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case a.full() || now.Sub(a.freed) < idleGap:
		// Callbacks run on goroutines of their own, so two can read the
		// clock in one order and take the lock in the other.
		if gap := now.Sub(a.last); gap > 0 && !a.last.IsZero() {
			a.cur += gap
		}
	default:
		a.n, a.cur, a.prev = 0, 0, 0
	}
	if now.After(a.last) {
		a.last = now
	}
	wait := a.cur + a.prev
	if a.n++; a.n >= a.window {
		a.n, a.cur, a.prev = 0, 0, a.cur
	}
	a.inflight++
	a.data += size
	return now.Add(-wait)
}

// leave records the callback for a message of size bytes returning at now.
func (a *arrivals) leave(now time.Time, size int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	was := a.full()
	a.inflight--
	a.data -= size
	a.room(was, now)
}

// park records a callback with an ordering key blocking on the full ring.
func (a *arrivals) park() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.keyed++
}

// unpark records a blocked callback with an ordering key going on at now.
func (a *arrivals) unpark(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	was := a.full()
	a.keyed--
	a.room(was, now)
}

// full reports whether the library's flow control can be without room: every
// slot is held, the data held leaves no room for a message as large as the
// service takes, or a callback with an ordering key is blocked.
func (a *arrivals) full() bool {
	return a.inflight >= a.slots || a.keyed > 0 || a.data+min(MaxMessageBytes, a.bytes) > a.bytes
}

// room records the flow control having room at now, if it was full before.
func (a *arrivals) room(wasFull bool, now time.Time) {
	if wasFull && !a.full() {
		a.freed = now
	}
}

// undecodable answers a message this source cannot decode. It is never handed
// out and never acknowledged.
//
// Nacking it at once was this driver's first answer, and on a subscription
// with no dead-letter policy it spun: the service makes a nacked message
// deliverable again at once and nothing ever ends it, so the same message came
// straight back to this callback, more than a million times a second against
// the in-process fake, for as long as it existed.
//
// So it is nacked at once only where the service is counting. A delivery that
// reports an attempt comes from a subscription with a dead-letter policy, and
// that policy moves the message to the dead-letter topic after
// MaxDeliveryAttempts deliveries, which is where a corrupt record belongs and
// which a delay would only postpone. Anywhere else it is held for one
// acknowledgement deadline and then nacked ([source.holdUndecodable]), which
// turns the spin into one redelivery per deadline: a message nobody here can
// read, coming back every deadline, is something only an operator can fix, and
// Health has already said so.
func (s *source) undecodable(ref *delivery, counted bool) {
	if counted {
		ref.settle.Nack()
		return
	}
	s.holdUndecodable(ref)
}

// holdUndecodable nacks an undecodable message one acknowledgement deadline
// from now, on the machinery a delayed Nack uses ([settler.holdNack]): the
// client library goes on extending the message's deadline while the hold
// lasts, so it does not come back early, and the hold sits in s.holds, so Close
// ends it with the others and under the same mutex.
//
// The deadline is the delay because it is the period the subscription already
// chose for "a delivery nobody settled comes back", and because it is never
// longer than the budget the library extends for, so the hold cannot outlive
// the lease. A held message is unacknowledged at the service, so it takes one
// of the MaxOutstandingMessages the stream asks for while it waits: under a
// prefetch of one, nothing behind it is delivered in that time.
func (s *source) holdUndecodable(ref *delivery) {
	timer := s.b.newTimer(s.deadline)
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		timer.Stop()
		// The source is closing, so nothing would end the hold; the message
		// is made deliverable again now, as Close does with everything else
		// this source holds.
		ref.settle.Nack()
		return
	}
	ref.state.Store(stateHeld)
	s.holds[ref] = timer
	s.holdWG.Add(1)
	s.mu.Unlock()
	go s.set.awaitHold(ref, timer)
}

// supersede records this delivery as the one the source vouches for, marking
// any earlier delivery of the same message lost and giving that delivery's
// message back to the client library.
//
// The give-back is the half that was missing. The library goes on extending an
// acknowledgement identifier until somebody acknowledges or nacks it, and its
// shutdown waits for that, so a superseded delivery that nobody settled held
// Close until Config.MaxExtension ran out: ten minutes on the default
// configuration. A nack is the one settlement that is safe here. The service
// has already made the message deliverable again under the earlier
// identifier's expired deadline, which is how the later delivery exists, while
// an acknowledgement could remove the message the later delivery is working on.
//
// A delivery a settlement has claimed is left to that settlement, which either
// settles the library's message itself or hands it back ([delivery.giveBack]).
func (s *source) supersede(ref *delivery) {
	s.mu.Lock()
	prev, ok := s.inflight[ref.id]
	s.inflight[ref.id] = ref
	s.mu.Unlock()
	if !ok || prev == ref {
		return
	}
	prev.release()
}

// forget drops a delivery from the in-flight map, if it is still the current
// one.
func (s *source) forget(ref *delivery) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.inflight[ref.id]; ok && cur == ref {
		delete(s.inflight, ref.id)
	}
}

// Fetch blocks until at least one delivery is available, ctx is done or the
// source is closed, and fills dst(n].
//
// It DROPS a delivery the ring still holds that a later delivery of the same
// message has superseded, or whose lease has run out while it waited. That is
// not an optimisation: handing out a delivery whose settlement is certain to
// be refused gives the caller work it cannot complete, while the message
// itself is either in the ring one slot along or back at the service with a
// full deadline. It is the shape `drivers/redis` recorded and `drivers/sqs`, on a third broker that reaches it by
// a third route. An expired delivery goes back to the client library while the
// library can still be extending it ([delivery.expire]).
func (s *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	for {
		if s.closed.Load() {
			return 0, mq.ErrClosed
		}
		n, err := s.ring.Fetch(ctx, dst)
		if n == 0 {
			return 0, err
		}
		live, now := 0, s.b.now()
		for i := range n {
			if ref, ok := dst[i].Ref.(*delivery); ok && s.dead(ref, now) {
				s.stale.Add(1)
				s.drop(ref, now)
				continue
			}
			if live != i {
				dst[live] = dst[i]
			}
			live++
		}
		if live > 0 || err != nil {
			return live, err
		}
	}
}

// dead reports whether a delivery in the ring is one nobody could settle: one
// a later delivery has superseded, or one whose lease has run out while it
// waited to be fetched.
func (s *source) dead(ref *delivery, now time.Time) bool {
	return ref.state.Load() == stateLost || ref.expired(now)
}

// drop takes a delivery nobody can settle any more out of circulation.
func (s *source) drop(ref *delivery, now time.Time) {
	s.forget(ref)
	ref.expire(now)
}

// Stale is how many superseded or expired deliveries Fetch and the callback
// have dropped, which is the number a test asserts on: a source that never
// drops one under a lease that keeps expiring is a source handing out
// deliveries nobody can settle.
func (s *source) Stale() uint64 { return s.stale.Load() }

// Settler settles this source's deliveries. There is one per source.
func (s *source) Settler() mq.Settler { return s.set }

// Close releases the source, nacks everything it has handed out and never
// settled, and unblocks a parked Fetch. It is idempotent.
func (s *source) Close() error {
	s.once.Do(func() {
		s.closed.Store(true)
		close(s.stop)
		// EVERYTHING this source is holding is settled BEFORE the receiver
		// is waited for, and the order is not a preference: the client
		// library's shutdown WAITS for every message it has handed to the
		// callback to be acknowledged or negatively settled
		// (messageIterator.stop blocks on exactly that), so a Close that
		// waited for Receive first and settled afterwards deadlocks — and a
		// caller sees Close hang for as long as the automatic extension keeps
		// those messages alive, which is ten minutes on the default
		// configuration.
		//
		// So: end the holds, wake the parked callbacks, nack what the ring
		// still holds, nack what was handed out and never settled, and only
		// then cancel the receiver and wait for it.
		s.endHolds()
		// The ring is closed BEFORE the receiver is waited for, so a callback
		// parked in Push is woken rather than waited on for the length of a
		// stream.
		_ = s.ring.Close()
		// Everything the ring still held is made deliverable again: a
		// delivery the application never saw must not wait out its
		// acknowledgement deadline (the specification's "Close requeues or abandons
		// everything it has handed out").
		s.ring.Drain(func(d *mq.DriverDelivery) {
			if ref, ok := d.Ref.(*delivery); ok && ref.claimLost() {
				ref.settle.Nack()
			}
		})
		// And everything this source HANDED OUT and nobody settled. The specification allows
		// abandoning — the acknowledgement deadline would expire and the
		// message would be redelivered — and nacking is strictly better: the
		// message is deliverable at once rather than after the deadline, and
		// a consumer restarted immediately does not stall on a backlog its
		// predecessor still nominally holds.
		//
		// claimLost only takes a delivery that is still active or held, so a
		// settlement already in flight is left to finish and is not nacked
		// underneath itself. A delivery a redelivery superseded is passed
		// over by both, the drain above and this, and needs no nack here: its
		// message went back to the library the moment it was superseded, or
		// goes back when the settlement holding it then ends.
		s.abandon()
		if s.cancel != nil {
			s.cancel()
		}
		s.wg.Wait()
		s.holdWG.Wait()
		s.mu.Lock()
		dlq := s.dlq
		s.dlq = nil
		s.mu.Unlock()
		if dlq != nil {
			// Stop flushes, so a rejection admitted before Close is one that
			// still reaches the dead-letter topic.
			dlq.Stop()
		}
		s.b.forget(s, nil)
	})
	return nil
}

// abandon nacks every delivery this source handed out and nobody settled.
func (s *source) abandon() {
	s.mu.Lock()
	refs := make([]*delivery, 0, len(s.inflight))
	for _, ref := range s.inflight {
		refs = append(refs, ref)
	}
	s.inflight = map[string]*delivery{}
	s.mu.Unlock()
	for _, ref := range refs {
		if ref.claimLost() {
			ref.settle.Nack()
		}
	}
}

// endHolds ends every delayed negative settlement at once, and refuses any
// that arrives afterwards.
//
// The closing flag is set under the same mutex the registration takes, which
// is the established fix for the recurring defect the playbook records: a
// placement that checks a flag, a Close that drains, and the placement landing
// after the drain. Here the cost of getting it wrong is a message held
// unacknowledged by a source that no longer exists until its deadline expires
// — a delay silently becoming the acknowledgement deadline — plus a goroutine
// and a timer that outlive the client.
func (s *source) endHolds() {
	s.mu.Lock()
	s.closing = true
	held := make([]*delivery, 0, len(s.holds))
	for ref, timer := range s.holds {
		timer.Stop()
		held = append(held, ref)
	}
	s.holds = map[*delivery]mq.Timer{}
	s.mu.Unlock()
	for _, ref := range held {
		if ref.claimLost() {
			ref.settle.Nack()
		}
	}
}

// The delivery settlement states, which are the as one source sees them.
const (
	// stateActive is a delivery that has been handed out and not settled.
	stateActive uint32 = iota
	// stateSettling is a settlement in flight at the service.
	stateSettling
	// stateSettled is a delivery whose settlement the service took.
	stateSettled
	// stateUnknown is a delivery whose settlement was ambiguous. It is
	// sticky: nothing later turns it into a success.
	stateUnknown
	// stateLost is a delivery this source no longer vouches for, because the
	// message has been received again or the source is closing, its lease
	// has run out, or the service refused the one settlement the client
	// library sends for it.
	stateLost
	// stateHeld is a delivery waiting out a delayed negative settlement. It
	// is not terminal — the hold ends in a nack — and it is distinct from
	// stateSettling so that a second settlement during the hold is refused
	// with the right answer.
	stateHeld
)

// delivery is everything a settlement needs, and the only thing this driver
// boxes per delivery.
//
// The settlement handle is why. Pub/Sub's acknowledgement identifier is a
// string the service mints per DELIVERY, it is held privately inside the
// client library's message, and the only way to use it is to call the
// message's own Ack or Nack — which is the case
// [github.com/iqhive/iqmq.DriverDelivery.Ref] exists for.
type delivery struct {
	id     string
	settle psapi.Settlement
	native any
	gen    uint32
	src    *source

	// received and expires are the local view of the lease, as Unix
	// nanoseconds. They are atomic because the local lease check reads them
	// from whichever goroutine is settling while an extension writes them
	// from another, and because a time.Time is two words and cannot be read
	// atomically at all.
	received atomic.Int64
	expires  atomic.Int64
	state    atomic.Uint32
	// kept is the latest the client library can still be extending the
	// message, in Unix nanoseconds: the budget from when the callback was
	// handed it. The lease runs from the earliest moment the library can
	// have had it ([arrivals]), so the library's own budget ends somewhere
	// between the two. It is written before the delivery is shared and never
	// again.
	kept int64
}

// expiresAt is the local view of when the lease runs out.
func (d *delivery) expiresAt() time.Time { return time.Unix(0, d.expires.Load()) }

// receivedAt is when this delivery arrived.
func (d *delivery) receivedAt() time.Time { return time.Unix(0, d.received.Load()) }

// extend moves the local view of the lease to at, unless it already runs
// later: an extension never shortens a lease, which is what keeps an Extend
// shorter than the remaining budget from losing a delivery it was meant to
// keep. The compare-and-swap keeps two concurrent extensions from storing the
// earlier of the two.
func (d *delivery) extend(at time.Time) {
	next := at.UnixNano()
	for {
		cur := d.expires.Load()
		if next <= cur || d.expires.CompareAndSwap(cur, next) {
			return
		}
	}
}

// lose marks the delivery one this source no longer vouches for.
func (d *delivery) lose() { d.state.Store(stateLost) }

// expire takes a delivery whose lease has ended out of circulation, giving its
// message back to the client library while the library can still be extending
// it. A message nobody settles is extended until the library's budget runs out
// and holds the library's shutdown, and Close, until then; the lease can end
// first because it starts at the earliest moment the library can have had the
// message. Past kept the library has let go and the service may have given the
// message to another consumer, so nothing is sent.
func (d *delivery) expire(now time.Time) {
	if now.Before(time.Unix(0, d.kept)) {
		d.release()
		return
	}
	d.lose()
}

// claimLost takes an active or held delivery out of circulation, reporting
// whether it was still one this source vouched for. It is what Close uses so
// that a delivery being settled concurrently is not nacked underneath its own
// settlement.
func (d *delivery) claimLost() bool {
	return d.state.CompareAndSwap(stateActive, stateLost) ||
		d.state.CompareAndSwap(stateHeld, stateLost)
}

// release takes a delivery that a redelivery superseded out of circulation,
// and gives its message back to the client library or leaves it to the
// settlement holding the delivery. That settlement settles the message itself
// or, finding the delivery lost, nacks it ([delivery.giveBack],
// [settler.holdNack]).
//
// It reads the state again until one compare-and-swap lands, because a
// settlement can end between two reads. It used to store stateLost outright
// once claimLost failed, and a settlement that handed the delivery back in that
// gap had already found it not superseded and made it active again: the store
// turned that into a lost delivery nobody would ever settle, so the library
// went on extending its message and Close waited for it until MaxExtension.
func (d *delivery) release() {
	for {
		switch d.state.Load() {
		case stateActive, stateHeld:
			if d.claimLost() {
				d.settle.Nack()
				return
			}
		case stateSettling:
			if d.state.CompareAndSwap(stateSettling, stateLost) {
				return
			}
		default:
			// Settled, unknown or already lost. Nothing leaves those for
			// stateActive, so a plain store cannot overwrite a hand-back,
			// and the message has had its settlement or is left to the one
			// still in flight.
			d.lose()
			return
		}
	}
}

// giveBack returns a delivery that a settlement claimed and left unsettled, so
// that it may be settled again. One that a redelivery superseded meanwhile is
// no longer this source's to return: its message goes back to the client
// library instead, as [source.supersede] would have done had the settlement
// not held it, because nothing else will ever settle it and the library's
// shutdown waits until somebody does.
func (d *delivery) giveBack() {
	if d.state.CompareAndSwap(stateSettling, stateActive) {
		return
	}
	d.settle.Nack()
}

// expired reports whether the local view of the lease has run out.
//
// This is the local lease check, and it is the reason it exists: the service
// accepts an acknowledgement for an identifier whose message another consumer
// now holds, so an acknowledgement sent after the deadline passed does not
// fail — it acknowledges somebody else's work. There is no API that asks "do I
// still hold this?", so the only place the refusal can happen is here, before
// the call.
//
// It is a LOCAL view and the driver says so. It cannot see a deadline the
// service moved for its own reasons, and it deliberately refuses on a lease
// this process believes has passed even when the service would still have
// accepted the acknowledgement. Both directions of that trade are the safe
// one: the cost is a redelivery, and the alternative is acknowledging somebody
// else's work.
func (d *delivery) expired(now time.Time) bool {
	return !now.Before(d.expiresAt())
}

// stateError maps a delivery's current state to the answer the specification gives a second
// settlement.
func stateError(state uint32) error {
	switch state {
	case stateSettling:
		return mq.ErrSettlementInProgress
	case stateHeld:
		return mq.ErrSettlementInProgress
	case stateSettled:
		return mq.ErrAlreadySettled
	case stateUnknown:
		return mq.ErrSettlementUnknown
	case stateLost:
		return mq.ErrLeaseLost
	case stateActive:
		return nil
	default:
		return mq.ErrLeaseLost
	}
}

// wholeSeconds rounds a duration UP to whole seconds, which is the unit the
// service's acknowledgement deadline is expressed in.
func wholeSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}

// newTimer is the driver's one timer constructor, on the injected clock.
func (b *Backend) newTimer(d time.Duration) mq.Timer {
	if b.clock != nil {
		return b.clock.NewTimer(d)
	}
	return wallTimer{time.NewTimer(d)}
}

// wallTimer is the default [github.com/iqhive/iqmq.Timer], for a
// configuration that injected no clock.
type wallTimer struct{ t *time.Timer }

// C is the expiry channel.
func (w wallTimer) C() <-chan time.Time { return w.t.C }

// Stop prevents the timer from firing.
func (w wallTimer) Stop() bool { return w.t.Stop() }

// Reset re-arms the timer.
func (w wallTimer) Reset(d time.Duration) bool { return w.t.Reset(d) }

// republish sends one message to the dead-letter topic, which is the specification's
// Reject on this broker (see [settler.reject]).
//
// The publisher is created once, lazily, under the source's mutex, so a
// consumer that never rejects anything pays nothing for the route and two
// concurrent rejections share one publisher. It carries the message
// UNCHANGED: the five provenance headers are the RUNNER's route and not the
// topology's , so a driver that stamped its own would put
// two different provenance schemes on one message.
func (s *source) republish(ctx context.Context, m *mq.Message) error {
	pub, err := s.deadLetterPublisher()
	if err != nil {
		return err
	}
	out, err := s.enc.encode(m, nil)
	if err != nil {
		return err
	}
	if _, perr := pub.Publish(ctx, &out.Out).Get(ctx); perr != nil {
		resumeKey(pub, out.Out.OrderingKey)
		return s.mapDeadLetter(perr)
	}
	return nil
}

// mapDeadLetter maps a dead-letter publication that failed: by the
// PUBLICATION rules, because a publication is what failed, and reported as the
// negative settlement it is part of.
//
// It used to be mapped by the settlement rules, which read every service
// answer as definite. That is right for a settlement, which leaves the message
// where it was when it fails, and wrong for a publication, which may already
// be stored: a DEADLINE_EXCEEDED can always come after the service stored the
// copy, an UNAVAILABLE can when the stream broke mid-RPC, and the client
// library retries a Publish on every transient code until its timeout, so
// whichever code the last attempt got, an earlier attempt may have been
// stored. Reported as definite, the Reject read as one that had not happened,
// the delivery stayed settleable, and a retried Reject put a second copy in the
// dead-letter topic. The publication rules make those codes ambiguous (see
// [newErrorMapper]), which the core turns into a refused second settlement.
//
// MapNet is still what is called, with the request counted as sent: a wait
// that ended (the settlement's context, or the library's own publish timeout)
// says nothing about whether the publication left the bundler, and a context
// error matches no rule. A refusal the service answered, such as a topic that
// does not exist or a credential that may not publish to it, stays definite.
func (s *source) mapDeadLetter(err error) error {
	mapped := s.b.errs.MapNet(mq.OpPublish, s.b.cfg.DeadLetterTopic, s.cfg.Subscription, err, true)
	var oe *mq.OpError
	if errors.As(mapped, &oe) {
		oe.Operation = mq.OpNack
	}
	return mapped
}

// deadLetterPublisher returns the source's dead-letter publisher, creating it
// on first use.
func (s *source) deadLetterPublisher() (psapi.Publisher, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil, mq.ErrClosed
	}
	if s.dlq != nil {
		return s.dlq, nil
	}
	topic, err := s.b.cfg.TopicID(s.b.cfg.DeadLetterTopic)
	if err != nil {
		return nil, err
	}
	pub, err := s.b.api.Publisher(psapi.PublisherConfig{
		Topic:          topic,
		Ordering:       s.b.cfg.Ordering,
		CountThreshold: 1,
		DelayThreshold: time.Millisecond,
		Timeout:        s.b.cfg.PublishTimeout,
	})
	if err != nil {
		return nil, s.b.errs.Map(mq.OpNack, s.b.cfg.DeadLetterTopic, s.cfg.Subscription, err)
	}
	s.dlq = pub
	return pub, nil
}
