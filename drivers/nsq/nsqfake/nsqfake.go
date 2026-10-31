// Package nsqfake is an in-process nsqd for this driver's tests.
//
//   - a per-message timeout that expires, which real nsqd takes tens of
//     seconds to do and which this does in one call to [Server.Advance];
//   - a message REDELIVERED to a second consumer while the first still holds a
//     handle to it, which is the case the driver's local lease check exists for
//     and which a real nsqd cannot be made to do on demand;
//   - a requeue with a delay, so that nsqd's own delayed redelivery is
//     exercised rather than described;
//   - a FIN, REQ or TOUCH for a message the sender no longer holds. nsqd
//     accepts all three by message identifier and answers none of them, so a
//     stale settlement lands on whatever is now under that identifier. This
//     fake does exactly that, and counts it: a fake that refused a stale
//     settlement would make the driver's defence untestable, because the test
//     would pass with the defence removed.
//
// It is a MODEL of nsqd and not a reimplementation. What it reproduces is the
// message lifecycle a channel has — pending, deferred, in flight, finished —
// the fan-out of a topic to its channels, the per-message timeout, and the
// settlement verbs' exact laxity. What it does not reproduce is the wire
// protocol, RDY accounting or lookupd's discovery; the container-backed
// conformance suite is what says the fake did not lie.
package nsqfake

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	gonsq "github.com/nsqio/go-nsq"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/nsq/internal/nsqapi"
)

// The compile-time proof that the model is a clock the driver and the core can
// both read.
var _ mq.Clock = (*Server)(nil)

// Server is an in-process nsqd.
//
// Its clock is its own and never the wall clock: every deadline in the model
// moves only when [Server.Advance] is called, so a test that needs an expired
// lease does not wait for one. It satisfies iqmq's Clock interface, so handing
// it to Config.Clock makes the driver and the model agree about the time.
type Server struct {
	mu     sync.Mutex
	offset time.Duration
	topics map[string]*topicState

	// publishErr is injected: the next n publications fail with it.
	publishErr   error
	publishFails int

	published int
	stopped   bool
}

// New returns an empty server.
func New() *Server {
	return &Server{topics: map[string]*topicState{}}
}

// Now is the model's clock: the wall clock plus whatever [Server.Advance] has
// added to it.
//
// It is an OFFSET on the real clock rather than a stored instant, and the
// reason is that the same value is handed to the core as its Clock: the core's
// lease scheduler compares a deadline against Now and then waits on a timer,
// so a Now that stood still while timers ran on real time would make every
// such comparison wrong by however long the test had been running. An offset
// keeps the two in step, and jumping it forward is still the only way an
// expired lease happens here.
func (s *Server) Now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Now().Add(s.offset)
}

// NewTimer is the real clock's timer.
//
// The model virtualises INSTANTS and not durations: nothing in this driver
// schedules work on a clock, so there is no timer here to fake, and a timer
// that waited for a virtual advance would deadlock any caller that did.
func (s *Server) NewTimer(d time.Duration) mq.Timer { return realTimer{t: time.NewTimer(d)} }

// realTimer adapts *time.Timer to iqmq's Timer.
type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Stop() bool                 { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// Advance moves the clock, expires every in-flight message whose timeout has
// passed and releases every deferred message that is now due.
//
// An expired in-flight message goes back to the front of the pending list with
// its attempt count raised, which is what nsqd does — and it is the ONLY way a
// second delivery of one message can be produced, so it is the entry point for
// every stale-settlement test.
func (s *Server) Advance(d time.Duration) {
	s.mu.Lock()
	s.offset += d
	now := time.Now().Add(s.offset)
	for _, t := range s.topics {
		for _, c := range t.channels {
			c.expireLocked(now)
		}
	}
	s.mu.Unlock()
	s.wake()
}

// Redeliver expires every in-flight message of one channel immediately,
// whatever its timeout says, and without moving the clock.
//
// It is nsqd timing a delivery out before the consumer's own view of the lease
// says it has, which happens when a delivery reached the consumer late: nsqd's
// timeout starts when it sends the message. The handles the previous consumer
// holds stay valid as far as nsqd is concerned, which is the point.
//
// It is not what a lost connection does. nsqd does not requeue a lost
// connection's messages when the connection goes: each one stays in flight
// until its own timeout, so a connection loss is modelled by [Server.Advance]
// past the timeout.
func (s *Server) Redeliver(topic, channel string) {
	s.mu.Lock()
	if c := s.channelLocked(topic, channel); c != nil {
		c.expireAllLocked()
	}
	s.mu.Unlock()
	s.wake()
}

// FailPublishes makes the next n publications fail with err.
func (s *Server) FailPublishes(n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishFails, s.publishErr = n, err
}

// Published is how many messages have been accepted.
func (s *Server) Published() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.published
}

// Stats reports one channel's model state.
func (s *Server) Stats(topic, channel string) Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.channelLocked(topic, channel)
	if c == nil {
		return Stats{}
	}
	return Stats{
		Depth:        len(c.pending),
		Deferred:     len(c.deferred),
		InFlight:     len(c.inflight),
		Finished:     c.finished,
		Requeued:     c.requeued,
		Touched:      c.touched,
		CrossSettled: c.crossSettled,
		CrossTouched: c.crossTouched,
		LateSettled:  c.lateSettled,
		Delivered:    c.delivered,
		BackoffAsked: c.backoffAsked,
	}
}

// Stats is what the model knows about one channel.
//
// The last three are the numbers the driver's correctness rests on, and they
// are the reason this type is exported at all:
//
//	CrossSettled  a FIN or REQ that settled a LATER delivery of the message
//	              it named. Every one of these is the specification's "lets a stale ack
//	              settle a later redelivery" having happened.
//	CrossTouched  a TOUCH that extended a later delivery's lease.
//	LateSettled   a settlement for a message the channel no longer had in
//	              flight, which nsqd answers with E_FIN_FAILED and which
//	              costs nothing.
type Stats struct {
	// Depth, Deferred and InFlight are the channel's queue lengths: ready,
	// waiting out a requeue delay, and handed to a client and not yet
	// settled.
	Depth    int
	Deferred int
	InFlight int
	// Finished, Requeued and Touched count the settlements the channel
	// accepted, one per FIN, REQ and TOUCH that named a current delivery.
	Finished     int
	Requeued     int
	Touched      int
	CrossSettled int
	CrossTouched int
	LateSettled  int
	// Delivered is how many deliveries the channel has handed out, counting
	// every redelivery separately.
	Delivered int
	// BackoffAsked is how many requeues asked go-nsq for its own backoff,
	// which sets RDY to zero for the whole subscription. It must always be
	// zero: the core owns the retry schedule.
	BackoffAsked int
}

// Idle reports whether no consumer is currently inside a handler.
//
// It exists because "the broker has delivered a message" and "the driver has
// recorded it" are two different moments, and a test that needs the second
// cannot see it from the first: take() counts a message in flight before the
// handler runs, so a test that advanced the clock as soon as InFlight rose
// could advance it BETWEEN the two — and the driver would then record a lease
// measured from the advanced clock, which is a live delivery where the test
// wanted a dead one. That made TestFetchDropsADeliveryThatWentDeadInTheRing
// fail one run in three.
func (s *Server) Idle() bool {
	s.mu.Lock()
	cons := make([]*consumer, 0, 8)
	for _, t := range s.topics {
		for _, c := range t.channels {
			cons = append(cons, c.consumers...)
		}
	}
	s.mu.Unlock()
	for _, c := range cons {
		if !c.idle.Load() {
			return false
		}
	}
	return true
}

// Producer returns a producer that publishes into this server.
func (s *Server) Producer() nsqapi.Producer { return &producer{s: s} }

// Consumer returns a consumer bound to one topic and channel, creating both.
func (s *Server) Consumer(topic, channel string, maxInFlight int, deadline time.Duration) nsqapi.Consumer {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.ensureChannelLocked(topic, channel)
	con := &consumer{s: s, ch: c, maxInFlight: maxInFlight, deadline: deadline, stop: make(chan int)}
	c.consumers = append(c.consumers, con)
	return con
}

// EnsureChannel creates a topic and a channel without a consumer, which is what
// the driver's own EnsureTopology does.
func (s *Server) EnsureChannel(topic, channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureChannelLocked(topic, channel)
}

// wake nudges every consumer's delivery loop after the model changed.
func (s *Server) wake() {
	s.mu.Lock()
	cons := make([]*consumer, 0, 8)
	for _, t := range s.topics {
		for _, c := range t.channels {
			cons = append(cons, c.consumers...)
		}
	}
	s.mu.Unlock()
	for _, c := range cons {
		c.nudge()
	}
}

func (s *Server) channelLocked(topic, channel string) *channelState {
	t := s.topics[topic]
	if t == nil {
		return nil
	}
	return t.channels[channel]
}

func (s *Server) ensureChannelLocked(topic, channel string) *channelState {
	t := s.topics[topic]
	if t == nil {
		t = &topicState{channels: map[string]*channelState{}}
		s.topics[topic] = t
	}
	c := t.channels[channel]
	if c == nil {
		c = &channelState{name: channel, topic: topic, server: s}
		// nsqd holds a topic's messages until it has a channel and then
		// hands the backlog to the channel that appears, so a publication
		// that preceded every subscriber is not lost.
		for _, m := range t.backlog {
			c.pending = append(c.pending, m.copy())
		}
		t.channels[channel] = c
	}
	return c
}

// topicState is one topic: its channels and the backlog it holds before it has
// any.
type topicState struct {
	channels map[string]*channelState
	backlog  []*msgState
}

// channelState is one channel's message lifecycle.
type channelState struct {
	name   string
	topic  string
	server *Server

	pending  []*msgState
	deferred []*deferredMsg
	inflight map[gonsq.MessageID]*msgState

	consumers []*consumer

	finished     int
	requeued     int
	touched      int
	crossSettled int
	crossTouched int
	lateSettled  int
	delivered    int
	backoffAsked int
}

// msgState is one message in the model.
type msgState struct {
	id      gonsq.MessageID
	body    []byte
	attempt uint16
	// timeout is when this delivery's lease expires, valid while in flight.
	timeout time.Time
}

func (m *msgState) copy() *msgState {
	c := *m
	return &c
}

// deferredMsg is a message nsqd is holding back.
type deferredMsg struct {
	msg *msgState
	at  time.Time
}

// put adds a message to the channel, honouring a delay.
func (c *channelState) put(m *msgState, delay time.Duration, now time.Time) {
	if delay > 0 {
		c.deferred = append(c.deferred, &deferredMsg{msg: m, at: now.Add(delay)})
		return
	}
	c.pending = append(c.pending, m)
}

// expireLocked moves due deferrals into pending and expired in-flight messages
// back to pending with a raised attempt count.
func (c *channelState) expireLocked(now time.Time) {
	kept := c.deferred[:0]
	for _, d := range c.deferred {
		if !d.at.After(now) {
			c.pending = append(c.pending, d.msg)
			continue
		}
		kept = append(kept, d)
	}
	c.deferred = kept
	for id, m := range c.inflight {
		if m.timeout.After(now) {
			continue
		}
		delete(c.inflight, id)
		m.attempt++
		c.pending = append(c.pending, m)
	}
}

// expireAllLocked makes every in-flight message available again at once.
func (c *channelState) expireAllLocked() {
	for id, m := range c.inflight {
		delete(c.inflight, id)
		m.attempt++
		c.pending = append(c.pending, m)
	}
}

// take claims the next pending message for delivery, or nil.
func (c *channelState) take(now time.Time, maxInFlight int, deadline time.Duration) (*msgState, uint16) {
	if len(c.inflight) >= maxInFlight || len(c.pending) == 0 {
		return nil, 0
	}
	m := c.pending[0]
	c.pending = c.pending[1:]
	if m.attempt == 0 {
		m.attempt = 1
	}
	m.timeout = now.Add(deadline)
	if c.inflight == nil {
		c.inflight = map[gonsq.MessageID]*msgState{}
	}
	c.inflight[m.id] = m
	c.delivered++
	return m, m.attempt
}

// settle applies a FIN or a REQ that names one delivery attempt.
//
// This is where nsqd's laxity lives. The verb names a message IDENTIFIER, and
// nsqd applies it to whatever it currently has in flight under that identifier
// — so a settlement from an attempt that has already been superseded settles
// the CURRENT attempt instead, and nothing anywhere reports it. Counting it is
// the only way a test can prove the driver never does it.
func (c *channelState) settle(id gonsq.MessageID, attempt uint16, requeue bool, delay time.Duration, now time.Time) {
	m := c.inflight[id]
	if m == nil {
		c.lateSettled++
		return
	}
	if m.attempt != attempt {
		c.crossSettled++
	}
	delete(c.inflight, id)
	if requeue {
		c.requeued++
		m.attempt++
		c.put(m, delay, now)
		return
	}
	c.finished++
}

// touch resets a delivery's timeout, with the same laxity as settle.
func (c *channelState) touch(id gonsq.MessageID, attempt uint16, now time.Time, deadline time.Duration) {
	m := c.inflight[id]
	if m == nil {
		c.lateSettled++
		return
	}
	if m.attempt != attempt {
		c.crossTouched++
	}
	c.touched++
	m.timeout = now.Add(deadline)
}

// producer publishes into the server.
type producer struct{ s *Server }

func (p *producer) Publish(ctx context.Context, topic string, body []byte) error {
	return p.put(ctx, topic, [][]byte{body}, 0)
}

func (p *producer) MultiPublish(ctx context.Context, topic string, bodies [][]byte) error {
	return p.put(ctx, topic, bodies, 0)
}

func (p *producer) DeferredPublish(ctx context.Context, topic string, delay time.Duration, body []byte) error {
	return p.put(ctx, topic, [][]byte{body}, delay)
}

func (p *producer) Ping(ctx context.Context) error { return ctx.Err() }

func (p *producer) Close() {
	p.s.mu.Lock()
	p.s.stopped = true
	p.s.mu.Unlock()
}

func (p *producer) Addr() string { return "nsqfake" }

// Consumer makes the producer a
// [github.com/iqhive/iqmq/drivers/nsq/internal/nsqapi.ConsumerFactory], so
// that handing one to OpenWithClient replaces the whole transport rather than
// only the publishing half.
func (p *producer) Consumer(topic, channel string, maxInFlight int, deadline time.Duration) nsqapi.Consumer {
	return p.s.Consumer(topic, channel, maxInFlight, deadline)
}

// put is the model's PUB, MPUB and DPUB.
func (p *producer) put(ctx context.Context, topic string, bodies [][]byte, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.s.mu.Lock()
	if p.s.stopped {
		p.s.mu.Unlock()
		return gonsq.ErrStopped
	}
	if p.s.publishFails > 0 {
		p.s.publishFails--
		err := p.s.publishErr
		p.s.mu.Unlock()
		if err == nil {
			err = errors.New("nsqfake: injected publish failure")
		}
		return err
	}
	for _, body := range bodies {
		if len(body) == 0 {
			p.s.mu.Unlock()
			return gonsq.ErrProtocol{Reason: "E_BAD_MESSAGE PUB invalid message body size 0"}
		}
		m := &msgState{id: newID(), body: append([]byte(nil), body...)}
		t := p.s.topics[topic]
		if t == nil {
			t = &topicState{channels: map[string]*channelState{}}
			p.s.topics[topic] = t
		}
		if len(t.channels) == 0 {
			t.backlog = append(t.backlog, m)
		}
		for _, c := range t.channels {
			c.put(m.copy(), delay, time.Now().Add(p.s.offset))
		}
		p.s.published++
	}
	p.s.mu.Unlock()
	p.s.wake()
	return nil
}

// consumer is one subscription's delivery loop.
type consumer struct {
	s           *Server
	ch          *channelState
	maxInFlight int
	deadline    time.Duration

	mu       sync.Mutex
	handler  nsqapi.Handler
	started  bool
	stopping bool

	// idle is false while a handler is running, which is what Server.Idle
	// reports.
	idle atomic.Bool

	wake chan struct{}
	stop chan int
	done sync.Once
	wg   sync.WaitGroup
}

func (c *consumer) AddConcurrentHandlers(h nsqapi.Handler, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handler = h
}

func (c *consumer) ConnectToNSQLookupds(addrs []string) error { return c.start() }

func (c *consumer) ConnectToNSQDs(addrs []string) error { return c.start() }

// ChangeMaxInFlight is the consumer's RDY: the delivery loop takes no message
// while as many as n are in flight, so zero stops the flow.
func (c *consumer) ChangeMaxInFlight(n int) {
	c.s.mu.Lock()
	c.maxInFlight = n
	c.s.mu.Unlock()
}

// start begins the delivery loop.
//
// There is one loop and not a pool, and the difference matters: a blocked
// handler stops delivery, which is exactly the backpressure the driver's push
// adapter is supposed to apply. A pool would hide a driver that buffered
// without bound.
func (c *consumer) start() error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return gonsq.ErrAlreadyConnected
	}
	if c.handler == nil {
		c.mu.Unlock()
		return errors.New("nsqfake: no handler; go-nsq panics on a connect before AddConcurrentHandlers")
	}
	c.started = true
	c.wake = make(chan struct{}, 1)
	c.mu.Unlock()
	c.idle.Store(true)
	c.wg.Add(1)
	go c.loop()
	return nil
}

func (c *consumer) nudge() {
	c.mu.Lock()
	ch := c.wake
	c.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (c *consumer) loop() {
	defer c.wg.Done()
	for {
		c.mu.Lock()
		stopping, h, ch := c.stopping, c.handler, c.wake
		c.mu.Unlock()
		if stopping {
			return
		}
		c.s.mu.Lock()
		m, attempt := c.ch.take(time.Now().Add(c.s.offset), c.maxInFlight, c.deadline)
		if m != nil {
			// Busy from the moment the message is in flight, under the
			// lock that counts it: Server.Idle's promise is that a caller
			// who has seen InFlight rise and then Idle report true is past
			// the handler. Marking busy after the unlock left a window in
			// which InFlight had risen and Idle still said true, and a test
			// that advanced the clock in it made the driver record a lease
			// from the advanced clock -- TestFetchDropsADeliveryThatWentDeadInTheRing,
			// about one run in fifteen hundred.
			c.idle.Store(false)
		}
		c.s.mu.Unlock()
		if m == nil {
			select {
			case <-ch:
			case <-time.After(2 * time.Millisecond):
			}
			continue
		}
		msg := gonsq.NewMessage(m.id, append([]byte(nil), m.body...))
		msg.Attempts = attempt
		msg.NSQDAddress = "nsqfake"
		msg.Delegate = &msgDelegate{c: c, attempt: attempt}
		_ = h.HandleMessage(msg)
		c.idle.Store(true)
	}
}

func (c *consumer) Stop() {
	c.mu.Lock()
	c.stopping = true
	c.mu.Unlock()
	c.nudge()
	go func() {
		c.wg.Wait()
		c.done.Do(func() { close(c.stop) })
	}()
}

func (c *consumer) WaitStopped(ctx context.Context) error {
	select {
	case <-c.stop:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// msgDelegate is one delivery's settlement handle.
//
// It carries the ATTEMPT it belongs to, which is what makes a stale settlement
// detectable in the model: nsqd has no such field, so it applies whatever
// arrives to whatever it currently holds.
type msgDelegate struct {
	c       *consumer
	attempt uint16
}

func (d *msgDelegate) OnFinish(m *gonsq.Message) {
	d.c.s.mu.Lock()
	d.c.ch.settle(m.ID, d.attempt, false, 0, time.Now().Add(d.c.s.offset))
	d.c.s.mu.Unlock()
	d.c.nudge()
}

func (d *msgDelegate) OnRequeue(m *gonsq.Message, delay time.Duration, backoff bool) {
	d.c.s.mu.Lock()
	if backoff {
		// The driver must never ask for this. go-nsq's Requeue (as
		// opposed to RequeueWithoutBackoff) sets the consumer's RDY to
		// zero for the whole subscription while a backoff of its own
		// runs, so one message's failure stops every other message —
		// a second owner of the redelivery policy the specification gives to one
		// layer. Counting it is what makes a driver that asks fail a
		// test rather than merely behave oddly under load.
		d.c.ch.backoffAsked++
	}
	d.c.ch.settle(m.ID, d.attempt, true, delay, time.Now().Add(d.c.s.offset))
	d.c.s.mu.Unlock()
	d.c.nudge()
}

func (d *msgDelegate) OnTouch(m *gonsq.Message) {
	d.c.s.mu.Lock()
	d.c.ch.touch(m.ID, d.attempt, time.Now().Add(d.c.s.offset), d.c.deadline)
	d.c.s.mu.Unlock()
}

// newID is a random sixteen-byte hexadecimal identifier, which is the shape
// nsqd's own identifiers have.
func newID() gonsq.MessageID {
	var raw [8]byte
	_, _ = rand.Read(raw[:])
	var id gonsq.MessageID
	hex.Encode(id[:], raw[:])
	return id
}
