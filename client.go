package iqmq

import (
	"context"
	"errors"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/lifecycle"
)

// Client is one connection to one broker.
//
// It is a concrete struct and not an interface, which is the winner:
// a caller holds a *Client, and the narrow interfaces —
// Publisher, Handler, Backend — are for the places where substitution is the
// point. A driver's Open builds one over its Backend with NewClient.
//
// Every method is safe for concurrent use. Close and Shutdown are
// idempotent, and either closes every publisher and consumer the client
// opened.
//
// The client holds no global state: the specification makes that a property of the
// package, and it is what lets two clients of the same driver coexist in one
// process.
type Client struct {
	backend Backend
	info    DriverInfo
	caps    Capabilities
	clock   clock.Clock
	hooks   Hooks

	// admin and health are the optional backend interfaces, asserted once
	// here so that no call site repeats the assertion.
	admin Admin
	// adminErr is what AdminBackend.Admin answered, when it failed.
	adminErr error
	health   HealthReporter
	// closing is the backend's ClosingBackend step, when it has one.
	closing ClosingBackend

	// err is a construction failure. NewClient returns a *Client and not an
	// error — the frozen signature — so a client built
	// over a nil backend or a malformed option reports the failure from
	// every operation instead of panicking.
	err error

	life *lifecycle.Lifecycle

	// mu guards the child list, the publisher map's writers and drainSeen.
	mu       sync.Mutex
	children []childHandle
	// drainSeen is the longest graceful bound drainTimeout has answered
	// with, so that a Shutdown joining one under way, once the drain has
	// taken the consumers off the list, is bounded as the first one was.
	drainSeen time.Duration
	// pubs is Client.Publish's bound publishers, one per topic, published
	// as an immutable map behind an atomic pointer: a publication reads it
	// without a lock (rule 5) and only a first publication to a new
	// topic writes.
	pubs atomic.Pointer[map[string]*PublisherHandle]

	// unfinished are the handles whose shutdown the drain left still going,
	// for closeNative to wait for before the backend closes. Only the run's
	// goroutine touches it: drainChildren writes it and closeNative reads
	// it, both steps of the run.
	unfinished []childHandle
}

// childHandle is a publisher or a consumer, from the client's point of view:
// something to flush, drain and close in the order.
type childHandle interface {
	Shutdown(ctx context.Context) error
	Close() error
	// beginClose begins what Close does without waiting for the close to
	// finish.
	beginClose()
	closed() bool
	// finished is closed once the handle's own close has run to its end.
	finished() <-chan struct{}
	// stuck is closed once the handle's own shutdown has outlived its bound
	// with an operation still parked, which may end only when the backend
	// closes (lifecycle.Lifecycle.Stuck).
	stuck() <-chan struct{}
}

// clientField names the setting a ClientOption carries. The zero value names
// none, so a stray ClientOption{} is refused rather than silently doing
// nothing.
type clientField uint8

const (
	clientFieldNone clientField = iota
	clientFieldClock
	clientFieldHooks

	clientFieldCount
)

// ClientOption is one client-wide setting.
//
// It is a value type and not a closure, which is the winner,
// and its fields are unexported, so an option can only come from a
// constructor this package exports.
type ClientOption struct {
	field clientField
	clock Clock
	hooks Hooks
}

// WithClock makes c the time source every part of the client reads (
// rule 4). The default is the wall clock, which follows the bubble clock
// inside testing/synctest.
//
// It is the one place a caller's Clock crosses into iqmq, and it is where the
// adapter internal/clock describes lives: the two packages declare the same
// method sets and Go's interface satisfaction is invariant in return types,
// so the boundary is a one-line re-wrap rather than an assignment.
// Nothing on a hot path crosses it — a clock is taken
// once per handle, and the specification forbids a timer per delivery.
func WithClock(c Clock) ClientOption {
	return ClientOption{field: clientFieldClock, clock: c}
}

// WithHooks sets the client's observation points. Every handle the
// client opens is built with them; a nil hook costs nothing.
func WithHooks(h Hooks) ClientOption {
	return ClientOption{field: clientFieldHooks, hooks: h}
}

// NewClient builds a client over a driver's backend.
//
// It performs no I/O: the backend is already open, and the specification forbids Open from
// creating production topology, so a client is ready the moment it exists.
//
// A nil backend, or a malformed option, is reported by every operation the
// client offers rather than by a panic here: the frozen signature returns no
// error.
func NewClient(backend Backend, opts ...ClientOption) *Client {
	c := &Client{
		backend: backend,
		clock:   clock.Real{},
	}
	for _, o := range opts {
		switch o.field {
		case clientFieldClock:
			if o.clock == nil {
				c.setErr(errNilClock)
				continue
			}
			c.clock = clockBridge{c: o.clock}
		case clientFieldHooks:
			c.hooks = o.hooks
		case clientFieldNone, clientFieldCount:
			c.setErr(errNoClientOption)
		default:
			c.setErr(errNoClientOption)
		}
	}
	if backend == nil {
		c.setErr(errNilBackend)
	} else {
		c.info = backend.Info()
		c.caps = backend.Capabilities()
		if ab, ok := backend.(AdminBackend); ok {
			// The error is KEPT rather than dropped. A driver that declares
			// AdminBackend and then cannot build the surface -- a management
			// endpoint that is not configured, a credential the topology API
			// refuses -- is not a driver without topology, and answering
			// ErrUnsupported from Client.Admin said it was. NewClient does
			// not fail for it, because publishing and consuming do not need
			// the surface; the error is reported to whoever asks for it.
			adm, err := ab.Admin()
			if err != nil {
				c.adminErr = err
			} else {
				c.admin = adm
			}
		}
		if hr, ok := backend.(HealthReporter); ok {
			c.health = hr
		}
		if cb, ok := backend.(ClosingBackend); ok {
			c.closing = cb
		}
	}
	empty := map[string]*PublisherHandle{}
	c.pubs.Store(&empty)
	c.life = lifecycle.New(lifecycle.Config{
		Clock:          c.clock,
		DrainTimeoutOf: c.drainTimeout,
		Steps: lifecycle.Steps{
			// the order for a client: publishers flush, consumers drain,
			// publishers close, then the backend closes. The Flush and
			// Drain steps run only for a graceful Shutdown; a prompt Close
			// goes straight to closing the children and the backend.
			Flush:       c.flushChildren,
			Drain:       c.drainChildren,
			CloseNative: c.closeNative,
		},
	})
	return c
}

// setErr records the first construction failure.
func (c *Client) setErr(err error) {
	if c.err == nil {
		c.err = err
	}
}

// Publisher opens a publisher bound to one destination (the specification's
// config struct).
func (c *Client) Publisher(ctx context.Context, cfg PublisherConfig) (*PublisherHandle, error) {
	if err := c.ready(); err != nil {
		return nil, err
	}
	p, err := newPublisher(ctx, c, cfg)
	if err != nil {
		return nil, err
	}
	if err := c.adopt(p); err != nil {
		//nolint:contextcheck // a handle that was never returned is closed promptly; there is nothing admitted to drain
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

// Consumer opens a consumer on one subscription (the config
// struct).
func (c *Client) Consumer(ctx context.Context, cfg ConsumerConfig) (*ConsumerHandle, error) {
	if err := c.ready(); err != nil {
		return nil, err
	}
	h, err := newConsumer(ctx, c, cfg)
	if err != nil {
		return nil, err
	}
	if err := c.adopt(h); err != nil {
		//nolint:contextcheck // a handle that was never returned is closed promptly; there is nothing admitted to drain
		_ = h.Close()
		return nil, err
	}
	return h, nil
}

// Publish sends one message to a topic without the caller holding a
// publisher.
//
// It is the convenience path and not the fast one: the client keeps a bound
// publisher per topic it has published to, which is the winner
// for a stable destination set and unbounded for a
// high-cardinality one. The specification marks the size-bounded cache as its own
// bake-off row, so a caller publishing to unboundedly many topics opens its
// own publishers today.
//
// An empty topic falls back to m.Topic, which is what makes Message.Topic
// meaningful on this path and ignored on a bound publisher's.
func (c *Client) Publish(ctx context.Context, topic string, m *Message, opts ...PublishOption) error {
	if err := c.ready(); err != nil {
		return err
	}
	if m == nil {
		return errNilMessage
	}
	if topic == "" {
		topic = m.Topic
	}
	if topic == "" {
		// Neither the call nor the message named a destination. The specification's
		// ErrInvalidTopic is the answer a caller can branch on; the
		// publisher's own configuration failure would name a struct the
		// caller never filled in.
		return ErrInvalidTopic
	}
	p, err := c.publisherFor(ctx, topic)
	if err != nil {
		// The publication failed before any driver saw it -- most often a
		// sink that would not open, which is the failure a misconfigured
		// deployment meets FIRST. It reached no hook, so the failure
		// count was blind to exactly that case.
		c.publishFailedBeforeTheDriver(ctx, topic, m, err)
		return err
	}
	return p.Publish(ctx, m, opts...)
}

// publishFailedBeforeTheDriver reports a publication that never reached a
// sink to the done hook. Seq is 0: there was no start event to pair with,
// because nothing was ever handed to a driver.
func (c *Client) publishFailedBeforeTheDriver(ctx context.Context, topic string, m *Message, err error) {
	if c.hooks.OnPublishDone == nil {
		return
	}
	var bytes int64
	if m != nil {
		bytes = int64(len(m.Body))
	}
	c.hooks.OnPublishDone(ctx, PublishDoneEvent{
		Topic:    topic,
		Messages: 1,
		Bytes:    bytes,
		Err:      err,
	})
}

// publisherFor returns the client's bound publisher for a topic, opening one
// the first time.
func (c *Client) publisherFor(ctx context.Context, topic string) (*PublisherHandle, error) {
	if p := (*c.pubs.Load())[topic]; p != nil {
		return p, nil
	}
	p, err := newPublisher(ctx, c, PublisherConfig{Topic: topic})
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	// Another publication may have opened one for this topic while this one
	// was opening its own; the first one published wins and the loser is
	// closed, so the client never holds two sinks for one topic.
	old := *c.pubs.Load()
	if existing := old[topic]; existing != nil {
		c.mu.Unlock()
		//nolint:contextcheck // the publisher that lost the race was never used; a prompt Close is all it needs
		_ = p.Close()
		return existing, nil
	}
	if c.life.Closing() {
		c.mu.Unlock()
		//nolint:contextcheck // the publisher was never used; a prompt Close is all it needs
		_ = p.Close()
		return nil, ErrClosed
	}
	next := make(map[string]*PublisherHandle, len(old)+1)
	for k, v := range old {
		next[k] = v
	}
	next[topic] = p
	c.pubs.Store(&next)
	c.children = append(c.children, p)
	c.mu.Unlock()
	return p, nil
}

// Info describes the driver behind this client.
func (c *Client) Info() DriverInfo { return c.info }

// Capabilities is what this client can and cannot do. It is constant
// for the life of the client.
func (c *Client) Capabilities() Capabilities { return c.caps }

// Admin returns the topology surface, or ErrUnsupported when the
// driver has none. Ensure operations are idempotent and destructive ones are
// explicit; Open never creates production topology.
func (c *Client) Admin() (Admin, error) {
	if err := c.ready(); err != nil {
		return nil, err
	}
	if c.admin == nil {
		if c.adminErr != nil {
			return nil, c.adminErr
		}
		return nil, ErrUnsupported
	}
	return c.admin, nil
}

// Native returns the driver's own client for callers who need the escape
// hatch. It is nil when the driver offers none.
func (c *Client) Native() any {
	if c.backend == nil {
		return nil
	}
	return c.backend.Native()
}

// Ping is a cheap liveness check. It never consumes a message and
// never creates topology.
func (c *Client) Ping(ctx context.Context) error {
	if err := c.ready(); err != nil {
		return err
	}
	return c.backend.Ping(ctx)
}

// Health is the client's current state. It performs no I/O: a caller
// polling it never becomes load on the broker.
func (c *Client) Health() HealthSnapshot {
	switch {
	case c.err != nil:
		return HealthSnapshot{State: HealthDegraded, Err: c.err}
	case c.life.Closing():
		return HealthSnapshot{State: HealthClosed}
	case c.health != nil:
		return c.health.Health()
	default:
		return HealthSnapshot{State: HealthReady}
	}
}

// Shutdown closes the client gracefully, bounded by ctx:
// publishers flush, consumers drain, publishers
// close, and then the backend closes. It is idempotent, and so is closing a
// child handle afterwards. A backend that implements ClosingBackend is told
// once the publishers have shut down, before the backend closes.
//
// It is bounded by ctx and by the client's drain timeout, measured from when
// it began: DefaultDrainTimeout, or the longest drain timeout of a run one of
// its consumers is serving when that is longer. The flushes and the drain
// all fit inside that one bound. Every consumer stops fetching as the drain
// begins, and they drain side by side, each also within its own run's drain
// timeout, as ConsumerHandle.Shutdown describes.
//
// A publisher whose own Close or Shutdown is already under way is left to
// it: Shutdown does not flush it again. It waits for that close when it is
// still under way as the publishers are drained, and reports its outcome
// then. A consumer or publisher whose shutdown another caller began, with a
// bound longer than this one's, is waited for only until this bound, and so
// is a publisher's send that the driver keeps past it, a batch parked in a
// connection the broker has stopped reading say. Shutdown returns at the
// bound and reports it, and the close goes on behind it. A ClosingBackend is
// told then, which ends a write or an acknowledgement parked on the
// connection, including one that a handle's own prompt Close is waiting for.
// Then the backend closes, once each handle's shutdown has either finished or
// outlived its own bound with an operation still parked, an acknowledgement
// or a write the broker is not answering, which only the backend's close
// ends, and the close then waits up to a second for such a handle to finish,
// as Close describes. A handle still draining under a longer bound another
// caller gave it is waited for, up to that bound, because its source and
// sinks still need the backend. A Close called meanwhile waits for the same.
// It does not wait for the broker, except behind a handle whose own Close
// was already under way: that close has no bound and never counts as stuck,
// so the client waits for it, and on a backend without BeginClose a write it
// is held by ends only when the broker reads again. The other exception is a
// drain another caller began, below.
//
// The publishers stay open until each consumer left draining that way has
// finished, is stuck past its own bound, or has had its drain cut short by a
// Close, because until then its handlers may still publish or dead-letter,
// and they shut down after it. When Shutdown waited out such a drain past its
// bound, the publishers it shuts down then do so side by side under one bound
// of their own, DefaultDrainTimeout, so that what those handlers published is
// still flushed. Shutdown has returned by then, but a Close joined behind it
// can take up to that much longer, however many publishers there are, and
// up to a second more for a publisher still stuck past that bound (Close).
// A drain this Shutdown used its own bound up on keeps that bound for the
// publishers, because its handlers were cut there.
//
// Behind such a drain, a ClosingBackend is told only once the drain has ended
// and the publishers have shut down: told first, as RabbitMQ is under a
// resource alarm, it would end the drain and the very flush that bound is
// for. So an operation the drain has parked on a connection the broker is not
// answering, an acknowledgement under the alarm say, holds the close, and a
// Close joined behind it, until the broker answers or that drain's own bound
// passes. On RabbitMQ under an alarm, every publication in the publishers'
// last flush then waits for a confirm the broker does not send until the
// alarm clears, so the joined Close takes the whole DefaultDrainTimeout
// whenever the draining handlers published during the alarm, and those
// publications end ambiguous unless it clears in time. A write parked in the
// full socket ends only with BeginClose, once the publisher reports itself
// stuck at that bound.
//
// Called from inside a PublishAsync callback it does not wait, for the reason
// Close gives, and the shutdown it begins is still bounded by ctx after it
// has returned. Called from inside a handler it does not wait either, for the
// reason ConsumerHandle.Shutdown gives: the drain waits for the calling
// handler instead, so a settlement it makes afterwards still reaches the
// broker. The shutdown begun there is bounded by the drain timeout and not by
// ctx, as a consumer's is.
func (c *Client) Shutdown(ctx context.Context) error {
	// A construction failure is reported, but the backend still closes: a
	// driver hands NewClient an open connection, and only Close releases it.
	switch {
	case insideHandler():
		return errors.Join(c.err, c.life.StartShutdown(context.WithoutCancel(ctx)))
	case insideDone():
		return errors.Join(c.err, c.life.StartShutdown(ctx))
	}
	return errors.Join(c.err, c.life.Shutdown(ctx))
}

// Close closes the client promptly: every publisher and consumer it
// opened is closed without flushing or draining, and then the backend. It is
// idempotent and safe for concurrent use. A backend that implements
// ClosingBackend is told before any of them closes, so that a write parked
// in its connection does not hold the close.
//
// Every call returns the outcome of the client's one close, and after a
// Shutdown that close is the Shutdown's, so every caller gets the same
// answer. The Shutdown's bound is part of that outcome when it cut a wait
// short. Given a context that has already ended, that can be one of the short
// waits every shutdown makes, such as a driver's flush, a batching
// publisher's last batch or a runner's stop, so the error alone does not mean
// anything was lost.
//
// A Close called while a Shutdown is under way joins it and waits for all of
// it. Behind a drain another caller began under a longer bound, that is the
// drain and then up to DefaultDrainTimeout for the publishers' flush, and up
// to a second more for a publisher stuck past that bound, as Shutdown
// describes.
//
// A handle whose own Shutdown is under way is closed as the handle's Close
// closes it, joining that shutdown, and waited for. The exception is a
// shutdown that has outlived its bound with an operation still parked, an
// acknowledgement or a write the broker is not answering, which only the
// backend's close ends: once that bound has passed, Close closes the backend
// under the handle, which ends the operation, and then waits up to a second
// for the handle's close to finish, so that the callbacks and settlements it
// still delivers have run when Close returns. The second is measured on the
// client's clock (WithClock), so under a test clock nobody moves, Close waits
// until the handle finishes. A handle the backend's close does not end in
// that time, one held by a write on a connection the caller lent the backend
// say, finishes behind Close. This holds after runner.Run's
// stop, or a deferred Shutdown of the handle, as it does after the client's
// own Shutdown.
//
// Called from inside a PublishAsync callback, of any of its publishers or of
// any other, it does not wait. Closing the client closes its publishers, and a
// publisher's close can wait for the goroutine that callback is running on,
// which is the wait PublisherHandle.Close does not make from there either. It
// begins the close and returns nil, or the close's error once one has
// finished; the close goes on once the callback returns, and a Close or
// Shutdown from anywhere else waits for all of it and reports the outcome.
//
// Called from inside a handler it does not wait either, and returns the same.
// A consumer's Shutdown already under way waits for that handler, and
// closing the client closes the consumer, which waits for the shutdown.
func (c *Client) Close() error {
	if insideDone() || insideHandler() {
		return errors.Join(c.err, c.life.StartClose())
	}
	return errors.Join(c.err, c.life.Close())
}

// ready reports the failure every operation answers with: a construction
// failure, or a closed client.
func (c *Client) ready() error {
	if c.err != nil {
		return c.err
	}
	if c.life.Closing() {
		return ErrClosed
	}
	return nil
}

// adopt registers a child handle so that closing the client closes it
// ("closing a client closes all child handles").
func (c *Client) adopt(h childHandle) error {
	c.mu.Lock()
	if c.life.Closing() {
		c.mu.Unlock()
		return ErrClosed
	}
	c.children = append(c.children, h)
	c.mu.Unlock()
	return nil
}

// forget drops a child that closed itself, so that a long-lived client does
// not accumulate handles nobody holds any more.
func (c *Client) forget(h childHandle) {
	c.mu.Lock()
	for i, child := range c.children {
		if child == h {
			c.children[i] = c.children[len(c.children)-1]
			c.children[len(c.children)-1] = nil
			c.children = c.children[:len(c.children)-1]
			break
		}
	}
	if p, ok := h.(*PublisherHandle); ok {
		old := *c.pubs.Load()
		if old[p.cfg.Topic] == p {
			next := make(map[string]*PublisherHandle, len(old))
			for k, v := range old {
				if v != p {
					next[k] = v
				}
			}
			c.pubs.Store(&next)
		}
	}
	c.mu.Unlock()
}

// forgetIfFinished is forget for a Shutdown that has returned: it drops h only
// once h's own close has run to its end. A Shutdown that returns at its own
// bound, with a shutdown another caller began still going, leaves h listed,
// so that Client.Close still closes it, and waits for it unless it is stuck
// (closeNative), before the backend closes; the close step forgets it at its
// end (ConsumerHandle.closeNative, PublisherHandle.closeStep). Forgotten as
// that Shutdown returned, it was a close nobody waited for, and Client.Close
// closed the backend under its open source or sink.
func (c *Client) forgetIfFinished(h childHandle) {
	select {
	case <-h.finished():
		c.forget(h)
	default:
	}
}

// takeChildren empties the child list and returns what it held. The list is
// emptied under the lock and the children are closed outside it, because a
// child's Close calls forget and would otherwise deadlock against the very
// lock that is closing it.
func (c *Client) takeChildren() []childHandle {
	c.mu.Lock()
	children := c.children
	c.children = nil
	empty := map[string]*PublisherHandle{}
	c.pubs.Store(&empty)
	c.mu.Unlock()
	return children
}

// flushChildren is the Flushing phase: every publisher completes what it has
// admitted.
//
// A publisher whose own Close or Shutdown has begun stays listed until that
// close has finished, and its Flush answers ErrClosed. That ErrClosed is not
// this shutdown's failure: the publisher's own close decides what is flushed,
// and the Draining phase joins that close and reports its outcome.
func (c *Client) flushChildren(ctx context.Context) error {
	c.mu.Lock()
	children := make([]childHandle, len(c.children))
	copy(children, c.children)
	c.mu.Unlock()
	var first error
	for _, h := range children {
		p, ok := h.(*PublisherHandle)
		if !ok {
			continue
		}
		err := p.Flush(ctx)
		if err == nil || closingOnItsOwn(p, err) {
			continue
		}
		if first == nil {
			first = err
		}
	}
	return first
}

// closingOnItsOwn reports whether err is the ErrClosed of a publisher whose
// own Close or Shutdown has begun.
func closingOnItsOwn(p *PublisherHandle, err error) bool {
	return errors.Is(err, ErrClosed) && p.closed()
}

// drainTimeout is the bound of a graceful Shutdown that begins now:
// DefaultDrainTimeout, or the longest drain timeout of a run one of the
// consumers is serving when that is longer.
//
// It is one bound for the whole shutdown, so the flushes before and after
// the drain cannot stretch it, and it is never shorter than a run allows its
// handlers: a consumer's drain ends short of its WithDrainTimeout by no more
// than the time the publishers took to flush before it. The client's own 30
// seconds used to end it there whatever the run said.
//
// A second Shutdown that joins the first once the drain has taken the
// consumers off the list is bounded by the longest answer given before
// (drainSeen), measured from its own arrival. It used to find no consumer and
// answer DefaultDrainTimeout, so it reported the drain deadline at 30 seconds
// while a consumer's run allowed its handlers longer.
func (c *Client) drainTimeout() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := max(DefaultDrainTimeout, c.drainSeen)
	for _, h := range c.children {
		if cons, ok := h.(*ConsumerHandle); ok {
			d = max(d, cons.drainTimeout())
		}
	}
	c.drainSeen = d
	return d
}

// drainChildren is the Draining phase of a graceful shutdown: consumers
// drain and close first, then publishers. A handler still draining may
// publish -- a runner's dead-letter publisher is one of this client's
// children, and so is any publisher a handler holds -- and a publisher
// closed under it turned every such publication into ErrClosed and its
// delivery into a redelivery. The publishers' own Shutdown then flushes
// whatever the draining handlers admitted.
//
// Every consumer's shutdown begins before any is waited for. Each one stops
// fetching first, so none fetches while another drains, and they drain side
// by side under ctx, each run's drain timeout measured from here. Shut down
// one at a time, a consumer kept fetching until its turn came and its drain
// timeout started only once the consumers before it had finished. Waiting
// for them in turn afterwards adds nothing to that, because they drain side
// by side: the waits end with the longest drain.
func (c *Client) drainChildren(ctx context.Context) error {
	children := c.takeChildren()
	for _, h := range children {
		if cons, ok := h.(*ConsumerHandle); ok {
			cons.startShutdown(ctx)
		}
	}
	var first error
	for _, h := range children {
		if _, ok := h.(*ConsumerHandle); !ok {
			continue
		}
		if err := h.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	// A consumer whose Shutdown above returned at this bound, while a
	// shutdown another caller began under a longer one goes on, still has
	// handlers draining, and they may publish. The publishers stay open for
	// them until that consumer has finished, or has reported itself stuck on
	// an operation only the backend's close ends, which an open publisher
	// does not help. Await reports this bound, so Shutdown still returns at
	// it. Shut down at once, the publishers turned the draining handlers'
	// publications and the runner's dead-letter publications into ErrClosed,
	// and their deliveries into redeliveries.
	//
	// A consumer this shutdown began is finished or stuck by the time its
	// Shutdown above returns, so only a drain another caller began is waited
	// for here.
	//
	// A consumer whose own close is prompt, a Close under way or one that
	// cut short the drain, is not waited for, and the wait for one ends when
	// such a Close arrives (interrupted). Its handlers have been told to stop,
	// so there is no drain left to keep the publishers open for, and what its
	// close still waits for may be an operation only BeginClose ends, an
	// acknowledgement parked on a connection under a RabbitMQ alarm say: a
	// prompt close never reports itself stuck. Waited for here, before
	// closeNative's BeginClose, it held the close, and a Close joined behind
	// it, until the broker answered. closeNative waits for it after
	// BeginClose (unfinished).
	waited := false
	for _, h := range children {
		cons, ok := h.(*ConsumerHandle)
		if !ok {
			continue
		}
		select {
		case <-h.finished():
			continue
		case <-h.stuck():
			continue
		case <-cons.interrupted():
			continue
		default:
		}
		// interrupted closes right after stuck does, so it ends this wait
		// for a stuck consumer too.
		c.life.Await(h.finished(), cons.interrupted())
		waited = true
	}
	// The publishers then flush what those handlers published, which they
	// owe, as every shutdown's flush does. When the wait outlived this bound,
	// their Shutdown under it was cut at once, and the handlers' last
	// asynchronous publications, a partial batch say, failed with ErrClosed
	// in their callbacks, after the handlers had seen them admitted. They get
	// one bound of their own then (shutdownPublishersAfresh).
	//
	// The wait outlived the bound when this run is stuck by then, as Await
	// makes it when it reports the bound. Shutdown's caller has been
	// released at the bound, so only the close behind it, and a Close that
	// joins it, wait the longer. Judged by ctx having ended instead, a drain
	// that finished as the bound passed, with nothing reported, held
	// Shutdown itself for the publishers' bound too. A drain this shutdown
	// used its own bound up on keeps that bound for the publishers: its
	// handlers were cut there, and nothing was waited for here.
	afresh := false
	if waited {
		select {
		case <-c.life.Stuck():
			afresh = true
		default:
		}
	}
	if afresh {
		if err := c.shutdownPublishersAfresh(ctx, children); err != nil && first == nil {
			first = err
		}
	} else {
		for _, h := range children {
			if _, ok := h.(*PublisherHandle); !ok {
				continue
			}
			if err := h.Shutdown(ctx); err != nil && first == nil {
				first = err
			}
		}
	}
	// A Shutdown above that returned at the bound may have left its handle
	// still closing: a publisher's shutdown another caller began under a
	// longer bound, or a handle stuck on a send or an acknowledgement parked
	// in the connection. closeNative waits for those.
	for _, h := range children {
		select {
		case <-h.finished():
		default:
			c.unfinished = append(c.unfinished, h)
		}
	}
	return first
}

// shutdownPublishersAfresh shuts the publishers down behind a drain the
// client's Shutdown waited out past its bound: under ctx's values without its
// end, so that each publisher's own drain timeout, DefaultDrainTimeout,
// bounds it, and with every publisher's shutdown begun before any is waited
// for, so that their bounds run side by side and a write parked in one flush
// holds the others' for no longer than its own.
//
// Each is waited for as the consumers are, until it finishes or reports
// itself stuck, and not through its Shutdown: a Shutdown that comes to a
// shutdown already stuck waits a bound of its own from then on. Shut down one
// after another, each publisher with a write parked held the close for that
// bound again, and a Close joined behind it waited 30 seconds for each.
//
// Only the publishers whose shutdown this begins are waited for here. One
// whose own Close or Shutdown was already under way is left to it, as
// without a fresh bound: it is joined under ctx, whose bound has passed, and
// closeNative waits for it after BeginClose. Waited for here, before
// BeginClose, a prompt Close of its own, which never reports itself stuck,
// held the close, and a Close joined behind it, for as long as the broker
// held a write only BeginClose ends.
func (c *Client) shutdownPublishersAfresh(ctx context.Context, children []childHandle) error {
	pctx := context.WithoutCancel(ctx)
	began := make([]bool, len(children))
	for i, h := range children {
		if p, ok := h.(*PublisherHandle); ok {
			began[i] = p.startShutdown(pctx)
		}
	}
	var first error
	for i, h := range children {
		if _, ok := h.(*PublisherHandle); !ok {
			continue
		}
		var err error
		switch {
		case !began[i]:
			err = h.Shutdown(ctx)
		default:
			c.life.Await(h.finished(), h.stuck())
			select {
			case <-h.finished():
				// Its Shutdown reports the outcome of the shutdown that has
				// finished, at once.
				err = h.Shutdown(pctx)
			default:
				// Stuck past its bound with a send parked: what a Shutdown
				// waiting for it answers there, pctx having no end of its
				// own.
				err = lifecycle.ErrDrainTimeout
			}
		}
		if err != nil && first == nil {
			first = err
		}
	}
	return first
}

// closeNative is the Closed phase: whatever is still open is closed
// promptly, and then the backend.
//
// A ClosingBackend hears first. A handle's close waits for its own operations,
// and one parked in a write only the connection's close ends held that close,
// and the backend's after it, for as long as the broker held the write.
//
// Under Shutdown, a handle the drain left still closing (unfinished) is
// waited for after BeginClose, which ends a write parked in the connection,
// and before the backend closes, which the handle's source or sink still
// needs: a requeue, a settlement or a source close after the backend's close
// fails, and the message is redelivered. The wait is the lifecycle's Await,
// so a Shutdown whose bound has passed still returns at it, and the backend
// closes behind it once the handle has finished. Without the wait, the
// backend closed at once under a consumer still draining under another
// caller's longer bound.
//
// The wait also ends when the handle reports itself stuck: its own shutdown
// has outlived its bound with an operation parked, an acknowledgement or a
// write the broker is not answering. Only the backend's close ends that on a
// backend without BeginClose, so waiting for the handle to finish first
// waited for the broker, and a Client.Close after the Shutdown with it. A
// handle whose own prompt Close was under way never reports itself stuck,
// because a prompt close has no bound, so it is still waited for to the end,
// however long the operation holding it takes.
//
// The handles still listed, all of them under Close, are waited for the same
// way: each one's close is begun, as its Close begins it, and waited for
// until it finishes or the handle reports itself stuck. A handle whose own
// Shutdown, from runner.Run's stop or a deferred call, returned at its bound
// with an operation parked stays listed, because its close is not done
// (forgetIfFinished). Its Close joined that shutdown and waited for the
// operation, which only the backend's close below ends, so Client.Close
// waited for the broker.
//
// Stuck says only that a wait outlived the handle's bound once; it stays
// closed after that. When the Interrupt step that the cut runs ends the
// operation after all, a settlement that honours its context say, the
// handle's close goes on to its commit and its source's or sink's close,
// and the backend may close under those here and under Shutdown alike. A
// requeue or a commit the closed backend refuses means a redelivery or a
// duplicate, never a loss. Client.Close behaved the same up to v1.1.1;
// telling a handle still parked from one whose cut ended its operation
// would need the lifecycle to report each wait afresh. What the client's
// close does instead is wait, once the backend has closed, for the handles
// it closed the backend under (awaitBehind).
func (c *Client) closeNative() error {
	if c.closing != nil {
		c.closing.BeginClose()
	}
	// behind is every handle the backend closes under, still closing.
	var behind []childHandle
	for _, h := range c.unfinished {
		c.life.Await(h.finished(), h.stuck())
		if !isFinished(h) {
			behind = append(behind, h)
		}
	}
	var first error
	for _, h := range c.takeChildren() {
		h.beginClose()
		c.life.Await(h.finished(), h.stuck())
		select {
		case <-h.finished():
			// Close reports the outcome of the close that has finished.
			if err := h.Close(); err != nil && first == nil {
				first = err
			}
		default:
			// Stuck: the backend closes under it, which ends the
			// operation, and its close is waited for after that.
			behind = append(behind, h)
		}
	}
	if c.backend != nil {
		if err := c.backend.Close(); err != nil && first == nil {
			first = err
		}
	}
	c.awaitBehind(behind)
	return first
}

// closeBehindWait bounds how long a client's close waits, once the backend
// has closed, for the handles it closed the backend under (awaitBehind).
const closeBehindWait = time.Second

// awaitBehind waits, for up to closeBehindWait after the backend's close, for
// the handles closeNative closed the backend under to finish their closes.
//
// Stuck says that a wait outlived the handle's bound, not that what it waited
// for is still parked. A batching publisher whose last flush a Shutdown's
// bound cut reports itself stuck from its close step, which has itself
// cancelled the send and closed the sink, as soon as its flusher is not back
// from the driver within a few yields: the driver may have answered and still
// be unwinding, or the flusher may not have been scheduled yet. The client's
// close then returned before that publisher's callbacks ran, while the
// close that delivers them was all but done. The backend's close ends
// whatever was still parked, so each such close finishes promptly once the
// backend has closed, and the client's close returns after it.
//
// The wait still has a bound, because an operation on a connection the
// backend does not own, one the caller lent it, outlives the backend's close,
// and a callback is the caller's code. A handle that takes longer finishes
// behind the client's close, as every stuck handle did before. The bound is
// on the client's clock, as every other bound of the client's is (WithClock),
// so a test clock nobody moves never ends it. The wait is the lifecycle's,
// so a Shutdown whose bound has passed still returns at it.
func (c *Client) awaitBehind(hs []childHandle) {
	if len(hs) == 0 {
		return
	}
	t := c.clock.NewTimer(closeBehindWait)
	defer t.Stop()
	for _, h := range hs {
		if !c.life.AwaitWithin(h.finished(), t.C()) {
			return
		}
	}
}

// isFinished reports whether h's close has run to its end.
func isFinished(h childHandle) bool {
	select {
	case <-h.finished():
		return true
	default:
		return false
	}
}

// HealthState is one of the five client states.
type HealthState uint8

// The health states.
const (
	// HealthConfigured means the client exists and has not connected.
	HealthConfigured HealthState = iota
	// HealthConnecting means a connection is being established.
	HealthConnecting
	// HealthReady means the client is usable.
	HealthReady
	// HealthDegraded means the client is usable but something is wrong, or
	// is not usable and may recover.
	HealthDegraded
	// HealthClosed means the client is closed. It is terminal.
	HealthClosed

	healthStateCount
)

var healthStateNames = [healthStateCount]string{"configured", "connecting", "ready", "degraded", "closed"}

// String returns the state's lower-case name.
func (h HealthState) String() string {
	if h < healthStateCount {
		return healthStateNames[h]
	}
	return "HealthState(?)"
}

// Valid reports whether h names a state.
func (h HealthState) Valid() bool { return h < healthStateCount }

// HealthSnapshot is what Client.Health answers. It is a snapshot and
// not a subscription: a driver's background failures also reach a caller
// through Hooks.OnReconnect.
type HealthSnapshot struct {
	// State is the client's state.
	State HealthState
	// Err is why the state is not Ready, and nil when it is. It never
	// carries a payload, a credential or an address.
	Err error
}

// Admin is the topology surface.
//
// Ensure operations are idempotent; the destructive ones are explicit and
// never implied. A driver's own typed administration remains the advanced
// API: this is the portable intersection, which is why its specs carry only
// what every broker can be told.
type Admin interface {
	// EnsureTopic creates the topic if it does not exist.
	EnsureTopic(ctx context.Context, spec TopicSpec) error
	// DeleteTopic removes a topic and everything in it.
	DeleteTopic(ctx context.Context, topic string) error
	// ListTopics iterates the topics the broker reports. The iteration
	// stops at the first error it yields.
	ListTopics(ctx context.Context) iter.Seq2[string, error]
	// EnsureSubscription creates the subscription if it does not exist.
	EnsureSubscription(ctx context.Context, spec SubscriptionSpec) error
	// DeleteSubscription removes a subscription.
	DeleteSubscription(ctx context.Context, topic, sub string) error
	// Purge discards everything a subscription has not consumed.
	Purge(ctx context.Context, topic, sub string) error
	// Stats reports a subscription's depth, in-flight count and lag.
	Stats(ctx context.Context, topic, sub string) (QueueStats, error)
}

// TopicSpec names a topic to create. It carries the portable
// intersection only — the name — because everything else a broker wants
// (partitions, replication, retention) is named by the driver's own typed
// administration, which the specification keeps as the advanced API.
type TopicSpec struct {
	// Name is the topic's name.
	Name string
}

// SubscriptionSpec names a subscription to create. A subscription is
// identified by its topic and its name, which is what DeleteSubscription
// takes.
type SubscriptionSpec struct {
	// Topic is the topic the subscription reads.
	Topic string
	// Name is the subscription's name.
	Name string
}

// QueueStats is what a broker reports about one subscription:
// "depth, in-flight and lag".
//
// Each is a Limit, so that a broker which cannot report one says so instead
// of reporting zero — the rule that an unknown is never an overloaded
// zero, which is the difference between an empty queue and a queue nobody
// can measure.
type QueueStats struct {
	// Depth is how many messages are waiting.
	Depth Limit[int64]
	// InFlight is how many have been delivered and not settled.
	InFlight Limit[int64]
	// Lag is how far behind the subscription is.
	Lag Limit[int64]
}

// The client's construction failures. The specification names no sentinel for them and
// errors.Is matches by code, so a caller compares CodeOf(err) against
// CodeInvalidConfig or CodeInvalidArgument.
var (
	errNilBackend     = errcode.New(errcode.InvalidConfig, "iqmq: NewClient was given a nil Backend")
	errNilClock       = errcode.New(errcode.InvalidArgument, "iqmq: WithClock was given a nil Clock")
	errNoClientOption = errcode.New(errcode.InvalidArgument, "iqmq: client option names no setting")
)
