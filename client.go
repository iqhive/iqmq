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
// It is a concrete struct and not an interface, which is the winner
// (ADR-003, Concrete): a caller holds a *Client, and the narrow interfaces —
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

	drainTimeout time.Duration

	// admin and health are the optional backend interfaces, asserted once
	// here so that no call site repeats the assertion.
	admin Admin
	// adminErr is what AdminBackend.Admin answered, when it failed.
	adminErr error
	health   HealthReporter

	// err is a construction failure. NewClient returns a *Client and not an
	// error — the signature docs/api-freeze.md freezes — so a client built
	// over a nil backend or a malformed option reports the failure from
	// every operation instead of panicking.
	err error

	life *lifecycle.Lifecycle

	// mu guards the child list and the publisher map's writers.
	mu       sync.Mutex
	children []childHandle
	// pubs is Client.Publish's bound publishers, one per topic, published
	// as an immutable map behind an atomic pointer: a publication reads it
	// without a lock (rule 5) and only a first publication to a new
	// topic writes.
	pubs atomic.Pointer[map[string]*PublisherHandle]
}

// childHandle is a publisher or a consumer, from the client's point of view:
// something to flush, drain and close in the order.
type childHandle interface {
	Shutdown(ctx context.Context) error
	Close() error
	closed() bool
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
// It is a value type and not a closure, which is the winner (ADR-006,
// Value), and its fields are unexported, so an option can only come from a
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
	}
	empty := map[string]*PublisherHandle{}
	c.pubs.Store(&empty)
	c.life = lifecycle.New(lifecycle.Config{
		Clock:        c.clock,
		DrainTimeout: c.drainTimeout,
		Steps: lifecycle.Steps{
			// the order for a client: publishers flush, then consumers
			// drain, then the backend closes. The Flush and Drain steps run
			// only for a graceful Shutdown; a prompt Close goes straight to
			// closing the children and the backend.
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
// (ADR-021, Bound) for a stable destination set and unbounded for a
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

// Shutdown closes the client gracefully, bounded by ctx (the specification's
// winner ADR-006, Shutdown): publishers flush, consumers drain, and then the
// backend closes. It is idempotent, and so is closing a child handle
// afterwards.
func (c *Client) Shutdown(ctx context.Context) error {
	// A construction failure is reported, but the backend still closes: a
	// driver hands NewClient an open connection, and only Close releases it.
	return errors.Join(c.err, c.life.Shutdown(ctx))
}

// Close closes the client promptly: every publisher and consumer it
// opened is closed without flushing or draining, and then the backend. It is
// idempotent and safe for concurrent use.
func (c *Client) Close() error {
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
		if err := p.Flush(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// drainChildren is the Draining phase of a graceful shutdown: publishers
// shut down first so that nothing new is admitted, then consumers, which is the order.
func (c *Client) drainChildren(ctx context.Context) error {
	children := c.takeChildren()
	var first error
	for _, h := range children {
		if _, ok := h.(*PublisherHandle); !ok {
			continue
		}
		if err := h.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	for _, h := range children {
		if _, ok := h.(*ConsumerHandle); !ok {
			continue
		}
		if err := h.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// closeNative is the Closed phase: whatever is still open is closed
// promptly, and then the backend.
func (c *Client) closeNative() error {
	var first error
	for _, h := range c.takeChildren() {
		if err := h.Close(); err != nil && first == nil {
			first = err
		}
	}
	if c.backend != nil {
		if err := c.backend.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
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
