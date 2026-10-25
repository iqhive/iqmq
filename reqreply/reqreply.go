// Package reqreply is the API's request/reply over the iqmq core:
// a correlation id, a reply topic, and ONE reply consumer per client that
// serves every outstanding request.
//
// # One consumer, never one per request
//
// The specification is explicit about it, and it is the whole design. A reply consumer
// per request means a subscription per request: topology created and
// destroyed on the message path, a broker round trip before the request is
// even sent, and — on brokers that do not garbage-collect subscriptions — a
// slow leak of server-side state that outlives the process. This client opens
// one reply consumer when it is created, keeps it for its lifetime, and
// matches replies to waiters through a correlation table
// (internal/corr, the store the specification measured).
//
// # Native first, emulation only when asked
//
// A driver whose broker has request/reply natively — NATS's inbox, RabbitMQ's
// direct reply-to — exposes it through NativeRequester on the handle its
// Native() returns, and New uses it: no reply topic is created and no reply
// consumer is opened.
//
// Everywhere else request/reply has to be emulated, and emulation CREATES
// TOPOLOGY: a reply topic and a subscription on it, which on a managed broker
// is a resource with a cost and a lifetime nobody asked for. The specification therefore
// makes it opt-in, and New refuses rather than quietly provisioning:
//
//	rr, err := reqreply.New(ctx, client, reqreply.Config{
//		ReplyTopic:   "svc.replies",
//		Subscription: "svc-instance-7",
//	}, reqreply.WithEmulatedRequestReply())
//
// # The slot is always released
//
// A request whose reply never arrives releases its correlation slot when its
// deadline passes, on the requesting goroutine itself, and Close drains the
// whole table. A client that leaked a slot per timed-out request would grow
// without bound on exactly the workload — an unreliable responder — that
// makes timeouts common; Pending exists so a caller, and a test, can see that
// it does not.
//
// # Concurrency
//
// A Client is safe for concurrent use: Request may be called from any number
// of goroutines. Close is idempotent.
package reqreply

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/corr"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Errors this package returns. Each carries a portable code.
var (
	// ErrNotEnabled (CodeUnsupported) reports a New whose backend has no
	// native request/reply and whose caller did not ask for emulation.
	// Emulation creates topology, so the specification makes it explicit.
	ErrNotEnabled error = errcode.New(errcode.Unsupported,
		"iqmq/reqreply: this backend has no native request/reply; pass WithEmulatedRequestReply to create the reply topology")
	// ErrInvalidConfig (CodeInvalidConfig) reports a Config that Validate
	// rejects, or a nil argument to Serve.
	ErrInvalidConfig error = errcode.New(errcode.InvalidConfig, "iqmq/reqreply: invalid configuration")
	// ErrTooManyPending (CodeResourceExhausted) reports a request refused
	// because Capacity outstanding requests are already waiting. The specification bounds
	// the table: a responder that never replies must not be able to grow a
	// client's memory without limit.
	ErrTooManyPending error = errcode.New(errcode.ResourceExhausted, "iqmq/reqreply: too many outstanding requests")
	// ErrClosed (CodeClosed) reports a request on a closed client.
	ErrClosed error = errcode.New(errcode.Closed, "iqmq/reqreply: client is closed")
	// ErrNoReplyTo (CodeInvalidArgument) reports a delivery Respond was given
	// that names no reply topic, so there is nowhere to send the reply.
	ErrNoReplyTo error = errcode.New(errcode.InvalidArgument, "iqmq/reqreply: the request names no reply topic")
)

// Defaults for the zero Config.
const (
	// DefaultTimeout is how long a request with no deadline of its own
	// waits. A request that waits for ever is a goroutine that never
	// returns, so there is no "no timeout" setting.
	DefaultTimeout = 30 * time.Second
	// DefaultCapacity is how many requests may be outstanding at once.
	DefaultCapacity = corr.DefaultCapacity
	// DefaultPrefetch is the reply consumer's prefetch.
	DefaultPrefetch = 64
)

// Config configures a Client.
type Config struct {
	// ReplyTopic is the topic replies are published to and the reply
	// consumer reads from. It is required for the emulated path and ignored
	// on the native one.
	ReplyTopic string
	// Subscription is the reply consumer's subscription.
	//
	// It must be unique to this client. Two clients sharing one subscription
	// on a queue-shaped broker take each other's replies, and a reply
	// delivered to the wrong client is a request that times out while its
	// answer is acked somewhere else. The safe spelling is a per-instance
	// name. Empty selects ReplyTopic + "-rr".
	Subscription string
	// Timeout is how long a request waits when its context has no deadline.
	// Zero selects DefaultTimeout; a negative value is refused.
	Timeout time.Duration
	// Capacity bounds the outstanding requests. Zero selects
	// DefaultCapacity.
	Capacity int
	// Prefetch is the reply consumer's prefetch. Zero selects
	// DefaultPrefetch.
	Prefetch int
	// Clock is the time source. Nil selects the client's own wall clock.
	Clock mq.Clock
	// OnUnmatched, when non-nil, is called for a reply that matched no
	// outstanding request: a reply to a request that has already timed out,
	// a duplicate redelivery, or a message from another client sharing the
	// subscription. It is settled either way.
	OnUnmatched func(ctx context.Context, d *mq.Delivery)
}

// Validate reports whether the configuration is usable.
func (cfg Config) Validate() error {
	if cfg.Timeout < 0 || cfg.Capacity < 0 || cfg.Prefetch < 0 {
		return ErrInvalidConfig
	}
	return nil
}

// resolve fills in the defaults.
func (cfg Config) resolve() Config {
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.Capacity == 0 {
		cfg.Capacity = DefaultCapacity
	}
	if cfg.Prefetch == 0 {
		cfg.Prefetch = DefaultPrefetch
	}
	if cfg.Subscription == "" && cfg.ReplyTopic != "" {
		cfg.Subscription = cfg.ReplyTopic + "-rr"
	}
	return cfg
}

// NativeRequester is the optional interface a driver's native handle
// implements when its broker has request/reply of its own — NATS's inbox,
// RabbitMQ's direct reply-to.
//
// New finds it by type-asserting what mq.Client.Native returns, which is the
// escape hatch ADR-003 already provides: the root package does not grow an
// SPI method for a feature four drivers have and eight do not.
//
// A native implementation owns the whole exchange, including the reply
// topology, the correlation and the timeout; nothing in this package's
// correlation table is used on that path.
type NativeRequester interface {
	// RequestNative sends m to topic and returns the reply. The deadline is
	// ctx's.
	RequestNative(ctx context.Context, topic string, m *mq.Message) (*mq.Message, error)
}

// Requester is what a caller depends on: one call that sends a request and
// waits for its reply. *Client implements it, and so does a driver's native
// path.
type Requester interface {
	// Request sends m to topic and waits for the reply.
	Request(ctx context.Context, topic string, m *mq.Message, opts ...mq.PublishOption) (*mq.Message, error)
}

// Option is a construction option for New.
type Option struct {
	emulate  bool
	native   NativeRequester
	noNative bool
}

// WithEmulatedRequestReply opts in to the emulated path on a backend that has
// no native request/reply.
//
// It is opt-in because emulation creates topology: a reply topic and a
// subscription on it. On a managed broker that is a resource with a cost, a
// quota and a lifetime, and creating one because a library needed it is
// exactly what the specification's "Open never creates production topology" refuses to do
// silently.
func WithEmulatedRequestReply() Option { return Option{emulate: true} }

// WithNativeRequester supplies the native implementation explicitly, for a
// driver that exposes it somewhere other than Client.Native, and for tests.
func WithNativeRequester(n NativeRequester) Option { return Option{native: n} }

// WithoutNativeRequestReply refuses the native path even when the backend has
// one, so that a caller can exercise the emulated path against a broker that
// would otherwise take the native one.
func WithoutNativeRequestReply() Option { return Option{noNative: true} }

// Client is one process's request/reply endpoint: one reply consumer, one
// correlation table, any number of concurrent requests.
//
// The zero value is not usable; New returns a ready one.
type Client struct {
	cl     *mq.Client
	cfg    Config
	native NativeRequester

	// store maps a correlation id to the waiter that must be woken. It is the winner (ADR-020) through corr.Auto.
	store corr.Store[chan *mq.Message]
	seq   atomic.Uint64
	// instance distinguishes this client's correlation ids from another
	// client's on a shared reply topic, so a stray reply is recognised as
	// not ours rather than matched against our own counter.
	instance string

	rc     *mq.ConsumerHandle
	cancel context.CancelFunc
	done   chan struct{}
	clk    mq.Clock

	closeOnce sync.Once
	closeErr  error
	closed    atomic.Bool
	// drained is Close's scratch buffer for the correlation sweep.
	drained []corr.Pending[chan *mq.Message]
	// tookReply, when non-nil, runs in dispatch between taking a waiter's
	// entry and handing it the reply. It is nil outside this package's
	// tests, which use it to hold the reply loop inside that window.
	tookReply func()
}

var _ Requester = (*Client)(nil)

// New opens a request/reply client over cl.
//
// It takes the native path when the backend has one and the caller did not
// refuse it, and the emulated path when WithEmulatedRequestReply was given.
// With neither it returns ErrNotEnabled rather than creating topology nobody
// asked for.
//
// The returned Client owns the reply consumer it opened and closes it in
// Close. It does not own cl.
func New(ctx context.Context, cl *mq.Client, cfg Config, opts ...Option) (*Client, error) {
	if cl == nil {
		return nil, ErrInvalidConfig
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var o Option
	for _, x := range opts {
		if x.emulate {
			o.emulate = true
		}
		if x.native != nil {
			o.native = x.native
		}
		if x.noNative {
			o.noNative = true
		}
	}
	cfg = cfg.resolve()
	c := &Client{cl: cl, cfg: cfg, clk: clockOr(cfg.Clock)}

	if !o.noNative {
		c.native = o.native
		if c.native == nil {
			c.native, _ = cl.Native().(NativeRequester)
		}
	}
	if c.native != nil {
		return c, nil
	}
	if !o.emulate {
		return nil, ErrNotEnabled
	}
	if cfg.ReplyTopic == "" {
		return nil, ErrInvalidConfig
	}

	store, err := corr.New[chan *mq.Message](corr.Auto, corr.Config{Capacity: cfg.Capacity})
	if err != nil {
		return nil, fmt.Errorf("iqmq/reqreply: %w", err)
	}
	c.store = store
	c.instance = newInstanceID(c.clk)
	rc, err := cl.Consumer(ctx, mq.ConsumerConfig{
		Topic:        cfg.ReplyTopic,
		Subscription: cfg.Subscription,
		Prefetch:     cfg.Prefetch,
	})
	if err != nil {
		return nil, err
	}
	c.rc = rc

	// The reply loop's context is detached from the caller's: the consumer
	// lives as long as the client, and a client built inside a request
	// handler would otherwise stop receiving replies the moment that
	// handler returned.
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c.cancel = cancel
	c.done = make(chan struct{})
	go c.replyLoop(loopCtx)
	return c, nil
}

// Native reports whether this client uses the driver's own request/reply.
func (c *Client) Native() bool { return c.native != nil }

// Pending returns how many requests are outstanding.
//
// It exists so that the one failure this design can have is observable: a
// request whose reply never arrives must release its correlation slot, and a
// client that leaked one per timeout would grow without bound. On the native
// path it is always zero — the driver owns the correlation.
func (c *Client) Pending() int {
	if c.store == nil {
		return 0
	}
	return c.store.Len()
}

// Request sends m to topic and waits for the reply.
//
// The deadline is ctx's when it has one and Config.Timeout otherwise; a
// request that outlives it returns the context's error and releases its slot
// before it does. m is stamped with the correlation id and the reply topic
// and is published through cl; opts are the publication's own options.
//
// The returned message is the reply's, copied out of the delivery before it
// is settled, so it stays valid after Request returns.
func (c *Client) Request(ctx context.Context, topic string, m *mq.Message, opts ...mq.PublishOption) (*mq.Message, error) {
	if m == nil {
		return nil, ErrInvalidConfig
	}
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if c.native != nil {
		ctx, cancel := c.withTimeout(ctx)
		defer cancel()
		return c.native.RequestNative(ctx, topic, m)
	}

	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	id := corr.ID(c.seq.Add(1))
	// The channel is buffered so that a reply arriving at the instant its
	// waiter gives up neither blocks the reply loop nor is lost: the loop
	// hands it over and moves on, and the waiter that took the entry out of
	// the store is the only one that can receive it.
	ch := make(chan *mq.Message, 1)
	deadline, _ := ctx.Deadline()
	if !c.store.Put(id, deadline.UnixNano(), ch) {
		return nil, ErrTooManyPending
	}

	m.Properties.CorrelationID = c.encode(id)
	m.Properties.ReplyTo = c.cfg.ReplyTopic
	if err := c.cl.Publish(ctx, topic, m, opts...); err != nil {
		// The request never left, so the slot goes straight back. Taking it
		// rather than assuming it is still there is deliberate: a reply
		// cannot have arrived, but a Close racing this one can have drained
		// the table, and Take is how the two agree on who owns the entry.
		c.store.Take(id)
		return nil, err
	}

	select {
	case reply := <-ch:
		return reply, nil
	case <-ctx.Done():
		// The slot is released HERE, on the goroutine that owns it, before
		// the error is returned. A client that left it for a sweeper would
		// hold one entry per timed-out request for as long as the sweep
		// interval, and one that left it for ever would grow without bound
		// on exactly the workload -- an unreliable responder -- that makes
		// timeouts common.
		if _, taken := c.store.Take(id); !taken {
			// The reply loop took the entry first and is about to hand the
			// reply over. Prefer the answer to the timeout: the reply
			// exists, and reporting a deadline for a request that was
			// answered would make the caller retry a completed operation.
			if reply, ok := c.takenReply(ch); ok {
				return reply, nil
			}
		}
		return nil, ctx.Err()
	case <-c.done:
		if _, taken := c.store.Take(id); !taken {
			// The reply loop may have answered this request before it
			// ended; the answer still beats ErrClosed, for the same reason
			// as above.
			if reply, ok := c.takenReply(ch); ok {
				return reply, nil
			}
		}
		return nil, ErrClosed
	}
}

// takenReply waits for the reply to a request whose correlation entry someone
// else took out of the store.
//
// The taker is the reply loop or Close's drain. The reply loop sends on ch as
// soon as it has copied the message, so waiting for it is waiting on a send
// already committed to: a non-blocking look at ch here used to find it empty
// while the loop was still copying, and the caller was told its answered
// request had timed out. Close's drain sends nothing, but it runs only after
// c.done is closed, and c.done closes only once the reply loop has returned,
// so by then any send the loop made is already in ch.
func (c *Client) takenReply(ch chan *mq.Message) (*mq.Message, bool) {
	select {
	case reply := <-ch:
		return reply, true
	case <-c.done:
	}
	select {
	case reply := <-ch:
		return reply, true
	default:
		return nil, false
	}
}

// withTimeout narrows ctx to Config.Timeout when it carries no deadline of
// its own.
func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.cfg.Timeout)
}

// The bounds of the reply loop's backoff after a failed receive.
const (
	receiveBackoffMin = 10 * time.Millisecond
	receiveBackoffMax = time.Second
)

// replyLoop is the single reply consumer the specification requires.
//
// A failed receive does not end it unless the failure is terminal. It used to
// end on any error, and one broker hiccup then left a client that still
// looked open: every later Request published its request -- the responder did
// the work -- and returned ErrClosed without waiting for the reply.
func (c *Client) replyLoop(ctx context.Context) {
	defer close(c.done)
	buf := make([]mq.Delivery, c.cfg.Prefetch)
	backoff := time.Duration(0)
	for {
		n, err := c.rc.Receive(ctx, buf)
		for i := range n {
			c.dispatch(ctx, &buf[i])
		}
		switch {
		case err == nil:
			backoff = 0
			continue
		case ctx.Err() != nil:
			return
		case receiveTerminal(err):
			// Marked before c.done closes, so that a Request arriving now
			// is refused before it publishes rather than after.
			c.closed.Store(true)
			return
		}
		backoff = min(max(2*backoff, receiveBackoffMin), receiveBackoffMax)
		if !c.pause(ctx, backoff) {
			return
		}
	}
}

// receiveTerminal reports whether a receive failure will not clear by itself:
// the consumer is closed, or the subscription is gone or not ours to read.
func receiveTerminal(err error) bool {
	switch mq.CodeOf(err) {
	case mq.CodeClosed, mq.CodeNotFound, mq.CodeUnauthorized, mq.CodeForbidden,
		mq.CodeInvalidConfig, mq.CodeInvalidArgument, mq.CodeUnsupported:
		return true
	default:
		return false
	}
}

// pause waits d on the client's clock, and reports false when ctx ended first.
func (c *Client) pause(ctx context.Context, d time.Duration) bool {
	t := c.clk.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C():
		return true
	}
}

// dispatch hands one reply to its waiter and settles the delivery.
//
// Every reply is settled, matched or not: a reply to a request that has
// already timed out is still a message this subscription took, and leaving it
// unsettled would hold a lease and have it redelivered for ever.
func (c *Client) dispatch(ctx context.Context, d *mq.Delivery) {
	id, ok := c.decode(correlationOf(d))
	if ok {
		if ch, taken := c.store.Take(id); taken {
			if c.tookReply != nil {
				c.tookReply()
			}
			// The copy is made before the settlement: under
			// ConsumerConfig.BufferReuse the delivery's storage returns to
			// the consumer as soon as it is settled, and the waiter
			// reads the message long afterwards.
			ch <- d.CloneMessage()
			_ = d.Ack()
			return
		}
	}
	if c.cfg.OnUnmatched != nil {
		c.cfg.OnUnmatched(ctx, d)
	}
	_ = d.Ack()
}

// encode renders a correlation id as this client's own.
func (c *Client) encode(id corr.ID) string {
	return c.instance + "." + strconv.FormatUint(uint64(id), 36)
}

// decode reads back an id encode produced. A value that is not this client's
// — another client's on a shared reply topic, or anything a broker or a
// responder invented — is refused rather than mapped onto some id of ours,
// which is what would make one request receive another's reply.
func (c *Client) decode(s string) (corr.ID, bool) {
	rest, ok := strings.CutPrefix(s, c.instance+".")
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(rest, 36, 64)
	if err != nil || n == 0 {
		return 0, false
	}
	return corr.ID(n), true
}

// correlationOf reads a delivery's correlation id: the typed property when
// the driver filled it, and the correlation-id header otherwise.
func correlationOf(d *mq.Delivery) string {
	if id := d.Properties.CorrelationID; id != "" {
		return id
	}
	if v, ok := d.Headers.Get(HeaderCorrelationID); ok {
		return string(v)
	}
	return ""
}

// HeaderCorrelationID is the portable header a correlation id travels in when
// the backend has no native property for it.
const HeaderCorrelationID = "correlation-id"

// Close stops the reply consumer and releases every outstanding request.
//
// Every waiter is woken with ErrClosed rather than left to its own deadline:
// a closed client will never deliver their replies, and making them wait out
// a thirty-second timeout for an answer that cannot come is the one thing a
// shutdown must not do. Close is idempotent.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if c.cancel != nil {
			c.cancel()
		}
		if c.rc != nil {
			c.closeErr = c.rc.Close()
		}
		if c.done != nil {
			<-c.done
		}
		if c.store != nil {
			// Drain: pass the largest timestamp so that every entry expires,
			// which is what corr's Expire contract calls a close. The
			// waiters have already been woken -- c.done is closed above and
			// every Request selects on it -- so this releases the slots of
			// any request that had registered but not yet reached its
			// select.
			//
			// The channels are deliberately NOT closed. A waiter selecting
			// on a closed reply channel and a closed c.done picks between
			// them at random, and the reply branch would then receive a nil
			// message and report it as a successful reply with no error.
			// Releasing the entry is the whole job; waking is c.done's.
			c.drained = c.store.Expire(int64(^uint64(0)>>1), c.drained[:0])
		}
	})
	return c.closeErr
}

// clockOr returns c, or the wall clock when c is nil.
func clockOr(c mq.Clock) mq.Clock {
	if c == nil {
		return wallClock{}
	}
	return c
}

// wallClock is the default mq.Clock.
type wallClock struct{}

// Now returns the current time.
func (wallClock) Now() time.Time { return time.Now() }

// NewTimer returns a single-shot timer.
func (wallClock) NewTimer(d time.Duration) mq.Timer { return wallTimer{time.NewTimer(d)} }

// wallTimer adapts *time.Timer to mq.Timer.
type wallTimer struct{ t *time.Timer }

// C returns the expiry channel.
func (w wallTimer) C() <-chan time.Time { return w.t.C }

// Stop prevents the timer from firing.
func (w wallTimer) Stop() bool { return w.t.Stop() }

// Reset re-arms the timer.
func (w wallTimer) Reset(d time.Duration) bool { return w.t.Reset(d) }

// newInstanceID returns a short identifier unique to this client.
//
// It is the clock's nanoseconds mixed with 64 random bits: two clients
// created in the same nanosecond, or under one fake clock, still differ
// without a counter shared by every client in the process, and a client in
// another process differs by the clock as well. It names nothing about the
// host, so it leaks nothing into a header.
func newInstanceID(clk mq.Clock) string {
	n := uint64(clk.Now().UnixNano()) ^ rand.Uint64() //nolint:gosec // uniqueness, not secrecy
	return strconv.FormatUint(n, 36)
}
