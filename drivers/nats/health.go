package nats

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
)

// The asynchronous handlers are where the specification lives for this driver.
//
// A NATS client reports everything that goes wrong in the background through
// callbacks: the link dropped, the link came back, a subscription fell behind
// and the server started dropping its messages. None of it surfaces on a
// publish or a fetch — a slow consumer in particular is invisible to the
// consumer, which simply stops seeing messages — so a driver that did not wire
// these would leave a caller polling a Health that never changed while its
// messages went missing.

// onHealth runs after every health transition. It exists so that a transition
// has one place to fan out from, and it is where the Degraded reason is
// counted.
func (b *Backend) onHealth(snap mq.HealthSnapshot) {
	if snap.State == mq.HealthDegraded {
		b.degraded.Add(1)
	}
}

// onAsyncError is nats.go's catch-all for a failure that belongs to no call.
//
// A slow consumer is the one this driver cares most about: the server has
// started dropping messages for a subscription whose pending limits are
// exceeded, which on an at-most-once broker is loss. The specification requires it to reach
// health rather than only a log, so it does, with the subscription's subject in
// the error the snapshot carries.
func (b *Backend) onAsyncError(_ *natsgo.Conn, sub *natsgo.Subscription, err error) {
	if err == nil {
		return
	}
	if errors.Is(err, natsgo.ErrPermissionViolation) {
		b.refuseWaits(err)
	}
	if isSlowConsumer(err) {
		b.slow.Add(1)
		topic, subscription := "", ""
		if sub != nil {
			topic, subscription = TopicOf(b.cfg.SubjectPrefix, sub.Subject), sub.Queue
		}
		b.health.Degraded(b.errs.Map(mq.OpFetch, topic, subscription, err))
		return
	}
	b.health.Degraded(b.errs.Map(mq.OpFetch, "", "", err))
}

// onDisconnect records that the link went away. The client is retrying, so the
// state is Degraded and not Closed: Degraded is "usable but something
// is wrong, or not usable and may recover", which is exactly a NATS client
// between two connections.
func (b *Backend) onDisconnect(_ *natsgo.Conn, err error) {
	if err == nil {
		err = natsgo.ErrDisconnected
	}
	b.health.Degraded(b.errs.Map(mq.OpOpen, "", "", err))
}

// onReconnect records that the link came back, clears the error and fires the
// caller's hook.
func (b *Backend) onReconnect(nc *natsgo.Conn) {
	n := int(b.reconnects.Add(1))
	b.health.Ready()
	if h := b.cfg.Hooks.OnReconnect; h != nil {
		// The nats client gives its callback no context, so the hook gets
		// Background: there is no operation here to be part of.
		h(context.Background(), mq.ReconnectEvent{Driver: DriverName, Attempt: n})
	}
	_ = nc
}

// onClosed records that the connection will not come back. Closed is terminal
// in driverkit.Health, so a connection the server closed for good cannot be
// reported Ready again by a late callback.
func (b *Backend) onClosed(*natsgo.Conn) { b.health.Close() }

// chainHandlers puts this backend behind the driver's connection handlers on
// a connection handed to [BackendWithClient]: in the [chain] already in front
// of the connection's own handlers, or in a new one put in front of them.
// The connection's own handlers still run after the driver's.
//
// nats.go keeps one handler of each kind. Setting the driver's own would
// silence the caller's, and setting none, which is what a handed-over
// connection used to get, left Health blind to a lost link, a reconnection, a
// closed connection and a slow consumer, and Hooks.OnReconnect never fired.
// A handler the caller sets later replaces the chain's, and with it the
// driver's half.
func (b *Backend) chainHandlers(nc *natsgo.Conn) {
	if ch := chainOf(nc); ch != nil && ch.join(b) {
		b.chain = ch
		return
	}
	ch := &chain{
		nc:         nc,
		backends:   []*Backend{b},
		asyncErr:   nc.ErrorHandler(),
		disconnect: nc.DisconnectErrHandler(),
		reconnect:  nc.ReconnectHandler(),
		closed:     nc.ClosedHandler(),
		// The deprecated disconnect handler has no getter, and nats.go
		// calls it only when no DisconnectErrHandler is set, which
		// installing one here would otherwise stop.
		legacyDisconnect: nc.Opts.DisconnectedCB,
	}
	nc.SetErrorHandler(ch.onAsyncError)
	nc.SetDisconnectErrHandler(ch.onDisconnect)
	nc.SetReconnectHandler(ch.onReconnect)
	nc.SetClosedHandler(ch.onClosed)
	b.chain = ch
}

// chain is the driver's handlers on one handed-over connection, one of each
// kind, in front of the handlers the connection had, serving every backend of
// this driver open behind it.
//
// A func cannot be compared, so a handler cannot safely be taken off a
// connection once anything may have been set after it. Each backend used to
// put four forwarders of its own in front, each calling the one it replaced,
// and they stayed after it closed: every event made one nested call per
// backend ever opened on the connection, and some millions of open and close
// cycles on a long-lived connection overflowed the stack of nats.go's callback
// goroutine, which ends the process. Now a backend joins the chain in front
// and leaves it when it closes, and the last to leave puts the connection's
// own handlers back while the chain's are still the ones set. A chain that
// something was put in front of stays, serving nobody, until it is in front
// again and another backend joins it.
type chain struct {
	nc *natsgo.Conn
	// The handlers the connection had when the chain was put in front of
	// them, never written after.
	asyncErr         natsgo.ErrHandler
	disconnect       natsgo.ConnErrHandler
	legacyDisconnect natsgo.ConnHandler
	reconnect        natsgo.ConnHandler
	closed           natsgo.ConnHandler

	mu sync.Mutex
	// backends are the backends the chain serves. The slice is replaced and
	// never written in place, so a handler ranges over the one it read.
	backends []*Backend
	// retired says the connection's own handlers are back and the chain
	// serves nobody.
	retired bool
}

// probe asks a chain's error handler which chain it is. It is an error only
// to fit the handler's signature, and [chainOf] gives one only to a handler
// it knows is a chain's, which answers it and passes nothing on.
type probe struct{ chain *chain }

func (*probe) Error() string { return "iqmq/nats: a probe for the driver's own handlers" }

// chainOf is the chain whose handlers are nc's, or nil when they are anything
// else.
//
// A func cannot be compared, but reflect reports a method value's code
// pointer, which is the same for every method value of one method and is no
// other function's. That tells a chain's handlers from any other, and the
// error handler, once it is known to be a chain's, is asked which chain with
// a [probe]. A chain made for another connection, whose handlers the caller
// copied onto this one, is not this connection's.
func chainOf(nc *natsgo.Conn) *chain {
	var c *chain
	asyncErr := nc.ErrorHandler()
	if !sameCode(asyncErr, c.onAsyncError) || !sameCode(nc.DisconnectErrHandler(), c.onDisconnect) ||
		!sameCode(nc.ReconnectHandler(), c.onReconnect) || !sameCode(nc.ClosedHandler(), c.onClosed) {
		return nil
	}
	p := &probe{}
	asyncErr(nc, nil, p)
	if p.chain == nil || p.chain.nc != nc {
		return nil
	}
	return p.chain
}

// sameCode reports whether the handler h is a method value of the same
// method as m.
func sameCode(h, m any) bool {
	vh, vm := reflect.ValueOf(h), reflect.ValueOf(m)
	return vh.Kind() == reflect.Func && !vh.IsNil() && vh.Pointer() == vm.Pointer()
}

// join adds b to the chain, unless the chain has retired since it was found.
func (ch *chain) join(b *Backend) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.retired {
		return false
	}
	ch.backends = append(slices.Clip(ch.backends), b)
	return true
}

// leave takes b out of the chain, and puts the connection's own handlers back
// when b was the last and the chain's handlers are still the ones set.
func (ch *chain) leave(b *Backend) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	kept := make([]*Backend, 0, len(ch.backends))
	for _, o := range ch.backends {
		if o != b {
			kept = append(kept, o)
		}
	}
	ch.backends = kept
	if len(kept) > 0 || ch.retired || chainOf(ch.nc) != ch {
		return
	}
	ch.nc.SetErrorHandler(ch.asyncErr)
	ch.nc.SetDisconnectErrHandler(ch.disconnect)
	ch.nc.SetReconnectHandler(ch.reconnect)
	ch.nc.SetClosedHandler(ch.closed)
	ch.retired = true
}

// serving is the backends the chain serves, for one event.
func (ch *chain) serving() []*Backend {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.backends
}

// The four handlers pass each event to every backend the chain serves whose
// health is not closed, then to the connection's own handler. A backend's
// health closes before it leaves, so no hook fires for an event after Close.

func (ch *chain) onAsyncError(c *natsgo.Conn, sub *natsgo.Subscription, err error) {
	var p *probe
	if errors.As(err, &p) {
		p.chain = ch
		return
	}
	for _, b := range ch.serving() {
		if !b.health.Closed() && !b.othersSlowConsumer(sub, err) {
			b.onAsyncError(c, sub, err)
		}
	}
	if ch.asyncErr != nil {
		ch.asyncErr(c, sub, err)
	}
}

func (ch *chain) onDisconnect(c *natsgo.Conn, err error) {
	for _, b := range ch.serving() {
		if !b.health.Closed() {
			b.onDisconnect(c, err)
		}
	}
	switch {
	case ch.disconnect != nil:
		ch.disconnect(c, err)
	case ch.legacyDisconnect != nil:
		ch.legacyDisconnect(c)
	}
}

func (ch *chain) onReconnect(c *natsgo.Conn) {
	for _, b := range ch.serving() {
		if !b.health.Closed() {
			b.onReconnect(c)
		}
	}
	if ch.reconnect != nil {
		ch.reconnect(c)
	}
}

func (ch *chain) onClosed(c *natsgo.Conn) {
	for _, b := range ch.serving() {
		if !b.health.Closed() {
			b.onClosed(c)
		}
	}
	if ch.closed != nil {
		ch.closed(c)
	}
}

// othersSlowConsumer reports whether err is a slow consumer on a subscription
// this backend did not open: the caller's own, or another backend's on the
// same handed-over connection. Its drops are not this backend's, so they
// neither degrade its health nor count in [Backend.SlowConsumers]. A slow
// consumer nats.go names no subscription for is the connection's, and counts.
func (b *Backend) othersSlowConsumer(sub *natsgo.Subscription, err error) bool {
	if sub == nil || !isSlowConsumer(err) {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for s := range b.srcs {
		if s.sub == sub {
			return false
		}
	}
	return true
}

// unlink takes this backend out of the chain on a handed-over connection,
// when it closes or fails to open. The chain held a closed backend, and every
// backend ever opened on a borrowed connection stayed alive for as long as
// the connection lived, about 12 KB each.
func (b *Backend) unlink() {
	if b.chain != nil {
		b.chain.leave(b)
	}
}

// seedHealth is the health a backend starts with: the state of its
// connection, which a handed-over one may have lost or closed. It used to be
// Ready whatever the connection was doing.
func (b *Backend) seedHealth(nc *natsgo.Conn) {
	switch {
	case nc.IsClosed():
		b.health.Close()
	case nc.IsConnected():
		b.health.Ready()
	default:
		b.seeded = b.errs.Map(mq.OpOpen, "", "", natsgo.ErrDisconnected)
		b.health.Degraded(b.seeded)
	}
}

// Health is the backend's current state.
//
// A backend handed a connection that is not connected starts Degraded
// ([Backend.seedHealth]), and the handlers make it Ready when the connection
// comes back. A connection still making its first connection, which
// nats.RetryOnFailedConnect allows, reports that connection to ConnectedCB
// alone, which nats.go gives no getter or setter to chain onto, so the
// backend stayed Degraded on a working connection. So while the state is
// still the one it started with and the connection is up, the answer is
// Ready. Any event since replaces that state, and is answered as it is.
func (b *Backend) Health() mq.HealthSnapshot {
	snap := b.BaseBackend.Health()
	//nolint:errorlint // identity is what says nothing has happened since the seed
	if snap.State == mq.HealthDegraded && b.seeded != nil && snap.Err == b.seeded && b.nc.IsConnected() {
		return mq.HealthSnapshot{State: mq.HealthReady}
	}
	return snap
}

// refuseWaits ends every Request waiting for a reply from a subject the
// server has now refused to publish to, with the refusal as the cause. The
// server discards a refused publication and sends nothing more, so the
// Request used to wait out its context for a reply that could never come.
func (b *Backend) refuseWaits(refusal error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for cancel, subject := range b.waits {
		if subject != "" && refuses(refusal, subject) {
			(*cancel)(refusal)
		}
	}
}

// SlowConsumers is how many times a subscription of this backend has been
// reported as a slow consumer — that is, how many times the client has started
// dropping messages it could not deliver.
//
// It is a counter and not a rate, and it is here because the health carries
// only the latest reason: a client that was Degraded and is Ready again still
// lost messages, and this is what says so.
func (b *Backend) SlowConsumers() uint64 { return b.slow.Load() }

// Reconnects is how many times this backend's connection has been
// re-established.
func (b *Backend) Reconnects() uint64 { return b.reconnects.Load() }

// DegradedTransitions is how many times this backend has entered the Degraded
// state, for a caller that samples health rather than watching it.
func (b *Backend) DegradedTransitions() uint64 { return b.degraded.Load() }

// now reads the driver's clock, which is the injected one when the caller gave
// us one and the wall clock otherwise.
func (b *Backend) now() time.Time {
	if b.clock != nil {
		return b.clock.Now()
	}
	return time.Now()
}

// dropped counts one piece of metadata left out under MetadataLenientDrop.
//
// The specification asks for a hook and a metric; the SPI gives a sink neither, so this is
// the metric and [Backend.DroppedMetadata] is how a caller reads it. The
// missing hook is recorded in the maintenance notes.
func (b *Backend) dropped(string) { b.drops.Add(1) }

// DroppedMetadata is how many header or property values this backend has left
// out under MetadataLenientDrop.
func (b *Backend) DroppedMetadata() uint64 { return b.drops.Load() }

// counters is the backend's background bookkeeping, split out so that the
// struct literal in backend.go stays about the connection.
type counters struct {
	slow       atomic.Uint64
	reconnects atomic.Uint64
	degraded   atomic.Uint64
	drops      atomic.Uint64
}
