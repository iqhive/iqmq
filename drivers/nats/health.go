package nats

import (
	"context"
	"errors"
	"reflect"
	"runtime"
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
	if _, ch := chains(nc); ch != nil && ch.join(b) {
		b.chain = ch
		return
	}
	asyncErr, disconnect, reconnect, closed := nc.ErrorHandler(), nc.DisconnectErrHandler(), nc.ReconnectHandler(), nc.ClosedHandler()
	ch := &chain{
		nc:         nc,
		backends:   []*Backend{b},
		asyncErr:   asyncErr,
		disconnect: disconnect,
		reconnect:  reconnect,
		closed:     closed,
		// The deprecated disconnect handler has no getter, and nats.go
		// calls it only when no DisconnectErrHandler is set, which
		// installing one here would otherwise stop.
		legacyDisconnect: nc.Opts.DisconnectedCB,
		onChain:          isChain(asyncErr, disconnect, reconnect, closed),
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
// goroutine, which ends the process. Now a backend joins this driver's chain
// on the connection, wherever it sits among the chains of the two NATS
// drivers, and leaves it when it closes. A chain whose last backend has left
// puts back the handlers it captured once it is in front, and then asks the
// chain those belong to to do the same if it serves nobody either. So a
// connection carries at most one chain of each NATS driver, and none once
// their backends have closed.
type chain struct {
	nc *natsgo.Conn
	// The handlers the connection had when the chain was put in front of
	// them, never written after.
	asyncErr         natsgo.ErrHandler
	disconnect       natsgo.ConnErrHandler
	legacyDisconnect natsgo.ConnHandler
	reconnect        natsgo.ConnHandler
	closed           natsgo.ConnHandler
	// onChain says those handlers are a chain's, of either NATS driver, so
	// the questions in [chainQuestion] and [chainIdle] go on to them. Never
	// written after.
	onChain bool

	mu sync.Mutex
	// backends are the backends the chain serves. The slice is replaced and
	// never written in place, so a handler ranges over the one it read.
	backends []*Backend
	// retired says the handlers the chain captured are back and the chain
	// serves nobody.
	retired bool
}

// The two NATS drivers can share a connection, which is how natsunified serves
// both profiles, and each must find its own chain however the other's sits in
// front of it: when it did not, every interleaving left another chain behind.
// The drivers share no code, so they share a protocol: two questions, passed
// to a chain's error handler as an error and recognised by their method
// names, which a chain answers and passes on only to handlers that are a
// chain's too, never to the caller's. drivers/jetstream declares the same
// two; the method names are the contract between them.

// chainQuestion is a question for the chains in front of a connection, front
// to back: each chain calls AskIQMQHandlerChain with itself, and passes the
// question on until the call answers true.
type chainQuestion interface {
	error
	AskIQMQHandlerChain(chain any) (answered bool)
}

// chainIdle asks the chain that receives it to put back the handlers it
// captured if it serves nobody and is in front ([chain.retireIfIdle]).
type chainIdle interface {
	error
	RetireIdleIQMQHandlerChain()
}

// probe is this driver's [chainQuestion]: which chain is in front of nc, and
// which is this driver's chain for nc.
type probe struct {
	nc    *natsgo.Conn
	front any
	own   *chain
}

func (*probe) Error() string { return "iqmq/nats: a question for the drivers' own handlers" }

// AskIQMQHandlerChain notes the first chain it is asked by and stops at this
// driver's chain for the connection. A chain made for another connection,
// whose handlers the caller copied onto this one, is not it.
func (p *probe) AskIQMQHandlerChain(c any) bool {
	if p.front == nil {
		p.front = c
	}
	ch, ok := c.(*chain)
	if !ok || ch.nc != p.nc {
		return false
	}
	p.own = ch
	return true
}

// idle is this driver's [chainIdle].
type idle struct{}

func (idle) Error() string { return "iqmq/nats: a chain whose backends have all closed" }

func (idle) RetireIdleIQMQHandlerChain() {}

// chains asks the chains in front of nc, if its handlers are a chain's, for
// the chain in front, of either NATS driver, and for this driver's chain for
// nc, each nil when there is none.
func chains(nc *natsgo.Conn) (front any, own *chain) {
	asyncErr := nc.ErrorHandler()
	if !isChain(asyncErr, nc.DisconnectErrHandler(), nc.ReconnectHandler(), nc.ClosedHandler()) {
		return nil, nil
	}
	p := &probe{nc: nc}
	asyncErr(nc, nil, p)
	return p.front, p.own
}

// isChain reports whether the four handlers are a chain's, of either NATS
// driver.
//
// A func cannot be compared, but reflect reports the code a func value runs,
// and a method value runs the method's wrapper, which the runtime names
// "<package>.(*chain).<method>-fm" for every method value of that method and
// for no other function. So the four are told from any other handler before
// one of them is ever asked a question.
func isChain(asyncErr natsgo.ErrHandler, disconnect natsgo.ConnErrHandler, reconnect, closed natsgo.ConnHandler) bool {
	return chainMethod(asyncErr, "onAsyncError") && chainMethod(disconnect, "onDisconnect") &&
		chainMethod(reconnect, "onReconnect") && chainMethod(closed, "onClosed")
}

// chainMethod reports whether the handler h is a method value of a NATS
// driver chain's method.
func chainMethod(h any, method string) bool {
	v := reflect.ValueOf(h)
	if v.Kind() != reflect.Func || v.IsNil() {
		return false
	}
	f := runtime.FuncForPC(v.Pointer())
	if f == nil {
		return false
	}
	switch f.Name() {
	case "github.com/iqhive/iqmq/drivers/nats.(*chain)." + method + "-fm",
		"github.com/iqhive/iqmq/drivers/jetstream.(*chain)." + method + "-fm":
		return true
	default:
		return false
	}
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

// leave takes b out of the chain, and retires the chain when b was the last.
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
	if len(kept) == 0 {
		ch.retire()
	}
}

// retireIfIdle retires the chain if it serves nobody.
func (ch *chain) retireIfIdle() {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if len(ch.backends) == 0 {
		ch.retire()
	}
}

// retire puts back the handlers the chain captured when its own are the
// connection's, and then asks the chain those belong to, if they are a
// chain's, to do the same if it serves nobody either: that chain's last
// backend may have closed while this one was in front of it. A chain with
// something in front of it stays until it is in front and idle, or until a
// backend of this driver joins it again. ch.mu is held, and the chains below
// lock theirs in turn, front to back.
func (ch *chain) retire() {
	if ch.retired {
		return
	}
	if front, _ := chains(ch.nc); front != any(ch) {
		return
	}
	ch.nc.SetErrorHandler(ch.asyncErr)
	ch.nc.SetDisconnectErrHandler(ch.disconnect)
	ch.nc.SetReconnectHandler(ch.reconnect)
	ch.nc.SetClosedHandler(ch.closed)
	ch.retired = true
	if ch.onChain {
		ch.asyncErr(ch.nc, nil, idle{})
	}
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
	var q chainQuestion
	if errors.As(err, &q) {
		if !q.AskIQMQHandlerChain(ch) && ch.onChain {
			ch.asyncErr(c, sub, err)
		}
		return
	}
	var i chainIdle
	if errors.As(err, &i) {
		ch.retireIfIdle()
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
