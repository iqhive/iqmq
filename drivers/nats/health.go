package nats

import (
	"context"
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

// chainHandlers puts this backend's connection handlers in front of the ones
// a connection handed to [BackendWithClient] already has, which still run
// after them.
//
// nats.go keeps one handler of each kind. Setting the driver's own would
// silence the caller's, and setting none, which is what a handed-over
// connection used to get, left Health blind to a lost link, a reconnection, a
// closed connection and a slow consumer, and Hooks.OnReconnect never fired.
// The forwarders stay on the connection after the backend closes: putting
// the old handler back would also remove any handler set after this one, the
// caller's or another backend's on the same connection. They reach the
// backend only through link, which [Backend.unlink] empties when the backend
// closes or fails to open, so a closed backend is not kept alive by the
// connection and its half does nothing; only the caller's handler runs. A
// handler the caller sets later replaces the forwarder, and with it the
// driver's half.
func (b *Backend) chainHandlers(nc *natsgo.Conn) {
	link := &atomic.Pointer[Backend]{}
	link.Store(b)
	b.link = link
	asyncErr, disconnect, reconnect, closed := nc.ErrorHandler(), nc.DisconnectErrHandler(), nc.ReconnectHandler(), nc.ClosedHandler()
	// The deprecated disconnect handler has no getter, and nats.go calls it
	// only when no DisconnectErrHandler is set, which installing one here
	// would otherwise stop.
	legacyDisconnect := nc.Opts.DisconnectedCB
	nc.SetErrorHandler(func(c *natsgo.Conn, sub *natsgo.Subscription, err error) {
		if b := linked(link); b != nil && !b.othersSlowConsumer(sub, err) {
			b.onAsyncError(c, sub, err)
		}
		if asyncErr != nil {
			asyncErr(c, sub, err)
		}
	})
	nc.SetDisconnectErrHandler(func(c *natsgo.Conn, err error) {
		if b := linked(link); b != nil {
			b.onDisconnect(c, err)
		}
		switch {
		case disconnect != nil:
			disconnect(c, err)
		case legacyDisconnect != nil:
			legacyDisconnect(c)
		}
	})
	nc.SetReconnectHandler(func(c *natsgo.Conn) {
		if b := linked(link); b != nil {
			b.onReconnect(c)
		}
		if reconnect != nil {
			reconnect(c)
		}
	})
	nc.SetClosedHandler(func(c *natsgo.Conn) {
		if b := linked(link); b != nil {
			b.onClosed(c)
		}
		if closed != nil {
			closed(c)
		}
	})
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

// linked is the backend a forwarder serves, or nil once that backend has
// closed or failed to open.
func linked(link *atomic.Pointer[Backend]) *Backend {
	b := link.Load()
	if b == nil || b.health.Closed() {
		return nil
	}
	return b
}

// unlink empties the link the forwarders reach this backend through. A
// forwarder that held the backend itself kept every backend ever opened on a
// borrowed connection alive for as long as the connection lived, about 12 KB
// each.
func (b *Backend) unlink() {
	if b.link != nil {
		b.link.Store(nil)
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
		b.health.Degraded(b.errs.Map(mq.OpOpen, "", "", natsgo.ErrDisconnected))
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
