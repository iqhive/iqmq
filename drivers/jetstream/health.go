package jetstream

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	natsgo "github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
)

// The asynchronous handlers are where the specification lives for this driver.
//
// A NATS client reports everything that goes wrong in the background through
// callbacks: the link dropped, the link came back, a subscription fell behind.
// JetStream adds a second channel — the consume error handler — which is how a
// deleted consumer, a leadership change and a missing heartbeat reach the
// driver. None of it surfaces on a publish or a fetch, so a driver that did not
// wire these would leave a caller polling a Health that never changed while its
// consumer had stopped existing.

// onHealth runs after every health transition. It exists so that a transition
// has one place to fan out from, and it is where the Degraded reason is counted.
func (b *Backend) onHealth(snap mq.HealthSnapshot) {
	if snap.State == mq.HealthDegraded {
		b.degraded.Add(1)
	}
}

// onAsyncError is nats.go's catch-all for a failure that belongs to no call.
func (b *Backend) onAsyncError(_ *natsgo.Conn, sub *natsgo.Subscription, err error) {
	if err == nil {
		return
	}
	if isSlowConsumer(err) {
		b.slow.Add(1)
		topic := ""
		if sub != nil {
			topic = TopicOf(b.cfg.SubjectPrefix, sub.Subject)
		}
		b.health.Degraded(b.errs.Map(mq.OpFetch, topic, "", err))
		return
	}
	b.health.Degraded(b.errs.Map(mq.OpFetch, "", "", err))
}

// onDisconnect records that the link went away. The client is retrying, so the
// state is Degraded and not Closed: Degraded is "usable but something is
// wrong, or not usable and may recover", which is exactly a NATS client between
// two connections.
func (b *Backend) onDisconnect(_ *natsgo.Conn, err error) {
	if err == nil {
		err = natsgo.ErrDisconnected
	}
	b.health.Degraded(b.errs.Map(mq.OpOpen, "", "", err))
}

// onReconnect records that the link came back, clears the error and fires the
// caller's hook.
//
// It deliberately does NOT invalidate the source token tables. On JetStream an
// acknowledgement is a message to a delivery's own `$JS.ACK.…` reply subject,
// and that subject survives a reconnection: the consumer, its pending state and
// its ack floor are the server's, not the connection's. So a settlement made
// after a reconnection is still the right settlement for its delivery, and
// invalidating the generation here would answer ErrLeaseLost to a caller whose
// ack would have worked. What DOES make a token stale on this broker is a
// redelivery of the same record, which source.go detects by stream sequence.
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

// onClosed records that the connection will not come back. Closed is terminal in
// driverkit.Health, so a connection the server closed for good cannot be
// reported Ready again by a late callback.
func (b *Backend) onClosed(*natsgo.Conn) { b.health.Close() }

// onConsumeError is the JetStream consume loop's own error channel: a deleted
// consumer, a leadership change, a missing heartbeat, a pull request the server
// refused.
//
// It is a background failure in exactly the sense — nothing on the fetch
// path reports it, and the symptom is a consumer that simply stops receiving —
// so it reaches health and a counter. The topic and subscription are the
// source's, which is what makes the snapshot readable when a client has several.
func (b *Backend) onConsumeError(src *source, err error) {
	if err == nil {
		return
	}
	b.consumeErrs.Add(1)
	b.health.Degraded(b.errs.Map(mq.OpFetch, src.topic, src.subscription, err))
}

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
		if b := linked(link); b != nil {
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
// reported as a slow consumer.
//
// It matters less here than on the Core profile — a delivery the client dropped
// is unacked, so the server redelivers it — but it is still the signal that the
// prefetch and the pending limits disagree, and the health carries only the
// latest reason.
func (b *Backend) SlowConsumers() uint64 { return b.slow.Load() }

// Reconnects is how many times this backend's connection has been
// re-established.
func (b *Backend) Reconnects() uint64 { return b.reconnects.Load() }

// DegradedTransitions is how many times this backend has entered the Degraded
// state, for a caller that samples health rather than watching it.
func (b *Backend) DegradedTransitions() uint64 { return b.degraded.Load() }

// ConsumeErrors is how many failures the JetStream consume loops of this backend
// have reported.
func (b *Backend) ConsumeErrors() uint64 { return b.consumeErrs.Load() }

// StaleSettlements is how many settlements this backend has refused because the
// record had already been redelivered, or because the delivery's lease had run
// out and the server may have redelivered it to another consumer.
//
// It is the counter the stale-ack guard is read off. A deployment whose handlers
// routinely take longer than their lease sees it climb, and every increment is a
// message that would otherwise have been acknowledged out from under the
// delivery that owned it.
func (b *Backend) StaleSettlements() uint64 { return b.stale.Load() }

// Redeliveries is how many times this backend has seen a record delivered again.
func (b *Backend) Redeliveries() uint64 { return b.redeliveries.Load() }

// SupersededDeliveries is how many deliveries this backend dropped from a fetch
// ring because the record had been delivered again, or their lease had run
// out, while they waited, counting those whose lease ran out in the client
// library before they reached the ring.
//
// It is the cost of a lease that is too short for the handlers: every increment
// is a delivery the broker sent, the client buffered and nobody will ever
// process. A deployment that sees it climb raises Config.AckWait or the
// consumer's AckDeadline.
func (b *Backend) SupersededDeliveries() uint64 { return b.superseded.Load() }

// AsyncBodyCopies is how many payloads this backend has copied because the
// publication was asynchronous.
//
// It is the obligation made countable. Every asynchronous path copies the
// caller's payload, because the JetStream client retains the message it was
// given and republishes it on a "no responders" retry and after a reconnection —
// a retry that read the caller's slice would put whatever the caller wrote next
// on the wire. The counter is also the honest answer to "what does PublishAsync
// allocate": one payload copy per publication, and nothing else.
func (b *Backend) AsyncBodyCopies() uint64 { return b.bodyCopies.Load() }

// DeadLettered is how many messages this backend has routed to the dead-letter
// subject and terminated.
func (b *Backend) DeadLettered() uint64 { return b.deadLettered.Load() }

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
// the metric and [Backend.DroppedMetadata] is how a caller reads it. The missing
// hook is recorded in the maintenance notes.
func (b *Backend) dropped(string) { b.drops.Add(1) }

// DroppedMetadata is how many header or property values this backend has left
// out under MetadataLenientDrop.
func (b *Backend) DroppedMetadata() uint64 { return b.drops.Load() }

// counters is the backend's background bookkeeping, split out so that the struct
// literal in backend.go stays about the connection.
type counters struct {
	slow         atomic.Uint64
	reconnects   atomic.Uint64
	degraded     atomic.Uint64
	drops        atomic.Uint64
	consumeErrs  atomic.Uint64
	stale        atomic.Uint64
	superseded   atomic.Uint64
	redeliveries atomic.Uint64
	deadLettered atomic.Uint64
	bodyCopies   atomic.Uint64
}

// consumeErrHandler adapts a source to the client library's consume error
// callback.
func (b *Backend) consumeErrHandler(src *source) js.ConsumeErrHandlerFunc {
	return func(_ js.ConsumeContext, err error) {
		if pullsAgain(err) {
			src.repulls.Add(1)
		}
		b.onConsumeError(src, err)
	}
}

// pullsAgain reports whether nats.go v1.55.0 reports err to a consume loop
// that has reset its count of the messages it asked for and asked for a whole
// prefetch, without counting what it already held: a missed heartbeat, and a
// change of the consumer's leader. The messages that prefetch brings can queue
// behind the ones already held ([arrivals]).
//
// A leadership change counts although the old leader's requests end with it:
// the library sets its count to zero there, which forgets the messages it has
// been sent and not yet counted off, and any request it made after the change.
// A missed heartbeat counts although its re-pull may never have reached a
// consumer: the library reports a 503 for it without correcting its count,
// and nothing ties that 503 to the re-pull rather than to a smaller request,
// so taking it back could undercount.
func pullsAgain(err error) bool {
	return errors.Is(err, js.ErrNoHeartbeat) || errors.Is(err, js.ErrConsumerLeadershipChanged)
}
