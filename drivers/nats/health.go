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
