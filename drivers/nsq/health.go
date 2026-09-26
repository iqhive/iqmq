package nsq

import (
	"context"
	"sync/atomic"

	mq "github.com/iqhive/iqmq"
)

// counters are the driver's own background-failure numbers, which the specification asks
// for beside the health state.
type counters struct {
	reconnects  atomic.Uint64
	degraded    atomic.Uint64
	dropped     atomic.Uint64
	undecodable atomic.Uint64
	stale       atomic.Uint64
}

// Reconnects is how many times a subscription's connection came back.
func (b *Backend) Reconnects() uint64 { return b.reconnects.Load() }

// DegradedTransitions is how many times the backend entered the degraded state.
func (b *Backend) DegradedTransitions() uint64 { return b.degraded.Load() }

// DroppedMetadata is how many pieces of metadata MetadataLenientDrop has
// discarded, which is the metric for the mode.
func (b *Backend) DroppedMetadata() uint64 { return b.dropped.Load() }

// Undecodable is how many deliveries could not be decoded.
//
// On this driver that is an envelope a producer wrote under wider limits than
// this consumer's, or a body a non-iqmq producer put on the topic while the
// subscription is in MetadataPortableEnvelope mode. There is no settlement to
// reject such a delivery with — nsqd's verbs are finish, requeue and touch,
// and none of them says "this is not my message" — so it is requeued and
// counted, and the specification makes the count the operator's evidence.
func (b *Backend) Undecodable() uint64 { return b.undecodable.Load() }

// Stale is how many settlements the local lease check refused.
//
// It is the number that says the check is doing something. A driver whose Stale
// is always zero under a workload that overruns its deadlines is not checking
// the lease, it is hoping.
func (b *Backend) Stale() uint64 { return b.stale.Load() }

// degrade records a background failure and moves the health state.
func (b *Backend) degrade(err error) {
	if err == nil {
		return
	}
	if b.health.State() != mq.HealthDegraded {
		b.degraded.Add(1)
	}
	b.health.Degraded(err)
}

// recovered records a connection coming back and fires OnReconnect.
//
// It is the one hook a driver fires: a Backend is built before the Client that
// holds the core's hooks, so the core cannot fire it (recorded in
// hack/FOLLOWUPS.md). A caller that installs one Hooks value on both
// mq.WithHooks and Config.Hooks counts every reconnection twice; that is the
// recorded v0 shape and this driver documents it rather than guessing.
func (b *Backend) recovered(ctx context.Context) {
	n := b.reconnects.Add(1)
	b.health.Ready()
	if b.cfg.Hooks.OnReconnect != nil {
		b.cfg.Hooks.OnReconnect(ctx, mq.ReconnectEvent{Driver: DriverName, Attempt: int(n)})
	}
}
