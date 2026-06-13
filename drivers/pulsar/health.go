package pulsar

import (
	"context"
	"sync/atomic"

	mq "bitbucket.org/iqhive/iqmq"
)

// counters are the driver's own background-failure numbers, which the specification asks
// for beside the health state.
type counters struct {
	reconnects  atomic.Uint64
	degraded    atomic.Uint64
	dropped     atomic.Uint64
	undecodable atomic.Uint64
	stale       atomic.Uint64
	leaseLost   atomic.Uint64
	deadLetter  atomic.Uint64
	held        atomic.Int64
}

// Reconnects is how many times the driver observed the client come back.
//
// pulsar-client-go reconnects inside its own connection pool and offers no
// callback for it, so the only reconnection this driver can SEE is a Ping that
// succeeds after one failed. It is coarse — a caller that never calls Ping
// never sees one — and the driver page says so.
func (b *Backend) Reconnects() uint64 { return b.reconnects.Load() }

// DegradedTransitions is how many times the backend entered the degraded state.
func (b *Backend) DegradedTransitions() uint64 { return b.degraded.Load() }

// DroppedMetadata is how many pieces of metadata MetadataLenientDrop has
// discarded, which is the metric for the mode.
func (b *Backend) DroppedMetadata() uint64 { return b.dropped.Load() }

// Undecodable is how many deliveries could not be decoded: an envelope written
// under wider limits than this consumer's, or a body a non-iqmq producer put on
// a topic whose consumer is in MetadataPortableEnvelope mode.
//
// Such a delivery is negatively acknowledged rather than acknowledged — it is
// somebody's message and this consumer cannot read it — and the count is the
// operator's evidence.
func (b *Backend) Undecodable() uint64 { return b.undecodable.Load() }

// Stale is how many settlements the local lease check refused.
//
// It is the number that says the check is doing something. A driver whose Stale
// is always zero under a workload that overruns its deadlines is not checking
// the lease, it is hoping.
func (b *Backend) Stale() uint64 { return b.stale.Load() }

// LeaseLost is how many deliveries the lease sweeper gave back before their
// deadline, which is the emulated lease working.
func (b *Backend) LeaseLost() uint64 { return b.leaseLost.Load() }

// DeadLettered is how many deliveries this driver routed to the dead-letter
// topic through an explicit Reject. It does NOT count the ones pulsar-client-go
// routed on its own once a message passed Config.MaxAttempts, because that
// happens inside the client and below this driver's view.
func (b *Backend) DeadLettered() uint64 { return b.deadLetter.Load() }

// HeldDelayedNacks is how many delayed negative settlements are waiting in this
// process right now, which is the EmulatedVolatile delay's cost made visible:
// each one is a delivery Pulsar still considers unsettled and a timer this
// process has to survive.
func (b *Backend) HeldDelayedNacks() int64 { return b.held.Load() }

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

// recovered records the client coming back and fires OnReconnect.
//
// It is the one hook a driver fires: a Backend is built before the Client that
// holds the core's hooks, so the core cannot fire it (recorded in
// the maintenance notes). A caller that installs one Hooks value on both
// mq.WithHooks and Config.Hooks counts every reconnection twice; that is the
// recorded v0 shape and this driver documents it rather than guessing.
func (b *Backend) recovered(ctx context.Context) {
	n := b.reconnects.Add(1)
	b.health.Ready()
	if b.cfg.Hooks.OnReconnect != nil {
		b.cfg.Hooks.OnReconnect(ctx, mq.ReconnectEvent{Driver: DriverName, Attempt: int(n)})
	}
}
