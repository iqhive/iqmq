package kafka

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
)

// The client's hooks are where the specification lives for this driver.
//
// franz-go owns its own dial loop — which is why
// [github.com/iqhive/iqmq/driverkit.Reconnector] is NOT used here, and the
// mismatch is recorded in the maintenance notes — so the only way a reconnection
// reaches a caller is through the client's hooks. A driver that did not wire
// them would leave a caller polling a Health that never changed while every
// publication timed out.
type clientHooks struct {
	b *Backend
	// down reports whether a connection has been lost since the last
	// successful one, so that the first dial of the client's life is not
	// reported as a reconnection.
	down atomic.Bool
}

// compile-time proof of which of franz-go's hooks this driver takes.
var (
	_ kgo.HookBrokerConnect    = (*clientHooks)(nil)
	_ kgo.HookBrokerDisconnect = (*clientHooks)(nil)
)

// OnBrokerConnect is franz-go's per-attempt dial report: it fires once per
// attempt, failures included, which is exactly what the specification wants reported.
func (h *clientHooks) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err != nil {
		h.b.health.Degraded(h.b.errs.MapNet(mq.OpOpen, "", "", err, false))
		return
	}
	h.b.health.Ready()
	if !h.down.CompareAndSwap(true, false) {
		// The first connection of the client's life, or one of several to
		// different brokers: neither is a reconnection.
		return
	}
	n := int(h.b.reconnects.Add(1))
	if hook := h.b.cfg.Hooks.OnReconnect; hook != nil {
		// franz-go gives its dial callback no context, so the hook gets
		// Background: there is no operation here to be part of.
		hook(context.Background(), mq.ReconnectEvent{Driver: DriverName, Attempt: n})
	}
}

// OnBrokerDisconnect records that a connection went away. The client is
// retrying, so the state is Degraded and not Closed: Degraded is
// "usable but something is wrong, or not usable and may recover", which is
// exactly a Kafka client between two connections.
//
// It asks the HEALTH REPORTER whether the backend is closed, never the
// backend. [github.com/iqhive/iqmq/driverkit.BaseBackend.Close] holds its own
// mutex across the driver's teardown AND the native client's close, and
// franz-go fires this hook from inside that close — so a hook that called
// b.Closed() would take the mutex its own caller is holding and hang the
// client's Close for ever. It is a real deadlock and it happened on the first
// run of this driver's first test (recorded in the maintenance notes).
//
// Asking the reporter is also the correct layering: driverkit.Health makes
// Closed terminal, so a Degraded after the close is refused rather than
// reported, and this check only saves the counter an increment.
func (h *clientHooks) OnBrokerDisconnect(_ kgo.BrokerMetadata, _ net.Conn) {
	if h.b.health.Closed() {
		// A disconnect during Close is the close, not a failure.
		return
	}
	h.down.Store(true)
	h.b.health.Degraded(errDisconnected)
}

// onHealth runs after every health transition. It exists so that a transition
// has one place to fan out from, and it is where the Degraded reason is
// counted.
func (b *Backend) onHealth(snap mq.HealthSnapshot) {
	if snap.State == mq.HealthDegraded {
		b.degraded.Add(1)
	}
}

// Reconnects is how many times this backend's client has re-established a
// broker connection after losing one.
func (b *Backend) Reconnects() uint64 { return b.reconnects.Load() }

// DegradedTransitions is how many times this backend has entered the Degraded
// state, for a caller that samples health rather than watching it.
func (b *Backend) DegradedTransitions() uint64 { return b.degraded.Load() }

// Rebalances is how many partition revocations this backend's sources have
// been through. It is the number that says whether a group is stable: the specification's
// health carries only the latest reason, and a subscription that rebalances
// every few seconds is one whose session timeout or handler duration is wrong.
func (b *Backend) Rebalances() uint64 { return b.rebalances.Load() }

// LostPartitions is how many partitions this backend's sources have had taken
// away without a chance to commit — OnPartitionsLost rather than
// OnPartitionsRevoked.
//
// It is the counter that matters most on this driver: a lost partition is
// redelivery, because the safe boundary could not be committed. A non-zero
// value means the at-least-once window opened wider than
// [Config.CommitInterval] and duplicates reached the application.
func (b *Backend) LostPartitions() uint64 { return b.lost.Load() }

// Commits is how many offset commits this backend's sources have made.
func (b *Backend) Commits() uint64 { return b.commits.Load() }

// AmbiguousPublications is how many publications this backend could not
// classify as either accepted or refused.
//
// It is a counter and not a rate, and it is here because an ambiguous
// publication is the one outcome a caller cannot resolve on its own: the
// record may be in the log. A deployment that sees this rise knows its
// at-least-once publication window is open and can reconcile.
func (b *Backend) AmbiguousPublications() uint64 { return b.ambiguous.Load() }

// DroppedMetadata is how many header or property values this backend has left
// out under MetadataLenientDrop.
//
// The specification asks lenient mode for "a hook and a metric"; the SPI gives a sink
// neither, so this is the metric and the missing hook is recorded in
// the maintenance notes. On this driver it is always zero: Kafka can represent
// every field the specification names — ordered, duplicate-preserving, binary headers and
// a nullable key and value — so lenient mode has nothing to drop and the
// counter exists to say so rather than to count.
func (b *Backend) DroppedMetadata() uint64 { return b.drops.Load() }
