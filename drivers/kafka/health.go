package kafka

import (
	"context"
	"errors"
	"net"
	"sync"
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
	// connected reports whether the client has ever connected, which is
	// what makes a dial that fails a lost link rather than a client that
	// has not come up yet.
	connected atomic.Bool
}

// compile-time proof of which of franz-go's hooks this driver takes.
var (
	_ kgo.HookBrokerConnect = (*clientHooks)(nil)
	_ kgo.HookBrokerE2E     = (*clientHooks)(nil)
)

// OnBrokerConnect is franz-go's per-attempt dial report: it fires once per
// attempt, failures included, which is exactly what the specification wants reported.
//
// A dial that fails once the client has connected is a lost link too, and
// the next one that succeeds is a reconnection. A client whose connections
// franz-go has reaped has nothing in flight when the broker goes away, so
// failed dials are all it sees of the outage: without this its recovery was
// Ready with no reconnection counted and OnReconnect never fired. A dial that
// fails before the first success, such as Open's, is not one: the first
// connection is not a reconnection.
func (h *clientHooks) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err != nil {
		if h.connected.Load() {
			h.down.Store(true)
		}
		h.b.failed(&h.b.linkPath, h.b.errs.MapNet(mq.OpOpen, "", "", err, false))
		return
	}
	h.connected.Store(true)
	h.b.recovered(&h.b.linkPath)
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

// OnBrokerE2E is franz-go's report of one request's round trip. A write or a
// read that failed is what a lost link is: franz-go closes the connection and
// dials again for the next request. So it records that a connection was lost,
// and the client is retrying, so the state is Degraded and not Closed:
// Degraded is "usable but something is wrong, or not usable and may recover",
// which is exactly a Kafka client between two connections. The next
// connection's [clientHooks.OnBrokerConnect] clears the lost link, which is
// Ready unless another path is failing ([paths]), and counts the
// reconnection.
//
// A disconnect alone is not a lost link, which is why this driver does not
// take franz-go's OnBrokerDisconnect. franz-go runs it for every connection it
// closes: one idle for its ConnIdleTimeout, thirty seconds by default, which
// it reaps, and every connection of a client that is closing, such as a
// consumer's group client. Reading those as losses set a healthy client
// Degraded until it next dialled, and counted that dial as a reconnection.
// The seed connection Open's Ping used is reaped once the client has learned
// the brokers, so every client went Degraded within a minute of opening and
// stayed there while it published.
//
// A request also fails when its own context ends or its client closes, and
// neither is the link failing.
//
// It asks the HEALTH REPORTER whether the backend is closed, never the
// backend. [github.com/iqhive/iqmq/driverkit.BaseBackend.Close] holds its own
// mutex across the driver's teardown AND the native client's close, and
// franz-go fires its hooks from inside that close, so a hook that called
// b.Closed() would take the mutex its own caller is holding and hang the
// client's Close for ever. It is a real deadlock and it happened on the first
// run of this driver's first test (recorded in the maintenance notes).
//
// Asking the reporter is also the correct layering: driverkit.Health makes
// Closed terminal, so a Degraded after the close is refused rather than
// reported, and this check only saves the counter an increment.
func (h *clientHooks) OnBrokerE2E(_ kgo.BrokerMetadata, _ int16, e2e kgo.BrokerE2E) {
	err := e2e.Err()
	if err == nil || !lostLink(err) || h.b.health.Closed() {
		return
	}
	h.down.Store(true)
	h.b.failed(&h.b.linkPath, errDisconnected)
}

// lostLink reports whether a request's failed write or read is the link
// failing, rather than the request's own context ending or its client
// closing.
func lostLink(err error) bool {
	return !errors.Is(err, kgo.ErrClientClosed) &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded)
}

// paths is how a background failure leaves the health: a failure is
// reported for the path it happened on ([Backend.failed]), and that path's
// next success, or its close, clears it ([Backend.recovered]). The health is
// Degraded while any path is failing, with the latest failure's reason, and
// Ready once none is.
//
// The health was set Ready only by a new broker connection, so one refused
// ConfirmNone record, one undecodable record or one failed commit left a
// client whose connections stayed up Degraded for good while every later
// publication, fetch, commit and Ping succeeded, and a readiness probe keyed on
// Health failed with it. A path clears only its own failure, so a path that
// keeps failing keeps the health Degraded through the successes of every
// other: one consumer that cannot commit is not hidden by a publisher that
// can publish, even one whose own failures come and go. A lost link is a path
// of its own, which only a new connection clears
// ([clientHooks.OnBrokerConnect]).
type paths struct {
	// healthMu orders the failures and recoveries, so the health always
	// reflects the set of failing paths as it stands.
	healthMu sync.Mutex
	// failingPaths is every path whose latest outcome is a failure, under
	// healthMu.
	failingPaths map[*healthPath]struct{}
	// failures numbers the failures, under healthMu, so that the reason the
	// health reports is the latest failure of a path still failing.
	failures uint64
	// linkPath is the broker connections' path.
	linkPath healthPath
	// subPaths are EnsureSubscription's start-position paths, one per
	// subscription ([Backend.subscriptionPath]), under healthMu.
	subPaths map[string]*healthPath
}

// healthPath is one background path: a sink's unconfirmed publications, a
// source's fetches or its commits, the broker connections, or one
// subscription's start position as EnsureSubscription left it.
type healthPath struct {
	// failing is set while the path's latest outcome is a failure. It is
	// written under healthMu, and read without it, so that a success on a
	// path that is not failing costs one atomic load of its own.
	failing atomic.Bool
	// err and seq are the path's latest failure and its number, under
	// healthMu.
	err error
	seq uint64
}

// failed reports a background failure on p, which the health now names.
func (b *Backend) failed(p *healthPath, err error) {
	b.healthMu.Lock()
	defer b.healthMu.Unlock()
	if b.failingPaths == nil {
		b.failingPaths = map[*healthPath]struct{}{}
	}
	b.failures++
	p.err, p.seq = err, b.failures
	b.failingPaths[p] = struct{}{}
	p.failing.Store(true)
	b.health.Degraded(err)
}

// recovered reports that p has worked again, or has closed. The health is
// Ready again once no path is failing, and otherwise names the latest failure
// of a path still failing.
func (b *Backend) recovered(p *healthPath) {
	if !p.failing.Load() {
		return
	}
	b.healthMu.Lock()
	defer b.healthMu.Unlock()
	if !p.failing.Load() {
		return
	}
	p.failing.Store(false)
	p.err = nil
	delete(b.failingPaths, p)
	var latest *healthPath
	for q := range b.failingPaths {
		if latest == nil || q.seq > latest.seq {
			latest = q
		}
	}
	if latest == nil {
		b.health.Ready()
		return
	}
	b.health.Degraded(latest.err)
}

// opened sets the health Ready as the backend opens. Open's Ping has just
// succeeded, so the cluster serves, and a dial that failed during it, to one
// broker of several, is not a lost link: it is cleared here, as the open
// always cleared it.
func (b *Backend) opened() {
	b.recovered(&b.linkPath)
	b.healthMu.Lock()
	defer b.healthMu.Unlock()
	if len(b.failingPaths) == 0 {
		b.health.Ready()
	}
}

// subscriptionPath is the path EnsureSubscription reports a soft failure of
// group's start position on, made on first use. It is one per subscription,
// so that what clears it is that subscription working: its next clean
// EnsureSubscription or consumer open ([Backend.ensureGroupPosition]), or a
// commit by any consumer of the group ([Backend.subscriptionWorks]).
func (b *Backend) subscriptionPath(group string) *healthPath {
	b.healthMu.Lock()
	defer b.healthMu.Unlock()
	if b.subPaths == nil {
		b.subPaths = map[string]*healthPath{}
	}
	p := b.subPaths[group]
	if p == nil {
		p = &healthPath{}
		b.subPaths[group] = p
	}
	return p
}

// subscriptionWorks clears an EnsureSubscription failure of group's start
// position, if one is outstanding: the group has committed, or its start
// position has been resolved cleanly since.
func (b *Backend) subscriptionWorks(group string) {
	b.healthMu.Lock()
	p := b.subPaths[group]
	b.healthMu.Unlock()
	if p != nil {
		b.recovered(p)
	}
}

// subscriptionDeleted clears and forgets group's EnsureSubscription path once
// the subscription is deleted: nothing can clear it the ways above any more,
// and a deleted subscription's start position is nobody's concern. Forgetting
// the entry is also what keeps the map from growing with every subscription
// name the process has ensured.
func (b *Backend) subscriptionDeleted(group string) {
	b.healthMu.Lock()
	p := b.subPaths[group]
	delete(b.subPaths, group)
	b.healthMu.Unlock()
	if p != nil {
		b.recovered(p)
	}
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
