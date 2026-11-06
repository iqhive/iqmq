package kafkago

import (
	"context"
	"sync"
	"sync/atomic"

	mq "github.com/iqhive/iqmq"
)

// The specification for this driver is the consumer group's generation lifecycle, and
// that is a narrower observation point than the franz-go driver has.
//
// kafka-go offers NO connection hook of any kind: neither [kgo.Writer] nor
// [kgo.Transport] nor [kgo.Dialer] reports a dial, a dial failure or a
// disconnection, and [github.com/iqhive/iqmq/driverkit.Reconnector] cannot be
// used either because the client owns its own dial loop. So the events this
// driver can report are the ones it drives itself:
//
//   - a generation ESTABLISHED clears its subscription's generation failure,
//     and the health is Ready unless another path is failing ([paths]);
//     every generation after the first is counted as a reconnection and fires
//     [github.com/iqhive/iqmq.Hooks.OnReconnect]. That is coarser than a
//     socket — a group rejoins for a rebalance as well as for a broken
//     connection — and it is the only signal the client gives, so the driver
//     page says what the number means ;
//   - a generation ENDED is Degraded, because the subscription is not
//     consuming until the next one is established, except when it ended
//     because its consumer is closing, which nothing rejoins;
//   - a fetch error, a commit failure or an undecodable record is Degraded
//     with the mapped reason, until the path it happened on works again
//     ([paths]).
//
// A backend with no source therefore stays Ready from the moment Open's
// metadata round trip succeeded, except while a publication nobody waited for
// has failed and its publisher has not published since: that goes to health
// on the paths where nobody is waiting. A caller that needs socket-level
// health polls [Backend.Ping].

// paths is how a background failure leaves the health: a failure is
// reported for the path it happened on ([Backend.failed]), and that path's
// next success, or its close, clears it ([Backend.recovered]). The health is
// Degraded while any path is failing, with the latest failure's reason, and
// Ready once none is.
//
// The health was set Ready only when a consumer group generation was
// established, so one refused ConfirmNone record left a client with no
// consumer Degraded for the rest of its life, and one with a consumer until
// its next rebalance, while every later publication, fetch and Ping
// succeeded. A path clears only its own failure, so a path that keeps failing
// keeps the health Degraded through the successes of every other: one
// partition that cannot be fetched is not hidden by the others, nor a
// consumer by a publisher, even one whose own failures come and go. A
// generation that ended is its subscription's own path, which only the
// subscription's next generation clears ([Backend.generationUp]).
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
	// subPaths are EnsureSubscription's start-position paths, one per
	// subscription ([Backend.subscriptionPath]), under healthMu.
	subPaths map[string]*healthPath
}

// healthPath is one background path: a sink's unconfirmed publications, a
// source's generations, its commits or its decoding, a partition reader's
// fetches, or one subscription's start position as EnsureSubscription left
// it.
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

// opened sets the health Ready as the backend opens, unless a path is failing
// already.
func (b *Backend) opened() {
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

// generationUp reports that a source established a generation, which clears
// path, the source's generation path, and no other.
//
// The first one is the subscription starting; every one after it is a rejoin,
// which is the closest thing this client has to a reconnection.
func (b *Backend) generationUp(ctx context.Context, path *healthPath, first bool) {
	b.recovered(path)
	if first {
		return
	}
	n := int(b.reconnects.Add(1))
	if hook := b.cfg.Hooks.OnReconnect; hook != nil {
		// The generation loop's own context, which is the consumer's: a
		// rejoin belongs to the subscription that is rejoining.
		hook(ctx, mq.ReconnectEvent{Driver: DriverName, Attempt: n})
	}
}

// generationDown reports that a generation ended, which is a subscription that
// is not consuming until the next one is established. path is the source's
// generation path.
func (b *Backend) generationDown(path *healthPath) {
	if b.health.Closed() {
		// A generation that ends during Close is the close, not a failure.
		//
		// It asks the HEALTH REPORTER whether the backend is closed, never the
		// backend: driverkit.BaseBackend.Close holds its own mutex across the
		// driver's teardown AND the native clients' close, and this runs from
		// inside that teardown — so a call to b.Closed() would take the mutex
		// its own caller is holding and hang Close for ever. It is the same
		// deadlock the franz-go driver records.
		return
	}
	b.failed(path, errDisconnected)
}

// Reconnects is how many times this backend's sources have re-established a
// consumer group generation after losing one.
//
// It is NOT a count of sockets. kafka-go gives a driver no connection event at
// all, so a rejoin is the only reconnection this driver can see, and a rejoin
// happens for a rebalance as well as for a broken link.
func (b *Backend) Reconnects() uint64 { return b.reconnects.Load() }

// DegradedTransitions is how many times this backend has entered the Degraded
// state, for a caller that samples health rather than watching it.
func (b *Backend) DegradedTransitions() uint64 { return b.degraded.Load() }

// Rebalances is how many consumer group generations this backend's sources
// have been through. It is the number that says whether a group is stable:
// the health carries only the latest reason, and a subscription that
// rebalances every few seconds is one whose session timeout or handler
// duration is wrong.
//
// Every generation here revokes the WHOLE assignment, because kafka-go has no
// cooperative assignor, so one rebalance costs every partition rather than the
// partitions that moved.
func (b *Backend) Rebalances() uint64 { return b.rebalances.Load() }

// LostPartitions is how many partitions this backend's sources have had taken
// away without a chance to commit: a generation that ended before the driver
// could commit its safe boundary.
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

// DuplicablePublications is how many publications may have left MORE THAN ONE
// copy in the log.
//
// It exists on this driver and not on the franz-go one, and the difference is
// the whole of the producer half: kafka-go's writer sends no producer
// id, so the broker cannot de-duplicate the writer's own retries, and a
// publication that was retried after a lost response is a duplicate nothing
// detects. [Config.MaxAttempts] bounds how many.
func (b *Backend) DuplicablePublications() uint64 { return b.duplicable.Load() }

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
