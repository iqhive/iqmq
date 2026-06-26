package kafkago

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq"
)

// The specification for this driver is the consumer group's generation lifecycle, and
// that is a narrower observation point than the franz-go driver has.
//
// kafka-go offers NO connection hook of any kind: neither [kgo.Writer] nor
// [kgo.Transport] nor [kgo.Dialer] reports a dial, a dial failure or a
// disconnection, and [bitbucket.org/iqhive/iqmq/driverkit.Reconnector] cannot be
// used either because the client owns its own dial loop. So the events this
// driver can report are the ones it drives itself:
//
//   - a generation ESTABLISHED is Ready, and every generation after the first
//     is counted as a reconnection and fires
//     [bitbucket.org/iqhive/iqmq.Hooks.OnReconnect]. That is coarser than a
//     socket — a group rejoins for a rebalance as well as for a broken
//     connection — and it is the only signal the client gives, so the driver
//     page says what the number means ;
//   - a generation ENDED is Degraded, because the subscription is not
//     consuming until the next one is established;
//   - a fetch error, a commit failure or an undecodable record is Degraded
//     with the mapped reason.
//
// A backend with no source therefore stays Ready from the moment Open's
// metadata round trip succeeded, and a publication that fails reports Degraded
// through [sink.produceFailed]'s health call on the paths where nobody is
// waiting. A caller that needs socket-level health polls [Backend.Ping].

// onHealth runs after every health transition. It exists so that a transition
// has one place to fan out from, and it is where the Degraded reason is
// counted.
func (b *Backend) onHealth(snap mq.HealthSnapshot) {
	if snap.State == mq.HealthDegraded {
		b.degraded.Add(1)
	}
}

// generationUp reports that a source established a generation.
//
// The first one is the subscription starting; every one after it is a rejoin,
// which is the closest thing this client has to a reconnection.
func (b *Backend) generationUp(ctx context.Context, first bool) {
	b.health.Ready()
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
// is not consuming until the next one is established.
func (b *Backend) generationDown() {
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
	b.health.Degraded(errDisconnected)
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
