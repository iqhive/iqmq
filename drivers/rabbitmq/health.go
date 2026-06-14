package rabbitmq

import (
	"errors"

	mq "bitbucket.org/iqhive/iqmq"
)

// Health is the backend's current state (HealthReporter).
//
// It is answered without I/O, so a caller polling it never becomes load on the
// broker, and it is driven from three places: the dial loop (Connecting,
// Ready, Degraded), `connection.blocked` (Degraded with the broker's own
// reason, then Ready again) and Close (Closed, which is terminal).
//
// The `connection.blocked` transition is the one worth knowing about, because
// it is the only explanation a stalled publisher has. RabbitMQ under a memory
// or disk alarm stops READING from a publishing connection, so a publication
// blocks in the socket with no error of its own; without this a caller sees a
// hung Publish and nothing else.
//
// A FOURTH thing is read here rather than reported into the state machine: a
// handle with no channel. The state machine belongs to the CONNECTION -- the
// dial loop owns every transition in it -- and a channel exception does not
// touch the connection, so a client whose one publisher had lost its channel
// answered Ready while refusing every publication. It is
// derived rather than reported because a derived answer cannot get stuck: the
// repair route would otherwise have to report Ready when it finished, which is
// a transition it is not entitled to make -- a redial may be in flight, and
// the dial loop's own Connecting would be overwritten by a publisher that had
// nothing to say about the connection. See [Backend.detachedHandle].
func (b *Backend) Health() mq.HealthSnapshot {
	var snap mq.HealthSnapshot
	if b.BaseBackend == nil {
		// Before the base exists -- a first dial that has not finished -- the
		// reporter is still the truth, and answering from it is what lets a
		// caller observe a client that is still connecting.
		snap = b.health.Snapshot()
	} else {
		snap = b.BaseBackend.Health()
	}
	if snap.State != mq.HealthReady {
		// Every other state already says something is happening, and Closed is
		// terminal. Only Ready can be a lie.
		return snap
	}
	if why := b.detachedHandle(); why != nil {
		return mq.HealthSnapshot{State: mq.HealthDegraded, Err: why}
	}
	return snap
}

// detachedHandle is why some open handle has no channel, or nil when every one
// of them is fine.
//
// A sink is attached from the moment it is opened, so a sink with no channel
// is always a publisher that refuses everything. A source attaches on its
// FIRST Fetch (see [source.attach]), so "no channel" is its ordinary state
// until then and only a STARTED source without one is broken. A handle that is
// closing is nobody's problem: it is on its way out and `b.forget` is about to
// drop it.
//
// It walks the handles rather than keeping a count, and that is deliberate.
// A counter has to be incremented and decremented on matching edges, and the
// edges here are not matched: `detachLocked` is reached from a channel
// exception, from a publication that met a dead channel, from the supervision
// loop and from Close, while only an attach puts the channel back. A count
// that drifted would report a healthy client Degraded for ever, which is the
// one failure mode worse than the one this fixes. The walk costs one snapshot
// of the handle maps and ONE ATOMIC LOAD per handle -- no handle's mutex is
// taken, because both are held across a socket write (see [sink.stranded]) --
// on a call the specification already makes I/O-free rather than allocation-free.
//
// A handle whose one-shot repair FAILED is reported in preference to one that
// is merely between channels, because the two are different news: the first
// will not mend itself and the caller has to close the handle and open
// another, the second is in hand. Both are looked for in one pass, and the
// failure wins.
func (b *Backend) detachedHandle() error {
	sinks, srcs := b.children()
	var detached error
	for _, s := range sinks {
		why := s.detachedReason()
		if why == nil {
			continue
		}
		if !errors.Is(why, errHandleDetached) {
			return why
		}
		detached = why
	}
	for _, s := range srcs {
		why := s.detachedReason()
		if why == nil {
			continue
		}
		if !errors.Is(why, errHandleDetached) {
			return why
		}
		detached = why
	}
	return detached
}
