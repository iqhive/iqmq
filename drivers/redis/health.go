package redis

import (
	mq "github.com/iqhive/iqmq"
)

// Health is the backend's current state.
//
// It is answered without I/O: a caller polling it must not become load on the
// broker, which for a driver whose every operation is a round trip would be
// easy to get wrong.
//
// What moves it is worth stating, because on this driver the interesting
// transitions come from the BACKGROUND rather than from an operation:
//
//   - Connecting, then Ready, at open.
//   - Degraded when a reclaim sweep fails. That is the transition that
//     matters most here and has no counterpart on most brokers: the sweep is
//     the only thing that redelivers, so a sweep that keeps failing is a
//     subscription that has silently stopped being at-least-once, and the specification
//     requires it to surface somewhere other than a log.
//   - Degraded when an entry cannot be decoded, which is a record in the
//     stream that nothing can deliver.
//   - Degraded when the delayed mover fails, which is a parked redelivery
//     that is not happening.
//   - Ready again on the next successful operation, and Closed by Close.
//
// rueidis owns its own reconnection loop and reports nothing back, so a lost
// link shows here as the failure of whatever command met it rather than as a
// transition of its own. That is the same gap `drivers/nats` recorded from
// the other side, and it is why driverkit.Reconnector is not used: there is no
// dial loop for it to run.
func (b *Backend) Health() mq.HealthSnapshot { return b.health.Snapshot() }

// HealthReporter is proved in backend.go's assertion block.
