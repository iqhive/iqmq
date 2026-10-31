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
//     stream that nothing can deliver. It counts as a failed sweep until the
//     source's next good one.
//   - Degraded when the delayed mover fails, which is a parked redelivery
//     that is not happening.
//   - Ready again once no open source's sweep or mover is failing: each one
//     that failed has had a good round since, or its source has closed. A
//     good round of one source says nothing about another's, so one source
//     failing beside a healthy one keeps the backend Degraded. Closed by
//     Close.
//
// A return to Ready calls Hooks.OnReconnect on the goroutine that made it:
// the sweep or mover of the source that recovered, or the Source.Close that
// removed the last failing source, in which case nothing reconnected. The
// hook must not block, as every hook must not, and it must not close the
// consumer or the client either: their Close waits for that sweep or mover,
// which is running the hook, so it would never return. Telling the two
// causes apart would need the transition's cause, which the health
// reporter's callback does not carry, and running the hook anywhere else
// would need a goroutine of its own.
//
// rueidis owns its own reconnection loop and reports nothing back, so a lost
// link shows here as the failure of whatever command met it rather than as a
// transition of its own. That is the same gap `drivers/nats` recorded from
// the other side, and it is why driverkit.Reconnector is not used: there is no
// dial loop for it to run.
func (b *Backend) Health() mq.HealthSnapshot { return b.health.Snapshot() }

// HealthReporter is proved in backend.go's assertion block.

// The bits of source.failing, one per background loop.
const (
	sweepFailing uint32 = 1 << iota
	moverFailing
)

// loopFailure is the latest failure of one background loop, numbered by the
// backend (Backend.loopFailures) so that the health can name the most recent
// failure of a loop that is still failing.
type loopFailure struct {
	err error
	seq uint64
}

// failure is the record of s's loop that bit names. The caller holds b.mu.
func (s *source) failure(bit uint32) *loopFailure {
	if bit == moverFailing {
		return &s.moverErr
	}
	return &s.sweepErr
}

// loopRound records how one round of a background loop of s went, bit naming
// the loop, and brings the health into line.
//
// A good round of a loop that was not failing changes nothing and returns at
// once, so a healthy source costs one atomic load per round.
func (b *Backend) loopRound(s *source, bit uint32, err error) {
	switch {
	case err != nil:
		b.mu.Lock()
		b.loopFailures++
		*s.failure(bit) = loopFailure{err: err, seq: b.loopFailures}
		b.mu.Unlock()
		s.failing.Or(bit)
	case s.failing.Load()&bit == 0:
		return
	default:
		s.failing.And(^bit)
	}
	b.healthGen.Add(1)
	b.followLoops()
}

// loopsGone brings the health into line once s has closed: a source that was
// failing when it closed cannot report a good round again, and must not keep
// the backend Degraded. The caller has already removed s from the backend.
func (b *Backend) loopsGone(s *source) {
	if s.failing.Load() == 0 {
		return
	}
	b.healthGen.Add(1)
	b.followLoops()
}

// followLoops sets the health from the open sources' loops: Degraded with the
// latest failure while any of them is failing, Ready once none is.
//
// The health is set outside the lock, because a return to Ready calls the
// configuration's OnReconnect hook, so another loop can change the picture
// while it is being set and the two updates can land in either order. Each
// pass therefore reads healthGen before it looks and again after it has set
// the health, and looks again when it moved: whichever update lands last was
// made from the latest picture.
func (b *Backend) followLoops() {
	for {
		gen := b.healthGen.Load()
		switch err := b.failingLoop(); {
		case err != nil:
			b.health.Degraded(err)
		default:
			b.health.Ready()
		}
		if b.healthGen.Load() == gen {
			return
		}
	}
}

// failingLoop is the latest failure of a loop of an open source that is still
// failing, and nil once none is.
//
// It is the latest failure of a loop STILL failing, and not the backend's
// latest failure, because a loop that failed and recovered beside another
// that keeps failing used to leave its own error in the health until the
// other's next failed round, up to a claim interval later.
func (b *Backend) failingLoop() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var latest loopFailure
	for s := range b.srcs {
		failing := s.failing.Load()
		for _, bit := range [...]uint32{sweepFailing, moverFailing} {
			if f := *s.failure(bit); failing&bit != 0 && f.seq > latest.seq {
				latest = f
			}
		}
	}
	return latest.err
}
