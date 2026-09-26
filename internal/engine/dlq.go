package engine

import (
	"errors"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/registry"
)

// Dead-lettering (the memory engine, adr/ADR-011-failure-policy.md).
//
// A delivery that fails once its record has used up TopologyConfig.MaxAttempts
// attempts is not redelivered: it enters the dead-letter-pending state of the lifecycle, the engine publishes the record to the subscription's
// dead-letter topic, and only a committed publication settles the original.
// A failed or ambiguous dead-letter publication never settles it — the record
// goes back to its lane and is tried again — which is the one rule
// ADR-011 states twice.
//
// The publication is the engine's own, so it never parks: a dead-letter topic
// under OverflowBlock refuses it with errcode.ErrQueueFull instead of waiting
// for room, and the record goes back to its lane like any other failed
// dead-letter publication. Otherwise a single full dead-letter topic would
// stop the engine's shared scheduler goroutine — delayed promotion, lease
// expiry and TTL sweeps for every lane — against the memory engine, and
// would park a Consumer.Nack that has no context to cancel.
//
// The publication carries the record itself and no copy: the engine takes one
// more reference on the payload, so the dead-letter topic's own settlement
// releases it, and the publisher's Release callback still runs exactly once.
// Provenance headers (`iqmq-origin-topic`, `iqmq-attempts`, …) are the
// adapter's business: the engine is generic in the payload and never inspects
// or extends it.

// ErrDeadLetterFailed is what the dead-letter fault of Faults.FailDeadLetter
// makes a dead-letter publication fail with. It is never returned to a caller:
// a failed dead-letter publication returns its record to the lane and is
// counted in Snapshot.DeadLetterFailures. A dead-letter topic that has no
// room for the record fails the same way, whatever its overflow policy says
// a publisher of its own would do: the engine never waits on itself.
var ErrDeadLetterFailed = errcode.New(errcode.Unavailable, "iqmq/engine: dead-letter publication failed")

// dlqTopic resolves the subscription's dead-letter topic, or reports
// ErrTopicNotFound when it has none or the named topic does not exist. A
// topic is never removed from an engine, so the resolution is cached.
func (s *Subscription[P]) dlqTopic() (*Topic[P], error) {
	if s.dlqName == "" {
		return nil, errcode.ErrTopicNotFound
	}
	if t := s.dlq.Load(); t != nil {
		return t, nil
	}
	t, ok := s.eng.topics.Load(registry.Intern(s.dlqName))
	if !ok {
		// A dead-letter topic is never created behind the operator's back
		// (the message contract: "never auto-create a production DLQ").
		return nil, errcode.ErrTopicNotFound
	}
	s.dlq.Store(t)
	return t, nil
}

// deadLetter routes e away from its subscription because the record has used
// up its delivery attempts. It reports whether the record left: false means
// the dead-letter publication failed or was ambiguous, and the caller must
// put the record back, which is the one rule ADR-011 states twice.
//
// A subscription with no dead-letter topic discards the record and counts it
// in Snapshot.Dropped, exactly as a broker with a redelivery limit and no
// dead-letter exchange discards it: MaxAttempts is a limit whether or not
// there is somewhere to send the record, and redelivering it for ever would
// make the limit mean nothing.
//
// It must be called with the lane mutex released and with the delivery
// already unlinked from the in-flight list, which is the dead-letter-pending
// state of the memory engine: nothing else can claim, settle or expire the
// record while the publication is in flight.
func (ln *lane[P]) deadLetter(e *Envelope[P]) bool {
	s := ln.sub
	eng := s.eng
	dlq, err := s.dlqTopic()
	if err != nil {
		ln.drop(e, true)
		return true
	}
	// The record is dead-letter-pending in the sense of the memory engine:
	// out of its lane and out of the in-flight list. Telling the lifecycle
	// is what keeps "accepted before acknowledged" true through a shutdown —
	// the settling phase waits for every open publication before it drops
	// what the lanes hold — and a nil answer means the engine is already
	// past the point where a record may leave a lane at all, so the record
	// goes back like any other dead-letter failure.
	pending := eng.lc.BeginDeadLetter()
	if pending == nil {
		s.st.dlqFailed.Add(1)
		return false
	}
	if err = eng.faults.dlqPublish(); err == nil {
		rec := e.rec
		// The dead-letter publication holds its own reference, so the
		// payload outlives the settlement of this delivery and is released
		// once — by whichever of the two settles last.
		rec.addRef()
		// The publication is internal, so it never parks: a dead-letter
		// topic with no room is a dead-letter failure like any other
		// (Topic.publishInternal). lane.expireLeases reaches this from the
		// engine's single scheduler goroutine and Consumer.Nack from a
		// caller that has no context to cancel, and neither may wait.
		err = dlq.publishInternal(eng.ctx, Publish[P]{
			Payload:   rec.v,
			Key:       rec.key,
			Size:      int(rec.size),
			Partition: -1,
			Release:   func(P) { eng.dropRecordRef(rec, true) },
		})
		if err != nil && !errors.Is(err, errcode.ErrPartial) {
			// Nothing was admitted, so the reference the publication would
			// have released is given back. It cannot reach zero here: this
			// delivery still holds one.
			rec.unref()
		}
	}
	if err != nil && !errors.Is(err, errcode.ErrPartial) {
		pending.Abort()
	} else {
		pending.Commit()
	}
	// The ordering hook itself: whether the original may be settled is read
	// from the publication rather than decided again here, so a settlement
	// cannot overtake a dead-letter publication that failed or was
	// abandoned (models/tla/Shutdown.tla, DLQBeforeAck).
	if !pending.SettleAllowed() {
		s.st.dlqFailed.Add(1)
		return false
	}
	ln.discard(e, true, s.st.deadLettered)
	return true
}
