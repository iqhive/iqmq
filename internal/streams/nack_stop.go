package streams

// stop is the ordering-preserving kind: leave the entry pending AND
// stop reading new entries from its shard until it is settled.
//
// It is [leavePending] plus back-pressure on one shard, and the difference is
// what a caller who asked for strict per-key order means by a retry. Under
// every other kind, a message that failed is retried while the messages
// behind it are being delivered: the retry arrives late (leave-pending,
// zset) or at the end (re-add), and in both cases a later message for the
// same key has already been handled. Under this kind nothing behind the
// failed message moves until it is settled, so the shard's order survives the
// retry exactly.
//
// The cost is head-of-line blocking, and it is not small: one message whose
// handler always fails stops its whole shard until MaxAttempts
// dead-letters it. That is the correct behaviour for a strictly ordered
// stream and the wrong behaviour for a work queue, which is why this is an
// arm of the bake-off and not the default.
//
// The hold is the SOURCE's to honour — [NackPlan.HoldShard] is how the plan
// says so — because the broker has no notion of it: a Redis consumer group
// will happily hand a consumer the next entry, and the only thing that can
// decline it is the consumer.
type stop struct{ base }

// Kind returns Stop.
func (stop) Kind() Kind { return Stop }

// Delays reports false: the redelivery is the reclaim loop's, as for
// leave-pending.
func (stop) Delays() bool { return false }

// Reorders reports false — it is the kind that exists not to.
func (stop) Reorders() bool { return false }

// Plan holds the shard and sends nothing, or for a delayed nack only the
// idle-clock restart [leavePending] sends for the same reason.
//
// A delay longer than the acknowledgement deadline is refused for
// [leavePending]'s reason: the redelivery would arrive early. A drop or a
// dead-letter does NOT hold the shard, because both settle the entry — there
// is nothing left to preserve the order of, and holding the shard would stop
// it for ever.
func (s stop) Plan(req NackRequest) (NackPlan, error) {
	if plan, done, err := s.commonPlan(req); done {
		return plan, err
	}
	if req.Options.Delay > s.cfg.AckDeadline {
		return NackPlan{}, ErrDelayUnsupported
	}
	return NackPlan{Cmds: s.holdBack(req), HoldShard: true, RedeliverAfter: s.cfg.AckDeadline}, nil
}
