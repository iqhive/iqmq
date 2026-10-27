package streams

import "time"

// leavePending is the reference kind: a nack that sends nothing.
//
// The entry is already in the group's pending-entries list — XREADGROUP put
// it there — and the mandatory reclaim loop takes it back once it has been
// idle for the acknowledgement deadline. So "make this deliverable again" is
// "do not acknowledge it", which is what the driver has already done by not
// acknowledging it.
//
// That is why it is the reference and the expected default: it is the only
// kind with no failure mode of its own. Nothing is written, so nothing can be
// half-written; the identifier does not change, so the shard's order and the
// delivery counter survive; and the redelivery happens even if this process
// dies between the nack and the redelivery, because the redelivery was never
// this process's to arrange. A nack that asks for a delay sends one guarded
// command, which restarts the idle clock and moves nothing ([base.holdBack]).
type leavePending struct{ base }

// Kind returns LeavePending.
func (leavePending) Kind() Kind { return LeavePending }

// Delays reports false: the redelivery happens when the reclaim loop sweeps.
func (leavePending) Delays() bool { return false }

// Reorders reports false: the entry keeps its identifier and therefore its
// position.
func (leavePending) Reorders() bool { return false }

// Plan returns the empty plan for a requeue, restarts the entry's idle clock
// for a delayed one, and refuses a delay longer than the acknowledgement
// deadline.
//
// The refusal is where this kind is honest and could easily not be. A delay
// SHORTER than the deadline is accepted, because the redelivery then happens
// later than asked and the contract is "held back for at least this long"
// — arriving late is within it. A delay LONGER than the deadline would arrive
// EARLY, which is outside it: the caller asked for the message not to be
// deliverable yet, and it would be. Reporting ErrUnsupported sends the caller
// to the kind that can do it; quietly redelivering at one deadline instead of
// the ten minutes asked for would look like it worked.
//
// "Later than asked" holds only once the idle clock restarts at the nack
// ([base.holdBack]): left alone it runs from the delivery, and a nack late in
// the lease would come back before its delay.
func (s leavePending) Plan(req NackRequest) (NackPlan, error) {
	if plan, done, err := s.commonPlan(req); done {
		return plan, err
	}
	if req.Options.Delay > s.cfg.AckDeadline {
		return NackPlan{}, ErrDelayUnsupported
	}
	return NackPlan{Cmds: s.holdBack(req), RedeliverAfter: s.redeliverAfter()}, nil
}

// redeliverAfter is one acknowledgement deadline: the entry becomes claimable
// when it has been idle for that long.
//
// It is the deadline and not the deadline plus a sweep interval, because the
// sweep interval is the driver's own polling and the plan describes what the
// BROKER will allow. A source whose sweep runs every deadline sees a
// redelivery at one to two deadlines; the driver page says so.
func (s leavePending) redeliverAfter() time.Duration { return s.cfg.AckDeadline }
