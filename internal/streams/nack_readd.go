package streams

import "strconv"

// reAdd is the immediate kind: acknowledge the entry and add it back
// under a new identifier, in one script.
//
// It is the fastest redelivery available — the message is deliverable again
// before the nack has returned — and the only kind whose pending-entries list
// does not grow with the number of messages in flight, because a nacked entry
// leaves the list instead of sitting in it until the reclaim loop sweeps.
// That is the "PEL growth" column, and it is the one column
// this kind wins.
//
// It pays with order and with attempt accounting. A new identifier puts the
// message at the END of its shard, so a retried message is delivered after
// everything published since it failed — which breaks the strict per-key
// order across a retry, and which is why [Strategy.Reorders] exists and why a
// driver bound to this kind cannot advertise ordered-by-key support.
// The attempt count starts again with the new entry, so it travels in the
// entry itself; without that, a message that always fails would be retried
// for ever and MaxAttempts would never dead-letter it.
type reAdd struct{ base }

// Kind returns ReAdd.
func (reAdd) Kind() Kind { return ReAdd }

// Delays reports false: there is nowhere to hold the message.
func (reAdd) Delays() bool { return false }

// Reorders reports true.
func (reAdd) Reorders() bool { return true }

// Plan re-adds the entry.
//
// Any positive delay is refused. This kind has no mechanism to hold a message
// back at all — the re-add makes it deliverable at once — so accepting a
// delay would mean redelivering immediately and reporting that the delay was
// honoured. The specification says a driver that cannot honour a nack's action returns
// ErrUnsupported and settles nothing, and that is what happens: the entry is
// left pending, which leaves the delivery settleable.
func (s reAdd) Plan(req NackRequest) (NackPlan, error) {
	if plan, done, err := s.commonPlan(req); done {
		return plan, err
	}
	if req.Options.Delay > 0 {
		return NackPlan{}, ErrDelayUnsupported
	}
	if len(req.Entry.Fields) == 0 {
		return NackPlan{}, ErrNoFields
	}
	fields := rewriteInternal(req.Entry.Fields, req.Attempt, s.cfg.Group)
	args := make([]string, 0, 4+len(fields))
	args = append(args, s.cfg.Group, s.cfg.Consumer, req.Entry.ID.String(),
		strconv.FormatInt(s.cfg.MaxLen, 10))
	args = append(args, fields...)
	cmd := EvalSHA(SHAOf(ScriptReAdd), []string{s.cfg.Keys.Stream(req.Entry.Shard)}, args)
	return NackPlan{Cmds: []Cmd{cmd}, Settled: true}, nil
}
