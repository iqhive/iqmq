package streams

import (
	"strconv"
	"time"
)

// zsetMover is the delayed kind: acknowledge the entry and park it
// in a sorted set scored by the instant it becomes deliverable.
//
// It is the only kind that honours an arbitrary delay, which is what the specification's
// tentative answer ("zset for delays") expects of it. Everything else about
// it is a cost:
//
//   - one script call per nack and one per release, against zero for
//     [leavePending];
//   - a sorted set per shard, which is state the driver has to sweep and which
//     an operator has to know about;
//   - a new stream identifier on release, so the message loses its position
//     in the shard and its delivery counter starts again — the attempt count
//     therefore travels in the entry ([FieldAttempt]);
//   - the durability of the delay is the server's persistence, not the
//     stream's: a parked entry lives in a sorted set, so it survives exactly
//     what the AOF or the snapshot preserves. That is why the capability
//     table advertises the delay as EmulatedDurable with the condition
//     stated, rather than as native.
//
// Both halves are atomic. The park acknowledges and adds in one script, so a
// crash cannot leave the message both pending and parked (a duplicate) or
// neither (a loss). The release removes and adds in one script, and the ZREM
// decides the winner, so two movers racing over one due member produce one
// redelivery.
type zsetMover struct{ base }

// Kind returns ZSetMover.
func (zsetMover) Kind() Kind { return ZSetMover }

// Delays reports true: this is the kind that can.
func (zsetMover) Delays() bool { return true }

// Reorders reports true: a released entry gets a new identifier, so it goes
// to the end of its shard.
func (zsetMover) Reorders() bool { return true }

// Plan parks the entry.
//
// A zero delay is parked with a score of now, not sent straight back into the
// stream. That keeps ONE code path for the kind — the mover releases it on
// its next pass, microseconds later — and it keeps the atomicity argument
// above true for every nack rather than for the delayed ones only. A caller
// who wants a zero-delay retry to be immediate asks for [ReAdd], which is the
// kind whose whole point that is.
func (s zsetMover) Plan(req NackRequest) (NackPlan, error) {
	if plan, done, err := s.commonPlan(req); done {
		return plan, err
	}
	if len(req.Entry.Fields) == 0 {
		return NackPlan{}, ErrNoFields
	}
	delay := req.Options.Delay
	if delay < 0 {
		delay = 0
	}
	deliverAt := req.Now.Add(delay)
	fields := rewriteInternal(req.Entry.Fields, req.Attempt, s.cfg.Group)
	member := PackDelayed(req.Entry.Shard, req.Entry.ID, req.Attempt, fields)
	stream := s.cfg.Keys.Stream(req.Entry.Shard)
	zset := s.cfg.Keys.Delayed(req.Entry.Shard)
	cmd := EvalSHA(SHAOf(ScriptDelayPark),
		[]string{stream, zset},
		[]string{
			s.cfg.Group, s.cfg.Consumer, req.Entry.ID.String(),
			strconv.FormatInt(unixMillis(deliverAt), 10),
			member,
		})
	return NackPlan{Cmds: []Cmd{cmd}, Settled: true, RedeliverAfter: delay}, nil
}

// Release builds the command that puts one parked member back into its
// stream.
//
// It is separate from [zsetMover.Plan] because the mover is a loop rather than
// a settlement: it reads the due members with [ZRangeDue] and releases them
// one at a time, so that a member it cannot decode stops that member and not
// the sweep.
func (s zsetMover) Release(d Delayed, member string) Cmd {
	return EvalSHA(SHAOf(ScriptDelayRelease),
		[]string{s.cfg.Keys.Delayed(d.Shard), s.cfg.Keys.Stream(d.Shard)},
		append([]string{member, strconv.FormatInt(s.cfg.MaxLen, 10)}, d.Fields...))
}

// unixMillis is a sorted-set score: milliseconds since the epoch.
//
// A zset score is a float64, which holds an exact integer up to 2^53 — about
// 285,000 years of milliseconds — so the score is exact for every time a
// caller can construct and the conversion loses nothing. Nanoseconds would
// not be: 2^53 nanoseconds is 104 days.
func unixMillis(t time.Time) int64 { return t.UnixMilli() }
