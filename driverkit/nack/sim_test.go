package nack

import (
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
)

// The executable reference model of One partition, one consumer, a
// virtual clock, and the strategy's plans applied to all three.
//
// It is here rather than in internal/model because the four kinds do not agree
// on an answer — that is the whole point of the family — so there is no single
// oracle to compare them against. What there IS is a contract every kind has to
// keep, and the model is what makes it checkable:
//
//   - the watermark never passes a record that has neither been handled nor
//     been routed somewhere by a plan (no loss);
//   - a record below the watermark is never delivered again (no zombie);
//   - a record is never finalised twice (no duplicate completion);
//   - a plan that pauses a partition never also completes its offset, because
//     the redelivery the pause waits for would never come;
//   - a paused partition always has something held to redeliver.
//
// # The cost model
//
// decision column is "throughput vs failure rate 0.1/1/10 %;
// ordering violations; lag", and none of the three is a property of the Plan
// call: they are properties of what the plan does to the partition. So the
// model carries a virtual clock, in ticks of one microsecond, and charges the
// main consumer for the work a real one would do:
//
//   - costHandle per delivery attempt, the handler's own time;
//   - costPublish per republication, the produce round trip a retry-topic or
//     dead-letter route pays inside the settlement;
//   - the plan's own Delay while a paused partition waits for its redelivery,
//     which is the dead time block-partition buys its ordering with;
//   - costRestart per consumer restart, the group rejoin a stopped consumer
//     pays before it sees anything again.
//
// A retry topic is drained by a SECOND consumer, so its handling time is
// charged to a clock of its own and does not slow the partition down. That is
// the deployment the strategy describes, and it is why retry topics win on
// throughput and lose on ordering: both halves are measured here rather than
// asserted.
//
// Lag is the time-average of the uncommitted backlog — the integral of
// (newest record − commit watermark) over the main consumer's virtual time,
// divided by that time. A strategy that holds a partition still keeps a large
// backlog for a long time and scores badly; one that commits immediately
// scores well.

// The cost model's constants, in ticks. One tick is a microsecond, so a
// millisecond of configured nack delay is a thousand ticks.
const (
	costHandle  = 10
	costPublish = 20
	costRestart = 5_000
	tickUnit    = time.Microsecond
)

// simConfig is one simulated workload.
type simConfig struct {
	// Records is how many records the partition holds.
	Records int
	// FailEvery makes every FailEvery-th record fail once. Zero means none
	// fails.
	FailEvery int
	// FailTwice makes a failing record fail on its second attempt too,
	// which is what exercises a strategy's second decision about the same
	// record.
	FailTwice bool
	// MaxSteps bounds the simulation, so that a strategy which redelivers
	// for ever ends the test rather than hanging it.
	MaxSteps int
	// RestartOnStop makes the simulation restart a stopped consumer from
	// the committed watermark, which is what a supervised deployment does
	// and what makes Stop's throughput and lag comparable with the other
	// kinds' rather than incomparable.
	RestartOnStop bool
}

// sim is the simulated consumer.
type sim struct {
	t   testing.TB
	s   Strategy
	cfg simConfig

	topic string

	// next is the next offset the partition would deliver.
	next int64
	// high is the offset after the newest record in the partition.
	high int64
	// committed is the commit watermark: every offset below it is resolved.
	committed int64
	// resolved holds the offsets completed above the watermark.
	resolved map[int64]bool
	// remaining maps an offset to the failures its handler has left.
	remaining map[int64]int
	// held are the records an OpRetryInPlace put back, in the order they
	// were held.
	held []heldRecord
	// paused reports whether the partition is paused: nothing new is
	// delivered until the held record has been redelivered.
	paused bool
	// retry maps a retry topic to the records republished onto it.
	retry map[string][]retried
	// onRetry counts how many retry topics hold a record, so the no-loss
	// check costs nothing.
	onRetry map[int64]int
	// dlq holds the records routed to the dead-letter topic.
	dlq []int64
	// stopped reports whether a plan stopped the consumer.
	stopped bool
	// stops counts how many times a plan stopped the consumer.
	stops int

	// finalised records the offsets whose record has reached a terminal
	// state, so that a second one is a defect rather than a retry.
	finalised map[int64]bool
	// maxFinal is the highest offset finalised so far; a lower one after it
	// is an ordering violation.
	maxFinal int64
	// hasFinal reports whether maxFinal means anything yet.
	hasFinal bool

	// ticks is the main consumer's virtual clock.
	ticks int64
	// retryTicks is the second consumer's, which the main one does not wait
	// for.
	retryTicks int64
	// lagIntegral is Σ (high − committed) × Δticks over the main clock.
	lagIntegral int64

	// Metrics.
	delivered  int
	violations int
	maxLag     int64
}

// heldRecord is a record an OpRetryInPlace put back into the partition.
type heldRecord struct {
	off     int64
	attempt uint32
	topic   string
}

// retried is a record republished onto a retry topic.
type retried struct {
	off     int64
	attempt uint32
}

func newSim(tb testing.TB, s Strategy, cfg simConfig) *sim {
	if cfg.MaxSteps == 0 {
		cfg.MaxSteps = 20 * (cfg.Records + 1)
	}
	sm := &sim{
		t: tb, s: s, cfg: cfg, topic: "orders",
		high:      int64(cfg.Records),
		resolved:  make(map[int64]bool, cfg.Records),
		remaining: make(map[int64]int, cfg.Records/max(cfg.FailEvery, 1)+1),
		retry:     map[string][]retried{},
		onRetry:   map[int64]int{},
		finalised: make(map[int64]bool, cfg.Records),
	}
	fails := 1
	if cfg.FailTwice {
		fails = 2
	}
	if cfg.FailEvery > 0 {
		for off := range sm.high {
			if off%int64(cfg.FailEvery) == 0 {
				sm.remaining[off] = fails
			}
		}
	}
	return sm
}

// run drives the simulation to completion or to MaxSteps, and returns how many
// steps it took.
func (sm *sim) run() int {
	for step := range sm.cfg.MaxSteps {
		if !sm.step() {
			return step
		}
	}
	return sm.cfg.MaxSteps
}

// advance moves the main clock and accumulates the lag integral.
func (sm *sim) advance(d int64) {
	if backlog := sm.high - sm.committed; backlog > 0 {
		sm.lagIntegral += backlog * d
	}
	sm.ticks += d
	if backlog := sm.high - sm.committed; backlog > sm.maxLag {
		sm.maxLag = backlog
	}
}

// step delivers and settles one record, and reports whether there was one.
func (sm *sim) step() bool {
	if sm.stopped {
		if !sm.cfg.RestartOnStop {
			return false
		}
		// A supervised deployment restarts the consumer, which re-joins
		// the group and re-reads from the committed watermark: everything
		// above it is delivered again. That is the cost Stop pays, and
		// charging it is what makes its throughput comparable rather than
		// incomparable.
		sm.advance(costRestart)
		sm.stopped = false
		sm.next = sm.committed
		sm.held = nil
		sm.paused = false
	}
	var (
		off     int64
		attempt uint32
		topic   string
	)
	switch {
	case len(sm.held) > 0:
		h := sm.held[0]
		sm.held = sm.held[1:]
		sm.paused = false
		off, attempt, topic = h.off, h.attempt, h.topic
	case sm.paused:
		sm.t.Fatal("the partition is paused with nothing held: the pause can never end")
		return false
	case sm.next < sm.high:
		off, attempt, topic = sm.next, 1, sm.topic
		sm.next++
	default:
		return sm.drainRetryTopics()
	}
	sm.deliver(off, attempt, topic)
	return true
}

// drainRetryTopics consumes one record from the lowest non-empty retry topic,
// which is what a retry-topic deployment's second consumer does. Its time is
// charged to that consumer's clock and not to the partition's.
func (sm *sim) drainRetryTopics() bool {
	for n := 1; n <= 64; n++ {
		name := RetryTopicName(sm.topic, n)
		q := sm.retry[name]
		if len(q) == 0 {
			continue
		}
		r := q[0]
		sm.retry[name] = q[1:]
		sm.onRetry[r.off]--
		// A republished record is a new record on a new topic, so its
		// offset is its own; the model keeps the original offset so that
		// "this record" stays identifiable across the republication.
		sm.deliver(r.off, r.attempt, name)
		return true
	}
	return false
}

// deliver hands one record to the handler and settles it.
func (sm *sim) deliver(off int64, attempt uint32, topic string) {
	sm.t.Helper()
	main := topic == sm.topic
	if main && off < sm.committed {
		sm.t.Fatalf("record %d was delivered below the commit watermark %d", off, sm.committed)
	}
	sm.delivered++
	if main {
		sm.advance(costHandle)
	} else {
		sm.retryTicks += costHandle
	}
	if sm.remaining[off] <= 0 {
		sm.ack(off, topic)
		return
	}
	sm.remaining[off]--
	p, err := sm.s.Plan(Delivery{Topic: topic, Partition: 0, Offset: off, Attempt: attempt}, mq.NackOptions{})
	if err != nil {
		sm.t.Fatalf("Plan(off=%d, attempt=%d, topic=%q): %v", off, attempt, topic, err)
	}
	if verr := p.Validate(); verr != nil {
		sm.t.Fatalf("Plan(off=%d) returned %+v, which does not validate: %v", off, p, verr)
	}
	sm.apply(off, topic, p)
}

// ack finalises a record the handler accepted.
func (sm *sim) ack(off int64, topic string) {
	sm.t.Helper()
	sm.finalise(off)
	if topic == sm.topic {
		sm.complete(off)
	}
}

// apply performs one plan.
func (sm *sim) apply(off int64, topic string, p Plan) {
	sm.t.Helper()
	main := topic == sm.topic
	switch p.Op {
	case OpRetryInPlace:
		sm.held = append(sm.held, heldRecord{off: off, attempt: p.Attempt, topic: topic})
		sm.paused = p.Pause
		// The partition waits out the hold: this is the dead time
		// block-partition buys its ordering with, and the reason the
		// criterion asks for throughput at three failure rates.
		if wait := int64(p.Delay / tickUnit); wait > 0 {
			if main {
				sm.advance(wait)
			} else {
				sm.retryTicks += wait
			}
		}
	case OpRepublish:
		sm.retry[p.Topic] = append(sm.retry[p.Topic], retried{off: off, attempt: p.Attempt})
		sm.onRetry[off]++
		sm.charge(main, costPublish)
		if main {
			sm.complete(off)
		}
	case OpDeadLetter:
		sm.dlq = append(sm.dlq, off)
		sm.charge(main, costPublish)
		sm.finalise(off)
		if main {
			sm.complete(off)
		}
	case OpDrop:
		sm.finalise(off)
		if main {
			sm.complete(off)
		}
	case OpStop:
		sm.stopped = true
		sm.stops++
	default:
		sm.t.Fatalf("plan for %d names no operation: %+v", off, p)
	}
}

// charge bills the produce round trip to whichever consumer performed it.
func (sm *sim) charge(main bool, d int64) {
	if main {
		sm.advance(d)
		return
	}
	sm.retryTicks += d
}

// finalise records that a record has reached a terminal state, counting the
// ordering violation when it did so after a higher offset.
func (sm *sim) finalise(off int64) {
	sm.t.Helper()
	if sm.finalised[off] {
		sm.t.Fatalf("record %d was finalised twice", off)
	}
	sm.finalised[off] = true
	switch {
	case !sm.hasFinal:
		sm.maxFinal, sm.hasFinal = off, true
	case off < sm.maxFinal:
		sm.violations++
	default:
		sm.maxFinal = off
	}
}

// complete resolves an offset and advances the watermark over everything now
// contiguous below the newest record.
func (sm *sim) complete(off int64) {
	sm.t.Helper()
	if off < sm.committed {
		sm.t.Fatalf("offset %d was completed below the watermark %d", off, sm.committed)
	}
	if sm.resolved[off] {
		sm.t.Fatalf("offset %d was completed twice", off)
	}
	sm.resolved[off] = true
	for sm.resolved[sm.committed] {
		// No loss: an offset the watermark passes must have been handled
		// or routed. Only the offsets the watermark actually moves over
		// are checked, so the whole run stays linear.
		if o := sm.committed; sm.remaining[o] > 0 && !sm.finalised[o] && sm.onRetry[o] == 0 {
			sm.t.Fatalf("the watermark reached %d while that record was neither handled nor routed", o)
		}
		delete(sm.resolved, sm.committed)
		sm.committed++
	}
}

// pending is how many of the partition's records have not reached a terminal
// state.
func (sm *sim) pending() int {
	n := 0
	for off := range sm.high {
		if !sm.finalised[off] {
			n++
		}
	}
	return n
}

// lag is the time-average uncommitted backlog over the main consumer's virtual
// time, which is the specification's "lag" for this family.
func (sm *sim) lag() float64 {
	if sm.ticks == 0 {
		return 0
	}
	return float64(sm.lagIntegral) / float64(sm.ticks)
}

// mustStrategy builds a strategy or fails the test.
func mustStrategy(tb testing.TB, k Kind, cfg Config) Strategy {
	tb.Helper()
	s, err := New(k, cfg)
	if err != nil {
		tb.Fatalf("New(%v): %v", k, err)
	}
	return s
}

// configFor is the configuration each kind is simulated with: enough retry
// levels and attempts for the workload to finish, a dead-letter destination so
// that ADR-011's failure route is a route rather than a halt, and a
// millisecond of nack delay, which is the hold a real deployment gives a
// transient failure.
func configFor(k Kind) Config {
	cfg := Config{MaxAttempts: 8, DeadLetterTopic: "orders.dlq", Retries: 3, Delay: time.Millisecond}
	if k.Resolve() == Custom {
		// The custom arm is a fourth policy rather than a copy of one of
		// the three: it republishes like a retry topic but keeps every
		// attempt on one side topic, which is what a deployment with a
		// single "needs attention" stream writes.
		cfg.Decide = func(_ Delivery, o mq.NackOptions) (Plan, error) {
			if o.Action != mq.NackActionRequeue {
				return Plan{Op: OpDrop}, nil
			}
			return Plan{Op: OpRepublish, Topic: "orders.retry.1", Attempt: 2}, nil
		}
	}
	return cfg
}
