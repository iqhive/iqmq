// Package fanout is the executable reference model of the engine's fan-out
// admission (the memory engine, the specification D). It is a pure,
// single-goroutine state machine over N subscriptions with a capacity each,
// a reservation ledger and the four admission policies, driven by Step, with
// Invariants checked after every step.
//
// The model is the oracle of the differential fuzz targets FuzzEngineFanout
// and FuzzEngineGroupIsolation: the same publications, settlements and closes
// are applied to internal/engine and to this model, and the outcome of every
// publication, the records every subscription holds and every counter must
// agree. models/tla/FanoutReserve.tla is the formal counterpart and
// models/tla/TRACE.md maps both onto the engine.
//
// # What it models and what it does not
//
// Admission only: which subscriptions a publication reaches, what the
// publisher is told, and how the reservations that were taken are released
// again. Delivery order, leases, redelivery and delays are the business of
// internal/model/memq; here a claimed record is simply held by its
// subscription until it is settled or dropped.
package fanout

import (
	"errors"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Policy is one fan-out admission policy. The values match
// engine.FanoutPolicy one for one.
type Policy uint8

// The admission policies.
const (
	// Atomic admits a publication to every subscription or to none.
	Atomic Policy = iota
	// Independent admits it wherever it fits and tells the publisher that
	// the delivery was partial, with errcode.ErrPartial.
	Independent
	// BestEffort admits it wherever it fits and says nothing unless nothing
	// fit.
	BestEffort
	// DropSlow makes room in a full subscription by dropping the oldest
	// record it holds.
	DropSlow

	policyCount
)

var policyNames = [policyCount]string{"atomic", "independent", "besteffort", "dropslow"}

// Policies returns every policy in declaration order.
func Policies() []Policy { return []Policy{Atomic, Independent, BestEffort, DropSlow} }

// String returns the lower-case policy name, which is the engine's slug.
func (p Policy) String() string {
	if p < policyCount {
		return policyNames[p]
	}
	return "policy(?)"
}

// Kind identifies an operation.
type Kind uint8

// The operations.
const (
	// Publish admits one record under the topic's policy.
	Publish Kind = iota
	// Claim takes one record from a subscription, so that DropSlow has
	// something it cannot drop.
	Claim
	// Settle finishes a record a subscription holds, releasing its
	// reservation.
	Settle
	// Close detaches a subscription; it takes part in no later publication.
	Close
	// Subscribe attaches a new subscription, which receives only what is
	// published after it.
	Subscribe

	kindCount
)

var kindNames = [kindCount]string{"publish", "claim", "settle", "close", "subscribe"}

// String returns the lower-case operation name.
func (k Kind) String() string {
	if k < kindCount {
		return kindNames[k]
	}
	return "op(?)"
}

// Config configures the model to match the engine under test.
type Config struct {
	// Policy is the topic's admission policy.
	Policy Policy
	// Subs is the number of subscriptions the topic starts with.
	Subs int
	// Capacity bounds the records one subscription holds. A non-positive
	// value means unbounded.
	Capacity int
	// CapacityBytes bounds the same set by size. A non-positive value means
	// unbounded.
	CapacityBytes int64
	// Shared reports that the subscriptions share one partition log rather
	// than holding a lane each (the LogCursor architecture): the capacity is
	// then the log's and a slot is reclaimed only when every subscription
	// that saw it has settled it.
	Shared bool
}

// Op is one operation applied by Step.
type Op struct {
	// Kind selects the operation.
	Kind Kind
	// Sub identifies the subscription for Claim, Settle and Close.
	Sub int
	// Size is the accounted size of a published record.
	Size int64
	// Seq identifies the record a Settle applies to.
	Seq uint64
}

// Result is what Step returns; it is what the engine must produce.
type Result struct {
	// Err is the error the engine must return, or nil.
	Err error
	// Reached lists, in subscription order, the subscriptions the
	// publication was admitted to.
	Reached []int
	// Dropped lists the subscriptions that dropped a record to make room.
	Dropped []int
	// Seq is the sequence number a Publish assigned, or 0 when nothing was
	// admitted.
	Seq uint64
}

// state is one record within one subscription.
type state uint8

const (
	// held is queued and not yet claimed.
	held state = iota
	// claimed is out with a consumer.
	claimed
	// gone was settled or dropped.
	gone
)

// rec is one record of one subscription.
type rec struct {
	seq   uint64
	size  int64
	state state
}

// sub is one subscription: the records it holds in admission order and its
// counters.
type sub struct {
	recs   []rec
	bytes  int64
	closed bool

	published, dropped, rejected, settled int64
	// offered counts the publications the subscription was open for, which
	// every policy must account for as either an admission or a rejection.
	offered int64
}

// depth returns the number of records the subscription still holds.
func (s *sub) depth() int {
	n := 0
	for i := range s.recs {
		if s.recs[i].state != gone {
			n++
		}
	}
	return n
}

// slot is one entry of the shared log: the record and the number of
// subscriptions that have not finished with it.
type slot struct {
	seq  uint64
	size int64
	refs int
}

// Model is the reference model of one topic partition.
type Model struct {
	cfg  Config
	subs []*sub

	// log is the shared partition log of a shared architecture.
	log   []slot
	bytes int64

	seq   uint64
	steps int

	// reserved counts the reservations the model has ever taken, released
	// the ones it gave back and enqueued the ones that became records. The
	// ledger invariant is reserved == enqueued + released.
	reserved, releasedRes, enqueued int64

	// starved is set when a publication that every open subscription had
	// room for did not reach all of them, which no policy may do.
	starved bool
}

// New returns a model with cfg.Subs subscriptions.
func New(cfg Config) *Model {
	if cfg.Subs < 0 {
		cfg.Subs = 0
	}
	m := &Model{cfg: cfg}
	for range cfg.Subs {
		m.subs = append(m.subs, &sub{})
	}
	return m
}

// Subs returns the number of subscriptions, closed ones included.
func (m *Model) Subs() int { return len(m.subs) }

// Steps returns the number of operations applied.
func (m *Model) Steps() int { return m.steps }

// Advance moves the model's clock, and is a deliberate no-op: this model is
// timeless. What it decides — which subscriptions a publication reaches, what
// the publisher is told and how the reservations are released again — no
// clock enters into; delays, leases and time to live are the business of
// internal/model/memq, which does carry one. The method is here so that the
// model has the shape every reference model of the specification D has and a
// harness driving several of them at once needs no special case
// (adr/ADR-017-assumptions.md).
func (m *Model) Advance(time.Duration) {}

// Depth returns the number of records subscription i holds.
func (m *Model) Depth(i int) int {
	if i < 0 || i >= len(m.subs) {
		return 0
	}
	return m.subs[i].depth()
}

// Counters returns the per-subscription counters the engine's Snapshot
// carries: records admitted, records dropped without settlement, refused
// publications and terminal settlements.
func (m *Model) Counters(i int) (published, dropped, rejected, settled int64) {
	if i < 0 || i >= len(m.subs) {
		return 0, 0, 0, 0
	}
	s := m.subs[i]
	return s.published, s.dropped, s.rejected, s.settled
}

// Held returns the sequence numbers subscription i still holds, in admission
// order.
func (m *Model) Held(i int) []uint64 {
	if i < 0 || i >= len(m.subs) {
		return nil
	}
	out := make([]uint64, 0, len(m.subs[i].recs))
	for _, r := range m.subs[i].recs {
		if r.state != gone {
			out = append(out, r.seq)
		}
	}
	return out
}

// Closed reports whether subscription i has been closed.
func (m *Model) Closed(i int) bool { return i >= 0 && i < len(m.subs) && m.subs[i].closed }

// Step applies op and returns the result the engine must produce. An
// undefined op.Kind is a harness error, reported in Result.Err.
func (m *Model) Step(op Op) Result {
	m.steps++
	switch op.Kind {
	case Publish:
		return m.publish(op)
	case Claim:
		return m.claim(op)
	case Settle:
		return m.settle(op)
	case Close:
		return m.close(op)
	case Subscribe:
		m.subs = append(m.subs, &sub{})
		return Result{}
	default:
		return Result{Err: errors.New("fanout: undefined operation " + op.Kind.String())}
	}
}

// live returns the indices of the subscriptions a publication reaches.
func (m *Model) live() []int {
	out := make([]int, 0, len(m.subs))
	for i, s := range m.subs {
		if !s.closed {
			out = append(out, i)
		}
	}
	return out
}

// room reports whether subscription i can take one more record of size bytes.
func (m *Model) room(i int, size int64) bool {
	s := m.subs[i]
	if m.cfg.Capacity > 0 && s.depth() >= m.cfg.Capacity {
		return false
	}
	if m.cfg.CapacityBytes > 0 && s.bytes+size > m.cfg.CapacityBytes {
		return false
	}
	return true
}

// dropOldest discards the oldest record subscription i holds that no consumer
// has claimed, which is what the DropSlow policy does to make room.
func (m *Model) dropOldest(i int) bool {
	s := m.subs[i]
	for j := range s.recs {
		if s.recs[j].state != held {
			continue
		}
		s.recs[j].state = gone
		s.bytes -= s.recs[j].size
		s.dropped++
		m.enqueued--
		m.releasedRes++
		m.logSettle(s.recs[j].seq)
		return true
	}
	return false
}

// publish applies the topic's admission policy.
func (m *Model) publish(op Op) Result {
	live := m.live()
	if len(live) == 0 {
		return Result{}
	}
	if m.cfg.Shared {
		return m.publishShared(live, op)
	}
	res := Result{}
	admitted := make([]int, 0, len(live))
	refused := 0
	roomForAll := true
	for _, i := range live {
		m.subs[i].offered++
		if !m.room(i, op.Size) {
			roomForAll = false
		}
	}
	for _, i := range live {
		m.reserved++
		if m.room(i, op.Size) {
			admitted = append(admitted, i)
			continue
		}
		if m.cfg.Policy == DropSlow && m.dropOldest(i) && m.room(i, op.Size) {
			res.Dropped = append(res.Dropped, i)
			admitted = append(admitted, i)
			continue
		}
		m.releasedRes++
		refused++
		if m.cfg.Policy == Atomic {
			break
		}
		m.subs[i].rejected++
	}
	if m.cfg.Policy == Atomic && refused > 0 {
		for range admitted {
			m.releasedRes++
		}
		for _, i := range live {
			m.subs[i].rejected++
		}
		if roomForAll {
			m.starved = true
		}
		return Result{Err: errcode.ErrQueueFull}
	}
	if len(admitted) == 0 {
		if roomForAll {
			m.starved = true
		}
		return Result{Err: errcode.ErrQueueFull}
	}
	m.seq++
	for _, i := range admitted {
		m.admit(i, m.seq, op.Size)
	}
	res.Reached = admitted
	res.Seq = m.seq
	if refused > 0 && m.cfg.Policy == Independent {
		// A partial admission is not a refusal: the record was admitted
		// somewhere, so it must not look like a publication the caller may
		// retry or whose payload it still owns.
		res.Err = errcode.ErrPartial
	}
	if roomForAll && len(admitted) != len(live) {
		m.starved = true
	}
	return res
}

// publishShared admits a publication to the shared partition log, which
// decides for every subscription at once.
func (m *Model) publishShared(live []int, op Op) Result {
	// One reservation per subscription that will hold the record, as in the
	// per-subscription case: the ledger counts what the subscriptions hold,
	// and the single slot is a storage detail.
	m.reserved += int64(len(live))
	room := m.logRoom(op.Size)
	for _, i := range live {
		m.subs[i].offered++
	}
	if !room {
		if m.cfg.Policy != DropSlow || !m.dropSlowest(live) || !m.logRoom(op.Size) {
			m.releasedRes += int64(len(live))
			for _, i := range live {
				m.subs[i].rejected++
			}
			return Result{Err: errcode.ErrQueueFull}
		}
	}
	m.seq++
	m.log = append(m.log, slot{seq: m.seq, size: op.Size, refs: len(live)})
	m.bytes += op.Size
	for _, i := range live {
		m.admit(i, m.seq, op.Size)
	}
	return Result{Reached: live, Seq: m.seq}
}

// logRoom reports whether the shared log can take one more record.
func (m *Model) logRoom(size int64) bool {
	if m.cfg.Capacity > 0 && len(m.log) >= m.cfg.Capacity {
		return false
	}
	if m.cfg.CapacityBytes > 0 && m.bytes+size > m.cfg.CapacityBytes {
		return false
	}
	return true
}

// dropSlowest drops the oldest unclaimed record of every subscription that is
// holding the shared log back.
func (m *Model) dropSlowest(live []int) bool {
	freed := false
	for _, i := range live {
		if m.dropOldest(i) {
			freed = true
		}
	}
	return freed
}

// admit gives subscription i one record.
func (m *Model) admit(i int, seq uint64, size int64) {
	s := m.subs[i]
	s.recs = append(s.recs, rec{seq: seq, size: size})
	s.bytes += size
	s.published++
	m.enqueued++
}

// logSettle drops one subscription's reference to a slot and reclaims the
// slots at the tail that no subscription holds any more.
func (m *Model) logSettle(seq uint64) {
	if !m.cfg.Shared {
		return
	}
	for i := range m.log {
		if m.log[i].seq == seq {
			if m.log[i].refs > 0 {
				m.log[i].refs--
			}
			break
		}
	}
	n := 0
	for n < len(m.log) && m.log[n].refs == 0 {
		m.bytes -= m.log[n].size
		n++
	}
	m.log = append(m.log[:0], m.log[n:]...)
}

// claim takes the oldest unclaimed record of a subscription.
func (m *Model) claim(op Op) Result {
	if op.Sub < 0 || op.Sub >= len(m.subs) {
		return Result{}
	}
	s := m.subs[op.Sub]
	if s.closed {
		return Result{Err: errcode.ErrClosed}
	}
	for j := range s.recs {
		if s.recs[j].state == held {
			s.recs[j].state = claimed
			return Result{Seq: s.recs[j].seq}
		}
	}
	return Result{}
}

// settle finishes a record a subscription holds.
func (m *Model) settle(op Op) Result {
	if op.Sub < 0 || op.Sub >= len(m.subs) {
		return Result{}
	}
	s := m.subs[op.Sub]
	for j := range s.recs {
		if s.recs[j].seq != op.Seq || s.recs[j].state == gone {
			continue
		}
		s.recs[j].state = gone
		s.bytes -= s.recs[j].size
		s.settled++
		m.enqueued--
		m.releasedRes++
		m.logSettle(op.Seq)
		return Result{Seq: op.Seq}
	}
	return Result{Err: errcode.ErrAlreadySettled}
}

// close detaches a subscription and drops everything it still holds.
func (m *Model) close(op Op) Result {
	if op.Sub < 0 || op.Sub >= len(m.subs) || m.subs[op.Sub].closed {
		return Result{}
	}
	s := m.subs[op.Sub]
	s.closed = true
	for j := range s.recs {
		if s.recs[j].state == gone {
			continue
		}
		s.recs[j].state = gone
		s.bytes -= s.recs[j].size
		s.dropped++
		m.enqueued--
		m.releasedRes++
		m.logSettle(s.recs[j].seq)
	}
	return Result{}
}
