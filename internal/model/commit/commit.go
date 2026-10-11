// Package commit is the executable reference model of cumulative-offset
// acknowledgement (the SPI kafka, the specification D). It
// mirrors models/tla/KafkaCommit.tla in Go: a pure, single-goroutine state
// machine over one partition, driven by Step, with Invariants checked after
// every step and a Snapshot for traces. driverkit/offset holds the production
// trackers and is fuzzed differentially against this model; the model imports
// nothing from them.
//
// The model keeps the offsets delivered to the application since the last
// assignment, the subset that completed, the watermark (the offset that is
// safe to commit: every delivered offset below it has completed), the
// assignment generation, the last watermark published to the broker, and the
// handles whose completion was rejected.
package commit

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Kind identifies an operation.
type Kind uint8

// The operations, mirroring the TLA+ actions and the Tracker methods.
const (
	// Track delivers an offset to the application (TLA Deliver).
	Track Kind = iota
	// Complete reports an offset as processed (TLA Complete).
	Complete
	// Commit publishes the watermark to the broker (TLA Commit).
	Commit
	// Reset re-assigns the partition at an offset under a fresh generation
	// (TLA Revoke followed by an assignment).
	Reset

	kindCount
)

var kindNames = [kindCount]string{"track", "complete", "commit", "reset"}

// String returns the lower-case operation name.
func (k Kind) String() string {
	if k < kindCount {
		return kindNames[k]
	}
	return "kind(?)"
}

// Op is one operation applied by Step.
type Op struct {
	// Kind selects the operation.
	Kind Kind
	// Gen is the generation the caller believes it holds. Reset ignores it.
	Gen uint32
	// Off is the offset for Track, and for Complete the offset the handle
	// carries. Reset uses it as the new base offset.
	Off int64
}

// Result is what Step returns; it is what a Tracker must return.
type Result struct {
	// Err is the error Track returns: nil, ErrStaleGeneration,
	// ErrOutOfOrder or ErrWindowFull. It is nil for every other kind.
	Err error
	// Watermark is the offset that is safe to commit after the operation.
	Watermark int64
	// Advanced reports that Complete moved the watermark.
	Advanced bool
	// Gen is the generation after the operation.
	Gen uint32
	// Pending is the number of delivered, incomplete offsets.
	Pending int
	// Paused reports that the caller should pause the partition: the
	// in-flight count or the tracking window reached its bound.
	Paused bool
}

// Errors returned by Track. They carry portable codes so a driver can map
// them without translation; errors.Is matches by code.
var (
	// ErrStaleGeneration reports a call carrying a generation that a
	// rebalance has superseded.
	ErrStaleGeneration = errcode.ErrLeaseLost
	// ErrOutOfOrder reports an offset at or below one already delivered.
	// Offsets of one partition are delivered in strictly increasing order.
	ErrOutOfOrder = errcode.New(errcode.InvalidArgument, "iqmq: offset out of order")
	// ErrWindowFull reports an offset beyond the tracking window: too many
	// offsets are outstanding below it for the tracker to record it.
	ErrWindowFull = errcode.New(errcode.ResourceExhausted, "iqmq: offset window full")
)

// ErrNoSuchOffset is returned by Delivered for an offset outside the window.
var ErrNoSuchOffset = errors.New("commit: no such offset")

// Config bounds one partition's tracking window.
type Config struct {
	// Base is the first offset of the assignment: the initial watermark.
	Base int64
	// Window is the largest span between the watermark and the offset after
	// the highest delivered one. Zero selects DefaultWindow.
	Window int
	// MaxPending is the number of in-flight offsets at which Paused reports
	// true. Zero selects Window.
	MaxPending int
	// MaxGap is the window span at which Paused reports true. Zero selects
	// Window.
	MaxGap int
}

// DefaultWindow is the tracking window Config.Window selects when zero.
const DefaultWindow = 1 << 16

// Normalize fills in the zero values and returns the effective configuration.
func (c Config) Normalize() Config {
	if c.Window <= 0 {
		c.Window = DefaultWindow
	}
	if c.MaxPending <= 0 {
		c.MaxPending = c.Window
	}
	if c.MaxGap <= 0 {
		c.MaxGap = c.Window
	}
	return c
}

// Validate reports whether the configuration is usable.
func (c Config) Validate() error {
	if c.Base < 0 {
		return errcode.New(errcode.InvalidConfig, "offset: Base must not be negative")
	}
	if c.Window < 0 || c.MaxPending < 0 || c.MaxGap < 0 {
		return errcode.New(errcode.InvalidConfig, "offset: Window, MaxPending and MaxGap must not be negative")
	}
	n := c.Normalize()
	if n.MaxPending > n.Window || n.MaxGap > n.Window {
		return errcode.New(errcode.InvalidConfig, "offset: MaxPending and MaxGap must not exceed Window")
	}
	return nil
}

// handle is a completion attempt: the offset and the generation presented.
type handle struct {
	Gen uint32
	Off int64
}

// Model is the reference model of one partition's offset tracker.
type Model struct {
	cfg Config
	// delivered maps a delivered offset to whether it completed. Offsets
	// below the watermark are dropped: they are settled history.
	delivered map[int64]bool
	base      int64 // watermark: the offset that is safe to commit
	high      int64 // the offset after the highest delivered one
	gen       uint32
	pending   int

	committed    int64  // last watermark published to the broker; -1 = none
	committedGen uint32 // the generation that published it

	rejected []handle // completion attempts the model refused
	stale    []handle // stale attempts, which must leave the state unchanged

	// lastErr and lastAdvanced carry the outcome of the operation Step
	// applied. They are fields rather than return values so that the
	// per-operation methods below read like the TLA+ actions they mirror.
	lastErr      error
	lastAdvanced bool

	prev  snapshotCore
	steps int
	now   time.Duration
}

// snapshotCore is the part of the state the step-relative invariants compare.
type snapshotCore struct {
	base, high, committed int64
	gen                   uint32
	pending               int
	delivered             map[int64]bool
}

// New returns a model of one partition configured by cfg. It panics when the
// configuration is invalid, which is a harness error, not a behaviour of the
// system.
func New(cfg Config) *Model {
	if err := cfg.Validate(); err != nil {
		panic("commit: " + err.Error())
	}
	cfg = cfg.Normalize()
	m := &Model{
		cfg:       cfg,
		delivered: make(map[int64]bool, 64),
		base:      cfg.Base,
		high:      cfg.Base,
		gen:       1,
		committed: -1,
	}
	m.snap()
	return m
}

// Gen returns the current assignment generation.
func (m *Model) Gen() uint32 { return m.gen }

// Watermark returns the offset that is safe to commit.
func (m *Model) Watermark() int64 { return m.base }

// Pending returns the number of delivered, incomplete offsets.
func (m *Model) Pending() int { return m.pending }

// Paused reports the pause signal.
func (m *Model) Paused() bool {
	return m.pending >= m.cfg.MaxPending || m.high-m.base >= int64(m.cfg.MaxGap)
}

// Steps returns the number of operations applied.
func (m *Model) Steps() int { return m.steps }

// Advance moves the model's logical clock. Offset tracking is time-free (a
// rebalance arrives as Reset, driven by the caller), so Advance changes no
// offset; it exists so every reference model shares one surface.
func (m *Model) Advance(dt time.Duration) {
	if dt > 0 {
		m.now += dt
	}
}

// Now returns the logical clock.
func (m *Model) Now() time.Duration { return m.now }

// Delivered reports whether off was delivered and whether it completed.
func (m *Model) Delivered(off int64) (completed bool, err error) {
	done, ok := m.delivered[off]
	if !ok {
		return false, ErrNoSuchOffset
	}
	return done, nil
}

// nextGen increments a generation, skipping zero so that a zero-valued handle
// can never match a live assignment.
func nextGen(g uint32) uint32 {
	if g == math.MaxUint32 {
		return 1
	}
	return g + 1
}

func (m *Model) snap() {
	d := make(map[int64]bool, len(m.delivered))
	for k, v := range m.delivered {
		d[k] = v
	}
	m.prev = snapshotCore{base: m.base, high: m.high, committed: m.committed, gen: m.gen, pending: m.pending, delivered: d}
}

// Step applies op and returns the result a Tracker must produce. It panics
// when op.Kind is undefined.
func (m *Model) Step(op Op) Result {
	m.snap()
	m.steps++
	switch op.Kind {
	case Track:
		m.track(op)
	case Complete:
		m.complete(op)
	case Commit:
		m.commit(op)
	case Reset:
		m.reset(op)
	default:
		panic("commit: undefined operation kind")
	}
	return Result{
		Err:       m.lastErr,
		Watermark: m.base,
		Advanced:  m.lastAdvanced,
		Gen:       m.gen,
		Pending:   m.pending,
		Paused:    m.Paused(),
	}
}

func (m *Model) track(op Op) {
	m.lastErr, m.lastAdvanced = nil, false
	switch {
	case op.Gen != m.gen:
		m.lastErr = ErrStaleGeneration
		m.stale = append(m.stale, handle{Gen: op.Gen, Off: op.Off})
	case op.Off < m.high:
		m.lastErr = ErrOutOfOrder
	case m.pending > 0 && op.Off-m.base >= int64(m.cfg.Window):
		m.lastErr = ErrWindowFull
	default:
		if m.pending == 0 {
			// Nothing is in flight, so every offset below this record is
			// resolved: the watermark jumps to the new record, skipping
			// offsets the broker never delivered.
			m.base = op.Off
		}
		m.delivered[op.Off] = false
		m.high = op.Off + 1
		m.pending++
	}
}

func (m *Model) complete(op Op) {
	m.lastErr, m.lastAdvanced = nil, false
	if op.Gen != m.gen {
		m.stale = append(m.stale, handle{Gen: op.Gen, Off: op.Off})
		return
	}
	done, ok := m.delivered[op.Off]
	if !ok || done {
		// Never delivered, already completed, or below the watermark: the
		// completion is refused and changes nothing.
		m.rejected = append(m.rejected, handle{Gen: op.Gen, Off: op.Off})
		return
	}
	m.delivered[op.Off] = true
	m.pending--
	before := m.base
	for m.base < m.high {
		d, ok := m.delivered[m.base]
		if ok && !d {
			break
		}
		if ok {
			delete(m.delivered, m.base)
		}
		m.base++
	}
	m.lastAdvanced = m.base > before
}

func (m *Model) commit(op Op) {
	m.lastErr, m.lastAdvanced = nil, false
	if op.Gen != m.gen {
		m.stale = append(m.stale, handle{Gen: op.Gen, Off: op.Off})
		return
	}
	m.committed, m.committedGen = m.base, m.gen
}

func (m *Model) reset(op Op) {
	m.lastErr, m.lastAdvanced = nil, false
	clear(m.delivered)
	base := op.Off
	if base < 0 {
		base = 0
	}
	m.base, m.high, m.pending = base, base, 0
	m.gen = nextGen(m.gen)
}

// Snapshot is a copy of the model state.
type Snapshot struct {
	Config    Config
	Delivered map[int64]bool
	Watermark int64
	High      int64
	Gen       uint32
	Pending   int
	Committed int64
	Steps     int
	Now       time.Duration
}

// Snapshot returns a deep copy of the model state.
func (m *Model) Snapshot() Snapshot {
	d := make(map[int64]bool, len(m.delivered))
	for k, v := range m.delivered {
		d[k] = v
	}
	return Snapshot{
		Config:    m.cfg,
		Delivered: d,
		Watermark: m.base,
		High:      m.high,
		Gen:       m.gen,
		Pending:   m.pending,
		Committed: m.committed,
		Steps:     m.steps,
		Now:       m.now,
	}
}

// Invariants checks the model after a step and returns the first violation.
// Each check carries the name of the TLA+ invariant or property it mirrors
// (models/tla/KafkaCommit.cfg; tools/modelcheck cross-references the names):
//
//   - TypeOK: the watermark, the delivered set and the window are well
//     formed: base ≤ high, every delivered offset lies below high, every
//     completed one below the watermark has been dropped as history,
//     the pending count equals the number of incomplete delivered offsets,
//     and the generation is never zero. An *incomplete* offset below the
//     watermark is SafeWatermark's fault to report, not this one's.
//   - SafeWatermark: the watermark never passes an incomplete offset — every
//     delivered, incomplete offset is at or above it, so committing the
//     watermark can never lose unfinished work.
//   - WatermarkProgress: when nothing is in flight the watermark has reached
//     the offset after the highest delivered one; the watermark is not merely
//     safe but as far advanced as it may be, and a gap of never-delivered
//     offsets does not stall it.
//   - BoundedWindow: the tracking window is bounded by the configuration, so
//     memory is bounded however completions are interleaved.
//   - StaleGenRejected: an operation carrying a superseded generation leaves
//     every part of the state unchanged, and no rejected completion was in
//     fact a valid one.
//   - CommitSafe: the offset published to the broker never exceeds the
//     watermark of the generation that published it.
//   - WatermarkMonotone: within one generation the watermark never goes
//     backwards, and the generation itself only ever advances.
//   - CommitMonotone: within one generation the published offset never goes
//     backwards.
func (m *Model) Invariants() error {
	// TypeOK
	if m.base > m.high {
		return fmt.Errorf("TypeOK: watermark %d above high %d", m.base, m.high)
	}
	if m.gen == 0 {
		return errors.New("TypeOK: generation zero")
	}
	incomplete := 0
	minIncomplete := int64(math.MaxInt64)
	for off, done := range m.delivered {
		if off < 0 || off >= m.high {
			return fmt.Errorf("TypeOK: delivered offset %d outside [0, %d)", off, m.high)
		}
		// A *completed* offset below the watermark is history the model
		// should have dropped, which is a well-formedness fault. An
		// *incomplete* one below it is the watermark having passed
		// unfinished work, which is SafeWatermark's, and this check used to
		// read `off < m.base` and take that case too — leaving SafeWatermark
		// unfalsifiable behind it.
		if done && off < m.base {
			return fmt.Errorf("TypeOK: completed offset %d below the watermark %d was not dropped", off, m.base)
		}
		if !done {
			incomplete++
			if off < minIncomplete {
				minIncomplete = off
			}
		}
	}
	if incomplete != m.pending {
		return fmt.Errorf("TypeOK: %d incomplete offsets but Pending reports %d", incomplete, m.pending)
	}
	// SafeWatermark
	if incomplete > 0 && minIncomplete < m.base {
		return fmt.Errorf("SafeWatermark: watermark %d passed incomplete offset %d", m.base, minIncomplete)
	}
	// WatermarkProgress
	if incomplete == 0 && m.base != m.high {
		return fmt.Errorf("WatermarkProgress: nothing in flight but watermark %d is below high %d", m.base, m.high)
	}
	if incomplete > 0 && m.base != minIncomplete {
		return fmt.Errorf("WatermarkProgress: watermark %d is not the lowest incomplete offset %d", m.base, minIncomplete)
	}
	// BoundedWindow
	if m.high-m.base > int64(m.cfg.Window) {
		return fmt.Errorf("BoundedWindow: window %d exceeds %d", m.high-m.base, m.cfg.Window)
	}
	if m.pending > m.cfg.Window {
		return fmt.Errorf("BoundedWindow: %d pending offsets exceed the window %d", m.pending, m.cfg.Window)
	}
	// StaleGenRejected
	for _, h := range m.stale {
		if h.Gen == m.gen {
			return fmt.Errorf("StaleGenRejected: handle for offset %d was recorded stale under the live generation %d", h.Off, h.Gen)
		}
	}
	for _, h := range m.rejected {
		if h.Gen == m.gen {
			if done, ok := m.prev.delivered[h.Off]; ok && !done && h.Off >= m.prev.base {
				return fmt.Errorf("StaleGenRejected: completion of live offset %d was rejected", h.Off)
			}
		}
	}
	m.rejected, m.stale = m.rejected[:0], m.stale[:0]
	// CommitSafe
	if m.committed >= 0 && m.committedGen == m.gen && m.committed > m.base {
		return fmt.Errorf("CommitSafe: committed %d above watermark %d", m.committed, m.base)
	}
	// WatermarkMonotone
	if m.gen != m.prev.gen && m.gen != nextGen(m.prev.gen) {
		return fmt.Errorf("WatermarkMonotone: generation went %d -> %d", m.prev.gen, m.gen)
	}
	if m.gen == m.prev.gen && m.base < m.prev.base {
		return fmt.Errorf("WatermarkMonotone: watermark went %d -> %d in generation %d", m.prev.base, m.base, m.gen)
	}
	// CommitMonotone
	if m.gen == m.prev.gen && m.committed < m.prev.committed {
		return fmt.Errorf("CommitMonotone: committed offset went %d -> %d in generation %d", m.prev.committed, m.committed, m.gen)
	}
	return nil
}
