// Package offset tracks the in-flight record offsets of one partition and
// computes the offset that is safe to commit, for backends whose
// acknowledgement model is cumulative (Kafka: Capabilities.Ack.Model =
// AckCumulative, the message contract).
//
// The contract is the one the Kafka driver needs: offsets of a partition are
// delivered in strictly increasing order and completed in any order; the
// watermark is the offset after the highest record whose completion can no
// longer lose work — every delivered offset below it has completed. Offsets
// the broker never delivered (compacted records, transaction markers) do not
// block the watermark. A failed or unresolved offset blocks every later
// commit, which is why the tracker also carries a bounded window and a pause
// signal: when too many offsets are in flight, or the window between the
// watermark and the newest record grows too wide, Paused reports true and the
// driver pauses the partition instead of buffering without bound.
//
// The package holds every variant
// behind one interface so the bake-off can measure them against each other:
// MapScan (the reference), Bitset, Interval, HeapMap, Tree and Segmented.
// Every kind implements exactly the same contract; internal/model/commit is
// the executable reference model they are fuzzed against, and
// models/tla/KafkaCommit.tla is the machine-checked specification.
//
// A Tracker is not safe for concurrent use: one partition is owned by one
// fetch loop, which serialises Track, Complete and Reset. Hot path: Track and
// Complete allocate nothing in steady state.
package offset

import (
	"math/bits"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Kind identifies a tracker variant. Auto resolves to AutoKind.
type Kind uint8

// Kinds. MapScan is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// MapScan keeps a map from offset to completion state and scans upward
	// from the watermark: the reference implementation.
	MapScan
	// Bitset keeps two bits per offset (delivered, completed) in a circular
	// window of machine words sized to Config.Window.
	Bitset
	// Interval keeps the merged intervals of resolved offsets (completed or
	// never delivered) above the watermark.
	Interval
	// HeapMap keeps the in-flight offsets in a binary min-heap with a map
	// from offset to heap position, so the watermark is the heap root.
	HeapMap
	// Tree keeps the in-flight offsets in an ordered tree (a treap over
	// array-backed nodes), so the watermark is the leftmost node.
	Tree
	// Segmented keeps a bitmap per fixed-size segment of offsets, allocated
	// on demand, so a wide window costs memory only where records are in
	// flight.
	Segmented

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "mapscan", "bitset", "interval", "heapmap", "tree", "segmented"}

var kindIdents = [kindCount]string{"Auto", "MapScan", "Bitset", "Interval", "HeapMap", "Tree", "Segmented"}

// String returns the kind's slug as used in benchmark names and ADRs.
func (k Kind) String() string {
	if k < kindCount {
		return kindSlugs[k]
	}
	return "kind(?)"
}

// Ident returns the kind's Go identifier.
func (k Kind) Ident() string {
	if k < kindCount {
		return kindIdents[k]
	}
	return ""
}

// Valid reports whether k names a concrete kind or Auto.
func (k Kind) Valid() bool { return k < kindCount }

// Resolve maps Auto to AutoKind and returns every other kind unchanged.
func (k Kind) Resolve() Kind {
	if k == Auto {
		return AutoKind
	}
	return k
}

// ParseKind returns the kind with the given slug.
func ParseKind(slug string) (Kind, bool) {
	for i, s := range kindSlugs {
		if s == slug {
			return Kind(i), true
		}
	}
	return 0, false
}

// Kinds lists every concrete kind in declaration order (Auto excluded).
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount-1)
	for k := MapScan; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Errors reported by Track. They carry portable codes, so a driver maps them
// with no translation; errors.Is matches by code.
var (
	// ErrStaleGeneration reports a call whose generation a rebalance has
	// superseded. It is errcode.ErrLeaseLost, the code the settlement path
	// returns for the same condition.
	ErrStaleGeneration = errcode.ErrLeaseLost
	// ErrOutOfOrder reports an offset at or below one already delivered.
	ErrOutOfOrder = errcode.New(errcode.InvalidArgument, "iqmq: offset out of order")
	// ErrWindowFull reports an offset beyond the tracking window: too many
	// offsets below it are still in flight for the tracker to record it. The
	// driver must pause the partition (see Tracker.Paused) before this
	// happens.
	ErrWindowFull = errcode.New(errcode.ResourceExhausted, "iqmq: offset window full")
	// ErrInvalidConfig reports a Config that Validate rejects.
	ErrInvalidConfig = errcode.New(errcode.InvalidConfig, "iqmq: invalid offset tracker configuration")
)

// DefaultWindow is the tracking window Config.Window selects when zero: 65536
// offsets, which is 16 KiB of bitset for the Bitset kind.
const DefaultWindow = 1 << 16

// Config bounds one partition's tracking window. The zero value is usable and
// selects DefaultWindow with the pause thresholds at the window bound.
type Config struct {
	// Base is the first offset of the assignment: the initial watermark, and
	// the offset the driver resumes fetching from.
	Base int64
	// Window is the largest span between the watermark and the offset after
	// the newest delivered record. Track refuses an offset that would exceed
	// it. Zero selects DefaultWindow.
	Window int
	// MaxPending is the number of in-flight offsets at which Paused reports
	// true. Zero selects Window.
	MaxPending int
	// MaxGap is the window span at which Paused reports true. Zero selects
	// Window.
	MaxGap int
}

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

// Validate reports whether the configuration is usable. It is pure: it
// performs no I/O and reads no clock.
func (c Config) Validate() error {
	if c.Base < 0 {
		return ErrInvalidConfig
	}
	if c.Window < 0 || c.MaxPending < 0 || c.MaxGap < 0 {
		return ErrInvalidConfig
	}
	n := c.Normalize()
	if n.MaxPending > n.Window || n.MaxGap > n.Window {
		return ErrInvalidConfig
	}
	return nil
}

// Tracker records the offsets of one partition that are in flight and reports
// the offset that is safe to commit.
//
// Implementations are not safe for concurrent use. Every method is a hot
// path: none allocates in steady state.
type Tracker interface {
	// Track records that the record at off was delivered to the application
	// under assignment generation gen. Offsets must increase strictly:
	// Track returns ErrOutOfOrder for an offset at or below one already
	// delivered, ErrStaleGeneration when gen is not the current generation,
	// and ErrWindowFull when off would push the span between the oldest
	// incomplete record and the newest beyond Config.Window. A successful
	// Track leaves the offset in flight; when nothing was in flight it also
	// moves the watermark to off, since every offset below it is resolved.
	Track(gen uint32, off int64) error
	// Complete records that the record at off finished processing under
	// generation gen, and returns the resulting watermark and whether it
	// advanced. A completion whose generation is stale, whose offset was
	// never delivered, or which repeats an earlier completion changes
	// nothing and reports the current watermark with advanced false.
	Complete(gen uint32, off int64) (watermark int64, advanced bool)
	// Watermark returns the offset that is safe to commit: every delivered
	// offset below it has completed. It is the offset to send to the broker,
	// which is "the offset of the next record to consume".
	Watermark() int64
	// Pending returns the number of delivered, incomplete offsets.
	Pending() int
	// Paused reports whether the partition should be paused: the in-flight
	// count reached Config.MaxPending, or the window between the watermark
	// and the newest delivered record reached Config.MaxGap.
	Paused() bool
	// Reset re-bases the tracker at next under a fresh generation, dropping
	// every in-flight offset. It is what a rebalance does: after Reset every
	// handle from the previous generation is stale. It returns the new
	// generation, which is never zero.
	Reset(next int64) uint32
	// Gen returns the current assignment generation.
	Gen() uint32
	// Kind returns the variant this tracker implements.
	Kind() Kind
}

// New returns a tracker of kind k configured by cfg. It panics on an invalid
// kind or configuration; both are programming errors, not runtime conditions.
func New(k Kind, cfg Config) Tracker {
	k = k.Resolve()
	if !k.Valid() || k == Auto {
		panic("offset: invalid kind")
	}
	if err := cfg.Validate(); err != nil {
		panic("offset: invalid configuration")
	}
	cfg = cfg.Normalize()
	switch k {
	case MapScan:
		return newMapScan(cfg)
	case Bitset:
		return newBitset(cfg)
	case Interval:
		return newInterval(cfg)
	case HeapMap:
		return newHeapMap(cfg)
	case Tree:
		return newTree(cfg)
	case Segmented:
		return newSegmented(cfg)
	default:
		panic("offset: unreachable")
	}
}

// common holds the state every kind keeps: the configuration, the watermark,
// the offset after the newest delivered record, the generation and the
// in-flight count. Each kind adds the structure that records which offsets
// are delivered and which of them completed.
type common struct {
	cfg  Config
	base int64 // watermark
	high int64 // one past the newest delivered offset
	gen  uint32
	pend int
	kind Kind
}

func newCommon(k Kind, cfg Config) common {
	return common{cfg: cfg, base: cfg.Base, high: cfg.Base, gen: 1, kind: k}
}

// Watermark implements Tracker.
func (c *common) Watermark() int64 { return c.base }

// Pending implements Tracker.
func (c *common) Pending() int { return c.pend }

// Gen implements Tracker.
func (c *common) Gen() uint32 { return c.gen }

// Kind implements Tracker.
func (c *common) Kind() Kind { return c.kind }

// Paused implements Tracker.
func (c *common) Paused() bool {
	return c.pend >= c.cfg.MaxPending || c.high-c.base >= int64(c.cfg.MaxGap)
}

// checkTrack applies the rules shared by every kind. It returns nil when the
// offset may be recorded.
func (c *common) checkTrack(gen uint32, off int64) error {
	switch {
	case gen != c.gen:
		return ErrStaleGeneration
	case off < c.high:
		return ErrOutOfOrder
	case c.pend > 0 && off-c.base >= int64(c.cfg.Window):
		// The window bounds the span between the oldest incomplete record and
		// the newest one. With nothing in flight the watermark jumps to the
		// new record, so any forward offset is trackable however far it is.
		return ErrWindowFull
	default:
		return nil
	}
}

// rebase resets the shared state and returns the new generation.
func (c *common) rebase(next int64) uint32 {
	if next < 0 {
		next = 0
	}
	c.base, c.high, c.pend = next, next, 0
	c.gen = nextGen(c.gen)
	return c.gen
}

// nextGen increments a generation, skipping zero so that a zero-valued handle
// can never match a live assignment (the same rule as internal/settlement).
func nextGen(g uint32) uint32 {
	if g == ^uint32(0) {
		return 1
	}
	return g + 1
}

// pow2ceil returns the smallest power of two ≥ n (n ≥ 1).
func pow2ceil(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}
