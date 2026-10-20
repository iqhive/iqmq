// Package stats holds every statistics-counter layout behind one interface, so that the bake-off can measure the cost of
// a contended increment at 8, 32 and 64 goroutines and the engine can bind
// the winner to Auto.
//
// A Counter is a 64-bit signed accumulator. Add and AddAt are atomic and
// never lose an update; Load returns the sum of every update that completed
// before it started, plus possibly some that were in progress: the result
// is exact once writers are quiescent and, under non-negative deltas, lies
// between the totals completed before and started during the call.
package stats

import (
	"errors"
	"math/rand/v2"
	"sync/atomic"
)

// Kind identifies a counter layout. Auto resolves to AutoKind.
type Kind uint8

// Kinds. Atomic is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// Atomic is one atomic.Int64: the reference, contended by every writer.
	Atomic
	// Striped spreads updates over cache-line separated cells: AddAt picks
	// the worker's cell, Add a pseudo-random one; Load sums the cells.
	Striped
	// PerWorker gives every worker its own cache-line separated cell that
	// only it writes; anonymous Adds share one extra cell; Load sums all.
	PerWorker

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "atomic", "striped", "perworker"}

var kindIdents = [kindCount]string{"Auto", "Atomic", "Striped", "PerWorker"}

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
	for k := Atomic; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Cells returns how many 64-bit cells a counter of kind k built for
// workers holds; Load reads every cell.
func (k Kind) Cells(workers int) int {
	switch k.Resolve() {
	case Striped:
		return pow2ceil(max(workers, 1))
	case PerWorker:
		return pow2ceil(max(workers, 1)) + 1
	default:
		return 1
	}
}

// SharedAdd reports whether Add without a worker hint lands on one cell
// shared by every caller, so that anonymous increments contend.
func (k Kind) SharedAdd() bool { return k.Resolve() != Striped }

// Counter is an atomic 64-bit accumulator.
type Counter interface {
	// Add adds delta.
	Add(delta int64)
	// AddAt adds delta on behalf of worker, a hint in [0, workers) that lets
	// striped layouts avoid contention. Any int is accepted; hints outside
	// the range are folded onto a cell.
	AddAt(worker int, delta int64)
	// Load returns the current total (see the package comment).
	Load() int64
}

// New builds a zero counter of kind k for the given number of workers (a
// hint for cell count; values below one are treated as one), or fails on
// an invalid kind.
func New(k Kind, workers int) (Counter, error) {
	switch k.Resolve() {
	case Atomic:
		return new(atomicCounter), nil
	case Striped:
		return newStriped(workers), nil
	case PerWorker:
		return newPerWorker(workers), nil
	default:
		return nil, errors.New("stats: invalid kind")
	}
}

// cacheLine is the assumed cache-line size used to separate hot cells.
const cacheLine = 64

// cell is one atomic counter on its own cache line.
type cell struct {
	v atomic.Int64
	_ [cacheLine - 8]byte
}

// pick returns a pseudo-random cell index in [0, n) for n a power of two.
// math/rand/v2's top-level generator is per-P and lock-free, so this costs
// a few nanoseconds and never serialises callers.
func pick(mask uint64) int {
	// Stripe selection needs speed and spread, not unpredictability: this is
	// load balancing over cache lines, and every stripe holds the same
	// total regardless of which one an increment lands on.
	return int(rand.Uint64() & mask) //nolint:gosec // not a security decision; see above
}

// pow2ceil returns the smallest power of two ≥ n (n ≥ 1).
func pow2ceil(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
