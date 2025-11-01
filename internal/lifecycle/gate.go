package lifecycle

import (
	"math/rand/v2"
	"runtime"
	"sync/atomic"
)

// maxShards caps the gate's counter cells. A gate is summed only while a
// shutdown waits for it, so more cells buy nothing past the point where
// concurrent entries stop colliding.
const maxShards = 64

// cacheLine is the assumed cache-line size used to separate hot cells.
const cacheLine = 64

// spinRounds is how many yields wait makes before it parks on the channel.
const spinRounds = 64

// gate counts the operations of one Op class that are inside their placement
// and refuses new ones once it is barred.
//
// # Why it is not a mutex
//
// The obvious implementation is an RWMutex held for reading across the
// placement and for writing by the phase transition. That is correct, but the
// read lock is one contended cache line on the engine's publish path. The
// counter is striped instead: an operation touches one cell, and only the
// shutdown — which happens once — sums them.
//
// # Why the count is taken before the flag is read
//
// Reading the flag first and then counting is the very bug this package
// exists to prevent: the read can see an open gate, the phase can bar and
// drain, and the count can arrive afterwards. acquire therefore counts first
// and reads the flag second, giving the count back when the flag is set.
//
// That ordering is what makes wait's answer trustworthy. If acquire's read of
// barred returned false then, in the sequentially consistent order Go's
// atomics have, that read precedes bar's store; acquire's increment precedes
// its own read; so the increment precedes the store, which precedes every
// pass wait makes over the cells. A pass that sums to zero cannot have missed
// an operation acquire admitted: the cell it was counted on is read after the
// increment and holds it until release. An operation acquire refuses may
// leave a transient count behind, but it places nothing, so a pass that
// misses it misses nothing.
//
// # Why the ticket names the cell
//
// A goroutine may be rescheduled onto another processor between acquire and
// release. If release picked a cell of its own, one cell could go negative
// while another stayed positive and the sum could read zero with an operation
// still running. The ticket carries the cell acquire chose and release gives
// the count back to that same cell, so every cell's value is the number of
// operations counted on it.
type gate struct {
	barred atomic.Bool
	// waking is set while wait is parked, so that a release knows to signal
	// rather than return silently. Off the shutdown path it is one load on
	// a line nothing writes.
	waking atomic.Bool
	_      [cacheLine - 2]byte

	mask  uint32
	cells []gcell
	wake  chan struct{}
}

// gcell is one counter on its own cache line.
type gcell struct {
	n atomic.Int64
	_ [cacheLine - 8]byte
}

// init sizes the gate for shards cells, rounded up to a power of two.
func (g *gate) init(shards int) {
	n := 1
	for n < shards {
		n <<= 1
	}
	g.mask = uint32(n - 1)
	g.cells = make([]gcell, n)
	g.wake = make(chan struct{}, 1)
}

// acquire counts one operation in and returns the cell it was counted on.
func (g *gate) acquire() uint32 {
	// Stripe selection needs speed and spread, not unpredictability;
	// math/rand/v2's top-level generator is per-P and lock-free.
	c := uint32(rand.Uint64()) & g.mask //nolint:gosec // load balancing over cache lines
	g.cells[c].n.Add(1)
	return c
}

// release gives one operation's count back to the cell it took.
func (g *gate) release(c uint32) {
	if g.cells[c].n.Add(-1) == 0 && g.waking.Load() {
		g.signal()
	}
}

// signal nudges a waiting shutdown without ever blocking a release.
func (g *gate) signal() {
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

// sum totals the cells. A cell holds one count per operation counted on it
// for as long as that operation runs, so a total of zero means every
// operation admitted before the bar has left.
func (g *gate) sum() int64 {
	var n int64
	for i := range g.cells {
		n += g.cells[i].n.Load()
	}
	return n
}

// bar refuses every further entry.
func (g *gate) bar() { g.barred.Store(true) }

// wait blocks until every operation admitted before the bar has left. It must
// be called after bar, or it may never return.
//
// It never gives up on the wait: abandoning a barrier would let the placement
// it is waiting for land behind the drain, which is the one fault this
// package exists to prevent. What bound buys is a *report* — cut is called
// once, if bound is closed before the gate empties — so that a graceful
// Shutdown can return to its caller on its own deadline while the run itself
// still waits for the placement and still drains it. A nil bound never
// reports.
//
// It spins first, because an admitted operation is a bounded placement and
// almost always finishes within a few yields, and only then parks.
func (g *gate) wait(bound <-chan struct{}, cut func()) {
	for range spinRounds {
		if g.sum() <= 0 {
			return
		}
		if cut != nil && stopped(bound) {
			cut()
			cut = nil
		}
		runtime.Gosched()
	}
	g.waking.Store(true)
	defer g.waking.Store(false)
	for {
		// Drain a signal left by a release that finished before waking was
		// set, so the park below cannot consume a stale wake and miss the
		// real one.
		select {
		case <-g.wake:
		default:
		}
		if g.sum() <= 0 {
			return
		}
		select {
		case <-g.wake:
		case <-bound:
			// Reported once: the bound stays closed, so the callback and
			// the channel are dropped rather than spun on.
			if cut != nil {
				cut()
				cut = nil
			}
			bound = nil
		}
	}
}

// stopped reports whether bound is closed. A nil bound is never stopped,
// which is what makes it the unreported wait.
func stopped(bound <-chan struct{}) bool {
	select {
	case <-bound:
		return true
	default:
		return false
	}
}
