package rabbitfake

import "sync"

// DeclareGate parks every `exchange.declare`, `queue.declare` and `queue.bind`
// this broker is asked for until a test lets them through one at a time.
//
// It exists because of a real flake, and the flake was in a TEST rather than in
// the driver. A reconnection against a broker that lost its topology replays N
// declarations in order; `TestCriticalReconnectRestoresTheTopology` waited for
// the first sign of life -- `len(broker.Queues()) > 0` -- and then asserted that
// the WHOLE topology was back. On a quiet host the replay finishes between two
// one-millisecond polls and the test passes; under load the driver's dial
// goroutine is descheduled between two declarations and the assertions run
// against a half-restored broker. It failed as
//
//	"orders.workers" was not restored; the broker holds queues [orders.workers.dlq]
//
// which is the first declaration landing and nothing else.
//
// A gate rather than a delay: a test that slept would be a slower version of
// the same race. Stepping the replay makes "the broker holds part of the
// topology" a state a test can stand in and assert about, so the invariant --
// nothing reports the client restored until the replay is complete -- is
// checkable rather than probable.
//
// Every method is safe for concurrent use, and [Broker.Close] releases the gate
// so that a test which forgets to cannot hang.
type DeclareGate struct {
	mu      sync.Mutex
	changed sync.Cond
	parked  int
	passed  int

	// tokens is how many declarations Step has let through that have not
	// arrived yet. It is a count rather than a wait for the declaration, so
	// that a test may release one that has not arrived yet.
	tokens   int
	released bool
}

// declareGateDepth is how many declarations may be released in advance. The
// replay of a topology this fake can hold is far smaller, and a bound is what
// keeps a runaway test from queueing tokens for ever.
const declareGateDepth = 256

// HoldDeclarations parks every declaration from now on, and returns the gate
// that lets them through.
//
// A gate replaces any gate already installed, and the replaced one is released
// so that nothing is left parked behind it.
func (b *Broker) HoldDeclarations() *DeclareGate {
	g := &DeclareGate{}
	g.changed.L = &g.mu
	b.mu.Lock()
	old, closed := b.declGate, b.closed
	b.declGate = g
	b.mu.Unlock()
	if old != nil {
		old.Release()
	}
	if closed {
		// A gate installed on a closed broker would never be released, because
		// releasing one is [Broker.Close]'s job and Close has already run. It
		// is handed over already open rather than refused: a test that reaches
		// here has a use-after-close of its own to fix, and hanging is the
		// least useful way to tell it so.
		g.Release()
	}
	return g
}

// Parked is how many declarations are waiting at the gate right now.
func (g *DeclareGate) Parked() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.parked
}

// Passed is how many declarations the gate has let through WHILE IT WAS IN
// FORCE: one per [DeclareGate.Step] that was taken, plus anything that was
// parked when [DeclareGate.Release] opened it. A declaration that arrives after
// Release is not gated and is not counted.
func (g *DeclareGate) Passed() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.passed
}

// Step lets one declaration through. A token released before the declaration
// arrives is taken by the next one that does, so a test may step ahead of the
// work; up to [declareGateDepth] tokens may be outstanding, and beyond that
// Step waits for one to be taken (or for [DeclareGate.Release]).
func (g *DeclareGate) Step() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.tokens >= declareGateDepth && !g.released {
		g.changed.Wait()
	}
	if g.released {
		return
	}
	g.tokens++
	g.changed.Broadcast()
}

// Release lets everything through, now and afterwards. It is idempotent, and
// calling it is how a test ends the stepped phase.
func (g *DeclareGate) Release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.released = true
	g.changed.Broadcast()
}

// enter parks one declaration. It is called with no broker lock held, which is
// what lets a test inspect the broker while a declaration is parked.
func (g *DeclareGate) enter() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.released {
		return
	}
	g.parked++
	for g.tokens == 0 && !g.released {
		g.changed.Wait()
	}
	if g.tokens > 0 {
		g.tokens--
	}
	g.parked--
	g.passed++
	// A Step waiting at the depth bound has room again.
	g.changed.Broadcast()
}

// declareGate is the gate in force, or nil.
func (b *Broker) declareGate() *DeclareGate {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.declGate
}
