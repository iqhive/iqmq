// Package wait holds the consumer wait strategies how a consumer that found its lane empty
// waits for a producer, and how a producer wakes it.
//
// Every strategy follows the same lost-wake-up-free protocol:
//
//	t := s.Prepare()          // announce intent to wait
//	if work available {       // re-check after announcing
//		s.Cancel(t)
//		return
//	}
//	err := s.Wait(ctx, t)     // returns nil once notified after Prepare, or ctx.Err()
//
// A Notify or NotifyAll that happens after Prepare and before Wait makes the
// Wait return immediately; one that happens before Prepare has no effect on
// that ticket. Producers may consult HasWaiters to skip Notify when nobody is
// waiting; the atomics used by Prepare and HasWaiters are sequentially
// consistent, so a producer that publishes work and then sees no waiter is
// guaranteed that any consumer preparing afterwards will re-check and find
// the work.
//
// What HasWaiters does not promise, in any kind: that a Notify following a
// true answer wakes anybody. A ticket that has been notified and whose owner
// has not yet returned from Wait is still counted, so the Notify after it can
// find nothing left to wake and do nothing at all. The protocol above is
// unharmed — the owner of such a ticket is on its way to the re-check, which
// happens after anything published before the dropped notification — but a
// producer must not read HasWaiters as "somebody is parked", nor a Notify as
// "delivered", and must not make a *second* wake conditional on the first
// having landed.
//
// The condition itself is the caller's, and every change to it is the
// caller's to announce. A wake protocol built on this one is only as complete
// as the set of state changes that call Notify: the shape that has cost this
// repository a deadlock is a state change on a withdrawal or refusal path
// that skips the notification its ordinary counterpart makes
// (`adr/ADR-017-assumptions.md`).
//
// That shape has now cost it eight, so it is checked rather than reviewed.
// `internal/engine/parkguard_test.go` scans that package's source for it:
// every write that can lower state somebody's park condition reads has to
// announce it, on every gate that guards it, by a statement the write cannot
// avoid -- and every park has to re-check the whole condition its own
// admission is refused by, or it spins instead of waiting. A caller of this
// package with counters of its own wants the same check; the one in
// `internal/engine` is written to be read as a pattern for it.
package wait

import (
	"context"
	"errors"
	"sync/atomic"
)

// Kind identifies a wait strategy. Auto resolves to AutoKind.
type Kind uint8

// Kinds. Chan is the Safe reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// Spin busy-polls an epoch counter and never yields the processor.
	Spin
	// Yield polls the epoch counter, calling runtime.Gosched between polls.
	Yield
	// Cond parks on a sync.Cond; cancellation is delivered by a Broadcast
	// registered with context.AfterFunc.
	Cond
	// EventCount is a monotonic event counter whose waiters park on a
	// per-generation channel that Notify closes: every notification is a
	// broadcast.
	EventCount
	// Chan keeps a list of waiters, each parked on its own channel, and
	// wakes exactly one per Notify. It is the Safe reference.
	Chan
	// SpinPark spins on the ticket for a bounded number of iterations before
	// parking like Chan.
	SpinPark
	// Hybrid spins, then yields, then parks, adapting the spin budget to
	// whether recent waits were satisfied while spinning.
	Hybrid

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "spin", "yield", "cond", "eventcount", "chan", "spinpark", "hybrid"}

var kindIdents = [kindCount]string{"Auto", "Spin", "Yield", "Cond", "EventCount", "Chan", "SpinPark", "Hybrid"}

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
	for k := Spin; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// WakesOne reports whether Notify wakes at most one waiter. Broadcast kinds
// wake every waiter on each Notify.
func (k Kind) WakesOne() bool {
	switch k.Resolve() {
	case Cond, Chan, SpinPark, Hybrid:
		return true
	default:
		return false
	}
}

// Parks reports whether a waiter eventually blocks in the scheduler instead
// of consuming a processor for the whole wait.
func (k Kind) Parks() bool {
	switch k.Resolve() {
	case Spin, Yield:
		return false
	default:
		return true
	}
}

// Ticket is a registration returned by Prepare and consumed by exactly one
// Wait or Cancel. Which fields are used depends on the kind.
type Ticket struct {
	epoch uint64      // epoch snapshot (Spin, Yield, Cond, EventCount)
	w     *waiter     // list entry (Chan, SpinPark, Hybrid)
	g     *generation // generation channel (EventCount)
}

// Strategy is a wait strategy shared by the consumers of one lane and the
// producers that feed it.
type Strategy interface {
	// Prepare registers the caller as a prospective waiter.
	Prepare() Ticket
	// Wait blocks until a Notify or NotifyAll issued after Prepare, or until
	// ctx is done, in which case it returns ctx.Err(). It consumes t.
	Wait(ctx context.Context, t Ticket) error
	// Cancel withdraws a ticket without waiting. A notification already
	// delivered to the ticket is forwarded to another waiter when the kind
	// wakes one at a time.
	Cancel(t Ticket)
	// Notify wakes at least one waiter registered before the call.
	Notify()
	// NotifyAll wakes every waiter registered before the call.
	NotifyAll()
	// HasWaiters reports whether any ticket is outstanding.
	HasWaiters() bool
}

// New builds a strategy of kind k, or fails on an invalid kind.
func New(k Kind) (Strategy, error) {
	k = k.Resolve()
	switch k {
	case Spin:
		return &epochStrategy{spin: spinForever}, nil
	case Yield:
		return &epochStrategy{spin: yieldForever}, nil
	case Cond:
		return newCondStrategy(), nil
	case EventCount:
		return newEventCount(), nil
	case Chan:
		return newList(0, 0, false), nil
	case SpinPark:
		return newList(spinIters, 0, false), nil
	case Hybrid:
		return newList(spinIters, yieldIters, true), nil
	default:
		return nil, errors.New("wait: invalid kind")
	}
}

// spinIters and yieldIters are the default spin and yield budgets.
const (
	spinIters  = 128
	yieldIters = 16
)

// cacheLine pads hot counters apart.
const cacheLine = 64

type pad [cacheLine]byte

// counter is a padded atomic.
type counter struct {
	_ pad
	v atomic.Uint64
	_ pad
}
