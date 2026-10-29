package engine

import "github.com/iqhive/iqmq/internal/errcode"

// FanoutPolicy decides what a publication does when one of the subscriptions
// it fans out to has no room for it (the memory engine TopologyConfig.Fanout).
//
// The four policies differ only in admission; once a record is admitted to a
// subscription every policy treats it identically. They trade the guarantee
// that every subscription sees the same records against the guarantee that
// one slow subscription cannot stop the others:
//
//	Atomic       all subscriptions or none; a full one refuses for everybody
//	Independent  each subscription decides for itself, and the publisher is
//	             told when at least one refused
//	BestEffort   each subscription decides for itself, silently
//	DropSlow     a full subscription makes room by dropping its oldest
//	             record, so it never refuses while it holds anything
//
// Independent and BestEffort therefore deliver partially: the record reaches
// the subscriptions that had room and the others count a rejection. A
// publisher must not retry such a publication — the subscriptions that did
// accept it would receive it twice — which is why Independent reports the
// partial outcome with errcode.ErrPartial rather than pretending the
// publication failed or that nothing was admitted. That is also what tells an
// owned publisher who holds the payload: ErrPartial means the engine took it
// and will release it, ErrQueueFull means nothing was admitted and ownership
// never left the caller (Publish.Release). Atomic, the default, is the only
// policy under which a successful publish means every subscription holds the
// record and a failed one means none does.
type FanoutPolicy uint8

// The admission policies. FanoutAuto resolves to the recorded default in
// auto_fanout.go.
const (
	// FanoutAuto selects the recorded bake-off winner.
	FanoutAuto FanoutPolicy = iota
	// Atomic reserves capacity in every target in a stable order and admits
	// the record only when every reservation succeeded, releasing them all
	// when one fails (the memory engine). The stable order is the topic's
	// subscription order, which is why concurrent publications cannot
	// deadlock against each other.
	Atomic
	// Independent admits the record to every subscription that has room and
	// counts a rejection on the ones that do not, then reports ErrPartial so
	// that the publisher knows the delivery was partial and that the engine
	// owns the payload. It reports ErrQueueFull only when no subscription
	// took the record at all.
	Independent
	// BestEffort admits the record to every subscription that has room and
	// counts a rejection on the ones that do not. It fails only when no
	// subscription accepted the record at all.
	BestEffort
	// DropSlow makes room in a full subscription by dropping the oldest
	// record it holds, so a subscription that is not keeping up loses its
	// backlog rather than its publisher. It fails for a subscription whose
	// records are all in flight, since there is then nothing to drop. When
	// a shared partition log is full, the record it drops is the log's
	// oldest slot, for the subscriptions still holding it: one that has read
	// past the slot keeps its records, and a lease on the slot leaves
	// nothing to drop. When the log has room and the engine-wide limits
	// refuse, each subscription drops its own oldest record.
	DropSlow

	fanoutCount
)

var fanoutNames = [fanoutCount]string{"auto", "atomic", "independent", "besteffort", "dropslow"}

var fanoutIdents = [fanoutCount]string{"FanoutAuto", "Atomic", "Independent", "BestEffort", "DropSlow"}

// FanoutPolicies returns the concrete policies in declaration order.
// FanoutAuto is not one of them.
func FanoutPolicies() []FanoutPolicy {
	return []FanoutPolicy{Atomic, Independent, BestEffort, DropSlow}
}

// String returns the benchmark slug of the policy.
func (f FanoutPolicy) String() string {
	if f < fanoutCount {
		return fanoutNames[f]
	}
	return "fanout(?)"
}

// Ident returns the Go identifier of the policy.
func (f FanoutPolicy) Ident() string {
	if f < fanoutCount {
		return fanoutIdents[f]
	}
	return ""
}

// Valid reports whether f names a policy or FanoutAuto.
func (f FanoutPolicy) Valid() bool { return f < fanoutCount }

// Resolve maps FanoutAuto to the recorded default.
func (f FanoutPolicy) Resolve() FanoutPolicy {
	if f == FanoutAuto {
		return FanoutAutoKind
	}
	return f
}

// ParseFanoutPolicy maps a slug back to its policy.
func ParseFanoutPolicy(s string) (FanoutPolicy, bool) {
	for i, n := range fanoutNames {
		if n == s {
			return FanoutPolicy(i), true
		}
	}
	return FanoutAuto, false
}

// AllOrNothing reports whether a successful publication under the policy
// reaches every subscription and a failed one reaches none.
func (f FanoutPolicy) AllOrNothing() bool { return f.Resolve() == Atomic }

// admission is the outcome of the reservation pass of one publication: which
// of the target subscriptions hold a reservation, and what the publisher is
// told.
//
// The mask is inline for up to 256 subscriptions, which is the size at which
// a topic's fan-out stops being a fan-out; beyond that the non-atomic
// policies allocate one small word slice per publication. Atomic, the
// default, never touches the overflow: it either takes every subscription or
// none.
type admission struct {
	words [4]uint64
	over  []uint64
	n     int
	// admitted counts the subscriptions that hold a reservation.
	admitted int
	// refused counts the subscriptions that had no room.
	refused int
}

// reset prepares the mask for n subscriptions.
func (a *admission) reset(n int) {
	a.words = [4]uint64{}
	a.over = nil
	a.n = n
	a.admitted = 0
	a.refused = 0
	if n > 256 {
		a.over = make([]uint64, (n+63)/64)
	}
}

// set marks subscription i as holding a reservation.
func (a *admission) set(i int) {
	a.admitted++
	if a.over != nil {
		a.over[i/64] |= 1 << (i % 64)
		return
	}
	a.words[i/64] |= 1 << (i % 64)
}

// clear gives up every reservation the mask holds, which is what an Atomic
// pass that was refused anywhere does before it reports the refusal: the
// caller has already returned the reservations themselves.
func (a *admission) clear() {
	a.words = [4]uint64{}
	if a.over != nil {
		clear(a.over)
	}
	a.admitted = 0
}

// has reports whether subscription i holds a reservation.
func (a *admission) has(i int) bool {
	if a.over != nil {
		return a.over[i/64]&(1<<(i%64)) != 0
	}
	return a.words[i/64]&(1<<(i%64)) != 0
}

// all reports whether every target holds a reservation.
func (a *admission) all() bool { return a.admitted == a.n }

// err returns what the publisher is told, given the policy. A partial
// admission is a distinct outcome from a refusal, and not only because it is
// not retryable: the engine holds the record, so an owned payload is the
// engine's to release from that point on, which the publisher can only know
// if the two do not look alike (Publish.Release).
func (a *admission) err(f FanoutPolicy) error {
	switch {
	case a.admitted == 0:
		return errcode.ErrQueueFull
	case a.refused == 0:
		return nil
	case f == Independent:
		return errcode.ErrPartial
	default:
		return nil
	}
}
