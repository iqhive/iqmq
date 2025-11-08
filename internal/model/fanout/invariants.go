package fanout

import (
	"errors"
	"fmt"
)

// errStarved reports the one invariant with nothing to say beyond its name:
// a publication every open subscription had room for did not reach all of
// them.
var errStarved = errors.New("fanout: NoStarvation: a publication every subscription had room for did not reach all of them")

// Snapshot is a copy of the whole model.
type Snapshot struct {
	// Steps is the number of operations applied.
	Steps int
	// Seq is the last sequence number a publication assigned.
	Seq uint64
	// Subs holds one entry per subscription, in creation order.
	Subs []SubSnapshot
	// LogSlots and LogBytes are the shared log's occupancy, both 0 for a
	// per-subscription architecture.
	LogSlots int
	LogBytes int64
	// Reserved, Enqueued and Released are the reservation ledger.
	Reserved, Enqueued, Released int64
}

// SubSnapshot is what one subscription holds.
type SubSnapshot struct {
	// Held lists the record sequences the subscription still holds, in
	// admission order.
	Held []uint64
	// Bytes is their accounted size.
	Bytes int64
	// Closed reports whether the subscription has been detached.
	Closed bool
	// The counters mirror the engine's Snapshot.
	Published, Dropped, Rejected, Settled int64
}

// Snapshot returns a copy of the model state.
func (m *Model) Snapshot() Snapshot {
	snap := Snapshot{
		Steps: m.steps, Seq: m.seq,
		LogSlots: len(m.log), LogBytes: m.bytes,
		Reserved: m.reserved, Enqueued: m.enqueued, Released: m.releasedRes,
		Subs: make([]SubSnapshot, 0, len(m.subs)),
	}
	for i, s := range m.subs {
		snap.Subs = append(snap.Subs, SubSnapshot{
			Held: m.Held(i), Bytes: s.bytes, Closed: s.closed,
			Published: s.published, Dropped: s.dropped,
			Rejected: s.rejected, Settled: s.settled,
		})
	}
	return snap
}

// Invariants checks the fan-out admission invariants after every step. Their
// names are the ones the TLA+ specification carries, so that
// tools/modelcheck can pair them:
//
//   - TypeOK: every counter is non-negative, every subscription's byte
//     accounting matches the records it holds, and no subscription holds more
//     than its capacity.
//   - ReservationBalance: reserved == enqueued + released. Every reservation
//     a publication takes is either the record a subscription now holds or a
//     reservation that was given back; none is lost and none is counted
//     twice.
//   - AtomicAllOrNothing: under the Atomic policy every publication reaches
//     every open subscription or none of them, so the subscriptions hold the
//     same records.
//   - PartialAccounted: under a per-subscription policy a publication that
//     did not reach a subscription counted a rejection there, so a partial
//     delivery is never silent.
//   - NoOverflow: a subscription never holds more records, or more bytes,
//     than its capacity allows, whichever policy admitted them.
//   - DropSlowAccounted: a record dropped to make room is counted as dropped
//     and its reservation is released, so capacity a drop frees is capacity
//     the next publication can use.
//   - Isolation: a subscription's records depend only on the publications it
//     was open for and on its own settlements. Under a per-subscription
//     architecture, a subscription that never settles cannot take capacity
//     from another; under the shared log it can, which is what the two
//     architectures trade.
//   - NoStarvation: a publication that every open subscription had room for
//     reaches all of them, whatever the policy — a policy may only lose
//     records that did not fit.
//   - NoUnreservedAdmission: every record a subscription holds was reserved
//     before it was admitted: the number of records held equals the number
//     of reservations the ledger turned into records, so nothing is admitted
//     past the capacity check.
func (m *Model) Invariants() error {
	if m.reserved < 0 || m.enqueued < 0 || m.releasedRes < 0 {
		return fmt.Errorf("fanout: TypeOK: ledger is reserved=%d enqueued=%d released=%d",
			m.reserved, m.enqueued, m.releasedRes)
	}
	if m.reserved != m.enqueued+m.releasedRes {
		return fmt.Errorf("fanout: ReservationBalance: reserved %d != enqueued %d + released %d",
			m.reserved, m.enqueued, m.releasedRes)
	}
	held := int64(0)
	for i, s := range m.subs {
		if s.published < 0 || s.dropped < 0 || s.rejected < 0 || s.settled < 0 {
			return fmt.Errorf("fanout: TypeOK: subscription %d has a negative counter", i)
		}
		var bytes int64
		n := 0
		for _, r := range s.recs {
			if r.state == gone {
				continue
			}
			bytes += r.size
			n++
		}
		if bytes != s.bytes {
			return fmt.Errorf("fanout: TypeOK: subscription %d accounts %d bytes but holds %d", i, s.bytes, bytes)
		}
		if m.cfg.Capacity > 0 && n > m.cfg.Capacity {
			return fmt.Errorf("fanout: NoOverflow: subscription %d holds %d records over a capacity of %d",
				i, n, m.cfg.Capacity)
		}
		if m.cfg.CapacityBytes > 0 && bytes > m.cfg.CapacityBytes {
			return fmt.Errorf("fanout: NoOverflow: subscription %d holds %d bytes over a capacity of %d",
				i, bytes, m.cfg.CapacityBytes)
		}
		if s.closed && n != 0 {
			return fmt.Errorf("fanout: Isolation: closed subscription %d still holds %d records", i, n)
		}
		if s.published != s.settled+s.dropped+int64(n) {
			return fmt.Errorf("fanout: DropSlowAccounted: subscription %d published %d but accounts %d settled, %d dropped, %d held",
				i, s.published, s.settled, s.dropped, n)
		}
		if s.offered != s.published+s.rejected {
			return fmt.Errorf("fanout: PartialAccounted: subscription %d was offered %d publications but accounts %d admitted and %d refused",
				i, s.offered, s.published, s.rejected)
		}
		held += int64(n)
	}
	if m.starved {
		return errStarved
	}
	// Every record a subscription holds has a reservation behind it, which
	// is NoUnreservedAdmission stated directly: the ledger's enqueued count
	// is the number of reservations that became records. It used to report
	// ReservationBalance, which left NoUnreservedAdmission documented,
	// paired with the specification and evaluated nowhere ().
	if held != m.enqueued {
		return fmt.Errorf("fanout: NoUnreservedAdmission: %d records held, the ledger accounts %d reservations", held, m.enqueued)
	}
	if err := m.atomicInvariant(); err != nil {
		return err
	}
	return m.logInvariant()
}

// atomicInvariant checks that the Atomic policy left every open subscription
// holding the same records.
func (m *Model) atomicInvariant() error {
	if m.cfg.Policy != Atomic {
		return nil
	}
	var want []uint64
	first := -1
	for i, s := range m.subs {
		if s.closed || s.published == 0 {
			continue
		}
		// Only the record sequences a subscription was open for can be
		// compared, so the check starts at the first publication both saw.
		seqs := seen(s)
		if first < 0 {
			first, want = i, seqs
			continue
		}
		if !sameFrom(want, seqs) {
			return fmt.Errorf("fanout: AtomicAllOrNothing: subscription %d saw %v, subscription %d saw %v",
				first, want, i, seqs)
		}
	}
	return nil
}

// seen returns every record sequence a subscription was ever admitted.
func seen(s *sub) []uint64 {
	out := make([]uint64, 0, len(s.recs))
	for _, r := range s.recs {
		out = append(out, r.seq)
	}
	return out
}

// sameFrom reports whether two admission histories agree from the first
// record they have in common, which is what "every subscription or none"
// means for subscriptions that started at different times.
func sameFrom(a, b []uint64) bool {
	if len(a) == 0 || len(b) == 0 {
		return true
	}
	start := a[0]
	if b[0] > start {
		start = b[0]
	}
	trim := func(s []uint64) []uint64 {
		for i, v := range s {
			if v >= start {
				return s[i:]
			}
		}
		return nil
	}
	x, y := trim(a), trim(b)
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// logInvariant checks the shared log's own accounting.
func (m *Model) logInvariant() error {
	if !m.cfg.Shared {
		if len(m.log) != 0 {
			return fmt.Errorf("fanout: TypeOK: a per-subscription model holds %d log slots", len(m.log))
		}
		return nil
	}
	var bytes int64
	for _, sl := range m.log {
		if sl.refs < 0 {
			return fmt.Errorf("fanout: ReservationBalance: log slot %d is held by %d subscriptions", sl.seq, sl.refs)
		}
		bytes += sl.size
	}
	// A log reclaims from the tail: a slot nobody holds any more sits in the
	// log until the slots before it are gone too, which is what makes the
	// slowest subscription decide how much room the publishers have. The
	// tail itself is therefore always held.
	if len(m.log) > 0 && m.log[0].refs == 0 {
		return fmt.Errorf("fanout: ReservationBalance: the log tail (slot %d) is held by nobody", m.log[0].seq)
	}
	if bytes != m.bytes {
		return fmt.Errorf("fanout: TypeOK: the log accounts %d bytes but holds %d", m.bytes, bytes)
	}
	if m.cfg.Capacity > 0 && len(m.log) > m.cfg.Capacity {
		return fmt.Errorf("fanout: NoOverflow: the log holds %d slots over a capacity of %d",
			len(m.log), m.cfg.Capacity)
	}
	return nil
}
