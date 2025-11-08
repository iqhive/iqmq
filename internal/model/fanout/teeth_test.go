package fanout

import (
	"strings"
	"testing"
)

// TestInvariantsHaveTeeth forces each fault the invariants exist to catch
// directly onto the model's state and checks that the invariant that names it
// reports it.
//
// Step cannot produce these states — that is what the model is for — so
// without this test an invariant could be documented, named in a failure
// message, paired with a TLA+ invariant by tools/modelcheck and still
// evaluate nothing. The mutation list is the same
// test on the specification side; tools/modelcheck fails when a documented
// invariant has no case here.
func TestInvariantsHaveTeeth(t *testing.T) {
	// atomic holds one record in each of three subscriptions of capacity
	// two, which leaves room for the faults that need it.
	atomic := func(t *testing.T) *Model {
		t.Helper()
		m := New(Config{Policy: Atomic, Subs: 3, Capacity: 2})
		if res := m.Step(Op{Kind: Publish, Size: 1}); res.Err != nil {
			t.Fatalf("publish: %v", res.Err)
		}
		return m
	}
	// dropslow is the same under the policy that may discard.
	dropslow := func(t *testing.T) *Model {
		t.Helper()
		m := New(Config{Policy: DropSlow, Subs: 2, Capacity: 1})
		for range 2 {
			if res := m.Step(Op{Kind: Publish, Size: 1}); res.Err != nil {
				t.Fatalf("publish: %v", res.Err)
			}
		}
		return m
	}
	// shared is the LogCursor architecture, whose log has accounting of its
	// own.
	shared := func(t *testing.T) *Model {
		t.Helper()
		m := New(Config{Policy: Atomic, Subs: 2, Capacity: 2, Shared: true})
		if res := m.Step(Op{Kind: Publish, Size: 1}); res.Err != nil {
			t.Fatalf("publish: %v", res.Err)
		}
		return m
	}
	cases := []struct {
		name  string
		want  string
		start func(t *testing.T) *Model
		fold  func(m *Model)
	}{
		{"TypeOK/ledger", "TypeOK", nil, func(m *Model) { m.reserved = -1 }},
		{"TypeOK/counter", "TypeOK", nil, func(m *Model) { m.subs[0].settled = -1 }},
		{"TypeOK/bytes", "TypeOK", nil, func(m *Model) { m.subs[0].bytes += 7 }},
		{"TypeOK/log", "TypeOK", nil, func(m *Model) {
			// A per-subscription model with a shared log slot.
			m.log = append(m.log, slot{seq: 99, refs: 1})
		}},
		{"TypeOK/logbytes", "TypeOK", shared, func(m *Model) { m.bytes += 5 }},
		{"ReservationBalance", "ReservationBalance", nil, func(m *Model) {
			// A reservation that became neither a record nor a release.
			m.reserved++
		}},
		{"ReservationBalance/logrefs", "ReservationBalance", shared, func(m *Model) {
			m.log[0].refs = -1
		}},
		{"ReservationBalance/logtail", "ReservationBalance", shared, func(m *Model) {
			m.log[0].refs = 0
		}},
		{"NoOverflow/records", "NoOverflow", nil, func(m *Model) {
			// Two more records than the capacity allows, accounted for, so
			// that the capacity check is the one that notices.
			s := m.subs[0]
			for _, seq := range []uint64{90, 91} {
				s.recs = append(s.recs, rec{seq: seq, size: 1})
				s.bytes++
				s.published++
				s.offered++
				m.reserved++
				m.enqueued++
			}
		}},
		{"NoOverflow/bytes", "NoOverflow", nil, func(m *Model) {
			m.cfg.CapacityBytes = 0
			m.subs[0].recs[0].size = 2
			m.subs[0].bytes = 2
			m.cfg.CapacityBytes = 1
		}},
		{"NoOverflow/log", "NoOverflow", shared, func(m *Model) {
			// More slots than the log's capacity, each held and weightless,
			// so that only the capacity check has anything to say.
			m.log = append(m.log, slot{seq: 90, refs: 1}, slot{seq: 91, refs: 1})
		}},
		{"AtomicAllOrNothing", "AtomicAllOrNothing", nil, func(m *Model) {
			// One subscription holds a record the others never saw, which is
			// the partial delivery Atomic promises cannot happen.
			s := m.subs[1]
			s.recs = append(s.recs, rec{seq: 99, size: 1})
			s.bytes++
			s.published++
			s.offered++
			m.reserved++
			m.enqueued++
		}},
		{"PartialAccounted", "PartialAccounted", nil, func(m *Model) {
			// A publication a subscription did not get and did not count.
			m.subs[0].offered++
		}},
		{"DropSlowAccounted", "DropSlowAccounted", dropslow, func(m *Model) {
			// A record dropped to make room that nothing counted.
			m.subs[0].dropped--
		}},
		{"Isolation", "Isolation", nil, func(m *Model) {
			// A detached subscription still holding records: its records
			// would outlive it and hold capacity nobody can free.
			m.subs[0].closed = true
		}},
		{"NoStarvation", "NoStarvation", nil, func(m *Model) { m.starved = true }},
		{"NoUnreservedAdmission", "NoUnreservedAdmission", nil, func(m *Model) {
			// A record admitted behind the ledger's back: the capacity check
			// never saw it.
			s := m.subs[0]
			s.recs = append(s.recs, rec{seq: 98, size: 1})
			s.bytes++
			s.published++
			s.offered++
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start := tc.start
			if start == nil {
				start = atomic
			}
			m := start(t)
			if err := m.Invariants(); err != nil {
				t.Fatalf("the fixture already violates an invariant: %v", err)
			}
			tc.fold(m)
			err := m.Invariants()
			if err == nil {
				t.Fatalf("%s did not fail on %s", tc.want, tc.name)
			}
			if !strings.HasPrefix(err.Error(), "fanout: "+tc.want+":") {
				t.Fatalf("%s reported %q, want the %s violation", tc.name, err, tc.want)
			}
		})
	}
}
