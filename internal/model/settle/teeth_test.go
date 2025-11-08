package settle

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
// message, paired with a TLA+ invariant by tools/modelcheck, and still
// evaluate nothing. That is the failure mode reviews of this repository keep
// finding; The mutation list is the same test on the specification
// side. tools/modelcheck fails when a documented invariant
// has no case here.
func TestInvariantsHaveTeeth(t *testing.T) {
	// leased returns a two-cell model with cell 0 leased under generation 1,
	// which every case below folds a fault into.
	leased := func(t *testing.T) *Model {
		t.Helper()
		m := New(2)
		if res := m.Step(Op{Kind: Lease}); !res.OK {
			t.Fatalf("lease: %+v", res)
		}
		// One more step, so that the generation of the previous state is the
		// current one: GenMonotone compares the two, and a fixture whose
		// last step was the lease itself would report that first whatever
		// the fault folded in.
		noop(t, m)
		return m
	}
	// settled leaves cell 0 Settled under generation 1, with one more step
	// applied so that the previous state is Settled too: the sticky
	// properties compare the state against the one before the last step.
	settled := func(t *testing.T) *Model {
		t.Helper()
		m := leased(t)
		if res := m.Step(Op{Kind: Begin, Racer: 1, Gen: 1}); !res.OK {
			t.Fatalf("begin: %+v", res)
		}
		if res := m.Step(Op{Kind: Commit, Racer: 1, Gen: 1}); !res.OK {
			t.Fatalf("commit: %+v", res)
		}
		noop(t, m)
		return m
	}
	// idle leaves cell 0 unleased again, its generation spent.
	idle := func(t *testing.T) *Model {
		t.Helper()
		m := leased(t)
		if res := m.Step(Op{Kind: Revoke, Gen: 1}); !res.OK {
			t.Fatalf("revoke: %+v", res)
		}
		noop(t, m)
		return m
	}
	cases := []struct {
		name  string
		want  string
		start func(t *testing.T) *Model
		fold  func(t *testing.T, m *Model)
	}{
		{"TypeOK/state", "TypeOK", nil, func(_ *testing.T, m *Model) {
			m.cells[0].State = stateCount
		}},
		{"TypeOK/pending", "TypeOK", nil, func(_ *testing.T, m *Model) {
			m.cells[0].Pending = Settling
		}},
		{"TypeOK/owner", "TypeOK", nil, func(_ *testing.T, m *Model) {
			// Active, so not settling, yet an attempt is recorded.
			m.cells[0].Owner = 1
		}},
		{"TypeOK/ownerless", "TypeOK", nil, func(_ *testing.T, m *Model) {
			m.cells[0].State = Settling
		}},
		{"OneWinner", "OneWinner", nil, func(_ *testing.T, m *Model) {
			// A second attempt in flight that the counter does not know
			// about: the engine's in-flight gauge and the cells disagree.
			m.cells[0].State, m.cells[0].Owner, m.cells[0].OwnerGen = Settling, 1, 1
		}},
		{"NoDoubleTerminal/twice", "NoDoubleTerminal", settled, func(_ *testing.T, m *Model) {
			d := &m.cells[0]
			d.Settled = append(d.Settled, d.Gen)
			d.Winners = append(d.Winners, Winner{Gen: d.Gen, Racer: 2})
		}},
		{"NoDoubleTerminal/unrecorded", "NoDoubleTerminal", settled, func(_ *testing.T, m *Model) {
			d := &m.cells[0]
			d.Settled, d.Winners = nil, nil
		}},
		{"StaleRejected/stale", "StaleRejected", nil, func(_ *testing.T, m *Model) {
			d := &m.cells[0]
			d.State, d.Owner, d.OwnerGen = Settling, 1, d.Gen+1
			m.open++
		}},
		{"StaleRejected/winners", "StaleRejected", settled, func(_ *testing.T, m *Model) {
			d := &m.cells[0]
			d.Winners = append(d.Winners, Winner{Gen: d.Gen, Racer: 3})
		}},
		{"LeaseLostNeverSettles", "LeaseLostNeverSettles", settled, func(_ *testing.T, m *Model) {
			d := &m.cells[0]
			d.Lost = append(d.Lost, d.Gen)
		}},
		{"UnknownSticky", "UnknownSticky", nil, func(t *testing.T, m *Model) {
			if res := m.Step(Op{Kind: Begin, Racer: 1, Gen: 1}); !res.OK {
				t.Fatalf("begin: %+v", res)
			}
			if res := m.Step(Op{Kind: Ambiguous, Racer: 1, Gen: 1}); !res.OK {
				t.Fatalf("ambiguous: %+v", res)
			}
			noop(t, m)
			m.cells[0].State = Idle
		}},
		{"SettledSticky", "SettledSticky", settled, func(_ *testing.T, m *Model) {
			m.cells[0].State = Idle
		}},
		{"ClosedSticky", "ClosedSticky", nil, func(t *testing.T, m *Model) {
			if res := m.Step(Op{Kind: Close, Gen: 1}); !res.OK {
				t.Fatalf("close: %+v", res)
			}
			noop(t, m)
			m.cells[0].State = Idle
		}},
		{"GenMonotone/jump", "GenMonotone", nil, func(_ *testing.T, m *Model) {
			m.cells[0].Gen += 2
		}},
		{"GenMonotone/unleased", "GenMonotone", idle, func(_ *testing.T, m *Model) {
			// The generation advances without the delivery becoming active,
			// which is a redelivery nobody can settle.
			m.cells[0].Gen = nextGen(m.cells[0].Gen)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start := tc.start
			if start == nil {
				start = leased
			}
			m := start(t)
			if err := m.Invariants(); err != nil {
				t.Fatalf("the fixture already violates an invariant: %v", err)
			}
			tc.fold(t, m)
			err := m.Invariants()
			if err == nil {
				t.Fatalf("%s did not fail on %s", tc.want, tc.name)
			}
			if !strings.HasPrefix(err.Error(), tc.want+":") {
				t.Fatalf("%s reported %q, want the %s violation", tc.name, err, tc.want)
			}
		})
	}
}

// noop applies an operation that changes nothing, so that the previous state
// the sticky properties compare against is the current one.
func noop(t *testing.T, m *Model) {
	t.Helper()
	before := m.cells[0]
	m.Step(Op{Kind: Revoke, Gen: before.Gen + 7})
	if m.cells[0].State != before.State || m.cells[0].Gen != before.Gen {
		t.Fatalf("the no-op step changed the cell: %+v -> %+v", before, m.cells[0])
	}
}
