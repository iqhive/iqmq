package memq

import (
	"strings"
	"testing"
	"time"
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
// invariant has no case here. TestInvariantsCatchCorruption covers the same
// ground for the faults a differential run is most likely to produce; this
// one is the coverage claim, one case per documented name.
func TestInvariantsHaveTeeth(t *testing.T) {
	// queued leaves one subscription holding one ready record.
	queued := func(t *testing.T) *Model {
		t.Helper()
		m := New(Config{
			Partitions: 1, CapacityMessages: 4, AckDeadline: time.Second,
			Ordered: true, MaxAttempts: 2, DeadLetter: true,
		})
		m.Step(Op{Kind: Subscribe, Sub: "s"})
		m.Step(Op{Kind: Attach, Sub: "s", Consumer: 1})
		if res := m.Step(Op{Kind: Publish, Size: 2}); res.Err != nil {
			t.Fatalf("publish: %v", res.Err)
		}
		return m
	}
	// leased leaves that record in flight under consumer 1.
	leased := func(t *testing.T) *Model {
		t.Helper()
		m := queued(t)
		if res := m.Step(Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 1}); res.Err != nil || res.N != 1 {
			t.Fatalf("fetch: %+v", res)
		}
		return m
	}
	// acked leaves the record terminal, held by no lane structure.
	acked := func(t *testing.T) *Model {
		t.Helper()
		m := leased(t)
		if res := m.Step(Op{Kind: Ack, Sub: "s", Consumer: 1, Seq: 1, Gen: 1}); res.Err != nil {
			t.Fatalf("ack: %v", res.Err)
		}
		return m
	}
	sub := func(m *Model) *sub {
		_, s := m.find("s")
		return s
	}
	cases := []struct {
		name  string
		want  string
		start func(t *testing.T) *Model
		fold  func(m *Model)
	}{
		{"TypeOK/state", "TypeOK", acked, func(m *Model) { sub(m).recs[1].state = stateCount }},
		{"TypeOK/counter", "TypeOK", nil, func(m *Model) { sub(m).st.attempts = -1 }},
		{"OneState/lane", "OneState", nil, func(m *Model) { sub(m).recs[1].state = InFlight }},
		{"OneState/twice", "OneState", nil, func(m *Model) {
			s := sub(m)
			s.lanes[0].ready.push(1, 0)
			s.lanes[0].depth++
		}},
		{"LeaseUnique", "LeaseUnique", leased, func(m *Model) { sub(m).recs[1].cell = cellIdle }},
		{"AttemptsMonotone", "AttemptsMonotone", leased, func(m *Model) { sub(m).recs[1].attempts = 0 }},
		{"MaxAttemptsRespected/record", "MaxAttemptsRespected", nil, func(m *Model) {
			sub(m).recs[1].attempts = 9
		}},
		{"MaxAttemptsRespected/nolimit", "MaxAttemptsRespected", nil, func(m *Model) {
			m.cfg.MaxAttempts = 0
			sub(m).st.deadLettered = 1
			sub(m).st.published = 2
		}},
		{"CapacityNonNegative/depth", "CapacityNonNegative", nil, func(m *Model) { sub(m).lanes[0].depth = 5 }},
		{"CapacityNonNegative/bytes", "CapacityNonNegative", nil, func(m *Model) { sub(m).lanes[0].bytes = -1 }},
		{"CapacityNonNegative/engine", "CapacityNonNegative", nil, func(m *Model) { m.msgs = -1 }},
		{"NoUnadmittedSettlement", "NoUnadmittedSettlement", nil, func(m *Model) {
			// A settlement of something the subscription never admitted.
			sub(m).st.acked = 2
		}},
		{"Conservation", "Conservation", nil, func(m *Model) { sub(m).st.published = 9 }},
		{"ReleaseOnce/early", "ReleaseOnce", nil, func(m *Model) { m.release[1] = 1 }},
		{"ReleaseOnce/negative", "ReleaseOnce", nil, func(m *Model) { m.refs[1] = -1 }},
		{"NoEarlyDelivery/delayed", "NoEarlyDelivery", nil, func(m *Model) {
			// The retry queue holds the record back to a time in the future
			// while the record itself says its due time has come: a
			// promotion that is about to happen early. The only way to
			// reach the first half of this invariant is to make the two
			// disagree — a record whose own due time has passed sits in a
			// structure OneState expects to be Ready.
			s := sub(m)
			s.recs[1].state = Delayed
			s.recs[1].dueAt = m.now
			s.lanes[0].ready.reset()
			s.lanes[0].retry = append(s.lanes[0].retry, retryRec{seq: 1, dueAt: m.now + time.Second})
		}},
		{"NoEarlyDelivery/inflight", "NoEarlyDelivery", leased, func(m *Model) {
			sub(m).recs[1].dueAt = m.now + time.Second
		}},
		{"ClosedTerminal", "ClosedTerminal", nil, func(m *Model) { sub(m).closed = true }},
		{"DeadLetterBeforeSettle", "DeadLetterBeforeSettle", nil, func(m *Model) {
			m.cfg.DeadLetter = false
			s := sub(m)
			s.st.deadLettered = 1
			s.st.published = 2
		}},
		{"OverflowAccounted/dropnew", "OverflowAccounted", nil, func(m *Model) { sub(m).st.overflowed = 1 }},
		{"OverflowAccounted/dropold", "OverflowAccounted", nil, func(m *Model) { sub(m).st.displaced = 1 }},
		{"StrictBlocksLane/count", "StrictBlocksLane", nil, func(m *Model) { sub(m).lanes[0].out = 1 }},
		{"StrictBlocksLane/two", "StrictBlocksLane", leased, func(m *Model) {
			s := sub(m)
			s.recs[1].sealed = true
			s.lanes[0].out = 2
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start := tc.start
			if start == nil {
				start = queued
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
			if !strings.HasPrefix(err.Error(), "memq: "+tc.want+":") {
				t.Fatalf("%s reported %q, want the %s violation", tc.name, err, tc.want)
			}
		})
	}
}
