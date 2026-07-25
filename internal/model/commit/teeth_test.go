package commit

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
// evaluate nothing. models/tla/mutations.txt is the same
// test on the specification side; tools/modelcheck fails when a documented
// invariant has no case here.
func TestInvariantsHaveTeeth(t *testing.T) {
	// tracking delivers offsets 0 and 1 and completes neither, so the
	// watermark is 0, high is 2 and two offsets are in flight.
	tracking := func(t *testing.T) *Model {
		t.Helper()
		m := New(Config{Window: 4})
		for _, off := range []int64{0, 1} {
			if res := m.Step(Op{Kind: Track, Gen: m.gen, Off: off}); res.Err != nil {
				t.Fatalf("track %d: %v", off, res.Err)
			}
		}
		return m
	}
	// settled completes both, so nothing is in flight and the watermark has
	// caught up with high.
	settled := func(t *testing.T) *Model {
		t.Helper()
		m := tracking(t)
		for _, off := range []int64{0, 1} {
			if res := m.Step(Op{Kind: Complete, Gen: m.gen, Off: off}); res.Err != nil {
				t.Fatalf("complete %d: %v", off, res.Err)
			}
		}
		return m
	}
	cases := []struct {
		name  string
		want  string
		start func(t *testing.T) *Model
		fold  func(m *Model)
	}{
		{"TypeOK/watermark", "TypeOK", nil, func(m *Model) { m.base = m.high + 1 }},
		{"TypeOK/gen", "TypeOK", nil, func(m *Model) { m.gen = 0 }},
		{"TypeOK/outside", "TypeOK", nil, func(m *Model) { m.delivered[m.high+3] = false }},
		{"TypeOK/pending", "TypeOK", nil, func(m *Model) { m.pending++ }},
		{"SafeWatermark", "SafeWatermark", nil, func(m *Model) {
			// The watermark passes an offset that has not completed, which is
			// the commit that loses a record.
			m.delivered[m.base] = false
			m.base++
			m.pending = 2
		}},
		{"WatermarkProgress/stalled", "WatermarkProgress", settled, func(m *Model) {
			// Nothing in flight, yet the watermark sits below the delivery
			// head: a tracker that never advances past a gap.
			m.base--
		}},
		{"WatermarkProgress/behind", "WatermarkProgress", nil, func(m *Model) {
			// Offset 0 completed but the watermark did not move to 1.
			m.delivered[0] = true
			m.pending--
		}},
		{"BoundedWindow/span", "BoundedWindow", nil, func(m *Model) {
			m.high = m.base + int64(m.cfg.Window) + 1
		}},
		{"BoundedWindow/pending", "BoundedWindow", nil, func(m *Model) {
			m.cfg.Window = 1
		}},
		{"StaleGenRejected/live", "StaleGenRejected", nil, func(m *Model) {
			// A call under the live generation recorded as stale: the tracker
			// refused work it owns.
			m.stale = append(m.stale, handle{Gen: m.gen, Off: 0})
		}},
		{"StaleGenRejected/rejected", "StaleGenRejected", nil, func(m *Model) {
			// A completion of a live, incomplete offset refused.
			m.rejected = append(m.rejected, handle{Gen: m.gen, Off: 0})
		}},
		{"CommitSafe", "CommitSafe", nil, func(m *Model) {
			m.committed, m.committedGen = m.high, m.gen
		}},
		{"WatermarkMonotone/gen", "WatermarkMonotone", nil, func(m *Model) {
			m.gen += 2
		}},
		{"WatermarkMonotone/back", "WatermarkMonotone", settled, func(m *Model) {
			m.prev.base = m.base + 1
		}},
		{"CommitMonotone", "CommitMonotone", nil, func(m *Model) {
			m.prev.committed = m.committed + 1
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start := tc.start
			if start == nil {
				start = tracking
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
			if !strings.HasPrefix(err.Error(), tc.want+":") {
				t.Fatalf("%s reported %q, want the %s violation", tc.name, err, tc.want)
			}
		})
	}
}
