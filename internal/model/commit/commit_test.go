package commit

import (
	"errors"
	"math"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

func step(t *testing.T, m *Model, op Op) Result {
	t.Helper()
	res := m.Step(op)
	if err := m.Invariants(); err != nil {
		t.Fatalf("after %v(gen=%d, off=%d): %v", op.Kind, op.Gen, op.Off, err)
	}
	return res
}

// TestScenarios walks the commit rules one by one.
func TestScenarios(t *testing.T) {
	m := New(Config{Window: 8, MaxPending: 4, MaxGap: 6})
	gen := m.Gen()
	if gen != 1 || m.Watermark() != 0 || m.Pending() != 0 || m.Paused() {
		t.Fatalf("fresh model: gen %d wm %d pending %d paused %v", gen, m.Watermark(), m.Pending(), m.Paused())
	}
	// Ordering, generation and window rules.
	if res := step(t, m, Op{Kind: Track, Gen: gen + 1, Off: 0}); !errors.Is(res.Err, ErrStaleGeneration) {
		t.Fatalf("stale track: %v", res.Err)
	}
	if res := step(t, m, Op{Kind: Track, Gen: gen, Off: 0}); res.Err != nil || res.Watermark != 0 || res.Pending != 1 {
		t.Fatalf("track 0: %+v", res)
	}
	if res := step(t, m, Op{Kind: Track, Gen: gen, Off: 0}); !errors.Is(res.Err, ErrOutOfOrder) {
		t.Fatalf("re-track: %v", res.Err)
	}
	if res := step(t, m, Op{Kind: Track, Gen: gen, Off: 8}); !errors.Is(res.Err, ErrWindowFull) {
		t.Fatalf("beyond the window: %v", res.Err)
	}
	// A completion of an offset that was never delivered changes nothing.
	if res := step(t, m, Op{Kind: Complete, Gen: gen, Off: 5}); res.Advanced || res.Watermark != 0 {
		t.Fatalf("unknown completion: %+v", res)
	}
	// Out-of-order completion: the watermark waits for the lowest.
	for off := int64(1); off < 4; off++ {
		step(t, m, Op{Kind: Track, Gen: gen, Off: off})
	}
	if !m.Paused() {
		t.Fatalf("four in flight did not pause")
	}
	if res := step(t, m, Op{Kind: Complete, Gen: gen, Off: 2}); res.Advanced || res.Watermark != 0 {
		t.Fatalf("complete 2: %+v", res)
	}
	if res := step(t, m, Op{Kind: Complete, Gen: gen, Off: 2}); res.Advanced {
		t.Fatal("repeated completion advanced the watermark")
	}
	if res := step(t, m, Op{Kind: Complete, Gen: gen, Off: 0}); !res.Advanced || res.Watermark != 1 {
		t.Fatalf("complete 0: %+v", res)
	}
	if res := step(t, m, Op{Kind: Complete, Gen: gen, Off: 1}); !res.Advanced || res.Watermark != 3 {
		t.Fatalf("complete 1: %+v", res)
	}
	// Commit publishes the watermark; a stale commit is refused.
	step(t, m, Op{Kind: Commit, Gen: gen})
	if s := m.Snapshot(); s.Committed != 3 {
		t.Fatalf("committed %d", s.Committed)
	}
	step(t, m, Op{Kind: Commit, Gen: gen + 7})
	if s := m.Snapshot(); s.Committed != 3 {
		t.Fatalf("a stale commit published %d", s.Committed)
	}
	// Gaps: an offset the broker never delivered does not block, and with
	// nothing in flight the watermark jumps to the new record.
	step(t, m, Op{Kind: Complete, Gen: gen, Off: 3})
	if m.Watermark() != 4 || m.Pending() != 0 {
		t.Fatalf("wm %d pending %d", m.Watermark(), m.Pending())
	}
	if res := step(t, m, Op{Kind: Track, Gen: gen, Off: 9}); res.Err != nil || res.Watermark != 9 {
		t.Fatalf("track over a gap: %+v", res)
	}
	if done, err := m.Delivered(9); err != nil || done {
		t.Fatalf("Delivered(9) = %v %v", done, err)
	}
	if _, err := m.Delivered(3); !errors.Is(err, ErrNoSuchOffset) {
		t.Fatalf("Delivered(3) err = %v", err)
	}
	// Rebalance: a fresh generation, everything in flight dropped.
	res := step(t, m, Op{Kind: Reset, Off: 20})
	if res.Gen != gen+1 || res.Watermark != 20 || res.Pending != 0 {
		t.Fatalf("reset: %+v", res)
	}
	if res := step(t, m, Op{Kind: Complete, Gen: gen, Off: 9}); res.Advanced || res.Watermark != 20 {
		t.Fatalf("stale completion after the rebalance: %+v", res)
	}
	if res := step(t, m, Op{Kind: Track, Gen: gen, Off: 20}); !errors.Is(res.Err, ErrStaleGeneration) {
		t.Fatalf("stale track after the rebalance: %v", res.Err)
	}
	if res := step(t, m, Op{Kind: Track, Gen: res.Gen, Off: 20}); res.Err != nil {
		t.Fatalf("track under the new generation: %v", res.Err)
	}
}

func TestConfigAndNames(t *testing.T) {
	if n := (Config{}).Normalize(); n.Window != DefaultWindow || n.MaxPending != DefaultWindow || n.MaxGap != DefaultWindow {
		t.Fatalf("zero config normalised to %+v", n)
	}
	for _, c := range []Config{{Base: -1}, {Window: -1}, {MaxPending: -1}, {MaxGap: -1}, {Window: 2, MaxGap: 3}} {
		if err := c.Validate(); !errors.Is(err, errcode.New(errcode.InvalidConfig, "")) {
			t.Fatalf("%+v: Validate = %v", c, err)
		}
	}
	if Track.String() != "track" || Reset.String() != "reset" || Kind(200).String() != "kind(?)" {
		t.Fatal("Kind.String")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("New with an invalid config should panic")
			}
		}()
		New(Config{Base: -1})
	}()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("an undefined operation should panic")
			}
		}()
		New(Config{}).Step(Op{Kind: Kind(200)})
	}()
}

func TestAdvanceAndSnapshot(t *testing.T) {
	m := New(Config{Base: 5, Window: 16})
	m.Advance(time.Second)
	m.Advance(-time.Second)
	if m.Now() != time.Second {
		t.Fatalf("Now = %v", m.Now())
	}
	step(t, m, Op{Kind: Track, Gen: m.Gen(), Off: 5})
	s := m.Snapshot()
	if s.Watermark != 5 || s.High != 6 || s.Pending != 1 || s.Steps != 1 || s.Now != time.Second || s.Committed != -1 {
		t.Fatalf("snapshot = %+v", s)
	}
	s.Delivered[5] = true
	if done, _ := m.Delivered(5); done {
		t.Fatal("the snapshot aliases model state")
	}
	if m.Steps() != 1 {
		t.Fatalf("Steps = %d", m.Steps())
	}
}

func TestGenWrapSkipsZero(t *testing.T) {
	m := New(Config{})
	m.gen = math.MaxUint32
	m.prev.gen = math.MaxUint32
	if res := step(t, m, Op{Kind: Reset}); res.Gen != 1 {
		t.Fatalf("generation at wrap: %d", res.Gen)
	}
}

// randomOp decodes one operation from fuzz input, biased towards operations
// the model accepts so that scripts reach interesting states.
func randomOp(r *fuzzutil.Reader, m *Model) Op {
	op := Op{Kind: Kind(r.Choice(int(kindCount))), Gen: m.Gen()}
	if r.Choice(8) == 0 {
		op.Gen = m.Gen() + uint32(r.Int(1, 3))
	}
	s := m.Snapshot()
	switch op.Kind {
	case Track:
		op.Off = s.High + int64(r.Int(0, 4))
	case Complete:
		op.Off = s.Watermark + int64(r.Int(0, 8))
	case Reset:
		op.Off = s.Watermark + int64(r.Int(0, 4))
	default: // Commit takes no offset
	}
	return op
}

// FuzzCommitModel feeds fuzzer-chosen operation streams to the model; any
// invariant violation is a bug in the model itself.
func FuzzCommitModel(f *testing.F) {
	w := fuzzutil.NewWriter()
	w.Int(1, 64, 8)
	for _, k := range []Kind{Track, Track, Complete, Commit, Track, Complete, Complete, Reset, Track} {
		w.Choice(int(kindCount), int(k))
		w.Choice(8, 1)
		w.Int(0, 8, 0)
	}
	f.Add(w.Data())
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		window := r.Int(1, 64)
		m := New(Config{Window: window})
		for i := 0; i < 512 && !r.Exhausted(); i++ {
			op := randomOp(r, m)
			m.Step(op)
			if err := m.Invariants(); err != nil {
				t.Fatalf("step %d (%v gen=%d off=%d): %v", m.Steps(), op.Kind, op.Gen, op.Off, err)
			}
			// The watermark equals its closed form: the lowest incomplete
			// offset, or the offset after the newest delivered record.
			s := m.Snapshot()
			lowest := int64(math.MaxInt64)
			for off, done := range s.Delivered {
				if !done && off < lowest {
					lowest = off
				}
			}
			want := s.High
			if lowest != math.MaxInt64 {
				want = lowest
			}
			if s.Watermark != want {
				t.Fatalf("step %d: watermark %d, closed form %d", m.Steps(), s.Watermark, want)
			}
		}
	})
}
