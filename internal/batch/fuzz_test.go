package batch

import (
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/clock"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// batchModel is the reference batcher: a plain slice, the arrival time of
// the first pending item and the same thresholds. It decides readiness
// from the configuration alone, so it is an independent oracle for the
// batcher's own bookkeeping.
type batchModel struct {
	kind     Kind
	cfg      Config
	period   time.Duration // shared-ticker grid
	epoch    time.Time
	items    []int
	bytes    int
	deadline time.Time
	timed    bool
	closed   bool
}

func (m *batchModel) add(v, size int, now time.Time) (bool, error) {
	if m.closed {
		return false, errcode.ErrClosed
	}
	if size < 0 {
		size = 0
	}
	if len(m.items) == 0 {
		switch m.kind {
		case PerBatchTimer:
			m.deadline, m.timed = now.Add(m.cfg.Linger), true
		case SharedTicker:
			want := now.Add(m.cfg.Linger)
			d := want.Sub(m.epoch)
			n := d / m.period
			if d <= 0 {
				n = 0
			} else if d%m.period != 0 {
				n++
			}
			m.deadline, m.timed = m.epoch.Add(n*m.period), true
		case SizeOnly:
			m.timed = false
		default:
			m.timed = false
		}
	}
	m.items = append(m.items, v)
	m.bytes += size
	return m.sizeReady(), nil
}

func (m *batchModel) sizeReady() bool {
	if len(m.items) == 0 {
		return false
	}
	if m.cfg.MaxMessages > 0 && len(m.items) >= m.cfg.MaxMessages {
		return true
	}
	return m.cfg.MaxBytes > 0 && m.bytes >= m.cfg.MaxBytes
}

func (m *batchModel) ready(now time.Time) bool {
	if len(m.items) == 0 {
		return false
	}
	return m.sizeReady() || m.timed && !now.Before(m.deadline)
}

func (m *batchModel) take() []int {
	out := append([]int(nil), m.items...)
	m.items = m.items[:0]
	m.bytes = 0
	m.timed = false
	m.deadline = time.Time{}
	return out
}

// fuzzState pairs the batcher with the model and the conservation ledger.
type fuzzState struct {
	b     Batcher[int]
	m     *batchModel
	clk   *clock.Fake
	added int
	out   []int
}

// FuzzBatcher drives a batcher and its reference model with the same
// operations and compares them after every step: the size and count
// thresholds, the linger deadline, and — the invariant that matters most
// — conservation: every item added comes out exactly once, in order, and
// nothing is stranded by a close (the test suite).
func FuzzBatcher(f *testing.F) {
	script := fuzzutil.Script[*fuzzState]{
		Steps: []fuzzutil.Step[*fuzzState]{
			{Name: "add", Weight: 6, Run: func(t testing.TB, r *fuzzutil.Reader, s *fuzzState) {
				size := r.Int(-2, 1024)
				v := s.added
				got, gotErr := s.b.Add(v, size)
				want, wantErr := s.m.add(v, size, s.clk.Now())
				if errcode.CodeOf(gotErr) != errcode.CodeOf(wantErr) {
					t.Fatalf("Add error %v, model %v", gotErr, wantErr)
				}
				if gotErr == nil {
					s.added++
				}
				if got != want {
					t.Fatalf("Add(size %d) ready = %v, model %v", size, got, want)
				}
			}},
			{Name: "advance", Weight: 3, Run: func(_ testing.TB, r *fuzzutil.Reader, s *fuzzState) {
				s.clk.Advance(r.Duration(50 * time.Millisecond))
			}},
			{Name: "takeready", Weight: 4, Run: func(t testing.TB, _ *fuzzutil.Reader, s *fuzzState) {
				now := s.clk.Now()
				wantReady := s.m.ready(now)
				if got := s.b.Ready(now); got != wantReady {
					t.Fatalf("Ready = %v, model %v (pending %d)", got, wantReady, s.b.Len())
				}
				before := len(s.out)
				out, took := s.b.TakeReady(now, s.out)
				if took != wantReady {
					t.Fatalf("TakeReady took %v, model %v", took, wantReady)
				}
				s.out = out
				if !took {
					return
				}
				want := s.m.take()
				got := s.out[before:]
				if len(got) != len(want) {
					t.Fatalf("took %d items, model %d", len(got), len(want))
				}
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("item %d = %d, model %d", i, got[i], want[i])
					}
				}
			}},
			{Name: "flush", Weight: 2, Run: func(t testing.TB, _ *fuzzutil.Reader, s *fuzzState) {
				before := len(s.out)
				s.out = s.b.Flush(s.out)
				want := s.m.take()
				got := s.out[before:]
				if len(got) != len(want) {
					t.Fatalf("Flush returned %d items, model %d", len(got), len(want))
				}
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("flushed item %d = %d, model %d", i, got[i], want[i])
					}
				}
			}},
			{Name: "close", Weight: 1, Run: func(t testing.TB, _ *fuzzutil.Reader, s *fuzzState) {
				if err := s.b.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				s.m.closed = true
				// Close disarms the deadline: a closed batcher is ready
				// only on its size thresholds, and the caller flushes.
				s.m.timed = false
				s.m.deadline = time.Time{}
				if ch := s.b.Wait(); ch != nil {
					t.Fatal("Wait is not nil after Close")
				}
			}},
			{Name: "inspect", Weight: 1, Run: func(t testing.TB, _ *fuzzutil.Reader, s *fuzzState) {
				if got, want := s.b.Len(), len(s.m.items); got != want {
					t.Fatalf("Len = %d, model %d", got, want)
				}
				if got, want := s.b.Bytes(), s.m.bytes; got != want {
					t.Fatalf("Bytes = %d, model %d", got, want)
				}
				gotDeadline, gotTimed := s.b.Deadline()
				if gotTimed != s.m.timed {
					t.Fatalf("Deadline timed = %v, model %v", gotTimed, s.m.timed)
				}
				if gotTimed && !gotDeadline.Equal(s.m.deadline) {
					t.Fatalf("deadline %v, model %v", gotDeadline, s.m.deadline)
				}
			}},
		},
		Check: func(t testing.TB, s *fuzzState) {
			// The batcher and the model hold the same pending batch, and
			// no item is lost: what has come out is what went in, in
			// order. (A count threshold is a trigger, not a cap: a caller
			// that ignores a ready batch keeps filling it.)
			if got, want := s.b.Len(), len(s.m.items); got != want {
				t.Fatalf("Len = %d, model %d", got, want)
			}
			if got, want := s.b.Bytes(), s.m.bytes; got != want {
				t.Fatalf("Bytes = %d, model %d", got, want)
			}
			for i, v := range s.out {
				if v != i {
					t.Fatalf("output item %d is %d: order or conservation broken", i, v)
				}
			}
		},
		MaxSteps: 200,
	}
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), ki)
		w.Int(0, 64, 4)
		w.Int(0, 1<<20, 256)
		w.Duration(time.Second, 5*time.Millisecond)
		for range 5 {
			script.WriteStep(w, "add")
			w.Int(-2, 1024, 32)
		}
		script.WriteStep(w, "takeready")
		script.WriteStep(w, "advance")
		w.Duration(50*time.Millisecond, 10*time.Millisecond)
		script.WriteStep(w, "takeready")
		script.WriteStep(w, "inspect")
		script.WriteStep(w, "flush")
		script.WriteStep(w, "close")
		script.WriteStep(w, "add")
		w.Int(-2, 1024, 1)
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		cfg := Config{
			MaxMessages: r.Int(0, 64),
			MaxBytes:    r.Int(0, 1<<20),
			Linger:      r.Duration(time.Second),
		}
		clk := clock.NewFake(modelStart)
		cfg.Clock = clk
		b := New[int](k, cfg)
		defer func() { _ = b.Close() }()
		s := &fuzzState{
			b:   b,
			clk: clk,
			m: &batchModel{
				kind:   k,
				cfg:    cfg,
				period: tickPeriod(cfg.Linger),
				epoch:  modelStart,
			},
		}
		script.Run(t, data[len(data)-r.Remaining():], s)
		// Nothing is stranded: a final flush plus what was taken accounts
		// for every item that was accepted.
		s.out = b.Flush(s.out)
		if len(s.out) != s.added {
			t.Fatalf("%d items accepted, %d returned", s.added, len(s.out))
		}
	})
}
