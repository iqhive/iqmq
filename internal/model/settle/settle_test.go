package settle

import (
	"errors"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

func step(t *testing.T, m *Model, op Op) Result {
	t.Helper()
	res := m.Step(op)
	if err := m.Invariants(); err != nil {
		t.Fatalf("after %v on cell %d: %v", op.Kind, op.Cell, err)
	}
	return res
}

func wantErr(t *testing.T, res Result, want error) {
	t.Helper()
	if res.OK || !errors.Is(res.Err, want) {
		t.Fatalf("result %+v, want error %v", res, want)
	}
}

// TestScenarios walks the specification rules one by one.
func TestScenarios(t *testing.T) {
	m := New(1)
	// Zero cell: nothing to settle.
	wantErr(t, step(t, m, Op{Kind: Begin, Gen: 0}), errcode.ErrLeaseLost)
	// Lease hands out generation 1.
	res := step(t, m, Op{Kind: Lease})
	if !res.OK || res.Gen != 1 || res.State != Active {
		t.Fatalf("lease: %+v", res)
	}
	// A second lease while active fails.
	if res := step(t, m, Op{Kind: Lease}); res.OK {
		t.Fatal("lease while active succeeded")
	}
	// Stale, one winner, in progress, already settled.
	wantErr(t, step(t, m, Op{Kind: Begin, Gen: 0}), errcode.ErrLeaseLost)
	wantErr(t, step(t, m, Op{Kind: Begin, Gen: 2}), errcode.ErrLeaseLost)
	if res := step(t, m, Op{Kind: Begin, Racer: 7, Gen: 1}); !res.OK || res.Err != nil || res.State != Settling {
		t.Fatalf("begin: %+v", res)
	}
	wantErr(t, step(t, m, Op{Kind: Begin, Racer: 8, Gen: 1}), errcode.ErrSettlementInProgress)
	wantErr(t, step(t, m, Op{Kind: Retry, Racer: 8, Gen: 1}), errcode.ErrSettlementInProgress)
	if res := step(t, m, Op{Kind: Commit, Gen: 0}); res.OK {
		t.Fatal("commit with stale gen succeeded")
	}
	if res := step(t, m, Op{Kind: Commit, Gen: 1}); !res.OK || res.State != Settled {
		t.Fatalf("commit: %+v", res)
	}
	wantErr(t, step(t, m, Op{Kind: Begin, Gen: 1}), errcode.ErrAlreadySettled)
	wantErr(t, step(t, m, Op{Kind: Retry, Gen: 1}), errcode.ErrAlreadySettled)
	if res := step(t, m, Op{Kind: Commit, Gen: 1}); res.OK {
		t.Fatal("second commit succeeded")
	}
	d, err := m.Delivery(0)
	if err != nil || len(d.Winners) != 1 || d.Winners[0] != (Winner{Gen: 1, Racer: 7}) {
		t.Fatalf("winners = %+v err %v", d.Winners, err)
	}
	// Pool reuse: lease again, old handle is stale.
	if res := step(t, m, Op{Kind: Lease}); !res.OK || res.Gen != 2 {
		t.Fatalf("release: %+v", res)
	}
	wantErr(t, step(t, m, Op{Kind: Begin, Gen: 1}), errcode.ErrLeaseLost)
	// Definite rejection returns to active; retry allowed.
	step(t, m, Op{Kind: Begin, Gen: 2})
	if res := step(t, m, Op{Kind: Reject, Gen: 2}); !res.OK || res.State != Active {
		t.Fatalf("reject: %+v", res)
	}
	// Ambiguous is sticky until Retry.
	step(t, m, Op{Kind: Begin, Gen: 2})
	if res := step(t, m, Op{Kind: Ambiguous, Gen: 2}); !res.OK || res.State != Unknown {
		t.Fatalf("ambiguous: %+v", res)
	}
	wantErr(t, step(t, m, Op{Kind: Begin, Gen: 2}), errcode.ErrSettlementUnknown)
	if res := step(t, m, Op{Kind: Revoke, Gen: 2}); res.OK || res.State != Unknown {
		t.Fatalf("revoke on unknown: %+v", res)
	}
	if res := step(t, m, Op{Kind: Close, Gen: 2}); res.OK || res.State != Unknown {
		t.Fatalf("close on unknown: %+v", res)
	}
	if res := step(t, m, Op{Kind: Retry, Gen: 2}); !res.OK || res.State != Settling {
		t.Fatalf("retry: %+v", res)
	}
	step(t, m, Op{Kind: Commit, Gen: 2})
	// Close while active, then the handle sees ErrClosed; a new lease works.
	step(t, m, Op{Kind: Lease})
	if res := step(t, m, Op{Kind: Close, Gen: 2}); res.OK {
		t.Fatal("close with stale gen succeeded")
	}
	if res := step(t, m, Op{Kind: Close, Gen: 3}); !res.OK || res.State != Closed {
		t.Fatalf("close: %+v", res)
	}
	wantErr(t, step(t, m, Op{Kind: Begin, Gen: 3}), errcode.ErrClosed)
	if res := step(t, m, Op{Kind: Close, Gen: 3}); res.OK {
		t.Fatal("second close succeeded")
	}
	if res := step(t, m, Op{Kind: Lease}); !res.OK || res.Gen != 4 {
		t.Fatalf("lease after close: %+v", res)
	}
	// Revoke while active: idle, handle sees ErrLeaseLost even with the right gen.
	if res := step(t, m, Op{Kind: Revoke, Gen: 4}); !res.OK || res.State != Idle {
		t.Fatalf("revoke: %+v", res)
	}
	wantErr(t, step(t, m, Op{Kind: Begin, Gen: 4}), errcode.ErrLeaseLost)
	if res := step(t, m, Op{Kind: Revoke, Gen: 4}); res.OK {
		t.Fatal("second revoke succeeded")
	}
}

// TestPendingRevocation checks that a revoke or close meeting a settlement in
// flight lands the eventual definite rejection in idle or closed, that close
// takes precedence, and that commit and ambiguous stand.
func TestPendingRevocation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ops   []Kind
		final State
	}{
		{"revoke", []Kind{Revoke}, Idle},
		{"close", []Kind{Close}, Closed},
		{"revoke-then-close", []Kind{Revoke, Close}, Closed},
		{"close-then-revoke", []Kind{Close, Revoke}, Closed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := New(1)
			step(t, m, Op{Kind: Lease})
			step(t, m, Op{Kind: Begin, Gen: 1})
			for _, k := range tc.ops {
				if res := step(t, m, Op{Kind: k, Gen: 1}); res.OK || res.State != Settling {
					t.Fatalf("%v while settling: %+v", k, res)
				}
			}
			if res := step(t, m, Op{Kind: Reject, Gen: 1}); !res.OK || res.State != tc.final {
				t.Fatalf("reject landed in %+v, want %v", res, tc.final)
			}
			d, _ := m.Delivery(0)
			if d.Pending != Active {
				t.Fatalf("pending not cleared: %+v", d)
			}
		})
	}
	for _, k := range []Kind{Commit, Ambiguous} {
		m := New(1)
		step(t, m, Op{Kind: Lease})
		step(t, m, Op{Kind: Begin, Gen: 1})
		step(t, m, Op{Kind: Revoke, Gen: 1})
		res := step(t, m, Op{Kind: k, Gen: 1})
		want := Settled
		if k == Ambiguous {
			want = Unknown
		}
		if !res.OK || res.State != want {
			t.Fatalf("%v with pending revoke: %+v", k, res)
		}
	}
}

func TestGenWrapSkipsZero(t *testing.T) {
	m := New(1)
	m.cells[0].Gen = math.MaxUint32
	m.prev[0].Gen = math.MaxUint32
	if res := step(t, m, Op{Kind: Lease}); !res.OK || res.Gen != 1 {
		t.Fatalf("lease at wrap: %+v", res)
	}
}

func TestAdvanceAndSnapshot(t *testing.T) {
	m := New(2)
	m.Advance(time.Second)
	m.Advance(-time.Second)
	if m.Now() != time.Second {
		t.Fatalf("Now = %v", m.Now())
	}
	step(t, m, Op{Kind: Lease, Cell: 1})
	s := m.Snapshot()
	if len(s.Cells) != 2 || s.Cells[1].Gen != 1 || s.Steps != 1 || s.Now != time.Second {
		t.Fatalf("snapshot = %+v", s)
	}
	s.Cells[1].Gen = 99
	if d, _ := m.Delivery(1); d.Gen != 1 {
		t.Fatal("snapshot aliases model state")
	}
	if _, err := m.Delivery(5); !errors.Is(err, ErrNoSuchCell) {
		t.Fatalf("Delivery(5) err = %v", err)
	}
	if m.Cells() != 2 || m.Steps() != 1 {
		t.Fatalf("Cells=%d Steps=%d", m.Cells(), m.Steps())
	}
}

func TestNames(t *testing.T) {
	if Idle.String() != "idle" || Closed.String() != "closed" || State(200).String() != "state(?)" {
		t.Fatal("State.String")
	}
	if Lease.String() != "lease" || Close.String() != "close" || Kind(200).String() != "kind(?)" {
		t.Fatal("Kind.String")
	}
}

func TestHarnessErrors(t *testing.T) {
	m := New(1)
	for _, op := range []Op{{Kind: Lease, Cell: 1}, {Kind: Lease, Cell: -1}, {Kind: kindCount}} {
		if res := m.Step(op); res.Err == nil {
			t.Errorf("Step(%+v) did not fail", op)
		}
	}
}

// randomOp draws an operation biased towards live generations.
func randomOp(r *fuzzutil.Reader, m *Model) Op {
	op := Op{Kind: Kind(r.Choice(int(kindCount))), Cell: r.Choice(m.Cells()), Racer: r.Choice(3)}
	d, _ := m.Delivery(op.Cell)
	switch r.Choice(4) {
	case 0:
		op.Gen = d.Gen - 1
	case 1:
		op.Gen = uint32(r.Byte())
	default:
		op.Gen = d.Gen
	}
	return op
}

// TestRandomInvariants drives the model with random operations; the
// invariants must hold after every step and the model must stay total.
func TestRandomInvariants(t *testing.T) {
	rng := rand.New(rand.NewPCG(10, 20))
	data := make([]byte, 4096)
	for run := range 50 {
		for i := range data {
			data[i] = byte(rng.IntN(256))
		}
		m := New(1 + run%3)
		r := fuzzutil.NewReader(data)
		for !r.Exhausted() {
			op := randomOp(r, m)
			m.Step(op)
			if err := m.Invariants(); err != nil {
				t.Fatalf("run %d step %d (%v): %v", run, m.Steps(), op.Kind, err)
			}
		}
	}
}

// FuzzSettleModel feeds fuzzer-chosen operation streams to the model; any
// invariant violation is a bug in the model itself.
func FuzzSettleModel(f *testing.F) {
	w := fuzzutil.NewWriter()
	w.Int(1, 4, 2)
	for _, k := range []Kind{Lease, Begin, Commit, Lease, Begin, Revoke, Reject, Lease, Close} {
		w.Choice(int(kindCount), int(k))
		w.Choice(2, 0)
		w.Choice(3, 0)
		w.Choice(4, 3)
	}
	f.Add(w.Data())
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		m := New(r.Int(1, 4))
		for i := 0; i < 512 && !r.Exhausted(); i++ {
			op := randomOp(r, m)
			m.Step(op)
			if err := m.Invariants(); err != nil {
				t.Fatalf("step %d (%v): %v", m.Steps(), op.Kind, err)
			}
		}
	})
}
