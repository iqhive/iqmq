package settlement

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
	"bitbucket.org/iqhive/iqmq/internal/model/settle"
)

// maxRacers is the number of handles the differential script keeps.
const maxRacers = 4

// handle is a racer's settlement handle: a cell index and the generation it
// believes it holds.
type handle struct {
	cell int
	gen  Gen
}

// harness pairs a table of cells with the reference model and the racers'
// handles. Both sides see identical operations.
type harness struct {
	cells []Cell
	model *settle.Model
	tok   [maxRacers]handle
}

func newHarness(cells int) *harness {
	return &harness{cells: make([]Cell, cells), model: settle.New(cells)}
}

// staleGen picks a generation for the "stale" variants: one behind, one
// ahead, or arbitrary.
func staleGen(r *fuzzutil.Reader, cur Gen) Gen {
	switch r.Choice(3) {
	case 0:
		return cur - 1
	case 1:
		return cur + 1
	default:
		return Gen(r.Byte())
	}
}

// expectErr compares a Begin/Retry result with the model's.
func expectErr(t testing.TB, op string, err error, res settle.Result) {
	t.Helper()
	if (err == nil) != (res.Err == nil) || (err != nil && !errors.Is(err, res.Err)) {
		t.Fatalf("%s: implementation %v, model %v", op, err, res.Err)
	}
	if err == nil && !res.OK {
		t.Fatalf("%s: won but the model says lost", op)
	}
}

func expectOK(t testing.TB, op string, ok bool, res settle.Result) {
	t.Helper()
	if ok != res.OK {
		t.Fatalf("%s: implementation %v, model %v", op, ok, res.OK)
	}
}

// settlementScript is the op-script differential between Cell and the model.
func settlementScript() *fuzzutil.Script[*harness] {
	begin := func(kind settle.Kind) func(testing.TB, *fuzzutil.Reader, *harness) {
		return func(t testing.TB, r *fuzzutil.Reader, h *harness) {
			rc := r.Choice(maxRacers)
			tk := h.tok[rc]
			var err error
			if kind == settle.Begin {
				err = h.cells[tk.cell].Begin(tk.gen)
			} else {
				err = h.cells[tk.cell].Retry(tk.gen)
			}
			expectErr(t, kind.String(), err, h.model.Step(settle.Op{Kind: kind, Cell: tk.cell, Racer: rc, Gen: uint32(tk.gen)}))
		}
	}
	resolve := func(kind settle.Kind) func(testing.TB, *fuzzutil.Reader, *harness) {
		return func(t testing.TB, r *fuzzutil.Reader, h *harness) {
			tk := h.tok[r.Choice(maxRacers)]
			c := &h.cells[tk.cell]
			var ok bool
			var st State
			switch kind {
			case settle.Commit:
				ok = c.Commit(tk.gen)
			case settle.Ambiguous:
				ok = c.Ambiguous(tk.gen)
			default:
				st, ok = c.Reject(tk.gen)
			}
			res := h.model.Step(settle.Op{Kind: kind, Cell: tk.cell, Gen: uint32(tk.gen)})
			expectOK(t, kind.String(), ok, res)
			if kind == settle.Reject && uint8(st) != uint8(res.State) {
				t.Fatalf("reject: implementation landed %v, model %v", st, res.State)
			}
		}
	}
	engine := func(kind settle.Kind) func(testing.TB, *fuzzutil.Reader, *harness) {
		return func(t testing.TB, r *fuzzutil.Reader, h *harness) {
			ci := r.Choice(len(h.cells))
			c := &h.cells[ci]
			_, g := c.Load()
			if r.Bool() {
				g = staleGen(r, g)
			}
			var ok bool
			if kind == settle.Revoke {
				ok = c.Revoke(g)
			} else {
				ok = c.Close(g)
			}
			expectOK(t, kind.String(), ok, h.model.Step(settle.Op{Kind: kind, Cell: ci, Gen: uint32(g)}))
		}
	}
	return &fuzzutil.Script[*harness]{
		Steps: []fuzzutil.Step[*harness]{
			{Name: "lease", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, h *harness) {
				ci := r.Choice(len(h.cells))
				g, ok := h.cells[ci].Lease()
				res := h.model.Step(settle.Op{Kind: settle.Lease, Cell: ci})
				expectOK(t, "lease", ok, res)
				if ok && uint32(g) != res.Gen {
					t.Fatalf("lease: implementation gen %d, model %d", g, res.Gen)
				}
			}},
			{Name: "receive", Weight: 4, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				rc, ci := r.Choice(maxRacers), r.Choice(len(h.cells))
				_, g := h.cells[ci].Load()
				h.tok[rc] = handle{cell: ci, gen: g}
			}},
			{Name: "forge", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				rc, ci := r.Choice(maxRacers), r.Choice(len(h.cells))
				_, g := h.cells[ci].Load()
				h.tok[rc] = handle{cell: ci, gen: staleGen(r, g)}
			}},
			{Name: "begin", Weight: 4, Run: begin(settle.Begin)},
			{Name: "retry", Weight: 1, Run: begin(settle.Retry)},
			{Name: "commit", Weight: 2, Run: resolve(settle.Commit)},
			{Name: "reject", Weight: 2, Run: resolve(settle.Reject)},
			{Name: "ambiguous", Weight: 1, Run: resolve(settle.Ambiguous)},
			{Name: "revoke", Weight: 1, Run: engine(settle.Revoke)},
			{Name: "close", Weight: 1, Run: engine(settle.Close)},
		},
		MaxSteps: 1024,
		Check: func(t testing.TB, h *harness) {
			if err := h.model.Invariants(); err != nil {
				t.Fatalf("model invariant: %v", err)
			}
			for i := range h.cells {
				st, pend, gen := h.cells[i].load3()
				d, _ := h.model.Delivery(i)
				if uint8(st) != uint8(d.State) || uint8(pend) != uint8(d.Pending) || uint32(gen) != d.Gen {
					t.Fatalf("cell %d: implementation %v/%v/%d, model %v/%v/%d", i, st, pend, gen, d.State, d.Pending, d.Gen)
				}
			}
		},
	}
}

// settlementSeeds authors readable seed scripts.
func settlementSeeds(sc *fuzzutil.Script[*harness]) [][]byte {
	type step = func(w *fuzzutil.Writer)
	lease := func(c int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "lease"); w.Choice(1, c) }
	}
	receive := func(r, c int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "receive"); w.Choice(maxRacers, r); w.Choice(1, c) }
	}
	racer := func(name string, r int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, name); w.Choice(maxRacers, r) }
	}
	engine := func(name string, c int, stale bool) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(w, name)
			w.Choice(1, c)
			w.Bool(stale)
			if stale {
				w.Choice(3, 0)
			}
		}
	}
	scripts := []struct {
		cells int
		steps []step
	}{
		// Happy path, double ack, stale after re-lease.
		{1, []step{
			lease(0), receive(0, 0), receive(1, 0), racer("begin", 0), racer("begin", 1), racer("commit", 0),
			racer("begin", 1), lease(0), racer("begin", 0), receive(2, 0), racer("begin", 2), racer("commit", 2),
		}},
		// Reject then retry; ambiguous sticky then retry.
		{1, []step{
			lease(0), receive(0, 0), racer("begin", 0), racer("reject", 0), racer("begin", 0), racer("ambiguous", 0),
			racer("begin", 0), racer("retry", 0), racer("commit", 0),
		}},
		// Revoke while active, close while active, pending revoke during settlement.
		{1, []step{
			lease(0), receive(0, 0), engine("revoke", 0, false), racer("begin", 0), lease(0), receive(1, 0),
			engine("close", 0, false), racer("begin", 1), lease(0), receive(2, 0), racer("begin", 2),
			engine("revoke", 0, false), engine("close", 0, true), racer("reject", 2), racer("begin", 2),
		}},
	}
	seeds := make([][]byte, 0, len(scripts))
	for _, s := range scripts {
		w := fuzzutil.NewWriter()
		w.Int(1, 4, s.cells)
		for _, st := range s.steps {
			st(w)
		}
		seeds = append(seeds, w.Data())
	}
	return seeds
}

// FuzzSettlement is the sequential differential: a fuzz-chosen script of
// leases, receives, forged handles, settlement attempts, native outcomes,
// revocations and closes runs against a table of Cells and the reference
// model; every result and every cell word must agree, and the model's
// invariants must hold after every step.
func FuzzSettlement(f *testing.F) {
	sc := settlementScript()
	for _, s := range settlementSeeds(sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		h := newHarness(r.Int(1, 4))
		sc.Run(t, data[len(data)-r.Remaining():], h)
	})
}

// tracker is the concurrent fuzz target's per-cell ledger.
type tracker struct {
	inflight atomic.Int32
	mu       sync.Mutex
	commits  map[Gen]int
	lost     map[Gen]bool
}

// FuzzSettlementConcurrent races fuzz-chosen numbers of settlers against an
// engine actor that leases, revokes and closes. Checked: at most one attempt
// is in flight per cell (one winner); a winner observes its own generation
// while settling; every loser gets one of the five settlement sentinels; a
// stale generation never wins and is always ErrLeaseLost; a generation is
// committed at most once; a generation whose lease was claimed by Revoke or
// Close is never committed; and nothing is left settling.
func FuzzSettlementConcurrent(f *testing.F) {
	for _, seed := range [][3]int{{1, 2, 50}, {2, 4, 100}, {4, 8, 40}} {
		w := fuzzutil.NewWriter()
		w.Int(1, 4, seed[0])
		w.Int(1, 8, seed[1])
		w.Int(1, 200, seed[2])
		for i := range 64 {
			w.Byte(byte(i * 37))
		}
		f.Add(w.Data())
	}
	sentinel := func(err error) bool {
		return errors.Is(err, errcode.ErrLeaseLost) || errors.Is(err, errcode.ErrSettlementInProgress) ||
			errors.Is(err, errcode.ErrAlreadySettled) || errors.Is(err, errcode.ErrSettlementUnknown) ||
			errors.Is(err, errcode.ErrClosed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		ncells := r.Int(1, 4)
		racers := r.Int(1, 8)
		per := r.Int(1, 200)
		cells := make([]Cell, ncells)
		tracks := make([]tracker, ncells)
		for i := range tracks {
			tracks[i].commits = map[Gen]int{}
			tracks[i].lost = map[Gen]bool{}
		}
		var failed atomic.Bool
		var msg atomic.Pointer[string]
		fail := func(s string) {
			if failed.CompareAndSwap(false, true) {
				msg.Store(&s)
			}
		}
		fuzzutil.Actors(t, data[len(data)-r.Remaining():], racers+1, func(id int, ar *fuzzutil.Reader) {
			for i := 0; i < per && !failed.Load(); i++ {
				ci := ar.Choice(ncells)
				c, tr := &cells[ci], &tracks[ci]
				_, g := c.Load()
				if id == 0 { // engine
					switch ar.Choice(4) {
					case 0:
						c.Lease()
					case 1:
						if c.Revoke(g) {
							tr.mu.Lock()
							tr.lost[g] = true
							tr.mu.Unlock()
						}
					case 2:
						if c.Close(g) {
							tr.mu.Lock()
							tr.lost[g] = true
							tr.mu.Unlock()
						}
					default:
						if c.Revoke(g-1) || c.Close(g-1) {
							fail("stale revoke or close claimed a lease")
						}
					}
					fuzzutil.MaybeYield(ar)
					continue
				}
				switch ar.Choice(5) {
				case 0, 1:
					var err error
					if ar.Bool() {
						err = c.Begin(g)
					} else {
						err = c.Retry(g)
					}
					if err != nil {
						if !sentinel(err) {
							fail("unexpected error " + err.Error())
						}
						break
					}
					if tr.inflight.Add(1) != 1 {
						fail("two settlements in flight on one cell")
					}
					if st, g2 := c.Load(); st != Settling || g2 != g {
						fail("settling word changed under its owner")
					}
					fuzzutil.MaybeYield(ar)
					tr.inflight.Add(-1)
					switch ar.Choice(3) {
					case 0:
						if !c.Commit(g) {
							fail("winner could not commit")
						}
						tr.mu.Lock()
						tr.commits[g]++
						tr.mu.Unlock()
					case 1:
						st, ok := c.Reject(g)
						if !ok || (st != Active && st != Idle && st != Closed) {
							fail("winner could not reject or landed in " + st.String())
						}
					default:
						if !c.Ambiguous(g) {
							fail("winner could not mark ambiguous")
						}
					}
				case 2:
					stale := g - 1 - Gen(ar.Choice(3))
					if err := c.Begin(stale); !errors.Is(err, errcode.ErrLeaseLost) {
						fail("stale begin did not return ErrLeaseLost")
					}
				case 3:
					if c.Commit(g-1) || c.Ambiguous(g-1) {
						fail("stale commit succeeded")
					}
					if _, ok := c.Reject(g - 1); ok {
						fail("stale reject succeeded")
					}
				default:
				}
				fuzzutil.MaybeYield(ar)
			}
		})
		if failed.Load() {
			t.Fatalf("cells=%d racers=%d per=%d: %s", ncells, racers, per, *msg.Load())
		}
		for i := range cells {
			st, pend, _ := cells[i].load3()
			if st == Settling || pend != Active || tracks[i].inflight.Load() != 0 {
				t.Fatalf("cell %d left %v/%v with %d in flight", i, st, pend, tracks[i].inflight.Load())
			}
			for g, n := range tracks[i].commits {
				if n != 1 {
					t.Fatalf("cell %d generation %d committed %d times", i, g, n)
				}
				if tracks[i].lost[g] {
					t.Fatalf("cell %d generation %d committed after its lease was lost", i, g)
				}
			}
		}
	})
}
