package settlement

import (
	"errors"
	"math"
	"runtime"
	"sync"
	"testing"
	"unsafe"

	"github.com/iqhive/iqmq/internal/errcode"
)

// set stores an arbitrary word for transition-matrix tests.
func (c *Cell) set(st, pend State, gen Gen) { c.w.Store(pack(st, pend, gen)) }

// load3 returns state, pending target and generation.
func (c *Cell) load3() (State, State, Gen) { return unpack(c.w.Load()) }

func TestZeroValue(t *testing.T) {
	var c Cell
	if st, gen := c.Load(); st != Idle || gen != 0 {
		t.Fatalf("zero cell = %v/%d", st, gen)
	}
	if err := c.Begin(0); !errors.Is(err, errcode.ErrLeaseLost) {
		t.Fatalf("Begin on zero cell = %v", err)
	}
	if err := c.Retry(0); !errors.Is(err, errcode.ErrLeaseLost) {
		t.Fatalf("Retry on zero cell = %v", err)
	}
	if c.Commit(0) || c.Ambiguous(0) || c.Revoke(0) || c.Close(0) {
		t.Fatal("terminal op on zero cell succeeded")
	}
	if st, ok := c.Reject(0); ok || st != Idle {
		t.Fatalf("Reject on zero cell = %v/%v", st, ok)
	}
	if unsafe.Sizeof(c) != 8 {
		t.Fatalf("Cell is %d bytes, want 8", unsafe.Sizeof(c))
	}
}

func TestLifecycle(t *testing.T) {
	var c Cell
	g, ok := c.Lease()
	if !ok || g != 1 {
		t.Fatalf("Lease = %d/%v", g, ok)
	}
	if _, ok := c.Lease(); ok {
		t.Fatal("Lease while active succeeded")
	}
	if err := c.Begin(0); !errors.Is(err, errcode.ErrLeaseLost) {
		t.Fatalf("Begin(0) = %v", err)
	}
	if err := c.Begin(2); !errors.Is(err, errcode.ErrLeaseLost) {
		t.Fatalf("Begin(2) = %v", err)
	}
	if err := c.Begin(1); err != nil {
		t.Fatalf("Begin(1) = %v", err)
	}
	if err := c.Begin(1); !errors.Is(err, errcode.ErrSettlementInProgress) {
		t.Fatalf("Begin while settling = %v", err)
	}
	if _, ok := c.Lease(); ok {
		t.Fatal("Lease while settling succeeded")
	}
	if c.Commit(0) {
		t.Fatal("Commit with stale gen succeeded")
	}
	if !c.Commit(1) {
		t.Fatal("Commit failed")
	}
	if c.Commit(1) {
		t.Fatal("second Commit succeeded")
	}
	if err := c.Begin(1); !errors.Is(err, errcode.ErrAlreadySettled) {
		t.Fatalf("Begin after settle = %v", err)
	}
	if err := c.Retry(1); !errors.Is(err, errcode.ErrAlreadySettled) {
		t.Fatalf("Retry after settle = %v", err)
	}
	// Pool reuse: the cell is re-leased, the old handle is stale.
	if g, ok := c.Lease(); !ok || g != 2 {
		t.Fatalf("second Lease = %d/%v", g, ok)
	}
	if err := c.Begin(1); !errors.Is(err, errcode.ErrLeaseLost) {
		t.Fatalf("stale Begin after reuse = %v", err)
	}
	// Definite rejection: back to active, retry allowed.
	if err := c.Begin(2); err != nil {
		t.Fatal(err)
	}
	if st, ok := c.Reject(2); !ok || st != Active {
		t.Fatalf("Reject = %v/%v", st, ok)
	}
	if st, ok := c.Reject(2); ok || st != Active {
		t.Fatalf("Reject while active = %v/%v", st, ok)
	}
	// Ambiguous: sticky until Retry.
	if err := c.Begin(2); err != nil {
		t.Fatal(err)
	}
	if !c.Ambiguous(2) {
		t.Fatal("Ambiguous failed")
	}
	if err := c.Begin(2); !errors.Is(err, errcode.ErrSettlementUnknown) {
		t.Fatalf("Begin on unknown = %v", err)
	}
	if c.Revoke(2) || c.Close(2) {
		t.Fatal("Revoke/Close on unknown succeeded")
	}
	if st, _ := c.Load(); st != Unknown {
		t.Fatalf("unknown not sticky: %v", st)
	}
	if err := c.Retry(2); err != nil {
		t.Fatalf("Retry on unknown = %v", err)
	}
	if !c.Commit(2) {
		t.Fatal("Commit after retry failed")
	}
	// Close while active.
	if g, ok := c.Lease(); !ok || g != 3 {
		t.Fatalf("third Lease = %d/%v", g, ok)
	}
	if c.Close(2) {
		t.Fatal("Close with stale gen succeeded")
	}
	if !c.Close(3) {
		t.Fatal("Close failed")
	}
	if err := c.Begin(3); !errors.Is(err, errcode.ErrClosed) {
		t.Fatalf("Begin after close = %v", err)
	}
	if err := c.Retry(3); !errors.Is(err, errcode.ErrClosed) {
		t.Fatalf("Retry after close = %v", err)
	}
	if c.Close(3) || c.Revoke(3) {
		t.Fatal("Close/Revoke on closed succeeded")
	}
	// Revoke while active.
	if g, ok := c.Lease(); !ok || g != 4 {
		t.Fatalf("fourth Lease = %d/%v", g, ok)
	}
	if c.Revoke(3) {
		t.Fatal("Revoke with stale gen succeeded")
	}
	if !c.Revoke(4) {
		t.Fatal("Revoke failed")
	}
	if st, gen := c.Load(); st != Idle || gen != 4 {
		t.Fatalf("after revoke = %v/%d", st, gen)
	}
	if err := c.Begin(4); !errors.Is(err, errcode.ErrLeaseLost) {
		t.Fatalf("Begin after revoke = %v", err)
	}
	if c.Revoke(4) {
		t.Fatal("second Revoke succeeded")
	}
}

// TestPendingRevocation: a revoke or close during a settlement in flight is
// recorded; Commit and Ambiguous stand, Reject lands in idle or closed, and
// close takes precedence over revoke in either order.
func TestPendingRevocation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ops   []func(*Cell, Gen) bool
		pend  State
		final State
	}{
		{"revoke", []func(*Cell, Gen) bool{(*Cell).Revoke}, Idle, Idle},
		{"close", []func(*Cell, Gen) bool{(*Cell).Close}, Closed, Closed},
		{"revoke-close", []func(*Cell, Gen) bool{(*Cell).Revoke, (*Cell).Close}, Closed, Closed},
		{"close-revoke", []func(*Cell, Gen) bool{(*Cell).Close, (*Cell).Revoke}, Closed, Closed},
		{"revoke-revoke", []func(*Cell, Gen) bool{(*Cell).Revoke, (*Cell).Revoke}, Idle, Idle},
		{"close-close", []func(*Cell, Gen) bool{(*Cell).Close, (*Cell).Close}, Closed, Closed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c Cell
			g, _ := c.Lease()
			if err := c.Begin(g); err != nil {
				t.Fatal(err)
			}
			for _, op := range tc.ops {
				if op(&c, g) {
					t.Fatal("revoke/close during settlement claimed the lease")
				}
			}
			if st, pend, gen := c.load3(); st != Settling || pend != tc.pend || gen != g {
				t.Fatalf("pending word = %v/%v/%d, want settling/%v/%d", st, pend, gen, tc.pend, g)
			}
			if err := c.Begin(g); !errors.Is(err, errcode.ErrSettlementInProgress) {
				t.Fatalf("Begin with pending = %v", err)
			}
			st, ok := c.Reject(g)
			if !ok || st != tc.final {
				t.Fatalf("Reject = %v/%v, want %v", st, ok, tc.final)
			}
			if st, pend, _ := c.load3(); st != tc.final || pend != Active {
				t.Fatalf("after reject = %v/%v", st, pend)
			}
			want := errcode.ErrLeaseLost
			if tc.final == Closed {
				want = errcode.ErrClosed
			}
			if err := c.Begin(g); !errors.Is(err, want) {
				t.Fatalf("Begin after landed reject = %v, want %v", err, want)
			}
		})
	}
	for name, resolve := range map[string]func(*Cell, Gen) bool{"commit": (*Cell).Commit, "ambiguous": (*Cell).Ambiguous} {
		t.Run(name+"-stands", func(t *testing.T) {
			var c Cell
			g, _ := c.Lease()
			_ = c.Begin(g)
			c.Revoke(g)
			c.Close(g)
			if !resolve(&c, g) {
				t.Fatal("resolve with pending failed")
			}
			st, pend, _ := c.load3()
			if pend != Active || (name == "commit" && st != Settled) || (name == "ambiguous" && st != Unknown) {
				t.Fatalf("after resolve = %v/%v", st, pend)
			}
		})
	}
}

// TestTransitionMatrix checks Begin's error for every (state, gen) pair and
// that the failing operations never change the word.
func TestTransitionMatrix(t *testing.T) {
	type want struct {
		begin, retry error
	}
	table := map[State]want{
		Idle:     {errcode.ErrLeaseLost, errcode.ErrLeaseLost},
		Active:   {nil, nil},
		Settling: {errcode.ErrSettlementInProgress, errcode.ErrSettlementInProgress},
		Settled:  {errcode.ErrAlreadySettled, errcode.ErrAlreadySettled},
		Unknown:  {errcode.ErrSettlementUnknown, nil},
		Closed:   {errcode.ErrClosed, errcode.ErrClosed},
	}
	for st, w := range table {
		for _, gen := range []Gen{1, 7, math.MaxUint32} {
			for _, stale := range []Gen{0, 1, 2, math.MaxUint32} {
				var c Cell
				c.set(st, Active, gen)
				before := c.w.Load()
				err := c.Begin(stale)
				switch {
				case stale != gen && !errors.Is(err, errcode.ErrLeaseLost):
					t.Errorf("%v gen=%d Begin(%d) = %v, want ErrLeaseLost", st, gen, stale, err)
				case stale == gen && !errors.Is(err, w.begin):
					t.Errorf("%v Begin(%d) = %v, want %v", st, gen, err, w.begin)
				}
				if err != nil && c.w.Load() != before {
					t.Errorf("%v: failed Begin changed the word", st)
				}
				c.set(st, Active, gen)
				err = c.Retry(stale)
				switch {
				case stale != gen && !errors.Is(err, errcode.ErrLeaseLost):
					t.Errorf("%v gen=%d Retry(%d) = %v, want ErrLeaseLost", st, gen, stale, err)
				case stale == gen && !errors.Is(err, w.retry):
					t.Errorf("%v Retry(%d) = %v, want %v", st, gen, err, w.retry)
				}
				if err == nil {
					if got, _, g := c.load3(); got != Settling || g != gen {
						t.Errorf("%v: successful Retry left %v/%d", st, got, g)
					}
				} else if c.w.Load() != before {
					t.Errorf("%v: failed Retry changed the word", st)
				}
				// Commit, Ambiguous, Reject succeed only from Settling under gen.
				for name, op := range map[string]func(Gen) bool{"commit": c.Commit, "ambiguous": c.Ambiguous, "reject": func(g Gen) bool { _, ok := c.Reject(g); return ok }} {
					c.set(st, Active, gen)
					before := c.w.Load()
					ok := op(stale)
					if want := st == Settling && stale == gen; ok != want {
						t.Errorf("%v gen=%d %s(%d) = %v, want %v", st, gen, name, stale, ok, want)
					}
					if !ok && c.w.Load() != before {
						t.Errorf("%v: failed %s changed the word", st, name)
					}
				}
				// Revoke and Close claim only Active under gen.
				for name, op := range map[string]func(Gen) bool{"revoke": c.Revoke, "close": c.Close} {
					c.set(st, Active, gen)
					before := c.w.Load()
					ok := op(stale)
					if want := st == Active && stale == gen; ok != want {
						t.Errorf("%v gen=%d %s(%d) = %v, want %v", st, gen, name, stale, ok, want)
					}
					if !ok && st != Settling && c.w.Load() != before {
						t.Errorf("%v: failed %s changed the word", st, name)
					}
				}
			}
		}
	}
}

func TestLeaseSkipsZeroOnWrap(t *testing.T) {
	var c Cell
	c.set(Settled, Active, math.MaxUint32)
	if g, ok := c.Lease(); !ok || g != 1 {
		t.Fatalf("Lease at wrap = %d/%v", g, ok)
	}
	for _, st := range []State{Idle, Settled, Unknown, Closed} {
		c.set(st, Active, 5)
		if g, ok := c.Lease(); !ok || g != 6 {
			t.Fatalf("Lease from %v = %d/%v", st, g, ok)
		}
		if got, pend, _ := c.load3(); got != Active || pend != Active {
			t.Fatalf("Lease from %v left %v/%v", st, got, pend)
		}
	}
}

func TestPackUnpack(t *testing.T) {
	for st := Idle; st < stateCount; st++ {
		for _, pend := range []State{Active, Idle, Closed} {
			for _, gen := range []Gen{0, 1, 1 << 31, math.MaxUint32} {
				s, p, g := unpack(pack(st, pend, gen))
				if s != st || p != pend || g != gen {
					t.Fatalf("pack/unpack(%v,%v,%d) = %v/%v/%d", st, pend, gen, s, p, g)
				}
			}
		}
	}
	// An undefined pending target packs as "nothing pending".
	if _, p, _ := unpack(pack(Settling, Settled, 1)); p != Active {
		t.Fatalf("undefined pending decoded as %v", p)
	}
	if State(200).String() != "state(?)" || State(200).Valid() || !Closed.Valid() || Settling.String() != "settling" {
		t.Fatal("State.String/Valid")
	}
}

func TestToken(t *testing.T) {
	tok := NewToken(0xdeadbeef, 0x01020304)
	if tok.Slot() != 0xdeadbeef || tok.Gen() != 0x01020304 {
		t.Fatalf("token = %x/%x", tok.Slot(), tok.Gen())
	}
	if Token(0).Slot() != 0 || Token(0).Gen() != 0 {
		t.Fatal("zero token")
	}
	// The zero token never settles a leased cell.
	cells := make([]Cell, 1)
	g, _ := cells[0].Lease()
	if err := cells[Token(0).Slot()].Begin(Token(0).Gen()); !errors.Is(err, errcode.ErrLeaseLost) {
		t.Fatalf("zero token Begin = %v", err)
	}
	if err := cells[NewToken(0, g).Slot()].Begin(NewToken(0, g).Gen()); err != nil {
		t.Fatalf("live token Begin = %v", err)
	}
}

// TestOneWinnerRace races many goroutines on one delivery: exactly one
// Begin wins, every loser sees ErrSettlementInProgress or, once the winner
// committed, ErrAlreadySettled; nothing else.
func TestOneWinnerRace(t *testing.T) {
	for round := range 200 {
		var c Cell
		g, _ := c.Lease()
		const n = 32
		results := make([]error, n)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				err := c.Begin(g)
				if err == nil {
					if !c.Commit(g) {
						err = errors.New("winner could not commit")
					}
				}
				results[i] = err
			}()
		}
		close(start)
		wg.Wait()
		winners := 0
		for _, err := range results {
			switch {
			case err == nil:
				winners++
			case errors.Is(err, errcode.ErrSettlementInProgress), errors.Is(err, errcode.ErrAlreadySettled):
			default:
				t.Fatalf("round %d: unexpected result %v", round, err)
			}
		}
		if winners != 1 {
			t.Fatalf("round %d: %d winners", round, winners)
		}
		if st, gen := c.Load(); st != Settled || gen != g {
			t.Fatalf("round %d: final %v/%d", round, st, gen)
		}
	}
}

// TestRevokeRace races the lease-expiry path against a settler. The legal
// outcomes: the revoke claims the lease first and Begin sees ErrLeaseLost;
// Begin wins, commits, and the revoke finds nothing to claim; Begin wins,
// the revoke is recorded as pending and the definite Reject lands in idle;
// or Begin wins, rejects back to active, and only then the revoke claims.
// A settlement of the claimed generation never succeeds afterwards.
func TestRevokeRace(t *testing.T) {
	var lostFirst, settled, rejectedIdle, rejectedThenLost int
	for round := range 500 {
		var c Cell
		g, _ := c.Lease()
		var wg sync.WaitGroup
		var claimed bool
		var beginErr error
		var commitOK bool
		var rejectSt State
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			claimed = c.Revoke(g)
		}()
		go func() {
			defer wg.Done()
			<-start
			beginErr = c.Begin(g)
			if beginErr == nil {
				runtime.Gosched() // widen the window for a pending revoke
				if round%2 == 0 {
					commitOK = c.Commit(g)
				} else {
					rejectSt, _ = c.Reject(g)
				}
			}
		}()
		close(start)
		wg.Wait()
		st, _ := c.Load()
		switch {
		case claimed && beginErr != nil:
			lostFirst++
			if !errors.Is(beginErr, errcode.ErrLeaseLost) || st != Idle {
				t.Fatalf("revoke claimed first but Begin = %v, state %v", beginErr, st)
			}
		case claimed:
			rejectedThenLost++
			if commitOK || rejectSt != Active || st != Idle {
				t.Fatalf("revoke claimed after a settlement: commit=%v reject=%v state %v", commitOK, rejectSt, st)
			}
		case beginErr != nil:
			t.Fatalf("neither revoke nor Begin won: %v", beginErr)
		case commitOK:
			settled++
			if st != Settled {
				t.Fatalf("committed but state %v", st)
			}
		case rejectSt == Idle:
			rejectedIdle++
			if st != Idle {
				t.Fatalf("reject landed idle but state %v", st)
			}
		default:
			t.Fatalf("revoke did not claim, yet reject landed %v (state %v)", rejectSt, st)
		}
		if err := c.Begin(g); err == nil {
			t.Fatal("a settlement of the raced generation won afterwards")
		}
	}
	t.Logf("lost-first=%d settled=%d rejected-idle=%d rejected-then-lost=%d", lostFirst, settled, rejectedIdle, rejectedThenLost)
}

// TestAllocs: the whole lifecycle and every error path allocate nothing.
func TestAllocs(t *testing.T) {
	var c Cell
	stale := Gen(0)
	allocs := testing.AllocsPerRun(1000, func() {
		g, ok := c.Lease()
		if !ok {
			t.Fatal("lease")
		}
		if err := c.Begin(stale); err == nil {
			t.Fatal("stale begin won")
		}
		if err := c.Begin(g); err != nil {
			t.Fatal(err)
		}
		if err := c.Begin(g); err == nil {
			t.Fatal("second begin won")
		}
		if _, ok := c.Reject(g); !ok {
			t.Fatal("reject")
		}
		if err := c.Retry(g); err != nil {
			t.Fatal(err)
		}
		if !c.Ambiguous(g) {
			t.Fatal("ambiguous")
		}
		if err := c.Begin(g); err == nil {
			t.Fatal("begin on unknown won")
		}
		if err := c.Retry(g); err != nil {
			t.Fatal(err)
		}
		c.Revoke(g)
		c.Close(g)
		if !c.Commit(g) {
			t.Fatal("commit")
		}
		if err := c.Begin(g); err == nil {
			t.Fatal("begin after settle won")
		}
		stale = g
	})
	if allocs != 0 {
		t.Fatalf("%.2f allocs per lifecycle", allocs)
	}
}
