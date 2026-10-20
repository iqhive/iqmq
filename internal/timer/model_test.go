package timer

import (
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
)

// ref is the oracle: the set of armed entries with their due times.
type ref struct {
	armed map[*Entry[int]]int64
}

func newRef() *ref { return &ref{armed: map[*Entry[int]]int64{}} }

// minFire returns the earliest time at which any armed entry may fire for a
// kind with resolution r, and whether any entry is armed.
func (r *ref) minFire(res time.Duration) (int64, bool) {
	best, ok := int64(0), false
	for _, due := range r.armed {
		t := due
		if res > 0 {
			t = tickTime(ceilTick(due))
		}
		if !ok || t < best {
			best, ok = t, true
		}
	}
	return best, ok
}

// checker validates a scheduler against the oracle after every operation.
type checker struct {
	t   *testing.T
	k   Kind
	s   Scheduler[int]
	ref *ref
	now int64
	dst []*Entry[int]
}

func (c *checker) add(e *Entry[int], due int64) {
	c.t.Helper()
	gen := e.Gen
	c.s.Add(e, due)
	c.ref.armed[e] = due
	if e.Gen != gen+1 || !e.Scheduled() {
		c.t.Fatalf("%v: Add gen %d -> %d scheduled=%v", c.k, gen, e.Gen, e.Scheduled())
	}
	c.checkLen()
}

func (c *checker) cancel(e *Entry[int]) {
	c.t.Helper()
	_, armed := c.ref.armed[e]
	gen := e.Gen
	got := c.s.Cancel(e)
	if got != armed {
		c.t.Fatalf("%v: Cancel = %v, oracle armed %v", c.k, got, armed)
	}
	if armed {
		delete(c.ref.armed, e)
		if e.Gen != gen+1 || e.Scheduled() {
			c.t.Fatalf("%v: Cancel gen %d -> %d", c.k, gen, e.Gen)
		}
	}
	c.checkLen()
}

func (c *checker) fire(now int64) []*Entry[int] {
	c.t.Helper()
	if now < c.now {
		now = c.now
	}
	c.now = now
	c.dst = c.s.Fire(now, c.dst[:0])
	res := int64(c.k.Resolution())
	seen := map[*Entry[int]]bool{}
	prev := int64(-1 << 62)
	for _, e := range c.dst {
		due, armed := c.ref.armed[e]
		if !armed || seen[e] {
			c.t.Fatalf("%v: fired an entry that was not armed (or twice): due=%d now=%d", c.k, e.due, now)
		}
		seen[e] = true
		if due > now {
			c.t.Fatalf("%v: fired early: due=%d now=%d", c.k, due, now)
		}
		if c.k.Ordered() && due < prev {
			c.t.Fatalf("%v: fired out of order: %d after %d", c.k, due, prev)
		}
		prev = due
		if e.Scheduled() {
			c.t.Fatalf("%v: fired entry still scheduled", c.k)
		}
		delete(c.ref.armed, e)
	}
	for e, due := range c.ref.armed {
		if due+res <= now && (res > 0 || due <= now) {
			c.t.Fatalf("%v: entry due=%d not fired by now=%d (resolution %d); gen=%d", c.k, due, now, res, e.Gen)
		}
	}
	c.checkLen()
	c.checkNext()
	return c.dst
}

func (c *checker) checkLen() {
	c.t.Helper()
	if c.s.Len() != len(c.ref.armed) {
		c.t.Fatalf("%v: Len = %d, oracle %d", c.k, c.s.Len(), len(c.ref.armed))
	}
}

// checkNext: Next is never later than the first possible firing, which for
// an entry already due is the last now (it fires at the next Fire).
func (c *checker) checkNext() {
	c.t.Helper()
	next, ok := c.s.Next()
	first, armed := c.ref.minFire(c.k.Resolution())
	if ok != armed {
		c.t.Fatalf("%v: Next ok=%v but oracle armed=%v", c.k, ok, armed)
	}
	if !ok {
		return
	}
	first = max(first, c.now)
	if next > first {
		c.t.Fatalf("%v: Next=%d later than first firing %d (now %d)", c.k, next, first, c.now)
	}
}

// progress: repeatedly firing at Next either fires or advances Next, and
// reaches the first entry within a bounded number of steps.
func (c *checker) progress() {
	c.t.Helper()
	for range 64 {
		next, ok := c.s.Next()
		if !ok {
			return
		}
		if len(c.fire(next)) > 0 {
			return
		}
		again, _ := c.s.Next()
		if again <= next {
			c.t.Fatalf("%v: Fire(Next=%d) fired nothing and Next did not advance (%d)", c.k, next, again)
		}
	}
	c.t.Fatalf("%v: no progress after 64 Fire(Next) steps", c.k)
}

func newChecker(t *testing.T, k Kind, start int64) *checker {
	return &checker{t: t, k: k, s: mustNew[int](t, k, start), ref: newRef(), now: start}
}

// TestModel drives every kind through scripted and randomised scenarios.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("basic", func(t *testing.T) { modelBasic(t, k) })
			t.Run("random", func(t *testing.T) { modelRandom(t, k, 1) })
			t.Run("random-skewed", func(t *testing.T) { modelRandom(t, k, 2) })
			t.Run("idle-gaps", func(t *testing.T) { modelIdleGaps(t, k) })
			t.Run("far-future", func(t *testing.T) { modelFarFuture(t, k) })
			t.Run("negative-origin", func(t *testing.T) { modelNegativeOrigin(t, k) })
			t.Run("mass-expiry", func(t *testing.T) { modelMassExpiry(t, k) })
			t.Run("wide", func(t *testing.T) { modelWide(t, k) })
			t.Run("churn", func(t *testing.T) { modelChurn(t, k) })
			t.Run("fake-clock", func(t *testing.T) { modelFakeClock(t, k) })
		})
	}
}

func modelBasic(t *testing.T, k Kind) {
	c := newChecker(t, k, 0)
	ms := int64(tick)
	es := make([]Entry[int], 6)
	c.add(&es[0], 30*ms)
	c.add(&es[1], 10*ms)
	c.add(&es[2], 20*ms)
	c.add(&es[3], 10*ms+1) // just past a tick boundary
	c.add(&es[4], 0)       // already due
	c.cancel(&es[2])
	c.cancel(&es[2]) // idempotent
	if got := c.fire(0); len(got) != 1 || got[0] != &es[4] {
		t.Fatalf("Fire(0) = %v", got)
	}
	if got := c.fire(10 * ms); len(got) != 1 || got[0] != &es[1] {
		t.Fatalf("Fire(10ms) fired %d entries", len(got))
	}
	c.fire(11 * ms) // es[3] must be gone by now for every kind
	if _, armed := c.ref.armed[&es[3]]; armed {
		t.Fatal("entry due 10ms+1ns not fired by 11ms")
	}
	c.progress()
	c.fire(1 << 40)
	if c.s.Len() != 0 {
		t.Fatal("not drained")
	}
	// Re-arm fired entries.
	c.add(&es[0], c.now+5*ms)
	c.add(&es[4], c.now+5*ms)
	c.progress()
	c.fire(c.now + 10*ms)
	if c.s.Len() != 0 {
		t.Fatal("re-armed entries not fired")
	}
}

func modelRandom(t *testing.T, k Kind, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, uint64(k)))
	c := newChecker(t, k, 1<<40)
	es := make([]Entry[int], 200)
	horizon := int64(5 * time.Second)
	for step := range 20000 {
		switch rng.IntN(10) {
		case 0, 1, 2, 3, 4:
			e := &es[rng.IntN(len(es))]
			if e.Scheduled() {
				continue
			}
			var d int64
			if seed == 2 && rng.IntN(5) > 0 {
				d = rng.Int64N(int64(20 * time.Millisecond)) // skewed: mostly near
			} else {
				d = rng.Int64N(horizon)
			}
			if rng.IntN(20) == 0 {
				d = -rng.Int64N(int64(time.Millisecond)) // slightly in the past
			}
			c.add(e, c.now+d)
		case 5:
			c.cancel(&es[rng.IntN(len(es))])
		case 6, 7:
			c.fire(c.now + rng.Int64N(int64(50*time.Millisecond)))
		case 8:
			c.fire(c.now + rng.Int64N(int64(2*time.Second)))
		case 9:
			c.progress()
		}
		if step%997 == 0 {
			c.checkNext()
		}
	}
	c.fire(c.now + 2*horizon)
	if c.s.Len() != 0 {
		t.Fatalf("%d entries left after draining", c.s.Len())
	}
}

func modelIdleGaps(t *testing.T, k Kind) {
	c := newChecker(t, k, 0)
	es := make([]Entry[int], 8)
	gaps := []int64{int64(time.Second), int64(time.Minute), int64(time.Hour), int64(24 * time.Hour), int64(60 * 24 * time.Hour)}
	for i, g := range gaps {
		c.add(&es[i], c.now+g)
		c.progress()
		c.fire(c.now + g)
		if c.s.Len() != 0 {
			t.Fatalf("entry at gap %v not fired", time.Duration(g))
		}
	}
}

func modelFarFuture(t *testing.T, k Kind) {
	c := newChecker(t, k, 0)
	es := make([]Entry[int], 4)
	far := []int64{int64(100 * 24 * time.Hour), int64(365 * 24 * time.Hour), int64(3 * time.Second), int64(10 * time.Millisecond)}
	for i, d := range far {
		c.add(&es[i], d)
	}
	c.progress() // fires the 10 ms entry
	if _, armed := c.ref.armed[&es[3]]; armed {
		t.Fatal("progress did not reach the 10 ms entry")
	}
	c.fire(int64(3 * time.Second))
	if c.s.Len() != 2 {
		t.Fatalf("%d entries left after firing the near ones, want the 2 far ones", c.s.Len())
	}
	c.cancel(&es[1])
	c.fire(int64(101 * 24 * time.Hour))
	if c.s.Len() != 0 {
		t.Fatalf("far entry not fired: %d left", c.s.Len())
	}
}

// modelNegativeOrigin drives a kind from a negative monotonic origin, where
// every tick a wheel kind computes is negative: a "no event" sentinel that
// hides in the sign of a tick (-1, or "< 0" as a min-selection guard) then
// swallows real entries. The origins run from far in the past to just before
// zero, so the scripted and randomised parts both cross it.
func modelNegativeOrigin(t *testing.T, k Kind) {
	ms := int64(tick)
	for _, start := range []int64{-(1 << 40), -int64(90 * 24 * time.Hour), -int64(time.Second)} {
		c := newChecker(t, k, start)
		es := make([]Entry[int], 64)
		// A single near entry must fire at the first Fire past its due.
		c.add(&es[0], c.now+5*ms)
		if got := c.fire(c.now + 10*ms); len(got) != 1 || got[0] != &es[0] {
			t.Fatalf("start=%d: Fire(now+10ms) fired %d entries, want the one due in 5 ms", start, len(got))
		}
		// One entry per level of the hierarchical wheel, reached by
		// Fire(Next) and then by jumps of growing size.
		spread := []int64{ms / 2, 2 * ms, 300 * ms, int64(time.Second), int64(time.Minute), int64(time.Hour), int64(30 * 24 * time.Hour)}
		for i, d := range spread {
			c.add(&es[i+1], c.now+d)
		}
		c.progress()
		c.checkNext()
		for _, jump := range []int64{int64(time.Second), int64(time.Hour), int64(24 * time.Hour), int64(60 * 24 * time.Hour)} {
			c.fire(c.now + jump)
		}
		if c.s.Len() != 0 {
			t.Fatalf("start=%d: %d entries left after the jumps", start, c.s.Len())
		}
		// Randomised churn from the same origin with the entries re-armed.
		rng := rand.New(rand.NewPCG(uint64(k), uint64(-start)))
		c = newChecker(t, k, start)
		for step := range 3000 {
			switch rng.IntN(8) {
			case 0, 1, 2:
				e := &es[rng.IntN(len(es))]
				if e.Scheduled() {
					continue
				}
				d := rng.Int64N(int64(2 * time.Second))
				if rng.IntN(16) == 0 {
					d = -rng.Int64N(int64(time.Millisecond)) // slightly in the past
				}
				c.add(e, c.now+d)
			case 3:
				c.cancel(&es[rng.IntN(len(es))])
			case 4, 5:
				c.fire(c.now + rng.Int64N(int64(80*time.Millisecond)))
			case 6:
				c.fire(c.now + rng.Int64N(int64(3*time.Second)))
			case 7:
				c.progress()
			}
			if step%199 == 0 {
				c.checkNext()
			}
		}
		c.fire(c.now + int64(10*time.Second))
		if c.s.Len() != 0 {
			t.Fatalf("start=%d: %d entries left after draining", start, c.s.Len())
		}
	}
}

func modelMassExpiry(t *testing.T, k Kind) {
	c := newChecker(t, k, 0)
	es := make([]Entry[int], 5000)
	same := int64(time.Second)
	for i := range es {
		c.add(&es[i], same)
	}
	if fired := c.fire(same - 1); len(fired) != 0 {
		t.Fatalf("fired %d early", len(fired))
	}
	if fired := c.fire(same); len(fired) != len(es) {
		t.Fatalf("mass expiry fired %d of %d", len(fired), len(es))
	}
}

// modelWide spreads entries over days and jumps hours at a time, so the
// hierarchical wheel cascades through its upper levels and the hashed wheel
// and calendar wrap many laps and years with entries still pending.
func modelWide(t *testing.T, k Kind) {
	rng := rand.New(rand.NewPCG(11, uint64(k)))
	c := newChecker(t, k, int64(time.Hour))
	es := make([]Entry[int], 500)
	for i := range es {
		c.add(&es[i], c.now+rng.Int64N(int64(3*24*time.Hour)))
	}
	for step := range 400 {
		switch rng.IntN(6) {
		case 0, 1:
			c.fire(c.now + rng.Int64N(int64(6*time.Hour)))
		case 2:
			c.fire(c.now + rng.Int64N(int64(500*time.Millisecond)))
		case 3:
			c.progress()
		case 4:
			c.cancel(&es[rng.IntN(len(es))])
		case 5:
			e := &es[rng.IntN(len(es))]
			if !e.Scheduled() {
				c.add(e, c.now+rng.Int64N(int64(2*24*time.Hour)))
			}
		}
		if step%50 == 0 {
			c.checkNext()
		}
	}
	c.fire(c.now + int64(10*24*time.Hour))
	if c.s.Len() != 0 {
		t.Fatalf("%d entries left after draining", c.s.Len())
	}
}

// modelChurn keeps thousands of entries in flight with mixed spacing, so
// resizing kinds grow and shrink repeatedly while the oracle watches.
func modelChurn(t *testing.T, k Kind) {
	rng := rand.New(rand.NewPCG(13, uint64(k)))
	c := newChecker(t, k, 1<<40)
	es := make([]Entry[int], 4000)
	for round := range 30 {
		for i := range es {
			if es[i].Scheduled() {
				continue
			}
			var d int64
			switch {
			case round%3 == 0:
				d = rng.Int64N(int64(2 * time.Second))
			case rng.IntN(10) > 0:
				d = rng.Int64N(int64(5 * time.Millisecond)) // dense cluster
			default:
				d = rng.Int64N(int64(time.Minute))
			}
			c.add(&es[i], c.now+d)
		}
		for range 40 {
			c.cancel(&es[rng.IntN(len(es))])
		}
		c.progress()
		c.fire(c.now + rng.Int64N(int64(300*time.Millisecond)))
		c.checkNext()
	}
	c.fire(c.now + int64(2*time.Minute))
	if c.s.Len() != 0 {
		t.Fatalf("%d entries left after draining", c.s.Len())
	}
}

// modelFakeClock drives the scheduler from a clock.Fake the way the engine
// will: sleep until Next, fire, compare fire times with the due times.
func modelFakeClock(t *testing.T, k Kind) {
	fc := clock.NewFake(time.Unix(1_700_000_000, 0))
	start := fc.Now().UnixNano()
	s := mustNew[int](t, k, start)
	rng := rand.New(rand.NewPCG(7, uint64(k)))
	es := make([]Entry[int], 300)
	dues := make(map[*Entry[int]]int64, len(es))
	for i := range es {
		d := start + rng.Int64N(int64(3*time.Second))
		es[i].Value = i
		s.Add(&es[i], d)
		dues[&es[i]] = d
	}
	var dst []*Entry[int]
	var maxLate time.Duration
	for s.Len() > 0 {
		next, ok := s.Next()
		if !ok {
			t.Fatal("Len > 0 but no Next")
		}
		if next > fc.Now().UnixNano() {
			fc.Set(time.Unix(0, next))
		}
		now := fc.Now().UnixNano()
		dst = s.Fire(now, dst[:0])
		for _, e := range dst {
			late := time.Duration(now - dues[e])
			if late < 0 {
				t.Fatalf("fired %v early", -late)
			}
			maxLate = max(maxLate, late)
		}
	}
	if res := k.Resolution(); maxLate > res {
		t.Fatalf("max lateness %v exceeds resolution %v", maxLate, res)
	}
}

// TestScale fills every kind with a large batch of each benchmark
// distribution, fires it in one jump and refills relative to the new cursor
// position, which exercises resizing, cascading and lap wrap at a size the
// oracle-driven scenarios cannot afford.
func TestScale(t *testing.T) {
	const n = 50_000
	for _, k := range Kinds() {
		for _, dist := range []string{"uniform", "skewed", "burst"} {
			rng := rand.New(rand.NewPCG(uint64(k), 99))
			base := int64(1 << 40)
			offsets := dues(n, dist, 0, rng)
			s := mustNew[int](t, k, base)
			entries := make([]Entry[int], n)
			dst := make([]*Entry[int], 0, n)
			for round := range 3 {
				for i := range entries {
					s.Add(&entries[i], base+offsets[i])
				}
				if got := s.Fire(base+int64(5*time.Second), dst[:0]); len(got) < n/4 {
					t.Fatalf("%v/%s round %d: only %d fired at 5 s", k, dist, round, len(got))
				}
				dst = s.Fire(base+int64(20*time.Second), dst[:0])
				if s.Len() != 0 {
					t.Fatalf("%v/%s round %d: %d left after the drain", k, dist, round, s.Len())
				}
				if k.Ordered() && !slices.IsSortedFunc(dst, func(a, b *Entry[int]) int { return int(a.due - b.due) }) {
					t.Fatalf("%v/%s round %d: unsorted drain", k, dist, round)
				}
				base += int64(20 * time.Second)
			}
		}
	}
}

// TestFireOrderExactKinds checks that ordered kinds return sorted output on
// a large random batch.
func TestFireOrderExactKinds(t *testing.T) {
	for _, k := range Kinds() {
		if !k.Ordered() {
			continue
		}
		s := mustNew[int](t, k, 0)
		rng := rand.New(rand.NewPCG(3, 4))
		es := make([]Entry[int], 2000)
		for i := range es {
			s.Add(&es[i], rng.Int64N(int64(time.Second)))
		}
		out := s.Fire(int64(time.Second), nil)
		if len(out) != len(es) {
			t.Fatalf("%v: fired %d", k, len(out))
		}
		if !slices.IsSortedFunc(out, func(a, b *Entry[int]) int { return int(a.due - b.due) }) {
			t.Fatalf("%v: unsorted output", k)
		}
	}
}
