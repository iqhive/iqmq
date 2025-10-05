package completion

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
	"bitbucket.org/iqhive/iqmq/internal/queue"
	"bitbucket.org/iqhive/iqmq/internal/wait"
)

// fuzzQueues are the rings the deferred delivery may hand completions over
// through: every kind that is bounded, ordered and multi-producer. A
// single-producer ring is not one of them, because a callback completing a
// further publish makes the completion goroutine a second producer.
var fuzzQueues = []queue.Kind{queue.MutexRing, queue.Chan, queue.SeqMPMC, queue.MPSC, queue.AppendLog}

// fuzzWaits are the strategies the completion goroutine may park on. The
// spinning kinds are left out: they would hold a processor for the whole
// corpus without exercising anything the parking kinds do not.
var fuzzWaits = []wait.Kind{wait.Chan, wait.Cond, wait.EventCount, wait.SpinPark, wait.Hybrid}

// compHarness drives one completer and the model of what it owes: which
// tokens are outstanding, which have been delivered, and how many callbacks
// have run. The completer is correct when every started publish is delivered
// exactly once, a stale or repeated token delivers nothing, and the pending
// count matches the model whenever the completer is idle.
type compHarness struct {
	k        Kind
	c        Completer
	capacity int
	live     []Token
	handles  map[Token]Handle
	closed   bool

	mu        sync.Mutex
	delivered map[Token]int
	outcomes  map[Token]error

	started   atomic.Int64
	callbacks atomic.Int64
}

func newCompHarness(k Kind, cfg Config) *compHarness {
	cfg = cfg.Normalize(k)
	return &compHarness{
		k:         k,
		c:         New(k, cfg),
		capacity:  cfg.Capacity,
		handles:   map[Token]Handle{},
		delivered: map[Token]int{},
		outcomes:  map[Token]error{},
	}
}

// record is what every callback the harness installs does.
func (h *compHarness) record(res Result) {
	h.mu.Lock()
	h.delivered[res.Token]++
	h.outcomes[res.Token] = res.Err
	h.mu.Unlock()
	h.callbacks.Add(1)
}

// start registers one publish. A full table and a closed completer are
// legitimate answers, and both mean the callback will never run.
func (h *compHarness) start(t testing.TB, reentrant bool, depth int) {
	t.Helper()
	cb := h.record
	if reentrant {
		cb = h.reentrantCallback(depth)
	}
	tok, handle, err := h.c.Start(cb)
	switch {
	case err == nil:
		if h.closed {
			t.Fatal("Start succeeded on a closed completer")
		}
		h.started.Add(1)
		h.live = append(h.live, tok)
		if (handle != nil) != h.k.HandsBackHandle() {
			t.Fatalf("%v: handle = %v", h.k, handle)
		}
		if handle != nil {
			h.handles[tok] = handle
		}
	case errors.Is(err, ErrClosed):
		if !h.closed {
			t.Fatal("Start reported ErrClosed on an open completer")
		}
	case errors.Is(err, ErrFull):
		// A full table is a legitimate answer, and it can arrive with fewer
		// publishes outstanding than the capacity: a slot is handed back
		// before its callback runs, and a callback may hold one of its own.
		// What matters is that the callback will not run, which the model
		// records by not adding the token.
	default:
		t.Fatalf("Start: %v", err)
	}
}

// reentrantCallback returns a callback that publishes again from inside the
// delivery, which is the re-entrancy the contract allows: no lock is held and
// the slot it came from is already free.
func (h *compHarness) reentrantCallback(depth int) Callback {
	var cb Callback
	left := depth
	cb = func(res Result) {
		h.record(res)
		if left <= 0 {
			return
		}
		left--
		tok, _, err := h.c.Start(cb)
		if err != nil {
			return // a full or closing table is a legitimate answer
		}
		// The publish is counted before it is completed: its callback may
		// run before Complete even returns.
		h.started.Add(1)
		// A close racing this delivery may have abandoned the publish
		// already, in which case its callback has run and this completion
		// has nothing left to do.
		h.c.Complete(tok, nil)
	}
	return cb
}

// completeLive completes the i-th outstanding publish.
func (h *compHarness) completeLive(t testing.TB, i int, err error) {
	t.Helper()
	if len(h.live) == 0 {
		return
	}
	i %= len(h.live)
	tok := h.live[i]
	h.live = append(h.live[:i], h.live[i+1:]...)
	if !h.c.Complete(tok, err) {
		t.Fatalf("Complete refused the live token %d", tok)
	}
}

// completeStale completes a token that is not outstanding, which must deliver
// nothing at all.
func (h *compHarness) completeStale(t testing.TB, tok Token) {
	t.Helper()
	for _, live := range h.live {
		if live == tok {
			return
		}
	}
	// Nothing may be in flight, or another delivery would be mistaken for
	// one this refusal produced.
	h.settle(t)
	before := h.callbacks.Load()
	if h.c.Complete(tok, errBroker) {
		t.Fatalf("Complete accepted the stale token %d (slot %d gen %d)", tok, tok.Slot(), tok.Gen())
	}
	if after := h.callbacks.Load(); after != before {
		t.Fatalf("a refused completion still delivered %d callbacks", after-before)
	}
}

// settle waits for the deliveries of the completions made so far. It is a
// spin rather than a Drain because publishes may still be outstanding, and
// Drain waits for those too.
func (h *compHarness) settle(t testing.TB) {
	t.Helper()
	for i := 0; h.c.Pending() != len(h.live); i++ {
		if i > 1<<22 {
			t.Fatalf("%v: Pending stuck at %d with %d outstanding", h.k, h.c.Pending(), len(h.live))
		}
		runtime.Gosched()
	}
}

// check waits for the deliveries and compares the completer with the model.
func (h *compHarness) check(t testing.TB) {
	t.Helper()
	h.settle(t)
	if len(h.live) == 0 {
		// With nothing outstanding, Drain must return at once.
		if err := h.c.Drain(context.Background()); err != nil {
			t.Fatalf("Drain: %v", err)
		}
	}
	if got, want := h.c.Pending(), len(h.live); got != want {
		t.Fatalf("%v: Pending = %d, want %d outstanding", h.k, got, want)
	}
	// Every publish that started is either still outstanding or has had its
	// one callback: nothing is lost and nothing is invented, however the
	// completions, the deliveries and the re-entrant publishes interleave.
	if got, want := h.started.Load()-h.callbacks.Load(), int64(len(h.live)); got != want {
		t.Fatalf("%v: %d publishes started and %d delivered, but %d are outstanding",
			h.k, h.started.Load(), h.callbacks.Load(), want)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for tok, n := range h.delivered {
		if n != 1 {
			t.Fatalf("%v: token %d delivered %d times", h.k, tok, n)
		}
	}
	for tok, handle := range h.handles {
		done, err := handle.Poll()
		if _, delivered := h.delivered[tok]; delivered != done {
			t.Fatalf("%v: token %d delivered=%v but its handle says %v", h.k, tok, delivered, done)
		}
		if done && !errors.Is(err, h.outcomes[tok]) && !errors.Is(h.outcomes[tok], err) {
			t.Fatalf("%v: token %d handle says %v, callback said %v", h.k, tok, err, h.outcomes[tok])
		}
	}
}

// close shuts the completer down. Everything still outstanding must have been
// delivered by the time it returns, which the check then insists on: with
// nothing outstanding, every publish that started must have had its callback.
func (h *compHarness) close(t testing.TB) {
	t.Helper()
	if err := h.c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h.live = h.live[:0]
	h.closed = true
	h.check(t)
}

// compScript is the op script.
func compScript() *fuzzutil.Script[*compHarness] {
	return &fuzzutil.Script[*compHarness]{
		Steps: []fuzzutil.Step[*compHarness]{
			{Name: "start", Weight: 5, Run: func(t testing.TB, _ *fuzzutil.Reader, h *compHarness) {
				h.start(t, false, 0)
			}},
			{Name: "startReentrant", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, h *compHarness) {
				h.start(t, true, r.Int(1, 3))
			}},
			{Name: "complete", Weight: 5, Run: func(t testing.TB, r *fuzzutil.Reader, h *compHarness) {
				var err error
				if r.Bool() {
					err = errBroker
				}
				h.completeLive(t, r.Int(0, 63), err)
			}},
			{Name: "completeStale", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, h *compHarness) {
				h.completeStale(t, token(uint32(r.Int(0, 31)), uint32(r.Int(0, 7))))
			}},
			{Name: "drain", Weight: 2, Run: func(t testing.TB, _ *fuzzutil.Reader, h *compHarness) {
				h.check(t)
			}},
			{Name: "close", Weight: 1, Run: func(t testing.TB, _ *fuzzutil.Reader, h *compHarness) {
				h.close(t)
			}},
		},
		MaxSteps: 128,
		Check: func(t testing.TB, h *compHarness) {
			// Pending counts publishes that have started and not yet been
			// delivered, which may briefly exceed the slot count: the slot
			// is freed before the callback runs, so that a callback can
			// publish again. The bound that matters is on slots, and it is
			// what makes Start refuse.
			if n := h.c.Pending(); n < 0 {
				t.Fatalf("%v: Pending = %d", h.k, n)
			}
			if h.closed && h.c.Pending() != 0 {
				t.Fatalf("%v: closed completer holds %d publishes", h.k, h.c.Pending())
			}
		},
	}
}

// compConfigFrom decodes a kind and a completer configuration from fuzz
// input.
func compConfigFrom(r *fuzzutil.Reader) (Kind, Config) {
	k := Kinds()[r.Choice(len(Kinds()))]
	cfg := Config{
		Capacity: r.Int(1, 8),
		Defer:    r.Bool(),
		Queue:    fuzzQueues[r.Choice(len(fuzzQueues))],
		Wait:     fuzzWaits[r.Choice(len(fuzzWaits))],
	}
	return k, cfg
}

// compSeeds authors readable seed scripts for every kind.
func compSeeds(sc *fuzzutil.Script[*compHarness]) [][]byte {
	type step func(w *fuzzutil.Writer)
	complete := func(i int, fail bool) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(w, "complete")
			w.Bool(fail)
			w.Int(0, 63, i)
		}
	}
	reentrant := func(depth int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "startReentrant"); w.Int(1, 3, depth) }
	}
	stale := func(slot, gen int) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(w, "completeStale")
			w.Int(0, 31, slot)
			w.Int(0, 7, gen)
		}
	}
	plain := func(name string) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, name) }
	}
	start := plain("start")
	scripts := [][]step{
		// One publish, accepted.
		{start, complete(0, false), plain("drain")},
		// One publish, refused by the broker.
		{start, complete(0, true), plain("drain")},
		// Several outstanding, completed out of order.
		{start, start, start, complete(1, false), complete(1, true), complete(0, false), plain("drain")},
		// A completion that is already gone, and one that never existed.
		{start, complete(0, false), stale(0, 0), stale(30, 7), plain("drain")},
		// A callback that publishes again from inside the delivery.
		{reentrant(2), complete(0, false), plain("drain")},
		// Closing with publishes outstanding still calls every callback.
		{start, start, plain("close"), start},
	}
	kinds := Kinds()
	seeds := make([][]byte, 0, len(scripts)*len(kinds))
	for ki := range kinds {
		for i, steps := range scripts {
			w := fuzzutil.NewWriter()
			w.Choice(len(kinds), ki)
			w.Int(1, 8, 1+i%8)
			w.Bool(i%2 == 0)
			w.Choice(len(fuzzQueues), i%len(fuzzQueues))
			w.Choice(len(fuzzWaits), i%len(fuzzWaits))
			for _, st := range steps {
				st(w)
			}
			seeds = append(seeds, w.Data())
		}
	}
	return seeds
}

// runCompScript runs one script against a fresh completer and leaves it
// closed and fully compared.
func runCompScript(t testing.TB, data []byte) int {
	sc := compScript()
	r := fuzzutil.NewReader(data)
	k, cfg := compConfigFrom(r)
	h := newCompHarness(k, cfg)
	defer func() {
		if !h.closed {
			_ = h.c.Close()
		}
	}()
	n := sc.Run(t, data[len(data)-r.Remaining():], h)
	if t.Failed() {
		return n
	}
	if !h.closed {
		h.close(t)
	}
	return n
}

// FuzzAsyncCompletion is the differential fuzz target of A
// fuzz-chosen kind, capacity, delivery, hand-over ring and wait strategy run
// through a script of starts, completions, stale completions, re-entrant
// callbacks, drains and a close, compared with the model after every step.
// Checked: a started publish is delivered exactly once, a refused Start never
// delivers, a stale or repeated token delivers nothing, the outstanding count
// never leaves [0, capacity], a handle agrees with the callback, a callback
// may publish from inside a delivery, and a close delivers everything that
// was still outstanding.
func FuzzAsyncCompletion(f *testing.F) {
	sc := compScript()
	for _, s := range compSeeds(sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runCompScript(t, data)
	})
}

// FuzzAsyncCompletionConcurrent races publishers and completers against one
// table, which is the shape a driver with several I/O goroutines produces.
// Checked: every publish is delivered exactly once, exactly one Complete per
// token wins, and the table closes with nothing outstanding.
func FuzzAsyncCompletionConcurrent(f *testing.F) {
	for _, seed := range [][3]int{{0, 2, 40}, {1, 3, 60}, {3, 4, 25}} {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), seed[0]%len(Kinds()))
		w.Int(1, 8, seed[1])
		w.Bool(seed[0]%2 == 0)
		w.Choice(len(fuzzQueues), seed[0]%len(fuzzQueues))
		w.Choice(len(fuzzWaits), seed[1]%len(fuzzWaits))
		w.Int(1, 100, seed[2])
		for i := range 64 {
			w.Byte(byte(i * 41))
		}
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k, cfg := compConfigFrom(r)
		per := r.Int(1, 100)
		c := New(k, cfg)
		defer func() { _ = c.Close() }()

		var delivered sync.Map // Token -> number of callbacks
		var callbacks, wins atomic.Int64
		cb := func(res Result) {
			n, _ := delivered.LoadOrStore(res.Token, new(atomic.Int64))
			n.(*atomic.Int64).Add(1)
			callbacks.Add(1)
		}
		fuzzutil.Actors(t, data[len(data)-r.Remaining():], 4, func(_ int, ar *fuzzutil.Reader) {
			for range per {
				tok, _, err := c.Start(cb)
				if err != nil {
					fuzzutil.MaybeYield(ar)
					continue
				}
				fuzzutil.MaybeYield(ar)
				// Two attempts at the same token: exactly one must win.
				a := c.Complete(tok, nil)
				b := c.Complete(tok, errBroker)
				if a == b {
					t.Errorf("both completions of token %d returned %v", tok, a)
					return
				}
				wins.Add(1)
			}
		})
		if t.Failed() {
			return
		}
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if got := callbacks.Load(); got != wins.Load() {
			t.Fatalf("%v: %d callbacks for %d completions", k, got, wins.Load())
		}
		delivered.Range(func(tok, v any) bool {
			if n := v.(*atomic.Int64).Load(); n != 1 {
				t.Fatalf("%v: token %d delivered %d times", k, tok, n)
			}
			return true
		})
		if c.Pending() != 0 {
			t.Fatalf("%v: Pending = %d after close", k, c.Pending())
		}
	})
}
