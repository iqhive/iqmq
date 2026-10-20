package completion

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/queue"
	"github.com/iqhive/iqmq/internal/testguard"
	"github.com/iqhive/iqmq/internal/wait"
)

// errBroker stands in for whatever the driver reports.
var errBroker = errors.New("broker refused the publish")

// recorder collects the outcomes a completer delivered, so a test can insist
// that every publish came back exactly once with the error it was completed
// with.
type recorder struct {
	mu   sync.Mutex
	got  map[Token][]error
	seen int
}

func newRecorder() *recorder { return &recorder{got: map[Token][]error{}} }

func (r *recorder) callback() Callback {
	return func(res Result) {
		r.mu.Lock()
		r.got[res.Token] = append(r.got[res.Token], res.Err)
		r.seen++
		r.mu.Unlock()
	}
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen
}

// outcome returns the single outcome recorded for t, failing when it came
// back a different number of times.
func (r *recorder) outcome(t testing.TB, tok Token) error {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	errs, ok := r.got[tok]
	if !ok {
		t.Fatalf("token %d never completed", tok)
	}
	if len(errs) != 1 {
		t.Fatalf("token %d completed %d times: %v", tok, len(errs), errs)
	}
	return errs[0]
}

// TestKindsList announces the family's kinds to benchmarks/cmd/bakeoff.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, len(Kinds()))
	for _, k := range Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

func TestKindProperties(t *testing.T) {
	if len(Kinds()) != 4 {
		t.Fatalf("Kinds() = %v", Kinds())
	}
	seen := map[string]bool{}
	for _, k := range Kinds() {
		if !k.Valid() || k == Auto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if seen[k.String()] {
			t.Fatalf("duplicate slug %q", k)
		}
		seen[k.String()] = true
		if p, ok := ParseKind(k.String()); !ok || p != k {
			t.Fatalf("ParseKind(%q) = %v %v", k, p, ok)
		}
		c := mustNew(t, k, Config{})
		if c.Kind() != k || c.Pending() != 0 {
			t.Fatalf("New(%v): kind %v pending %d", k, c.Kind(), c.Pending())
		}
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := c.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	}
	if !Future.HandsBackHandle() || !Channel.HandsBackHandle() || Inline.HandsBackHandle() {
		t.Fatal("HandsBackHandle")
	}
	if !Inline.FixedDelivery() || !Deferred.FixedDelivery() || Future.FixedDelivery() {
		t.Fatal("FixedDelivery")
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Valid() || AutoKind == Auto || Auto.String() != "auto" {
		t.Fatal("Auto binding")
	}
	if Kind(99).Valid() || Kind(99).Ident() != "" || Kind(99).String() != "kind(?)" {
		t.Fatal("invalid kind rendering")
	}
	if _, ok := ParseKind("nope"); ok {
		t.Fatal("ParseKind accepted junk")
	}
}

func TestNewRejects(t *testing.T) {
	expectErr := func(name string, fn func() error) {
		t.Helper()
		if fn() == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
	expectErr("invalid kind", func() error { _, err := New(Kind(99), Config{}); return err })
	expectErr("negative capacity", func() error { _, err := New(Inline, Config{Capacity: -1}); return err })
	expectErr("unbounded ring", func() error { _, err := New(Deferred, Config{Queue: queue.Chunked}); return err })
	c := mustNew(t, Auto, Config{})
	defer func() { _ = c.Close() }()
	if c.Kind() != AutoKind {
		t.Fatalf("New(Auto).Kind() = %v", c.Kind())
	}
}

func TestConfig(t *testing.T) {
	if n := (Config{}).Normalize(Inline); n.Capacity != DefaultCapacity || n.Defer {
		t.Fatalf("zero config normalised to %+v", n)
	}
	if n := (Config{Defer: false}).Normalize(Deferred); !n.Defer {
		t.Fatal("Deferred did not pin deferred delivery")
	}
	if n := (Config{Defer: true}).Normalize(Inline); n.Defer {
		t.Fatal("Inline did not pin inline delivery")
	}
	if n := (Config{Defer: true}).Normalize(Future); !n.Defer {
		t.Fatal("Future ignored the delivery choice")
	}
	for _, bad := range []Config{
		{Capacity: -1},
		{Queue: queue.Kind(99)},
		{Wait: wait.Kind(99)},
		{Queue: queue.Chunked},
		{Queue: queue.ShardedMPMC},
	} {
		if err := bad.Validate(); err == nil || errcode.CodeOf(err) != errcode.InvalidConfig {
			t.Fatalf("Validate(%+v) = %v", bad, err)
		}
	}
	if err := (Config{Capacity: 8, Queue: queue.MPSC, Wait: wait.Hybrid}).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

// TestTokenPacking checks that a token carries its slot and generation.
func TestTokenPacking(t *testing.T) {
	tok := token(7, 3)
	if tok.Slot() != 7 || tok.Gen() != 3 {
		t.Fatalf("token(7,3) = slot %d gen %d", tok.Slot(), tok.Gen())
	}
}

// TestExactlyOnce walks the contract for one kind at a time: a
// started publish completes exactly once, a stale or repeated token is
// ignored, and a refused Start never calls back.
func TestExactlyOnce(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			r := newRecorder()
			c := mustNew(t, k, Config{Capacity: 4})
			defer func() { _ = c.Close() }()
			ctx := context.Background()

			tok, h, err := c.Start(r.callback())
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if (h != nil) != k.HandsBackHandle() {
				t.Fatalf("%v: handle = %v", k, h)
			}
			if c.Pending() != 1 {
				t.Fatalf("Pending = %d", c.Pending())
			}
			if h != nil {
				if done, _ := h.Poll(); done {
					t.Fatal("handle resolved before the publish completed")
				}
			}
			if !c.Complete(tok, errBroker) {
				t.Fatal("Complete refused a live token")
			}
			if err := c.Drain(ctx); err != nil {
				t.Fatalf("Drain: %v", err)
			}
			if got := r.outcome(t, tok); !errors.Is(got, errBroker) {
				t.Fatalf("callback got %v", got)
			}
			if h != nil {
				if err := h.Await(ctx); !errors.Is(err, errBroker) {
					t.Fatalf("Await = %v", err)
				}
				if done, err := h.Poll(); !done || !errors.Is(err, errBroker) {
					t.Fatalf("Poll = %v %v", done, err)
				}
				// A handle answers as often as it is asked.
				if err := h.Await(ctx); !errors.Is(err, errBroker) {
					t.Fatalf("second Await = %v", err)
				}
			}
			// A repeated completion is ignored, and so is a token from an
			// earlier occupant of the slot.
			if c.Complete(tok, nil) {
				t.Fatal("Complete accepted the same token twice")
			}
			if c.Complete(token(tok.Slot(), tok.Gen()+5), nil) {
				t.Fatal("Complete accepted a stale generation")
			}
			if c.Complete(token(9999, 0), nil) {
				t.Fatal("Complete accepted a token outside the table")
			}
			if c.Pending() != 0 {
				t.Fatalf("Pending = %d", c.Pending())
			}
			if r.count() != 1 {
				t.Fatalf("callback ran %d times", r.count())
			}
		})
	}
}

// TestCapacityIsBounded checks the bound: the table refuses beyond its
// capacity instead of growing, and a completion frees its slot again.
func TestCapacityIsBounded(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			r := newRecorder()
			c := mustNew(t, k, Config{Capacity: 2})
			defer func() { _ = c.Close() }()
			t1, _, err := c.Start(r.callback())
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, _, err := c.Start(r.callback()); err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, _, err := c.Start(r.callback()); !errors.Is(err, ErrFull) {
				t.Fatalf("third Start = %v, want ErrFull", err)
			}
			// Completing frees the slot at once, before the callback is
			// even delivered: a driver that publishes from its completion
			// path must not be blocked by its own delivery.
			if !c.Complete(t1, nil) {
				t.Fatal("Complete refused")
			}
			if _, _, err := c.Start(r.callback()); err != nil {
				t.Fatalf("Start after a completion: %v", err)
			}
		})
	}
}

// TestCloseCompletesOutstanding checks that shutting down keeps the promise:
// every started publish gets its callback, and nothing starts afterwards.
func TestCloseCompletesOutstanding(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			r := newRecorder()
			c := mustNew(t, k, Config{Capacity: 8})
			toks := make([]Token, 0, 4)
			for range 4 {
				tok, _, err := c.Start(r.callback())
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				toks = append(toks, tok)
			}
			if err := c.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if c.Pending() != 0 {
				t.Fatalf("Pending = %d after Close", c.Pending())
			}
			for _, tok := range toks {
				if got := r.outcome(t, tok); !errors.Is(got, ErrAbandoned) {
					t.Fatalf("token %d got %v", tok, got)
				}
			}
			if _, _, err := c.Start(r.callback()); !errors.Is(err, ErrClosed) {
				t.Fatalf("Start after Close = %v", err)
			}
			if r.count() != 4 {
				t.Fatalf("%d callbacks ran, want 4", r.count())
			}
			if err := c.Drain(context.Background()); err != nil {
				t.Fatalf("Drain after Close: %v", err)
			}
		})
	}
}

// TestReentrantCallback checks the re-entrancy model: a callback may start
// and complete further publishes, including one that reuses the very slot it
// was called from.
func TestReentrantCallback(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			c := mustNew(t, k, Config{Capacity: 2})
			defer func() { _ = c.Close() }()
			var depth atomic.Int64
			var runs atomic.Int64
			var cb Callback
			cb = func(Result) {
				runs.Add(1)
				if depth.Add(1) > 3 {
					return
				}
				tok, _, err := c.Start(cb)
				if err != nil {
					t.Errorf("re-entrant Start: %v", err)
					return
				}
				if !c.Complete(tok, nil) {
					t.Error("re-entrant Complete refused")
				}
			}
			tok, _, err := c.Start(cb)
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if !c.Complete(tok, nil) {
				t.Fatal("Complete refused")
			}
			if err := c.Drain(context.Background()); err != nil {
				t.Fatalf("Drain: %v", err)
			}
			if runs.Load() != 4 {
				t.Fatalf("callback ran %d times, want 4", runs.Load())
			}
		})
	}
}

// TestConcurrentCompleteHasOneWinner races several completers of the same
// token: exactly one wins and the callback runs once.
func TestConcurrentCompleteHasOneWinner(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			const n = 200
			r := newRecorder()
			c := mustNew(t, k, Config{Capacity: n})
			defer func() { _ = c.Close() }()
			toks := make([]Token, 0, n)
			for range n {
				tok, _, err := c.Start(r.callback())
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				toks = append(toks, tok)
			}
			var winners atomic.Int64
			var wg sync.WaitGroup
			for range 4 {
				wg.Go(func() {
					for _, tok := range toks {
						if c.Complete(tok, nil) {
							winners.Add(1)
						}
					}
				})
			}
			wg.Wait()
			if err := c.Drain(context.Background()); err != nil {
				t.Fatalf("Drain: %v", err)
			}
			if winners.Load() != n {
				t.Fatalf("%d completions won, want %d", winners.Load(), n)
			}
			if r.count() != n {
				t.Fatalf("%d callbacks ran, want %d", r.count(), n)
			}
			for _, tok := range toks {
				_ = r.outcome(t, tok)
			}
		})
	}
}

// TestDeferredRunsOffTheCallersGoroutine checks the property the deferred
// kind exists for: user code does not run on the goroutine that completed the
// publish.
func TestDeferredRunsOffTheCallersGoroutine(t *testing.T) {
	c := mustNew(t, Deferred, Config{Capacity: 4})
	defer func() { _ = c.Close() }()
	block := make(chan struct{})
	done := make(chan struct{})
	tok, _, err := c.Start(func(Result) {
		<-block
		close(done)
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	// The callback is blocked, and Complete still returns: that is what keeps
	// a slow callback off the driver's I/O path.
	if !c.Complete(tok, nil) {
		t.Fatal("Complete refused")
	}
	close(block)
	<-done
}

// TestAwaitRespectsContext checks that a caller may give up on a handle.
func TestAwaitRespectsContext(t *testing.T) {
	for _, k := range []Kind{Future, Channel} {
		t.Run(k.String(), func(t *testing.T) {
			c := mustNew(t, k, Config{Capacity: 2})
			defer func() { _ = c.Close() }()
			_, h, err := c.Start(nil)
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := h.Await(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("Await = %v", err)
			}
		})
	}
}

// TestAllocs reports what one asynchronous publish costs. The callback kinds
// must not allocate at all; the adapters allocate their handle, which is the
// cost the bake-off weighs ("not the default if it needs a channel
// allocation per message").
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			c := mustNew(t, k, Config{Capacity: 64})
			defer func() { _ = c.Close() }()
			var runs int64
			cb := func(Result) { runs++ }
			ctx := context.Background()
			cycle := func() {
				tok, _, err := c.Start(cb)
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				if !c.Complete(tok, nil) {
					t.Fatalf("Complete refused")
				}
				if err := c.Drain(ctx); err != nil {
					t.Fatalf("Drain: %v", err)
				}
			}
			for range 200 {
				cycle()
			}
			allocs := testing.AllocsPerRun(500, cycle)
			t.Logf("%v: %.2f allocs per publish", k, allocs)
			if !k.HandsBackHandle() && allocs != 0 {
				t.Fatalf("%v: %.2f allocs per publish", k, allocs)
			}
			// The adapters allocate their handle and nothing else: the
			// future is one object with one channel, and the channel
			// adapter's buffered channel of Result needs a separate buffer
			// because a Result carries an error. That is the per-message
			// price the specification says has to be worth paying.
			if want := map[Kind]float64{Future: 2, Channel: 3}[k]; k.HandsBackHandle() && allocs > want {
				t.Fatalf("%v: %.2f allocs per publish, want at most %.0f", k, allocs, want)
			}
		})
	}
}

// TestAutoBindingMatchesADR checks that when an ADR binds the completion
// family (front-matter `auto: {completion: <slug>}`), AutoKind agrees.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "completion")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds completion yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds completion to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestLayering keeps the package on the standard library and off the root.
func TestLayering(t *testing.T) {
	testguard.NoRootImport(t, ".")
	testguard.StdlibOnly(t, ".")
}

// TestCloseRacesCompletions closes a completer while other goroutines are
// still starting and completing publishes, which is what a driver's I/O
// goroutine does during a shutdown. Every publish that started must still
// get exactly one callback, and nothing may be left outstanding.
func TestCloseRacesCompletions(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			for range 20 {
				var started, delivered atomic.Int64
				c := mustNew(t, k, Config{Capacity: 64})
				var wg sync.WaitGroup
				for range 4 {
					wg.Go(func() {
						for range 500 {
							tok, _, err := c.Start(func(Result) { delivered.Add(1) })
							if err != nil {
								return
							}
							started.Add(1)
							c.Complete(tok, nil)
						}
					})
				}
				wg.Go(func() { _ = c.Close() })
				wg.Wait()
				if err := c.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				if c.Pending() != 0 {
					t.Fatalf("Pending = %d after Close", c.Pending())
				}
				if got, want := delivered.Load(), started.Load(); got != want {
					t.Fatalf("%d callbacks for %d publishes", got, want)
				}
			}
		})
	}
}

func mustNew(tb testing.TB, k Kind, cfg Config) Completer {
	tb.Helper()
	c, err := New(k, cfg)
	if err != nil {
		tb.Fatalf("New(%v): %v", k, err)
	}
	return c
}
