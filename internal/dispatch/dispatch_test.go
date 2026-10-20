package dispatch

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/queue"
	"github.com/iqhive/iqmq/internal/testguard"
	"github.com/iqhive/iqmq/internal/wait"
)

// item is what the tests dispatch: an ordering key and the position of the
// item within that key's stream, so a handler can check both that everything
// arrived and that it arrived in order.
type item struct {
	key uint64
	seq int
}

// sink records what the handlers saw. It is the oracle side of every
// dispatch test: the pool is correct when the sink's per-key sequences equal
// what was submitted, nothing was handled twice, and no two items of one key
// were ever in a handler at the same time.
type sink struct {
	mu      sync.Mutex
	got     map[uint64][]int
	active  map[uint64]int
	overlap int
	total   int
	slow    map[uint64]time.Duration
	yield   func()
}

func newSink() *sink {
	return &sink{got: map[uint64][]int{}, active: map[uint64]int{}, slow: map[uint64]time.Duration{}}
}

// handle is the pool handler. It brackets the item with an active count so
// that a second handler for the same key is visible however the two are
// scheduled.
func (s *sink) handle(it item) {
	s.mu.Lock()
	if it.key != keylessKey {
		s.active[it.key]++
		if s.active[it.key] > 1 {
			s.overlap++
		}
	}
	d := s.slow[it.key]
	yield := s.yield
	s.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
	if yield != nil {
		yield()
	}
	s.mu.Lock()
	if it.key != keylessKey {
		s.active[it.key]--
	}
	s.got[it.key] = append(s.got[it.key], it.seq)
	s.total++
	s.mu.Unlock()
}

// counts returns the number of items handled.
func (s *sink) counts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

// compare checks the sink against the submitted streams. ordered says whether
// the kind promises per-key order.
func (s *sink) compare(t testing.TB, want map[uint64][]int, ordered bool) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if ordered && s.overlap != 0 {
		t.Fatalf("%d handler pairs ran on one key at the same time", s.overlap)
	}
	if len(s.got) != len(want) {
		t.Fatalf("handled %d keys, submitted %d", len(s.got), len(want))
	}
	for key, wantSeqs := range want {
		gotSeqs := s.got[key]
		if len(gotSeqs) != len(wantSeqs) {
			t.Fatalf("key %d: handled %d items, submitted %d", key, len(gotSeqs), len(wantSeqs))
		}
		if ordered && key != keylessKey {
			for i := range wantSeqs {
				if gotSeqs[i] != wantSeqs[i] {
					t.Fatalf("key %d: handled %v, submitted %v", key, gotSeqs, wantSeqs)
				}
			}
			continue
		}
		seen := make(map[int]bool, len(gotSeqs))
		for _, v := range gotSeqs {
			if seen[v] {
				t.Fatalf("key %d: item %d handled twice", key, v)
			}
			seen[v] = true
		}
		for _, v := range wantSeqs {
			if !seen[v] {
				t.Fatalf("key %d: item %d never handled", key, v)
			}
		}
	}
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
	if len(Kinds()) != 6 {
		t.Fatalf("Kinds() = %v", Kinds())
	}
	seen := map[string]bool{}
	ordered := 0
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
		if k.PreservesKeyOrder() {
			ordered++
		}
		p := mustNew[item](t, k, Config{}, func(item) {})
		if p.Kind() != k || p.Workers() != 1 || p.Lanes() != 1 || p.Pending() != 0 {
			t.Fatalf("New(%v): kind %v workers %d lanes %d", k, p.Kind(), p.Workers(), p.Lanes())
		}
		if err := p.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if ordered != 4 {
		t.Fatalf("%d kinds claim per-key order, want 4", ordered)
	}
	if !WorkStealing.Steals() || HashLanes.Steals() {
		t.Fatal("Steals")
	}
	if !PerPartition.RoutesByPartition() || HashLanes.RoutesByPartition() {
		t.Fatal("RoutesByPartition")
	}
	if !ChanPerLane.FixedLanes() || !SharedQueue.FixedLanes() || HashLanes.FixedLanes() {
		t.Fatal("FixedLanes")
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
	expectErr("invalid kind", func() error { _, err := New[item](Kind(99), Config{}, func(item) {}); return err })
	expectErr("nil handler", func() error { _, err := New[item](HashLanes, Config{}, nil); return err })
	expectErr("negative workers", func() error { _, err := New[item](HashLanes, Config{Workers: -1}, func(item) {}); return err })
	expectErr("single-consumer queue", func() error {
		_, err := New[item](HashLanes, Config{Queue: queue.SPSC}, func(item) {})
		return err
	})
	p := mustNew[item](t, Auto, Config{}, func(item) {})
	defer func() { _ = p.Close() }()
	if p.Kind() != AutoKind {
		t.Fatalf("New(Auto).Kind() = %v", p.Kind())
	}
}

func TestConfig(t *testing.T) {
	n := Config{}.Normalize(HashLanes)
	if n.Workers != 1 || n.Lanes != 1 || n.Capacity != DefaultCapacity || n.Batch != DefaultBatch || n.Steal != DefaultSteal {
		t.Fatalf("zero config normalised to %+v", n)
	}
	if n := (Config{Workers: 4}).Normalize(HashLanes); n.Lanes != 4 {
		t.Fatalf("lanes did not default to workers: %+v", n)
	}
	if n := (Config{Workers: 4, Lanes: 16}).Normalize(SharedQueue); n.Lanes != 1 {
		t.Fatalf("SharedQueue kept %d lanes", n.Lanes)
	}
	if n := (Config{Workers: 4, Lanes: 16}).Normalize(ChanPerLane); n.Lanes != 4 {
		t.Fatalf("ChanPerLane kept %d lanes", n.Lanes)
	}
	for _, bad := range []Config{
		{Workers: -1},
		{Lanes: -1},
		{Capacity: -1},
		{Batch: -1},
		{Steal: -1},
		{Queue: queue.Kind(99)},
		{Wait: wait.Kind(99)},
		{Queue: queue.SPSC},
		{Queue: queue.MPSC},
	} {
		if err := bad.Validate(); err == nil {
			t.Fatalf("Validate accepted %+v", bad)
		} else if errcode.CodeOf(err) != errcode.InvalidConfig {
			t.Fatalf("Validate(%+v) = %v", bad, err)
		}
	}
	if err := (Config{Workers: 2, Lanes: 8, Capacity: 4, Queue: queue.Chan, Wait: wait.Hybrid}).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestRoutes(t *testing.T) {
	if r := Keyed(7); !r.Keyed || r.Key != 7 || r.Partition != NoPartition {
		t.Fatalf("Keyed(7) = %+v", r)
	}
	if r := Keyless(); r.Keyed || r.Partition != NoPartition {
		t.Fatalf("Keyless() = %+v", r)
	}
	if r := Keyed(7).InPartition(3); r.Partition != 3 || !r.Keyed {
		t.Fatalf("InPartition = %+v", r)
	}
}

// TestRouting checks the router half of the family: a keyed item always lands
// on the same lane, a keyless one is spread, and PerPartition follows the
// partition it was given.
func TestRouting(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			p := mustNew[item](t, k, Config{Workers: 4, Lanes: 8, Capacity: 4}, func(item) {})
			defer func() { _ = p.Close() }()
			lanes := p.Lanes()
			if k.FixedLanes() {
				if (k == SharedQueue && lanes != 1) || (k == ChanPerLane && lanes != 4) {
					t.Fatalf("%v: %d lanes", k, lanes)
				}
			} else if lanes != 8 {
				t.Fatalf("%v: %d lanes", k, lanes)
			}
			// A kind that promises per-key order must send a key to one
			// lane every time; RoundRobin promises nothing and spreads.
			for key := range uint64(64) {
				first := p.Route(Keyed(key))
				if first < 0 || first >= lanes {
					t.Fatalf("lane %d out of range", first)
				}
				if !k.PreservesKeyOrder() {
					continue
				}
				for range 8 {
					if got := p.Route(Keyed(key)); got != first {
						t.Fatalf("key %d routed to %d then %d", key, first, got)
					}
				}
			}
			// Keyless items must not pile onto one lane.
			if lanes > 1 {
				seen := map[int]bool{}
				for range 100 {
					seen[p.Route(Keyless())] = true
				}
				if len(seen) < 2 {
					t.Fatalf("%v: keyless items all went to lane %v", k, seen)
				}
			}
			if k.RoutesByPartition() && lanes > 1 {
				if p.Route(Keyed(1).InPartition(3)) != 3%lanes {
					t.Fatal("PerPartition ignored the partition")
				}
			}
		})
	}
}

// submitStreams pushes n items for each of keys keys and returns what was
// submitted per key.
func submitStreams(t testing.TB, p Pool[item], keys, n int, partitions int32) map[uint64][]int {
	t.Helper()
	want := map[uint64][]int{}
	ctx := context.Background()
	for i := range n {
		for k := range keys {
			key := uint64(k) + 1
			r := Keyed(key)
			if partitions > 0 {
				r = r.InPartition(int32(mix(key) % uint64(partitions)))
			}
			if err := p.Submit(ctx, r, item{key: key, seq: i}); err != nil {
				t.Fatalf("Submit: %v", err)
			}
			want[key] = append(want[key], i)
		}
	}
	return want
}

// TestSemantics walks the pool contract for one kind at a time.
func TestSemantics(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			s := newSink()
			p := mustNew(t, k, Config{Workers: 4, Lanes: 8, Capacity: 4}, s.handle)
			want := submitStreams(t, p, 6, 50, 3)
			if err := p.Drain(context.Background()); err != nil {
				t.Fatalf("Drain: %v", err)
			}
			if p.Pending() != 0 {
				t.Fatalf("Pending = %d after Drain", p.Pending())
			}
			s.compare(t, want, k.PreservesKeyOrder())
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("second Close: %v", err)
			}
			// After close nothing is accepted and nothing is lost.
			if p.TrySubmit(Keyed(1), item{key: 1, seq: -1}) {
				t.Fatal("TrySubmit accepted after Close")
			}
			if err := p.Submit(context.Background(), Keyed(1), item{key: 1, seq: -1}); !errors.Is(err, ErrClosed) {
				t.Fatalf("Submit after Close = %v", err)
			}
			if got := s.counts(); got != 6*50 {
				t.Fatalf("handled %d items, submitted %d", got, 6*50)
			}
			if err := p.Drain(context.Background()); err != nil {
				t.Fatalf("Drain after Close: %v", err)
			}
		})
	}
}

// TestKeylessSpread checks that unkeyed items reach every worker: the runner
// relies on it to use its whole pool for a subscription without keys.
func TestKeylessSpread(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			var handled atomic.Int64
			var mu sync.Mutex
			lanes := map[int]bool{}
			p := mustNew(t, k, Config{Workers: 4, Lanes: 4, Capacity: 8}, func(it item) {
				handled.Add(1)
				mu.Lock()
				lanes[it.seq] = true
				mu.Unlock()
			})
			defer func() { _ = p.Close() }()
			ctx := context.Background()
			for range 400 {
				r := Keyless()
				lane := p.Route(r)
				if err := p.Submit(ctx, r, item{seq: lane}); err != nil {
					t.Fatalf("Submit: %v", err)
				}
			}
			if err := p.Drain(ctx); err != nil {
				t.Fatalf("Drain: %v", err)
			}
			if handled.Load() != 400 {
				t.Fatalf("handled %d of 400", handled.Load())
			}
			mu.Lock()
			defer mu.Unlock()
			if p.Lanes() > 1 && len(lanes) < 2 {
				t.Fatalf("keyless items used lanes %v", lanes)
			}
		})
	}
}

// TestBlockingSubmit checks that a full lane blocks the producer instead of
// growing, and that the producer's context releases it.
func TestBlockingSubmit(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			release := make(chan struct{})
			var handled atomic.Int64
			p := mustNew(t, k, Config{Workers: 1, Lanes: 1, Capacity: 1}, func(item) {
				<-release
				handled.Add(1)
			})
			defer func() {
				close(release)
				_ = p.Close()
			}()
			ctx := context.Background()
			// One item reaches the handler, one fills the lane.
			for i := range 2 {
				if err := p.Submit(ctx, Keyed(1), item{key: 1, seq: i}); err != nil {
					t.Fatalf("Submit %d: %v", i, err)
				}
			}
			// The lane is full and the worker is stuck in the handler, so a
			// third item must block until the context is cancelled.
			cctx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- p.Submit(cctx, Keyed(1), item{key: 1, seq: 2}) }()
			// The lane cannot free up while the handler is held, so the
			// submitter is blocked whatever the scheduler does; the pause
			// only decides whether it is parked or still spinning when the
			// context is cancelled, and both must return the same error.
			time.Sleep(20 * time.Millisecond)
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("blocked Submit returned %v", err)
			}
			if p.Pending() != 2 {
				t.Fatalf("Pending = %d after the cancelled submitter left", p.Pending())
			}
		})
	}
}

// TestCloseWithBlockedSubmitter checks that closing releases a producer that
// is waiting for room.
func TestCloseWithBlockedSubmitter(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			release := make(chan struct{})
			p := mustNew(t, k, Config{Workers: 1, Lanes: 1, Capacity: 1}, func(item) {
				<-release
			})
			ctx := context.Background()
			for i := range 2 {
				if err := p.Submit(ctx, Keyed(1), item{key: 1, seq: i}); err != nil {
					t.Fatalf("Submit %d: %v", i, err)
				}
			}
			done := make(chan error, 1)
			go func() { done <- p.Submit(ctx, Keyed(1), item{key: 1, seq: 2}) }()
			time.Sleep(20 * time.Millisecond)
			closed := make(chan error, 1)
			go func() {
				close(release)
				closed <- p.Close()
			}()
			if err := <-done; err != nil && !errors.Is(err, ErrClosed) {
				t.Fatalf("blocked Submit returned %v", err)
			}
			if err := <-closed; err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

// TestCloseRace closes a pool while producers are still submitting: every
// accepted item must be handled and every refused one must say ErrClosed.
func TestCloseRace(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			var handled atomic.Int64
			p := mustNew(t, k, Config{Workers: 4, Lanes: 4, Capacity: 8}, func(item) { handled.Add(1) })
			var accepted atomic.Int64
			var wg sync.WaitGroup
			ctx := context.Background()
			for w := range 4 {
				wg.Go(func() {
					for i := range 500 {
						err := p.Submit(ctx, Keyed(uint64(w)), item{key: uint64(w), seq: i})
						switch {
						case err == nil:
							accepted.Add(1)
						case errors.Is(err, ErrClosed):
							return
						default:
							t.Errorf("Submit: %v", err)
							return
						}
					}
				})
			}
			wg.Go(func() {
				for range 1000 {
					if p.Pending() > 0 {
						break
					}
					runtime.Gosched()
				}
				_ = p.Close()
			})
			wg.Wait()
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if handled.Load() != accepted.Load() {
				t.Fatalf("accepted %d items but handled %d", accepted.Load(), handled.Load())
			}
			if p.Pending() != 0 {
				t.Fatalf("Pending = %d after Close", p.Pending())
			}
		})
	}
}

// TestStragglerDoesNotStallThePool checks that one slow key keeps its own
// lane busy without stopping the others.
func TestStragglerDoesNotStallThePool(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			s := newSink()
			s.slow[1] = 2 * time.Millisecond
			p := mustNew(t, k, Config{Workers: 4, Lanes: 8, Capacity: 8}, s.handle)
			defer func() { _ = p.Close() }()
			want := submitStreams(t, p, 4, 20, 0)
			ctx := context.Background()
			if err := p.Drain(ctx); err != nil {
				t.Fatalf("Drain: %v", err)
			}
			s.compare(t, want, k.PreservesKeyOrder())
		})
	}
}

// TestAllocs pins the runner's hot path: routing an item and handing it to a
// worker must not allocate.
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			var handled atomic.Int64
			p := mustNew(t, k, Config{Workers: 2, Lanes: 4, Capacity: 256}, func(item) { handled.Add(1) })
			defer func() { _ = p.Close() }()
			ctx := context.Background()
			seq := 0
			cycle := func() {
				seq++
				if err := p.Submit(ctx, Keyed(uint64(seq)), item{key: uint64(seq), seq: seq}); err != nil {
					t.Fatalf("Submit: %v", err)
				}
			}
			// Warm up: the wait strategies pool their waiters, and the
			// goroutines have to reach their parking spots at least once.
			for range 2000 {
				cycle()
			}
			if err := p.Drain(ctx); err != nil {
				t.Fatalf("Drain: %v", err)
			}
			allocs := testing.AllocsPerRun(2000, cycle)
			t.Logf("%v: %.2f allocs per submit", k, allocs)
			if allocs != 0 {
				t.Fatalf("%v: %.2f allocs per submit", k, allocs)
			}
			if err := p.Drain(ctx); err != nil {
				t.Fatalf("Drain: %v", err)
			}
			drain := func() {
				if err := p.Drain(ctx); err != nil {
					t.Fatalf("Drain: %v", err)
				}
			}
			if allocs := testing.AllocsPerRun(100, drain); allocs != 0 {
				t.Fatalf("%v: idle Drain allocated %.2f", k, allocs)
			}
		})
	}
}

// TestAutoBindingMatchesADR checks that when an ADR binds the dispatch family
// (front-matter `auto: {dispatch: <slug>}`), AutoKind agrees with it.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "dispatch")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds dispatch yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds dispatch to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestLayering keeps the package on the standard library and off the root.
func TestLayering(t *testing.T) {
	testguard.NoRootImport(t, ".")
	testguard.StdlibOnly(t, ".")
}

// TestNothingIsStrandedUnderStealing hammers the pool with a small batch and
// many more lanes than workers, which is the shape that makes a worker scan
// a lane while another worker holds it. A lane whose owner skipped it because
// it was claimed, and which the holder then left with work in it, must still
// be woken: this test fails by timing out in Drain when it is not.
func TestNothingIsStrandedUnderStealing(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			const n = 200_000
			var handled atomic.Int64
			p := mustNew(t, k, Config{Workers: 8, Lanes: 32, Capacity: 4, Batch: 1}, func(item) {
				handled.Add(1)
			})
			defer func() { _ = p.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			for i := range n {
				if err := p.Submit(ctx, Keyed(uint64(i%97)+1), item{key: uint64(i%97) + 1, seq: i}); err != nil {
					t.Fatalf("Submit %d: %v", i, err)
				}
			}
			if err := p.Drain(ctx); err != nil {
				t.Fatalf("Drain: %v (handled %d of %d, pending %d)", err, handled.Load(), n, p.Pending())
			}
			if handled.Load() != n {
				t.Fatalf("handled %d of %d", handled.Load(), n)
			}
		})
	}
}

// The close path parks on the admission gate's counter, so its tests need a
// bound on "the wake has been issued and has not arrived". A missed wake is
// permanent, so the bound only has to outlast scheduling noise on a loaded
// host; it is never reached by a working pool.
const (
	closeWakeTimeout = 10 * time.Second
	closeStateLimit  = 5 * time.Second
)

// closeProbe is what the close-path tests need of a pool beyond the Pool
// interface: the admission gate, the withdrawal a refused submission
// performs, and whether the things that park on the counter are parked. It is
// a type switch rather than an interface so that no production method exists
// only for a test.
type closeProbe struct {
	g        *gate
	withdraw func()
	parked   func() bool
	draining func() bool
	dump     func() string
}

func probeClose(t *testing.T, p Pool[item]) closeProbe {
	t.Helper()
	switch pp := p.(type) {
	case *lanePool[item]:
		return closeProbe{
			g:        &pp.g,
			withdraw: pp.withdraw,
			parked: func() bool {
				for i := range pp.wq {
					if !pp.wq[i].s.HasWaiters() {
						return false
					}
				}
				return true
			},
			draining: pp.idle.s.HasWaiters,
			dump: func() string {
				lanes := 0
				for i := range pp.lanes {
					lanes += pp.lanes[i].q.Len()
				}
				return fmt.Sprintf("pending=%d closing=%v queued=%d workersParked=%v drainParked=%v",
					pp.g.load(), pp.g.closed(), lanes, pp.wq[0].s.HasWaiters(), pp.idle.s.HasWaiters())
			},
		}
	case *chanPool[item]:
		return closeProbe{
			g:        &pp.g,
			withdraw: pp.withdraw,
			// This kind's workers poll the counter instead of parking on it.
			parked:   func() bool { return true },
			draining: pp.idle.s.HasWaiters,
			dump: func() string {
				return fmt.Sprintf("pending=%d closing=%v drainParked=%v",
					pp.g.load(), pp.g.closed(), pp.idle.s.HasWaiters())
			},
		}
	default:
		t.Fatalf("close probe: unexpected pool type %T", p)
		return closeProbe{}
	}
}

// awaitState polls until cond holds and keeps holding, which is how a test
// tells a parked goroutine from one that is merely about to park: the states
// these tests wait for are ones nothing but the test itself can leave.
func awaitState(t *testing.T, what string, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(closeStateLimit)
	for time.Now().Before(deadline) {
		if !cond() {
			runtime.Gosched()
			continue
		}
		time.Sleep(2 * time.Millisecond)
		if cond() {
			return true
		}
	}
	t.Fatalf("timed out waiting for %s", what)
	return false
}

// TestCloseJoinsAWorkerThatSampledAWithdrawnReservation drives the
// interleaving that used to deadlock Close rather than waiting for luck to
// find it. A submitter announces itself on the admission
// gate; a worker then samples the counter, finds the pair "closing, and the
// count is not zero", and parks on it; the submitter reads the closing flag,
// finds the pool closing, and gives the reservation back. That withdrawal is
// the last event that can ever wake the worker, so Close returns only if the
// withdrawal announces the fall to zero.
func TestCloseJoinsAWorkerThatSampledAWithdrawnReservation(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			exercised := 0
			for range 3 {
				p := mustNew(t, k, Config{Workers: 2, Lanes: 4, Capacity: 4}, func(item) {})
				pr := probeClose(t, p)
				awaitState(t, "the workers to park on an idle pool", pr.parked)
				// The submitter is now between gate.reserve's two halves: it
				// is counted, and has not yet read the closing flag.
				pr.g.announce()
				closed := make(chan error, 1)
				go func() { closed <- p.Close() }()
				awaitState(t, "a worker to park on the reservation", func() bool {
					return pr.g.closed() && pr.parked()
				})
				select {
				case err := <-closed:
					// Close beat the park; this round proved nothing.
					if err != nil {
						t.Fatalf("Close: %v", err)
					}
					pr.withdraw()
					continue
				default:
				}
				exercised++
				pr.withdraw()
				select {
				case err := <-closed:
					if err != nil {
						t.Fatalf("Close: %v", err)
					}
				case <-time.After(closeWakeTimeout):
					t.Fatalf("Close did not return within %s of the refused reservation being withdrawn: %s",
						closeWakeTimeout, pr.dump())
				}
			}
			if exercised == 0 {
				t.Fatal("never reached the state the test is about: Close returned before any worker parked")
			}
		})
	}
}

// TestDrainReturnsWhenARefusedReservationIsWithdrawn is the same defect on
// the other thing that parks on the counter. A Drain that runs after Close
// has already returned has nothing else to wake it: Close's own trailing
// NotifyAll is spent, so a submission refused by the closing flag must
// announce the fall to zero itself.
func TestDrainReturnsWhenARefusedReservationIsWithdrawn(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			p := mustNew(t, k, Config{Workers: 2, Lanes: 4, Capacity: 4}, func(item) {})
			pr := probeClose(t, p)
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// A TrySubmit that started before the pool closed reserves its
			// slot before it can know: the counter carries the reservation
			// until the submitter reads the flag and withdraws.
			pr.g.announce()
			drained := make(chan error, 1)
			go func() { drained <- p.Drain(context.Background()) }()
			awaitState(t, "Drain to park on the reservation", pr.draining)
			pr.withdraw()
			select {
			case err := <-drained:
				if err != nil {
					t.Fatalf("Drain: %v", err)
				}
			case <-time.After(closeWakeTimeout):
				t.Fatalf("Drain did not return within %s of the refused reservation being withdrawn: %s",
					closeWakeTimeout, pr.dump())
			}
		})
	}
}

// TestCloseRaceWithdrawnReservations is the end-to-end form of the two tests
// above: real submitters race a real Close and the deadlock is whatever
// interleaving they find, which is how it was found in the first place (a
// test-tags run of TestCloseRace that timed out after ten minutes).
//
// The window a refused submission holds its reservation in is two
// instructions wide, so on its own this race needs of the order of 34,000
// rounds to land inside it — measured at 14 deadlocks in 480,000 rounds over
// 24 concurrent processes. An iqmq_debug build yields the processor there
// (refusalWindow), which brings it down to a few hundred rounds; that is why
// the guard that runs in the test-tags gate is the reliable one, and why this
// test is a net rather than a proof in an ordinary build.
func TestCloseRaceWithdrawnReservations(t *testing.T) {
	rounds := 600
	if !debugWindows {
		rounds = 100
	}
	if testing.Short() {
		rounds = 20
	}
	ctx := context.Background()
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			for round := range rounds {
				var handled, accepted atomic.Int64
				p := mustNew(t, k, Config{Workers: 4, Lanes: 4, Capacity: 8}, func(item) { handled.Add(1) })
				pr := probeClose(t, p)
				var wg sync.WaitGroup
				for w := range 4 {
					wg.Go(func() {
						for i := range 64 {
							switch err := p.Submit(ctx, Keyed(uint64(w)), item{key: uint64(w), seq: i}); {
							case err == nil:
								accepted.Add(1)
							case errors.Is(err, ErrClosed):
								return
							default:
								t.Errorf("Submit: %v", err)
								return
							}
						}
					})
				}
				wg.Go(func() {
					for range 1000 {
						if p.Pending() > 0 {
							break
						}
						runtime.Gosched()
					}
					_ = p.Close()
				})
				done := make(chan struct{})
				go func() { wg.Wait(); close(done) }()
				select {
				case <-done:
				case <-time.After(closeWakeTimeout):
					t.Fatalf("round %d: Close and the submitters did not finish within %s: %s",
						round, closeWakeTimeout, pr.dump())
				}
				if err := p.Close(); err != nil {
					t.Fatalf("round %d: Close: %v", round, err)
				}
				if handled.Load() != accepted.Load() {
					t.Fatalf("round %d: accepted %d items but handled %d", round, accepted.Load(), handled.Load())
				}
				if p.Pending() != 0 {
					t.Fatalf("round %d: Pending = %d after Close", round, p.Pending())
				}
			}
		})
	}
}

func mustNew[T any](tb testing.TB, k Kind, cfg Config, h Handler[T]) Pool[T] {
	tb.Helper()
	p, err := New[T](k, cfg, h)
	if err != nil {
		tb.Fatalf("New(%v): %v", k, err)
	}
	return p
}
