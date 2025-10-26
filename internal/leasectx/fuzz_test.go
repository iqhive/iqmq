package leasectx

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
	"bitbucket.org/iqhive/iqmq/internal/timer"
)

// fuzzTimers are the schedulers the Clock kind may order its deadlines with.
// Every kind is fair game: a wheel that rounds a due time up must still
// never fire a lease early, which is the property that matters here.
var fuzzTimers = []timer.Kind{
	timer.Heap, timer.Heap4, timer.HierarchicalWheel, timer.HashedWheel, timer.HybridHeapWheel, timer.Calendar,
}

// lease is the model of one derived context: when it is due, whether it was
// released, and the outcome that was observed for it, which must never change
// once it has one.
type lease struct {
	ctx      context.Context //nolint:containedctx // the model records the context it is checking
	release  Release
	deadline time.Time
	released bool
	settled  error
}

// leaseHarness drives one source and the model of what it owes. Every step
// ends with a check of every live lease against the model, so a context that
// fires early, fires late, fires twice with different errors, or never fires
// is caught at the step that caused it.
type leaseHarness struct {
	k        Kind
	s        Source
	margin   time.Duration
	leases   []*lease
	parent   context.Context //nolint:containedctx // one parent for the whole script, as a runner has
	cancel   context.CancelFunc
	canceled bool
}

func newLeaseHarness(k Kind, tk timer.Kind, margin time.Duration) *leaseHarness {
	parent, cancel := context.WithCancel(context.Background())
	return &leaseHarness{
		k:      k,
		s:      New(k, Config{Timer: tk, Margin: margin}),
		margin: margin,
		parent: parent,
		cancel: cancel,
	}
}

// derive adds one lease due in d.
func (h *leaseHarness) derive(t testing.TB, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	ctx, release := h.s.Derive(h.parent, deadline)
	if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline.Add(-h.margin)) {
		t.Fatalf("Deadline = %v %v, want %v", got, ok, deadline.Add(-h.margin))
	}
	h.leases = append(h.leases, &lease{ctx: ctx, release: release, deadline: deadline.Add(-h.margin)})
}

// releaseOne releases the i-th live lease, wrapping around.
func (h *leaseHarness) releaseOne(i int) {
	if len(h.leases) == 0 {
		return
	}
	l := h.leases[i%len(h.leases)]
	l.release()
	l.released = true
}

// cancelParent cancels the context every lease was derived from.
func (h *leaseHarness) cancelParent() {
	h.cancel()
	h.canceled = true
}

// check compares every lease with the model. It runs after the bubble has
// gone quiet, so the source has had every chance to act.
func (h *leaseHarness) check(t testing.TB) {
	t.Helper()
	synctest.Wait()
	now := time.Now()
	for i, l := range h.leases {
		want := h.want(l, now)
		got := l.ctx.Err()
		if want == nil {
			if got != nil {
				t.Fatalf("lease %d (%v, due in %v): done early with %v", i, h.k, l.deadline.Sub(now), got)
			}
			continue
		}
		if got == nil {
			t.Fatalf("lease %d (%v, due %v ago, released=%v, parent cancelled=%v): not done, want %v",
				i, h.k, now.Sub(l.deadline), l.released, h.canceled, want)
		}
		if !errors.Is(got, want) {
			t.Fatalf("lease %d (%v): Err = %v, want %v (deadline %v, now %v, released=%v, parent=%v, settled=%v)",
				i, h.k, got, want, l.deadline, now, l.released, h.canceled, l.settled)
		}
		// Whatever a lease settled on, it keeps: a context never changes its
		// mind, and the source must not overwrite one outcome with another.
		if l.settled == nil {
			l.settled = got
		} else if !errors.Is(got, l.settled) {
			t.Fatalf("lease %d (%v): Err changed from %v to %v", i, h.k, l.settled, got)
		}
		select {
		case <-l.ctx.Done():
		default:
			t.Fatalf("lease %d (%v): Err is %v but Done is open", i, h.k, got)
		}
	}
	if live := h.liveCount(); h.s.Live() != live {
		t.Fatalf("%v: Live = %d, want %d", h.k, h.s.Live(), live)
	}
}

// want is the model: the outcome a lease must have by now, or nil when it
// must still be open. A lease that was released or whose parent was cancelled
// is cancelled; one whose deadline has passed exceeded it; an outcome already
// observed wins over both, because a context settles once.
func (h *leaseHarness) want(l *lease, now time.Time) error {
	if l.settled != nil {
		return l.settled
	}
	switch {
	case l.released || h.canceled:
		return context.Canceled
	case !now.Before(l.deadline):
		return context.DeadlineExceeded
	default:
		return nil
	}
}

// liveCount is the model's count of leases that have not been released.
func (h *leaseHarness) liveCount() int {
	n := 0
	for _, l := range h.leases {
		if !l.released {
			n++
		}
	}
	return n
}

// close releases everything and shuts the source down, which a bubble
// requires: a leaked scheduler goroutine is a bubble deadlock.
func (h *leaseHarness) close(t testing.TB) {
	t.Helper()
	for _, l := range h.leases {
		l.release()
		l.released = true
	}
	h.cancel()
	if err := h.s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if h.s.Live() != 0 {
		t.Fatalf("%v: Live = %d after close", h.k, h.s.Live())
	}
}

// leaseScript is the op script.
func leaseScript() *fuzzutil.Script[*leaseHarness] {
	return &fuzzutil.Script[*leaseHarness]{
		Steps: []fuzzutil.Step[*leaseHarness]{
			{Name: "derive", Weight: 5, Run: func(t testing.TB, r *fuzzutil.Reader, h *leaseHarness) {
				h.derive(t, time.Duration(r.Int(0, 200))*time.Millisecond)
				h.check(t)
			}},
			{Name: "deriveLate", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, h *leaseHarness) {
				// A lease that has already expired: the handler must never
				// get an open context for it.
				h.derive(t, -time.Duration(r.Int(0, 100))*time.Millisecond)
				h.check(t)
			}},
			{Name: "release", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, h *leaseHarness) {
				h.releaseOne(r.Int(0, 63))
				h.check(t)
			}},
			{Name: "sleep", Weight: 4, Run: func(t testing.TB, r *fuzzutil.Reader, h *leaseHarness) {
				time.Sleep(time.Duration(r.Int(0, 120)) * time.Millisecond)
				h.check(t)
			}},
			{Name: "cancelParent", Weight: 1, Run: func(t testing.TB, _ *fuzzutil.Reader, h *leaseHarness) {
				h.cancelParent()
				h.check(t)
			}},
		},
		MaxSteps: 64,
		Check: func(t testing.TB, h *leaseHarness) {
			if h.s.Timers() < 0 {
				t.Fatal("negative timer count")
			}
		},
	}
}

// leaseConfigFrom decodes a kind, a scheduler and a margin from fuzz input.
func leaseConfigFrom(r *fuzzutil.Reader) (Kind, timer.Kind, time.Duration) {
	k := Kinds()[r.Choice(len(Kinds()))]
	tk := fuzzTimers[r.Choice(len(fuzzTimers))]
	margin := time.Duration(r.Int(0, 50)) * time.Millisecond
	return k, tk, margin
}

// leaseSeeds authors readable seed scripts for both kinds.
func leaseSeeds(sc *fuzzutil.Script[*leaseHarness]) [][]byte {
	type step func(w *fuzzutil.Writer)
	derive := func(ms int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "derive"); w.Int(0, 200, ms) }
	}
	sleep := func(ms int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "sleep"); w.Int(0, 120, ms) }
	}
	release := func(i int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "release"); w.Int(0, 63, i) }
	}
	plain := func(name string) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, name) }
	}
	scripts := [][]step{
		// A handler that finishes in time.
		{derive(100), sleep(10), release(0), sleep(120)},
		// A handler that overruns its lease.
		{derive(50), sleep(60), release(0)},
		// Several leases, the later ones outliving the earlier.
		{derive(30), derive(60), derive(90), sleep(40), sleep(30), release(0), sleep(40)},
		// A lease that was already expired when it was derived.
		{plain("deriveLate"), sleep(1)},
		// The runner shuts down under the handlers.
		{derive(100), derive(100), plain("cancelParent"), sleep(10)},
	}
	kinds := Kinds()
	seeds := make([][]byte, 0, len(scripts)*len(kinds))
	for ki := range kinds {
		for i, steps := range scripts {
			w := fuzzutil.NewWriter()
			w.Choice(len(kinds), ki)
			w.Choice(len(fuzzTimers), i%len(fuzzTimers))
			w.Int(0, 50, (i%3)*10)
			for _, st := range steps {
				st(w)
			}
			seeds = append(seeds, w.Data())
		}
	}
	return seeds
}

// runLeaseScript runs one script inside a synctest bubble, where both kinds
// are deterministic: the bubble's clock drives context.WithTimeout and the
// Clock kind's scheduler alike, and a leaked goroutine is a bubble deadlock.
func runLeaseScript(t *testing.T, data []byte) {
	sc := leaseScript()
	r := fuzzutil.NewReader(data)
	k, tk, margin := leaseConfigFrom(r)
	rest := data[len(data)-r.Remaining():]
	synctest.Test(t, func(t *testing.T) {
		h := newLeaseHarness(k, tk, margin)
		defer h.close(t)
		sc.Run(t, rest, h)
	})
}

// FuzzLeaseDeadline is the differential fuzz target of A
// fuzz-chosen kind, deadline scheduler, margin and script of derives,
// releases, parent cancellations and advances of the bubble clock, checked
// after every step against the model of what each lease owes. Checked: a
// context is never done before its deadline, is always done once the deadline
// has passed, is cancelled by its release and by its parent, never changes
// the outcome it settled on, and never outlives the source.
func FuzzLeaseDeadline(f *testing.F) {
	sc := leaseScript()
	for _, s := range leaseSeeds(sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runLeaseScript(t, data)
	})
}
