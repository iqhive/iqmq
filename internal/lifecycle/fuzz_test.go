package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/clock"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
	"bitbucket.org/iqhive/iqmq/internal/model/shutdown"
)

// handle is the fake handle FuzzShutdown drives: a store of records with a
// release ledger, closed by a Lifecycle whose steps drain it. It is the
// smallest thing that can lose a record the way the memory engine forbids —
// a placement that lands after the drain is a payload nothing releases.
type handle struct {
	lc *Lifecycle

	mu       sync.Mutex
	state    []shutdown.State
	releases []int
	// ticket holds the admission ticket of a record between its two steps.
	ticket []Ticket
	// dlq holds the dead-letter publication of a record that has one.
	dlq []*Pending
	// drained is set by the settling step; a placement that finds it set has
	// escaped the drain, which is the fault the whole package prevents.
	drained bool
	// escaped names the first record that did.
	escaped int

	wake   chan struct{}
	finish chan struct{}
	once   sync.Once
}

func newHandle(records int, drain time.Duration, clk clock.Clock) *handle {
	h := &handle{
		state:    make([]shutdown.State, records),
		releases: make([]int, records),
		ticket:   make([]Ticket, records),
		dlq:      make([]*Pending, records),
		escaped:  -1,
		wake:     make(chan struct{}, 1),
		finish:   make(chan struct{}),
	}
	h.lc = New(Config{
		Clock:        clk,
		DrainTimeout: drain,
		Steps: Steps{
			Drain:  h.drain,
			Settle: h.settle,
		},
	})
	return h
}

// drain waits for the records a consumer holds, or for the deadline, or for
// the driver to say the script is over — which is what keeps a fuzz input
// from hanging a graceful shutdown for ever.
func (h *handle) drain(ctx context.Context) error {
	for {
		if h.count(shutdown.InFlight) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.finish:
			return nil
		case <-h.wake:
		}
	}
}

// settle drops every record the store still holds, releasing each once.
func (h *handle) settle() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.drained = true
	for i := range h.state {
		switch h.state[i] {
		case shutdown.Held, shutdown.InFlight:
			h.state[i] = shutdown.Dropped
			h.releases[i]++
		case shutdown.Unpublished, shutdown.Admitting, shutdown.Claiming,
			shutdown.DLQPending, shutdown.Acked, shutdown.DeadLettered, shutdown.Dropped:
		}
	}
}

func (h *handle) count(s shutdown.State) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for i := range h.state {
		if h.state[i] == s {
			n++
		}
	}
	return n
}

func (h *handle) signal() {
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

// place puts a record in the store, and records it if the drain has already
// run: with the gate held that is impossible, and it is the whole point.
func (h *handle) place(i int, to shutdown.State) {
	h.mu.Lock()
	if h.drained && h.escaped < 0 {
		h.escaped = i
	}
	h.state[i] = to
	h.mu.Unlock()
}

func (h *handle) set(i int, to shutdown.State) {
	h.mu.Lock()
	h.state[i] = to
	h.mu.Unlock()
}

func (h *handle) at(i int) shutdown.State {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state[i]
}

// stop lets a parked drain finish and joins every closer.
func (h *handle) stop() { h.once.Do(func() { close(h.finish) }) }

// FuzzShutdown drives internal/lifecycle and internal/model/shutdown with the
// same script of admissions, placements, fetches, settlements, dead-letter
// publications, clock advances and closes (the test suite shutdown state machine, no double close, drains).
//
// Before a close is asked for, the two must agree operation by operation: the
// same admissions accepted and refused, the same record states, the same
// release ledger. Once a close is under way the lifecycle runs its phases on
// a goroutine of its own while the model runs them synchronously, so the two
// cannot be stepped together; from there the script checks the model's
// invariants and the handle's own — nothing is placed after the drain, every
// record is released exactly once, and once the run is joined the handle
// holds nothing and admits nothing.
func FuzzShutdown(f *testing.F) {
	f.Add(seedScript(2, 2, 4,
		op(shutdown.Admit, 0), op(shutdown.Place, 0), op(shutdown.Claim, 0),
		op(shutdown.Take, 0), op(shutdown.Ack, 0), op(shutdown.Close, 0),
	))
	f.Add(seedScript(3, 1, 4,
		op(shutdown.Admit, 1), op(shutdown.Place, 1), op(shutdown.Claim, 1),
		op(shutdown.Take, 1), op(shutdown.DeadLetter, 1), op(shutdown.Shutdown, 1),
		op(shutdown.CommitDeadLetter, 1),
	))
	f.Add(seedScript(2, 2, 2,
		op(shutdown.Admit, 0), op(shutdown.Place, 1), op(shutdown.Claim, 0),
		op(shutdown.Take, 1), op(shutdown.Shutdown, 0), op(shutdown.Tick, 1),
		op(shutdown.Tick, 0), op(shutdown.Tick, 1),
	))
	f.Add(seedScript(2, 1, 4,
		op(shutdown.Admit, 0), op(shutdown.Close, 1), op(shutdown.Place, 0),
		op(shutdown.Admit, 1),
	))
	// The two scripts below were found by the fuzzer and were committed as
	// raw corpus files. A minimised crasher is a position in a byte string,
	// not a description of the run it was found on: extending the header or
	// the step table re-points it at a different script, and it then passes
	// for a reason that has nothing to do with what it covered
	// (adr/ADR-017-assumptions.md). They are written out here
	// instead, where the decoder change that would re-point them breaks the
	// build rather than the coverage. Neither pins a defect in this package:
	// they were the inputs that found two faults in this harness — a prompt
	// Close joining a graceful run inherits that run's drain timeout, and a
	// settlement not taken under the same mutex can release a record twice.
	//
	// Every field is carried over, not just the kinds and the records:
	// `delay` is the clock schedule (`clk.Advance`, `Model.Advance`) and it
	// is what decides whether a run reaches its drain deadline at all. The
	// second script's ticks advance 4s, 7s and 5s against a 5s deadline, and
	// with them flattened to op's one second it never reached the deadline
	// its original crossed — a graceful run that never times out, which is
	// most of what that entry covered.
	f.Add(seedScript(2, 3, 3,
		opBy(shutdown.Admit, 0, 0, 1), opBy(shutdown.Place, 0, 2, 3),
		opBy(shutdown.Ack, 1, 2, 3), opBy(shutdown.Ack, 3, 0, 3),
		opBy(shutdown.Close, 0, 2, 6), opBy(shutdown.Claim, 0, 0, 7),
		opBy(shutdown.DeadLetter, 0, 2, 6), opBy(shutdown.CommitDeadLetter, 3, 1, 5),
		opBy(shutdown.Tick, 0, 2, 3), opBy(shutdown.Take, 0, 2, 3),
		opBy(shutdown.Close, 3, 2, 6), opBy(shutdown.CommitDeadLetter, 0, 0, 3),
		opBy(shutdown.Tick, 3, 2, 5), opBy(shutdown.DeadLetter, 0, 0, 0),
	))
	f.Add(seedScript(4, 3, 5,
		opBy(shutdown.Shutdown, 0, 0, 5), opBy(shutdown.Admit, 3, 0, 0),
		opBy(shutdown.Place, 3, 0, 5), opBy(shutdown.Claim, 0, 2, 5),
		opBy(shutdown.Claim, 3, 2, 2), opBy(shutdown.Tick, 3, 2, 4),
		opBy(shutdown.Place, 1, 0, 5), opBy(shutdown.Claim, 3, 1, 1),
		opBy(shutdown.FailDeadLetter, 2, 2, 4), opBy(shutdown.Tick, 2, 0, 7),
		opBy(shutdown.Tick, 1, 1, 5), opBy(shutdown.CommitDeadLetter, 1, 0, 2),
		opBy(shutdown.Place, 1, 1, 7), opBy(shutdown.DeadLetter, 1, 0, 0),
	))
	f.Add([]byte{2})
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		records := r.Int(1, 4)
		closers := r.Int(1, 3)
		drain := time.Duration(r.Int(0, 8)) * time.Second
		clk := clock.NewFake(time.Unix(0, 0))
		h := newHandle(records, drain, clk)
		m := shutdown.New(shutdown.Config{
			Records: records, Closers: closers, DrainTimeout: drain,
		})
		var wg sync.WaitGroup
		defer func() {
			// Whatever the script left half-done, let the run finish. A
			// half-finished operation is completed rather than abandoned,
			// because that is what the interval between Enter and Leave
			// means: the placement always happens. It cannot land behind the
			// drain — the phase that would drain is waiting for this very
			// ticket — which is why the escape check still holds here.
			for i := range records {
				switch h.at(i) {
				case shutdown.Admitting:
					h.place(i, shutdown.Held)
				case shutdown.Claiming:
					h.place(i, shutdown.InFlight)
				case shutdown.Unpublished, shutdown.Held, shutdown.InFlight,
					shutdown.DLQPending, shutdown.Acked, shutdown.DeadLettered,
					shutdown.Dropped:
				}
				h.lc.Leave(h.ticket[i])
				h.ticket[i] = Ticket{}
				if p := h.dlq[i]; p != nil {
					p.Abort()
					h.dlq[i] = nil
					h.mu.Lock()
					if h.state[i] == shutdown.DLQPending {
						h.state[i] = shutdown.Dropped
						h.releases[i]++
					}
					h.mu.Unlock()
				}
			}
			h.stop()
			h.signal()
			wg.Wait()
			// Joining the run reports its outcome, which is the point of
			// CloseIdempotent: a graceful run whose drain deadline passed
			// returns ErrDrainTimeout to every caller, prompt ones included.
			if err := h.lc.Close(); err != nil && !errors.Is(err, ErrDrainTimeout) {
				t.Fatalf("final Close: %v", err)
			}
			h.check(t, m)
		}()

		closing := false
		for !r.Exhausted() {
			op := shutdown.Op{
				Kind:   shutdown.Kinds()[r.Choice(len(shutdown.Kinds()))],
				Rec:    r.Int(0, 3),
				Closer: r.Int(0, 2),
				Delay:  time.Duration(r.Int(0, 8)) * time.Second,
			}
			want := m.Step(op)
			if err := m.Invariants(); err != nil {
				t.Fatalf("model after %s(rec=%d): %v", op.Kind, op.Rec, err)
			}
			got, started := h.apply(t, &wg, clk, op)
			closing = closing || started
			if !closing && op.Rec < records && got != want.State {
				t.Fatalf("after %s(rec=%d): handle says %s, model says %s",
					op.Kind, op.Rec, got, want.State)
			}
			h.escapeCheck(t, op)
		}
	})
}

// fuzzStep is one operation of a written seed script.
type fuzzStep struct {
	kind   shutdown.Kind
	rec    int
	closer int
	delay  int
}

// op is one step against record rec, by the first closer, advancing one
// second where the kind reads a delay. It is what a seed written by hand
// uses: the fields it fixes are the ones such a seed has no reason to vary.
func op(k shutdown.Kind, rec int) fuzzStep {
	return fuzzStep{kind: k, rec: rec, delay: 1}
}

// opBy is one step with every field spelled out, for a seed transcribed from
// a corpus entry: closer says which closer the step acts as, and delay how
// many seconds the clock advances where the kind reads one. A transcription
// that let these default would be a different script — the delays are the
// clock schedule, and a drain deadline is reached or not reached by them.
func opBy(k shutdown.Kind, rec, closer, delay int) fuzzStep {
	return fuzzStep{kind: k, rec: rec, closer: closer, delay: delay}
}

// seedScript writes a seed in the encoding FuzzShutdown's reader expects, so
// that a seed says what it drives instead of being an opaque byte string.
func seedScript(records, closers, drain int, steps ...fuzzStep) []byte {
	w := fuzzutil.NewWriter()
	w.Int(1, 4, records)
	w.Int(1, 3, closers)
	w.Int(0, 8, drain)
	for _, st := range steps {
		w.Choice(len(shutdown.Kinds()), int(st.kind))
		w.Int(0, 3, st.rec)
		w.Int(0, 2, st.closer)
		w.Int(0, 8, st.delay)
	}
	return w.Data()
}

// escapeCheck fails as soon as a placement lands behind the drain.
func (h *handle) escapeCheck(t testing.TB, op shutdown.Op) {
	t.Helper()
	h.mu.Lock()
	i := h.escaped
	h.mu.Unlock()
	if i >= 0 {
		t.Fatalf("after %s(rec=%d): record %d was placed after the drain", op.Kind, op.Rec, i)
	}
}

// apply performs one operation on the handle and reports the record's state
// afterwards, and whether the operation started a shutdown run.
func (h *handle) apply(t testing.TB, wg *sync.WaitGroup, clk *clock.Fake, op shutdown.Op) (shutdown.State, bool) {
	t.Helper()
	i := op.Rec
	if i >= len(h.state) {
		switch op.Kind {
		case shutdown.Close:
			h.start(wg, false)
			return shutdown.Unpublished, true
		case shutdown.Shutdown:
			h.start(wg, true)
			return shutdown.Unpublished, true
		case shutdown.Tick:
			clk.Advance(op.Delay)
			return shutdown.Unpublished, false
		default:
			return shutdown.Unpublished, false
		}
	}
	switch op.Kind {
	case shutdown.Admit:
		if h.at(i) == shutdown.Unpublished {
			tk := h.lc.Enter(OpPublish)
			if tk.OK() {
				h.ticket[i] = tk
				h.set(i, shutdown.Admitting)
			}
		}
	case shutdown.Place:
		if h.at(i) == shutdown.Admitting {
			h.place(i, shutdown.Held)
			h.lc.Leave(h.ticket[i])
			h.ticket[i] = Ticket{}
		}
	case shutdown.Claim:
		if h.at(i) == shutdown.Held {
			tk := h.lc.Enter(OpFetch)
			if tk.OK() {
				h.ticket[i] = tk
				h.set(i, shutdown.Claiming)
			}
		}
	case shutdown.Take:
		if h.at(i) == shutdown.Claiming {
			h.place(i, shutdown.InFlight)
			h.lc.Leave(h.ticket[i])
			h.ticket[i] = Ticket{}
		}
	case shutdown.Ack:
		// The settling step takes the same mutex, so a settlement and the
		// drain cannot both act on one record — which is what the lane
		// mutex and the settlement cell do in the engine.
		h.mu.Lock()
		if h.state[i] == shutdown.InFlight {
			tk := h.lc.Enter(OpSettle)
			if tk.OK() {
				h.state[i] = shutdown.Acked
				h.releases[i]++
			}
			h.lc.Leave(tk)
		}
		h.mu.Unlock()
		h.signal()
	case shutdown.DeadLetter:
		h.mu.Lock()
		if h.state[i] == shutdown.InFlight && h.dlq[i] == nil {
			if p := h.lc.BeginDeadLetter(); p != nil {
				h.dlq[i] = p
				h.state[i] = shutdown.DLQPending
			}
		}
		h.mu.Unlock()
		h.signal()
	case shutdown.CommitDeadLetter:
		h.mu.Lock()
		if p := h.dlq[i]; p != nil && h.state[i] == shutdown.DLQPending {
			p.Commit()
			h.dlq[i] = nil
			// Only a committed publication settles the original: the
			// decision is read from the hook, not taken again here.
			if p.SettleAllowed() {
				h.state[i] = shutdown.DeadLettered
				h.releases[i]++
			}
		}
		h.mu.Unlock()
	case shutdown.FailDeadLetter:
		h.mu.Lock()
		if p := h.dlq[i]; p != nil && h.state[i] == shutdown.DLQPending {
			p.Abort()
			h.dlq[i] = nil
			if h.drained {
				// There is no lane left to put it back into.
				h.state[i] = shutdown.Dropped
				h.releases[i]++
			} else {
				h.state[i] = shutdown.Held
			}
		}
		h.mu.Unlock()
	case shutdown.Close:
		h.start(wg, false)
		return h.at(i), true
	case shutdown.Shutdown:
		h.start(wg, true)
		return h.at(i), true
	case shutdown.Tick:
		clk.Advance(op.Delay)
	}
	return h.at(i), false
}

// start asks for a shutdown on a goroutine of its own, because a graceful one
// blocks until the drain is done and the script has to keep running.
func (h *handle) start(wg *sync.WaitGroup, graceful bool) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		if graceful {
			_ = h.lc.Shutdown(context.Background())
			return
		}
		_ = h.lc.Close()
	}()
}

// check is what must hold once the run has been joined.
func (h *handle) check(t testing.TB, m *shutdown.Model) {
	t.Helper()
	if err := m.Invariants(); err != nil {
		t.Fatalf("model: %v", err)
	}
	if got := h.lc.Phase(); got != Closed {
		t.Fatalf("phase %s after the run was joined", got)
	}
	for _, op := range Ops() {
		if tk := h.lc.Enter(op); tk.OK() {
			h.lc.Leave(tk)
			t.Fatalf("%s admitted after close", op)
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.escaped >= 0 {
		t.Fatalf("record %d was placed after the drain", h.escaped)
	}
	for i, st := range h.state {
		switch st {
		case shutdown.Held, shutdown.InFlight, shutdown.Admitting, shutdown.Claiming:
			t.Fatalf("record %d is %s after the handle closed", i, st)
		case shutdown.Unpublished, shutdown.DLQPending, shutdown.Acked,
			shutdown.DeadLettered, shutdown.Dropped:
		}
		want := 0
		if st.Terminal() {
			want = 1
		}
		if h.releases[i] != want {
			t.Fatalf("record %d is %s with %d release(s), want %d", i, st, h.releases[i], want)
		}
	}
}
