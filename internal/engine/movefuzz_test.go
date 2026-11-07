package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/model/memq"
)

// harnessMoved and harnessRefused are the topics an explicit dead-letter move
// of the differential publishes its copy to. The first has no subscription,
// so it takes every copy and discards it, which is the sink the model moves
// to (memq DeadLetterEnd). The second has one subscription whose lanes hold
// one byte, under OverflowReject, so it refuses every copy as one it could
// never hold. Both keep a dedup window, which makes a copy read the engine
// clock before it is placed, and that read is where moveTrap parks it.
const (
	harnessMoved   = "moved"
	harnessRefused = "refused"
)

// moveCopySize is the size of a move's copy: larger than the refusing
// topic's lanes.
const moveCopySize = 2

// moveTopics creates the two topics a move publishes to.
func moveTopics(t testing.TB, eng *Engine[int64]) (moved, refused *Topic[int64]) {
	t.Helper()
	moved, err := eng.CreateTopic(harnessMoved, TopologyConfig{DedupWindow: time.Hour, CapacityMessages: -1})
	if err != nil {
		t.Fatalf("CreateTopic(%s): %v", harnessMoved, err)
	}
	refused, err = eng.CreateTopic(harnessRefused, TopologyConfig{
		DedupWindow: time.Hour, CapacityBytes: 1, Overflow: OverflowReject,
	})
	if err != nil {
		t.Fatalf("CreateTopic(%s): %v", harnessRefused, err)
	}
	if _, err := refused.Subscribe("r", SubscriptionConfig{}); err != nil {
		t.Fatalf("Subscribe(%s): %v", harnessRefused, err)
	}
	return moved, refused
}

// moveTrap is the engine clock of a run with moves. Armed, it parks the first
// clock read an explicit dead-letter move makes while publishing its copy
// until the script has run what it runs during the move; every other read is
// the clock's own. The read is told apart by the stack of the goroutine
// making it (calledFrom), because the engine's scheduler reads the same clock
// at the same time.
type moveTrap struct {
	clock.Clock
	armed   atomic.Bool
	parked  chan struct{}
	release chan struct{}
}

func newMoveTrap() *moveTrap {
	return &moveTrap{Clock: clock.Real{}, parked: make(chan struct{}), release: make(chan struct{})}
}

// Now parks the armed read until the script releases it.
func (m *moveTrap) Now() time.Time {
	if m.armed.Load() && calledFrom("(*Consumer[", ").DeadLetter") && calledFrom("(*Topic[", ").publish") &&
		m.armed.CompareAndSwap(true, false) {
		m.parked <- struct{}{}
		<-m.release
	}
	return m.Clock.Now()
}

// move runs an explicit dead-letter move (Consumer.DeadLetter, the memory
// driver's Reject) of a held delivery, optionally with a forged generation.
// The copy goes to harnessMoved, or to harnessRefused when refuse is set, and
// during runs further steps of the script while it is in flight, so a lease
// expiry, a reassignment, a close or a Seek can land on the move's
// settlement. The model sees the two halves of the move as DeadLetterBegin
// and DeadLetterEnd, with during's own steps between them.
func (h *harness) move(idx, delta int, refuse bool, during func()) {
	h.t.Helper()
	if len(h.held) == 0 {
		return
	}
	hd := h.held[idx%len(h.held)]
	sh := &h.subs[hd.sub]
	if hd.cons >= len(sh.cons) {
		return
	}
	cs := sh.cons[hd.cons]
	engGen := uint32(int64(hd.engGen) + int64(delta))
	modGen := uint32(int64(hd.modGen) + int64(delta))
	if ambiguous(hd, delta, engGen) {
		return
	}
	dlq := h.moved
	if refuse {
		dlq = h.refused
	}
	begin := memq.Op{Kind: memq.DeadLetterBegin, Sub: sh.name, Consumer: cs.id, Seq: hd.modSeq, Gen: modGen}
	h.trap.armed.Store(true)
	errc := make(chan error, 1)
	go func() {
		errc <- cs.c.DeadLetter(context.Background(), hd.env, engGen, dlq,
			Publish[int64]{Size: moveCopySize, Partition: -1})
	}()
	select {
	case <-h.trap.parked:
	case err := <-errc:
		// The settlement never began, so no copy was published.
		h.trap.armed.Store(false)
		res := h.step(begin)
		if res.Err == nil {
			h.t.Fatalf("deadletter: engine %v before the copy, model began the move", err)
		}
		h.sameErr("deadletter", err, res)
		return
	}
	if res := h.step(begin); res.Err != nil {
		h.t.Fatalf("deadletter: engine began the move, model %v", res.Err)
	}
	during()
	h.trap.release <- struct{}{}
	err := <-errc
	res := h.step(memq.Op{
		Kind: memq.DeadLetterEnd, Sub: sh.name, Consumer: cs.id, Seq: hd.modSeq, Gen: modGen, Refuse: refuse,
	})
	h.sameErr("deadletter", err, res)
}

// moveDuring is the steps a script may run while a move's copy is in flight:
// every step of the engine script but the engine's close, whose stopped
// acceptance refuses the copy for a reason of its own.
func moveDuring(steps []fuzzutil.Step[*harness]) []fuzzutil.Step[*harness] {
	var during []fuzzutil.Step[*harness]
	for _, st := range steps {
		if st.Name != "closeengine" {
			during = append(during, st)
		}
	}
	return during
}

// moveScript is the engine script with the explicit dead-letter move added:
// a held delivery, a generation offset, whether the copy is refused, and up to
// two further steps of the script run during the move.
func moveScript() *fuzzutil.Script[*harness] {
	sc := engineScript()
	during := moveDuring(sc.Steps)
	sc.Steps = append(sc.Steps, fuzzutil.Step[*harness]{
		Name: "deadletter", Weight: 4,
		Run: func(t testing.TB, r *fuzzutil.Reader, h *harness) {
			idx, delta, refuse, n := r.Choice(maxHandles), genDelta(r), r.Bool(), r.Int(0, 2)
			h.move(idx, delta, refuse, func() {
				for range n {
					during[r.Choice(len(during))].Run(t, r, h)
				}
			})
		},
	})
	return sc
}

// moveSeeds adds to the engine seeds the moves whose refused copy meets each
// withdrawal: an expiry of a record that has used its attempts, a sticky
// reassignment, a consumer's close and a Seek, and a copy taken with an
// expiry pending, which the settlement wins.
func moveSeeds(tb testing.TB, sc *fuzzutil.Script[*harness]) [][]byte {
	during := moveDuring(engineScript().Steps)
	inner := func(name string) int {
		for i, st := range during {
			if st.Name == name {
				return i
			}
		}
		tb.Fatalf("no step %q", name)
		return 0
	}
	set := 0
	for i, ks := range fuzzKindSets() {
		if r := ks.Resolve(); !r.Arch.Shared() && r.QueueOrdered() {
			set = i
			break
		}
	}
	type step func(w *fuzzutil.Writer)
	sub := func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, "subscribe"); w.Choice(maxSubs, 0) }
	attach := func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, "attach"); w.Choice(maxSubs, 0) }
	pub := func(part int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, "publish"); w.Int(-1, 2, part); w.Int(0, 32, 4) }
	}
	fetch := func(ci, n int) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(tb, w, "fetch")
			w.Choice(maxSubs, 0)
			w.Choice(maxConsumers, ci)
			w.Int(1, maxFetch, n)
		}
	}
	move := func(idx int, refuse bool, inners ...step) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(tb, w, "deadletter")
			w.Choice(maxHandles, idx)
			writeDelta(w, 0)
			w.Bool(refuse)
			w.Int(0, 2, len(inners))
			for _, in := range inners {
				in(w)
			}
		}
	}
	advance := func(w *fuzzutil.Writer) { w.Choice(len(during), inner("advance")); w.Int(1, 16, 16) }
	join := func(w *fuzzutil.Writer) { w.Choice(len(during), inner("attach")); w.Choice(maxSubs, 0) }
	leave := func(w *fuzzutil.Writer) {
		w.Choice(len(during), inner("closeconsumer"))
		w.Choice(maxSubs, 0)
		w.Choice(maxConsumers, 0)
	}
	seek := func(w *fuzzutil.Writer) {
		w.Choice(len(during), inner("seek"))
		w.Choice(maxSubs, 0)
		for i, k := range StartPositions() {
			if k == StartEarliest {
				w.Choice(len(StartPositions()), i)
			}
		}
		w.Int(0, 8, 0)
		w.Int(0, 16, 0)
	}
	type seed struct {
		parts    int
		attempts int
		stream   bool
		sticky   bool
		steps    []step
	}
	seeds := []seed{
		{parts: 1, attempts: 1, steps: []step{sub, attach, pub(0), fetch(0, 1), move(0, true, advance), fetch(0, 1)}},
		{parts: 2, sticky: true, steps: []step{sub, attach, pub(0), pub(1), fetch(0, 2), move(0, true, join), fetch(1, 2)}},
		{parts: 2, sticky: true, steps: []step{sub, attach, pub(0), pub(1), fetch(0, 2), move(1, true, join), fetch(1, 2)}},
		{parts: 1, steps: []step{sub, attach, pub(0), fetch(0, 1), move(0, true, leave), attach, fetch(1, 1)}},
		{parts: 1, stream: true, steps: []step{sub, attach, pub(0), fetch(0, 1), move(0, true, seek), fetch(0, 2)}},
		{parts: 1, attempts: 1, steps: []step{sub, attach, pub(0), fetch(0, 1), move(0, false, advance), fetch(0, 1)}},
	}
	out := engineSeeds(tb, sc)
	for _, s := range seeds {
		w := fuzzutil.NewWriter()
		w.Choice(len(fuzzKindSets()), set)
		w.Int(1, 3, s.parts)
		w.Int(1, 8, 4)
		w.Int(1, 20, 10)
		w.Int(0, 32, 0)
		w.Choice(len(RetryLanes()), 0)
		w.Choice(len(OrderingModes()), 0)
		w.Choice(len(scriptOverflows()), 0)
		w.Int(0, 4, s.attempts)
		w.Bool(true) // dead-letter topic
		w.Bool(s.stream)
		w.Int(1, 8, 4)
		w.Bool(false) // priority
		w.Bool(false) // wildcards
		w.Bool(false) // dedup
		w.Bool(s.sticky)
		for _, st := range s.steps {
			st(w)
		}
		out = append(out, w.Data())
	}
	return out
}

// FuzzEngineDeadLetterMove is FuzzEngineModel with the explicit dead-letter
// move (Consumer.DeadLetter, the memory driver's Reject) in the script: a held
// delivery is moved to a topic that takes the copy or to one that refuses it,
// with up to two further steps of the script running while the copy is in
// flight (harness.move). A lease expiry, a reassignment, a consumer's close or
// a Seek that lands then is left pending on the move's settlement, and a
// refused copy must carry it out as the withdrawal's own path would. It is a
// target of its own so that every corpus entry of the flagship keeps the
// meaning it was committed with.
func FuzzEngineDeadLetterMove(f *testing.F) {
	sc := moveScript()
	for _, s := range moveSeeds(f, sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runEngineScript(t, sc, data, func(hc *harnessConfig) { hc.moves = true })
	})
}
