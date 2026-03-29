package engine

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// engineScript is the op-script differential between the engine and
// internal/model/memq. Every step applies the same operation to both and the
// script's Check compares every observable after it.
func engineScript() *fuzzutil.Script[*harness] {
	return &fuzzutil.Script[*harness]{
		MaxSteps: 512,
		Steps: []fuzzutil.Step[*harness]{
			{Name: "subscribe", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.subscribe(r.Choice(maxSubs))
			}},
			{Name: "attach", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.attach(r.Choice(maxSubs))
			}},
			{Name: "publish", Weight: 8, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.publish(r.Int(-1, 2), r.Int(0, 32), 0)
			}},
			{Name: "publishdelayed", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.publish(r.Int(-1, 2), r.Int(0, 32), time.Duration(r.Int(1, 8))*tick)
			}},
			{Name: "publishprio", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.publishFull(r.Int(-1, 2), r.Int(0, 32), 0, uint8(r.Choice(PriorityLevels+1)), "")
			}},
			{Name: "publishdedup", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.publishFull(r.Int(-1, 2), r.Int(0, 32), 0, 0, dedupKeys[r.Choice(len(dedupKeys))])
			}},
			{Name: "seek", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.seek(r.Choice(maxSubs), StartPositions()[r.Choice(len(StartPositions()))],
					uint64(r.Int(0, 8)), time.Duration(r.Int(0, 16))*tick)
			}},
			{Name: "rebalance", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.rebalance(r.Choice(maxSubs))
			}},
			{Name: "fetch", Weight: 6, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.fetch(r.Choice(maxSubs), r.Choice(maxConsumers), r.Int(1, maxFetch))
			}},
			{Name: "ack", Weight: 5, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.settle(r.Choice(maxHandles), true, genDelta(r), 0)
			}},
			{Name: "nack", Weight: 3, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				delay := time.Duration(0)
				if r.Bool() {
					delay = time.Duration(r.Int(1, 8)) * tick
				}
				h.settle(r.Choice(maxHandles), false, genDelta(r), delay)
			}},
			{Name: "extend", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.extend(r.Choice(maxHandles), genDelta(r), time.Duration(r.Int(0, 20))*tick)
			}},
			{Name: "advance", Weight: 3, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.advance(time.Duration(r.Int(1, 16)) * tick)
			}},
			{Name: "expire", Weight: 2, Run: func(_ testing.TB, _ *fuzzutil.Reader, h *harness) {
				h.expire()
			}},
			{Name: "closeconsumer", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.closeConsumer(r.Choice(maxSubs), r.Choice(maxConsumers))
			}},
			{Name: "closesub", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, h *harness) {
				h.closeSub(r.Choice(maxSubs))
			}},
			{Name: "closeengine", Weight: 1, Run: func(_ testing.TB, _ *fuzzutil.Reader, h *harness) {
				h.closeEngine()
			}},
		},
		Check: func(_ testing.TB, h *harness) { h.check() },
	}
}

// dedupKeys are the deduplication keys a script publishes with. A small set
// makes a collision inside the window likely, which is the case the window
// exists for; the empty key is one of them, because a publication without one
// is never deduplicated.
var dedupKeys = []string{"", "k0", "k1"}

// genDelta picks the generation offset a settlement presents: the live one,
// one behind, one ahead, or an arbitrary forgery.
func genDelta(r *fuzzutil.Reader) int {
	switch r.Choice(6) {
	case 0:
		return -1
	case 1:
		return 1
	case 2:
		return -int(r.Byte())
	default:
		return 0
	}
}

// benchOverflows are the overflow policies a script may choose. OverflowBlock
// is not one of them: a script publishes from the goroutine that drives it and
// a parked publisher would stop the script rather than exercise it, so
// blocking has its own tests, which park a publisher on purpose.
func scriptOverflows() []OverflowPolicy {
	return []OverflowPolicy{OverflowReject, OverflowDropNewest, OverflowDropOldest}
}

// fuzzKindSets is the kind-set table a script's first choice indexes. It is
// deliberately *not* kindMatrix(matrixMode()): a committed corpus entry must
// mean the same engine under every ENGINE_MATRIX value, and a mode-dependent
// table makes a minimised crasher reproduce a different configuration than
// the one it was found in — under ENGINE_MATRIX=full it also rebuilds a
// million-entry slice on every execution. The pairwise table is the widest
// one a corpus entry can name and is built once.
var fuzzKindSets = sync.OnceValue(func() []KindSet { return kindMatrix("pairwise") })

// engineConfigFrom decodes the engine shape from the head of the script.
func engineConfigFrom(r *fuzzutil.Reader) harnessConfig {
	sets := fuzzKindSets()
	hc := harnessConfig{
		kinds:       sets[r.Choice(len(sets))],
		partitions:  r.Int(1, 3),
		capMessages: r.Int(1, 8),
		ackDeadline: time.Duration(r.Int(1, 20)) * tick,
		maxTotal:    r.Int(0, 32),
	}
	hc.kinds.RetryLane = RetryLanes()[r.Choice(len(RetryLanes()))]
	hc.ordering = OrderingModes()[r.Choice(len(OrderingModes()))]
	hc.overflow = scriptOverflows()[r.Choice(len(scriptOverflows()))]
	hc.maxAttempts = uint32(r.Int(0, 4))
	hc.deadLetter = r.Bool()
	// Slice-4 topology. Each knob is decoded whether or not the run ends up
	// using it, so that one script byte always means the same thing.
	hc.stream = r.Bool()
	hc.retention = Retention{MaxMessages: r.Int(1, 8)}
	hc.priority = r.Bool()
	hc.wildcards = r.Bool()
	if r.Bool() {
		hc.dedup = time.Duration(r.Int(1, 16)) * tick
	}
	hc.sticky = r.Bool()
	return hc
}

// writeConfig mirrors engineConfigFrom for readable seeds.
func writeConfig(w *fuzzutil.Writer, set, parts, capacity, deadline, total int) {
	w.Choice(len(fuzzKindSets()), set)
	w.Int(1, 3, parts)
	w.Int(1, 8, capacity)
	w.Int(1, 20, deadline)
	w.Int(0, 32, total)
	w.Choice(len(RetryLanes()), 0)
	w.Choice(len(OrderingModes()), 0)
	w.Choice(len(scriptOverflows()), 0)
	w.Int(0, 4, 0)
	w.Bool(false)
	w.Bool(false) // stream
	w.Int(1, 8, 4)
	w.Bool(false) // priority
	w.Bool(false) // wildcards
	w.Bool(false) // dedup
	w.Bool(false) // sticky
}

// engineSeeds authors readable scripts covering the paths the invariants care
// about most: fan-out, redelivery, delay, expiry, close and stale handles.
func engineSeeds(sc *fuzzutil.Script[*harness]) [][]byte {
	type step func(w *fuzzutil.Writer)
	sub := func(i int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "subscribe"); w.Choice(maxSubs, i) }
	}
	attach := func(i int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "attach"); w.Choice(maxSubs, i) }
	}
	pub := func(part, size int) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(w, "publish")
			w.Int(-1, 2, part)
			w.Int(0, 32, size)
		}
	}
	pubDelay := func(part, size, ticks int) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(w, "publishdelayed")
			w.Int(-1, 2, part)
			w.Int(0, 32, size)
			w.Int(1, 8, ticks)
		}
	}
	fetch := func(si, ci, n int) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(w, "fetch")
			w.Choice(maxSubs, si)
			w.Choice(maxConsumers, ci)
			w.Int(1, maxFetch, n)
		}
	}
	settle := func(name string, idx, delta int) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(w, name)
			w.Choice(maxHandles, idx)
			if name == "nack" {
				w.Bool(false)
			}
			writeDelta(w, delta)
		}
	}
	advance := func(ticks int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "advance"); w.Int(1, 16, ticks) }
	}
	expire := func() step { return func(w *fuzzutil.Writer) { sc.WriteStep(w, "expire") } }
	closeEng := func() step { return func(w *fuzzutil.Writer) { sc.WriteStep(w, "closeengine") } }
	closeCons := func(si, ci int) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(w, "closeconsumer")
			w.Choice(maxSubs, si)
			w.Choice(maxConsumers, ci)
		}
	}

	scripts := [][]step{
		// Fan-out to two subscriptions, then settle one of them.
		{sub(0), sub(1), attach(0), attach(1), pub(0, 8), pub(0, 8), fetch(0, 0, 2), settle("ack", 0, 0), settle("ack", 0, 0), fetch(1, 0, 2), settle("ack", 0, 0)},
		// Redelivery through nack and through lease expiry.
		{sub(0), attach(0), pub(0, 4), fetch(0, 0, 1), settle("nack", 0, 0), fetch(0, 0, 1), advance(16), expire(), fetch(0, 0, 1), settle("ack", 0, 0)},
		// Delayed publication is not delivered early.
		{sub(0), attach(0), pubDelay(0, 4, 4), fetch(0, 0, 2), advance(4), fetch(0, 0, 2), settle("ack", 0, 0)},
		// Stale handles: one behind and one ahead of the live generation.
		{sub(0), attach(0), pub(0, 4), fetch(0, 0, 1), settle("ack", 0, -1), settle("ack", 0, 1), settle("ack", 0, 0)},
		// Close with records in every state.
		{sub(0), sub(1), attach(0), attach(1), pub(0, 8), pubDelay(0, 8, 8), fetch(0, 0, 2), closeEng(), fetch(0, 0, 2)},
		// A consumer that closes while it holds a lease: every settlement
		// through it afterwards is told the consumer closed, which
		// is the half of the withdrawal rule the rebalance crashers do not
		// reach.
		{sub(0), attach(0), pub(0, 4), pub(1, 4), fetch(0, 0, 2), closeCons(0, 0), settle("ack", 0, 0), settle("nack", 1, 0), settle("ack", 1, 0)},
		// A join that moves a lane away from a consumer still holding a
		// lease on it, and then that consumer's close: the moved delivery is
		// told its lease is gone, the kept one that the consumer closed
		// (under FuzzEngineRebalance, which makes every run sticky).
		{sub(0), attach(0), pub(0, 4), pub(1, 4), fetch(0, 0, 2), attach(0), settle("ack", 0, 0), closeCons(0, 0), settle("ack", 1, 0), settle("ack", 0, 0)},
	}
	seeds := make([][]byte, 0, len(scripts))
	for i, steps := range scripts {
		w := fuzzutil.NewWriter()
		writeConfig(w, 0, 1+i%3, 4, 10, 0)
		for _, st := range steps {
			st(w)
		}
		seeds = append(seeds, w.Data())
	}
	return seeds
}

// writeDelta mirrors genDelta.
func writeDelta(w *fuzzutil.Writer, delta int) {
	switch delta {
	case -1:
		w.Choice(6, 0)
	case 1:
		w.Choice(6, 1)
	default:
		w.Choice(6, 3)
	}
}

// runEngineScript decodes a script's configuration, lets the target narrow it
// and drives the engine and the model through the rest of it inside a
// synctest bubble, where the clock is deterministic and a goroutine the
// engine forgot surfaces as a bubble deadlock.
func runEngineScript(t *testing.T, sc *fuzzutil.Script[*harness], data []byte, adjust func(*harnessConfig)) {
	t.Helper()
	r := fuzzutil.NewReader(data)
	cfg := engineConfigFrom(r)
	if adjust != nil {
		adjust(&cfg)
	}
	rest := data[len(data)-r.Remaining():]
	t.Logf("engine %v partitions=%d capacity=%d deadline=%v total=%d overflow=%v ordering=%v "+
		"attempts=%d dlq=%v stream=%v retention=%+v priority=%v wildcards=%v dedup=%v sticky=%v",
		cfg.kinds.Resolve(), cfg.partitions, cfg.capMessages, cfg.ackDeadline, cfg.maxTotal,
		cfg.overflow, cfg.ordering, cfg.maxAttempts, cfg.deadLetter,
		cfg.stream, cfg.retention, cfg.priority, cfg.wildcards, cfg.dedup, cfg.sticky)
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, cfg)
		defer h.close()
		sc.Run(t, rest, h)
		h.check()
	})
}

// FuzzEngineModel is the flagship differential target: a fuzz-chosen kind
// set, topology and op script are applied to the engine and to
// internal/model/memq inside a synctest bubble, and every result, counter,
// gauge, release callback and invariant must agree after every step.
func FuzzEngineModel(f *testing.F) {
	sc := engineScript()
	for _, s := range engineSeeds(sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runEngineScript(t, sc, data, nil)
	})
}

// FuzzEngineRebalance concentrates the differential on the versioned sticky
// assignment of the memory engine: every run has more than one lane and an
// exclusive assignment, so every attach, every consumer close and every
// explicit rebalance moves lanes between consumers and revokes the leases the
// new table no longer allows. What is checked is what the flagship target
// checks — every result, counter, gauge and invariant — with the assignment
// generation and the lane ownership among them: harness.check compares both
// Snapshot.Assignment and the table Subscription.Assignment returns against
// the model's, so a version that counts differently and a table that hands
// the same lanes to different consumers are each caught.
func FuzzEngineRebalance(f *testing.F) {
	sc := engineScript()
	for _, s := range engineSeeds(sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runEngineScript(t, sc, data, func(hc *harnessConfig) {
			hc.sticky = true
			if hc.partitions < 2 {
				hc.partitions = 2
			}
			// A rebalance moves records between lanes rather than between
			// states, so the run keeps the topologies that make the model's
			// prediction exact.
			hc.stream, hc.priority, hc.wildcards = false, false, false
		})
	})
}

// FuzzEngineRetention concentrates the differential on the stream topology of
// the memory engine: every run retains, so every publication appends to the
// retained log, the bounds trim it, and every Seek discards what a
// subscription holds and replays the log from a position. The release ledger
// is the sharp edge — a retained record holds a reference of its own, so an
// owned payload must not come back until retention has let go of it — and the
// harness checks it after every step.
func FuzzEngineRetention(f *testing.F) {
	sc := engineScript()
	for _, s := range engineSeeds(sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runEngineScript(t, sc, data, func(hc *harnessConfig) {
			hc.stream = true
			hc.sticky = false
			if !hc.retention.Enabled() {
				hc.retention.MaxMessages = 4
			}
		})
	})
}
