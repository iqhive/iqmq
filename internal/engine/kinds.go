package engine

import (
	"bitbucket.org/iqhive/iqmq/internal/hash"
	"bitbucket.org/iqhive/iqmq/internal/pool"
	"bitbucket.org/iqhive/iqmq/internal/queue"
	"bitbucket.org/iqhive/iqmq/internal/registry"
	"bitbucket.org/iqhive/iqmq/internal/stats"
	"bitbucket.org/iqhive/iqmq/internal/timer"
	"bitbucket.org/iqhive/iqmq/internal/wait"
)

// KindSet selects one kind from every variant family the engine is assembled
// from (the memory engine EngineConfig). The zero value selects each
// family's Auto, which resolves to the bake-off winner recorded in that
// package's auto.go.
//
// Slice 3 completes the set: Arch and RetryLane.
type KindSet struct {
	// Arch is the fan-out storage architecture.
	Arch Arch
	// Queue is the lane ready-queue kind. Publishers and
	// consumers of a lane are both unbounded in number, so the kind must
	// declare queue.ModeMPMC; the single-producer and single-consumer kinds
	// are rejected by New rather than used unsafely. QueueKinds lists the
	// kinds an engine accepts.
	Queue queue.Kind
	// Wait is the consumer wait strategy.
	Wait wait.Kind
	// Timer is the delayed-delivery scheduler kind.
	Timer timer.Kind
	// Hash is the key→partition hash.
	Hash hash.Kind
	// Pool is the envelope and record pool kind.
	Pool pool.Kind
	// Registry is the topic registry kind.
	Registry registry.Kind
	// Stats is the counter kind used by Inspect.
	Stats stats.Kind
	// RetryLane is where an immediate redelivery is placed.
	RetryLane RetryLane
	// Safe forces every family to its mutex-backed reference kind, whatever
	// the other fields say. It is the differential-testing configuration:
	// the engine then contains no lock-free data structure.
	Safe bool
}

// SafeKinds is the reference configuration: the Safe kind of every family.
// Resolve returns it for any KindSet with Safe set.
var SafeKinds = KindSet{
	Arch:      PerSubEnvelopes,
	Queue:     queue.MutexRing,
	Wait:      wait.Chan,
	Timer:     timer.Heap,
	Hash:      hash.FNV1a,
	Pool:      pool.None,
	Registry:  registry.RWMutexMap,
	Stats:     stats.Atomic,
	RetryLane: BlockLane,
}

// Resolve maps every Auto to its family's AutoKind, or the whole set to
// SafeKinds when Safe is set. The returned set contains only concrete kinds,
// so String is stable and the engine never resolves twice.
func (ks KindSet) Resolve() KindSet {
	if ks.Safe {
		out := SafeKinds
		out.Safe = true
		return out
	}
	return KindSet{
		RetryLane: ks.RetryLane.Resolve(),

		Arch:     ks.Arch.Resolve(),
		Queue:    ks.Queue.Resolve(),
		Wait:     ks.Wait.Resolve(),
		Timer:    ks.Timer.Resolve(),
		Hash:     ks.Hash.Resolve(),
		Pool:     ks.Pool.Resolve(),
		Registry: ks.Registry.Resolve(),
		Stats:    ks.Stats.Resolve(),
	}
}

// Valid reports whether every field names a concrete kind or Auto and the
// ready-queue kind supports the concurrency the engine subjects it to.
func (ks KindSet) Valid() bool {
	if !ks.Arch.Valid() || !ks.Queue.Valid() || !ks.Wait.Valid() || !ks.Timer.Valid() || !ks.Hash.Valid() ||
		!ks.Pool.Valid() || !ks.Registry.Valid() || !ks.Stats.Valid() || !ks.RetryLane.Valid() {
		return false
	}
	return ks.Safe || ks.Queue.Resolve().Modes().Has(queue.ModeMPMC)
}

// QueueKinds lists the ready-queue kinds an engine can be built on: the
// members that declare queue.ModeMPMC. SPSC and MPSC are
// complete, model-tested and benchmarked kinds of that family, but a lane is
// fed by any number of publishers and drained by any number of consumers, so
// the engine narrows the family to the kinds that allow it rather than use a
// kind outside the modes it declares.
func QueueKinds() []queue.Kind {
	all := queue.Kinds()
	out := make([]queue.Kind, 0, len(all))
	for _, k := range all {
		if k.Modes().Has(queue.ModeMPMC) {
			out = append(out, k)
		}
	}
	return out
}

// String renders the resolved set as benchmark dimensions in a fixed order:
// "arch=…/queue=…/wait=…/timer=…/hash=…/pool=…/registry=…/stats=…/retry=…",
// with "/safe" appended for the reference configuration.
func (ks KindSet) String() string {
	r := ks.Resolve()
	n := len("arch=/queue=/wait=/timer=/hash=/pool=/registry=/stats=/retry=/safe") +
		len(r.Arch.String()) + len(r.Queue.String()) + len(r.Wait.String()) + len(r.Timer.String()) + len(r.Hash.String()) +
		len(r.Pool.String()) + len(r.Registry.String()) + len(r.Stats.String()) + len(r.RetryLane.String())
	b := make([]byte, 0, n)
	b = append(b, "arch="...)
	b = append(b, r.Arch.String()...)
	b = append(b, "/queue="...)
	b = append(b, r.Queue.String()...)
	b = append(b, "/wait="...)
	b = append(b, r.Wait.String()...)
	b = append(b, "/timer="...)
	b = append(b, r.Timer.String()...)
	b = append(b, "/hash="...)
	b = append(b, r.Hash.String()...)
	b = append(b, "/pool="...)
	b = append(b, r.Pool.String()...)
	b = append(b, "/registry="...)
	b = append(b, r.Registry.String()...)
	b = append(b, "/stats="...)
	b = append(b, r.Stats.String()...)
	b = append(b, "/retry="...)
	b = append(b, r.RetryLane.String()...)
	if r.Safe {
		b = append(b, "/safe"...)
	}
	return string(b)
}

// Ordered reports whether the engine delivers the records of a lane in
// publication order. ShardedMPMC does not and the shared-log architecture
// serves redeliveries before the log; either relaxes the per-lane ordering
// checks of the model tests.
func (ks KindSet) Ordered() bool { return ks.QueueOrdered() && ks.Resolve().RetryLane.Ordered() }

// QueueOrdered reports whether the structures a record passes through on its
// way to a consumer preserve publication order, leaving the retry placement
// out of it. The distinction matters to a differential test: an
// unordered queue kind makes the delivery order unpredictable and the model
// has to relax, while a retry placement is a deterministic function of the
// kind and the record sequence, which the model reproduces exactly.
//
// The scheduler kind is deliberately NOT a term here, although
// timer.Kind.Ordered() is false for the three wheels. Nothing the engine
// delivers is in the order a scheduler fired it: every store sorts the batch
// it promotes by (due time, publication sequence) in sortFired, so the order
// a lane serves its delayed records in is the topology's and not the
// scheduler kind's. What the wheels give up is only the order *within* one
// firing, and no wheel can fire a record while one due earlier is still
// armed -- ceilTick is non-decreasing in the due time, so a target that has
// reached the later record's tick has already reached the earlier one's
// (internal/timer's TestFireNeverInvertsDueOrder). Folding the kind in here
// was written before sortFired existed and would take the message contract's
// strict ordering away from the default engine for a reordering the engine
// does not perform.
func (ks KindSet) QueueOrdered() bool {
	r := ks.Resolve()
	return r.Queue.Ordered() && r.Arch.Ordered()
}
