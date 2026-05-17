package memory

import (
	"bitbucket.org/iqhive/iqmq/internal/engine"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/hash"
	"bitbucket.org/iqhive/iqmq/internal/pool"
	"bitbucket.org/iqhive/iqmq/internal/queue"
	"bitbucket.org/iqhive/iqmq/internal/registry"
	"bitbucket.org/iqhive/iqmq/internal/stats"
	"bitbucket.org/iqhive/iqmq/internal/timer"
	"bitbucket.org/iqhive/iqmq/internal/wait"
)

// EngineConfig selects one kind from every variant family the engine is
// assembled from (the memory engine).
//
// It mirrors the engine's own kind set: every family the engine can be built
// on has a field here, so a caller can pin the whole engine and a differential
// test can sweep it. The zero value selects each family's Auto, which resolves
// to the bake-off winner ADR-014 records; Safe overrides the lot with each
// family's mutex-backed reference kind, which is the configuration that
// contains no lock-free data structure at all.
//
// Every kind of every family ships (the literal Phase 0): none of them is
// a stub, and a losing kind stays selectable so that a test can compare the
// engine against its own reference.
type EngineConfig struct {
	// Queue is the lane ready-queue kind. A lane is fed by any
	// number of publishers and drained by any number of consumers, so the
	// single-producer and single-consumer specialisations are refused
	// rather than used unsafely.
	Queue QueueKind
	// Arch is the fan-out storage architecture.
	Arch ArchKind
	// Wait is the consumer wait strategy.
	Wait WaitKind
	// Timer is the delayed-delivery scheduler kind.
	Timer TimerKind
	// Hash is the key-to-lane hash.
	Hash HashKind
	// Pool is the envelope and record pool kind.
	Pool PoolKind
	// Registry is the topic registry kind.
	Registry RegistryKind
	// Stats is the counter layout Inspect reads. The API contract
	// the sketch omits it; it is a family of the engine's kind set, so it
	// is pinnable here.
	Stats StatsKind
	// RetryLane is where an immediate redelivery is placed.
	RetryLane RetryLaneKind
	// Safe forces every family to its mutex-backed reference kind, whatever
	// the other fields say. It is the differential-testing configuration.
	Safe bool
}

// kindSet is the engine's form of the selection.
func (e EngineConfig) kindSet() engine.KindSet {
	return engine.KindSet{
		Arch:      engine.Arch(e.Arch),
		Queue:     queue.Kind(e.Queue),
		Wait:      wait.Kind(e.Wait),
		Timer:     timer.Kind(e.Timer),
		Hash:      hash.Kind(e.Hash),
		Pool:      pool.Kind(e.Pool),
		Registry:  registry.Kind(e.Registry),
		Stats:     stats.Kind(e.Stats),
		RetryLane: engine.RetryLane(e.RetryLane),
		Safe:      e.Safe,
	}
}

// Valid reports whether every field names a concrete kind or Auto and the
// ready-queue kind supports the concurrency a lane subjects it to.
func (e EngineConfig) Valid() bool { return e.kindSet().Valid() }

// String renders the resolved selection as the benchmark dimensions,
// in a fixed order, with "/safe" appended for the reference configuration.
func (e EngineConfig) String() string { return e.kindSet().Resolve().String() }

// Resolve replaces every Auto with the kind it resolves to, so that a caller
// can see what it is actually running and a test can log it.
func (e EngineConfig) Resolve() EngineConfig {
	ks := e.kindSet().Resolve()
	return EngineConfig{
		Queue:     QueueKind(ks.Queue),
		Arch:      ArchKind(ks.Arch),
		Wait:      WaitKind(ks.Wait),
		Timer:     TimerKind(ks.Timer),
		Hash:      HashKind(ks.Hash),
		Pool:      PoolKind(ks.Pool),
		Registry:  RegistryKind(ks.Registry),
		Stats:     StatsKind(ks.Stats),
		RetryLane: RetryLaneKind(ks.RetryLane),
	}
}

// SafeEngine is the reference configuration: the mutex-backed Safe kind of
// every family at once, which is what the differential tests compare the
// engine against.
func SafeEngine() EngineConfig { return EngineConfig{Safe: true} }

// The kind enumerations below carry the same numeric values as the internal
// families they name, so a conversion is a cast rather than a table. That is
// a coupling and it is pinned rather than assumed:
// TestEngineConfigMirrorsTheFamilies compares every public kind's name and
// count against its family's own, so a family that gains, loses or reorders a
// kind fails a test instead of silently renaming one here.

// QueueKind is one lane ready-queue kind.
type QueueKind uint8

// The ready-queue kinds. Auto resolves to ADR-014's recorded winner.
const (
	// QueueAuto selects the recorded bake-off winner.
	QueueAuto QueueKind = iota
	// QueueMutexRing is the mutex-backed ring, and the Safe reference kind.
	QueueMutexRing
	// QueueChan is the buffered-channel baseline.
	QueueChan
	// QueueSeqMPMC is the bounded sequence-number MPMC ring.
	QueueSeqMPMC
	// QueueDisruptor is the single-sequence ring with per-consumer cursors.
	QueueDisruptor
	// QueueShardedMPMC spreads a lane's records over several MPMC rings and
	// therefore keeps no order.
	QueueShardedMPMC
	// QueueAppendLog is the append-log-and-cursor kind.
	QueueAppendLog
	// QueueSPSC is the single-producer single-consumer specialisation. A
	// lane has many of both, so an engine refuses it.
	QueueSPSC
	// QueueMPSC is the multi-producer single-consumer specialisation, which
	// an engine refuses for the same reason.
	QueueMPSC
	// QueueChunked is the unbounded chunked queue, which an unbounded lane
	// capacity selects.
	QueueChunked
)

// String returns the kind's benchmark slug.
func (k QueueKind) String() string { return queue.Kind(k).String() }

// Valid reports whether k names a concrete kind or Auto.
func (k QueueKind) Valid() bool { return queue.Kind(k).Valid() }

// QueueKinds lists every ready-queue kind, Auto first.
func QueueKinds() []QueueKind { return kindList[QueueKind](len(queue.Kinds())) }

// ArchKind is one fan-out storage architecture.
type ArchKind uint8

// The architectures. Auto resolves to ADR-014's recorded winner.
const (
	// ArchAuto selects the recorded bake-off winner.
	ArchAuto ArchKind = iota
	// ArchPerSubEnvelopes shares one reference-counted payload between the
	// subscriptions and gives each its own pooled envelope in its own lane.
	ArchPerSubEnvelopes
	// ArchChanFanout is the channel baseline: a lane's ready set is a Go
	// channel. The specification makes it a required baseline.
	ArchChanFanout
	// ArchFullCopy gives every subscription an independent copy of the
	// payload, made at publication time. The specification makes it a required baseline.
	ArchFullCopy
	// ArchLogCursor stores one sequence-numbered log per topic partition
	// with a cursor per subscription, so fan-out copies nothing and the
	// slowest subscription holds capacity for all of them.
	ArchLogCursor
)

// String returns the architecture's benchmark slug.
func (k ArchKind) String() string { return engine.Arch(k).String() }

// Valid reports whether k names an architecture or Auto.
func (k ArchKind) Valid() bool { return engine.Arch(k).Valid() }

// Shared reports whether the architecture stores one record per topic
// partition for every subscription at once, so that capacity is shared and a
// per-subscription capacity cannot be honoured.
func (k ArchKind) Shared() bool { return engine.Arch(k).Resolve().Shared() }

// ArchKinds lists every architecture, Auto first.
func ArchKinds() []ArchKind { return kindList[ArchKind](len(engine.Arches())) }

// WaitKind is one consumer wait strategy.
type WaitKind uint8

// The wait strategies. Auto resolves to ADR-014's recorded winner.
const (
	// WaitAuto selects the recorded bake-off winner.
	WaitAuto WaitKind = iota
	// WaitSpin spins without parking.
	WaitSpin
	// WaitYield yields to the scheduler.
	WaitYield
	// WaitCond parks on a sync.Cond with a cancellation broadcast.
	WaitCond
	// WaitEventCount parks on a monotonic event count.
	WaitEventCount
	// WaitChan parks on a channel, which is the Safe reference kind.
	WaitChan
	// WaitSpinPark spins and then joins a parked waiter list.
	WaitSpinPark
	// WaitHybrid spins, yields and then parks.
	WaitHybrid
)

// String returns the strategy's benchmark slug.
func (k WaitKind) String() string { return wait.Kind(k).String() }

// Valid reports whether k names a strategy or Auto.
func (k WaitKind) Valid() bool { return wait.Kind(k).Valid() }

// WaitKinds lists every wait strategy, Auto first.
func WaitKinds() []WaitKind { return kindList[WaitKind](len(wait.Kinds())) }

// TimerKind is one delayed-delivery scheduler kind.
type TimerKind uint8

// The scheduler kinds. Auto resolves to ADR-014's recorded winner.
const (
	// TimerAuto selects the recorded bake-off winner.
	TimerAuto TimerKind = iota
	// TimerHeap is the binary heap, and the Safe reference kind.
	TimerHeap
	// TimerHeap4 is the four-ary heap.
	TimerHeap4
	// TimerHierarchicalWheel is the hierarchical timing wheel.
	TimerHierarchicalWheel
	// TimerHashedWheel is the hashed timing wheel.
	TimerHashedWheel
	// TimerHybridHeapWheel is a wheel for the near future and a heap
	// beyond it.
	TimerHybridHeapWheel
	// TimerCalendar is the calendar or bucket queue.
	TimerCalendar
)

// String returns the kind's benchmark slug.
func (k TimerKind) String() string { return timer.Kind(k).String() }

// Valid reports whether k names a kind or Auto.
func (k TimerKind) Valid() bool { return timer.Kind(k).Valid() }

// TimerKinds lists every scheduler kind, Auto first.
func TimerKinds() []TimerKind { return kindList[TimerKind](len(timer.Kinds())) }

// HashKind is one key-to-lane hash.
type HashKind uint8

// The hashes. Auto resolves to ADR-014's recorded winner.
const (
	// HashAuto selects the recorded bake-off winner.
	HashAuto HashKind = iota
	// HashFNV1a is FNV-1a, and the Safe reference kind.
	HashFNV1a
	// HashWyhash is wyhash.
	HashWyhash
	// HashXXH3 is the xxh3-style hash.
	HashXXH3
	// HashMaphash is the seeded hash/maphash.
	HashMaphash
	// HashMurmur2 is Kafka's murmur2, which a Kafka-compatible partitioner
	// needs whatever the engine's own default is.
	HashMurmur2
)

// String returns the hash's benchmark slug.
func (k HashKind) String() string { return hash.Kind(k).String() }

// Valid reports whether k names a hash or Auto.
func (k HashKind) Valid() bool { return hash.Kind(k).Valid() }

// HashKinds lists every hash, Auto first.
func HashKinds() []HashKind { return kindList[HashKind](len(hash.Kinds())) }

// PoolKind is one envelope and record pool kind.
type PoolKind uint8

// The pool kinds. Auto resolves to ADR-014's recorded winner.
const (
	// PoolAuto selects the recorded bake-off winner.
	PoolAuto PoolKind = iota
	// PoolNone allocates every record and lets the collector reclaim it. It
	// is the Safe reference kind.
	PoolNone
	// PoolSyncPool is a sync.Pool.
	PoolSyncPool
	// PoolFreeList is a bounded free list.
	PoolFreeList
	// PoolSizeClass is a set of size-class pools.
	PoolSizeClass
	// PoolSlab is a slab allocator.
	PoolSlab
)

// String returns the kind's benchmark slug.
func (k PoolKind) String() string { return pool.Kind(k).String() }

// Valid reports whether k names a kind or Auto.
func (k PoolKind) Valid() bool { return pool.Kind(k).Valid() }

// PoolKinds lists every pool kind, Auto first.
func PoolKinds() []PoolKind { return kindList[PoolKind](len(pool.Kinds())) }

// RegistryKind is one topic registry kind.
type RegistryKind uint8

// The registry kinds. Auto resolves to ADR-014's recorded winner.
const (
	// RegistryAuto selects the recorded bake-off winner.
	RegistryAuto RegistryKind = iota
	// RegistryRWMutexMap is a map behind a sync.RWMutex, and the Safe
	// reference kind.
	RegistryRWMutexMap
	// RegistryCOW is a copy-on-write map behind an atomic pointer.
	RegistryCOW
	// RegistrySyncMap is a sync.Map.
	RegistrySyncMap
	// RegistrySharded is a sharded map.
	RegistrySharded
)

// String returns the kind's benchmark slug.
func (k RegistryKind) String() string { return registry.Kind(k).String() }

// Valid reports whether k names a kind or Auto.
func (k RegistryKind) Valid() bool { return registry.Kind(k).Valid() }

// RegistryKinds lists every registry kind, Auto first.
func RegistryKinds() []RegistryKind { return kindList[RegistryKind](len(registry.Kinds())) }

// StatsKind is one counter layout.
type StatsKind uint8

// The counter layouts. Auto resolves to ADR-017's recorded winner.
const (
	// StatsAuto selects the recorded bake-off winner.
	StatsAuto StatsKind = iota
	// StatsAtomic is one shared atomic per counter, and the Safe reference
	// kind.
	StatsAtomic
	// StatsStriped stripes each counter over several cache lines.
	StatsStriped
	// StatsPerWorker gives each worker its own cell.
	StatsPerWorker
)

// String returns the layout's benchmark slug.
func (k StatsKind) String() string { return stats.Kind(k).String() }

// Valid reports whether k names a layout or Auto.
func (k StatsKind) Valid() bool { return stats.Kind(k).Valid() }

// StatsKinds lists every counter layout, Auto first.
func StatsKinds() []StatsKind { return kindList[StatsKind](len(stats.Kinds())) }

// RetryLaneKind is where an immediate redelivery is placed.
type RetryLaneKind uint8

// The retry placements. Auto resolves to ADR-014's recorded winner.
const (
	// RetryLaneAuto selects the recorded bake-off winner.
	RetryLaneAuto RetryLaneKind = iota
	// RetryLaneBlock holds the lane on the redelivery until it is settled,
	// whatever the ordering mode asks for. It is the reference kind.
	RetryLaneBlock
	// RetryLanePushFront serves the newest redelivery first.
	RetryLanePushFront
	// RetryLaneSideQueue serves the redeliveries in the order they failed.
	RetryLaneSideQueue
)

// String returns the placement's benchmark slug.
func (k RetryLaneKind) String() string { return engine.RetryLane(k).String() }

// Valid reports whether k names a placement or Auto.
func (k RetryLaneKind) Valid() bool { return engine.RetryLane(k).Valid() }

// RetryLaneKinds lists every placement, Auto first.
func RetryLaneKinds() []RetryLaneKind { return kindList[RetryLaneKind](len(engine.RetryLanes())) }

// kindList builds the Auto-first list of a family with n concrete kinds. The
// numeric alignment it relies on is what TestEngineConfigMirrorsTheFamilies
// pins.
func kindList[K ~uint8](n int) []K {
	out := make([]K, 0, n+1)
	for i := range n + 1 {
		out = append(out, K(i))
	}
	return out
}

// errEngineKinds is what an EngineConfig the engine cannot honour is refused
// with.
var errEngineKinds = errcode.New(errcode.InvalidConfig,
	"iqmq/memory: Config.Engine names a kind the engine cannot use; a lane needs a multi-producer multi-consumer ready queue")
