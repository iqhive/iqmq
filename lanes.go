package iqmq

import (
	"context"

	"github.com/iqhive/iqmq/internal/dispatch"
	"github.com/iqhive/iqmq/internal/hash"
)

// The lane bounds ("lanes bounded and fair") and.
const (
	// MaxConcurrency bounds WithConcurrency. A lane is a goroutine and a
	// bounded queue, so the lane count is a resource like any other buffer
	// and is refused rather than rounded down.
	MaxConcurrency = 1024
	// laneSeed is the hash seed every runner routes with. It is a constant
	// so that one key always reaches one lane: a per-runner random seed
	// would make the lane a key lands on differ between two runs of the same
	// program, which is invisible until a test asks whether two keys shared
	// a lane.
	laneSeed uint64 = 0
	// minLaneCapacity is the smallest per-lane queue the runner builds. A
	// lane of one item still works — the runner's own in-flight bound is
	// what admits — but it serialises the fetch loop against the handler,
	// so the floor keeps a small MaxInFlight from costing a round trip per
	// message.
	minLaneCapacity = 2
)

// lanes is the concurrency: N handler goroutines over one consumer, with
// the deliveries of one key routed to one of them.
//
// It is a thin binding of internal/dispatch (ADR-017), which owns
// the lane queues, the worker pool, the wake protocol and the drain. Two
// things are this file's:
//
//   - the ROUTE, which turns a Delivery into the lane decision the pool takes:
//     a keyed delivery goes to the hash of its key, so every delivery of one
//     key is handled by one worker and in arrival order, and a keyless one is
//     spread over the lanes in turn (the specification's "partition | hash(key) % lanes |
//     round-robin for keyless").
//   - the DRAIN contract, which is what Close means here: a Close stops
//     accepting and then waits for every item already accepted to be handled,
//     because an abandoned item is an unsettled delivery the caller was told
//     nothing about.
//
// A panic in a handler does not reach the pool: the runner recovers inside the
// item it hands over (run.go's invoke), so a lane that panics settles its own
// delivery by the failure policy and its siblings never see it. That is the
// half a pool cannot provide — dispatch's worker has no recover of its own,
// and a panic through it would take the worker goroutine and, with it, every
// lane the worker owns.
type lanes struct {
	pool dispatch.Pool[*runItem]
	// byPartition reports that the pool's kind routes by source partition
	// rather than by key hash, read once at construction (rule 4) so
	// that no delivery pays for the question.
	byPartition bool
	// keyless and partitioned are WithOrdering, compiled into booleans at
	// construction (rule 4): a delivery never asks what the ordering
	// is, it takes the branch the construction already decided.
	keyless     bool
	partitioned bool
}

// newLanes starts the pool. h runs one item on a worker goroutine.
//
// workers is the concurrency and inFlight the runner's in-flight bound; the
// per-lane capacity is derived from the two so that the lanes together hold
// about what the runner admits, which is what keeps a full lane blocking the
// fetch loop rather than the fetch loop starving the lanes.
func newLanes(workers, inFlight int, ordering Ordering, h func(*runItem)) *lanes {
	k := dispatch.Auto.Resolve()
	cfg := dispatch.Config{
		Workers:  workers,
		Capacity: laneCapacity(workers, inFlight),
	}
	return &lanes{
		pool:        dispatch.New[*runItem](k, cfg, h),
		byPartition: k.RoutesByPartition(),
		keyless:     ordering == OrderingUnordered,
		partitioned: ordering == OrderingByPartition,
	}
}

// laneCapacity is how many items one lane holds.
//
// The worker floor is not decoration: resolveRun refuses a concurrency below
// 1, and a division that trusted it turned a dropped validation check into a
// divide-by-zero panic inside the pool's constructor rather than the
// CodeInvalidConfig the caller asked for.
func laneCapacity(workers, inFlight int) int {
	if workers < 1 {
		workers = 1
	}
	c := inFlight / workers
	if c < minLaneCapacity {
		c = minLaneCapacity
	}
	return c
}

// route is the lane decision for one delivery.
//
// A delivery with a key is routed by the hash of that key, which is what makes
// the per-key order a property of the runner rather than of the driver: one
// key, one lane, one worker at a time. A delivery without one may be handled
// anywhere, and the pool spreads the keyless items over the lanes in turn.
//
// The ordering the runner was configured with decides which of the three
// answers this is (the specification's "partition | hash(key) % lanes | round-robin for
// keyless"): OrderingUnordered spreads everything, OrderingByPartition routes
// by the partition the driver reported, and OrderingByKey — the default —
// routes by the key.
//
// Info() is read for a partition-ordered runner, which asked for the
// partition, and otherwise only for a pool that routes by partition, because
// it is an indirect call and every other kind ignores the answer.
func (l *lanes) route(d *Delivery) dispatch.Route {
	if l.keyless {
		// Nothing is ordered against anything: every delivery is spread over
		// the lanes in turn, whatever key it carries.
		return dispatch.Keyless()
	}
	if l.partitioned {
		info := d.Info()
		if !info.HasPartition {
			// A driver that reports no partition gives this delivery nothing
			// to be ordered by, and inventing one — the key's hash, lane
			// zero — would order it against messages it has no relation to.
			return dispatch.Keyless()
		}
		// Keyed by the partition so that one partition is one lane under
		// EVERY pool kind, and InPartition as well so that a pool which
		// routes by partition natively uses the number rather than its hash.
		return dispatch.Keyed(partitionKey(info.Partition)).InPartition(info.Partition)
	}
	var r dispatch.Route
	if len(d.Key) > 0 {
		r = dispatch.Keyed(hash.Bytes(hash.Auto, laneSeed, d.Key))
	} else {
		r = dispatch.Keyless()
	}
	if l.byPartition {
		if info := d.Info(); info.HasPartition {
			r = r.InPartition(info.Partition)
		}
	}
	return r
}

// partitionKey turns a partition number into a lane key.
//
// It is a multiply-and-fold and not a hash of the four bytes, because the
// input is a small integer: the lane a partition lands on comes from the
// pool's own reduction of this word, and consecutive partitions have to reach
// different lanes rather than the same one.
func partitionKey(p int32) uint64 {
	h := uint64(uint32(p)) * 0x9E3779B97F4A7C15
	return h ^ (h >> 29)
}

// submit queues it on its lane, blocking until the lane has room, ctx is done
// or the pool closes. An item it accepts is always handled.
func (l *lanes) submit(ctx context.Context, it *runItem) error {
	return l.pool.Submit(ctx, l.route(&it.d), it)
}

// close stops accepting items and waits for every item already accepted to be
// handled, then joins the workers. It is idempotent.
//
// Draining rather than abandoning is the whole contract: an item still in a
// lane names a delivery nothing has settled, and dropping it would leave the
// message to a lease expiry that the caller was never told about. The pool's
// own Close is what waits; this method exists to say so where the runner
// reads it.
func (l *lanes) close() error { return l.pool.Close() }
