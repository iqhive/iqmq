package engine

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/lifecycle"
	"github.com/iqhive/iqmq/internal/pool"
	"github.com/iqhive/iqmq/internal/stats"
	"github.com/iqhive/iqmq/internal/subject"
	"github.com/iqhive/iqmq/internal/timer"
	"github.com/iqhive/iqmq/internal/wait"
)

// lane is one subscription-partition: the record storage of the selected
// architecture (ready and delayed), the deadline-ordered in-flight list and
// the accounting that bounds them (the memory engine).
//
// depth and bytes are the authoritative capacity accounting for the lane and
// cover ready, in-flight and delayed records alike, so capacity means the
// same thing for every queue kind, including the ones that round their
// capacity up to a power of two and the unbounded one. They are reserved
// with a compare-and-swap before the record is enqueued, which keeps the
// publish fast path off the lane mutex. Under a shared architecture
// (Arch.Shared) the partition log has already decided admission for every
// subscription at once and these two are gauges only.
type lane[P any] struct {
	sub   *Subscription[P]
	idx   int32
	store partitionStore[P]
	envs  pool.ObjectPool[Envelope[P]]

	// retries is where a failed delivery waits for its next attempt, placed
	// by the kind the engine was built with. It is served
	// before the store, and while it is sealed it is served instead of it.
	retries retryQueue[P]

	depth atomic.Int64
	bytes atomic.Int64

	// ttlAt is the earliest time to live the lane knows of, or 0 when no
	// record it holds has one. The engine scheduler wakes on it.
	ttlAt atomic.Int64

	// cursor is the highest publication sequence the lane has admitted, and
	// is the position a stream subscription replays on from
	// (Subscription.Cursors). It is maintained only for a subscription of a
	// topic that retains, so no other topology pays for it.
	cursor atomic.Uint64

	// snode is the lane's single entry in the engine scheduler, carrying the
	// earliest of its lease deadlines, its due times and its expiries; armed
	// is the time that entry currently holds (the memory engine: one
	// scheduler for the whole broker, one entry per lane, never a timer per
	// message).
	snode timer.Entry[*lane[P]]
	armed atomic.Int64

	mu   sync.Mutex
	live inflight[P]

	// sweep is the scratch buffer the time-to-live sweep collects the
	// expired envelopes in. A sweep takes it and puts it back, so it is not
	// covered by the lane mutex: the sweep releases that mutex before it
	// drops anything, and two sweeps that overlap simply use one buffer
	// each rather than sharing one.
	sweep atomic.Pointer[envBuf[P]]
}

func newLane[P any](sub *Subscription[P], idx int, now int64) (*lane[P], error) {
	ln := &lane[P]{sub: sub, idx: int32(idx)}
	// A lane is sealed by a redelivery either because the placement always
	// seals (BlockLane) or because the topic asks for strict ordering, under
	// which every placement does (the memory engine).
	ln.retries.sealing = sub.eng.kinds.RetryLane.Blocks() || sub.strict
	envs, err := pool.New(sub.eng.kinds.Pool, pool.Config[Envelope[P]]{Reset: resetEnvelope[P]})
	if err != nil {
		return nil, err
	}
	ln.envs = envs
	ln.snode.Value = ln
	if ln.store, err = newStore(ln, now); err != nil {
		return nil, err
	}
	return ln, nil
}

// reserve books one message slot and size bytes. It reports false when the
// lane is at its message or byte capacity, leaving the accounting unchanged.
//
// The message slot is taken before the byte capacity is looked at, so a
// refusal by the bytes has to give the slot back — and that is a fall in the
// lane's depth like any other, which unreserve announces and this used not
// to. A publisher under OverflowBlock parks on sub.room when hasRoom reads
// the depth as full, and the depth it reads includes a slot another
// publisher is about to give back: the withdrawal is then the only event
// that can wake it.
func (ln *lane[P]) reserve(size int64) bool {
	capMsgs := int64(ln.sub.capMessages)
	for {
		d := ln.depth.Load()
		if capMsgs > 0 && d >= capMsgs {
			return false
		}
		if ln.depth.CompareAndSwap(d, d+1) {
			break
		}
	}
	capBytes := ln.sub.capBytes
	if capBytes <= 0 {
		ln.bytes.Add(size)
		return true
	}
	for {
		b := ln.bytes.Load()
		if b+size > capBytes {
			ln.depth.Add(-1)
			ln.sub.room.notify()
			return false
		}
		if ln.bytes.CompareAndSwap(b, b+size) {
			return true
		}
	}
}

// hasRoom reports whether the lane could take a record of size bytes now. It
// is a hint: a publisher under OverflowBlock uses it to decide whether to
// park, and the reservation itself is what decides admission.
func (ln *lane[P]) hasRoom(size int64) bool {
	if c := int64(ln.sub.capMessages); c > 0 && ln.depth.Load() >= c {
		return false
	}
	if c := ln.sub.capBytes; c > 0 && ln.bytes.Load()+size > c {
		return false
	}
	return true
}

// fits reports whether a record of size bytes could ever be admitted to the
// lane, however much of it is settled first. A publisher under OverflowBlock
// is refused rather than parked for ever on a record the lane cannot hold.
func (ln *lane[P]) fits(size int64) bool {
	if c := ln.sub.capBytes; c > 0 && size > c {
		return false
	}
	return ln.sub.capMessages != 0
}

// roomAfterDrops reports whether a record of size bytes would be admitted
// once the lane had dropped everything it can give up. Its leases are out of
// reach of a drop, so a lane whose in-flight records alone leave no room has
// nothing worth displacing: every drop would be lost for a refusal.
func (ln *lane[P]) roomAfterDrops(size int64) bool {
	if !ln.fits(size) {
		return false
	}
	capMsgs, capBytes := int64(ln.sub.capMessages), ln.sub.capBytes
	ln.mu.Lock()
	n, b := int64(ln.live.n), int64(0)
	if capBytes > 0 {
		for e := ln.live.head; e != nil; e = e.next {
			if e.rec != nil {
				b += int64(e.rec.size)
			}
		}
	}
	ln.mu.Unlock()
	if capMsgs > 0 && n >= capMsgs {
		return false
	}
	return capBytes <= 0 || b+size <= capBytes
}

// account books one record whose admission a shared partition log has
// already decided. The per-subscription gauges can never exceed the log's own
// capacity — a subscription holds a subset of the slots the log holds — so
// there is nothing left to check.
func (ln *lane[P]) account(size int64) {
	ln.depth.Add(1)
	ln.bytes.Add(size)
}

// unreserve returns a reservation to the lane and wakes whoever is waiting
// for room in it. The wake is one atomic load unless a publisher is actually
// parked under OverflowBlock, which is what keeps it off the settle path's
// cost (notifyGate.notify).
func (ln *lane[P]) unreserve(size int64) {
	ln.depth.Add(-1)
	ln.bytes.Add(-size)
	ln.sub.room.notify()
}

// newEnvelope takes a pooled envelope and binds it to rec. The lane
// back-pointer and the scheduler entry are set once, when the envelope is
// first allocated, and survive every recycle.
func (ln *lane[P]) newEnvelope(rec *record[P]) *Envelope[P] {
	e := ln.envs.Get()
	if e.lane == nil {
		e.lane = ln
		e.tnode.Value = e
	}
	e.rec = rec
	e.seq = rec.seq
	return e
}

// admit places a freshly published record in the lane: ready when it is due,
// delayed otherwise. Its capacity is already reserved.
func (ln *lane[P]) admit(rec *record[P], at, now int64) {
	// Everything the lane needs from the record is read before the store
	// makes it visible: from that instant a consumer may claim it, settle it
	// and hand it back to the pool, and the record is no longer this
	// goroutine's to read.
	expire := rec.expireAt
	if ln.sub.stream {
		ln.advanceCursor(rec.seq)
	}
	ln.store.Publish(rec, at, now)
	// The expiry is noted from that copy, after the record is in the store:
	// a sweep whose scan did not see the record cannot then overwrite this
	// note, because it merges what it found rather than replacing the lane's
	// value (expireTTL). Noting first left one ordering — note, sweep scans
	// without the record, store — in which the expiry was lost.
	ln.noteTTL(expire, now)
	ln.rearm()
}

// advanceCursor moves the lane's replay cursor to seq when seq is ahead of
// it. Records reach a lane in publication order per publisher but not across
// publishers, so the cursor is a maximum rather than a store.
func (ln *lane[P]) advanceCursor(seq uint64) {
	for {
		cur := ln.cursor.Load()
		if cur >= seq || ln.cursor.CompareAndSwap(cur, seq) {
			return
		}
	}
}

// noteTTL remembers a record expiry the scheduler has to wake for. now is the
// instant the caller read the clock at.
//
// An expiry the lane already holds covers a later one only while it is still
// ahead: a value that has passed belongs to a sweep that is due or already
// running, and that sweep took the value it is acting on before it scanned.
// Treating a value that has passed as covering this expiry would lose it
// altogether whenever the sweep's scan ran before the record reached the
// store — the record would never be delivered, because the claim path
// re-checks it, but it would stay counted as ready with nothing left to
// collect it, Faults.ExpireRecords included. Together with the caller noting
// the expiry only once the record is in its store (admit), and with the sweep
// merging rather than replacing, that leaves no ordering in which an expiry
// goes unrecorded.
func (ln *lane[P]) noteTTL(at, now int64) {
	if at <= 0 {
		return
	}
	for {
		cur := ln.ttlAt.Load()
		if cur > now && cur <= at {
			return
		}
		if ln.ttlAt.CompareAndSwap(cur, at) {
			return
		}
	}
}

// expired reports whether the record's time to live has run out.
func expired[P any](e *Envelope[P], now int64) bool {
	return e.rec != nil && e.rec.expireAt > 0 && e.rec.expireAt <= now
}

// unleased reports that a lease was given back without the record leaving the
// engine: a negative settlement, a lease that expired, a consumer that closed,
// a revocation or a rebalance. The record is still the engine's, so nothing
// was settled in lane.discard's sense, but the in-flight list is shorter —
// which is exactly what WaitIdle(IdleSettled) waits for, so the idle gate is
// notified here too or a waiter parked on the last outstanding lease would
// sleep until its context ran out.
//
// Like lane.discard it must be called with the lane mutex released, for the
// same reason: the waiter it wakes reads every lane of the engine.
func (s *Subscription[P]) unleased() { s.eng.settled() }

// requeueOrDrop returns e to its lane, or drops it when the engine or the
// subscription has closed or the record has outlived its time to live: after
// a close nothing is admitted again, and an expired record is never delivered
// again, so the record is released rather than redelivered.
//
// The close is looked at twice, because neither look can be taken under one
// lock with the placement: a redelivery is placed in the lane's redelivery
// queue, which has a mutex of its own, and Subscription.drain empties that
// queue with the lane mutex released (it must: a drop runs the publisher's
// release callback). A close that lands between the first look and the
// placement has therefore already drained this lane, and nothing will ever
// visit it again — Subscription.Close detaches the subscription before it
// drains, so Engine.Close's walk over the topics no longer finds it — which
// leaks the payload and the engine-wide message and byte reservation for the
// life of the process. The second look re-drains the subscription, exactly
// as Topic.reap does for the publish path. Draining twice is harmless: the
// second pass finds the lane empty.
//
// lane.pushReady needs none of this. It places under the lane mutex, and
// drain takes that mutex before it empties either structure, so a placement
// either falls wholly before the drain — and is collected by it — or wholly
// after a critical section that the close preceded, where the closed flag it
// reads is already set.
func (ln *lane[P]) requeueOrDrop(e *Envelope[P], at, now int64) bool {
	return ln.requeueOrDropTo(e, at, now, nil)
}

// requeueOrDropTo is requeueOrDrop with somewhere else to put the two things
// it does that run the caller's code: the drop, whose release is the
// publisher's callback, and the re-drain, which drops whatever the lane still
// holds. A nil fd does both here; a caller holding a lifecycle ticket passes
// one and runs it once the ticket is home, because a callback inside the
// ticket is a callback Engine.Close waits for (Consumer.take, the API contract
// The specification, adr/ADR-017-assumptions.md).
func (ln *lane[P]) requeueOrDropTo(e *Envelope[P], at, now int64, fd *fetchDrops[P]) bool {
	s := ln.sub
	if s.closed.Load() || s.eng.closing() || expired(e, now) {
		if fd != nil {
			fd.putBack(e)
		} else {
			ln.drop(e, false)
		}
		return false
	}
	ln.requeue(e, at, now)
	ln.rearm()
	s.unleased()
	if s.closed.Load() || s.eng.closing() {
		if fd != nil {
			fd.redrain = true
		} else {
			s.drain()
		}
	}
	return true
}

// requeue hands e to the engine's retry placement, which decides
// whether it goes to the head of the lane's redelivery queue, to its tail, or
// holds the lane altogether. The seal e was holding, if any, is given back by
// the placement itself, in the critical section that queues the record
// (retryQueue.push): a record that is placed again is no longer the one the
// lane is waiting on, but until it is back in the queue the lane is still
// waiting on it.
func (ln *lane[P]) requeue(e *Envelope[P], at, now int64) {
	expire := e.rec.expireAt
	ln.sub.eng.placer.Requeue(ln, e, at, now, ln.sub.strict)
	// The lane's expiry note may have been cleared by a sweep while the
	// record was in flight, and a record waiting for redelivery is a record
	// waiting: it is swept like any other (lane.expireTTL).
	ln.noteTTL(expire, now)
}

// drop discards e without settling it: the reservation is returned, the
// record loses one reference and the drop is counted. The caller must have
// unlinked e from the in-flight list; the store is told first, so that a
// shared architecture reclaims the slot before the record itself goes.
//
// It must be called with the lane mutex released, because two of the things
// it does may not run under one. It runs the publisher's release callback,
// which the message contract says is never called under an internal lock; and
// it reports the settlement, which wakes a WaitIdle waiter that reads every
// lane of the engine. A path that holds the lane mutex therefore collects
// the envelopes it is giving up and calls dropAll once it has released it.
func (ln *lane[P]) drop(e *Envelope[P], recycle bool) {
	ln.discard(e, recycle, ln.sub.st.dropped)
}

// dropLater drops e the way drop does, but hands the record's last reference
// to pb instead of releasing it here. A publication that displaces a record
// to make room for itself does so inside the topic's join boundary and, for
// the routes that take one, inside a publish ticket: the lane's capacity has
// to come back at once, and the payload has to go back to its publisher only
// once both are given up, because that release is the caller's callback
// (publication.dropQueued, the message contract and).
func (ln *lane[P]) dropLater(e *Envelope[P], pb *publication[P]) {
	ln.discardTo(e, true, ln.sub.st.dropped, pb)
}

// discard is what every record that leaves a subscription goes through: the
// seal it may be holding is given back, the store releases whatever it holds
// for it, the record loses one reference, the lane and the engine get their
// capacity back and the outcome is counted in c — acked, dead-lettered or
// dropped. Like drop, it must be called with the lane mutex released.
func (ln *lane[P]) discard(e *Envelope[P], recycle bool, c stats.Counter) {
	ln.discardTo(e, recycle, c, nil)
}

// discardTo is discard with somewhere else to put the record's last
// reference: a nil pb releases it here, and a publication takes it to release
// once it has given up the topic's join boundary (lane.dropLater).
func (ln *lane[P]) discardTo(e *Envelope[P], recycle bool, c stats.Counter, pb *publication[P]) {
	ln.retries.release(e)
	size := int64(e.rec.size)
	eng := ln.sub.eng
	ln.store.Settle(e)
	if pb != nil {
		pb.dropLater(e.rec)
	} else {
		eng.dropRecordRef(e.rec, recycle)
	}
	ln.unreserve(size)
	eng.releaseTotal(1, size)
	c.Add(1)
	e.rec = nil
	if recycle {
		ln.envs.Put(e)
	}
	if ln.retries.sealing {
		// The lane may have been sealed on this delivery; whoever is parked
		// on it can make progress now.
		ln.sub.waiters.NotifyAll()
	}
	eng.settled()
}

// dropAll discards every envelope in es. Like drop, it must be called with
// the lane mutex released.
func (ln *lane[P]) dropAll(es []*Envelope[P], recycle bool) {
	for _, e := range es {
		ln.drop(e, recycle)
	}
}

// nextEvent returns the earliest time the lane needs the scheduler to look at
// it: a lease deadline, a due time or a record expiry. It must not be called
// with the lane mutex held.
func (ln *lane[P]) nextEvent() int64 {
	at := ln.store.Next()
	ln.mu.Lock()
	if h := ln.live.head; h != nil && h.deadline != neverExpires {
		if at == 0 || h.deadline < at {
			at = h.deadline
		}
	}
	ln.mu.Unlock()
	if t := ln.retries.next(); t > 0 && (at == 0 || t < at) {
		at = t
	}
	if t := ln.ttlAt.Load(); t > 0 && (at == 0 || t < at) {
		at = t
	}
	if at != 0 {
		// A time that has already passed would make the scheduler fire the
		// lane again at once, and again after that. The only way to produce
		// one is an expiry that raced the settlement of the same delivery,
		// and that settlement re-arms the lane itself, so the lane is looked
		// at again shortly rather than continuously.
		if now := ln.sub.eng.now(); at <= now {
			at = now + int64(minRearm)
		}
	}
	return at
}

// rearm tells the engine scheduler when the lane next needs attention.
func (ln *lane[P]) rearm() { ln.sub.eng.sched.arm(ln, ln.nextEvent()) }

// tick is what the engine scheduler does to a lane whose time has come:
// records that have outlived their time to live are dropped, leases whose
// deadline has passed are reclaimed, and whoever is waiting on the
// subscription is woken so that the records that came due are claimed.
func (ln *lane[P]) tick(now int64) {
	dropped := ln.expireTTL(now)
	promoted := ln.store.Promote(now)
	requeued := ln.expireLeases(now)
	// A redelivery waiting for its due time is held in the lane's own
	// redelivery queue rather than in the store, so nothing has to move it:
	// waking the consumers is enough for them to find it deliverable.
	if ready, _ := ln.retries.depth(now); ready > 0 {
		promoted += ready
	}
	if dropped+promoted+requeued > 0 {
		ln.sub.waiters.NotifyAll()
	}
}

// expireTTL discards every record the lane holds whose time to live has run
// out, and returns how many. It runs only when the lane has an expiry due, so
// the cost is paid by the topics that ask for one.
//
// The sweep takes the lane mutex; publication does not, so a record published
// during a sweep may be ordered ahead of the survivors of that lane. Nothing
// promises an order between a publication and a concurrent expiry, and TTL is
// not combined with strict ordering by any conformance requirement.
func (ln *lane[P]) expireTTL(now int64) int {
	at := ln.ttlAt.Load()
	if at == 0 || at > now {
		return 0
	}
	// Take the expiry the sweep is acting on before scanning the store: a
	// publication that notes one while the scan runs then sees a lane with
	// no expiry and records its own, which the merge below keeps. Leaving
	// the stale value in place until the end would let noteTTL read it as
	// covering an expiry this scan cannot have seen.
	ln.ttlAt.CompareAndSwap(at, 0)
	buf := ln.sweep.Swap(nil)
	if buf == nil {
		buf = &envBuf[P]{}
	}
	ln.mu.Lock()
	var next int64
	buf.s, next = ln.store.ExpireTTL(now, buf.s[:0])
	var rnext int64
	buf.s, rnext = ln.retries.expireTTL(now, buf.s)
	if rnext > 0 && (next == 0 || rnext < next) {
		next = rnext
	}
	ln.mu.Unlock()
	ln.noteTTL(next, now)
	// A store may empty itself and put the survivors back (expireByDrain),
	// and a claim does not take the lane mutex: one that ran in between
	// found nothing and parked, and no drop may follow to wake it.
	ln.sub.waiters.NotifyAll()
	// The lane mutex is released before anything is dropped: a drop runs the
	// publisher's release callback and wakes WaitIdle (drop).
	n := len(buf.s)
	ln.dropAll(buf.s, true)
	clear(buf.s)
	buf.s = buf.s[:0]
	ln.sweep.Store(buf)
	return n
}

// expireLeases reclaims every lease of the lane whose deadline has passed and
// returns the records to the lane for redelivery. It returns the number
// reclaimed. A lease whose settlement is already in flight is left to that
// settlement, which is the pending-revocation rule of the message contract.
func (ln *lane[P]) expireLeases(now int64) int {
	var gone, dead []*Envelope[P]
	ready := 0
	var back bool
	ln.mu.Lock()
	for e := ln.live.head; e != nil && e.deadline <= now; {
		next := e.next
		_, g := e.cell.Load()
		if e.cell.Revoke(g) {
			ln.live.remove(e)
			if ln.sub.exhausted(e) {
				// The record has had every attempt it is allowed. It is
				// routed away once the lane mutex is released, because a
				// dead-letter publication is a publication like any other
				// and must not run under an internal lock.
				dead = append(dead, e)
			} else if gone, back = ln.pushReady(e, now, gone); back {
				ready++
			}
		}
		e = next
	}
	// The records already back in the lane are counted before the mutex is
	// released. The scheduler's tick and Faults.Advance both sweep a lane
	// whose time has come, and the loser finds it empty and returns at once:
	// counted after the unlock, a snapshot taken when Advance returned could
	// show the record ready again with no expiry behind it.
	if ready > 0 {
		ln.sub.st.expired.Add(int64(ready))
		ln.sub.st.requeued.Add(int64(ready))
	}
	ln.mu.Unlock()
	ln.dropAll(gone, false)
	requeued := 0
	routed := 0
	for _, e := range dead {
		if ln.deadLetter(e) {
			routed++
			continue
		}
		// A failed or ambiguous dead-letter publication never settles the
		// original: the record goes back to its lane for another attempt
		// (the memory engine, adr/ADR-011-failure-policy.md).
		if ln.requeueOrDrop(e, 0, now) {
			requeued++
		}
	}
	if n := requeued + routed; n > 0 {
		ln.sub.st.expired.Add(int64(n))
	}
	if requeued > 0 {
		ln.sub.st.requeued.Add(int64(requeued))
	}
	if ready+requeued > 0 {
		ln.sub.unleased()
	}
	n := ready + requeued + routed
	if n > 0 || len(gone) > 0 {
		ln.sub.waiters.NotifyAll()
	}
	return n
}

// subStats are the per-subscription counters of the memory engine, built
// from the selected kind.
type subStats struct {
	published    stats.Counter
	acked        stats.Counter
	nacked       stats.Counter
	requeued     stats.Counter
	expired      stats.Counter
	dropped      stats.Counter
	rejected     stats.Counter
	attempts     stats.Counter
	deadLettered stats.Counter
	overflowed   stats.Counter
	dlqFailed    stats.Counter
}

func newSubStats(k stats.Kind, workers int) (subStats, error) {
	var s subStats
	for _, c := range []*stats.Counter{
		&s.published, &s.acked, &s.nacked, &s.requeued, &s.expired, &s.dropped,
		&s.rejected, &s.attempts, &s.deadLettered, &s.overflowed, &s.dlqFailed,
	} {
		var err error
		if *c, err = stats.New(k, workers); err != nil {
			return subStats{}, err
		}
	}
	return s, nil
}

// SubscriptionConfig configures one subscription of a topic. The capacities
// are per-subscription quantities, which a shared architecture (Arch.Shared)
// has none of: under one, Topic.Subscribe refuses a non-zero capacity with
// ErrInvalidConfig rather than ignoring it.
type SubscriptionConfig struct {
	// AckDeadline is the lease duration of a delivery. Zero means the
	// topic's AckDeadline; a negative value means leases never expire. It is
	// honoured under every architecture.
	AckDeadline time.Duration
	// CapacityMessages bounds the ready, in-flight and delayed records of
	// one lane. Zero means the topic's CapacityMessages.
	CapacityMessages int
	// CapacityBytes bounds the same set by payload size. Zero means the
	// topic's CapacityBytes; a non-positive value after that means no byte
	// limit.
	CapacityBytes int64
	// MaxAttempts is the number of deliveries a record may have before a
	// failure routes it to the dead-letter topic. Zero means the topic's
	// MaxAttempts; a negative value means no limit whatever the topic says.
	MaxAttempts int
	// DeadLetterTopic names the topic a record is published to once it has
	// used up its attempts. An empty name means the topic's
	// DeadLetterTopic; "/none" — a spelling no topic can have — means none,
	// whatever the topic says.
	DeadLetterTopic string
	// Filter is the subject pattern the subscription matches publications
	// against on a topic with Wildcards (internal/subject: dot-separated
	// tokens, `*` for one and `>` for the rest). An empty filter takes
	// every publication. Naming one on a topic without wildcards is a
	// configuration error.
	Filter string
	// Start is where the subscription begins reading a stream topic. Zero
	// means StartLatest, which takes only what is published after the
	// subscription exists — the behaviour of every other topology.
	Start StartPosition
	// Sticky assigns each lane to one consumer exclusively and rebalances
	// on every join and leave (the memory engine). It is implied by a
	// topic that asks for strict ordering over more than one partition,
	// where competing consumers would interleave a lane's records.
	Sticky bool
	// ZeroCopy is not a field: the receive contract is engine-wide
	// (Config.Receive), because a record is shared by every subscription
	// and one of them cannot scrub what the others are still reading.
}

// Subscription is one independent consumer group of a topic: its own lanes,
// its own settlement state and its own backpressure (the memory engine).
type Subscription[P any] struct {
	name  string
	topic *Topic[P]
	eng   *Engine[P]

	lanes       []*lane[P]
	capMessages int
	capBytes    int64
	ackDeadline time.Duration
	// maxAttempts is the delivery budget of a record; zero means no limit.
	maxAttempts uint32
	// strict reports that the topic asks for strict ordering, under which a
	// redelivery blocks its lane whatever the retry placement is.
	strict bool
	// stream reports that the topic retains its publications, so the lanes
	// maintain a replay cursor.
	stream bool
	// filter is the subject pattern the subscription matches publications
	// against on a topic with wildcards, and nil when it takes everything.
	filter *subject.Pattern
	// dlqName is the dead-letter topic, empty when the subscription has
	// none, and dlq caches the topic once it has been resolved: a topic is
	// never removed from an engine, so the resolution is stable.
	dlqName string
	dlq     atomic.Pointer[Topic[P]]
	// room wakes the publishers parked by OverflowBlock whenever a record
	// leaves one of the lanes.
	room notifyGate

	waiters wait.Strategy
	st      subStats

	nextConsumer atomic.Uint32
	active       atomic.Int64
	closed       atomic.Bool

	// sticky reports that each lane belongs to one consumer exclusively
	// (the memory engine), assign holds the versioned table and members is
	// the attached consumer set the table is computed from, in id order.
	sticky  bool
	assign  atomic.Pointer[assignment]
	cmu     sync.Mutex
	members []uint32
	cons    map[uint32]*Consumer[P]
}

// Name returns the subscription name.
func (s *Subscription[P]) Name() string { return s.name }

// Partitions returns the number of lanes.
func (s *Subscription[P]) Partitions() int { return len(s.lanes) }

// Snapshot is the observable state of one subscription (the memory engine).
// The gauges are exact while the subscription is quiescent, which is the
// condition every model and invariant check runs under.
type Snapshot struct {
	// Ready counts records waiting for a consumer.
	Ready int
	// InFlight counts leased records.
	InFlight int
	// Delayed counts records waiting for their due time.
	Delayed int
	// Retained counts the records the subscription's topic keeps for replay
	// (the memory engine). It is a property of the topic, so every
	// subscription of a stream reports the same number, and it is 0 on a
	// topology that retains nothing.
	Retained int
	// Bytes is the payload size of every record the lane accounts for.
	Bytes int64
	// Published counts records admitted to the subscription.
	Published int64
	// Acked counts records terminally settled by a consumer.
	Acked int64
	// Nacked counts negative settlements.
	Nacked int64
	// Requeued counts records returned to a lane by a nack, a lease expiry
	// or a consumer close.
	Requeued int64
	// Expired counts leases reclaimed by expiry.
	Expired int64
	// DeadLettered counts records published to a dead-letter topic because
	// they used up their delivery attempts.
	DeadLettered int64
	// Overflowed counts publications the overflow policy discarded before
	// admission (OverflowDropNewest). A record discarded to make room for a
	// newer one — OverflowDropOldest, or the DropSlow fan-out policy — is
	// counted in Dropped instead, because it was admitted.
	Overflowed int64
	// DeadLetterFailures counts dead-letter publications that failed, each
	// of which returned its record to the lane rather than settling it.
	DeadLetterFailures int64
	// Dropped counts admitted records discarded without settlement: the
	// engine or the subscription closed while it held the record, its time
	// to live ran out, the DropSlow policy took its place, or it came back
	// from a redelivery already expired.
	Dropped int64
	// Rejected counts publications refused before admission.
	Rejected int64
	// Attempts is the total number of deliveries, including redeliveries.
	Attempts int64
	// ActiveConsumers counts attached, open consumers.
	ActiveConsumers int
	// Deduplicated counts the publications the topic's dedup window
	// discarded. It is a property of the topic, so every subscription
	// reports the same number.
	Deduplicated int64
	// Assignment is the version of the subscription's partition assignment,
	// which every attach, detach and explicit Rebalance increments
	// (the memory engine). It is a membership generation, so it counts up
	// whether or not the subscription is sticky; what stickiness decides is
	// the *table*, which Subscription.Assignment returns alongside it and
	// which is empty when consumers compete on every lane. It is 0 only for
	// a subscription no consumer has ever attached to.
	Assignment uint64
}

// Conserved reports whether the conservation invariant of the memory engine
// holds: every admitted record is either settled, dead-lettered, dropped, or
// still held in one of the three lane structures.
func (s Snapshot) Conserved() bool {
	return s.Published == s.Acked+int64(s.Ready)+int64(s.InFlight)+int64(s.Delayed)+
		s.DeadLettered+s.Dropped
}

// detach closes the consumer with the given id, if it is still attached.
func (s *Subscription[P]) detach(id uint32) {
	s.cmu.Lock()
	c := s.cons[id]
	s.cmu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}

// revokeAll reclaims every lease the subscription has outstanding, whoever
// holds it, and returns the records to their lanes. It is what a reconnect
// does: the deliveries issued under the previous generation are no longer
// valid, and none of them is a failure, so none consumes an attempt.
func (s *Subscription[P]) revokeAll() {
	var back []*Envelope[P]
	for _, ln := range s.lanes {
		// Per lane, for the reason revoke gives: dropAll runs callbacks
		// between lanes.
		now := s.eng.now()
		requeued := 0
		ln.mu.Lock()
		// Under the lane mutex, for the reason revoke gives: a table read
		// before it can predate the join of a holder that claimed since.
		table := s.assign.Load()
		for e := ln.live.head; e != nil; {
			next := e.next
			_, g := e.cell.Load()
			// A reconnect withdraws the leases of consumers that are still
			// open, so it takes Revoke, not Close: internal/settlement gives
			// the two withdrawals different resting states on purpose --
			// Idle, whose stale settlement is told its lease is lost,
			// against Closed, whose settlement is told its consumer closed
			// -- and a reconnect that left the cell Closed made a delivery
			// report ErrClosed from a consumer nobody had closed, against
			// the memory engine's "gen mismatch -> ErrLeaseLost". The one holder it closes is one the table no longer
			// names: a Consumer.Close that has left the membership and whose
			// own walk has not reached this lane yet.
			if withdrawLease(e, g, !table.member(e.owner.Load())) {
				ln.live.remove(e)
				var ok bool
				if back, ok = ln.pushReady(e, now, back); ok {
					requeued++
				}
			}
			e = next
		}
		ln.mu.Unlock()
		ln.dropAll(back, false)
		clear(back)
		back = back[:0]
		if requeued > 0 {
			s.st.requeued.Add(int64(requeued))
			s.waiters.NotifyAll()
			s.unleased()
		}
		ln.rearm()
	}
}

// deliverable reports whether the subscription takes a publication with the
// given subject. A subscription with no filter takes everything, which is
// what every topic without wildcards does.
func (s *Subscription[P]) deliverable(subj string) bool {
	return s.filter == nil || s.filter.Match(subj)
}

// exhausted reports whether a failed delivery has used up the record's
// attempts and must be routed away rather than redelivered (the API contract
// The specification: "past MaxAttempts → DLQ-pending"). A consumer that closes is not a
// failure and never consults it.
func (s *Subscription[P]) exhausted(e *Envelope[P]) bool {
	return s.maxAttempts > 0 && e.attempts >= s.maxAttempts
}

// retained returns the number of records the topic keeps for replay.
func (s *Subscription[P]) retained() int {
	n, _ := s.topic.Retained()
	return n
}

// Inspect returns a snapshot of the subscription.
func (s *Subscription[P]) Inspect() Snapshot {
	now := s.eng.now()
	snap := Snapshot{
		Retained:           s.retained(),
		Deduplicated:       s.topic.deduped.Load(),
		Assignment:         s.assignmentVersion(),
		DeadLettered:       s.st.deadLettered.Load(),
		Overflowed:         s.st.overflowed.Load(),
		DeadLetterFailures: s.st.dlqFailed.Load(),

		Published:       s.st.published.Load(),
		Acked:           s.st.acked.Load(),
		Nacked:          s.st.nacked.Load(),
		Requeued:        s.st.requeued.Load(),
		Expired:         s.st.expired.Load(),
		Dropped:         s.st.dropped.Load(),
		Rejected:        s.st.rejected.Load(),
		Attempts:        s.st.attempts.Load(),
		ActiveConsumers: int(s.active.Load()),
	}
	for _, ln := range s.lanes {
		ready, delayed := ln.store.Depth(now)
		rready, rdelayed := ln.retries.depth(now)
		ln.mu.Lock()
		infl := ln.live.Len()
		ln.mu.Unlock()
		snap.InFlight += infl
		snap.Delayed += delayed + rdelayed
		snap.Ready += ready + rready
		snap.Bytes += ln.bytes.Load()
	}
	return snap
}

// Close detaches the subscription from its topic and drops every record it
// still holds, releasing each payload exactly once. It is idempotent.
func (s *Subscription[P]) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	s.topic.removeSub(s)
	s.drain()
	s.waiters.NotifyAll()
	s.room.notify()
	return nil
}

// Closed reports whether the subscription is closed.
func (s *Subscription[P]) Closed() bool { return s.closed.Load() }

// purgeBatch is how many records Purge takes out of a lane at a time.
const purgeBatch = 256

// Purge discards what every lane of the subscription could deliver now —
// its due redeliveries and its ready records — and returns how many it
// discarded. It attaches no consumer, so a sticky assignment is not
// rebalanced and no lease is revoked: an in-flight record stays with its
// consumer, a record still waiting for its due time stays, and so does the
// store of a lane an outstanding redelivery holds sealed. The records count
// as acked, the engine's only terminal settlement; one that outlived its
// time to live is dropped, as a claim would have dropped it.
func (s *Subscription[P]) Purge() int {
	buf := make([]*Envelope[P], purgeBatch)
	n := 0
	for _, ln := range s.lanes {
		for {
			got := ln.purgeClaim(buf)
			if got == 0 {
				break
			}
			now := s.eng.now()
			for _, e := range buf[:got] {
				if expired(e, now) {
					ln.drop(e, true)
					continue
				}
				ln.discard(e, true, s.st.acked)
				n++
			}
			clear(buf[:got])
			ln.rearm()
		}
	}
	return n
}

// purgeClaim takes up to len(dst) deliverable records out of the lane for
// Purge, in the order a claim takes them. It holds a fetch ticket while it
// does, for the reason Consumer.take gives, and the caller discards them once
// it is released.
func (ln *lane[P]) purgeClaim(dst []*Envelope[P]) int {
	eng := ln.sub.eng
	tk := eng.lc.Enter(lifecycle.OpFetch)
	defer eng.lc.Leave(tk)
	if !tk.OK() || ln.sub.closed.Load() {
		return 0
	}
	now := eng.now()
	got, open := ln.retries.claim(now, dst)
	if open && got < len(dst) {
		got += ln.store.Claim(now, dst[got:])
	}
	return got
}

// HasWaiters reports whether a consumer of this subscription has announced
// its intent to wait on the subscription's wait strategy — the registration
// Consumer.Fetch makes when it finds its lane empty, after which it can only
// leave on a notification, on its context or on a close (internal/wait).
//
// It exists so that a test which needs a *parked* fetch can synchronise on
// one instead of sleeping and hoping: a sleep proves nothing on a loaded
// host, where the consuming goroutine may not have been scheduled yet. It
// reads one atomic and is on no engine path.
func (s *Subscription[P]) HasWaiters() bool { return s.waiters.HasWaiters() }

// wakeReady announces a record that has become ready to claim.
//
// A sticky subscription wakes every parked fetch. The default wait kind wakes
// one waiter per Notify, the oldest, and under a sticky assignment that one
// may not own the lane the record is in: it takes nothing, parks again, and
// the wake is spent while the owner stays parked beside a ready record that no
// lease or due time will ever tick. Only the owner can claim it, and the
// waiter list does not know which waiter that is. Without stickiness every
// consumer may claim from every lane, so one wake is enough.
func (s *Subscription[P]) wakeReady() {
	if s.sticky {
		s.waiters.NotifyAll()
		return
	}
	s.waiters.Notify()
}

// closeAll is Close without detaching: the engine uses it when it shuts down
// and is itself removing every topic.
func (s *Subscription[P]) closeAll() {
	if s.closed.Swap(true) {
		return
	}
	s.drain()
	s.waiters.NotifyAll()
	s.room.notify()
}

// drain drops every record held by the subscription: in flight first, then
// delayed, then ready. Envelopes are not recycled, so a stale handle can
// still read its settlement cell safely after the engine has closed.
func (s *Subscription[P]) drain() {
	var gone []*Envelope[P]
	for _, ln := range s.lanes {
		ln.mu.Lock()
		for e := ln.live.head; e != nil; {
			next := e.next
			_, g := e.cell.Load()
			if !e.cell.Close(g) {
				// A settlement is already in flight: its outcome stands and
				// its own path releases the record.
				e = next
				continue
			}
			ln.live.remove(e)
			gone = append(gone, e)
			e = next
		}
		ln.mu.Unlock()
		// The in-flight records leave first and the stored ones after them,
		// and both are dropped with the lane mutex released (drop).
		gone = ln.retries.drain(gone)
		gone = ln.store.Drain(gone)
		ln.dropAll(gone, false)
		clear(gone)
		gone = gone[:0]
		ln.ttlAt.Store(0)
	}
}

// pushReady returns e to its store while the caller holds the lane mutex. A
// record that must not go back — the engine or the subscription has closed,
// or the record has outlived its time to live — is appended to gone instead
// of being dropped here, because a drop must not run under the lane mutex
// (drop); the caller drops what it collected once it has released the mutex.
// It returns gone and reports whether the record was requeued.
func (ln *lane[P]) pushReady(e *Envelope[P], now int64, gone []*Envelope[P]) ([]*Envelope[P], bool) {
	if ln.sub.closed.Load() || ln.sub.eng.closing() || expired(e, now) {
		return append(gone, e), false
	}
	ln.requeue(e, now, now)
	return gone, true
}
