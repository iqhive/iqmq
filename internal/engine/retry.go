package engine

import (
	"slices"
	"sync"
	"sync/atomic"
)

// RetryLane is where an immediate redelivery goes: the placement one failed
// delivery takes when it returns to its lane (the memory engine, "Nack: delay → scheduler; immediate → retry lane placement").
//
// The three kinds trade head-of-line blocking against redelivery latency:
//
//	BlockLane   the redelivery holds its lane — nothing behind it is served
//	            until it has been settled or dead-lettered. Order is never
//	            disturbed, and one poisonous record stops the lane.
//	PushFront   the redelivery goes to the head of the lane's redelivery
//	            queue, which is served before the ready queue: the newest
//	            failure is retried first, like a broker that re-heads its
//	            channel.
//	SideQueue   the redelivery goes to the tail of that queue, so the
//	            failures are retried in the order they failed.
//
// Under strict ordering (OrderingStrict) the kinds converge: every one of
// them blocks the lane and keeps the redeliveries in publication order,
// because that is what strict ordering means ("strict-ordering
// retries block their lane"). They then differ only in cost, which is what
// BenchmarkMemory/OrderedRetry measures.
type RetryLane uint8

// The retry placements. RetryLaneAuto resolves to the recorded bake-off
// winner in auto_retry.go.
const (
	// RetryLaneAuto selects the recorded bake-off winner.
	RetryLaneAuto RetryLane = iota
	// BlockLane holds the lane on the redelivery until it is settled. It is
	// the reference kind: the strictest, and the one that needs no ordering
	// decision at all.
	BlockLane
	// PushFront serves the newest redelivery first.
	PushFront
	// SideQueue serves the redeliveries in the order they failed.
	SideQueue

	retryLaneCount
)

var retryLaneNames = [retryLaneCount]string{"auto", "blocklane", "pushfront", "sidequeue"}

var retryLaneIdents = [retryLaneCount]string{"RetryLaneAuto", "BlockLane", "PushFront", "SideQueue"}

// RetryLanes returns the concrete placements in declaration order.
// RetryLaneAuto is not one of them.
func RetryLanes() []RetryLane { return []RetryLane{BlockLane, PushFront, SideQueue} }

// String returns the benchmark slug of the placement.
func (k RetryLane) String() string {
	if k < retryLaneCount {
		return retryLaneNames[k]
	}
	return "retrylane(?)"
}

// Ident returns the Go identifier of the placement, which is what the
// bake-off writes into auto_retry.go.
func (k RetryLane) Ident() string {
	if k < retryLaneCount {
		return retryLaneIdents[k]
	}
	return ""
}

// Valid reports whether k names a placement or RetryLaneAuto.
func (k RetryLane) Valid() bool { return k < retryLaneCount }

// Resolve maps RetryLaneAuto to the recorded default.
func (k RetryLane) Resolve() RetryLane {
	if k == RetryLaneAuto {
		return RetryLaneAutoKind
	}
	return k
}

// ParseRetryLane maps a slug back to its placement.
func ParseRetryLane(s string) (RetryLane, bool) {
	for i, n := range retryLaneNames {
		if n == s {
			return RetryLane(i), true
		}
	}
	return RetryLaneAuto, false
}

// Blocks reports whether the placement holds the lane on every redelivery,
// whatever the ordering mode asks for. Only BlockLane does.
func (k RetryLane) Blocks() bool { return k.Resolve() == BlockLane }

// Ordered reports whether the placement delivers the records of a lane in
// publication order when the topic does not ask for strict ordering.
// BlockLane does — it never lets anything past a redelivery — and the other
// two serve redeliveries before the ready queue, which is the
// redelivery-first order real brokers use.
func (k RetryLane) Ordered() bool { return k.Resolve() == BlockLane }

// retryPlacer decides where an immediate redelivery goes. It is
// the whole of the variation: everything else about a redelivery — the
// settlement cell, the attempt count, the accounting, the dead-letter
// decision — is the same for every kind, so a new placement is one file and
// cannot change the meaning of a negative acknowledgement.
//
// A placer holds no state of its own; the records live in the lane's
// redelivery queue, which every kind shares (the specification A, RetryPlacer{Requeue(lane, env, strict)}). At is the redelivery's due time,
// which is non-zero only for a negative acknowledgement that asked for a
// delay under strict ordering, where the record must keep its place in the
// lane while it waits.
type retryPlacer[P any] interface {
	// Requeue returns e to ln for redelivery. strict reports that the topic
	// asks for strict ordering, under which every kind blocks the lane.
	Requeue(ln *lane[P], e *Envelope[P], at, now int64, strict bool)
	// Kind returns the placement the placer implements.
	Kind() RetryLane
}

// newRetryPlacer builds the placer of one engine.
func newRetryPlacer[P any](k RetryLane) retryPlacer[P] {
	switch k.Resolve() {
	case PushFront:
		return frontPlacer[P]{}
	case SideQueue:
		return sidePlacer[P]{}
	default:
		return blockPlacer[P]{}
	}
}

// retryQueue is the redelivery structure of one lane, shared by every
// placement kind: an ordered deque of envelopes, each with the time it
// becomes deliverable again, plus the seal that makes a lane serve nothing
// but its redeliveries, one at a time.
//
// The queue is served before the lane's store, so a redelivery is never
// behind the records that were published after it. While the lane is sealed
// — BlockLane, or any kind under strict ordering — the queue hands out one
// record at a time and the store is not served at all until the queue is
// empty, which is the "strict-ordering retries block their lane" invariant of
// the memory engine.
type retryQueue[P any] struct {
	// sealing is fixed when the lane is built: BlockLane always seals, the
	// other kinds seal when the topic asks for strict ordering.
	sealing bool

	// n mirrors the number of queued redeliveries, so that the paths which
	// only look at the queue can leave the mutex alone.
	n atomic.Int32
	// live mirrors the records the queue is answerable for: the queued ones
	// plus the ones leased under a seal. It is one counter rather than two
	// because it is read without the mutex — a claim on an idle lane never
	// takes it — and a redelivery that moves from leased to queued must
	// never be observed as neither. It is updated only in sync().
	live atomic.Int32
	// due is the earliest time a queued redelivery becomes deliverable, or 0
	// when the queue holds nothing that has to wait. The lane scheduler
	// wakes on it.
	due atomic.Int64

	mu   sync.Mutex
	q    []retryEntry[P]
	head int
	// out is the number of redeliveries leased under the seal, which is at
	// most one. It is guarded by mu, like every field below it.
	out int32
	// timed reports that the queue holds, or held, a redelivery waiting for
	// a due time. It is what keeps the common case — every redelivery
	// deliverable at once — from scanning the queue on every push.
	timed bool
}

// retryEntry is one record waiting for redelivery, with the time it becomes
// deliverable again (0 for immediately).
type retryEntry[P any] struct {
	e  *Envelope[P]
	at int64
}

// len returns the number of queued redeliveries. The caller holds the mutex.
func (r *retryQueue[P]) len() int { return len(r.q) - r.head }

// sync mirrors the queue length, the outstanding total and the earliest due
// time into the atomics the lock-free readers use. Every mutation of the
// queue or of out ends in one call to it, so the counters a reader sees
// without the mutex always describe one state of the queue rather than two.
// The caller holds the mutex.
func (r *retryQueue[P]) sync() {
	n := int32(r.len())
	r.n.Store(n)
	r.live.Store(n + r.out)
	if !r.timed {
		r.due.Store(0)
		return
	}
	at, timed := int64(0), false
	for _, ent := range r.q[r.head:] {
		if ent.at > 0 {
			timed = true
			if at == 0 || ent.at < at {
				at = ent.at
			}
		}
	}
	r.timed = timed
	r.due.Store(at)
}

// compact reclaims the consumed prefix of the deque.
func (r *retryQueue[P]) compact() {
	switch {
	case r.head == len(r.q):
		clear(r.q)
		r.q, r.head = r.q[:0], 0
	case r.head >= 8 && r.head*2 >= len(r.q):
		n := copy(r.q, r.q[r.head:])
		clear(r.q[n:])
		r.q, r.head = r.q[:n], 0
	}
}

// push inserts e. front asks for the head of the queue and ordered asks for
// its place in publication order; neither is consulted while the lane is
// sealed, where publication order is the only order allowed.
//
// A record that was holding the lane's seal gives it back here, in the same
// critical section that queues it: leaving and re-entering the queue is one
// transition, and a claim that saw the halves apart would find the lane
// empty and open with a redelivery still outstanding — and serve the record
// published behind it, which is exactly what strict ordering promises it
// will not do (the memory engine).
func (r *retryQueue[P]) push(e *Envelope[P], at int64, front bool) {
	r.mu.Lock()
	if r.sealing && e.sealed {
		e.sealed = false
		r.out--
	}
	ent := retryEntry[P]{e: e, at: at}
	if at > 0 {
		r.timed = true
	}
	switch {
	case r.sealing:
		// Publication order: scan back from the tail, which stops at once
		// for the common case of one redelivery at a time.
		i := len(r.q)
		for i > r.head && r.q[i-1].e.seq > e.seq {
			i--
		}
		r.q = append(r.q, retryEntry[P]{})
		copy(r.q[i+1:], r.q[i:])
		r.q[i] = ent
	case front:
		if r.head > 0 {
			r.head--
			r.q[r.head] = ent
		} else {
			r.q = append(r.q, retryEntry[P]{})
			copy(r.q[1:], r.q)
			r.q[0] = ent
		}
	default:
		r.q = append(r.q, ent)
	}
	r.sync()
	r.mu.Unlock()
}

// claim takes the redeliveries a consumer may have now and reports whether
// the lane's store may be served as well. A sealed lane hands out at most one
// record and never opens the store while a redelivery is outstanding.
func (r *retryQueue[P]) claim(now int64, dst []*Envelope[P]) (int, bool) {
	if r.live.Load() == 0 {
		return 0, true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealing {
		if r.out > 0 || len(dst) == 0 {
			return 0, false
		}
		if r.len() == 0 {
			return 0, true
		}
		if ent := r.q[r.head]; ent.at == 0 || ent.at <= now {
			r.q[r.head] = retryEntry[P]{}
			r.head++
			r.compact()
			r.out++
			ent.e.sealed = true
			r.sync()
			dst[0] = ent.e
			return 1, false
		}
		return 0, false
	}
	// A lane that is not sealed serves every redelivery that has come due,
	// in queue order, and steps over the ones still waiting for theirs: a
	// redelivery the consumer asked to hold back does not hold back the
	// ones behind it when nothing promises an order.
	n := 0
	kept := r.q[:r.head]
	for _, ent := range r.q[r.head:] {
		if n < len(dst) && (ent.at == 0 || ent.at <= now) {
			dst[n] = ent.e
			n++
			continue
		}
		kept = append(kept, ent)
	}
	if n > 0 {
		clear(r.q[len(kept):])
		r.q = kept
		r.compact()
		r.sync()
	}
	return n, true
}

// release gives the seal back when a redelivery that held it leaves the lane
// for good — settled, dead-lettered or dropped. A redelivery that is placed
// again gives it back inside push instead, so that the lane is never
// observed open between the two.
func (r *retryQueue[P]) release(e *Envelope[P]) {
	if !r.sealing {
		return
	}
	r.mu.Lock()
	if e.sealed {
		e.sealed = false
		r.out--
		r.sync()
	}
	r.mu.Unlock()
}

// depth reports the redeliveries a consumer could take now and the ones still
// waiting for their due time.
func (r *retryQueue[P]) depth(now int64) (ready, delayed int) {
	if r.n.Load() == 0 {
		return 0, 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ent := range r.q[r.head:] {
		if ent.at > 0 && ent.at > now {
			delayed++
			continue
		}
		ready++
	}
	return ready, delayed
}

// next returns the earliest time a queued redelivery becomes deliverable.
func (r *retryQueue[P]) next() int64 { return r.due.Load() }

// oldest removes the redelivery that has been waiting longest, for the
// overflow policies that make room by dropping a record.
func (r *retryQueue[P]) oldest() (*Envelope[P], bool) {
	if r.n.Load() == 0 {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.len() == 0 {
		return nil, false
	}
	e := r.q[r.head].e
	r.q[r.head] = retryEntry[P]{}
	r.head++
	r.compact()
	r.sync()
	return e, true
}

// takeSlot removes the queued redelivery of shared-log slot idx, which is
// how a full shared log takes its oldest slot back from a subscription that
// failed it (Topic.reclaimBase): that record is not necessarily the oldest
// redelivery the queue holds.
func (r *retryQueue[P]) takeSlot(idx uint64) (*Envelope[P], bool) {
	if r.n.Load() == 0 {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := r.head; i < len(r.q); i++ {
		if e := r.q[i].e; e.rec != nil && e.rec.slot == idx {
			r.q = slices.Delete(r.q, i, i+1)
			r.compact()
			r.sync()
			return e, true
		}
	}
	return nil, false
}

// drain removes every queued redelivery and appends it to dst.
func (r *retryQueue[P]) drain(dst []*Envelope[P]) []*Envelope[P] {
	if r.n.Load() == 0 {
		return dst
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ent := range r.q[r.head:] {
		dst = append(dst, ent.e)
	}
	clear(r.q)
	r.q, r.head = r.q[:0], 0
	r.sync()
	return dst
}

// expireTTL removes every queued redelivery whose time to live has run out,
// appends it to dst and returns the earliest expiry still queued.
func (r *retryQueue[P]) expireTTL(now int64, dst []*Envelope[P]) ([]*Envelope[P], int64) {
	if r.n.Load() == 0 {
		return dst, 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	next := int64(0)
	kept := r.q[:r.head]
	for _, ent := range r.q[r.head:] {
		if expired(ent.e, now) {
			dst = append(dst, ent.e)
			continue
		}
		if at := ent.e.rec.expireAt; at > 0 && (next == 0 || at < next) {
			next = at
		}
		kept = append(kept, ent)
	}
	clear(r.q[len(kept):])
	r.q = kept
	r.compact()
	r.sync()
	return dst, next
}
