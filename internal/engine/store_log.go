package engine

import (
	"sync"
	"sync/atomic"

	"github.com/iqhive/iqmq/internal/timer"
)

// logSlot is one entry of a shared partition log.
type logSlot[P any] struct {
	rec  *record[P]
	at   int64 // due time; a slot that is not due yet holds the cursors back
	size int32
	refs int32 // subscriptions that have not settled the record yet
}

// logRing is the shared, sequence-numbered log of one topic partition: the
// storage of the LogCursor architecture (the memory engine). A
// publication writes one slot however many subscriptions read it, each
// subscription reads the log through its own cursor, and a slot is reclaimed
// only when every subscription that was subscribed at publication time has
// settled it. Capacity is therefore shared: the slowest subscription decides
// how much room the publishers have.
type logRing[P any] struct {
	// top is the topic the log belongs to. A reservation reads its
	// subscription set under the ring mutex, which is what makes a
	// publication and a concurrent Subscribe agree on who the publication
	// is for (reserve).
	top *Topic[P]

	mu sync.Mutex
	// slots is the log's storage; slots[off] holds log index base and
	// slots[len-1] holds head-1. Reclaiming advances off rather than moving
	// the slots down, and the slice is compacted only when the reclaimed
	// prefix is worth the copy, so settling a record is O(1) amortised
	// however deep the log is.
	slots []logSlot[P]
	off   int
	base  uint64 // index of the oldest slot still held
	head  uint64 // index the next publication takes

	// pending counts reservations taken but not yet appended, so that
	// concurrent publications cannot both fit into the last slot.
	pending  int
	capMsgs  int
	capBytes int64
	bytes    int64

	// room wakes the publishers parked on the log under OverflowBlock. The
	// log's capacity is shared, so the record that frees a slot may be
	// settled by any subscription — a publisher that waited on one of them
	// would be woken by a settlement that did not reclaim anything and
	// would spin (Topic.waitLogRoom).
	room notifyGate
}

func newLogRing[P any](top *Topic[P], cfg TopologyConfig) *logRing[P] {
	return &logRing[P]{top: top, capMsgs: cfg.CapacityMessages, capBytes: cfg.CapacityBytes}
}

// reserveResult is the outcome of a reservation: the slot was booked, the log
// itself has no room — which is what a slow subscription looks like from the
// publish side, and the only case the DropSlow policy can do anything about —
// or the engine-wide limit refused it. A topic that lost its last
// subscription between the two reports that instead: there is then nothing to
// admit the record to.
type reserveResult uint8

const (
	reserveOK reserveResult = iota
	reserveLogFull
	reserveEngineFull
	reserveNoSubs
)

// reserve books one slot of size bytes together with the engine-wide
// reservation for the subscriptions that will read it, and returns those
// subscriptions. A successful reservation is followed by an append or by
// unreserve, and by nothing else: the one thing left that can refuse the
// publication is its admission ticket (Topic.publishShared), and a refused
// ticket rolls the reservation back field for field.
//
// A subscription that joined between the two starts one slot past a record
// that will never be appended (nextIndexLocked hands it an index past every
// outstanding reservation). That is unobservable: the ticket is refused only
// once the shutdown has stopped accepting, and every lane is drained by the
// settling phase that follows.
//
// The subscription set, the engine-wide reservation and the slot are taken
// under one hold of the ring mutex, which Topic.Subscribe also takes when it
// gives a new subscription its cursor (Topic.storeSubs). That is what keeps
// the two sides of a publication from disagreeing: a subscription is either
// part of this publication — accounted for it and counted in the reference
// count of its slot — or its cursor starts past the slot this reservation
// will take. Taking the set outside the lock let a Subscribe that landed
// between the reservation and the append see a record whose reference count
// and lane gauges were fixed from an older set, which loses the record for
// the subscriptions that were admitted to it and recycles the record while
// one of them still holds it.
// fits reports whether a record of size bytes could ever be held by the log,
// however much of it is reclaimed first. A record that could not is never
// worth displacing another for (lane.fits).
func (r *logRing[P]) fits(size int64) bool { return r.capBytes <= 0 || size <= r.capBytes }

func (r *logRing[P]) reserve(size int64) ([]*Subscription[P], reserveResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	subs := r.top.subs.Load().subs
	if len(subs) == 0 {
		return subs, reserveNoSubs
	}
	if r.capMsgs > 0 && int(r.head-r.base)+r.pending >= r.capMsgs {
		return subs, reserveLogFull
	}
	if r.capBytes > 0 && r.bytes+size > r.capBytes {
		return subs, reserveLogFull
	}
	n := int64(len(subs))
	if !r.top.eng.reserveTotal(n, size*n) {
		return subs, reserveEngineFull
	}
	r.pending++
	r.bytes += size
	return subs, reserveOK
}

// unreserve gives back a slot reservation that will not be appended, in the
// log and in the engine-wide ledger. It is the rollback of publishShared's
// reservation when the shutdown stopped accepting before the append.
//
// It announces the fall on the log's own gate as settle does, and not only
// through the engine's: what a publisher parked in Topic.waitLogRoom
// re-checks is logRing.hasRoom, which counts this reservation against the
// log's capacity, and the engine's gate is registered on only when the engine
// has a total limit at all. Nothing reaches this today but a publication the
// shutdown refused, whose parked publisher also leaves on the engine's
// context — but the rule is the gate's and not the caller's, and the
// park/notify scan holds every fall in the log's counters to it.
func (r *logRing[P]) unreserve(size int64, subs int) {
	r.mu.Lock()
	r.pending--
	r.bytes -= size
	r.mu.Unlock()
	r.room.notify()
	n := int64(subs)
	r.top.eng.releaseTotal(n, size*n)
}

// nextIndexLocked returns the log index past every slot the ring holds and
// every reservation it has handed out, which is where a subscription that
// joins now starts reading. The caller holds the ring mutex.
func (r *logRing[P]) nextIndexLocked() uint64 { return r.head + uint64(r.pending) }

// append writes the reserved slot. refs is the number of subscriptions that
// will read it.
//
// The record's slot index is stamped on the record here, under the same lock
// and before the slot is visible: a consumer that claimed the record from
// another goroutine settles it by that index, so a record must never be
// reachable through the log without it.
func (r *logRing[P]) append(rec *record[P], at int64, size, refs int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending--
	rec.slot = r.head
	r.head++
	r.slots = append(r.slots, logSlot[P]{rec: rec, at: at, size: size, refs: refs})
}

// settle drops one subscription's reference to a slot and reclaims every slot
// at the tail that no subscription holds any more.
func (r *logRing[P]) settle(idx uint64) {
	r.mu.Lock()
	if idx < r.base || idx >= r.head {
		r.mu.Unlock()
		return
	}
	s := &r.slots[r.off+int(idx-r.base)]
	if s.refs > 0 {
		s.refs--
	}
	r.trimLocked()
	r.mu.Unlock()
	// Whoever is parked for room looks again. The wake is one atomic load
	// unless a publisher is actually waiting (notifyGate.notify), and it is
	// unconditional: what a settlement reclaims depends on the whole tail,
	// so this is the cheaper answer than deciding it here. The engine-wide
	// limit the same reservation has to pass is released after this, and
	// wakes the publisher through the engine's own gate (Engine.releaseTotal,
	// Topic.waitLogRoom).
	r.room.notify()
}

// hasRoom reports whether a reservation of size bytes would find room now,
// both in the log and engine-wide. It is what a parked publisher re-checks:
// a wait that woke on either condition alone would spin against the other.
func (r *logRing[P]) hasRoom(size int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.capMsgs > 0 && int(r.head-r.base)+r.pending >= r.capMsgs {
		return false
	}
	if r.capBytes > 0 && r.bytes+size > r.capBytes {
		return false
	}
	n := int64(len(r.top.subs.Load().subs))
	return r.top.eng.hasTotalRoom(n, size*n)
}

// trimLocked reclaims the slots at the tail that hold no reference.
func (r *logRing[P]) trimLocked() {
	n := 0
	for r.off+n < len(r.slots) && r.slots[r.off+n].refs == 0 {
		r.bytes -= int64(r.slots[r.off+n].size)
		r.slots[r.off+n].rec = nil
		n++
	}
	if n == 0 {
		return
	}
	r.off += n
	r.base += uint64(n)
	if r.off >= len(r.slots) {
		r.slots = r.slots[:0]
		r.off = 0
		return
	}
	if r.off > len(r.slots)/2 {
		r.slots = append(r.slots[:0], r.slots[r.off:]...)
		r.off = 0
	}
}

// logRef is one record a cursor took from the log.
type logRef[P any] struct {
	rec *record[P]
	idx uint64
}

// claim reads up to len(dst) records the cursor at from can take now and
// returns how many it wrote and where the cursor lands. A slot that is not
// due yet stops the scan: a log is ordered, so a delayed publication holds
// the records behind it.
func (r *logRing[P]) claim(from uint64, now int64, dst []logRef[P]) (int, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := from
	if i < r.base {
		i = r.base
	}
	n := 0
	for n < len(dst) && i < r.head {
		s := &r.slots[r.off+int(i-r.base)]
		if s.at > now {
			break
		}
		dst[n] = logRef[P]{rec: s.rec, idx: i}
		n++
		i++
	}
	return n, i
}

// takeNotDue hands the cursor at from the slot it points to when that slot is
// not due yet, together with the cursor position that follows and the due
// time. It is how a delayed publication leaves the log for the
// subscriptions' own delayed structures instead of holding up the records
// behind it.
func (r *logRing[P]) takeNotDue(from uint64, now int64) (logRef[P], uint64, int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := from
	if i < r.base {
		i = r.base
	}
	if i >= r.head {
		return logRef[P]{}, i, 0, false
	}
	s := &r.slots[r.off+int(i-r.base)]
	if s.at <= now {
		return logRef[P]{}, i, 0, false
	}
	return logRef[P]{rec: s.rec, idx: i}, i + 1, s.at, true
}

// dropAt takes the slot the cursor at from points to, whatever its due time,
// and returns it with the cursor position that follows. It is how a
// subscription gives up its oldest record: the reference it holds is released
// by the settle that follows, and the slot leaves the log once every other
// subscription has done the same.
func (r *logRing[P]) dropAt(from uint64) (logRef[P], uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := from
	if i < r.base {
		i = r.base
	}
	if i >= r.head {
		return logRef[P]{}, i, false
	}
	s := &r.slots[r.off+int(i-r.base)]
	return logRef[P]{rec: s.rec, idx: i}, i + 1, true
}

// count reports how many records the cursor at from could take now and how
// many are still waiting for their due time.
func (r *logRing[P]) count(from uint64, now int64) (ready, delayed int, next uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := from
	if i < r.base {
		i = r.base
	}
	next = i
	for j := i; j < r.head; j++ {
		s := &r.slots[r.off+int(j-r.base)]
		if s.at > now {
			delayed++
			continue
		}
		ready++
	}
	return ready, delayed, next
}

// nextDue returns the earliest due time at or after the cursor from, or 0.
func (r *logRing[P]) nextDue(from uint64, now int64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := from
	if i < r.base {
		i = r.base
	}
	best := int64(0)
	for j := i; j < r.head; j++ {
		s := &r.slots[r.off+int(j-r.base)]
		if s.at <= now {
			continue
		}
		if best == 0 || s.at < best {
			best = s.at
		}
	}
	return best
}

// expireTTL takes the run of slots at the cursor whose records have outlived
// their time to live and returns it with the cursor position that follows and
// the earliest expiry still ahead. Only the run at the cursor can be taken: a
// subscription releases the reference it holds on a slot exactly once, and
// the cursor is what records which references it has released. An expired
// slot further along the log is dropped when the cursor reaches it, which the
// lane's claim path does before it would lease it.
func (r *logRing[P]) expireTTL(from uint64, now int64, dst []logRef[P]) ([]logRef[P], uint64, int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := from
	if i < r.base {
		i = r.base
	}
	next := int64(0)
	for i < r.head {
		s := &r.slots[r.off+int(i-r.base)]
		at := s.rec.expireAt
		if at <= 0 || at > now {
			break
		}
		dst = append(dst, logRef[P]{rec: s.rec, idx: i})
		i++
	}
	for j := i; j < r.head; j++ {
		at := r.slots[r.off+int(j-r.base)].rec.expireAt
		if at <= 0 || at <= now {
			// A slot that is already expired but sits behind one that is
			// not cannot be swept: a subscription releases its reference to
			// a slot exactly once and in cursor order, so this one leaves
			// when the cursor reaches it — the claim path drops an expired
			// record rather than delivering it. Naming its time here would
			// hand lane.nextEvent an instant that has already passed, which
			// it turns into a wake-up every minRearm, so a stalled
			// subscription would poll the one scheduler goroutine of the
			// engine for as long as it stayed stalled.
			continue
		}
		if next == 0 || at < next {
			next = at
		}
	}
	return dst, i, next
}

// logStore is one subscription's view of a shared partition log: a cursor
// into the log, a redelivery queue for the records the subscription has
// nacked or lost a lease on, and a delayed structure for the ones that are
// coming back later.
//
// Redeliveries leave the log — the cursor never goes backwards — and the
// redelivery queue is served before the log, which is what a log broker with
// a retry queue does. That is the one way this architecture differs
// observably from the per-subscription ones beyond capacity: after a negative
// acknowledgement, delivery order is no longer publication order (Arch.Ordered).
type logStore[P any] struct {
	ln   *lane[P]
	ring *logRing[P]

	ndelay atomic.Int32

	mu     sync.Mutex
	cursor uint64
	// retry is the redelivery queue, oldest first, and rhead is the index of
	// its oldest entry: a pop advances the index instead of shifting the
	// slice down, which keeps a claim O(1) however deep the queue is. The
	// prefix left behind is reclaimed by popRetry once it is worth the copy.
	retry   []*Envelope[P]
	rhead   int
	refs    []logRef[P]
	delayed timer.Scheduler[*Envelope[P]]
	fired   []*timer.Entry[*Envelope[P]]
}

// retryLen returns the number of redeliveries waiting. The caller holds the
// store mutex.
func (s *logStore[P]) retryLen() int { return len(s.retry) - s.rhead }

// popRetry takes the oldest redelivery. The caller holds the store mutex.
func (s *logStore[P]) popRetry() (*Envelope[P], bool) {
	if s.rhead >= len(s.retry) {
		return nil, false
	}
	e := s.retry[s.rhead]
	s.retry[s.rhead] = nil
	s.rhead++
	switch {
	case s.rhead == len(s.retry):
		s.retry, s.rhead = s.retry[:0], 0
	case s.rhead >= 32 && s.rhead*2 >= len(s.retry):
		// The reclaimed prefix is now at least half the slice, so one copy
		// per half a queue keeps the amortised cost of a pop constant.
		s.retry = append(s.retry[:0], s.retry[s.rhead:]...)
		s.rhead = 0
	}
	return e, true
}

// resetRetry empties the redelivery queue. The caller holds the store mutex.
func (s *logStore[P]) resetRetry() {
	clear(s.retry)
	s.retry, s.rhead = s.retry[:0], 0
}

// newLogStore builds the store with a cursor of zero. The cursor a
// subscription actually starts from is set by Topic.storeSubs, under the ring
// mutex and together with the store of the subscription set, so that a
// publication cannot be half-way between its reservation and its append while
// the cursor is chosen (logRing.reserve).
func newLogStore[P any](ln *lane[P]) (*logStore[P], error) {
	delayed, err := timer.New[*Envelope[P]](ln.sub.eng.kinds.Timer, ln.sub.eng.now())
	if err != nil {
		return nil, err
	}
	return &logStore[P]{
		ln:      ln,
		ring:    ln.sub.topic.ring(int(ln.idx)),
		delayed: delayed,
	}, nil
}

// Publish does nothing: the topic wrote one slot for the whole publication
// and every cursor behind the head already sees it.
func (s *logStore[P]) Publish(*record[P], int64, int64) {}

// Requeue takes a record out of the log and into the subscription's own
// redelivery structures; its log reference stays held until it is settled.
func (s *logStore[P]) Requeue(e *Envelope[P], at, now int64) {
	s.mu.Lock()
	if at > now {
		e.deadline = at
		s.delayed.Add(&e.tnode, at)
		s.ndelay.Add(1)
	} else {
		e.deadline = 0
		s.retry = append(s.retry, e)
	}
	s.mu.Unlock()
}

// Settle drops the subscription's reference to the record's slot, which is
// what lets the log reclaim it once every subscription has done the same.
func (s *logStore[P]) Settle(e *Envelope[P]) {
	if e.rec != nil {
		s.ring.settle(e.rec.slot)
	}
}

// deferNotDue moves the slots at the cursor that are not due yet into the
// subscription's own delayed structure and advances the cursor past them. A
// log is ordered, so a delayed publication would otherwise hold up every
// record behind it for every subscription; taking it out of the log for the
// subscription that reached it keeps the delay a property of the record and
// not of the log. The reference the subscription holds on the slot stays held
// until the record is settled, so the capacity is still shared.
//
// The caller holds the store mutex.
func (s *logStore[P]) deferNotDue(now int64) {
	for {
		ref, next, at, ok := s.ring.takeNotDue(s.cursor, now)
		if !ok {
			return
		}
		e := s.ln.newEnvelope(ref.rec)
		e.deadline = at
		s.delayed.Add(&e.tnode, at)
		s.ndelay.Add(1)
		s.cursor = next
	}
}

// Claim serves the redelivery queue first and then the log.
func (s *logStore[P]) Claim(now int64, dst []*Envelope[P]) int {
	if s.detached() {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pump(now)
	n := 0
	for n < len(dst) {
		e, ok := s.popRetry()
		if !ok {
			break
		}
		dst[n] = e
		n++
	}
	for n < len(dst) {
		s.deferNotDue(now)
		// The scratch is re-sliced to the window that is read back: its own
		// length is whatever the last user left behind — the time-to-live
		// sweep empties it — and indexing that instead of the window handed
		// to the log would read past the end of it.
		refs := s.refsFor(len(dst) - n)
		got, next := s.ring.claim(s.cursor, now, refs)
		s.cursor = next
		if got == 0 {
			break
		}
		for i := range got {
			dst[n] = s.ln.newEnvelope(refs[i].rec)
			n++
		}
	}
	return n
}

// refsFor returns the store's scratch slice, grown to hold n log references.
// The caller holds the store mutex.
func (s *logStore[P]) refsFor(n int) []logRef[P] {
	if cap(s.refs) < n {
		s.refs = make([]logRef[P], n)
	}
	return s.refs[:n]
}

// pump moves the due delayed envelopes into the redelivery queue. The caller
// holds the store mutex.
func (s *logStore[P]) pump(now int64) int {
	if s.ndelay.Load() == 0 {
		return 0
	}
	s.fired = s.delayed.Fire(now, s.fired[:0])
	sortFired(s.fired)
	for _, ent := range s.fired {
		s.retry = append(s.retry, ent.Value)
	}
	n := len(s.fired)
	s.ndelay.Add(int32(-n))
	clear(s.fired)
	s.fired = s.fired[:0]
	return n
}

// Depth reports what the subscription still holds in the log and in its own
// structures.
func (s *logStore[P]) Depth(now int64) (int, int) {
	if s.detached() {
		return 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pump(now)
	s.deferNotDue(now)
	ready, delayed, next := s.ring.count(s.cursor, now)
	s.cursor = next
	return ready + s.retryLen(), delayed + int(s.ndelay.Load())
}

// DropOldest takes the oldest record the subscription still holds: its
// redelivery queue first, then its delayed structure, then the next log slot
// its cursor has not passed.
//
// That order is the one that frees capacity. The records a subscription has
// taken out of the log — promoted redeliveries and records waiting for a due
// time — still hold their slot, and they hold the ones nearest the tail,
// which is where the log reclaims. Dropping a slot the cursor has not reached
// while an older one is still held frees nothing, and the publisher would go
// on dropping until it reached the record that was actually in the way.
func (s *logStore[P]) DropOldest() (*Envelope[P], bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.popRetry(); ok {
		return e, true
	}
	if s.ndelay.Load() > 0 {
		var (
			e  *Envelope[P]
			ok bool
		)
		s.delayed, s.fired, e, ok = oldestDelayed(s.delayed, s.ln.sub.eng.now(), s.fired)
		clear(s.fired)
		s.fired = s.fired[:0]
		if ok {
			s.ndelay.Add(-1)
			return e, true
		}
	}
	if ref, next, ok := s.ring.dropAt(s.cursor); ok {
		s.cursor = next
		return s.ln.newEnvelope(ref.rec), true
	}
	return nil, false
}

// Drain removes everything the subscription still holds.
func (s *logStore[P]) Drain(dst []*Envelope[P]) []*Envelope[P] {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fired = s.delayed.Fire(neverExpires, s.fired[:0])
	sortFired(s.fired)
	for _, ent := range s.fired {
		dst = append(dst, ent.Value)
	}
	s.ndelay.Store(0)
	clear(s.fired)
	s.fired = s.fired[:0]
	dst = append(dst, s.retry[s.rhead:]...)
	s.resetRetry()
	for {
		const batch = 32
		refs := s.refsFor(batch)
		got, next := s.ring.claim(s.cursor, neverExpires, refs)
		s.cursor = next
		for i := range got {
			dst = append(dst, s.ln.newEnvelope(refs[i].rec))
		}
		if got < batch {
			return dst
		}
	}
}

// Next returns the earliest time the subscription would get a record it
// cannot get now: its own delayed structure, or a log slot that is not due.
func (s *logStore[P]) Next() int64 {
	if s.detached() {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	best := int64(0)
	if s.ndelay.Load() > 0 {
		if at, ok := s.delayed.Next(); ok {
			best = at
		}
	}
	if at := s.ring.nextDue(s.cursor, s.ln.sub.eng.now()); at != 0 && (best == 0 || at < best) {
		best = at
	}
	return best
}

// ExpireTTL discards what the subscription still holds and has outlived its
// time to live: its redelivery queue, its delayed structure and the log slots
// its cursor has not passed.
func (s *logStore[P]) ExpireTTL(now int64, dst []*Envelope[P]) ([]*Envelope[P], int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := int64(0)
	note := func(e *Envelope[P]) {
		if at := e.rec.expireAt; at > 0 && (next == 0 || at < next) {
			next = at
		}
	}
	// The survivors are written back over the queue from its head, so the
	// prefix a pop has already reclaimed is reclaimed here too.
	kept := s.retry[:0]
	for _, e := range s.retry[s.rhead:] {
		if expired(e, now) {
			dst = append(dst, e)
			continue
		}
		note(e)
		kept = append(kept, e)
	}
	clear(s.retry[len(kept):])
	s.retry, s.rhead = kept, 0
	if s.ndelay.Load() > 0 {
		s.fired = s.delayed.Fire(neverExpires, s.fired[:0])
		sortFired(s.fired)
		n := 0
		for _, ent := range s.fired {
			e := ent.Value
			if expired(e, now) {
				dst = append(dst, e)
				continue
			}
			note(e)
			s.delayed.Add(ent, e.deadline)
			n++
		}
		s.ndelay.Store(int32(n))
		clear(s.fired)
		s.fired = s.fired[:0]
	}
	var at int64
	var cursor uint64
	s.refs, cursor, at = s.ring.expireTTL(s.cursor, now, s.refs[:0])
	s.cursor = cursor
	for _, ref := range s.refs {
		dst = append(dst, s.ln.newEnvelope(ref.rec))
	}
	s.refs = s.refs[:0]
	if at > 0 && (next == 0 || at < next) {
		next = at
	}
	return dst, next
}

// detached reports whether the subscription has left the topic. Its cursor
// then stops meaning anything: the log goes on growing for the subscriptions
// that are still reading it, and a detached cursor must not count those slots
// as records it holds — it released every reference it had when it drained.
func (s *logStore[P]) detached() bool {
	return s.ln.sub.closed.Load() || s.ln.sub.eng.closing()
}

// Promote moves the redeliveries that have come due into the retry queue.
// The log itself needs no promotion: a cursor simply stops at a slot that is
// not due yet.
func (s *logStore[P]) Promote(now int64) int {
	if s.detached() {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.pump(now)
	s.deferNotDue(now)
	return n
}
