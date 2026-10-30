package memq

import (
	"errors"
	"math"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/subject"
)

// Step applies op and returns the result the engine must produce. A harness
// error, such as an undefined Kind, is reported in Result.Err.
func (m *Model) Step(op Op) Result {
	m.steps++
	switch op.Kind {
	case Subscribe:
		return m.subscribe(op)
	case Attach:
		return m.attach(op)
	case CloseConsumer:
		return m.closeConsumer(op)
	case Publish:
		return m.publish(op)
	case Fetch:
		return m.fetch(op)
	case Ack:
		return m.settle(op, true)
	case Nack:
		return m.settle(op, false)
	case Extend:
		return m.extend(op)
	case Advance:
		m.Advance(op.Dt)
		return Result{}
	case ExpireLeases:
		return Result{N: m.expire()}
	case CloseSubscription:
		return m.closeSub(op)
	case CloseEngine:
		m.closeEngine()
		return Result{}
	case Seek:
		return m.seek(op)
	case Rebalance:
		return m.rebalanceOp(op)
	default:
		return Result{Err: errors.New("memq: undefined operation " + op.Kind.String())}
	}
}

func (m *Model) subscribe(op Op) Result {
	if m.closed {
		return Result{Err: errcode.ErrClosed}
	}
	if _, ok := m.byName[op.Sub]; ok {
		return Result{}
	}
	filter, err := m.compileFilter(op.Filter)
	if err != nil {
		return Result{Err: err}
	}
	s := &sub{
		name:      op.Sub,
		filter:    filter,
		lanes:     make([]lane, m.cfg.Partitions),
		recs:      map[uint64]*rec{},
		consumers: map[uint32]bool{},
		owner:     make([]uint32, m.cfg.Partitions),
	}
	m.subs = append(m.subs, s)
	m.byName[op.Sub] = s
	// Everything the topic still retains was published before this
	// subscription existed, so the whole log is its replay and the live path
	// covers exactly what follows. The engine draws the same boundary with a
	// generation of the subscription set rather than with a sequence, because
	// its publish path is concurrent and a record's sequence does not say
	// which set it was routed on (engine Topic.subscribe, joinBoundary); stepped
	// one operation at a time, the two are the same boundary.
	m.replay(s, op.Start, m.seq)
	return Result{}
}

// compileFilter validates a subscription's subject pattern against the
// wildcard setting.
//
// A nil pattern is the match-everything filter, which is a value rather than
// a missing one (engine Topic.compileFilter).
func (m *Model) compileFilter(pat string) (*subject.Pattern, error) {
	if pat == "" {
		return nil, nil //nolint:nilnil // a nil pattern is the match-everything filter
	}
	if !m.cfg.Wildcards {
		return nil, ErrNoWildcards
	}
	return subject.Compile(pat)
}

// rebalanceOp recomputes a subscription's assignment on demand, which is what
// engine.Subscription.Rebalance and Faults.Reconnect do.
func (m *Model) rebalanceOp(op Op) Result {
	_, s := m.find(op.Sub)
	if m.closed || s == nil || s.closed {
		return Result{Err: errcode.ErrClosed}
	}
	m.rebalance(s)
	m.revoke(s, 0)
	return Result{}
}

func (m *Model) attach(op Op) Result {
	s := m.byName[op.Sub]
	if m.closed || s == nil || s.closed {
		return Result{Err: errcode.ErrClosed}
	}
	m.join(s, op.Consumer)
	return Result{}
}

func (m *Model) closeConsumer(op Op) Result {
	_, s := m.find(op.Sub)
	if s == nil || !s.consumers[op.Consumer] {
		return Result{}
	}
	// The leave rebalances first and then revokes: the lanes the consumer is
	// giving up already belong to whoever takes them over, and the records
	// go back through the retry placement without consulting MaxAttempts —
	// a rebalance is not a failed delivery (engine Consumer.Close).
	m.leave(s, op.Consumer)
	return Result{}
}

func (m *Model) publish(op Op) Result {
	if m.closed {
		return Result{Err: errcode.ErrClosed}
	}
	if !m.dedupAdmit(op.DedupKey) {
		// The topic has seen this key inside its window: nothing is
		// admitted, the payload comes straight back and the duplicate is
		// counted (the message contract).
		m.deduped++
		return Result{Deduped: true}
	}
	live := m.targets(op.Subject)
	size := op.Size
	if len(live) == 0 {
		// A stream's retained log is a property of the topic, not of its
		// subscription set: the publication is still retained and still
		// replayable (engine Topic.retainOnly).
		m.orphans++
		if !m.retains() {
			return Result{Orphan: true}
		}
		m.retainOnly(m.partitionFor(op), op, size)
		return Result{Orphan: true, Retained: true}
	}
	part := m.partitionFor(op)
	if m.cfg.SharedLog {
		// One shared slot decides for every subscription at once, and the
		// log checks its own capacity before the engine-wide limit.
		if !m.reserveShared(part, live, size) {
			return m.overflow(live, part, op, size)
		}
		m.logs[part].append(m.seq+1, size, len(live))
		for _, s := range live {
			ln := &s.lanes[part]
			ln.depth++
			ln.bytes += size
		}
	} else {
		// The engine reserves subscription by subscription, the
		// engine-wide slot first and then the lane, and under DropOldest a
		// lane makes room for either: a record it drops gives back its
		// engine-wide slot as well as its own (engine Topic.reserveTotalIn,
		// Topic.reserveLane).
		if m.cfg.Overflow == OverflowDropOldest {
			// One refusal refuses every subscription, so no lane displaces
			// a record until every lane is known to take the publication
			// (engine Topic.reserveAll).
			for _, s := range live {
				if ln := &s.lanes[part]; !m.laneHasRoom(ln, size) && !m.roomAfterDrops(s, ln, size) {
					return m.overflow(live, part, op, size)
				}
			}
			// The engine-wide limit is the publication's as a whole, and the
			// lanes make room for all of it together before any of them
			// books its share (engine Topic.reserveAll).
			if !m.fitsTogether(live, part, size) {
				return m.overflow(live, part, op, size)
			}
			m.makeRoomTogether(live, part, size)
		}
		done := 0
		for _, s := range live {
			if !m.reserveTotalIn(s, &s.lanes[part], size) {
				break
			}
			if !m.admitLane(s, &s.lanes[part], size) {
				m.releaseTotal(1, size)
				break
			}
			done++
		}
		if done < len(live) {
			for i := range done {
				m.unreserveLane(&live[i].lanes[part], size)
				m.releaseTotal(1, size)
			}
			return m.overflow(live, part, op, size)
		}
	}
	m.seq++
	m.rid++
	origin, id := m.seq, m.rid
	prio := m.prioOf(op.Priority)
	m.refs[origin] = len(live)
	if m.cfg.SafeCopy {
		// The publisher's payload comes back as soon as every clone is
		// taken; the clones themselves are engine-owned.
		m.release[origin]++
	}
	for _, s := range live {
		r := &rec{seq: id, origin: origin, size: size, lane: part, state: Ready, prio: prio}
		if op.Delay > 0 {
			r.state = Delayed
			r.reordered = true
			r.dueAt = m.now + op.Delay
			s.lanes[part].delayed = append(s.lanes[part].delayed, id)
		} else {
			s.lanes[part].ready.push(id, prio)
		}
		s.recs[id] = r
		s.order = append(s.order, id)
		s.st.published++
	}
	if m.retains() {
		// The retained log holds a reference of its own, which is why an
		// owned payload is handed back only once retention has let go of it.
		m.retain(part, origin, size, prio, op.Subject, m.dueOf(op))
	}
	return Result{}
}

// targets returns the subscriptions a publication fans out to: the live ones
// whose filter matches its subject.
func (m *Model) targets(subj string) []*sub {
	out := make([]*sub, 0, len(m.subs))
	for _, s := range m.subs {
		if !s.closed && m.deliverable(s, subj) {
			out = append(out, s)
		}
	}
	return out
}

func (m *Model) rejectAll(subs []*sub) {
	for _, s := range subs {
		s.st.rejected++
	}
}

// dueOf returns the time a publication is due, or 0 when it is due at once.
func (m *Model) dueOf(op Op) time.Duration {
	if op.Delay > 0 {
		return m.now + op.Delay
	}
	return 0
}

func (m *Model) reserveTotal(n, size int64) bool {
	if m.cfg.MaxTotalMessages > 0 && m.msgs+n > int64(m.cfg.MaxTotalMessages) {
		return false
	}
	if m.cfg.MaxTotalBytes > 0 && m.bytes+size > m.cfg.MaxTotalBytes {
		return false
	}
	m.msgs += n
	m.bytes += size
	return true
}

func (m *Model) releaseTotal(n, size int64) { //nolint:unparam // mirrors reserveTotal, whose n varies
	m.msgs -= n
	m.bytes -= size
}

func (m *Model) reserveLane(ln *lane, size int64) bool {
	if m.cfg.CapacityMessages > 0 && ln.depth >= m.cfg.CapacityMessages {
		return false
	}
	if m.cfg.CapacityBytes > 0 && ln.bytes+size > m.cfg.CapacityBytes {
		return false
	}
	ln.depth++
	ln.bytes += size
	return true
}

// reserveTotalIn books one record against the engine-wide limits, first
// making room for it in ln when the overflow policy allows a record to be
// displaced: a record the lane drops gives its engine-wide share back too
// (engine Topic.reserveTotalIn). A lane that could not make room even by
// dropping everything but its leases displaces nothing, for the engine-wide
// limits or for its own capacity. The model's one fan-out has every lane
// pass the lane's half before any drop (Model.publish), so here it only
// keeps the model's rule the engine's. Its one fan-out is Atomic, whose
// lanes have made the engine-wide room together before they get here
// (Model.makeRoomTogether), so a single-goroutine model never drops here.
func (m *Model) reserveTotalIn(s *sub, ln *lane, size int64) bool {
	if m.reserveTotal(1, size) {
		return true
	}
	if m.cfg.Overflow != OverflowDropOldest || !m.dropsMakeRoom(s, ln, size) {
		return false
	}
	for m.dropOldest(s, ln) {
		if m.reserveTotal(1, size) {
			return true
		}
	}
	return false
}

// dropsMakeRoom reports whether ln's own drops could make room for one
// record of size bytes in the lane and under the engine-wide limits (engine
// Topic.dropsMakeRoom).
func (m *Model) dropsMakeRoom(s *sub, ln *lane, size int64) bool {
	return m.roomAfterDrops(s, ln, size) && m.totalRoomAfterDrops(s, ln, size)
}

// totalRoomAfterDrops reports whether the engine-wide limits would take one
// more record of size bytes once ln had dropped everything but its leases
// (engine Engine.roomAfterDrops).
func (m *Model) totalRoomAfterDrops(s *sub, ln *lane, size int64) bool {
	if m.cfg.MaxTotalBytes > 0 && size > m.cfg.MaxTotalBytes {
		return false
	}
	var leased int64
	for _, seq := range ln.live {
		leased += s.recs[seq].size
	}
	n := int64(ln.depth - len(ln.live))
	b := ln.bytes - leased
	return (m.cfg.MaxTotalMessages <= 0 || m.msgs-n+1 <= int64(m.cfg.MaxTotalMessages)) &&
		(m.cfg.MaxTotalBytes <= 0 || m.bytes-b+size <= m.cfg.MaxTotalBytes)
}

// fitsTogether reports whether the engine-wide limits take a publication to
// subs, one record and its bytes for each, now or once the lanes together
// have dropped everything but their leases (engine Topic.fitsTogether).
func (m *Model) fitsTogether(subs []*sub, part int, size int64) bool {
	k := int64(len(subs))
	return m.hasTotalRoom(k, size*k) || m.publicationRoomAfterDrops(subs, part, size)
}

// makeRoomTogether has every lane of the publication give up its oldest
// record in turn, one per pass, until the engine-wide limits would take a
// record for each of them (engine Topic.makeRoomTogether), which is the
// shared log's rule under the same limits (Model.reserveShared).
func (m *Model) makeRoomTogether(live []*sub, part int, size int64) {
	k := int64(len(live))
	for !m.hasTotalRoom(k, size*k) {
		if !m.dropEach(live, part) {
			return
		}
	}
}

// dropEach has every subscription give up its oldest record in partition
// part, one each, and reports whether any of them had one to give.
func (m *Model) dropEach(live []*sub, part int) bool {
	freed := false
	for _, s := range live {
		if m.dropOldest(s, &s.lanes[part]) {
			freed = true
		}
	}
	return freed
}

// hasTotalRoom reports whether the engine-wide limits would take n more
// records of size bytes now (engine Engine.hasTotalRoom).
func (m *Model) hasTotalRoom(n, size int64) bool {
	return (m.cfg.MaxTotalMessages <= 0 || m.msgs+n <= int64(m.cfg.MaxTotalMessages)) &&
		(m.cfg.MaxTotalBytes <= 0 || m.bytes+size <= m.cfg.MaxTotalBytes)
}

// publicationRoomAfterDrops reports whether the engine-wide limits would take
// a publication of size bytes, one record and its bytes for each of subs,
// once every one of them had dropped everything but its leases (engine
// Topic.publicationRoomAfterDrops).
func (m *Model) publicationRoomAfterDrops(subs []*sub, part int, size int64) bool {
	maxMsgs, maxBytes := int64(m.cfg.MaxTotalMessages), m.cfg.MaxTotalBytes
	if maxMsgs <= 0 && maxBytes <= 0 {
		return true
	}
	var n, b int64
	for _, s := range subs {
		ln := &s.lanes[part]
		var leased int64
		for _, seq := range ln.live {
			leased += s.recs[seq].size
		}
		n += int64(ln.depth - len(ln.live))
		b += ln.bytes - leased
	}
	k := int64(len(subs))
	return (maxMsgs <= 0 || (k <= maxMsgs && m.msgs-n+k <= maxMsgs)) &&
		(maxBytes <= 0 || (size*k <= maxBytes && m.bytes-b+size*k <= maxBytes))
}

// laneHasRoom reports whether the lane could take a record of size bytes now
// (engine lane.hasRoom).
func (m *Model) laneHasRoom(ln *lane, size int64) bool {
	if m.cfg.CapacityMessages > 0 && ln.depth >= m.cfg.CapacityMessages {
		return false
	}
	return m.cfg.CapacityBytes <= 0 || ln.bytes+size <= m.cfg.CapacityBytes
}

// roomAfterDrops reports whether the lane would take a record of size bytes
// once it had dropped everything but its leases (engine lane.roomAfterDrops).
func (m *Model) roomAfterDrops(s *sub, ln *lane, size int64) bool {
	if !m.fits(size) {
		return false
	}
	if m.cfg.CapacityMessages > 0 && len(ln.live) >= m.cfg.CapacityMessages {
		return false
	}
	var b int64
	for _, seq := range ln.live {
		b += s.recs[seq].size
	}
	return m.cfg.CapacityBytes <= 0 || b+size <= m.cfg.CapacityBytes
}

// admitLane books one record in the lane, first making room for it when the
// overflow policy allows a record to be displaced (engine Topic.reserveLane).
func (m *Model) admitLane(s *sub, ln *lane, size int64) bool {
	if m.reserveLane(ln, size) {
		return true
	}
	// A record the lane could never hold displaces nothing (engine
	// lane.fits): emptying a lane for a record that will not fit is the one
	// drop that buys nothing.
	if m.cfg.Overflow != OverflowDropOldest || !m.fits(size) {
		return false
	}
	for m.dropOldest(s, ln) {
		if m.reserveLane(ln, size) {
			return true
		}
	}
	return false
}

// reserveShared books one shared slot and the engine-wide capacity that goes
// with it, making room by displacing records when the overflow policy allows.
//
// What a drop can do depends on which half refused, which the log checks in
// order (engine logRing.reserve). A full log has room only its base can
// make, so its base slot goes from every subscription still holding it
// (reclaimBase); under the engine-wide limit any record of the topic makes
// room, so every subscription gives up its own oldest record, unless even
// emptying them all would leave the limit refusing the reservation (engine
// Topic.makeRoomShared).
func (m *Model) reserveShared(part int, live []*sub, size int64) bool {
	n := int64(len(live))
	take := func() (ok, logFull bool) {
		if !m.logs[part].reserve(m.cfg, size) {
			return false, true
		}
		return m.reserveTotal(n, size*n), false
	}
	ok, logFull := take()
	if ok {
		return true
	}
	if m.cfg.Overflow != OverflowDropOldest || !m.fits(size) {
		return false
	}
	for {
		freed := false
		switch {
		case logFull:
			freed = m.reclaimBase(part, live, size)
		case m.publicationRoomAfterDrops(live, part, size):
			freed = m.dropEach(live, part)
		}
		if !freed {
			return false
		}
		if ok, logFull = take(); ok {
			return true
		}
	}
}

// reclaimBase gives up the oldest slot of a full shared log for every
// subscription still holding it, and reports whether it took any (engine
// Topic.reclaimBase). A record from any other slot frees nothing, so a
// subscription that has read past the base keeps everything it holds.
// Nothing is dropped while a lease holds the base, nor when the slots ahead
// of the oldest leased one would not free enough of the log for the record,
// nor when the engine-wide limit would refuse the reservation once the log
// had room.
func (m *Model) reclaimBase(part int, live []*sub, size int64) bool {
	lg := &m.logs[part]
	if len(lg.slots) == 0 {
		return false
	}
	base := lg.slots[0].seq
	leased := uint64(math.MaxUint64)
	for _, s := range live {
		for _, seq := range s.lanes[part].live {
			leased = min(leased, s.recs[seq].origin)
		}
	}
	if leased <= base || !lg.roomAfterDrops(m.cfg, size, leased) || !m.publicationRoomAfterDrops(live, part, size) {
		return false
	}
	freed := false
	for _, s := range live {
		if m.dropOrigin(s, &s.lanes[part], base) {
			freed = true
		}
	}
	return freed
}

// dropOrigin discards the subscription's record of publication origin when
// a drop can reach it, wherever it waits: ready, delayed or for redelivery.
// It reports false when the subscription has settled it, holds it on a lease
// or never held it (engine lane.takeSlot).
func (m *Model) dropOrigin(s *sub, ln *lane, origin uint64) bool {
	for i, seq := range ln.ready.seqs {
		if s.recs[seq].origin == origin {
			ln.ready.removeAt(i)
			return m.displace(s, ln, seq)
		}
	}
	for i, seq := range ln.delayed {
		if s.recs[seq].origin == origin {
			ln.delayed = append(ln.delayed[:i], ln.delayed[i+1:]...)
			return m.displace(s, ln, seq)
		}
	}
	for i, ent := range ln.retry {
		if s.recs[ent.seq].origin == origin {
			ln.retry = append(ln.retry[:i], ln.retry[i+1:]...)
			return m.displace(s, ln, ent.seq)
		}
	}
	return false
}

// displace drops a record an overflow policy took out of its lane to make
// room for a newer one, and reports that it did.
func (m *Model) displace(s *sub, ln *lane, seq uint64) bool {
	m.dropRecord(s, ln, s.recs[seq])
	s.st.displaced++
	return true
}

// fits reports whether a record of size bytes could ever be held.
func (m *Model) fits(size int64) bool {
	return m.cfg.CapacityBytes <= 0 || size <= m.cfg.CapacityBytes
}

// dropOldest discards the record a lane has held longest so that a newer one
// can take its place (engine makeRoom over partitionStore.DropOldest).
//
// The order mirrors the engine's exactly: the store is asked ring by ring
// from the least urgent level up, and each ring gives up what a consumer
// would have taken from it next before what is still waiting for a due time;
// the lane's redeliveries are the last thing given up, because a record that
// has already been delivered once is worth more than one that never has.
func (m *Model) dropOldest(s *sub, ln *lane) bool {
	for p := range m.cfg.Levels {
		if seq, ok := m.oldestReady(ln, uint8(p)); ok {
			m.dropRecord(s, ln, s.recs[seq])
			s.st.displaced++
			return true
		}
		if seq, ok := m.oldestDelayed(s, ln, uint8(p)); ok {
			m.dropRecord(s, ln, s.recs[seq])
			s.st.displaced++
			return true
		}
	}
	if len(ln.retry) > 0 {
		seq := ln.retry[0].seq
		ln.retry = ln.retry[1:]
		m.dropRecord(s, ln, s.recs[seq])
		s.st.displaced++
		return true
	}
	return false
}

// oldestReady removes and returns the record a consumer would have taken next
// from the given priority ring.
func (m *Model) oldestReady(ln *lane, prio uint8) (uint64, bool) {
	i := ln.ready.firstAt(prio)
	if i < 0 {
		return 0, false
	}
	return ln.ready.removeAt(i), true
}

// oldestDelayed removes and returns the delayed record of the given ring with
// the earliest due time; a tie goes to the record published first, which is
// what the engine's oldestDelayed does.
func (m *Model) oldestDelayed(s *sub, ln *lane, prio uint8) (uint64, bool) {
	best := -1
	for j, seq := range ln.delayed {
		r := s.recs[seq]
		if r.prio != prio {
			continue
		}
		if best < 0 {
			best = j
			continue
		}
		cur := s.recs[ln.delayed[best]]
		switch {
		case r.dueAt < cur.dueAt:
			best = j
		case r.dueAt == cur.dueAt && r.seq < cur.seq:
			best = j
		}
	}
	if best < 0 {
		return 0, false
	}
	seq := ln.delayed[best]
	ln.delayed = append(ln.delayed[:best], ln.delayed[best+1:]...)
	return seq, true
}

// overflow is what a publication no subscription had room for does.
func (m *Model) overflow(live []*sub, part int, op Op, size int64) Result {
	if m.cfg.Overflow == OverflowDropNewest {
		for _, s := range live {
			s.st.overflowed++
		}
		if m.retains() {
			// The engine accepted the publication and discarded it; a
			// stream retains what it accepted (engine Topic.retainOnly).
			m.retainOnly(part, op, size)
			return Result{Dropped: true, Retained: true}
		}
		return Result{Dropped: true}
	}
	m.rejectAll(live)
	// The engine refused the publication, so the caller still owns the
	// payload and its dedup key goes back into the window for the retry the
	// error asks for (engine Topic.dedupSettle).
	m.dedupForget(op.DedupKey)
	return Result{Err: errcode.ErrQueueFull}
}

// partitionFor draws the partition of a publication, advancing the
// round-robin counter when the publication names none (engine
// Topic.partition).
func (m *Model) partitionFor(op Op) int {
	if op.Partition < 0 {
		part := m.rr % m.cfg.Partitions
		m.rr++
		return part
	}
	return op.Partition % m.cfg.Partitions
}

func (m *Model) unreserveLane(ln *lane, size int64) {
	ln.depth--
	ln.bytes -= size
}

// consumerKey identifies a consumer of a subscription in the rotation table.
func consumerKey(si int, id uint32) uint64 { return uint64(si)<<32 | uint64(id) }

func (m *Model) fetch(op Op) Result {
	si, s := m.find(op.Sub)
	if m.closed || s == nil || s.closed || !s.consumers[op.Consumer] {
		return Result{Err: errcode.ErrClosed}
	}
	key := consumerKey(si, op.Consumer)
	start := m.fetchRR[key]
	m.fetchRR[key] = start + 1
	maxN := op.Max
	claims := make([]Claim, 0, maxN)
	for i := 0; i < len(s.lanes) && len(claims) < maxN; i++ {
		li := (start + i) % len(s.lanes)
		if !m.assigned(s, li, op.Consumer) {
			// The lane belongs to another consumer in this rebalance
			// generation (the memory engine).
			continue
		}
		ln := &s.lanes[li]
		m.promote(s, ln)
		// The lane's redeliveries are served before its ready queue, and
		// while they seal the lane they are served instead of it.
		var open bool
		var err error
		claims, open, err = m.claimRetry(s, ln, li, op, claims)
		if err != nil {
			return Result{Err: err, Claims: claims}
		}
		if !open {
			continue
		}
		for len(claims) < maxN && ln.ready.len() > 0 {
			seq, err := m.take(s, ln, op.Observed, len(claims))
			if err != nil {
				return Result{Err: err, Claims: claims}
			}
			claims = append(claims, m.lease(s, ln, li, seq, op.Consumer))
		}
	}
	if op.Observed != nil && len(op.Observed) != len(claims) {
		return Result{Err: errMismatch, Claims: claims}
	}
	return Result{N: len(claims), Claims: claims}
}

// take removes the next ready record of the lane. When the harness supplies
// the sequence the engine returned, it must be takeable: the head of the most
// urgent ring for an ordered ready queue, any record of that ring otherwise.
//
// Priorities narrow the candidate set before ordering does: a lane serves its
// most urgent ring until it is empty, so a record of a lower ring is not
// takeable while a higher one holds anything at all.
func (m *Model) take(s *sub, ln *lane, observed []uint64, i int) (uint64, error) {
	top := ln.ready.topPrio()
	if observed == nil {
		return ln.ready.removeAt(ln.ready.firstAt(top)), nil
	}
	if i >= len(observed) {
		return 0, errMismatch
	}
	want := observed[i]
	if !m.cfg.Ordered {
		for j, seq := range ln.ready.seqs {
			if ln.ready.prios[j] == top && s.recs[seq].origin == want {
				return ln.ready.removeAt(j), nil
			}
		}
		return 0, errMismatch
	}
	head := int32(-1)
	for j, seq := range ln.ready.seqs {
		if ln.ready.prios[j] != top {
			continue
		}
		if head < 0 {
			head = ln.ready.groups[j]
		}
		if ln.ready.groups[j] != head {
			break
		}
		if s.recs[seq].origin == want {
			return ln.ready.removeAt(j), nil
		}
	}
	return 0, errMismatch
}

// promote moves every delayed record of the lane that is due into the ready
// queue, in due order, which is what the engine's scheduler structure fires.
func (m *Model) promote(s *sub, ln *lane) {
	// A redelivery waiting for its due time stays in the retry queue and
	// simply becomes deliverable, which is what the engine's lane does: the
	// record never moves, so only its reported state changes.
	for _, ent := range ln.retry {
		if ent.dueAt <= m.now {
			s.recs[ent.seq].state = Ready
		}
	}
	if len(ln.delayed) == 0 {
		return
	}
	due := make([]uint64, 0, len(ln.delayed))
	kept := ln.delayed[:0]
	for _, seq := range ln.delayed {
		if s.recs[seq].dueAt <= m.now {
			due = append(due, seq)
			continue
		}
		kept = append(kept, seq)
	}
	ln.delayed = kept
	sortByDue(s, due)
	// Records that come due at the same instant form one group: a scheduler
	// promotes them together and their order among themselves is not
	// specified, while records due at different instants keep theirs.
	var group int32
	last := time.Duration(-1)
	for _, seq := range due {
		if d := s.recs[seq].dueAt; d != last {
			group, last = ln.ready.newGroup(), d
		}
		s.recs[seq].state = Ready
		ln.ready.pushGroup(seq, group, s.recs[seq].prio)
	}
}

// pushLive inserts seq into the lane's in-flight list in deadline order,
// scanning back from the tail like the engine's intrusive list.
func pushLive(ln *lane, s *sub, seq uint64) {
	d := s.recs[seq].deadline
	i := len(ln.live)
	for i > 0 && s.recs[ln.live[i-1]].deadline > d {
		i--
	}
	ln.live = append(ln.live, 0)
	copy(ln.live[i+1:], ln.live[i:])
	ln.live[i] = seq
}

// removeLive unlinks seq from the lane's in-flight list.
func removeLive(ln *lane, seq uint64) {
	for i, v := range ln.live {
		if v == seq {
			ln.live = append(ln.live[:i], ln.live[i+1:]...)
			return
		}
	}
}

// maxDuration is the deadline of a lease that never expires.
const maxDuration = time.Duration(1<<63 - 1)

// errMismatch reports that the engine's observed behaviour is not one the
// model allows. It is never an error the engine may return.
var errMismatch = errcode.New(errcode.Conflict, "memq: observation contradicts the model")

// ErrMismatch is returned by Step when the observation supplied with an
// operation is not one the model allows.
var ErrMismatch error = errMismatch
