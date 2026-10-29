package engine

import (
	"sync"
	"time"

	"github.com/iqhive/iqmq/internal/lifecycle"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Retention bounds the retained log of a stream topic (the memory engine,
// The specification). A stream keeps every publication in a per-partition log until one of
// the three bounds is reached, and the log is what a cursor replays from.
//
// The three bounds are independent and all of them are enforced: the log is
// trimmed from its tail until it holds at most MaxMessages records, at most
// MaxBytes accounted bytes and no record older than MaxAge.
type Retention struct {
	// MaxMessages is the number of records one partition retains. A
	// non-positive value means the bound is not applied.
	MaxMessages int
	// MaxBytes is the accounted payload size one partition retains. A
	// non-positive value means the bound is not applied.
	MaxBytes int64
	// MaxAge is how long a record is retained after its publication. A
	// non-positive value means the bound is not applied.
	MaxAge time.Duration
}

// Enabled reports whether any bound is set.
func (r Retention) Enabled() bool {
	return r.MaxMessages > 0 || r.MaxBytes > 0 || r.MaxAge > 0
}

// StartKind selects where a stream subscription starts reading
// (the memory engine "replayable cursors").
type StartKind uint8

// The start positions.
const (
	// StartLatest reads only what is published after the subscription
	// exists. It is the zero value and the behaviour of every non-stream
	// topology.
	StartLatest StartKind = iota
	// StartEarliest replays every record the topic still retains.
	StartEarliest
	// StartAtSeq replays from a publication sequence, inclusive.
	StartAtSeq
	// StartAtTime replays from a publication time, inclusive.
	StartAtTime

	startKindCount
)

var startKindNames = [startKindCount]string{"latest", "earliest", "atseq", "attime"}

// String returns the lower-case position name.
func (k StartKind) String() string {
	if k < startKindCount {
		return startKindNames[k]
	}
	return "start(?)"
}

// Valid reports whether k names a start position.
func (k StartKind) Valid() bool { return k < startKindCount }

// StartPositions returns the start positions in benchmark order.
func StartPositions() []StartKind {
	return []StartKind{StartLatest, StartEarliest, StartAtSeq, StartAtTime}
}

// StartPosition is where a stream subscription begins reading.
type StartPosition struct {
	// Kind selects how the position is expressed.
	Kind StartKind
	// Seq is the first publication sequence to replay under StartAtSeq.
	Seq uint64
	// At is the first publication time to replay under StartAtTime.
	At time.Time
}

// retainedEntry is one record of a partition's retained log. It holds a
// reference of its own on the record, so an owned payload is handed back only
// once retention has let go of it.
type retainedEntry[P any] struct {
	rec *record[P]
	// subj is the subject the record was published on, copied here so that a
	// replay can apply a subscription's filter without reading a field of a
	// record the log may already have let go of.
	subj string
	seq  uint64
	at   int64
	size int64
	// dueAt is the instant the publisher asked for the record to be
	// delivered (Delay, DeliverAt), or 0 when it was due at once. A replay
	// admits the record held back until then, as the publication did: a
	// retained record is the publication, not a copy that is due at once.
	dueAt int64
	// gen is the generation of the subscription set the publication routed
	// on. A subscription replays the entries stamped below its own
	// generation and no others: the rest were published to a set that
	// already held it, so it was offered them live (joinBoundary).
	gen uint64
}

// retainedLog is the bounded, replayable log of one partition of a stream
// topic. Records are appended at the head and trimmed from the tail, which is
// what makes a cursor a position in a sequence rather than a pointer into a
// structure that moves.
type retainedLog[P any] struct {
	mu    sync.Mutex
	ents  []retainedEntry[P]
	bytes int64
}

// subSet is a topic's subscription set together with the generation that
// names it. The generation is what a publication is stamped with and what a
// joining subscription is bounded by: every set stored from a subscription's
// own generation onwards holds it, so an entry stamped below that generation
// is one its publication could not have routed to it.
type subSet[P any] struct {
	gen  uint64
	subs []*Subscription[P]
}

// joinBoundary is the boundary between a publication in flight and a
// subscription joining the topic under it (adr/ADR-017-assumptions.md,
// ).
//
// It is one reader/writer lock. A publication of a stream holds it for
// reading from before it reads the subscription set until after it has
// appended to the retained log; a subscription joining the topic takes it for
// writing around the store of the set that holds it. Reader and writer never
// overlap, so every publication falls wholly on one side of every join:
//
//   - it read a set that already held the joining subscription, in which case
//     it delivers to it live and stamps its entry at or above that
//     subscription's generation, which the replay skips; or
//   - it had appended before the set was stored, and so before the replay
//     walks the log, which finds the entry there — stamped below the
//     subscription's generation, so the replay takes it.
//
// Exactly once in both directions, and the argument is the lock's rather than
// this package's. The two-counter gate it replaces was not: `enter` read the
// half and counted itself in as two separate steps, so a publication that
// slipped between them was counted in the half the *previous* join had just
// moved away from, and the *next* join waited on the other half and never saw
// it. That is a record lost to the joining subscription — it reproduces on one
// publisher and two joins per topic in well under a second
// (TestStreamJoinAdmitsEveryPublicationExactlyOnce) — and it is the second
// boundary of this increment to fail; a protocol whose correctness is a
// paragraph of interleavings is not the right shape here.
//
// The cost lands on the join path, which is where a stream can afford it: a
// joining subscription waits for the publications already in flight, and Go's
// RWMutex does not let a reader in ahead of a waiting writer, so publications
// starting under a join wait for it. Both waits are bounded by a reservation
// and an append — neither spans a wait for room, which `publication.resume`
// exists to give the hold up for, and neither spans any code of the caller's,
// which is what publication carries its copy and its drops for. Only a
// topology that retains takes it at all: a queue's joining subscription has
// nothing to replay, which is what `Topic.Subscribe` already documents.
type joinBoundary = sync.RWMutex

// publication is one publication's hold on that boundary, together with the
// generation of the subscription set it routed on. A topic that retains
// nothing holds nothing: held is false and every method is a no-op.
//
// Nothing the caller wrote runs while the hold is held. That is the whole
// point of the boundary — a joining subscription waits for it, so a callback
// inside it is a callback a joining subscription waits for, and a callback
// that joins the topic itself would then wait for its own publication
// (the message contract). The publication therefore carries the callbacks its
// own work would otherwise run inside the hold: logCopy is the copy the
// retained log keeps, cloned by Topic.publish before the hold is taken, and
// gone are the records whose last reference its work dropped — the retention
// trim's, the engine-close sweep's and a displaced reservation's — released
// by end once the hold is given up.
type publication[P any] struct {
	top  *Topic[P]
	held bool
	gen  uint64

	// logCopy is the payload the retained log keeps under a copying mode.
	// Config.Clone is the caller's code, so it is cloned by Topic.publish
	// before the hold is taken; usedCopy records that a record took it, so
	// that a publication the engine refused scrubs the copy nobody read.
	logCopy  P
	mode     CopyMode
	hasCopy  bool
	usedCopy bool

	// gone are the records the publication's own work took the last
	// reference of while it held the boundary: the retention trim's, the
	// engine-close sweep's and the ones a reservation displaced. Releasing
	// one runs the publisher's Release callback.
	gone []*record[P]
}

// begin starts a publication of t under copy mode mode. cp is the copy the
// retained log will keep and copied says whether one was made: Topic.publish
// clones it before this is called, because Config.Clone is the caller's code
// and no code of the caller's runs inside the boundary.
func (t *Topic[P]) begin(mode CopyMode, cp P, copied bool) publication[P] {
	pb := publication[P]{top: t, logCopy: cp, mode: mode, hasCopy: copied}
	if t.retains() {
		t.join.RLock()
		pb.held = true
	}
	return pb
}

// takeCopy hands the retained log the copy Topic.publish made for it.
func (pb *publication[P]) takeCopy() P {
	pb.usedCopy = true
	return pb.logCopy
}

// dropLater queues one record whose last reference the publication's work
// dropped, so that the payload it releases goes back to the caller once the
// boundary is given up rather than inside it.
func (pb *publication[P]) dropLater(rec *record[P]) {
	pb.gone = append(pb.gone, rec)
}

// dropAllLater queues a whole batch. It takes the caller's slice rather than
// copying it when it is the first: a trim already builds one, and the publish
// path of a stream that is at its retention bound trims on every publication
// (the memory engine).
func (pb *publication[P]) dropAllLater(recs []*record[P]) {
	switch {
	case len(recs) == 0:
	case pb.gone == nil:
		pb.gone = recs
	default:
		pb.gone = append(pb.gone, recs...)
	}
}

// targets takes the publication's snapshot of the subscription set, records
// the generation that names it and narrows it to the subject's subscribers.
func (pb *publication[P]) targets(subj string) []*Subscription[P] {
	set := pb.top.subs.Load()
	pb.gen = set.gen
	return pb.top.targets(set.subs, subj)
}

// end gives up the hold, and nothing else.
//
// The hold is given up as soon as the publication has appended to the
// retained log — before the lanes it admits to, before the caller's payload
// goes back and before any subscription that closed under it is re-drained.
// Together with the two things this type carries — the log's copy, cloned
// before the hold was taken, and the queued drops dropQueued releases — that
// is what makes the claim on the type true: no code of the caller's runs
// while the hold is held, so a joining subscription never waits for a
// callback and a callback can never wait for the publication that is running
// it.
//
// Calling it twice is a no-op, which is what lets the publish path both end
// the hold early and defer it.
func (pb *publication[P]) end() {
	if pb.held {
		pb.top.join.RUnlock()
		pb.held = false
	}
}

// dropQueued gives back the records the publication displaced or trimmed
// while it held the hold. Each drop may release a payload, which is the
// caller's own callback, so it runs where neither the hold nor a lifecycle
// ticket is held: the boundary is what a joining subscription waits for and
// the ticket is what Engine.Close waits for, and a callback under either is a
// callback one of them waits for (the message contract and,
// adr/ADR-017-assumptions.md and). Topic.after is where a
// ticketed publication reaches it; the publish path's own deferred finish
// catches the routes that took no ticket.
//
// Calling it twice is a no-op: the second call finds the queue empty.
func (pb *publication[P]) dropQueued() {
	if len(pb.gone) == 0 {
		return
	}
	gone := pb.gone
	pb.gone = pb.gone[:0]
	for _, rec := range gone {
		pb.top.eng.dropRecordRef(rec, true)
	}
}

// finish ends the publication for good: it gives up the hold, releases the
// queued drops and scrubs the copy the retained log never took. It is what
// Topic.publish defers; every other caller uses end, which the publish path
// may follow with resume.
func (pb *publication[P]) finish() {
	pb.end()
	pb.dropQueued()
	if pb.hasCopy && !pb.usedCopy {
		// The publication was refused before the log took the copy made for
		// it. Nothing will ever read that storage, and it is the engine's:
		// it gets the scrub a released engine-owned payload gets, and no
		// Release, because a copy is not the caller's payload.
		pb.top.eng.scrub(pb.logCopy, pb.mode)
		pb.hasCopy = false
	}
}

// resume takes the hold again after the publication has parked for room. The
// snapshot it took is stale by then and its caller re-reads it.
func (pb *publication[P]) resume() {
	if !pb.held && pb.top.retains() {
		pb.top.join.RLock()
		pb.held = true
	}
}

// storeJoined publishes the subscription set that holds a joining
// subscription, inside the topic's join boundary.
//
// It is called with the topic mutex held, which is what serialises two
// subscriptions joining at once; the boundary is what serialises the join
// against the publications in flight (joinBoundary). Taking the boundary
// under the topic mutex is the one order there is: no publish path takes the
// topic mutex, and the boundary spans no callback of the caller's, so the
// wait it adds to `Subscription.Close` — which also takes the topic mutex —
// is bounded by a reservation and an append (the message contract).
func (t *Topic[P]) storeJoined(next []*Subscription[P], s *Subscription[P]) uint64 {
	if !t.retains() {
		return t.storeSubs(next, s)
	}
	t.join.Lock()
	defer t.join.Unlock()
	return t.storeSubs(next, s)
}

// retains reports whether the topic keeps a retained log.
func (t *Topic[P]) retains() bool { return t.retained != nil }

// retain draws the publication's sequence, appends rec to its partition's
// retained log and trims the log back within its bounds. It returns the
// sequence it drew, and stamps the entry with the generation of the
// subscription set the publication routed on (publication.gen) and with
// dueAt, the publication's due time, for a replay to hold the record back
// until. The reference the entry holds was taken by the publication
// (Topic.commit).
//
// The records the trim gives back are handed to pb rather than released here:
// this runs inside the join boundary, and a release is the caller's callback.
//
// The sequence is drawn here, under the log's mutex and in the same step as
// the append, because three readers of the log read it as a sequence:
// retainedLog.replayFrom stops at the first entry past its bound,
// trimLocked's age loop stops at the first entry inside the window, and
// Topic.Oldest reads the head. A sequence drawn earlier — before the
// records are built and the lanes reserved, which is where the lock-free
// publish path does its work — lets two concurrent publications append 6
// before 5, and all three answers are then wrong for a log that is not the
// sequence it claims to be.
func (t *Topic[P]) retain(pb *publication[P], rec *record[P], dueAt, now int64) uint64 {
	lg := t.retained[rec.part]
	lg.mu.Lock()
	rec.seq = t.seq.Add(1)
	seq := rec.seq
	at := now
	if n := len(lg.ents); n > 0 && at < lg.ents[n-1].at {
		// The clock is read outside the mutex too, so the same interleaving
		// can carry one publication's reading past an older one's. The log's
		// times are kept non-decreasing, which is what makes the age bound a
		// property of a prefix — trimLocked stops at the first entry inside
		// the window.
		//
		// This clamped time is the entry's time for every reader, replay
		// included, so `StartAtTime` is a position in the log rather than a
		// predicate over publication instants: a record whose real instant
		// precedes the requested one is replayed when the publication that
		// overtook it gave it a later time here, and it is trimmed with that
		// publication rather than a moment before. Keeping a raw time beside
		// the clamped one and replaying from the raw one was rejected — the
		// replay would then start at an entry the age trim may already have
		// passed over, so a position would name different records depending
		// on which order a reader used (TestStartAtTimeIsAPositionInTheLog).
		at = lg.ents[n-1].at
	}
	lg.ents = append(lg.ents, retainedEntry[P]{
		rec: rec, subj: rec.subj, seq: seq, at: at, size: rec.size, dueAt: dueAt, gen: pb.gen,
	})
	lg.bytes += rec.size
	gone := lg.trimLocked(t.cfg.Retention, now, nil)
	lg.mu.Unlock()
	// The trim's records are given back by publication.dropQueued, once the
	// join boundary this append runs inside has been given up and the publish
	// ticket that brackets it is home: a drop runs the publisher's Release
	// callback, which is neither what a joining subscription may wait for nor
	// what Engine.Close may wait for (the message contract and,
	// adr/ADR-017-assumptions.md and).
	//
	// The engine's own close is not looked at again here. Every route into
	// this append holds a publish ticket (Topic.commit, Topic.retainOnlyGated)
	// and the settling drain runs only once the barrier that bars that gate
	// has collected every one of them, so the drain walks this log after this
	// entry is in it. The recheck this replaces belonged to the tree that had
	// no such barrier.
	pb.dropAllLater(gone)
	return seq
}

// retainOnlyGated is retainOnly under a publish ticket, which is how every
// caller inside a publication reaches it: the append to the retained log is a
// placement like any other, so the settling phase must not walk the logs
// while one is in progress. There is no ungated route to retainOnly —
// Topic.publish's no-subscription arm was the last one, and it took the
// record to a log the settling phase had already emptied, where nothing but
// Topic.retain's post-append recheck of the engine close could catch it —
// the recheck this gate replaced.
func (t *Topic[P]) retainOnlyGated(pb *publication[P], part int, p *Publish[P], size int64) error {
	tk := t.eng.lc.Enter(lifecycle.OpPublish)
	if !tk.OK() {
		t.eng.lc.Leave(tk)
		// The engine stopped accepting between this publication's own look
		// at the phase and the append. The caller keeps its payload, which
		// is what every other refusal means, rather than being told the
		// engine took a record it is about to drop.
		return errcode.ErrClosed
	}
	give := t.retainOnly(pb, part, p, size)
	t.eng.lc.Leave(tk)
	// Nothing above ran a callback of the caller's; everything that does runs
	// here, with the ticket back (Topic.after).
	t.after(pb, give, p, nil)
	return nil
}

// retainOnly puts a publication that reached no subscription into the topic's
// retained log, and reports whether the caller's own payload is to be handed
// back.
//
// A stream's retained log is a property of the topic, not of its subscription
// set: the memory engine makes a stream "a bounded retained log
// (count/bytes/age); replayable cursors", so a publication made before anyone
// subscribed, one whose subject no filter matched, and one every lane was too
// full to take under OverflowDropNewest are all still retained and still
// replayable. The rule is that a stream retains every publication the engine
// *accepted*: a refusal (errcode.ErrQueueFull) hands the payload back for the
// caller to retry, and retaining it would put a record in the log that the
// caller was told the engine had not taken
// (adr/ADR-017-assumptions.md).
//
// It releases nothing itself: the entries a retention trim displaced are
// queued on the publication and the caller's own payload is reported back,
// both for the ticketed caller to do once it has left the gate (Topic.after,
// publication.dropQueued).
func (t *Topic[P]) retainOnly(pb *publication[P], part int, p *Publish[P], size int64) bool {
	if !t.retains() {
		return true
	}
	now := t.eng.now()
	mode := t.mode(p)
	// The sequence is drawn by the append (Topic.retain), which is the only
	// thing this publication does with the record.
	rec := t.newRecord(p, 0, part, size, ttlAt(p, now), mode)
	if t.copiesFor(mode) {
		// The log keeps a copy of its own and the caller's payload goes
		// straight back, exactly as it does when a subscription took it.
		// The copy was cloned before the boundary was taken (Topic.publish).
		rec.v = pb.takeCopy()
		rec.retain(1)
		_ = t.retain(pb, rec, due(p, now), now)
		pb.end()
		return true
	}
	// An owned or borrowed payload is now the log's: the engine hands it
	// back once retention has trimmed the entry.
	rec.v = p.Payload
	rec.release = p.Release
	rec.retain(1)
	_ = t.retain(pb, rec, due(p, now), now)
	pb.end()
	return false
}

// trimLocked removes the entries the retention bounds no longer cover and
// appends them to gone. It trims a prefix and stops at the first entry the
// bounds still cover, which is sound because the log is appended in sequence
// order and with non-decreasing times (Topic.retain). The caller holds the log mutex and drops them once it
// has released it, because a drop runs the publisher's release callback,
// which never runs under an engine lock (the message contract).
func (lg *retainedLog[P]) trimLocked(r Retention, now int64, gone []*record[P]) []*record[P] {
	n := 0
	for n < len(lg.ents) {
		e := lg.ents[n]
		over := (r.MaxMessages > 0 && len(lg.ents)-n > r.MaxMessages) ||
			(r.MaxBytes > 0 && lg.bytes > r.MaxBytes) ||
			(r.MaxAge > 0 && later(e.at, r.MaxAge) <= now)
		if !over {
			break
		}
		lg.bytes -= e.size
		gone = append(gone, e.rec)
		n++
	}
	if n == 0 {
		return gone
	}
	rest := copy(lg.ents, lg.ents[n:])
	// The vacated tail is cleared: a *record left past len keeps a record
	// the log has already given back reachable, and with a recycling pool
	// that record now belongs to another publication.
	clear(lg.ents[rest:])
	lg.ents = lg.ents[:rest]
	return gone
}

// dropRetained gives back the references trimming released.
func (t *Topic[P]) dropRetained(gone []*record[P]) {
	for _, rec := range gone {
		t.eng.dropRecordRef(rec, true)
	}
}

// dropAllRetained empties every retained log and releases the references it
// held, which is what the engine's close does with them.
func (t *Topic[P]) dropAllRetained() {
	t.dropRetained(t.takeAllRetained())
}

// takeAllRetained empties every retained log and returns the references it
// held, without releasing them, so that a caller which may not run a release
// where it stands can drop them where it may (Topic.dropAllRetained,
// publication.dropQueued).
func (t *Topic[P]) takeAllRetained() []*record[P] {
	if !t.retains() {
		return nil
	}
	var gone []*record[P]
	for _, lg := range t.retained {
		lg.mu.Lock()
		for _, e := range lg.ents {
			gone = append(gone, e.rec)
		}
		clear(lg.ents)
		lg.ents = lg.ents[:0]
		lg.bytes = 0
		lg.mu.Unlock()
	}
	return gone
}

// trimRetention applies the age bound to every partition. Retention by count
// and by size is applied by the append that breaches them; age has no such
// event, so it is applied whenever the log is read or written — a
// publication, a replay, an Inspect or the scheduler's own sweep — which is
// what keeps "retained" the same answer for every observer without a timer
// per record (adr/ADR-017-assumptions.md).
func (t *Topic[P]) trimRetention(now int64) {
	if !t.retains() || t.cfg.Retention.MaxAge <= 0 {
		return
	}
	var gone []*record[P]
	for _, lg := range t.retained {
		lg.mu.Lock()
		gone = lg.trimLocked(t.cfg.Retention, now, gone)
		lg.mu.Unlock()
	}
	t.dropRetained(gone)
}

// Retained returns the number of records and the accounted bytes the topic
// retains for replay, after applying the age bound.
func (t *Topic[P]) Retained() (count int, bytes int64) {
	if !t.retains() {
		return 0, 0
	}
	t.trimRetention(t.eng.now())
	return t.retainedLen()
}

// retainedLen returns the number of records and the accounted bytes the
// retained logs hold, without applying the age bound first.
func (t *Topic[P]) retainedLen() (count int, bytes int64) {
	if !t.retains() {
		return 0, 0
	}
	for _, lg := range t.retained {
		lg.mu.Lock()
		count += len(lg.ents)
		bytes += lg.bytes
		lg.mu.Unlock()
	}
	return count, bytes
}

// Oldest returns the earliest publication sequence the topic still retains,
// and false when it retains nothing. Each log is ordered by sequence
// (Topic.retain), so the head of a partition is its earliest.
func (t *Topic[P]) Oldest() (uint64, bool) {
	if !t.retains() {
		return 0, false
	}
	t.trimRetention(t.eng.now())
	seq, ok := uint64(0), false
	for _, lg := range t.retained {
		lg.mu.Lock()
		if len(lg.ents) > 0 && (!ok || lg.ents[0].seq < seq) {
			seq, ok = lg.ents[0].seq, true
		}
		lg.mu.Unlock()
	}
	return seq, ok
}

// replayFrom copies the retained entries of one partition that start at pos,
// that s takes and that were published before s existed, into dst. The caller
// must not hold the lane mutex.
//
// maxGen is the subscription's own generation, or zero for a Seek, which
// replays everything the log still holds whatever the subscription has
// already seen.
//
// The subscription's subject filter is applied here and not only on the live
// publish path: a replay is an admission like any other, so a subscription
// must never be handed a retained record whose subject its filter excludes
// (adr/ADR-017-assumptions.md).
func (lg *retainedLog[P]) replayFrom(
	pos StartPosition, maxGen uint64, s *Subscription[P], dst []retainedEntry[P],
) []retainedEntry[P] {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	for _, e := range lg.ents {
		if maxGen > 0 && e.gen >= maxGen {
			// The publication that appended this entry routed on a set that
			// already held s, so s was offered it live; replaying it would
			// admit one publication twice. The test is not a break: an
			// older publication may append after a newer one, which is
			// precisely the window the generation exists to close.
			continue
		}
		if !s.deliverable(e.subj) {
			continue
		}
		switch pos.Kind {
		case StartAtSeq:
			if e.seq < pos.Seq {
				continue
			}
		case StartAtTime:
			// e.at is the log's time for the entry, which Topic.retain keeps
			// non-decreasing, so this is a position in the log and not a
			// predicate over publication instants.
			if e.at < pos.At.UnixNano() {
				continue
			}
		case StartLatest:
			continue
		case StartEarliest, startKindCount:
			// Everything the log still holds, which is what a replay from
			// the earliest retained record is; the count is not a position.
		}
		// The reference is taken last, once the entry is one the replay
		// will actually carry: every entry this function returns holds one,
		// and the caller gives back the ones it does not admit. Taking it
		// before the position filter would leak one for every entry the
		// position skipped.
		//
		// It is what makes every later read of the record safe. Without it
		// a trim running between this walk and Topic.replayRecord could
		// reset the record and hand it to the engine's pool under the read,
		// and the copying branch would clone a payload the engine had
		// already released. A record the log still holds always has a
		// reference to give, because trimLocked takes the entry out before
		// the reference it held is dropped.
		if !e.rec.tryAddRef() {
			continue
		}
		dst = append(dst, e)
	}
	return dst
}

// Cursors returns, per partition, the highest publication sequence the
// subscription has admitted — the position a further replay continues from.
// It is zero for a partition that has admitted nothing.
func (s *Subscription[P]) Cursors() []uint64 {
	out := make([]uint64, len(s.lanes))
	for i, ln := range s.lanes {
		out[i] = ln.cursor.Load()
	}
	return out
}

// Seek replays the topic's retained log into the subscription from pos: every
// record the subscription still holds is discarded, and the retained records
// at or after the position are admitted again, in publication order, as far
// as the subscription's capacity allows. It returns the number of records
// admitted.
//
// Seek fails with ErrInvalidConfig on a topic that retains nothing, and with
// ErrClosed once the subscription or the engine is closed. It is a control
// operation: a publication running concurrently with it may be admitted both
// live and by the replay, so a caller that wants an exact replay quiesces the
// topic first (the memory engine "replayable cursors").
func (s *Subscription[P]) Seek(pos StartPosition) (int, error) {
	if !pos.Kind.Valid() {
		return 0, ErrInvalidConfig
	}
	if !s.topic.retains() {
		return 0, ErrInvalidConfig
	}
	if s.closed.Load() || s.eng.closing() {
		return 0, errcode.ErrClosed
	}
	s.drain()
	for _, ln := range s.lanes {
		ln.cursor.Store(0)
	}
	s.topic.trimRetention(s.eng.now())
	n, gone := s.replay(pos, 0, nil)
	// Seek takes no ticket — it is not part of a publication — so it keeps
	// the place-then-re-read protocol of the contributor notes in full.
	s.eng.dropRefs(gone)
	if s.closed.Load() || s.eng.closing() {
		s.drain()
	}
	return n, nil
}

// replay admits the retained records at or after pos, ignoring the ones a
// publication routed on a subscription set that already held s — the ones
// stamped at or above maxGen — when maxGen is not zero. It reports how many
// records it admitted, and returns gone with the references it gave up.
//
// It releases nothing. Topic.Subscribe reaches it under a publish ticket, so
// the references it gives up — the entries it had no room for, and the
// retained record a per-subscription copy replaces — go back to the caller
// in gone, to be dropped once the ticket is home (Topic.after): every one of
// them may be the last reference on a record, and the last reference runs the
// publisher's Release callback.
func (s *Subscription[P]) replay(pos StartPosition, maxGen uint64, gone []*record[P]) (int, []*record[P]) {
	if pos.Kind == StartLatest || !s.topic.retains() {
		return 0, gone
	}
	t := s.topic
	now := t.eng.now()
	n := 0
	var buf []retainedEntry[P]
	for part, lg := range t.retained {
		buf = lg.replayFrom(pos, maxGen, s, buf[:0])
		ln := s.lanes[part]
		i := 0
		for ; i < len(buf); i++ {
			ent := buf[i]
			if !t.eng.reserveTotal(1, ent.size) {
				break
			}
			if !ln.reserve(ent.size) {
				t.eng.releaseTotal(1, ent.size)
				break
			}
			var rec *record[P]
			rec, gone = t.replayRecord(ent, gone)
			ln.admit(rec, ent.dueAt, now)
			s.st.published.Add(1)
			n++
		}
		// The entries the reservations had no room for still hold the
		// reference replayFrom took for them.
		for ; i < len(buf); i++ {
			gone = append(gone, buf[i].rec)
		}
		clear(buf)
	}
	if n > 0 {
		s.waiters.NotifyAll()
	}
	return n, gone
}

// replayRecord returns the record a replay admits for one retained entry and
// consumes the reference retainedLog.replayFrom took on it: the retained
// record itself when the topic's subscriptions share one record, and a fresh
// copy — after which the entry's reference is given back — when the topic
// copies per subscription.
//
// Every field it reads belongs to a record it holds a reference on, so a
// concurrent retention trim cannot reset it and hand it to the engine's
// record pool underneath the read (resetRecord, engine.dropRecordRef), and
// the copying branch cannot clone a payload the engine has already released.
// It returns gone with the entry's reference added when it gave one back.
func (t *Topic[P]) replayRecord(ent retainedEntry[P], gone []*record[P]) (*record[P], []*record[P]) {
	if !t.copiesFor(ent.rec.mode) {
		return ent.rec, gone
	}
	rec := t.eng.records.Get()
	rec.v = t.eng.cfg.Clone(ent.rec.v)
	rec.key, rec.seq, rec.size, rec.part = ent.rec.key, ent.seq, ent.size, ent.rec.part
	rec.subj = ent.subj
	rec.expireAt = ent.rec.expireAt
	rec.mode = ent.rec.mode
	rec.prio = ent.rec.prio
	rec.retain(1)
	return rec, append(gone, ent.rec)
}
