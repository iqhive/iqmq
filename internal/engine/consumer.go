package engine

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/lifecycle"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/settlement"
)

// ConsumerConfig configures one attached consumer.
type ConsumerConfig struct {
	// Prefetch bounds the number of claims a single Fetch takes, whatever
	// the destination slice can hold. Zero means the destination length.
	Prefetch int
}

// Consumer is one attached client of a subscription. Consumers of the same
// subscription compete for its records.
//
// Fetch must be called from one goroutine at a time; Ack, Nack, Extend and
// Close are safe from any goroutine.
type Consumer[P any] struct {
	sub      *Subscription[P]
	id       uint32
	prefetch int
	// scratch is the buffer a claim moves envelopes through, kept on the
	// consumer so that the claim path allocates nothing whatever the
	// architecture. A fetch takes it and puts it back, so a second fetch
	// running at the same time — which the contract does not promise to
	// serve, but which must not corrupt anything — simply uses one of its
	// own.
	scratch atomic.Pointer[envBuf[P]]
	rr      atomic.Uint32
	closed  atomic.Bool
}

// envBuf is the claim scratch buffer of one consumer.
type envBuf[P any] struct{ s []*Envelope[P] }

// ID returns the consumer's identity within its subscription.
func (c *Consumer[P]) ID() uint32 { return c.id }

// Subscription returns the subscription the consumer is attached to.
func (c *Consumer[P]) Subscription() *Subscription[P] { return c.sub }

// Attach creates a consumer of the subscription.
func (s *Subscription[P]) Attach(cfg ConsumerConfig) (*Consumer[P], error) {
	if s.eng.closing() || s.closed.Load() {
		return nil, errcode.ErrClosed
	}
	c := &Consumer[P]{sub: s, id: s.nextConsumer.Add(1), prefetch: cfg.Prefetch}
	s.active.Add(1)
	// A join rebalances the sticky assignment, which is what gives the new
	// consumer lanes of its own and takes them from whoever had them
	// (the memory engine).
	s.join(c)
	return c, nil
}

// Info describes a claimed record. It carries what a consumer needs without
// making the engine inspect the payload.
type Info struct {
	// Key is the publisher's routing key; the engine never copies it.
	Key []byte
	// Subject is the subject the record was published on — the resolved
	// name a wildcard subscription needs to report (the API).
	// It is empty on a topic without wildcards, which routes to every
	// subscription and has no subject to resolve.
	Subject string
	// Seq is the record's publication sequence within its topic.
	Seq uint64
	// Attempts counts deliveries of this record to this subscription,
	// including the current one, so a first delivery reports 1.
	Attempts uint32
	// Partition is the lane the record was claimed from.
	Partition int32
	// Size is the accounted payload size.
	Size int32
}

// Claim is one leased record handed to a consumer by Fetch. Env and Gen
// together identify the delivery for Ack, Nack and Extend; a Gen from an
// earlier delivery of the same record is stale and settles nothing.
type Claim[P any] struct {
	// Payload is the published value.
	Payload P
	// Env identifies the record. It is opaque and must not outlive the
	// delivery.
	Env *Envelope[P]
	// Gen is the delivery generation.
	Gen uint32
	// Info describes the record.
	Info Info
}

// Alive reports whether the delivery is still leased under the generation
// this Claim carries, and therefore whether Payload still aliases live engine
// storage.
//
// It is the use-after-free guard of the zero-copy receive mode
// (the memory engine): under ReceiveZeroCopy the payload is the engine's own
// and is scrubbed with Config.Poison as the record is released, so a consumer
// that reads a settled delivery sees the canary; Alive is how it asks first
// rather than finding out. Under ReceiveCopy it is a diagnostic only: the
// payload is the consumer's own copy and outlives the delivery.
func (c Claim[P]) Alive() bool {
	if c.Env == nil {
		return false
	}
	st, g := c.Env.cell.Load()
	return st == settlement.Active && uint32(g) == c.Gen
}

// Fetch claims up to len(dst) records for the consumer and returns how many
// it wrote. It takes whatever is immediately available and blocks only when
// nothing is: a caller that wants a poll passes a context that is already
// done, which then makes an empty fetch return the context's error rather
// than blocking.
//
// It fails with ErrClosed once the consumer, its subscription or the engine
// is closed, and with the context's error when that ends the wait.
func (c *Consumer[P]) Fetch(ctx context.Context, dst []Claim[P]) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	n := len(dst)
	if c.prefetch > 0 && c.prefetch < n {
		n = c.prefetch
	}
	dst = dst[:n]
	buf := c.scratch.Swap(nil)
	if buf == nil || cap(buf.s) < n {
		buf = &envBuf[P]{s: make([]*Envelope[P], n)}
	}
	defer c.scratch.Store(buf)
	scratch := buf.s[:n]
	for {
		if err := c.state(); err != nil {
			return 0, err
		}
		if n, took, err := c.take(dst, scratch); took {
			return n, err
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		t := c.sub.waiters.Prepare()
		if n, took, err := c.take(dst, scratch); took {
			c.sub.waiters.Cancel(t)
			return n, err
		}
		if err := c.state(); err != nil {
			c.sub.waiters.Cancel(t)
			return 0, err
		}
		if err := c.sub.waiters.Wait(ctx, t); err != nil {
			return 0, err
		}
	}
}

// take claims what the lanes have and hands it over, under a fetch ticket. It
// reports whether it took anything; a fetch that found nothing has placed
// nothing and falls through to the wait, which must happen outside the gate.
//
// The ticket is what makes the shutdown's second phase mean something: it
// bars this gate and waits for it, so no fetch is between its claim and its
// hand-over when the settling drain walks the lanes. A refused ticket is not
// an error here — c.state has already reported ErrClosed, or deliver is about
// to, and deliver puts the records back under the lane mutex either way.
func (c *Consumer[P]) take(dst []Claim[P], scratch []*Envelope[P]) (int, bool, error) {
	// Everything this fetch gives up is collected rather than dropped: a drop
	// runs the publisher's Release callback, and the shutdown's StopFetch
	// barrier waits for this ticket, so a callback inside it is a callback
	// Engine.Close waits for — which the message contract forbids and
	// internal/lifecycle's own contract states ("the section between Enter
	// and Leave must not block"). It is not only the deliveries a raced close
	// could not put back: a claim gives up a record that outlived its time to
	// live and one a rebalance took the lane away under, and a close racing
	// the redelivery it queues asks for a re-drain — all of them run a
	// release (fetchDrops).
	fd := fetchDrops[P]{sub: c.sub}
	tk := c.sub.eng.lc.Enter(lifecycle.OpFetch)
	got := c.drain(dst, scratch, &fd)
	if got == 0 {
		c.sub.eng.lc.Leave(tk)
		fd.run()
		return 0, false, nil
	}
	n, err := c.deliver(dst, got, &fd)
	c.sub.eng.lc.Leave(tk)
	fd.run()
	return n, true, err
}

// fetchDrops is the work a fetch defers past its ticket: the records it gave
// up, and the re-drain a close racing its own placement asks for. Every one
// of them may release a payload, which is the caller's own callback
// (Consumer.take).
//
// The records are already out of their lanes and out of the in-flight list
// when they reach it, so nothing else can reach them while they wait; the
// lane's own capacity is what the drop gives back, a moment later than the
// claim that took it.
type fetchDrops[P any] struct {
	sub *Subscription[P]
	// back are the deliveries a raced close could not put back, and the
	// records a claim gave up: they are dropped without being recycled,
	// because the envelope may still be named by a claim the caller holds.
	back []*Envelope[P]
	// spent are the records the lease itself gave up before any claim named
	// them — an expired one — so the envelope goes back to its pool.
	spent   []*Envelope[P]
	redrain bool
}

// putBack queues a record whose delivery is being given up.
func (fd *fetchDrops[P]) putBack(e *Envelope[P]) { fd.back = append(fd.back, e) }

// drop queues a record the lease gave up before it was ever delivered.
func (fd *fetchDrops[P]) drop(e *Envelope[P]) { fd.spent = append(fd.spent, e) }

// run does the deferred work. The caller must hold no lifecycle ticket.
func (fd *fetchDrops[P]) run() {
	for _, e := range fd.back {
		e.lane.drop(e, false)
	}
	for _, e := range fd.spent {
		e.lane.drop(e, true)
	}
	if fd.redrain {
		fd.sub.drain()
	}
}

// deliver hands over the claims a drain produced unless a close raced it. A
// consumer that closed under its own fetch returns the records to its lane,
// which is the close policy of the memory engine; when the subscription or
// the engine closed, nothing is admitted again and the records are given to
// fd instead, for take to drop once it has left the fetch gate.
func (c *Consumer[P]) deliver(dst []Claim[P], got int, fd *fetchDrops[P]) (int, error) {
	if err := c.state(); err == nil {
		return got, nil
	}
	now := c.sub.eng.now()
	requeued := 0
	for i := range got {
		cl := dst[i]
		e := cl.Env
		// Close claims the record only when the lease is still live and
		// still this generation, so the engine's own close and this one
		// can never both act on the same delivery.
		if !e.cell.Close(settlement.Gen(cl.Gen)) {
			continue
		}
		ln := e.lane
		ln.mu.Lock()
		ln.live.remove(e)
		back := ln.pushReady(e, now, &fd.back)
		ln.mu.Unlock()
		if back {
			requeued++
		}
	}
	if requeued > 0 {
		c.sub.st.requeued.Add(int64(requeued))
		c.sub.waiters.NotifyAll()
		c.sub.unleased()
	}
	return 0, errcode.ErrClosed
}

// state reports the closed state of the consumer, its subscription and the
// engine.
func (c *Consumer[P]) state() error {
	if c.closed.Load() || c.sub.closed.Load() || c.sub.eng.closing() {
		return errcode.ErrClosed
	}
	return nil
}

// drain visits the subscription's lanes from a rotating start index and
// claims what it finds, which keeps consumers from starving a lane.
func (c *Consumer[P]) drain(dst []Claim[P], scratch []*Envelope[P], fd *fetchDrops[P]) int {
	s := c.sub
	now := s.eng.now()
	start := int(c.rr.Add(1) - 1)
	n := 0
	for i := 0; i < len(s.lanes) && n < len(dst); i++ {
		ln := s.lanes[(start+i)%len(s.lanes)]
		if !ln.assigned(c) {
			// The lane belongs to another consumer in this rebalance
			// generation (the memory engine).
			continue
		}
		n += ln.claim(c, now, dst[n:], scratch, fd)
	}
	return n
}

// neverExpires is the deadline of a lease that cannot expire.
const neverExpires = math.MaxInt64

// claim takes deliverable records from the lane and leases them to c. The
// lane's redelivery queue is served first — a record that failed is never
// behind the records published after it — and, while that queue holds the
// lane sealed (BlockLane, or any placement under strict ordering), it is served *instead* of the store: nothing overtakes a
// redelivery that is still outstanding.
func (ln *lane[P]) claim(c *Consumer[P], now int64, dst []Claim[P], scratch []*Envelope[P], fd *fetchDrops[P]) int {
	deadline := int64(neverExpires)
	if d := ln.sub.ackDeadline; d > 0 {
		deadline = now + int64(d)
	}
	n := 0
	got, open := ln.retries.claim(now, scratch[:len(dst)])
	if got > 0 {
		ln.sub.eng.faults.reorderBatch(ln, scratch[:got])
		n = ln.lease(c, scratch[:got], now, deadline, dst, fd)
	}
	if !open {
		if n > 0 {
			ln.rearm()
		}
		return n
	}
	for n < len(dst) {
		buf := scratch[:len(dst)-n]
		got := ln.store.Claim(now, buf)
		if got == 0 {
			break
		}
		ln.sub.eng.faults.reorderBatch(ln, buf[:got])
		n += ln.lease(c, buf[:got], now, deadline, dst[n:], fd)
	}
	if n > 0 {
		ln.rearm()
	}
	return n
}

// lease turns claimed envelopes into deliveries of c, writing one Claim per
// envelope into dst and returning how many it wrote. An envelope whose record
// has outlived its time to live is given up instead of delivered.
//
// It gives records up to fd rather than dropping them, because it runs inside
// the fetch ticket: an expired record's last reference is the publisher's
// payload and dropping it here would run that callback with the ticket held
// (Consumer.take, fetchDrops).
func (ln *lane[P]) lease(c *Consumer[P], es []*Envelope[P], now, deadline int64, dst []Claim[P], fd *fetchDrops[P]) int {
	n := 0
	for _, e := range es {
		if expired(e, now) {
			// A record that outlived its time to live is never delivered;
			// the sweep may not have reached it yet. Nothing has named this
			// envelope, so it is recycled with the drop.
			fd.drop(e)
			continue
		}
		// The lease is taken under the lane mutex, together with the link
		// into the in-flight list and the read of the record: from the
		// moment the generation exists, a settlement presenting it (a forged
		// one included) must take the same mutex to unlink the envelope, so
		// it can never find it unlinked or recycle it while the claim is
		// still being built.
		ln.mu.Lock()
		if !ln.assigned(c) {
			// A rebalance landed between the fetch's lane check and this
			// lease. The lane mutex is the one a revocation takes too, so
			// refusing here and revoking there cover the two orders between
			// them and never both act on the same delivery.
			ln.mu.Unlock()
			// requeueOrDrop, not requeue: the placement runs with the lane
			// mutex released, so a close that lands under it has already
			// drained this lane and the guarded route is the one that looks
			// again (lane.requeueOrDrop).
			ln.requeueOrDropTo(e, now, now, fd)
			continue
		}
		g, ok := e.cell.Lease()
		if !ok {
			// Unreachable: a ready record holds no lease. Put it back rather
			// than lose it, through the route that re-checks the close (a
			// claim runs with the lane mutex released here).
			ln.mu.Unlock()
			ln.requeueOrDropTo(e, now, now, fd)
			continue
		}
		e.attempts++
		e.owner.Store(c.id)
		e.deadline = deadline
		ln.live.push(e)
		rec := e.rec
		v := rec.v
		if ln.sub.eng.cfg.Receive == ReceiveCopy {
			// A copying receive materialises the consumer's own payload
			// while the lease is held, so it can never observe a record
			// another consumer settled after a revocation. The clone runs
			// under the lane mutex for exactly that reason, which is the
			// cost the mode is opt-in for.
			v = ln.sub.eng.cfg.Clone(rec.v)
		}
		cl := Claim[P]{
			Payload: v,
			Env:     e,
			Gen:     uint32(g),
			Info: Info{
				Key:       rec.key,
				Subject:   rec.subj,
				Seq:       e.seq,
				Attempts:  e.attempts,
				Partition: ln.idx,
				Size:      rec.size,
			},
		}
		ln.mu.Unlock()
		ln.sub.st.attempts.Add(1)
		if ln.sub.eng.faults.duplicate() {
			// The delivery is handed out and the record is admitted to the
			// lane again, so a second consumer receives it: the duplicate an
			// at-least-once broker produces when an acknowledgement is lost.
			// It is a second admission, accounted for like any other, which
			// is what keeps the conservation invariant true through it.
			ln.duplicateTo(rec, now, fd)
		}
		dst[n] = cl
		n++
	}
	return n
}

// duplicate admits rec to the lane a second time. It is called with the lane
// mutex released, because a reservation the lane refuses gives the engine-wide
// slot back and a successful one wakes the subscription's waiters.
//
// It is an admission, so it takes the discipline every other admission takes:
// nothing is admitted after a close, and the close is looked at again
// afterwards because the placement runs with the lane mutex released — a
// close that lands between the two has already drained this lane and nothing
// will visit it again (lane.requeueOrDrop, Topic.reap).
//
// Only the second look has a failure of its own: the re-drain runs in the
// same goroutine straight after the placement and empties every lane of the
// subscription, so an admission the first look would have refused is one the
// re-drain takes back again. The first look is what stops the engine doing an
// admission's work after a close, and it is the half that survives if this
// placement ever grows a route the re-drain cannot reach; it is deliberately
// not pinned by a test, because there is no state it can leave behind
// (adr/ADR-017-assumptions.md).
func (ln *lane[P]) duplicateTo(rec *record[P], now int64, fd *fetchDrops[P]) {
	s := ln.sub
	if s.closed.Load() || s.eng.closing() {
		return
	}
	size := int64(rec.size)
	if !s.eng.reserveTotal(1, size) {
		return
	}
	if !ln.reserve(size) {
		s.eng.releaseTotal(1, size)
		return
	}
	if !rec.tryAddRef() {
		ln.unreserve(size)
		s.eng.releaseTotal(1, size)
		return
	}
	ln.store.Requeue(ln.newEnvelope(rec), 0, now)
	s.st.published.Add(1)
	s.waiters.Notify()
	if s.closed.Load() || s.eng.closing() {
		fd.redrain = true
	}
}

// faultsBefore applies the settlement faults that act before the settlement
// begins: a settlement delayed past its lease finds the delivery revoked and
// the record already back in its lane (Faults.DelaySettlement).
func (ln *lane[P]) faultsBefore(e *Envelope[P], gen uint32) error {
	if !ln.sub.eng.faults.delaySettle() {
		return nil
	}
	now := ln.sub.eng.now()
	var gone []*Envelope[P]
	requeued := false
	ln.mu.Lock()
	if e.cell.Revoke(settlement.Gen(gen)) {
		ln.live.remove(e)
		requeued = ln.pushReady(e, now, &gone)
	}
	ln.mu.Unlock()
	ln.dropAll(gone, false)
	if requeued {
		ln.sub.st.requeued.Add(1)
		ln.sub.st.expired.Add(1)
		ln.sub.waiters.NotifyAll()
		ln.sub.unleased()
	}
	ln.rearm()
	return nil
}

// faultsAfter applies the settlement faults that act once the attempt is in
// flight: a refused settlement is rejected definitely and the lease survives
// it, unless a revocation or a close landed while the attempt was pending, in
// which case the record goes back to its lane exactly as that revocation
// would have put it there (Faults.FailSettlement).
func (ln *lane[P]) faultsAfter(e *Envelope[P], gen uint32) error {
	if !ln.sub.eng.faults.settle() {
		return nil
	}
	st, ok := e.cell.Reject(settlement.Gen(gen))
	if !ok || st == settlement.Active {
		return ErrSettlementFailed
	}
	now := ln.sub.eng.now()
	var gone []*Envelope[P]
	requeued := false
	ln.mu.Lock()
	if e.inList {
		ln.live.remove(e)
		requeued = ln.pushReady(e, now, &gone)
	}
	ln.mu.Unlock()
	ln.dropAll(gone, false)
	if requeued {
		ln.sub.st.requeued.Add(1)
		ln.sub.waiters.NotifyAll()
		ln.sub.unleased()
	}
	ln.rearm()
	return ErrSettlementFailed
}

// own reports whether e is a live delivery of this consumer.
func (c *Consumer[P]) own(e *Envelope[P]) error {
	if e == nil || e.lane == nil {
		return errcode.ErrNotSettleable
	}
	if e.lane.sub != c.sub || e.owner.Load() != c.id {
		return errcode.ErrLeaseLost
	}
	return nil
}

// stateErr maps an observed settlement state to the sentinel a caller that
// presented the current generation must see.
func stateErr(st settlement.State) error {
	switch st {
	case settlement.Settling:
		return errcode.ErrSettlementInProgress
	case settlement.Settled:
		return errcode.ErrAlreadySettled
	case settlement.Unknown:
		return errcode.ErrSettlementUnknown
	case settlement.Closed:
		return errcode.ErrClosed
	default:
		return errcode.ErrLeaseLost
	}
}

// Ack settles the delivery terminally: the record leaves the subscription
// and its payload is released once every subscription has settled it.
//
// It fails with ErrLeaseLost when gen is not the delivery's generation or
// the consumer no longer owns it, ErrAlreadySettled after a successful
// settlement, ErrSettlementInProgress when another attempt is in flight and
// ErrClosed after the owning consumer closed.
func (c *Consumer[P]) Ack(e *Envelope[P], gen uint32) error {
	if err := c.own(e); err != nil {
		return err
	}
	if err := e.lane.faultsBefore(e, gen); err != nil {
		return err
	}
	if err := e.cell.Begin(settlement.Gen(gen)); err != nil {
		return err
	}
	if err := e.lane.faultsAfter(e, gen); err != nil {
		return err
	}
	ln := e.lane
	ln.mu.Lock()
	ln.live.remove(e)
	ln.mu.Unlock()
	e.cell.Commit(settlement.Gen(gen))
	ln.discard(e, true, ln.sub.st.acked)
	ln.rearm()
	return nil
}

// Nack settles the delivery negatively and returns the record to its lane,
// immediately when delay is not positive and through the lane's delayed
// structure otherwise. The record keeps its attempt count, which the next
// delivery increments. Its errors are those of Ack.
func (c *Consumer[P]) Nack(e *Envelope[P], gen uint32, delay time.Duration) error {
	if err := c.own(e); err != nil {
		return err
	}
	if err := e.lane.faultsBefore(e, gen); err != nil {
		return err
	}
	if err := e.cell.Begin(settlement.Gen(gen)); err != nil {
		return err
	}
	if err := e.lane.faultsAfter(e, gen); err != nil {
		return err
	}
	ln := e.lane
	ln.mu.Lock()
	ln.live.remove(e)
	ln.mu.Unlock()
	e.cell.Commit(settlement.Gen(gen))
	ln.sub.st.nacked.Add(1)
	now := ln.sub.eng.now()
	if ln.sub.exhausted(e) && ln.deadLetter(e) {
		// The record used up its attempts and the dead-letter publication
		// committed, which is the only thing that settles it (
		// adr/ADR-011-failure-policy.md).
		ln.rearm()
		return nil
	}
	at := int64(0)
	if delay > 0 {
		at = now + int64(delay)
	}
	if ln.requeueOrDrop(e, at, now) {
		ln.sub.st.requeued.Add(1)
		ln.sub.waiters.Notify()
	}
	ln.rearm()
	return nil
}

// Extend moves the delivery's lease deadline to d from now. A non-positive d
// uses the subscription's acknowledgement deadline. Its errors are those of
// Ack.
func (c *Consumer[P]) Extend(e *Envelope[P], gen uint32, d time.Duration) error {
	if err := c.own(e); err != nil {
		return err
	}
	if d <= 0 {
		d = c.sub.ackDeadline
	}
	ln := e.lane
	ln.mu.Lock()
	st, g := e.cell.Load()
	if uint32(g) != gen {
		ln.mu.Unlock()
		return errcode.ErrLeaseLost
	}
	if st != settlement.Active {
		ln.mu.Unlock()
		return stateErr(st)
	}
	ln.live.remove(e)
	if d > 0 {
		e.deadline = ln.sub.eng.now() + int64(d)
	} else {
		e.deadline = neverExpires
	}
	ln.live.push(e)
	ln.mu.Unlock()
	// The head of the in-flight list may have moved, so the scheduler is
	// told when to look at the lane again.
	ln.rearm()
	return nil
}

// Close detaches the consumer. Every record it still holds returns to its
// lane for another consumer, which is the close policy of the memory engine;
// a settlement already in flight keeps its outcome. Close is idempotent.
func (c *Consumer[P]) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	s := c.sub
	s.active.Add(-1)
	// The leave rebalances first, so that the lanes this consumer is about
	// to give up already belong to whoever takes them over.
	s.leave(c.id)
	now := s.eng.now()
	var gone []*Envelope[P]
	for _, ln := range s.lanes {
		requeued := 0
		ln.mu.Lock()
		for e := ln.live.head; e != nil; {
			next := e.next
			if e.owner.Load() == c.id {
				_, g := e.cell.Load()
				if e.cell.Close(g) {
					ln.live.remove(e)
					if !ln.pushReady(e, now, &gone) {
						e = next
						continue
					}
					requeued++
				}
			}
			e = next
		}
		ln.mu.Unlock()
		ln.dropAll(gone, false)
		clear(gone)
		gone = gone[:0]
		if requeued > 0 {
			s.st.requeued.Add(int64(requeued))
			s.waiters.NotifyAll()
			s.unleased()
		}
	}
	// The close itself is a wake, whether or not it requeued anything: this
	// consumer's own Fetch may be parked on an empty lane, and a parked
	// fetch can only leave on a notification, on its context or on a close
	// (internal/wait). Announcing it only when a requeue happened left the
	// one state a parked fetch is always in -- nothing ready -- with no wake
	// at all, so Source.Close could not unblock it and the driver above had
	// to merge contexts to do the engine's job
	// (adr/ADR-017-assumptions.md).
	//
	// Every other parked consumer of the subscription is woken with it; they
	// re-check their lanes, find them as they were and park again, which is
	// what a spurious wake costs on a path that runs once per consumer
	// close.
	s.waiters.NotifyAll()
	return nil
}
