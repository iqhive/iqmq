package engine

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/model/memq"
)

// tick is the granularity every delay and clock advance in the differential
// tests is a multiple of. The coarsest scheduler kind rounds due times up to
// one millisecond, so aligning every time to that tick keeps the model's
// prediction exact for every kind.
const tick = time.Millisecond

// harnessTopic is the single topic the differential tests publish to, and
// harnessDLQ is the dead-letter topic they route exhausted records to. The
// dead-letter topic has no subscription, so a record published to it leaves
// the engine at once and the release ledger of the original stays
// comparable, which is what internal/model/memq models (Config.DeadLetter).
const (
	harnessTopic = "t"
	harnessDLQ   = "dlq"
)

// maxSubs, maxConsumers and maxHandles bound the shape of a script.
const (
	maxSubs      = 3
	maxConsumers = 3
	maxHandles   = 8
	maxFetch     = 4
)

// handle is a delivery a script is holding: the engine's envelope and
// generation next to the model's generation for the same delivery. The two
// generations differ by a constant offset because the engine continues the
// generation of the pooled envelope it recycles, so the harness perturbs both
// by the same amount when it forges a stale handle.
type handle struct {
	sub  int
	cons int
	// seq is the publication sequence both sides report (engine Info.Seq,
	// model Claim.Origin), and modSeq is the model's own identity for the
	// admission, which a replay makes distinct from it.
	seq    uint64
	modSeq uint64
	env    *Envelope[int64]
	engGen uint32
	modGen uint32
	live   bool
}

// consumer pairs an engine consumer with the id the model knows it by.
type consumer struct {
	c  *Consumer[int64]
	id uint32
}

// subHandle pairs an engine subscription with its consumers.
type subHandle struct {
	name  string
	sub   *Subscription[int64]
	cons  []consumer
	alive bool
}

// harness runs one op script against the engine and the model at once.
type harness struct {
	t     testing.TB
	eng   *Engine[int64]
	top   *Topic[int64]
	mod   *memq.Model
	subs  []subHandle
	held  []handle
	claim []Claim[int64]

	// released counts the release callbacks the engine issued per payload,
	// and seqOf maps a model record sequence to the payload published with
	// it, so that the model's release ledger can be compared.
	//
	// relMu guards released, because the engine's scheduler goroutine issues
	// release callbacks of its own: a record given up by the TTL sweep or
	// dead-lettered by the lease sweep is released from lane.tick, which
	// Publish.Release documents (topic.go, "the engine's own TTL sweep is
	// the exception") and Engine.Close waits for. synctest only orders that
	// goroutine's writes before a later synctest.Wait, not this goroutine's
	// earlier reads before the sweep, so the ledger needs the same mutex
	// race_test.go's rig and archown_test.go's already put on theirs.
	relMu    sync.Mutex
	released map[int64]int
	seqOf    map[uint64]int64
	pubs     int64
	admitted uint64

	partitions int
	// poll is a context that is already done, so a Fetch with nothing to
	// claim returns at once instead of parking.
	poll context.Context

	// ordered turns the strict-ordering check of checkOrder on: it is kept
	// only when the harness is configured for strict ordering over a single
	// lane, which is where per-lane order is a promise.
	ordered bool
	// wildcards, sticky and stream mirror the topology so that the script
	// steps know which operations the run supports.
	wildcards bool
	sticky    bool
	stream    bool
	// start is the engine clock when the harness was built, which is where
	// the model's own clock starts. It converts a model instant into the
	// engine's time.Time, so a StartAtTime position means the same thing on
	// both sides.
	start time.Time
	// subj is the round-robin index of the next subject a wildcard run
	// publishes to.
	subj int
}

// harnessConfig is the shape of one differential run.
type harnessConfig struct {
	kinds       KindSet
	partitions  int
	capMessages int
	capBytes    int64
	ackDeadline time.Duration
	maxTotal    int
	overflow    OverflowPolicy
	ordering    OrderingMode
	maxAttempts uint32
	deadLetter  bool
	// Slice-4 topology knobs. They are independent of one another and of
	// everything above, except where the engine refuses the combination
	// (newHarness narrows those the same way the engine does).
	stream    bool
	retention Retention
	priority  bool
	wildcards bool
	dedup     time.Duration
	sticky    bool
}

// harnessFilters are the subject patterns the subscriptions of a wildcard run
// take, by subscription index: the first takes everything, the others a
// narrowing pair of patterns.
var harnessFilters = [maxSubs]string{"", "a.*", "a.b.>"}

// harnessSubjects are the subjects a wildcard run publishes to. They cover
// every combination of the filters: one that only the catch-all takes, one
// that the single-token pattern takes, and one that the tail pattern takes.
var harnessSubjects = []string{"a.b", "a.b.c", "x.y"}

// newHarness builds an engine and a model with matching configuration.
func newHarness(t testing.TB, hc harnessConfig) *harness {
	t.Helper()
	if hc.partitions <= 0 {
		hc.partitions = 1
	}
	ks := hc.kinds.Resolve()
	if !ks.QueueOrdered() {
		// The engine refuses strict ordering on kinds that do not order
		// what they hold (Engine.CreateTopic), so a script over such a set
		// runs relaxed.
		hc.ordering = OrderingRelaxed
	}
	if !ks.QueueOrdered() && hc.overflow.Resolve() == OverflowDropOldest {
		// A full lane displaces "the oldest record", and an unordered ready
		// queue does not promise which of its records that is: the engine
		// and the model then hold different records, not merely different
		// byte gauges, and a later fetch compares two different lanes. The
		// differential therefore runs the displacing policy on the kinds
		// that order what they hold; DropOldest itself is covered for every
		// kind by overflow_test.go, which asserts the accounting rather than
		// the identity of the record that went.
		hc.overflow = OverflowReject
	}
	if ks.Arch.Shared() && hc.overflow.Resolve() == OverflowDropOldest {
		// A shared log gives up the record that is holding its tail, which
		// is the only one whose slot can be reclaimed, and that is not
		// always the record a per-subscription lane would give up: a
		// promoted redelivery or one still waiting for a due time holds a
		// slot older than anything the cursor has reached. The reference
		// model has one lane structure per state and cannot predict which
		// of them the log will empty, so the differential runs the shared
		// architecture under the policies that displace nothing. DropSlow
		// under a shared log is covered by the fan-out differential, whose
		// records have no due times and so no ambiguity.
		hc.overflow = OverflowReject
	}
	if hc.overflow == OverflowDefault {
		// Every script publishes into a lane that fills up and expects an
		// answer rather than a wait: OverflowBlock is exercised by its own
		// tests, which park a publisher on purpose.
		hc.overflow = OverflowReject
	}
	if ks.Arch.Shared() {
		// A shared partition log decides admission and the subscription set
		// for every subscription at once, in slot order: it has no
		// per-priority ring, no per-subscription routing decision and no way
		// to admit a replayed record. The engine refuses the three, so a
		// script over such a set runs without them.
		hc.stream, hc.priority, hc.wildcards = false, false, false
	}
	if hc.priority {
		// Priorities reorder a lane by construction, so the engine refuses
		// them under strict ordering and the model relaxes the lane.
		hc.ordering = OrderingRelaxed
	}
	if !hc.stream {
		hc.retention = Retention{}
	} else if !hc.retention.Enabled() {
		hc.retention.MaxMessages = hc.capMessages
	}
	if hc.ordering.Strict() && hc.partitions > 1 {
		// Strict ordering over more than one lane implies an exclusive
		// assignment: competing consumers would interleave the records of
		// the lane they share (engine Topic.Subscribe).
		hc.sticky = true
	}
	dlqName := ""
	if hc.deadLetter {
		dlqName = harnessDLQ
	}
	eng, err := New(Config[int64]{
		Kinds:            ks,
		MaxTotalMessages: hc.maxTotal,
		Clone:            func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{
			Partitions:       hc.partitions,
			CapacityMessages: hc.capMessages,
			CapacityBytes:    hc.capBytes,
			AckDeadline:      hc.ackDeadline,
			Overflow:         hc.overflow,
			Ordering:         hc.ordering,
			MaxAttempts:      hc.maxAttempts,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if hc.deadLetter {
		if _, err := eng.CreateTopic(harnessDLQ, TopologyConfig{CapacityMessages: -1}); err != nil {
			t.Fatalf("CreateTopic(dlq): %v", err)
		}
	}
	kind := TopologyTopic
	if hc.stream {
		kind = TopologyStream
	}
	top, err := eng.CreateTopic(harnessTopic, TopologyConfig{
		Kind:            kind,
		DeadLetterTopic: dlqName,
		Retention:       hc.retention,
		Priority:        hc.priority,
		Wildcards:       hc.wildcards,
		DedupWindow:     hc.dedup,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	poll, cancel := context.WithCancel(context.Background())
	cancel() // every Fetch in a script polls; it must never block
	h := &harness{
		t:          t,
		eng:        eng,
		top:        top,
		claim:      make([]Claim[int64], maxFetch),
		released:   map[int64]int{},
		seqOf:      map[uint64]int64{},
		partitions: hc.partitions,
		poll:       poll,
		ordered:    hc.ordering.Strict() && hc.partitions == 1,
		wildcards:  hc.wildcards,
		sticky:     hc.sticky,
		stream:     hc.stream,
		start:      eng.clock.Now(),
	}
	h.mod = memq.New(memq.Config{
		Partitions:       hc.partitions,
		CapacityMessages: top.Topology().CapacityMessages,
		CapacityBytes:    top.Topology().CapacityBytes,
		AckDeadline:      top.Topology().AckDeadline,
		MaxTotalMessages: hc.maxTotal,
		// Ordering is checked exactly only when the ready queue, the
		// scheduler and the architecture all promise it: an unordered queue
		// kind claims no order at all, an unordered scheduler kind fires a
		// batch of due records in an order it does not specify, and the
		// shared-log architecture serves redeliveries before the log. The
		// model relaxes the whole lane for those kinds; records that come
		// due at the same instant are relaxed for every kind, because no
		// scheduler orders a tie.
		Ordered: ks.QueueOrdered(),
		Retry:   modelRetry(ks.RetryLane),
		Strict:  hc.ordering.Strict(),
		Overflow: map[OverflowPolicy]memq.Overflow{
			OverflowBlock:      memq.OverflowBlock,
			OverflowReject:     memq.OverflowReject,
			OverflowDropNewest: memq.OverflowDropNewest,
			OverflowDropOldest: memq.OverflowDropOldest,
		}[hc.overflow.Resolve()],
		MaxAttempts: hc.maxAttempts,
		DeadLetter:  hc.deadLetter,
		// A copying architecture is a safe copy by another name: the
		// subscriptions share nothing and the publisher's payload comes
		// straight back.
		SafeCopy: ks.Safe || ks.Arch.Copies(),
		// A shared architecture takes one slot per publication for every
		// subscription at once, so the capacity the publishers see is the
		// log's, not the lane's.
		SharedLog: ks.Arch.Shared(),

		Priority:    hc.priority,
		Levels:      PriorityLevels,
		Wildcards:   hc.wildcards,
		DedupWindow: hc.dedup,
		Sticky:      hc.sticky,
		Retention: memq.Retention{
			MaxMessages: hc.retention.MaxMessages,
			MaxBytes:    hc.retention.MaxBytes,
			MaxAge:      hc.retention.MaxAge,
		},
	})
	return h
}

// modelRetry maps the engine's retry placement onto the model's.
func modelRetry(k RetryLane) memq.Retry {
	switch k.Resolve() {
	case PushFront:
		return memq.RetryFront
	case SideQueue:
		return memq.RetrySide
	default:
		return memq.RetryBlock
	}
}

// close shuts the engine down; the model has already been closed by the
// script when the script asked for it.
func (h *harness) close() {
	_ = h.eng.Close()
}

// sameErr compares an engine error with the model's expectation.
func (h *harness) sameErr(op string, got error, res memq.Result) {
	h.t.Helper()
	want := res.Err
	if errors.Is(got, want) || (got == nil && want == nil) {
		return
	}
	if res.Stale && errors.Is(got, errcode.ErrLeaseLost) {
		return // a recycled envelope may report the coarser sentinel
	}
	h.t.Fatalf("%s: engine %v, model %v (stale=%v)", op, got, want, res.Stale)
}

// step applies op to the model and returns its result.
func (h *harness) step(op memq.Op) memq.Result { return h.mod.Step(op) }

// subscribe adds a subscription (idempotent, like the engine's).
func (h *harness) subscribe(i int) {
	h.t.Helper()
	for i >= len(h.subs) {
		name := "s" + string(rune('0'+len(h.subs)))
		h.subs = append(h.subs, subHandle{name: name})
	}
	sh := &h.subs[i]
	if sh.alive {
		return
	}
	filter := ""
	if h.wildcards {
		filter = harnessFilters[i%len(harnessFilters)]
	}
	sub, err := h.top.Subscribe(sh.name, SubscriptionConfig{Filter: filter, Sticky: h.sticky})
	res := h.step(memq.Op{Kind: memq.Subscribe, Sub: sh.name, Filter: filter})
	h.sameErr("subscribe", err, res)
	if err != nil {
		return
	}
	sh.sub = sub
	sh.alive = true
}

// attach adds a consumer to a subscription.
func (h *harness) attach(i int) {
	h.t.Helper()
	if i >= len(h.subs) || !h.subs[i].alive || len(h.subs[i].cons) >= maxConsumers {
		return
	}
	sh := &h.subs[i]
	c, err := sh.sub.Attach(ConsumerConfig{})
	if err != nil {
		res := h.step(memq.Op{Kind: memq.Attach, Sub: sh.name, Consumer: 0})
		h.sameErr("attach", err, res)
		return
	}
	res := h.step(memq.Op{Kind: memq.Attach, Sub: sh.name, Consumer: c.ID()})
	h.sameErr("attach", nil, res)
	sh.cons = append(sh.cons, consumer{c: c, id: c.ID()})
}

// releases reports how many release callbacks the engine has issued for
// payload v, under relMu.
func (h *harness) releases(v int64) int {
	h.relMu.Lock()
	defer h.relMu.Unlock()
	return h.released[v]
}

// publish admits one record. prio, subj and dedup are ignored by a run whose
// topology does not offer them, exactly as the engine ignores them.
func (h *harness) publish(part, size int, delay time.Duration) {
	h.t.Helper()
	h.publishFull(part, size, delay, 0, "")
}

// publishFull admits one record with every publication property the slice-4
// topologies read.
func (h *harness) publishFull(part, size int, delay time.Duration, prio uint8, dedup string) {
	h.t.Helper()
	h.pubs++
	v := h.pubs
	subj := ""
	if h.wildcards {
		subj = harnessSubjects[h.subj%len(harnessSubjects)]
		h.subj++
	}
	err := h.top.Publish(context.Background(), Publish[int64]{
		Payload:   v,
		Size:      size,
		Partition: part,
		Delay:     delay,
		Priority:  prio,
		Subject:   subj,
		DedupKey:  []byte(dedup),
		Release: func(x int64) {
			h.relMu.Lock()
			h.released[x]++
			h.relMu.Unlock()
		},
	})
	res := h.step(memq.Op{
		Kind: memq.Publish, Partition: part, Size: int64(size), Delay: delay,
		Priority: prio, Subject: subj, DedupKey: dedup,
	})
	h.sameErr("publish", err, res)
	if err != nil {
		return
	}
	if res.Retained {
		// No subscription took the publication but the topic's retained log
		// did: a stream's log is a property of the topic, so the record is
		// still replayable and the payload is the engine's until the log
		// trims it (engine Topic.retainOnly). It takes a publication
		// sequence like any other admission.
		h.admitted++
		h.seqOf[h.admitted] = v
		return
	}
	if res.Orphan {
		// The publication reached no subscription — the topic has none, or
		// none whose filter matches its subject — so the engine discarded it
		// and handed the payload straight back.
		if n := h.releases(v); n != 1 {
			h.t.Fatalf("orphaned publication %d released %d times", v, n)
		}
		return
	}
	if res.Deduped {
		// The dedup window discarded the publication: nothing holds the
		// record and the engine released the payload it was handed.
		if n := h.releases(v); n != 1 {
			h.t.Fatalf("deduplicated publication %d released %d times", v, n)
		}
		return
	}
	if res.Dropped {
		// The overflow policy discarded the publication: nothing holds the
		// record, and the engine released the payload it was handed, which
		// is what a caller reads a nil error as (Publish.Release).
		if n := h.releases(v); n != 1 {
			h.t.Fatalf("dropped publication %d released %d times", v, n)
		}
		return
	}
	h.admitted++
	h.seqOf[h.admitted] = v
}

// seek replays a stream subscription from a position. back is how far before
// the current model instant a StartAtTime position points, which both sides
// are given as the same instant: a zero At would make StartAtTime take the
// whole log on both sides and so be indistinguishable from StartEarliest.
func (h *harness) seek(si int, kind StartKind, seq uint64, back time.Duration) {
	h.t.Helper()
	if !h.stream || si >= len(h.subs) || !h.subs[si].alive {
		return
	}
	sh := &h.subs[si]
	at := h.mod.Now() - back
	if at < 0 {
		at = 0
	}
	pos := StartPosition{Kind: kind, Seq: seq}
	mpos := memq.StartPosition{Kind: memq.StartKind(kind), Seq: seq}
	if kind == StartAtTime {
		pos.At = h.start.Add(at)
		mpos.At = at
	}
	n, err := sh.sub.Seek(pos)
	res := h.step(memq.Op{Kind: memq.Seek, Sub: sh.name, Start: mpos})
	h.sameErr("seek", err, res)
	if err == nil && n != res.N {
		h.t.Fatalf("seek: engine replayed %d records, model %d", n, res.N)
	}
}

// rebalance recomputes a subscription's assignment on both sides.
func (h *harness) rebalance(si int) {
	h.t.Helper()
	if si >= len(h.subs) || !h.subs[si].alive {
		return
	}
	sh := &h.subs[si]
	sh.sub.Rebalance()
	h.step(memq.Op{Kind: memq.Rebalance, Sub: sh.name})
}

// checkOrder holds the engine to strict per-lane order: the records a fetch
// returns must be the first deliverable records of the subscription, in
// publication order, where a record already leased to somebody is not
// deliverable and a record that failed is — it blocks its lane until it is
// settled (the memory engine). The membership comes from the model, which
// the harness compares against the engine's own gauges after every step; the
// order does not, so this is an independent check of the strict-ordering
// invariant rather than a second reading of the model's placement rules.
func (h *harness) checkOrder(before []uint64, n int) {
	h.t.Helper()
	if !h.ordered {
		return
	}
	in := make(map[uint64]bool, len(before))
	for _, seq := range before {
		in[seq] = true
	}
	i := 0
	for j := range n {
		got := h.claim[j].Info.Seq
		if !in[got] {
			// A record the publisher held back takes its place in the lane
			// at its due time, not at its publication: strict ordering is a
			// promise about the records nobody moved.
			continue
		}
		if i >= len(before) || got != before[i] {
			h.t.Fatalf("strict ordering: fetch %d returned %d, want %v", j, got, before)
		}
		i++
	}
}

// deliverable returns the records a subscription could hand to a fetch now,
// in publication order: everything it holds that nobody is holding a lease
// on.
func (h *harness) deliverable(name string) []uint64 {
	if !h.ordered {
		return nil
	}
	held, leased := h.mod.Held(name)
	live := make(map[uint64]bool, len(leased))
	for _, seq := range leased {
		live[seq] = true
	}
	out := make([]uint64, 0, len(held))
	for _, seq := range held {
		if !live[seq] {
			out = append(out, seq)
		}
	}
	return out
}

// fetch claims records for one consumer and checks the model agrees.
func (h *harness) fetch(si, ci, max int) {
	h.t.Helper()
	if si >= len(h.subs) || ci >= len(h.subs[si].cons) {
		return
	}
	if max < 1 {
		max = 1
	}
	if max > maxFetch {
		max = maxFetch
	}
	sh := &h.subs[si]
	cs := sh.cons[ci]
	before := h.deliverable(sh.name)
	n, err := cs.c.Fetch(h.poll, h.claim[:max])
	if err != nil && !errors.Is(err, context.Canceled) {
		res := h.step(memq.Op{Kind: memq.Fetch, Sub: sh.name, Consumer: cs.id, Max: max, Observed: []uint64{}})
		h.sameErr("fetch", err, res)
		return
	}
	observed := make([]uint64, n)
	for i := range n {
		observed[i] = h.claim[i].Info.Seq
	}
	res := h.step(memq.Op{Kind: memq.Fetch, Sub: sh.name, Consumer: cs.id, Max: max, Observed: observed})
	if res.Err != nil {
		h.t.Fatalf("fetch: model rejects %v: %v", observed, res.Err)
	}
	if res.N != n {
		h.t.Fatalf("fetch: engine claimed %d, model %d", n, res.N)
	}
	h.checkOrder(before, n)
	for i := range n {
		cl := h.claim[i]
		mc := res.Claims[i]
		if cl.Info.Seq != mc.Origin || cl.Info.Attempts != mc.Attempts || int(cl.Info.Partition) != mc.Partition {
			h.t.Fatalf("fetch %d: engine seq=%d attempts=%d part=%d, model seq=%d attempts=%d part=%d",
				i, cl.Info.Seq, cl.Info.Attempts, cl.Info.Partition, mc.Origin, mc.Attempts, mc.Partition)
		}
		if cl.Payload != h.seqOf[cl.Info.Seq] {
			h.t.Fatalf("fetch %d: payload %d for record %d, published %d", i, cl.Payload, cl.Info.Seq, h.seqOf[cl.Info.Seq])
		}
		h.hold(handle{
			sub: si, cons: ci, seq: cl.Info.Seq, modSeq: mc.Seq,
			env: cl.Env, engGen: cl.Gen, modGen: mc.Gen, live: true,
		})
	}
}

// hold records a handle, evicting the oldest when the table is full.
func (h *harness) hold(hd handle) {
	if len(h.held) < maxHandles {
		h.held = append(h.held, hd)
		return
	}
	copy(h.held, h.held[1:])
	h.held[len(h.held)-1] = hd
}

// ambiguous reports whether a forged generation would land on a delivery the
// model cannot see. The engine recycles envelopes, so a handle whose envelope
// has moved on can, with a forged generation, coincide with the generation
// the envelope now carries; the model tracks admissions, not envelopes, and
// has no way to predict the coincidence.
//
// The condition is exactly "the forgery names the envelope's current
// generation": an envelope nobody recycled still carries the handle's own
// generation, which only an unforged handle can name, so a match under a
// non-zero delta means the envelope is on a later delivery. Comparing the
// record's publication sequence as well — which is what this did before the
// stream topology — is not enough any more, because a replayed record keeps
// the sequence of the publication it replays while being a second,
// independently settled admission of it.
//
// Every other combination, including every unforged handle, stays a strict
// comparison.
func ambiguous(hd handle, delta int, engGen uint32) bool {
	return delta != 0 && hd.env.Gen() == engGen
}

// settle acks or nacks through a held handle, optionally with a forged
// generation offset.
func (h *harness) settle(idx int, ack bool, delta int, delay time.Duration) {
	h.t.Helper()
	if len(h.held) == 0 {
		return
	}
	hd := h.held[idx%len(h.held)]
	sh := &h.subs[hd.sub]
	if hd.cons >= len(sh.cons) {
		return
	}
	cs := sh.cons[hd.cons]
	engGen := uint32(int64(hd.engGen) + int64(delta))
	modGen := uint32(int64(hd.modGen) + int64(delta))
	if ambiguous(hd, delta, engGen) {
		return
	}
	var err error
	kind := memq.Ack
	if ack {
		err = cs.c.Ack(hd.env, engGen)
	} else {
		kind = memq.Nack
		err = cs.c.Nack(hd.env, engGen, delay)
	}
	res := h.step(memq.Op{Kind: kind, Sub: sh.name, Consumer: cs.id, Seq: hd.modSeq, Gen: modGen, Delay: delay})
	h.sameErr(kind.String(), err, res)
}

// extend moves a lease deadline through a held handle.
func (h *harness) extend(idx, delta int, d time.Duration) {
	h.t.Helper()
	if len(h.held) == 0 {
		return
	}
	hd := h.held[idx%len(h.held)]
	sh := &h.subs[hd.sub]
	if hd.cons >= len(sh.cons) {
		return
	}
	cs := sh.cons[hd.cons]
	engGen := uint32(int64(hd.engGen) + int64(delta))
	modGen := uint32(int64(hd.modGen) + int64(delta))
	if ambiguous(hd, delta, engGen) {
		return
	}
	err := cs.c.Extend(hd.env, engGen, d)
	res := h.step(memq.Op{Kind: memq.Extend, Sub: sh.name, Consumer: cs.id, Seq: hd.modSeq, Gen: modGen, Dur: d})
	h.sameErr("extend", err, res)
}

// closeConsumer detaches one consumer.
func (h *harness) closeConsumer(si, ci int) {
	h.t.Helper()
	if si >= len(h.subs) || ci >= len(h.subs[si].cons) {
		return
	}
	sh := &h.subs[si]
	cs := sh.cons[ci]
	if err := cs.c.Close(); err != nil {
		h.t.Fatalf("consumer close: %v", err)
	}
	h.step(memq.Op{Kind: memq.CloseConsumer, Sub: sh.name, Consumer: cs.id})
}

// expire reclaims every lease whose deadline has passed.
func (h *harness) expire() {
	h.t.Helper()
	n := h.eng.Faults().ExpireLeases()
	res := h.step(memq.Op{Kind: memq.ExpireLeases})
	if n != res.N {
		h.t.Fatalf("expire: engine reclaimed %d, model %d", n, res.N)
	}
}

// advance moves the clock by a multiple of the scheduler tick. It is called
// only from inside a synctest bubble: after the sleep it waits for the
// engine's scheduler goroutine to become durably blocked again, so that every
// promotion and every lease expiry the clock move implies has happened before
// the model is told about it. The model then applies the same expiry sweep,
// which is what the engine's scheduler did on its own.
func (h *harness) advance(d time.Duration) {
	h.t.Helper()
	if d <= 0 {
		return
	}
	time.Sleep(d)
	synctest.Wait()
	h.step(memq.Op{Kind: memq.Advance, Dt: d})
	h.step(memq.Op{Kind: memq.ExpireLeases})
}

// closeSub detaches one subscription.
func (h *harness) closeSub(si int) {
	h.t.Helper()
	if si >= len(h.subs) || !h.subs[si].alive {
		return
	}
	sh := &h.subs[si]
	if err := sh.sub.Close(); err != nil {
		h.t.Fatalf("subscription close: %v", err)
	}
	h.step(memq.Op{Kind: memq.CloseSubscription, Sub: sh.name})
}

// closeEngine shuts both sides down.
func (h *harness) closeEngine() {
	h.t.Helper()
	if err := h.eng.Close(); err != nil {
		h.t.Fatalf("engine close: %v", err)
	}
	h.step(memq.Op{Kind: memq.CloseEngine})
}

// check compares every observable of the engine with the model and asserts
// the model's own invariants.
func (h *harness) check() {
	h.t.Helper()
	if err := h.mod.Invariants(); err != nil {
		h.t.Fatalf("model invariant: %v", err)
	}
	for i := range h.subs {
		sh := &h.subs[i]
		if !sh.alive {
			continue
		}
		want, ok := h.mod.Sub(sh.name)
		if !ok {
			h.t.Fatalf("model has no subscription %s", sh.name)
		}
		got := sh.sub.Inspect()
		if got.Ready != want.Ready || got.InFlight != want.InFlight || got.Delayed != want.Delayed ||
			got.Bytes != want.Bytes || got.Published != want.Published || got.Acked != want.Acked ||
			got.Nacked != want.Nacked || got.Requeued != want.Requeued || got.Expired != want.Expired ||
			got.Dropped != want.Dropped || got.Rejected != want.Rejected || got.Attempts != want.Attempts ||
			got.ActiveConsumers != want.ActiveConsumers {
			h.t.Fatalf("subscription %s:\n engine %+v\n model  %+v", sh.name, got, want)
		}
		if got.Assignment != want.Assignment {
			// The rebalance generation, which FuzzEngineRebalance's doc
			// names: lane ownership is checked through what a fetch may
			// claim, but nothing pinned the version the engine reports.
			h.t.Fatalf("subscription %s: engine assignment=%d, model assignment=%d",
				sh.name, got.Assignment, want.Assignment)
		}
		// The table itself, not only its generation: a rotated one hands the
		// same lanes to different consumers at the same version.
		gotVer, gotOwners := sh.sub.Assignment()
		wantVer, wantOwners := h.mod.Assignment(sh.name)
		if gotVer != wantVer || !slices.Equal(gotOwners, wantOwners) {
			h.t.Fatalf("subscription %s: engine assignment %d%v, model %d%v",
				sh.name, gotVer, gotOwners, wantVer, wantOwners)
		}
		if got.Retained != want.Retained || got.Deduplicated != want.Deduplicated {
			h.t.Fatalf("subscription %s: engine retained=%d deduped=%d, model retained=%d deduped=%d",
				sh.name, got.Retained, got.Deduplicated, want.Retained, want.Deduplicated)
		}
		if !got.Conserved() {
			h.t.Fatalf("subscription %s is not conserved: %+v", sh.name, got)
		}
	}
	// Release ledger: every payload the engine took is released exactly
	// once, and by the same step the model releases it — a shared record
	// only once its last holder has let go of it: a record a shared log
	// holds until the subscription that settles it last lets go, and under
	// a stream the retained log is a holder too.
	engTotal, modTotal := 0, 0
	for seq, v := range h.seqOf {
		got, want := h.releases(v), h.mod.Released(seq)
		if got > 1 {
			h.t.Fatalf("record %d (payload %d) released %d times", seq, v, got)
		}
		engTotal += got
		modTotal += want
		if got == want {
			continue
		}
		h.t.Fatalf("record %d (payload %d) released %d times, model %d", seq, v, got, want)
	}
	if engTotal != modTotal {
		h.t.Fatalf("release ledger: engine released %d payloads, model %d", engTotal, modTotal)
	}
	// Publications discarded for want of a subscription, which the engine
	// counts engine-wide: the ones a script makes to a topic with no
	// subscription, and the dead-letter publications, whose topic the
	// differential configures with none (memq.Config.DeadLetter).
	if _, _, discarded := h.eng.Totals(); discarded != h.mod.Orphans() {
		h.t.Fatalf("orphans: engine %d, model %d", discarded, h.mod.Orphans())
	}
}
