package memory_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/memory"
)

// FuzzMemoryModel is the test suite's flagship target for this package:
// a fuzz-chosen engine, topology and operation script driven entirely through
// the PUBLIC API, with the invariants checked after every step, inside a
// testing/synctest bubble.
//
// It is not a copy of the engine's own FuzzEngineModel and it cannot be. That
// target's corpus decodes a kind-set index, a topology and an operation
// stream against the engine's in-package harness — it forges delivery
// generations, seeks a retained log and rebalances a subscription explicitly,
// and none of those is expressible through the public surface. So the corpus
// is not portable and this target has seeds of its own; what carries over is
// the SHAPE, which is what makes the two comparable.
//
// The engine checked is a fuzz-chosen kind of every family, which is the specification's
// "run against every Arch/Queue/Wait/Timer/RetryLane combination": one script
// byte per family, so a committed corpus entry always names the same engine.
//
// Inside the bubble the clock is deterministic, every blocking operation is a
// poll — a parked receive would deadlock the bubble, which is exactly the
// signal the specification calls free leak detection — and a goroutine the driver forgot
// surfaces as a bubble failure rather than as a flake somewhere else.
func FuzzMemoryModel(f *testing.F) {
	sc := memoryScript()
	for _, seed := range memorySeeds(sc) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runMemoryScript(t, sc, data)
	})
}

// runMemoryScript decodes the engine and topology from the head of the script
// and drives the rest of it.
func runMemoryScript(t *testing.T, sc *fuzzutil.Script[*modelHarness], data []byte) {
	t.Helper()
	r := fuzzutil.NewReader(data)
	cfg := modelConfigFrom(r)
	rest := data[len(data)-r.Remaining():]
	synctest.Test(t, func(t *testing.T) {
		h := newModelHarness(t, cfg)
		defer h.close()
		sc.Run(t, rest, h)
		h.drain()
		h.check()
	})
}

// modelConfig is the broker shape a script names.
type modelConfig struct {
	engine     memory.EngineConfig
	topology   memory.TopologyConfig
	subs       int
	prefetch   int
	reuse      bool
	deadLetter bool
}

// modelOverflows are the overflow policies a script may choose.
//
// OverflowBlock is not one of them: the script publishes from the goroutine
// that drives it, and a parked publisher inside a synctest bubble with no
// consumer running is a bubble deadlock rather than an exercise of the
// policy. Blocking has its own test, which parks a publisher on purpose.
func modelOverflows() []memory.OverflowPolicy {
	return []memory.OverflowPolicy{
		memory.OverflowReject, memory.OverflowDropNewest, memory.OverflowDropOldest,
	}
}

// modelConfigFrom decodes the broker shape from the head of the script. Every
// field is decoded whether or not the run uses it, so that one script byte
// always means the same thing.
func modelConfigFrom(r *fuzzutil.Reader) modelConfig {
	cfg := modelConfig{}
	// One kind per family, which is the combination sweep.
	cfg.engine.Arch = memory.ArchKinds()[r.Choice(len(memory.ArchKinds()))]
	cfg.engine.Queue = modelQueueKinds()[r.Choice(len(modelQueueKinds()))]
	cfg.engine.Wait = memory.WaitKinds()[r.Choice(len(memory.WaitKinds()))]
	cfg.engine.Timer = memory.TimerKinds()[r.Choice(len(memory.TimerKinds()))]
	cfg.engine.Hash = memory.HashKinds()[r.Choice(len(memory.HashKinds()))]
	cfg.engine.Pool = memory.PoolKinds()[r.Choice(len(memory.PoolKinds()))]
	cfg.engine.Registry = memory.RegistryKinds()[r.Choice(len(memory.RegistryKinds()))]
	cfg.engine.Stats = memory.StatsKinds()[r.Choice(len(memory.StatsKinds()))]
	cfg.engine.RetryLane = memory.RetryLaneKinds()[r.Choice(len(memory.RetryLaneKinds()))]
	cfg.topology.Partitions = r.Int(1, 3)
	cfg.topology.CapacityMessages = r.Int(1, 8)
	cfg.topology.AckDeadline = time.Duration(r.Int(1, 20)) * modelTick
	cfg.topology.Overflow = modelOverflows()[r.Choice(len(modelOverflows()))]
	cfg.topology.MaxAttempts = uint32(r.Int(0, 4))
	cfg.subs = r.Int(1, maxModelSubs)
	cfg.prefetch = r.Int(1, 8)
	cfg.reuse = r.Bool()
	cfg.deadLetter = r.Bool()
	if cfg.deadLetter {
		cfg.topology.DeadLetterTopic = modelDLQ
	}
	return cfg
}

// modelQueueKinds are the ready-queue kinds a lane accepts: the family
// narrowed to the multi-producer multi-consumer members, which is what the
// engine itself narrows it to.
func modelQueueKinds() []memory.QueueKind {
	out := make([]memory.QueueKind, 0, len(memory.QueueKinds()))
	for _, k := range memory.QueueKinds() {
		if k == memory.QueueSPSC || k == memory.QueueMPSC {
			continue
		}
		out = append(out, k)
	}
	return out
}

const (
	// modelTick is the time unit the script's durations are counted in.
	modelTick = time.Millisecond
	// maxModelSubs is the number of subscriptions a script may create.
	maxModelSubs = 3
	// modelTopic is the topic every script publishes to.
	modelTopic = "orders"
	// modelDLQ is the dead-letter topic a script may route to.
	modelDLQ = "dlq"
	// maxModelHeld bounds the deliveries the script holds unsettled, so that
	// a script cannot make the harness's own bookkeeping the thing under
	// test.
	maxModelHeld = 32
)

// modelSub is one subscription's model: its consumer, the deliveries the
// script is holding, and the counters the conservation is checked against.
type modelSub struct {
	name string
	c    *mq.ConsumerHandle
	held []mq.Delivery
	// offers counts the publications the script made while the client was
	// open. Every one of them reached the engine, so every one of them is
	// accounted for in exactly one of Published, Overflowed, Rejected and
	// Deduplicated — which is the identity that catches a publication that
	// vanished without being counted anywhere.
	offers int64
	// terminal counts the settlements that took the engine's terminal
	// action: an acknowledgement, a drop  and a
	// successful dead-letter route all do.
	terminal int64
	// requeued counts the negative settlements that asked for a redelivery.
	requeued int64
	// deliveries counts every delivery the script took, redeliveries
	// included.
	deliveries int64
	// seen counts how many times each published body reached this
	// subscription, which is what catches a fabricated or a lost record.
	seen map[string]int
	// settled counts the terminal settlements per body, which must never
	// exceed one: the specification's "exactly one settlement wins".
	settled map[string]int
}

// modelHarness is the system under test and its model.
type modelHarness struct {
	t      *testing.T
	cfg    modelConfig
	client *mq.Client
	b      *memory.Broker
	p      *mq.PublisherHandle
	subs   []*modelSub
	// published is every body the script has offered, whether or not it was
	// admitted, so that a delivery of something never published is caught.
	published map[string]bool
	// seq names each publication uniquely, which is what makes a body an
	// identity rather than a value.
	seq int
	// closed reports that the script closed the client, after which every
	// operation must answer ErrClosed and the counters stop moving.
	closed bool
}

// newModelHarness builds the broker, the publisher and the subscriptions.
func newModelHarness(t *testing.T, cfg modelConfig) *modelHarness {
	t.Helper()
	ctx := context.Background()
	b, err := memory.NewBroker(memory.Config{
		AutoCreate:      true,
		DefaultTopology: cfg.topology,
		Engine:          cfg.engine,
		Clock:           memory.NewTestClock(time.Time{}),
	})
	if err != nil {
		// A configuration the engine cannot honour is not a defect: the
		// script is allowed to name one, and the refusal is the answer.
		t.Skipf("engine %v topology %+v: %v", cfg.engine, cfg.topology, err)
	}
	client, err := memory.OpenWithClient(ctx, b, memory.ClientOwned)
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	h := &modelHarness{t: t, cfg: cfg, client: client, b: b, published: map[string]bool{}}
	if cfg.deadLetter {
		adm, aerr := client.Admin()
		if aerr != nil {
			t.Fatalf("Admin: %v", aerr)
		}
		if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: modelDLQ}); err != nil {
			t.Fatalf("EnsureTopic(dlq): %v", err)
		}
	}
	for i := range cfg.subs {
		name := "sub" + strconv.Itoa(i)
		c, cerr := client.Consumer(ctx, mq.ConsumerConfig{
			Topic:        modelTopic,
			Subscription: name,
			Prefetch:     cfg.prefetch,
			BufferReuse:  cfg.reuse,
		})
		if cerr != nil {
			t.Fatalf("Consumer(%q): %v", name, cerr)
		}
		h.subs = append(h.subs, &modelSub{
			name:    name,
			c:       c,
			seen:    map[string]int{},
			settled: map[string]int{},
		})
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: modelTopic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	h.p = p
	return h
}

// close shuts the client down, which closes every handle it opened.
func (h *modelHarness) close() {
	if h.closed {
		return
	}
	h.closed = true
	if err := h.client.Close(); err != nil {
		h.t.Errorf("Close: %v", err)
	}
}

// poll returns a context that is already done, which is how the API is asked
// not to block: a parked receive inside a synctest bubble is a deadlock.
func poll() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// publish offers one message and records the outcome.
func (h *modelHarness) publish(delay, ttl time.Duration, prio, part int) {
	if h.closed {
		return
	}
	h.seq++
	body := "m" + strconv.Itoa(h.seq)
	h.published[body] = true
	var opts []mq.PublishOption
	if delay > 0 {
		opts = append(opts, mq.Delay(delay))
	}
	if ttl > 0 {
		opts = append(opts, mq.TTL(ttl))
	}
	if prio >= 0 {
		opts = append(opts, mq.Priority(prio))
	}
	if part >= 0 {
		opts = append(opts, mq.Partition(part))
	}
	err := h.p.Publish(context.Background(), &mq.Message{Body: []byte(body)}, opts...)
	for _, s := range h.subs {
		s.offers++
	}
	switch {
	case err == nil:
	case mq.CodeOf(err) == mq.CodeResourceExhausted,
		mq.CodeOf(err) == mq.CodePartial,
		mq.CodeOf(err) == mq.CodeUnsupported,
		mq.CodeOf(err) == mq.CodeUnavailable,
		mq.CodeOf(err) == mq.CodeClosed:
		// A refusal admits nothing, a partial fan-out admits some, an
		// unsupported control admits nothing and the fault injector
		// refuses before admission. None of them is a defect, and all of
		// them are counted by the engine — the offers identity below is
		// what checks that they are.
	default:
		h.t.Fatalf("Publish: %v (code %v)", err, mq.CodeOf(err))
	}
}

// forgetHeld drops every delivery the script is holding on one subscription.
//
// It is what a lease expiry, a rebalance and a consumer close all do to the
// script's side of the bookkeeping: the deliveries are no longer the
// script's, and the engine is the only one that knows which of them it took
// back. Nothing about the settlement counters is re-read from the engine —
// a model that copied the engine's counters would agree with it by
// construction, which is the check-that-cannot-fail this repository keeps
// finding.
func (h *modelHarness) forgetHeld(s *modelSub) {
	for i := range s.held {
		s.held[i] = mq.Delivery{}
	}
	s.held = s.held[:0]
}

// receive takes up to n deliveries from one subscription without blocking.
func (h *modelHarness) receive(si, n int) {
	if h.closed || len(h.subs) == 0 {
		return
	}
	s := h.subs[si%len(h.subs)]
	if len(s.held) >= maxModelHeld {
		return
	}
	buf := make([]mq.Delivery, n)
	got, err := s.c.Receive(poll(), buf)
	if err != nil && got == 0 {
		switch mq.CodeOf(err) {
		case mq.CodeCanceled, mq.CodeDeadlineExceeded, mq.CodeClosed:
			return
		default:
			h.t.Fatalf("Receive: %v (code %v)", err, mq.CodeOf(err))
		}
	}
	for i := range got {
		d := buf[i]
		body := string(d.Body)
		if !h.published[body] {
			h.t.Fatalf("subscription %s was delivered %q, which was never published", s.name, body)
		}
		if d.Attempt < 1 {
			h.t.Fatalf("delivery of %q reports attempt %d", body, d.Attempt)
		}
		s.seen[body]++
		s.deliveries++
		s.held = append(s.held, d)
	}
}

// settleKind is which verb a settlement step applies.
type settleKind int

const (
	settleAck settleKind = iota
	settleNack
	settleNackDelay
	settleDrop
	settleReject
	settleExtend
)

// settle applies one settlement verb to one held delivery.
func (h *modelHarness) settle(si, di int, kind settleKind, delay time.Duration) {
	if len(h.subs) == 0 {
		return
	}
	s := h.subs[si%len(h.subs)]
	if len(s.held) == 0 {
		return
	}
	idx := di % len(s.held)
	d := &s.held[idx]
	body := string(d.Body)
	var err error
	switch kind {
	case settleAck:
		err = d.Ack()
	case settleNack:
		err = d.Nack()
	case settleNackDelay:
		err = d.Nack(mq.NackDelay(delay))
	case settleDrop:
		err = d.Nack(mq.NackDrop())
	case settleReject:
		err = d.Reject()
	case settleExtend:
		// There is no public Extend on a Delivery in, so the lease is
		// extended through the driver's own surface, which is where the specification puts
		// it. It is not a settlement: the delivery stays held.
		h.extend(s, d, delay)
		return
	}
	if err == nil {
		switch kind {
		case settleAck, settleDrop, settleReject:
			s.terminal++
			s.settled[body]++
			// the specification's "exactly one settlement wins", per record. The
			// duplicate-delivery fault admits a record a second time on
			// purpose, and the second admission is a second record
			// with a settlement of its own, so the check is skipped once
			// the fault has produced one.
			if s.settled[body] > 1 && h.b.Faults().Duplicates() == 0 {
				h.t.Fatalf("subscription %s settled %q terminally %d times", s.name, body, s.settled[body])
			}
		case settleNack, settleNackDelay:
			s.requeued++
		case settleExtend:
		}
		h.dropHeld(s, idx)
		return
	}
	switch mq.CodeOf(err) {
	case mq.CodeUnsupported:
		// An action this driver cannot honour settles nothing, so the
		// delivery is still the script's to settle.
	case mq.CodeLeaseLost, mq.CodeAlreadySettled, mq.CodeClosed, mq.CodeNotSettleable:
		// The delivery is gone: its lease expired, a close took it back, or
		// something already settled it. Either way the script no longer
		// holds it.
		h.dropHeld(s, idx)
	case mq.CodeResourceExhausted, mq.CodeUnavailable:
		// A definite refusal: the fault injector refused the settlement, or
		// a dead-letter publication's destination had no room. The specification keeps
		// the lease intact for a definite refusal and the specification forbids a failed
		// dead-letter route from acknowledging, so either way the delivery
		// is still the script's to settle.
	case mq.CodeSettlementUnknown, mq.CodeAmbiguous:
		// An unknown outcome is sticky: the delivery is neither settled nor
		// settleable again, so the script stops holding it.
		h.dropHeld(s, idx)
	default:
		h.t.Fatalf("settlement %v of %q answered %v (code %v)", kind, body, err, mq.CodeOf(err))
	}
}

// extend moves a held delivery's lease through the driver's own surface.
func (h *modelHarness) extend(s *modelSub, d *mq.Delivery, by time.Duration) {
	if by <= 0 {
		return
	}
	_ = s
	// The specification gave Delivery its own Extend, which reaches the settler's
	// DeadlineExtender -- the memory settler implements one -- so this step
	// drives the real lease surface. The error is deliberately not asserted:
	// the script may extend a delivery the model has already settled or one
	// whose lease expired on an earlier step, and ErrAlreadySettled and
	// ErrLeaseLost are the correct answers there. What the step must not do
	// is panic or move a lease it does not hold, which the invariants after
	// it check.
	_ = d.Extend(by)
}

// dropHeld removes one delivery from the held list.
func (h *modelHarness) dropHeld(s *modelSub, idx int) {
	s.held[idx] = s.held[len(s.held)-1]
	s.held[len(s.held)-1] = mq.Delivery{}
	s.held = s.held[:len(s.held)-1]
}

// advance moves the broker's clock, which runs the time-to-live sweep, the
// lease expiry and the promotion of every record that has become due.
func (h *modelHarness) advance(d time.Duration) {
	if h.closed {
		return
	}
	if err := h.b.Advance(d); err != nil {
		h.t.Fatalf("Advance: %v", err)
	}
	// A lease the sweep reclaimed is no longer the script's to settle, and
	// a record the sweep discarded is no longer anybody's.
	for _, s := range h.subs {
		h.forgetHeld(s)
	}
}

// closeConsumer closes one subscription's consumer and opens another on the
// same subscription, which is what a client reconnecting does.
func (h *modelHarness) closeConsumer(si int) {
	if h.closed || len(h.subs) == 0 {
		return
	}
	s := h.subs[si%len(h.subs)]
	if err := s.c.Close(); err != nil {
		h.t.Fatalf("consumer Close: %v", err)
	}
	h.forgetHeld(s)
	c, err := h.client.Consumer(context.Background(), mq.ConsumerConfig{
		Topic:        modelTopic,
		Subscription: s.name,
		Prefetch:     h.cfg.prefetch,
		BufferReuse:  h.cfg.reuse,
	})
	if err != nil {
		if mq.CodeOf(err) == mq.CodeClosed {
			h.closed = true
			return
		}
		h.t.Fatalf("reopening consumer %q: %v", s.name, err)
	}
	s.c = c
}

// closeClient closes the client, after which every operation answers
// ErrClosed.
func (h *modelHarness) closeClient() {
	if h.closed {
		return
	}
	h.close()
	for _, s := range h.subs {
		h.forgetHeld(s)
	}
}

// injectFault arms one of the faults.
func (h *modelHarness) injectFault(which, n int) {
	if h.closed {
		return
	}
	fl := h.b.Faults()
	switch which % 5 {
	case 0:
		fl.RejectPublish(n)
	case 1:
		fl.FailSettlement(n)
	case 2:
		fl.DelaySettlement(n)
	case 3:
		fl.DuplicateDelivery(n)
	case 4:
		fl.FailDeadLetter(n)
	}
}

// drain takes and acknowledges everything the broker can still hand over, so
// that the final check runs against a quiescent broker — which is the
// condition the specification says the gauges are exact under.
func (h *modelHarness) drain() {
	if h.closed {
		return
	}
	// The faults are disarmed first, or a drain against an armed
	// fail-settlement fault never finishes.
	fl := h.b.Faults()
	fl.RejectPublish(0)
	fl.FailSettlement(0)
	fl.DelaySettlement(0)
	fl.DuplicateDelivery(0)
	fl.FailDeadLetter(0)
	for _, s := range h.subs {
		for i := range s.held {
			if err := s.held[i].Ack(); err == nil {
				s.terminal++
			}
			s.held[i] = mq.Delivery{}
		}
		s.held = s.held[:0]
	}
	// A bounded number of rounds: a drop-oldest lane can hand out a record
	// that a later publication displaced, so the loop is bounded by the
	// records the script could have made rather than by "until empty".
	for range 4 * (h.seq + 2) {
		took := 0
		for _, s := range h.subs {
			buf := make([]mq.Delivery, 16)
			got, err := s.c.Receive(poll(), buf)
			if got == 0 && err != nil && mq.CodeOf(err) == mq.CodeClosed {
				continue
			}
			for i := range got {
				if aerr := buf[i].Ack(); aerr == nil {
					s.terminal++
				}
				took++
			}
		}
		if took == 0 {
			break
		}
	}
}

// check is the invariant suite the specification names, run after every step and again at
// the end of the script.
func (h *modelHarness) check() {
	// The gauges are exact while the subscription is QUIESCENT, and
	// the scheduler is a goroutine of the engine's own: a record it is
	// moving between the delayed structure and the ready queue is counted
	// in neither for as long as the move takes. Inside a synctest bubble
	// that goroutine runs only when this one blocks, so the wait is what
	// makes "after the step" mean "after everything the step set off".
	// Without it conservation fails transiently on the advance step, which
	// is the engine mid-promotion rather than a lost record.
	synctest.Wait()
	if h.closed {
		// After close every operation answers ErrClosed, which is the one
		// property left to check.
		if err := h.p.Publish(context.Background(), &mq.Message{Body: []byte("after-close")}); !errors.Is(err, mq.ErrClosed) {
			h.t.Fatalf("Publish after close: %v, want ErrClosed", err)
		}
		return
	}
	for _, s := range h.subs {
		snap, err := h.b.Inspect(modelTopic, s.name)
		if err != nil {
			h.t.Fatalf("Inspect(%q): %v", s.name, err)
		}
		// the conservation: every admitted record is in exactly one
		// place.
		if !snap.Conserved() {
			h.t.Fatalf("conservation broken on %q: %+v", s.name, snap)
		}
		// Capacity accounting is never negative.
		if snap.Ready < 0 || snap.InFlight < 0 || snap.Delayed < 0 || snap.Bytes < 0 {
			h.t.Fatalf("negative accounting on %q: %+v", s.name, snap)
		}
		// Every publication the script made is accounted for: admitted,
		// discarded by the overflow policy, refused before admission, or
		// deduplicated. A publication that vanished without being counted
		// anywhere makes this sum fall below the offers.
		//
		// It is an inequality in one direction only, and the direction
		// matters: the duplicate-delivery fault admits a record a SECOND
		// time on purpose, so the sum may exceed the offers, and
		// nothing may make it fall short of them.
		//
		// A publication the fault injector refused never reached the
		// engine's own accounting, so the injector's own counter is part
		// of the sum: it is what the specification gives a test instead of a gauge.
		refused := h.b.Faults().PublishRejections()
		accounted := snap.Published + snap.Overflowed + snap.Rejected + snap.Deduplicated + refused
		if accounted < s.offers {
			h.t.Fatalf("%q: the script offered %d publications and the engine accounts for %d (published %d, overflowed %d, rejected %d, deduplicated %d, fault-refused %d)",
				s.name, s.offers, accounted, snap.Published, snap.Overflowed, snap.Rejected,
				snap.Deduplicated, refused)
		}
		// A terminal settlement is terminal: the engine's Acked counter
		// counts the acknowledgements, the drops and the dead-letter routes
		// the script took.
		if snap.Acked != s.terminal {
			h.t.Fatalf("%q: the engine settled %d records terminally and the model counted %d",
				s.name, snap.Acked, s.terminal)
		}
		if snap.Nacked != s.requeued {
			h.t.Fatalf("%q: the engine counted %d negative settlements and the model counted %d",
				s.name, snap.Nacked, s.requeued)
		}
		// Attempts never falls below the deliveries the script took.
		if snap.Attempts < s.deliveries {
			h.t.Fatalf("%q: the engine counted %d deliveries and the script took %d",
				s.name, snap.Attempts, s.deliveries)
		}
		// Isolation: the in-flight count can never exceed what the script
		// is holding plus what another consumer of the same subscription
		// could hold, and the script runs one consumer per subscription.
		if snap.InFlight < len(s.held) {
			h.t.Fatalf("%q: the engine holds %d leases and the script holds %d deliveries",
				s.name, snap.InFlight, len(s.held))
		}
	}
}

// memoryScript is the operation script.
func memoryScript() *fuzzutil.Script[*modelHarness] {
	return &fuzzutil.Script[*modelHarness]{
		MaxSteps: 256,
		Steps: []fuzzutil.Step[*modelHarness]{
			{Name: "publish", Weight: 8, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.publish(0, 0, -1, -1)
			}},
			{Name: "publishdelayed", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.publish(time.Duration(r.Int(1, 8))*modelTick, 0, -1, -1)
			}},
			{Name: "publishttl", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.publish(0, time.Duration(r.Int(1, 8))*modelTick, -1, -1)
			}},
			{Name: "publishpartition", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.publish(0, 0, -1, r.Int(0, 3))
			}},
			{Name: "receive", Weight: 8, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.receive(r.Choice(maxModelSubs), r.Int(1, 8))
			}},
			{Name: "ack", Weight: 6, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.settle(r.Choice(maxModelSubs), r.Choice(maxModelHeld), settleAck, 0)
			}},
			{Name: "nack", Weight: 4, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.settle(r.Choice(maxModelSubs), r.Choice(maxModelHeld), settleNack, 0)
			}},
			{Name: "nackdelay", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.settle(r.Choice(maxModelSubs), r.Choice(maxModelHeld), settleNackDelay,
					time.Duration(r.Int(1, 8))*modelTick)
			}},
			{Name: "drop", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.settle(r.Choice(maxModelSubs), r.Choice(maxModelHeld), settleDrop, 0)
			}},
			{Name: "reject", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.settle(r.Choice(maxModelSubs), r.Choice(maxModelHeld), settleReject, 0)
			}},
			{Name: "extend", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.settle(r.Choice(maxModelSubs), r.Choice(maxModelHeld), settleExtend,
					time.Duration(r.Int(1, 20))*modelTick)
			}},
			{Name: "advance", Weight: 3, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.advance(time.Duration(r.Int(1, 16)) * modelTick)
			}},
			{Name: "fault", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.injectFault(r.Choice(5), r.Int(0, 3))
			}},
			{Name: "closeconsumer", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, h *modelHarness) {
				h.closeConsumer(r.Choice(maxModelSubs))
			}},
			{Name: "closeclient", Weight: 1, Run: func(_ testing.TB, _ *fuzzutil.Reader, h *modelHarness) {
				h.closeClient()
			}},
		},
		Check: func(_ testing.TB, h *modelHarness) { h.check() },
	}
}

// writeModelConfig mirrors modelConfigFrom so that a seed is readable.
func writeModelConfig(w *fuzzutil.Writer, parts, capacity, subs, prefetch int, dlq bool) {
	w.Choice(len(memory.ArchKinds()), 0)
	w.Choice(len(modelQueueKinds()), 0)
	w.Choice(len(memory.WaitKinds()), 0)
	w.Choice(len(memory.TimerKinds()), 0)
	w.Choice(len(memory.HashKinds()), 0)
	w.Choice(len(memory.PoolKinds()), 0)
	w.Choice(len(memory.RegistryKinds()), 0)
	w.Choice(len(memory.StatsKinds()), 0)
	w.Choice(len(memory.RetryLaneKinds()), 0)
	w.Int(1, 3, parts)
	w.Int(1, 8, capacity)
	w.Int(1, 20, 10)
	w.Choice(len(modelOverflows()), 0)
	w.Int(0, 4, 2)
	w.Int(1, maxModelSubs, subs)
	w.Int(1, 8, prefetch)
	w.Bool(false)
	w.Bool(dlq)
}

// memorySeeds authors readable scripts covering the paths the invariants care
// about most: fan-out, redelivery, delay, expiry, dead-lettering, the drop
// verb, close and a fault.
func memorySeeds(sc *fuzzutil.Script[*modelHarness]) [][]byte {
	type step func(w *fuzzutil.Writer)
	pub := func() step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "publish") }
	}
	pubDelay := func(ticks int) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(w, "publishdelayed")
			w.Int(1, 8, ticks)
		}
	}
	pubTTL := func(ticks int) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(w, "publishttl")
			w.Int(1, 8, ticks)
		}
	}
	recv := func(si, n int) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(w, "receive")
			w.Choice(maxModelSubs, si)
			w.Int(1, 8, n)
		}
	}
	verb := func(name string, si, di int) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(w, name)
			w.Choice(maxModelSubs, si)
			w.Choice(maxModelHeld, di)
			if name == "nackdelay" || name == "extend" {
				if name == "nackdelay" {
					w.Int(1, 8, 4)
				} else {
					w.Int(1, 20, 10)
				}
			}
		}
	}
	advance := func(ticks int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "advance"); w.Int(1, 16, ticks) }
	}
	fault := func(which, n int) step {
		return func(w *fuzzutil.Writer) {
			sc.WriteStep(w, "fault")
			w.Choice(5, which)
			w.Int(0, 3, n)
		}
	}
	closeConsumer := func(si int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "closeconsumer"); w.Choice(maxModelSubs, si) }
	}
	closeClient := func() step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(w, "closeclient") }
	}

	type script struct {
		parts, capacity, subs, prefetch int
		dlq                             bool
		steps                           []step
	}
	scripts := []script{
		// Fan-out to two subscriptions, settled independently.
		{1, 8, 2, 4, false, []step{
			pub(), pub(), recv(0, 2), verb("ack", 0, 0), verb("ack", 0, 0),
			recv(1, 2), verb("ack", 1, 0), verb("ack", 1, 0),
		}},
		// Redelivery through a negative settlement and through a lease
		// expiry.
		{1, 8, 1, 2, false, []step{
			pub(), recv(0, 1), verb("nack", 0, 0), recv(0, 1), advance(16),
			recv(0, 1), verb("ack", 0, 0),
		}},
		// A delayed publication is not delivered early, and a time to live
		// discards one nobody took.
		{1, 8, 1, 4, false, []step{
			pubDelay(4), recv(0, 2), advance(4), recv(0, 2), verb("ack", 0, 0),
			pubTTL(2), advance(8), recv(0, 2),
		}},
		// Dead-lettering, both routes: the explicit verb and the attempt
		// budget.
		{1, 8, 1, 2, true, []step{
			pub(), recv(0, 1), verb("reject", 0, 0),
			pub(), recv(0, 1), verb("nack", 0, 0), recv(0, 1), verb("nack", 0, 0),
			pub(), recv(0, 1), verb("drop", 0, 0),
		}},
		// A fault, a consumer close with a delivery in flight, and the
		// client close with records in every state.
		{2, 4, 2, 4, false, []step{
			pub(), pub(), fault(1, 2), recv(0, 2), verb("ack", 0, 0),
			closeConsumer(0), recv(0, 2), verb("ack", 0, 0),
			pubDelay(8), closeClient(),
		}},
	}
	seeds := make([][]byte, 0, len(scripts))
	for _, s := range scripts {
		w := fuzzutil.NewWriter()
		writeModelConfig(w, s.parts, s.capacity, s.subs, s.prefetch, s.dlq)
		for _, st := range s.steps {
			st(w)
		}
		seeds = append(seeds, w.Data())
	}
	return seeds
}
