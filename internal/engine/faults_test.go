package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
)

// fakeEngine builds an engine on a controllable clock, which is what
// Faults.Advance needs.
func fakeEngine(t *testing.T, ks KindSet, top TopologyConfig) (*Engine[int64], *clock.Fake) {
	t.Helper()
	fake := clock.NewFake(time.Unix(0, 0))
	e, err := New(Config[int64]{
		Kinds: ks, Clock: fake, Clone: func(v int64) int64 { return v }, DefaultTopology: top,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e, fake
}

// TestFaultRejectPublish: the fault refuses exactly n publications and the
// payload never leaves the caller.
func TestFaultRejectPublish(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, CapacityMessages: 8})
	top, sub, _ := queueSub(t, e)
	released := 0
	send := func() error {
		return top.Publish(context.Background(), Publish[int64]{
			Payload: 1, Size: 1, Mode: CopyOwned, Release: func(int64) { released++ },
		})
	}
	e.Faults().RejectPublish(2)
	for i := range 2 {
		if err := send(); !errors.Is(err, ErrPublishRejected) {
			t.Fatalf("publication %d = %v, want ErrPublishRejected", i, err)
		}
	}
	if err := send(); err != nil {
		t.Fatalf("the third publication = %v, want nil", err)
	}
	if released != 0 {
		t.Fatalf("a refused publication released %d payloads; ownership never left the caller", released)
	}
	if got := e.Faults().PublishRejections(); got != 2 {
		t.Fatalf("PublishRejections = %d, want 2", got)
	}
	if got := sub.Inspect().Published; got != 1 {
		t.Fatalf("the subscription holds %d publications, want 1", got)
	}
	// A negative arm count refuses everything until it is disarmed.
	e.Faults().RejectPublish(-1)
	for range 3 {
		if err := send(); !errors.Is(err, ErrPublishRejected) {
			t.Fatalf("armed for ever, got %v", err)
		}
	}
	e.Faults().RejectPublish(0)
	if err := send(); err != nil {
		t.Fatalf("after disarming: %v", err)
	}
}

// TestFaultFailSettlement: a refused settlement leaves the lease intact and
// the caller may settle again.
func TestFaultFailSettlement(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, CapacityMessages: 8})
	top, sub, c := queueSub(t, e)
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
		t.Fatal(err)
	}
	dst := make([]Claim[int64], 1)
	if _, err := c.Fetch(poll(t), dst); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	e.Faults().FailSettlement(1)
	if err := c.Ack(dst[0].Env, dst[0].Gen); !errors.Is(err, ErrSettlementFailed) {
		t.Fatalf("Ack under the fault = %v, want ErrSettlementFailed", err)
	}
	if snap := sub.Inspect(); snap.InFlight != 1 || snap.Acked != 0 {
		t.Fatalf("a refused settlement changed the state: %+v", snap)
	}
	if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatalf("the retry after a refused settlement: %v", err)
	}
	if snap := sub.Inspect(); snap.Acked != 1 || !snap.Conserved() {
		t.Fatalf("after the retry: %+v", snap)
	}
	if got := e.Faults().SettlementFailures(); got != 1 {
		t.Fatalf("SettlementFailures = %d", got)
	}
}

// TestFaultDelaySettlement: a settlement delayed past its lease finds the
// delivery revoked and the record back in its lane.
func TestFaultDelaySettlement(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, CapacityMessages: 8})
	top, sub, c := queueSub(t, e)
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
		t.Fatal(err)
	}
	dst := make([]Claim[int64], 1)
	if _, err := c.Fetch(poll(t), dst); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	e.Faults().DelaySettlement(1)
	if err := c.Ack(dst[0].Env, dst[0].Gen); !errors.Is(err, errcode.ErrLeaseLost) {
		t.Fatalf("a delayed settlement = %v, want ErrLeaseLost", err)
	}
	snap := sub.Inspect()
	if snap.InFlight != 0 || snap.Ready != 1 || snap.Acked != 0 || !snap.Conserved() {
		t.Fatalf("after the delayed settlement: %+v", snap)
	}
	if got := e.Faults().SettlementDelays(); got != 1 {
		t.Fatalf("SettlementDelays = %d", got)
	}
}

// TestFaultDuplicateDelivery: the fault admits the record a second time, so a
// consumer receives it twice, and the accounting stays conserved.
func TestFaultDuplicateDelivery(t *testing.T) {
	// The fault is a documented no-op under a shared partition log, which
	// hands each slot to each cursor exactly once and has no second envelope
	// to give (Faults.DuplicateDelivery), so this pins an architecture that
	// has one.
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 8})
	top, sub, c := queueSub(t, e)
	released := 0
	if err := top.Publish(context.Background(), Publish[int64]{
		Payload: 5, Size: 1, Mode: CopyOwned, Release: func(int64) { released++ },
	}); err != nil {
		t.Fatal(err)
	}
	e.Faults().DuplicateDelivery(1)
	dst := make([]Claim[int64], 2)
	// One claim at a time: the duplicate is admitted while the first claim
	// is being built and is deliverable at once, so a wider fetch would take
	// both in one call.
	n, err := c.Fetch(poll(t), dst[:1])
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("the first fetch took %d records", n)
	}
	if got := e.Faults().Duplicates(); got != 1 {
		t.Fatalf("Duplicates = %d, want 1", got)
	}
	if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatal(err)
	}
	if released != 0 {
		t.Fatal("the payload was released while the duplicate still held it")
	}
	n, err = c.Fetch(poll(t), dst[:1])
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n != 1 || dst[0].Payload != 5 {
		t.Fatalf("the duplicate was not delivered: %d records", n)
	}
	if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatal(err)
	}
	if released != 1 {
		t.Fatalf("the payload was released %d times, want 1", released)
	}
	snap := sub.Inspect()
	if snap.Published != 2 || snap.Acked != 2 || !snap.Conserved() {
		t.Fatalf("a duplicate broke the accounting: %+v", snap)
	}
}

// TestFaultReorder: the fault reverses a claim batch on a lane that promises
// no order, and never on one that does.
func TestFaultReorder(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, CapacityMessages: 8})
	top, _, c := queueSub(t, e)
	for i := range 4 {
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: int64(i), Size: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	e.Faults().Reorder(1)
	dst := make([]Claim[int64], 4)
	n, err := c.Fetch(poll(t), dst)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("fetched %d records", n)
	}
	got := make([]int64, n)
	for i := range n {
		got[i] = dst[i].Payload
	}
	want := []int64{3, 2, 1, 0}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("reordered batch %v, want %v", got, want)
		}
	}
	if e.Faults().Reorders() != 1 {
		t.Fatalf("Reorders = %d", e.Faults().Reorders())
	}
	// A strict-ordering topic is never reordered: the fault would break a
	// promise instead of exercising one.
	strict := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{
		Partitions: 1, CapacityMessages: 8, Ordering: OrderingStrict,
	})
	st, _, sc := queueSub(t, strict)
	for i := range 4 {
		if err := st.Publish(context.Background(), Publish[int64]{Payload: int64(i), Size: 1}); err != nil {
			t.Fatal(err)
		}
	}
	strict.Faults().Reorder(-1)
	n, err = sc.Fetch(poll(t), dst)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for i := range n {
		if dst[i].Payload != int64(i) {
			t.Fatalf("a strict-ordering lane was reordered: %v at %d", dst[i].Payload, i)
		}
	}
	if strict.Faults().Reorders() != 0 {
		t.Fatalf("the fault fired on a strict lane: %d", strict.Faults().Reorders())
	}
}

// TestFaultCloseConsumersAndReconnect: the two fault-driven ways of taking
// every lease back.
func TestFaultCloseConsumersAndReconnect(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 2, CapacityMessages: 8})
	top, sub, c := queueSub(t, e)
	for part := range 2 {
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: int64(part), Size: 1, Partition: part,
		}); err != nil {
			t.Fatal(err)
		}
	}
	dst := make([]Claim[int64], 4)
	if _, err := c.Fetch(poll(t), dst); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if sub.Inspect().InFlight != 2 {
		t.Fatalf("expected two leases: %+v", sub.Inspect())
	}
	before := sub.assignmentVersion()
	if n := e.Faults().Reconnect(); n != 1 {
		t.Fatalf("Reconnect rebalanced %d subscriptions, want 1", n)
	}
	if after := sub.assignmentVersion(); after <= before {
		t.Fatalf("Reconnect did not bump the generation: %d then %d", before, after)
	}
	snap := sub.Inspect()
	if snap.InFlight != 0 || snap.Ready != 2 || !snap.Conserved() {
		t.Fatalf("after a reconnect: %+v", snap)
	}
	// The consumer is still attached; CloseConsumers detaches it.
	if _, err := c.Fetch(poll(t), dst); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n := e.Faults().CloseConsumers(); n != 1 {
		t.Fatalf("CloseConsumers closed %d consumers, want 1", n)
	}
	if got := sub.Inspect().ActiveConsumers; got != 0 {
		t.Fatalf("%d consumers are still attached", got)
	}
	if _, err := c.Fetch(poll(t), dst); !errors.Is(err, errcode.ErrClosed) {
		t.Fatalf("a closed consumer fetched: %v", err)
	}
	if snap := sub.Inspect(); snap.InFlight != 0 || snap.Ready != 2 {
		t.Fatalf("a closed consumer kept its leases: %+v", snap)
	}
}

// TestFaultAdvance: the deterministic clock fault promotes delayed records,
// expires leases and trims retention, and refuses an engine it cannot drive.
func TestFaultAdvance(t *testing.T) {
	e, _ := fakeEngine(t, KindSet{Arch: featureArch}, TopologyConfig{
		Partitions: 1, CapacityMessages: 8, AckDeadline: 10 * time.Millisecond,
	})
	top, err := e.CreateTopic("t", TopologyConfig{
		Kind: TopologyStream, Retention: Retention{MaxAge: 20 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if err := top.Publish(context.Background(), Publish[int64]{
		Payload: 2, Size: 1, Delay: 5 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	dst := make([]Claim[int64], 4)
	if _, err := c.Fetch(poll(t), dst); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if snap := sub.Inspect(); snap.InFlight != 1 || snap.Delayed != 1 {
		t.Fatalf("before the advance: %+v", snap)
	}
	if err := e.Faults().Advance(11 * time.Millisecond); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	snap := sub.Inspect()
	if snap.InFlight != 0 || snap.Ready != 2 || snap.Expired != 1 || !snap.Conserved() {
		t.Fatalf("after the advance: %+v", snap)
	}
	if err := e.Faults().Advance(20 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if n, _ := top.Retained(); n != 0 {
		t.Fatalf("the age bound left %d records retained", n)
	}
	// An engine on the real clock cannot be driven, and says so.
	real := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
	if err := real.Faults().Advance(time.Second); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Advance on a real clock = %v, want ErrInvalidConfig", err)
	}
}

// TestFaultTrimRetention reports how many records the age bound released.
func TestFaultTrimRetention(t *testing.T) {
	e, fake := fakeEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 8})
	top, err := e.CreateTopic("t", TopologyConfig{
		Kind: TopologyStream, Retention: Retention{MaxAge: 10 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := top.Subscribe("s", SubscriptionConfig{}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if n := e.Faults().TrimRetention(); n != 0 {
		t.Fatalf("trimmed %d records before the age bound", n)
	}
	fake.Advance(11 * time.Millisecond)
	if n := e.Faults().TrimRetention(); n != 3 {
		t.Fatalf("trimmed %d records, want 3", n)
	}
}

// TestRecorder: the decorator keeps what was published, when and what the
// engine answered, including the publications a fault refused.
func TestRecorder(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, CapacityMessages: 4})
	top, _, _ := queueSub(t, e)
	r := NewRecorder[int64](0)
	ctx := context.Background()
	for i := range 3 {
		if err := r.Publish(ctx, top, Publish[int64]{
			Payload: int64(i), Size: 1, Key: []byte("k"), Partition: -1,
		}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	e.Faults().RejectPublish(1)
	if err := r.Publish(ctx, top, Publish[int64]{Payload: 9, Size: 1}); !errors.Is(err, ErrPublishRejected) {
		t.Fatalf("the refused publication returned %v", err)
	}
	recs := r.Records()
	if len(recs) != 4 || r.Seen() != 4 {
		t.Fatalf("recorded %d of 4 publications (seen %d)", len(recs), r.Seen())
	}
	for i := range 3 {
		if recs[i].Payload != int64(i) || recs[i].Err != nil || recs[i].Topic != "t" {
			t.Fatalf("record %d = %+v", i, recs[i])
		}
		if recs[i].Partition != -1 || string(recs[i].Key) != "k" || recs[i].Mode == CopyDefault {
			t.Fatalf("record %d lost its publication: %+v", i, recs[i])
		}
	}
	if !errors.Is(recs[3].Err, ErrPublishRejected) {
		t.Fatalf("the refusal was recorded as %v", recs[3].Err)
	}
	// Every field the specification asks a Recorder to keep round-trips, on an engine
	// whose topology offers all of them: without this, zeroing Subject,
	// DedupKey, Priority, At and Size leaves the suite green.
	fake := clock.NewFake(time.Unix(0, 1234))
	fe, err := New(Config[int64]{
		Kinds: KindSet{Arch: featureArch},
		Clock: fake, Clone: func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{Partitions: 1, CapacityMessages: 8},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = fe.Close() })
	ftop, err := fe.CreateTopic("full", TopologyConfig{
		Wildcards: true, Priority: true, DedupWindow: time.Hour,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := ftop.Subscribe("s", SubscriptionConfig{}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	full := NewRecorder[int64](0)
	if err := full.Publish(ctx, ftop, Publish[int64]{
		Payload: 5, Size: 64, Subject: "a.b", DedupKey: []byte("dk"),
		Priority: PriorityLevels - 1, Partition: 0, Mode: CopyOwned,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := full.Records()[0]
	switch {
	case got.Subject != "a.b":
		t.Fatalf("Recorded.Subject = %q, want a.b", got.Subject)
	case string(got.DedupKey) != "dk":
		t.Fatalf("Recorded.DedupKey = %q, want dk", got.DedupKey)
	case got.Priority != PriorityLevels-1:
		t.Fatalf("Recorded.Priority = %d, want %d", got.Priority, PriorityLevels-1)
	case got.Size != 64:
		t.Fatalf("Recorded.Size = %d, want 64", got.Size)
	case !got.At.Equal(time.Unix(0, 1234)):
		t.Fatalf("Recorded.At = %v, want the engine clock reading %v", got.At, time.Unix(0, 1234))
	case got.Mode != CopyOwned:
		t.Fatalf("Recorded.Mode = %v, want CopyOwned", got.Mode)
	}
	// The limit keeps the most recent window.
	lim := NewRecorder[int64](2)
	for i := range 5 {
		if err := lim.Publish(ctx, top, Publish[int64]{Payload: int64(i), Size: 1}); err != nil &&
			!errors.Is(err, errcode.ErrQueueFull) {
			t.Fatalf("Publish: %v", err)
		}
	}
	if lim.Len() != 2 || lim.Seen() != 5 {
		t.Fatalf("the limited recorder holds %d of %d", lim.Len(), lim.Seen())
	}
	if got := lim.Records(); got[0].Payload != 3 || got[1].Payload != 4 {
		t.Fatalf("the limited recorder kept %v", got)
	}
	lim.Reset()
	if lim.Len() != 0 || lim.Seen() != 0 {
		t.Fatal("Reset left something behind")
	}
	// PublishBatch records every element, refusals included.
	b := NewRecorder[int64](0)
	e.Faults().RejectPublish(1)
	n, err := b.PublishBatch(ctx, top, []Publish[int64]{{Payload: 1, Size: 1}, {Payload: 2, Size: 1}})
	if n != 0 || !errors.Is(err, ErrPublishRejected) {
		t.Fatalf("PublishBatch = %d, %v", n, err)
	}
	if b.Len() != 1 {
		t.Fatalf("the batch recorded %d publications", b.Len())
	}
}

// TestARecorderBatchReportsAPartialFanOut: Topic.PublishBatch answers a batch
// the Independent fan-out admitted to only some subscriptions with
// errcode.ErrPartial once every element has been offered. The Recorder's
// batch answered nil for the same batch, so a caller that recorded its
// publications lost the one answer that says a subscription missed a record,
// although the Recorder had recorded it on the element.
func TestARecorderBatchReportsAPartialFanOut(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{
		Partitions: 1, CapacityMessages: 1, AckDeadline: -1, Fanout: Independent, Overflow: OverflowReject,
	})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for _, name := range []string{"full", "roomy"} {
		if _, err := top.Subscribe(name, SubscriptionConfig{}); err != nil {
			t.Fatalf("Subscribe(%s): %v", name, err)
		}
	}
	roomy, err := top.Subscription("roomy")
	if err != nil {
		t.Fatal(err)
	}
	c, err := roomy.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	// Fill both, then empty roomy: the next record fits roomy and not full.
	if err := top.Publish(t.Context(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
		t.Fatal(err)
	}
	for _, cl := range fetchSeqs(t, c, 1) {
		if err := c.Ack(cl.Env, cl.Gen); err != nil {
			t.Fatal(err)
		}
	}
	r := NewRecorder[int64](0)
	n, err := r.PublishBatch(t.Context(), top, []Publish[int64]{{Payload: 2, Size: 1}})
	if n != 1 || !errors.Is(err, errcode.ErrPartial) {
		t.Fatalf("Recorder.PublishBatch = (%d, %v), want (1, ErrPartial) as Topic.PublishBatch answers", n, err)
	}
	if recs := r.Records(); len(recs) != 1 || !errors.Is(recs[0].Err, errcode.ErrPartial) {
		t.Fatalf("recorded %+v, want the element's ErrPartial", recs)
	}
}

// TestWaitIdleModes: each mode waits for what it names.
func TestWaitIdleModes(t *testing.T) {
	e, _ := fakeEngine(t, KindSet{}, TopologyConfig{Partitions: 1, CapacityMessages: 8, AckDeadline: -1})
	top, sub, c := queueSub(t, e)
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if err := top.Publish(context.Background(), Publish[int64]{
		Payload: 2, Size: 1, Delay: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	dst := make([]Claim[int64], 1)
	if _, err := c.Fetch(poll(t), dst); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// A lease is outstanding: no mode is satisfied.
	for _, mode := range IdleModes() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := e.WaitIdle(ctx, mode); err == nil {
			t.Fatalf("WaitIdle(%v) reported idle with a lease outstanding", mode)
		}
	}
	if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatal(err)
	}
	// Nothing is in flight and nothing is ready, but a delayed record is
	// still held: the first two modes are satisfied, the third is not.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.WaitIdle(ctx, IdleSettled); err != nil {
		t.Fatalf("IdleSettled: %v", err)
	}
	if err := e.WaitIdle(ctx, IdleDelivered); err != nil {
		t.Fatalf("IdleDelivered: %v", err)
	}
	if err := e.WaitIdle(ctx, IdleIncludingDelayed); err == nil {
		t.Fatal("IdleIncludingDelayed reported idle with a delayed record")
	}
	// A ready record nobody has leased is what separates IdleSettled from
	// IdleDelivered: the first is "every consumer has finished what it took"
	// and is satisfied, the second is "everything deliverable is delivered"
	// and is not. Without this the two modes are indistinguishable and
	// collapsing them leaves the suite green.
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 3, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if got := sub.Inspect(); got.Ready != 1 || got.InFlight != 0 {
		t.Fatalf("want one ready, unleased record: %+v", got)
	}
	if err := e.WaitIdle(ctx, IdleSettled); err != nil {
		t.Fatalf("IdleSettled with a ready, unleased record: %v", err)
	}
	if err := e.WaitIdle(ctx, IdleDelivered); err == nil {
		t.Fatal("IdleDelivered reported idle with a ready record nobody has taken")
	}
}
