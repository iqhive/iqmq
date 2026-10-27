package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// Every fault in this file is checked by its COUNTER as well as by its
// effect. The specification gives each fault a counter for exactly this reason: a test
// that asserts an outcome without asserting that the fault fired passes for
// a fault that did nothing, which is the check-that-cannot-fail this
// repository keeps finding.

// TestRejectPublishRefusesBeforeAdmission pins the first fault the specification names.
func TestRejectPublishRefusesBeforeAdmission(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	b.Faults().RejectPublish(2)
	for i := range 2 {
		if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); mq.CodeOf(err) != mq.CodeUnavailable {
			t.Fatalf("publish %d: %v (code %v), want CodeUnavailable", i, err, mq.CodeOf(err))
		}
	}
	if got := b.Faults().PublishRejections(); got != 2 {
		t.Fatalf("the fault fired %d times, want 2", got)
	}
	// Armed for two and disarmed after them: the third publication is
	// admitted.
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("the third publish: %v", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Published != 1 {
		t.Fatalf("the engine admitted %d records, want the 1 the fault let through", snap.Published)
	}
}

// TestFailSettlementLeavesTheLeaseIntact pins the definite refusal: the
// caller may settle the same delivery again.
func TestFailSettlementLeavesTheLeaseIntact(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ds := receiveN(t, ctx, c, 1)
	b.Faults().FailSettlement(1)
	if err := ds[0].Ack(); mq.CodeOf(err) != mq.CodeUnavailable {
		t.Fatalf("a refused settlement answered %v (code %v)", err, mq.CodeOf(err))
	}
	if got := b.Faults().SettlementFailures(); got != 1 {
		t.Fatalf("the fault fired %d times, want 1", got)
	}
	if ds[0].Settled() {
		t.Fatal("a refused settlement reported the delivery settled")
	}
	// The lease survived, so the same delivery settles on the retry.
	if err := ds[0].Ack(); err != nil {
		t.Fatalf("the retry: %v", err)
	}
	if !ds[0].Settled() {
		t.Fatal("the retry did not settle the delivery")
	}
}

// TestDelaySettlementReportsLeaseLost pins the deterministic form of the specification's
// settlement-races-expiry window: the lease is revoked first and the
// settlement then finds the delivery gone.
func TestDelaySettlementReportsLeaseLost(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ds := receiveN(t, ctx, c, 1)
	b.Faults().DelaySettlement(1)
	if err := ds[0].Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a settlement that arrived too late answered %v, want ErrLeaseLost", err)
	}
	if got := b.Faults().SettlementDelays(); got != 1 {
		t.Fatalf("the fault fired %d times, want 1", got)
	}
	// The record went back to its lane, so the message was not lost.
	again := receiveN(t, ctx, c, 1)
	if again[0].Attempt != 2 {
		t.Fatalf("the redelivery reports attempt %d, want 2", again[0].Attempt)
	}
	if err := again[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestDuplicateDeliveryProducesASecondDelivery pins the duplicate an
// at-least-once broker makes when an acknowledgement is lost, and the
// documented no-op under a shared partition log.
func TestDuplicateDeliveryProducesASecondDelivery(t *testing.T) {
	ctx := context.Background()
	for _, arch := range memory.ArchKinds() {
		t.Run("arch="+arch.String(), func(t *testing.T) {
			client, b := memory.NewForTest(t, memory.TestEngine(memory.EngineConfig{Arch: arch}))
			c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			b.Faults().DuplicateDelivery(1)
			first := receiveN(t, ctx, c, 1)
			if err := first[0].Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			shared := arch.Shared()
			if got := b.Faults().Duplicates(); (got > 0) == shared {
				t.Fatalf("the fault produced %d duplicates under a shared=%v architecture", got, shared)
			}
			if shared {
				// The fault is a documented no-op here, and Duplicates is
				// how a test learns that rather than assuming it fired.
				return
			}
			second := receiveN(t, ctx, c, 1)
			if string(second[0].Body) != "x" {
				t.Fatalf("the duplicate carries %q", second[0].Body)
			}
			if err := second[0].Ack(); err != nil {
				t.Fatalf("Ack of the duplicate: %v", err)
			}
		})
	}
}

// TestCloseConsumersReturnsTheirLeases pins the close policy reached
// through the fault: every consumer is detached and its records go back.
func TestCloseConsumersReturnsTheirLeases(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ds := receiveN(t, ctx, c, 1)
	if got := b.Faults().CloseConsumers(); got != 1 {
		t.Fatalf("the fault closed %d consumers, want 1", got)
	}
	if err := ds[0].Ack(); err == nil {
		t.Fatal("a delivery of a closed consumer was acknowledged")
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Ready != 1 || snap.InFlight != 0 {
		t.Fatalf("snapshot %+v, want the record back in its lane", snap)
	}
	if _, err := c.Receive(ctx, make([]mq.Delivery, 1)); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Receive on a consumer the fault closed: %v, want ErrClosed", err)
	}
}

// TestReconnectRevokesTheLeasesOfTheOldGeneration pins the versioned
// assignment: a reconnect increments the generation, the deliveries issued
// under the old one are no longer valid, and — because a revocation is not a
// failed delivery — it never routes a record to the dead-letter topic.
//
// The error it reports is the point. A revoked lease is
// a LOST lease and not a closed consumer: nobody closed the consumer, and
// internal/settlement gives the two withdrawals different resting states
// precisely so that a caller can tell them apart. Before both rebalance
// paths took the close transition and this delivery reported ErrClosed.
func TestReconnectRevokesTheLeasesOfTheOldGeneration(t *testing.T) {
	ctx := context.Background()
	// One attempt and a dead-letter topic, so that a revocation which
	// consulted MaxAttempts would route the record instead of redelivering
	// it — which is the double-retry accounting the specification forbids.
	client, b := memory.NewForTest(t,
		memory.TestTopology(memory.TopologyConfig{MaxAttempts: 1, DeadLetterTopic: "dlq"}),
		memory.TestTopics("dlq"))
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ds := receiveN(t, ctx, c, 1)
	before, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if got := b.Faults().Reconnect(); got != 1 {
		t.Fatalf("the fault rebalanced %d subscriptions, want 1", got)
	}
	after, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if after.Assignment <= before.Assignment {
		t.Fatalf("the assignment generation went from %d to %d", before.Assignment, after.Assignment)
	}
	if after.DeadLettered != 0 {
		t.Fatalf("a reconnect dead-lettered %d records: a revocation is not a failed delivery", after.DeadLettered)
	}
	if err := ds[0].Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a settlement under the revoked generation answered %v, want ErrLeaseLost ", err)
	}
	if errors.Is(ds[0].Ack(), mq.ErrClosed) {
		t.Fatal("a revoked delivery reports a closed consumer, and nobody closed one ")
	}
	again := receiveN(t, ctx, c, 1)
	if err := again[0].Ack(); err != nil {
		t.Fatalf("Ack of the redelivery: %v", err)
	}
	final, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if !final.Conserved() || final.Acked != 1 {
		t.Fatalf("snapshot %+v, want the record acknowledged once", final)
	}
}

// TestFailDeadLetterNeverAcks is the rule on the engine's own
// dead-lettering route, which is a different route from an explicit Reject:
// the record runs out of attempts, the internal publication fails, and the
// record goes back to its lane rather than being settled.
func TestFailDeadLetterNeverAcks(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t,
		memory.TestTopology(memory.TopologyConfig{MaxAttempts: 1, DeadLetterTopic: "dlq"}),
		memory.TestTopics("dlq"))
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("poison")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	b.Faults().FailDeadLetter(1)
	ds := receiveN(t, ctx, c, 1)
	if err := ds[0].Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if got := b.Faults().DeadLetterFailures(); got != 1 {
		t.Fatalf("the fault failed %d dead-letter publications, want 1", got)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.DeadLettered != 0 {
		t.Fatalf("a failed dead-letter publication counted %d records as dead-lettered", snap.DeadLettered)
	}
	if snap.DeadLetterFailures != 1 {
		t.Fatalf("Snapshot.DeadLetterFailures is %d, want 1", snap.DeadLetterFailures)
	}
	if !snap.Conserved() {
		t.Fatalf("conservation broken: %+v", snap)
	}
}

// TestExpireRecordsAndTrimRetention pins the two sweeps the scheduler does on
// its own and a fault makes happen now.
func TestExpireRecordsAndTrimRetention(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
		Kind:      memory.TopologyStream,
		Retention: memory.Retention{MaxMessages: 8, MaxAge: time.Minute},
	}))
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("keep")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("expire")}, mq.TTL(time.Second)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Retained != 2 {
		t.Fatalf("the stream retains %d records, want 2", snap.Retained)
	}
	// The clock has not moved, so neither sweep has anything to do.
	if got := b.Faults().ExpireRecords(); got != 0 {
		t.Fatalf("the expiry sweep discarded %d records before anything expired", got)
	}
	if got := b.Faults().TrimRetention(); got != 0 {
		t.Fatalf("the retention trim released %d records before anything aged out", got)
	}
	if err := b.Advance(2 * time.Second); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	snap, err = b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Dropped != 1 {
		t.Fatalf("the time-to-live sweep dropped %d records, want 1", snap.Dropped)
	}
	if err := b.Advance(2 * time.Minute); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	snap, err = b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Retained != 0 {
		t.Fatalf("the retained log still holds %d records past its age bound", snap.Retained)
	}
}

// TestReorderNeverBreaksAPromise pins the rule that makes the reorder fault
// safe to have: it never fires on a lane whose topic asked for strict
// ordering, because a fault that broke a promise would test the test.
func TestReorderNeverBreaksAPromise(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
		Ordering: memory.OrderingStrict,
	}))
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	const n = 8
	for i := range n {
		if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte{byte(i)}}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	b.Faults().Reorder(-1)
	ds := receiveN(t, ctx, c, n)
	if got := b.Faults().Reorders(); got != 0 {
		t.Fatalf("the reorder fault fired %d times on a strictly ordered topic", got)
	}
	for i := range ds {
		if ds[i].Body[0] != byte(i) {
			t.Fatalf("a strictly ordered lane served %d at position %d", ds[i].Body[0], i)
		}
		if err := ds[i].Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	b.Faults().Reorder(0)
}

// TestRecorderKeepsWhatWasPublished pins the publisher decorator: it goes
// where a publisher goes, it records what the caller made rather than what
// the broker kept, and it records the outcome — which is what makes a
// rejected publication distinguishable from an admitted one.
func TestRecorderKeepsWhatWasPublished(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
		CapacityMessages: 1,
		Overflow:         memory.OverflowReject,
	}))
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	rec := memory.NewRecorder(p, 0, b.Clock())
	// A Recorder IS a Publisher, which is what lets it be handed to code
	// that takes one.
	var asPublisher mq.Publisher = rec
	body := []byte("first")
	if err := asPublisher.Publish(ctx, &mq.Message{Body: body}, mq.Delay(0), mq.TTL(time.Minute)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// The lane is full, so the second is refused — and recorded with its
	// refusal.
	if err := asPublisher.Publish(ctx, &mq.Message{Body: []byte("second")}); !errors.Is(err, mq.ErrQueueFull) {
		t.Fatalf("the second publish: %v, want ErrQueueFull", err)
	}
	// The caller reuses its storage the instant Publish returns, which is
	// exactly why the Recorder keeps a copy.
	copy(body, "XXXXX")
	recs := rec.Records()
	if len(recs) != 2 || rec.Len() != 2 || rec.Seen() != 2 {
		t.Fatalf("the Recorder holds %d records (Len %d, Seen %d), want 2", len(recs), rec.Len(), rec.Seen())
	}
	if got := string(recs[0].Message.Body); got != "first" {
		t.Fatalf("the first record's body is %q: the Recorder kept the caller's storage", got)
	}
	if recs[0].Err != nil {
		t.Fatalf("the first record carries %v", recs[0].Err)
	}
	if recs[0].Options.TTL != time.Minute {
		t.Fatalf("the first record's options are %+v, want the TTL the call named", recs[0].Options)
	}
	if recs[0].At.IsZero() {
		t.Fatal("the first record carries no timestamp")
	}
	if !errors.Is(recs[1].Err, mq.ErrQueueFull) {
		t.Fatalf("the second record carries %v, want the refusal", recs[1].Err)
	}
	// A batch is recorded per element, with the per-element outcome.
	ms := []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}}
	_ = asPublisher.PublishBatch(ctx, ms)
	recs = rec.Records()
	if len(recs) != 4 {
		t.Fatalf("the Recorder holds %d records after a batch of 2, want 4", len(recs))
	}
	for _, r := range recs[2:] {
		if !r.Batch {
			t.Fatalf("a batch element was recorded as a single publication: %+v", r)
		}
	}
	if recs[2].Index != 0 || recs[3].Index != 1 {
		t.Fatalf("the batch elements carry indices %d and %d", recs[2].Index, recs[3].Index)
	}
	// The limit keeps the most recent window and Seen counts everything.
	bounded := memory.NewRecorder(p, 2, b.Clock())
	for range 5 {
		_ = bounded.Publish(ctx, &mq.Message{Body: []byte("x")})
	}
	if bounded.Len() != 2 {
		t.Fatalf("a Recorder limited to 2 holds %d records", bounded.Len())
	}
	if bounded.Seen() != 5 {
		t.Fatalf("Seen is %d, want the 5 publications the limit evicted from", bounded.Seen())
	}
	rec.Reset()
	if rec.Len() != 0 || rec.Seen() != 0 {
		t.Fatalf("Reset left %d records and a count of %d", rec.Len(), rec.Seen())
	}
	if err := rec.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := rec.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	empty := memory.NewRecorder(nil, 0, nil)
	if err := empty.Publish(ctx, &mq.Message{}); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Fatalf("a Recorder over no publisher answered %v (code %v)", err, mq.CodeOf(err))
	}
}

// TestRecorderRecordsTheBoundTopic pins Recorded.Topic to the destination a
// publication went to. A bound publisher ignores Message.Topic, and the
// Recorder recorded that field: empty for a message that named no topic, and
// a topic nothing was published to for one that named another.
func TestRecorderRecordsTheBoundTopic(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	rec := memory.NewRecorder(p, 0, b.Clock())
	if err := rec.Publish(ctx, &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := rec.Publish(ctx, &mq.Message{Topic: "elsewhere", Body: []byte("y")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := rec.PublishBatch(ctx, []*mq.Message{{Topic: "elsewhere", Body: []byte("z")}}); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	for i, r := range rec.Records() {
		if r.Topic != "orders" {
			t.Fatalf("record %d names topic %q for a publication that went to %q", i, r.Topic, "orders")
		}
	}
	// The message is still recorded as the caller made it.
	if got := rec.Records()[1].Message.Topic; got != "elsewhere" {
		t.Fatalf("the recorded message's own Topic is %q, want the caller's %q", got, "elsewhere")
	}
}
