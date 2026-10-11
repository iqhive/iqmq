package kafkago_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// TestNackBatchKeepsAMixedSettlementOutcome is the settlement rule for a
// batch none of whose deliveries was settled.
//
// Both halves of the mixture are real on this driver:
//
//   - a delivery whose generation no longer matches its partition's is
//     refused by the source without a request being sent, so its outcome is
//     DEFINITE — lease lost, which the state machine returns to `active`
//     where a retry is allowed; and
//   - a nack that dead-letters performs a PRODUCE, and a produce that answers
//     REQUEST_TIMED_OUT may already have appended. The specification forbids acking the
//     original after it, and the specification puts the delivery in `settlement-unknown`,
//     which must "stay explicitly unknown".
//
// NackBatch collapsed a wholly failed batch to its FIRST error, and
// [driverkit.BatchSettler] hands one error to every delivery of the batch — so
// the delivery whose dead-letter copy may already be in the log was told its
// lease was lost. A caller reads that as "somebody else has it, forget it",
// and the record's only trace is a copy nobody knows about.
func TestNackBatchKeepsAMixedSettlementOutcome(t *testing.T) {
	topic, dlq := newTopic("mixed-settle"), newTopic("mixed-settle-dead")
	cl := cluster(t, 1, []string{topic, dlq})
	b := backendOn(t, cl, func(cfg *kq.Config) {
		cfg.NackConfig.DeadLetterTopic = dlq
		// A short retry budget: the produce is refused for as long as the
		// fault is installed, and this test only needs it to give up.
		cfg.MaxAttempts = 2
		cfg.WriteBackoffMin = 10 * time.Millisecond
		cfg.WriteBackoffMax = 20 * time.Millisecond
	})
	ctx := t.Context()

	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: topic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	for _, body := range []string{"a", "b"} {
		if err := sink.Publish(ctx, &mq.Message{Topic: topic, Body: []byte(body)}, nil); err != nil {
			t.Fatalf("Publish(%s): %v", body, err)
		}
	}
	src, err := b.OpenSource(ctx, mq.SourceConfig{
		Topic: topic, Subscription: "g-mixed-settle", Prefetch: 16,
		Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	dst := fetchN(t, ctx, src, 2)
	bs, ok := src.Settler().(mq.BatchSettler)
	if !ok {
		t.Fatal("the settler is not a BatchSettler; FeatureBatchSettle is advertised")
	}

	// Every produce to the dead-letter topic times out for as long as the
	// fault is installed: REQUEST_TIMED_OUT is the answer that may or may
	// not have appended.
	code := *kerr.RequestTimedOut
	fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: dlq, Err: &code, Count: -1})
	defer fault.Remove()

	// Delivery 0 is refused locally and definitely; delivery 1 reaches the
	// broker and its move is ambiguous.
	dst[0].Gen++
	o := mq.NackOptions{Action: mq.NackActionDeadLetter}
	got := bs.NackBatch(ctx, []*mq.DriverDelivery{&dst[0], &dst[1]}, o)

	var be *mq.BatchError
	if !errors.As(got, &be) {
		t.Fatalf("NackBatch(lease-lost, ambiguous move) = %v (ambiguous %v), want an index-aligned *mq.BatchError: "+
			"one delivery was definitely refused and the other's outcome is unknown, and one error cannot say both",
			got, mq.IsAmbiguous(got))
	}
	if len(be.Errs) != 2 {
		t.Fatalf("BatchError.Errs holds %d results for 2 deliveries", len(be.Errs))
	}
	if !errors.Is(be.Errs[0], mq.ErrLeaseLost) || mq.IsAmbiguous(be.Errs[0]) {
		t.Errorf("Errs[0] = %v (ambiguous %v), want a definite lease-lost", be.Errs[0], mq.IsAmbiguous(be.Errs[0]))
	}
	if !mq.IsAmbiguous(be.Errs[1]) {
		t.Errorf("Errs[1] = %v (code %v), want an ambiguous settlement: the dead-letter copy may already be in the log",
			be.Errs[1], mq.CodeOf(be.Errs[1]))
	}
	if !mq.IsAmbiguous(got) {
		t.Errorf("NackBatch = %v: IsAmbiguous is false although one delivery's outcome is unknown", got)
	}

	// The collapse that is still right, and the mutation guard: with the
	// generation restored both moves are ambiguous, so every delivery has ONE
	// outcome and the single error is the answer. A fix that always built a
	// *BatchError would fail here.
	dst[0].Gen--
	uniform := bs.NackBatch(ctx, []*mq.DriverDelivery{&dst[0], &dst[1]}, o)
	if errors.As(uniform, &be) {
		t.Fatalf("NackBatch(ambiguous, ambiguous) = %v, want the single error: both deliveries have the same outcome", uniform)
	}
	if !mq.IsAmbiguous(uniform) {
		t.Fatalf("NackBatch(ambiguous, ambiguous) = %v, want an ambiguous answer", uniform)
	}
}

// fetchN fetches exactly n deliveries, retrying until they have all arrived.
func fetchN(t *testing.T, ctx context.Context, src mq.Source, n int) []mq.DriverDelivery {
	t.Helper()
	got := make([]mq.DriverDelivery, 0, n)
	deadline := time.Now().Add(20 * time.Second)
	for len(got) < n && time.Now().Before(deadline) {
		dst := make([]mq.DriverDelivery, n-len(got))
		fctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		k, err := src.Fetch(fctx, dst)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Fetch: %v", err)
		}
		got = append(got, dst[:k]...)
	}
	if len(got) != n {
		t.Fatalf("fetched %d of %d published messages", len(got), n)
	}
	return got
}
