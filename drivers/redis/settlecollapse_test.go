package redis_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// errCut is what a connection that died mid-flight looks like to the driver:
// the pipeline was written and the reply never arrived. driverkit.ClassifyNet
// reads io.ErrUnexpectedEOF with sentAlready true as CodeUnavailable AND
// ambiguous, which is the classification this test turns on.
var errCut = fmt.Errorf("read tcp 127.0.0.1:6379: %w", io.ErrUnexpectedEOF)

// cutCommander is the fake server with a switch on it: once armed, every
// command fails at the transport, which is the one thing a fake that always
// answers cannot do. The specification turns on that distinction — an XACK whose round
// trip failed AFTER the command was written has an UNKNOWN outcome, and the
// driver maps it with sentAlready true for exactly that reason.
type cutCommander struct {
	*fakestreams.Server
	armed atomic.Bool
}

func (c *cutCommander) Do(ctx context.Context, cmd streams.Cmd) (streams.Value, error) {
	if c.armed.Load() {
		return streams.Value{}, errCut
	}
	return c.Server.Do(ctx, cmd)
}

func (c *cutCommander) DoMulti(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	if c.armed.Load() {
		return nil, errCut
	}
	return c.Server.DoMulti(ctx, cmds)
}

// TestAckBatchKeepsAMixedSettlementOutcome is the settlement rule for a
// batch none of whose deliveries was settled.
//
// The two halves of the mixture are both real and both reachable here:
//
//   - a delivery whose generation does not match the one in its own Ref is
//     refused by [settler.ref] without a command being sent, so its outcome
//     is DEFINITE — ErrLeaseLost, and the state machine puts it back in
//     `active`, where a retry is allowed; and
//   - an XACK whose round trip failed after the pipeline was written has an
//     UNKNOWN outcome, which the specification puts in `settlement-unknown` and requires
//     to "stay explicitly unknown".
//
// AckBatch collapsed a wholly failed batch to its FIRST error, and
// [driverkit.BatchSettler] hands a single error to every delivery of the
// batch — so the delivery whose acknowledgement may already have been applied
// was told its lease was lost. That is the misclassification the specification forbids,
// and a caller acting on it reprocesses a message the broker has already
// forgotten.
func TestAckBatchKeepsAMixedSettlementOutcome(t *testing.T) {
	srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	cmder := &cutCommander{Server: srv}
	b, err := rd.BackendWithCommander(context.Background(), cmder, driverkit.ClientOwned, rd.Config{
		AckDeadline:   testLease,
		ClaimInterval: time.Hour,
		Block:         time.Millisecond,
		AutoCreate:    true,
		Clock:         fakeClock{srv},
	})
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	ctx := context.Background()

	src, err := b.OpenSource(ctx, mq.SourceConfig{
		Topic: testTopic, Subscription: testSub, Prefetch: 16,
		AckDeadline: testLease, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: testTopic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })

	for _, body := range []string{"a", "b"} {
		if err := sink.Publish(ctx, &mq.Message{Topic: testTopic, Body: []byte(body)}, nil); err != nil {
			t.Fatalf("Publish(%s): %v", body, err)
		}
	}
	dst := make([]mq.DriverDelivery, 2)
	fctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	n, err := src.Fetch(fctx, dst)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if n != 2 {
		t.Fatalf("Fetch returned %d of 2 published messages", n)
	}
	bs, ok := src.Settler().(mq.BatchSettler)
	if !ok {
		t.Fatal("the settler is not a BatchSettler; FeatureBatchSettle is advertised as native")
	}

	// Delivery 0 is refused locally and definitely; delivery 1 reaches the
	// server and its round trip is cut.
	dst[0].Gen++
	cmder.armed.Store(true)
	got := bs.AckBatch(ctx, []*mq.DriverDelivery{&dst[0], &dst[1]})

	var be *mq.BatchError
	if !errors.As(got, &be) {
		t.Fatalf("AckBatch(lease-lost, cut) = %v (ambiguous %v), want an index-aligned *mq.BatchError: "+
			"one delivery was definitely refused and the other's outcome is unknown, and one error cannot say both",
			got, mq.IsAmbiguous(got))
	}
	if len(be.Errs) != 2 {
		t.Fatalf("BatchError.Errs holds %d results for 2 deliveries", len(be.Errs))
	}
	if !errors.Is(be.Errs[0], mq.ErrLeaseLost) || mq.IsAmbiguous(be.Errs[0]) {
		t.Errorf("Errs[0] = %v (ambiguous %v), want a definite ErrLeaseLost", be.Errs[0], mq.IsAmbiguous(be.Errs[0]))
	}
	if !mq.IsAmbiguous(be.Errs[1]) {
		t.Errorf("Errs[1] = %v (code %v), want an ambiguous settlement: the XACK was written and its reply never came",
			be.Errs[1], mq.CodeOf(be.Errs[1]))
	}
	if !mq.IsAmbiguous(got) {
		t.Errorf("AckBatch = %v: IsAmbiguous is false although one delivery's settlement outcome is unknown", got)
	}

	// The collapse that is still right, and the mutation guard: with the
	// generation restored both deliveries reach the server, the cut takes
	// both, and every delivery has ONE outcome — so the single error is the
	// answer and no *BatchError is built. A fix that always built one would
	// fail here.
	dst[0].Gen--
	uniform := bs.AckBatch(ctx, []*mq.DriverDelivery{&dst[0], &dst[1]})
	if errors.As(uniform, &be) {
		t.Fatalf("AckBatch(cut, cut) = %v, want the single error: both deliveries have the same outcome", uniform)
	}
	if !mq.IsAmbiguous(uniform) {
		t.Fatalf("AckBatch(cut, cut) = %v, want an ambiguous answer", uniform)
	}
}
