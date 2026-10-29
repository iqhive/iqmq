package stomp_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// A publication that waits for its RECEIPT. go-stomp's `Conn.Send` takes no
// context and waits up to MsgSendTimeout to queue the frame and then up to
// RcvReceiptTimeout for the answer, both Config.ReceiptTimeout, so a caller's
// deadline ended nothing: a publication with a 100 ms deadline came back up to
// a minute later as SettlementUnknown rather than DeadlineExceeded. The tests
// here set a receipt timeout no test waits out, so that only the context can
// end the wait.

// longReceipt is a receipt timeout these tests never reach.
const longReceipt = time.Minute

// shortDeadline is the caller's deadline the wait has to honour.
const shortDeadline = 100 * time.Millisecond

// TestAPublicationWaitingForItsReceiptEndsWithItsContext is the sink's own
// wait.
func TestAPublicationWaitingForItsReceiptEndsWithItsContext(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	cfg.ReceiptTimeout = longReceipt
	b := openBackend(t, cfg)
	snk := openSink(t, b, "orders", mq.ConfirmAccepted)

	srv.HoldReceipts(1)
	ctx, cancel := context.WithTimeout(context.Background(), shortDeadline)
	defer cancel()
	err := within(t, "a publication whose receipt is held", func() error {
		return snk.Publish(ctx, &mq.Message{Body: []byte("one")}, nil)
	})
	if got := mq.CodeOf(err); got != mq.CodeDeadlineExceeded {
		t.Errorf("the publication reports %v (code %v), want %v: its caller's deadline is what ended it",
			err, got, mq.CodeDeadlineExceeded)
	}
	if !mq.IsAmbiguous(err) {
		t.Errorf("the publication reports %v as definite. Its SEND frame left the socket, so the broker may have "+
			"the message and a caller told it failed republishes it", err)
	}
	if got := countOf(srv, frame.SEND); got != 1 {
		t.Errorf("the server received %d SEND frame(s), want 1", got)
	}

	next, cancelNext := context.WithTimeout(context.Background(), budget)
	defer cancelNext()
	if err := snk.Publish(next, &mq.Message{Body: []byte("two")}, nil); err != nil {
		t.Errorf("the publication after one its caller gave up on failed: %v", err)
	}
}

// TestADeadLetterCopyWaitingForItsReceiptEndsWithItsContext is the settler's
// dead-letter copy, which publishes through the same sink and waited the same
// way. The original stays unacknowledged and settleable.
func TestADeadLetterCopyWaitingForItsReceiptEndsWithItsContext(t *testing.T) {
	t.Parallel()
	cfg := settings(activemq.Artemis)
	cfg.ReceiptTimeout = longReceipt
	cfg.DeadLetterTopic = "orders-dead"
	f, d := settleFixture(t, cfg)
	deadLetter, _ := destinations(t, cfg, "orders-dead", "")

	f.srv.HoldReceiptsOn(deadLetter, 1)
	ctx, cancel := context.WithTimeout(context.Background(), shortDeadline)
	defer cancel()
	err := within(t, "a dead-letter Nack whose copy's receipt is held", func() error {
		return f.src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackDeadLetter()))
	})
	if err == nil {
		t.Fatal("the dead-letter Nack succeeded although its copy was never confirmed")
	}
	if mq.IsAmbiguous(err) || !mq.IsRetryable(err) {
		t.Errorf("the dead-letter Nack reports %v (code %v, ambiguous %v, retryable %v), want a definite, "+
			"retryable refusal: the original was never settled", err, mq.CodeOf(err), mq.IsAmbiguous(err),
			mq.IsRetryable(err))
	}
	if got := countOf(f.srv, frame.ACK); got != 0 {
		t.Fatalf("the original was acknowledged %d time(s) behind a copy nobody confirmed\nframes:%s",
			got, frameLog(f.srv))
	}

	live, cancelLive := context.WithTimeout(context.Background(), budget)
	defer cancelLive()
	if err := f.src.Settler().Ack(live, &d); err != nil {
		t.Fatalf("acknowledging the delivery after the abandoned copy: %v. Nothing settled it, so it must still "+
			"be settleable", err)
	}
}

// TestCloseDoesNotWaitOutAGiveBacksReceipt is the pump's copy of a delivery
// it could not decode. The pump's context never ends, and Close waits for the
// pump, so a broker that never answered the copy held Close for the whole of
// the receipt timeout.
func TestCloseDoesNotWaitOutAGiveBacksReceipt(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	cfg.ReceiptTimeout = longReceipt
	cfg.Metadata = mq.MetadataPortableEnvelope
	cfg.DeadLetterTopic = "poison"
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
	deadLetter, _ := destinations(t, cfg, "poison", "")

	srv.HoldReceiptsOn(deadLetter, 1)
	deliver(t, srv, consume, stompsrv.Message{Body: poisonBody})
	if err := srv.WaitFrames(frame.SEND, 1, budget); err != nil {
		t.Fatalf("the source never published its copy of the undecodable delivery: %v\nframes:%s", err, frameLog(srv))
	}
	closesWithin(t, src)
	if got := countOf(srv, frame.ACK); got != 0 {
		t.Errorf("the undecodable delivery was acknowledged %d time(s) behind a copy nobody confirmed", got)
	}
}

// within runs op and fails the test unless it returns well inside the budget,
// which is far below the receipt timeout these tests configure.
func within(t *testing.T, what string, op func() error) error {
	t.Helper()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- op() }()
	select {
	case err := <-done:
		if took := time.Since(start); took > budget/2 {
			t.Errorf("%s took %v against a deadline of %v", what, took, shortDeadline)
		}
		return err
	case <-time.After(budget):
		t.Fatalf("%s did not return within %v of a %v deadline: it is waiting out Config.ReceiptTimeout (%v)",
			what, budget, shortDeadline, longReceipt)
		return nil
	}
}
