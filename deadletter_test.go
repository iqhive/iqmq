package iqmq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestRunUsesTheDriversOwnDeadLetterRoute is WithNativeDeadLetter: a driver
// that advertises FeatureDeadLetter knows where its dead letters go, and
// settling with Nack(NackDeadLetter()) gets the message there in one native
// settlement — no publication, nothing to confirm, and no window in which the
// message exists in both places.
func TestRunUsesTheDriversOwnDeadLetterRoute(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{deadLetter: true}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	h := &collector{fn: func(int, *mq.Delivery) error { return errors.New("undecodable") }}
	run := startRun(t, c, h,
		mq.WithMaxAttempts(1),
		mq.WithNativeDeadLetter(),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	waitFor(t, "the native dead-letter settlement", func() bool {
		return b.counts("orders", "billing").deadLettered == 1
	})
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	counts := b.counts("orders", "billing")
	if counts.acked != 0 {
		t.Fatalf("acked = %d, want 0: the native route settles the delivery itself", counts.acked)
	}
	if !counts.conserved() {
		t.Fatalf("The specification conservation broken: %+v", counts)
	}
	if got := c.Stats().DeadLettered; got != 1 {
		t.Fatalf("ConsumerStats.DeadLettered = %d, want 1", got)
	}
	if got := c.Stats().DeadLetterFailures; got != 0 {
		t.Fatalf("DeadLetterFailures = %d, want 0", got)
	}
}

// TestRunRefusesTheNativeDeadLetterRouteOnADriverWithoutOne is the
// route is refused rather than emulated in silence, and it is refused at Run
// rather than at the first poison message.
func TestRunRefusesTheNativeDeadLetterRouteOnADriverWithoutOne(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	h := mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil })
	// Already done, for the reason
	// TestRunRefusesLeaseExtensionOnADriverThatCannotExtend gives.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.Run(ctx, h, mq.WithNativeDeadLetter())
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Run = %v, want ErrUnsupported", err)
	}
}

// TestRunOpensTheDeadLetterPublisherWithTheConfirmationAsked is the other half: "confirmed publish" meant the driver's own acceptance
// boundary and a caller had no way to ask for more. WithDeadLetterConfirmation
// is the asking, and it reaches the driver at open — so a profile that cannot
// define the boundary fails the Run rather than the first poison message.
func TestRunOpensTheDeadLetterPublisherWithTheConfirmationAsked(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders", "orders.dlq")
	c := consume(t, cl, mq.ConsumerConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }),
			mq.WithDeadLetter("orders.dlq"),
			mq.WithDeadLetterConfirmation(mq.ConfirmDurable))
	}()
	waitFor(t, "the dead-letter publisher to be opened", func() bool {
		_, ok := b.sinkConfirmation("orders.dlq")
		return ok
	})
	got, _ := b.sinkConfirmation("orders.dlq")
	if got != mq.ConfirmDurable {
		t.Fatalf("the dead-letter sink was opened with %v, want ConfirmDurable", got)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
}

// TestRunCountsASettlementItCouldNotComplete is the finding behind
// runner.settleFailed counting nothing: a settlement the runner could not
// complete is already counted, by the settlement itself, in the
// specification's iqmq_settle_failures_total. A second count inside the
// runner would double it.
func TestRunCountsASettlementItCouldNotComplete(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	b.settlerErr = errors.New("the broker refused the settlement")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	h := &collector{}
	run := startRun(t, c, h)
	waitFor(t, "the refused settlement to be counted", func() bool {
		return c.Stats().SettleFailures >= 1
	})
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got := c.Stats().Acked; got != 0 {
		t.Fatalf("Acked = %d, want 0: the settlement never succeeded", got)
	}
}

// dlqHangBroker is the fake broker with a dead-letter destination that never
// answers: a publication to it parks until its context ends, or until the
// test releases it.
type dlqHangBroker struct {
	*fakeBroker
	entered chan struct{}
	release chan struct{}
}

func (b *dlqHangBroker) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if cfg.Topic != "orders.dlq" {
		return b.fakeBroker.OpenSink(ctx, cfg)
	}
	return dlqHangSink{b}, nil
}

type dlqHangSink struct{ b *dlqHangBroker }

func (s dlqHangSink) Publish(ctx context.Context, _ *mq.Message, _ *mq.PublishOptions) error {
	select {
	case s.b.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.b.release:
		return mq.ErrClosed
	}
}

func (dlqHangSink) Close() error { return nil }

// TestRunDrainTimeoutEndsAStuckDeadLetterPublication is WithDrainTimeout
// reaching a dead-letter publication: the publication outlives Run's context
// on purpose, but once the drain's patience is gone it is cancelled like the
// handlers are, so Run returns and the delivery goes back rather than the
// shutdown waiting for a destination that never answers.
func TestRunDrainTimeoutEndsAStuckDeadLetterPublication(t *testing.T) {
	fb := newFakeBroker(fakeConfig{}, "orders", "orders.dlq")
	b := &dlqHangBroker{fakeBroker: fb, entered: make(chan struct{}, 1), release: make(chan struct{})}
	cl := mq.NewClient(b)
	defer func() { _ = cl.Close() }()
	// Released before the client closes, so a failing run still ends.
	defer close(b.release)
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	h := mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		return mq.DeadLetter(errors.New("poison"))
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx, h, mq.WithDeadLetter("orders.dlq"), mq.WithDrainTimeout(50*time.Millisecond))
	}()
	select {
	case <-b.entered:
	case <-time.After(runWait):
		cancel()
		t.Fatal("the dead-letter publication never reached the driver")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within the drain timeout while a dead-letter publication was stuck")
	}
	if got := fb.counts("orders", "billing"); got.requeued != 1 || got.acked != 0 {
		t.Fatalf("counts = %+v, want the delivery requeued and not acked", got)
	}
}
