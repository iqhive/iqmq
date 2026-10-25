package driverkit_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// ctxRecordingSettler records whether the context each settlement was sent
// under had already ended.
type ctxRecordingSettler struct {
	mu    sync.Mutex
	acked []uint64
	dead  int
}

func (s *ctxRecordingSettler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		s.dead++
		return ctx.Err()
	}
	s.acked = append(s.acked, d.Token)
	return nil
}

func (s *ctxRecordingSettler) Nack(ctx context.Context, d *mq.DriverDelivery, _ mq.NackOptions) error {
	return s.Ack(ctx, d)
}

func (s *ctxRecordingSettler) counts() (acked, dead int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.acked), s.dead
}

// TestBatchSettlerFlushesUnderItsOwnContext holds that one caller's context
// does not decide everyone's settlement: the caller whose arrival fills the
// batch may have a context that has already ended, and the settlement queued
// before it, under a live one, must still be sent.
func TestBatchSettlerFlushesUnderItsOwnContext(t *testing.T) {
	driver := &ctxRecordingSettler{}
	bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{
		Batching: driverkit.SettleCount, MaxItems: 2,
	})
	if err != nil {
		t.Fatalf("NewBatchSettler: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	first := make(chan error, 1)
	go func() { first <- bs.Ack(context.Background(), &mq.DriverDelivery{Message: &mq.Message{}, Token: 1}) }()
	time.Sleep(10 * time.Millisecond)
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	_ = bs.Ack(ended, &mq.DriverDelivery{Message: &mq.Message{}, Token: 2})
	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("the settlement queued under a live context got %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the settlement queued under a live context was never resolved")
	}
	if _, dead := driver.counts(); dead != 0 {
		t.Fatalf("%d settlements were sent under a context that had already ended, want 0", dead)
	}
}

// TestBatchSettlerWithdrawsACancelledSettlement holds that a caller told its
// settlement failed is not then settled behind its back: a settlement still
// queued when its caller's context ends is withdrawn, so a later flush does
// not send it.
func TestBatchSettlerWithdrawsACancelledSettlement(t *testing.T) {
	driver := &ctxRecordingSettler{}
	bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{
		Batching: driverkit.SettleCount, MaxItems: 100,
	})
	if err != nil {
		t.Fatalf("NewBatchSettler: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := bs.Ack(ctx, &mq.DriverDelivery{Message: &mq.Message{}, Token: 1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a queued settlement whose context expired returned %v, want context.DeadlineExceeded", err)
	}
	if err := bs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if acked, _ := driver.counts(); acked != 0 {
		t.Fatalf("the driver acknowledged %d settlements whose caller had been told they failed, want 0", acked)
	}
}

// parkedSettler holds every settlement until release is closed, and fails one
// whose context ends first.
type parkedSettler struct {
	entered chan struct{}
	release chan struct{}
}

func (s *parkedSettler) Ack(ctx context.Context, _ *mq.DriverDelivery) error {
	close(s.entered)
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *parkedSettler) Nack(ctx context.Context, d *mq.DriverDelivery, _ mq.NackOptions) error {
	return s.Ack(ctx, d)
}

// TestBatchSettlerCloseWaitsForASettlementInFlight holds that Close does not
// cancel a settlement a flush has already taken from the queue.
//
// Under SettleImmediate the caller's own flush takes its item and is inside the
// driver's round trip, so Close finds the queue empty. It used to end the
// settler's context there and then, which failed an acknowledgement submitted
// before the close and still on its way to the broker.
func TestBatchSettlerCloseWaitsForASettlementInFlight(t *testing.T) {
	driver := &parkedSettler{entered: make(chan struct{}), release: make(chan struct{})}
	bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{Batching: driverkit.SettleImmediate})
	if err != nil {
		t.Fatalf("NewBatchSettler: %v", err)
	}
	acked := make(chan error, 1)
	go func() { acked <- bs.Ack(context.Background(), &mq.DriverDelivery{Message: &mq.Message{}, Token: 1}) }()
	select {
	case <-driver.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the settlement never reached the driver")
	}
	closed := make(chan error, 1)
	go func() { closed <- bs.Close() }()
	// Close may not return while the settlement is in flight. Give a Close
	// that does not wait the time to end the context, then let the driver
	// answer: a settlement whose context was cancelled answers with that.
	select {
	case err := <-closed:
		// Put it back: the check below is the one that judges it.
		closed <- err
	case <-time.After(200 * time.Millisecond):
	}
	close(driver.release)
	if err := <-acked; err != nil {
		t.Fatalf("an acknowledgement in flight when Close ran got %v, want nil", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
}
