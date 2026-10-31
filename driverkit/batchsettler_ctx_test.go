package driverkit_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
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

// parkedBatchSettler parks every round trip, single or batched, until release
// is closed or the context it was sent under ends, and counts the ones the
// context ended.
type parkedBatchSettler struct {
	entered   chan int
	release   chan struct{}
	mu        sync.Mutex
	cancelled int
}

func newParkedBatchSettler() *parkedBatchSettler {
	return &parkedBatchSettler{entered: make(chan int, 1), release: make(chan struct{})}
}

func (s *parkedBatchSettler) park(ctx context.Context, n int) error {
	s.entered <- n
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		s.cancelled++
		s.mu.Unlock()
		return ctx.Err()
	}
}

func (s *parkedBatchSettler) cancels() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelled
}

func (s *parkedBatchSettler) Ack(ctx context.Context, _ *mq.DriverDelivery) error {
	return s.park(ctx, 1)
}

func (s *parkedBatchSettler) Nack(ctx context.Context, _ *mq.DriverDelivery, _ mq.NackOptions) error {
	return s.park(ctx, 1)
}

func (s *parkedBatchSettler) AckBatch(ctx context.Context, ds []*mq.DriverDelivery) error {
	return s.park(ctx, len(ds))
}

func (s *parkedBatchSettler) NackBatch(ctx context.Context, ds []*mq.DriverDelivery, _ mq.NackOptions) error {
	return s.park(ctx, len(ds))
}

// TestBatchSettlerCancelsARoundTripItsCallerGaveUpOn is the consumer shutdown
// the settler used to hang.
//
// The core settles under one context per consumer and cancels it as the
// consumer shuts down, which is what releases a settlement parked in a driver;
// Committing then waits for every settlement to leave before the source, and
// with it this settler, is closed. The round trip ran under the settler's own
// context, which only Close ends, so the cancellation never reached the driver
// and a broker that did not answer held Close until the native client gave up.
func TestBatchSettlerCancelsARoundTripItsCallerGaveUpOn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		driver := newParkedBatchSettler()
		bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{Batching: driverkit.SettleImmediate})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		acked := make(chan error, 1)
		go func() { acked <- bs.Ack(ctx, &mq.DriverDelivery{Message: &mq.Message{}, Token: 1}) }()
		<-driver.entered
		cancel()
		synctest.Wait()
		returned := len(acked) == 1
		close(driver.release)
		_ = bs.Close()
		if !returned {
			t.Fatal("Ack was still parked in the driver after its caller's context ended: " +
				"the core's settlement cancel never reaches the driver, and Committing waits for it for ever")
		}
		if n := driver.cancels(); n != 1 {
			t.Fatalf("the driver saw %d round trips cancelled, want 1", n)
		}
	})
}

// TestBatchSettlerCancelsARoundTripOnlyOnceNobodyWaits holds the other half: a
// round trip carrying two callers' settlements runs on while either of them
// still waits, so one caller's cancellation does not fail the other's
// settlement, and is cancelled once neither does.
func TestBatchSettlerCancelsARoundTripOnlyOnceNobodyWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		driver := newParkedBatchSettler()
		bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{Batching: driverkit.SettleCount, MaxItems: 2})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		ctxA, cancelA := context.WithCancel(context.Background())
		ctxB, cancelB := context.WithCancel(context.Background())
		a, b := make(chan error, 1), make(chan error, 1)
		go func() { a <- bs.Ack(ctxA, &mq.DriverDelivery{Message: &mq.Message{}, Token: 1}) }()
		synctest.Wait()
		go func() { b <- bs.Ack(ctxB, &mq.DriverDelivery{Message: &mq.Message{}, Token: 2}) }()
		if n := <-driver.entered; n != 2 {
			t.Errorf("the round trip carried %d settlements, want both", n)
		}
		cancelA()
		synctest.Wait()
		if driver.cancels() != 0 {
			t.Error("one caller's cancellation cancelled a round trip another caller still waits for")
		}
		if err := <-a; !mq.IsAmbiguous(err) {
			t.Errorf("the caller that gave up on a settlement already sent got %v, want an unknown outcome", err)
		}
		cancelB()
		synctest.Wait()
		if n := driver.cancels(); n != 1 {
			t.Errorf("nobody waits for the round trip any more and the driver saw %d cancelled, want 1", n)
		}
		close(driver.release)
		if err := <-b; err == nil {
			t.Error("a settlement whose round trip was cancelled reported success")
		}
		_ = bs.Close()
	})
}

// TestBatchSettlerFlushStopsOnceNobodyWaits holds Flush to its context, which
// it used to ignore: the round trip a Flush sends is cancelled once Flush's
// caller and the settlements' own callers have all stopped waiting.
func TestBatchSettlerFlushStopsOnceNobodyWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		driver := newParkedBatchSettler()
		bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{Batching: driverkit.SettleCount, MaxItems: 100})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		acked := make(chan error, 1)
		go func() { acked <- bs.Ack(ctx, &mq.DriverDelivery{Message: &mq.Message{}, Token: 1}) }()
		synctest.Wait()
		fctx, fcancel := context.WithCancel(context.Background())
		flushed := make(chan error, 1)
		go func() { flushed <- bs.Flush(fctx) }()
		<-driver.entered
		fcancel()
		synctest.Wait()
		if driver.cancels() != 0 {
			t.Error("Flush's cancellation cancelled a settlement its own caller still waits for")
		}
		cancel()
		synctest.Wait()
		returned := len(flushed) == 1
		close(driver.release)
		ackErr := <-acked
		_ = bs.Close()
		if !returned {
			t.Fatal("Flush was still parked in the driver after its context and the settlement's had both ended")
		}
		if err := <-flushed; !errors.Is(err, context.Canceled) {
			t.Errorf("Flush returned %v, want context.Canceled", err)
		}
		if ackErr == nil {
			t.Error("a settlement whose round trip was cancelled reported success")
		}
	})
}

// TestBatchSettlerCloseWaitsForWhatATickTook pins the line Flush's doc draws
// between it and Close. Flush sends what is still queued, and a settlement the
// ticker has already taken is on its way, so Flush does not wait for it; Close
// does, which is why a source about to close its client calls Close. Flush's
// doc used to call it what a source does before it closes, and a source that
// closed its client after it cut off the round trip still under way.
func TestBatchSettlerCloseWaitsForWhatATickTook(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		driver := newParkedBatchSettler()
		bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{
			Batching: driverkit.SettleTime, Interval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		acked := make(chan error, 1)
		go func() { acked <- bs.Ack(context.Background(), &mq.DriverDelivery{Message: &mq.Message{}, Token: 1}) }()
		<-driver.entered // the tick took the settlement, and its round trip is under way
		flushed := make(chan error, 1)
		go func() { flushed <- bs.Flush(context.Background()) }()
		synctest.Wait()
		flushReturned := len(flushed) == 1
		closed := make(chan error, 1)
		go func() { closed <- bs.Close() }()
		synctest.Wait()
		closeReturned := len(closed) == 1
		close(driver.release)
		if err := <-flushed; err != nil {
			t.Errorf("Flush: %v", err)
		}
		if err := <-closed; err != nil {
			t.Errorf("Close: %v", err)
		}
		if !flushReturned {
			t.Error("Flush waited for a round trip the tick had started; it waits for what it sends")
		}
		if closeReturned {
			t.Error("Close returned while the settlement the tick took was still on its way")
		}
		if err := <-acked; err != nil {
			t.Errorf("the settlement the tick took = %v, want nil", err)
		}
		if n := driver.cancels(); n != 0 {
			t.Errorf("Close cancelled %d round trip(s) the tick had started", n)
		}
	})
}

// TestBatchSettlerCloseSendsWhatItHoldsInBatchesOfMaxItems holds Close to the
// ceiling every strategy splits a batch on.
//
// A time-based settler holds whatever arrived since its last tick, which can be
// more than MaxItems, and Close used to send all of it as one batch: a broker
// with a hard batch limit (SQS takes ten) refused the whole batch, and every
// settlement Close was holding failed.
func TestBatchSettlerCloseSendsWhatItHoldsInBatchesOfMaxItems(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		driver := &batchingDriverSettler{}
		bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{
			Batching: driverkit.SettleTime, Interval: time.Hour, MaxItems: 2,
		})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		errs := make(chan error, 5)
		for i := range 5 {
			go func() {
				errs <- bs.Ack(context.Background(), &mq.DriverDelivery{Message: &mq.Message{}, Token: uint64(i)})
			}()
		}
		synctest.Wait()
		if err := bs.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		for range 5 {
			if err := <-errs; err != nil {
				t.Errorf("a settlement Close was holding got %v, want nil", err)
			}
		}
		driver.mu.Lock()
		defer driver.mu.Unlock()
		for _, batch := range driver.batches {
			if len(batch) > 2 {
				t.Errorf("Close sent a batch of %d, past MaxItems 2", len(batch))
			}
		}
		// A batch of one goes through the driver's single Ack, which records
		// it in acked but not in batches.
		if len(driver.acked) != 5 {
			t.Errorf("Close settled %d of the 5 settlements it was holding", len(driver.acked))
		}
	})
}

// TestBatchSettlerLetsTheCallerThatFlushedGiveUp holds the caller whose
// settlement triggered the flush to its own context, like every other caller.
//
// That caller used to run the flush itself, so it was inside the driver's
// round trip until the round trip ended. One that carries another caller's
// live settlement runs on by design, which left the flushing caller parked
// after its context had ended, and the core's shutdown waited for it.
func TestBatchSettlerLetsTheCallerThatFlushedGiveUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		driver := newParkedBatchSettler()
		bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{Batching: driverkit.SettleCount, MaxItems: 2})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		ctxA, cancelA := context.WithCancel(context.Background())
		ctxB, cancelB := context.WithCancel(context.Background())
		a, b := make(chan error, 1), make(chan error, 1)
		go func() { a <- bs.Ack(ctxA, &mq.DriverDelivery{Message: &mq.Message{}, Token: 1}) }()
		synctest.Wait()
		// B fills the batch, so B's Ack is the one that sends it.
		go func() { b <- bs.Ack(ctxB, &mq.DriverDelivery{Message: &mq.Message{}, Token: 2}) }()
		if n := <-driver.entered; n != 2 {
			t.Errorf("the round trip carried %d settlements, want both", n)
		}
		cancelB()
		synctest.Wait()
		returned := len(b) == 1
		if driver.cancels() != 0 {
			t.Error("the flushing caller's cancellation cancelled a round trip another caller still waits for")
		}
		cancelA()
		synctest.Wait()
		close(driver.release)
		if err := <-a; err == nil {
			t.Error("a settlement whose round trip was cancelled reported success")
		}
		_ = bs.Close()
		if !returned {
			t.Fatal("the caller that sent the batch was still parked in the driver after its context ended")
		}
		if err := <-b; !mq.IsAmbiguous(err) {
			t.Errorf("the flushing caller that gave up on a settlement already sent got %v, want an unknown outcome", err)
		}
	})
}

// TestBatchSettlerRefusesABatchAfterClose holds the native batch to Close as
// well as the single settlements: after Close both are refused with ErrClosed
// and neither reaches the driver. The batch used to go straight through.
func TestBatchSettlerRefusesABatchAfterClose(t *testing.T) {
	driver := &batchingDriverSettler{}
	bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{Batching: driverkit.SettleImmediate})
	if err != nil {
		t.Fatalf("NewBatchSettler: %v", err)
	}
	batch, ok := bs.Settler().(mq.BatchSettler)
	if !ok {
		t.Fatalf("the settler of a driver with a native batch is %T, not a BatchSettler", bs.Settler())
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ds := []*mq.DriverDelivery{{Message: &mq.Message{}, Token: 1}, {Message: &mq.Message{}, Token: 2}}
	if err := batch.AckBatch(context.Background(), ds); !errors.Is(err, mq.ErrClosed) {
		t.Errorf("AckBatch after Close = %v, want ErrClosed", err)
	}
	if err := batch.NackBatch(context.Background(), ds, mq.NackOptions{}); !errors.Is(err, mq.ErrClosed) {
		t.Errorf("NackBatch after Close = %v, want ErrClosed", err)
	}
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if driver.ackCalls != 0 || len(driver.nacked) != 0 {
		t.Errorf("after Close the driver saw %d AckBatch calls and %d nacks, want none", driver.ackCalls, len(driver.nacked))
	}
}

// TestBatchSettlerCloseWaitsForABatchInFlight holds Close to a native batch a
// caller has already started: Close waits for it as it waits for a flush, and
// does not return while the batch is still on its way to the broker.
func TestBatchSettlerCloseWaitsForABatchInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		driver := newParkedBatchSettler()
		bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{Batching: driverkit.SettleImmediate})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		batch, ok := bs.Settler().(mq.BatchSettler)
		if !ok {
			t.Fatalf("the settler of a driver with a native batch is %T, not a BatchSettler", bs.Settler())
		}
		acked := make(chan error, 1)
		go func() {
			acked <- batch.AckBatch(context.Background(), []*mq.DriverDelivery{{Message: &mq.Message{}, Token: 1}})
		}()
		<-driver.entered
		closed := make(chan error, 1)
		go func() { closed <- bs.Close() }()
		synctest.Wait()
		early := len(closed) == 1
		close(driver.release)
		if err := <-acked; err != nil {
			t.Errorf("a batch in flight when Close ran got %v, want nil", err)
		}
		if err := <-closed; err != nil {
			t.Errorf("Close: %v", err)
		}
		if early {
			t.Error("Close returned while a batch it had admitted was still in the driver")
		}
	})
}

// deafSettler parks every round trip until release is closed, whatever its
// context says: a driver whose round trip ignores cancellation.
type deafSettler struct {
	entered chan struct{}
	release chan struct{}
}

func (s *deafSettler) Ack(context.Context, *mq.DriverDelivery) error {
	s.entered <- struct{}{}
	<-s.release
	return nil
}

func (s *deafSettler) Nack(ctx context.Context, d *mq.DriverDelivery, _ mq.NackOptions) error {
	return s.Ack(ctx, d)
}

// TestBatchSettlerCloseIsBoundedForARoundTripThatIgnoresItsContext holds Close
// to the drain timeout whoever started the round trip: the ticker of a
// time-based settler, or Close itself sending what it held.
//
// Close used to wait for the ticker to stop before its bounded wait began, and
// the ticker stops only once its flush returns, so a round trip that ignored
// its cancellation held Close for as long as the driver liked. What Close held
// it sent in line, with the same result, and a second Close waited for the
// ticker too.
func TestBatchSettlerCloseIsBoundedForARoundTripThatIgnoresItsContext(t *testing.T) {
	for _, tc := range []struct {
		name     string
		interval time.Duration
	}{
		{"a-flush-the-ticker-started", 10 * time.Millisecond},
		{"what-close-sends-itself", time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				driver := &deafSettler{entered: make(chan struct{}, 1), release: make(chan struct{})}
				bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{
					Batching: driverkit.SettleTime, Interval: tc.interval,
				})
				if err != nil {
					t.Fatalf("NewBatchSettler: %v", err)
				}
				acked := make(chan error, 1)
				go func() { acked <- bs.Ack(context.Background(), &mq.DriverDelivery{Message: &mq.Message{}, Token: 1}) }()
				// Under the hour-long interval the settlement is still queued
				// when Close runs, and Close is what sends it.
				queued := tc.interval == time.Hour
				if queued {
					synctest.Wait()
				} else {
					<-driver.entered
				}
				closed := make(chan error, 2)
				go func() { closed <- bs.Close() }()
				if queued {
					<-driver.entered
				}
				time.Sleep(mq.DefaultDrainTimeout + time.Second)
				first := len(closed) == 1
				go func() { closed <- bs.Close() }()
				synctest.Wait()
				second := len(closed) == 2
				close(driver.release)
				if err := <-acked; err != nil {
					t.Errorf("the settlement got %v once the driver answered, want nil", err)
				}
				for range 2 {
					if err := <-closed; err != nil {
						t.Errorf("Close: %v", err)
					}
				}
				if !first {
					t.Fatalf("Close was still waiting %v after it began, past the drain timeout, for a round trip "+
						"that ignores its context", mq.DefaultDrainTimeout+time.Second)
				}
				if !second {
					t.Fatal("a second Close was still waiting after the first had returned")
				}
			})
		})
	}
}
