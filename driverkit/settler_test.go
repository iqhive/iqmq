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

// extendingDriverSettler is a driver's own settler: it acks, it nacks, and —
// like every broker with a visibility timeout — it extends a lease.
type extendingDriverSettler struct {
	mu       sync.Mutex
	acked    []uint64
	nacked   []uint64
	extended []time.Duration
	fail     error
}

func (s *extendingDriverSettler) Ack(_ context.Context, d *mq.DriverDelivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	s.acked = append(s.acked, d.Token)
	return nil
}

func (s *extendingDriverSettler) Nack(_ context.Context, d *mq.DriverDelivery, _ mq.NackOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nacked = append(s.nacked, d.Token)
	return nil
}

func (s *extendingDriverSettler) Extend(_ context.Context, _ *mq.DriverDelivery, by time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.extended = append(s.extended, by)
	return nil
}

func (s *extendingDriverSettler) extends() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.extended)
}

// plainDriverSettler is a driver with no lease to extend: NATS Core, a broker
// whose settlement is fire and forget.
type plainDriverSettler struct{ extendingDriverSettler }

func (s *plainDriverSettler) Extend() {} // shadowed on purpose; not a DeadlineExtender

// batchingDriverSettler adds the SPI's batched settlement, with a partial
// result whose entries are index-aligned with the batch.
type batchingDriverSettler struct {
	extendingDriverSettler
	failAt   map[int]error
	ackCalls int
	batches  [][]uint64
}

func (s *batchingDriverSettler) AckBatch(_ context.Context, ds []*mq.DriverDelivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ackCalls++
	tokens := make([]uint64, len(ds))
	errs := make([]error, len(ds))
	partial := false
	for i, d := range ds {
		tokens[i] = d.Token
		if err, bad := s.failAt[int(d.Token)]; bad {
			errs[i] = err
			partial = true
			continue
		}
		s.acked = append(s.acked, d.Token)
	}
	s.batches = append(s.batches, tokens)
	if partial {
		return &mq.BatchError{Errs: errs}
	}
	return nil
}

func (s *batchingDriverSettler) NackBatch(_ context.Context, ds []*mq.DriverDelivery, _ mq.NackOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range ds {
		s.nacked = append(s.nacked, d.Token)
	}
	return nil
}

// The check is end to end. The driver's settler is a DeadlineExtender, it goes
// through the BatchSettler adapter, and the value the adapter hands back — the
// value a driver's Source.Settler returns, and therefore the one the core
// type-asserts once at open — must still be a DeadlineExtender and must still
// reach the driver. An adapter that dropped Extend would leave every wrapped
// driver unable to extend a lease with no error reported anywhere: the core
// decides at open that there is no extender and answers ErrUnsupported for ever.
func TestWrappedSettlerStillExtends(t *testing.T) {
	ctx := context.Background()
	driver := &extendingDriverSettler{}
	for _, batching := range []driverkit.SettleBatching{
		driverkit.SettleImmediate, driverkit.SettleCount, driverkit.SettleTime, driverkit.SettleHybrid,
	} {
		t.Run(batching.String(), func(t *testing.T) {
			bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{
				Batching: batching, MaxItems: 4, Interval: 5 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("NewBatchSettler: %v", err)
			}
			t.Cleanup(func() { _ = bs.Close() })
			wrapped := bs.Settler()
			ext, ok := wrapped.(mq.DeadlineExtender)
			if !ok {
				t.Fatalf("the %s adapter dropped DeadlineExtender: a wrapped driver silently loses lease extension", batching)
			}
			before := driver.extends()
			d := &mq.DriverDelivery{Message: &mq.Message{}, Token: 1}
			if err := ext.Extend(ctx, d, 30*time.Second); err != nil {
				t.Fatalf("Extend through the adapter: %v", err)
			}
			if driver.extends() != before+1 {
				t.Fatal("Extend through the adapter never reached the driver")
			}
		})
	}
}

// TestWrapSettlerDoesNotInventExtension is the other half: a driver with no
// lease to extend must not be given one by an adapter. The specification decides whether a
// settler is a DeadlineExtender once, when the source is opened, and a wrapper
// that always implements Extend would make a broker with no visibility timeout
// look as though it had one.
func TestWrapSettlerDoesNotInventExtension(t *testing.T) {
	plain := &plainDriverSettler{}
	if _, ok := any(plain).(mq.DeadlineExtender); ok {
		t.Fatal("the fixture is wrong: plainDriverSettler must not be a DeadlineExtender")
	}
	bs, err := driverkit.NewBatchSettler(plain, driverkit.BatchConfig{})
	if err != nil {
		t.Fatalf("NewBatchSettler: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if _, ok := bs.Settler().(mq.DeadlineExtender); ok {
		t.Fatal("the adapter presented a DeadlineExtender over a driver that has none")
	}
}

// TestBatchSettlerCoalesces pins the coalescing itself: settlements go to the
// broker in batches, every caller gets its own outcome, and a partial result is
// distributed by index.
func TestBatchSettlerCoalesces(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("receipt handle expired")
	driver := &batchingDriverSettler{failAt: map[int]error{2: boom}}
	bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{
		Batching: driverkit.SettleCount, MaxItems: 4,
	})
	if err != nil {
		t.Fatalf("NewBatchSettler: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	errs := make([]error, 4)
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = bs.Ack(ctx, &mq.DriverDelivery{Message: &mq.Message{}, Token: uint64(i)})
		}()
	}
	wg.Wait()
	driver.mu.Lock()
	calls, batches := driver.ackCalls, len(driver.batches)
	driver.mu.Unlock()
	if calls != 1 || batches != 1 {
		t.Fatalf("four settlements made %d batched calls, want one", calls)
	}
	for i, err := range errs {
		switch {
		case i == 2 && !errors.Is(err, boom):
			t.Errorf("the failed item got %v, want the broker's own error", err)
		case i != 2 && err != nil:
			t.Errorf("item %d got %v, want nil: a partial result was distributed by the wrong index", i, err)
		}
	}
}

// TestBatchSettlerBoundaries pins that Ack returns at the configured boundary
// and that a time-based settler really does send on its ticker.
func TestBatchSettlerBoundaries(t *testing.T) {
	ctx := context.Background()
	t.Run("immediate sends one at a time", func(t *testing.T) {
		driver := &batchingDriverSettler{}
		bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{Batching: driverkit.SettleImmediate})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		t.Cleanup(func() { _ = bs.Close() })
		for i := range 3 {
			if err := bs.Ack(ctx, &mq.DriverDelivery{Message: &mq.Message{}, Token: uint64(i)}); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
		driver.mu.Lock()
		defer driver.mu.Unlock()
		if len(driver.acked) != 3 {
			t.Fatalf("acked %d", len(driver.acked))
		}
	})
	t.Run("time sends on the ticker", func(t *testing.T) {
		driver := &batchingDriverSettler{}
		bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{
			Batching: driverkit.SettleTime, Interval: 5 * time.Millisecond, MaxItems: 100,
		})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		t.Cleanup(func() { _ = bs.Close() })
		done := make(chan error, 1)
		go func() { done <- bs.Ack(ctx, &mq.DriverDelivery{Message: &mq.Message{}, Token: 7}) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Ack: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a time-based settler never sent")
		}
	})
	t.Run("Flush sends what is queued", func(t *testing.T) {
		driver := &batchingDriverSettler{}
		bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{
			Batching: driverkit.SettleCount, MaxItems: 100,
		})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		t.Cleanup(func() { _ = bs.Close() })
		done := make(chan error, 1)
		go func() { done <- bs.Ack(ctx, &mq.DriverDelivery{Message: &mq.Message{}, Token: 1}) }()
		// The settlement is held: a count-based settler at one of a hundred
		// has not sent, which is exactly why Flush exists.
		time.Sleep(10 * time.Millisecond)
		select {
		case err := <-done:
			t.Fatalf("a count-based settler sent a batch of one: %v", err)
		default:
		}
		if err := bs.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Ack: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Flush did not release the queued settlement")
		}
	})
	t.Run("a settlement after Close is refused, never parked", func(t *testing.T) {
		driver := &batchingDriverSettler{}
		bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{
			Batching: driverkit.SettleCount, MaxItems: 100,
		})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		if err := bs.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := bs.Ack(ctx, &mq.DriverDelivery{Message: &mq.Message{}, Token: 1}); !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("Ack after Close: %v", err)
		}
		if err := bs.Close(); err != nil {
			t.Errorf("a second Close: %v", err)
		}
	})
	t.Run("Close sends what it was still holding", func(t *testing.T) {
		driver := &batchingDriverSettler{}
		bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{
			Batching: driverkit.SettleCount, MaxItems: 100,
		})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		done := make(chan error, 1)
		go func() { done <- bs.Ack(ctx, &mq.DriverDelivery{Message: &mq.Message{}, Token: 3}) }()
		time.Sleep(10 * time.Millisecond)
		if err := bs.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("the settlement Close was holding: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close abandoned a queued settlement: the caller is parked for ever")
		}
		driver.mu.Lock()
		defer driver.mu.Unlock()
		if len(driver.acked) != 1 {
			t.Fatalf("the driver saw %d acks, want the one Close was holding", len(driver.acked))
		}
	})
}

// TestBatchSettlerRefusals pins the construction rules.
func TestBatchSettlerRefusals(t *testing.T) {
	if _, err := driverkit.NewBatchSettler(nil, driverkit.BatchConfig{}); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Errorf("a nil settler: %v", err)
	}
	for _, cfg := range []driverkit.BatchConfig{
		{Batching: driverkit.SettleBatching(9)},
		{MaxItems: -1},
		{Interval: -time.Second},
	} {
		if _, err := driverkit.NewBatchSettler(&extendingDriverSettler{}, cfg); mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Errorf("NewBatchSettler(%+v): %v", cfg, err)
		}
	}
}

// collapsingDriverSettler answers its batched settlement the way the drivers
// do, through CollapseBatch, with the error the test gave each token.
type collapsingDriverSettler struct {
	extendingDriverSettler
	errs map[uint64]error
}

func (s *collapsingDriverSettler) AckBatch(_ context.Context, ds []*mq.DriverDelivery) error {
	errs := make([]error, len(ds))
	for i, d := range ds {
		errs[i] = s.errs[d.Token]
	}
	return driverkit.CollapseBatch(errs)
}

func (s *collapsingDriverSettler) NackBatch(ctx context.Context, ds []*mq.DriverDelivery, _ mq.NackOptions) error {
	return s.AckBatch(ctx, ds)
}

// TestBatchSettlerHandsEachCallerItsOwnFailure is two settlements coalesced
// into one batch that fails for each of them differently: one lost its lease
// to another consumer, and the other met a replica still loading, which the
// driver answers as retryable. Each caller must get its own answer. The batch
// used to collapse to its first error, so the caller told to retry heard that
// its lease was lost and never retried, and the message was processed twice;
// in the other order the lost lease was retried.
func TestBatchSettlerHandsEachCallerItsOwnFailure(t *testing.T) {
	ctx := context.Background()
	loading := &mq.OpError{Operation: mq.OpAck, Code: mq.CodeUnavailable, Retryable: true, Err: errors.New("LOADING")}
	driver := &collapsingDriverSettler{errs: map[uint64]error{1: mq.ErrLeaseLost, 2: loading}}
	bs, err := driverkit.NewBatchSettler(driver, driverkit.BatchConfig{Batching: driverkit.SettleCount, MaxItems: 2})
	if err != nil {
		t.Fatalf("NewBatchSettler: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	got := make(map[uint64]error, 2)
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, token := range []uint64{1, 2} {
		wg.Go(func() {
			err := bs.Ack(ctx, &mq.DriverDelivery{Message: &mq.Message{}, Token: token})
			mu.Lock()
			got[token] = err
			mu.Unlock()
		})
	}
	wg.Wait()
	if !errors.Is(got[1], mq.ErrLeaseLost) {
		t.Errorf("the caller whose lease was lost got %v, want ErrLeaseLost", got[1])
	}
	if got[2] != loading { //nolint:errorlint // identity: the caller's own answer from the driver
		t.Errorf("the caller the driver told to retry got %v (IsRetryable %v), want its own retryable %v",
			got[2], mq.IsRetryable(got[2]), loading)
	}
}

// slowDriverSettler takes a fixed round trip for every settlement.
type slowDriverSettler struct{ roundTrip time.Duration }

func (s slowDriverSettler) Ack(context.Context, *mq.DriverDelivery) error {
	time.Sleep(s.roundTrip)
	return nil
}

func (s slowDriverSettler) Nack(ctx context.Context, d *mq.DriverDelivery, _ mq.NackOptions) error {
	return s.Ack(ctx, d)
}

// TestATimedSettlementWaitsForTheTickAndItsRoundTrip pins the latency the
// SettleTime and SettleHybrid docs state: an Ack returns once the tick has
// sent the batch carrying it and that round trip has come back, and the
// ticker restarts only once a tick's sends have finished. So a settlement
// queued while a tick's round trip is under way waits for the rest of that
// round trip, a whole interval and its own round trip. The docs said the
// interval bounded the latency, which no settlement that needs a round trip
// can keep.
func TestATimedSettlementWaitsForTheTickAndItsRoundTrip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const interval, roundTrip = 50 * time.Millisecond, 20 * time.Millisecond
		bs, err := driverkit.NewBatchSettler(slowDriverSettler{roundTrip}, driverkit.BatchConfig{
			Batching: driverkit.SettleTime, Interval: interval,
		})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		ctx := context.Background()
		start := time.Now()
		first := make(chan time.Duration, 1)
		go func() {
			_ = bs.Ack(ctx, &mq.DriverDelivery{Message: &mq.Message{}, Token: 1})
			first <- time.Since(start)
		}()
		time.Sleep(interval + roundTrip/2)
		queued := time.Now()
		if err := bs.Ack(ctx, &mq.DriverDelivery{Message: &mq.Message{}, Token: 2}); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		second := time.Since(queued)
		if err := bs.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if got, want := <-first, interval+roundTrip; got != want {
			t.Errorf("the first Ack took %v, want %v: the interval to the tick and the round trip", got, want)
		}
		if want := roundTrip/2 + interval + roundTrip; second != want {
			t.Errorf("an Ack queued during a tick's round trip took %v, want %v: the rest of that round trip, "+
				"an interval and its own round trip", second, want)
		}
	})
}
