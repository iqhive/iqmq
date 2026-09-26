package iqmq_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestBackpressureModeNames pins the mode set and the reliability
// each mode claims: the two drop modes are never "reliable", which is what
// makes them opt-in.
func TestBackpressureModeNames(t *testing.T) {
	t.Parallel()
	if got := mq.BackpressureMode(0); got != mq.BackpressureBlock {
		t.Fatalf("the zero BackpressureMode is %v, want block: the specification makes blocking the default", got)
	}
	for mode, want := range map[mq.BackpressureMode]string{
		mq.BackpressureBlock:      "block",
		mq.BackpressureReject:     "reject",
		mq.BackpressureDropNewest: "dropnewest",
		mq.BackpressureDropOldest: "dropoldest",
	} {
		if got := mode.String(); got != want {
			t.Fatalf("BackpressureMode(%d).String() = %q, want %q", mode, got, want)
		}
		if !mode.Valid() {
			t.Fatalf("%v is not Valid", mode)
		}
	}
	if !mq.BackpressureBlock.Reliable() || !mq.BackpressureReject.Reliable() {
		t.Fatal("blocking and rejecting must be reliable: they never lose an admitted publication")
	}
	if mq.BackpressureDropNewest.Reliable() || mq.BackpressureDropOldest.Reliable() {
		t.Fatal("a drop mode must not claim to be reliable ")
	}
	if mq.BackpressureMode(9).Valid() {
		t.Fatal("BackpressureMode(9) is Valid")
	}
	if mq.DefaultMaxPendingMessages <= 0 {
		t.Fatal("the default pending bound must be positive: the specification bounds every buffer by default")
	}
}

// TestBackpressureBlockIsBounded is the specification's "every internal buffer is
// bounded by default", checked the only way a bound can be: by counting what
// the driver held at once and by watching the publication that does not fit
// actually wait.
//
// It is the defect class this project keeps finding — a buffer that "bounds"
// by growing — and a bound that is merely a counter nobody consults reads
// exactly like a real one from the outside.
func TestBackpressureBlockIsBounded(t *testing.T) {
	t.Parallel()
	const bound = 4
	b, p := advClient(t, openAsync, mq.PublisherConfig{
		Backpressure:       mq.BackpressureBlock,
		MaxPendingMessages: bound,
	})
	sink := advSinkOf[*advAsyncSink](t, b)
	ctx := context.Background()

	for i := range bound {
		if err := p.PublishAsync(ctx, &mq.Message{Body: []byte("x")}, nil); err != nil {
			t.Fatalf("PublishAsync(%d): %v", i, err)
		}
	}
	// The buffer is full. The next publication must wait, not grow it.
	parked := make(chan error, 1)
	go func() {
		parked <- p.PublishAsync(ctx, &mq.Message{Body: []byte("blocked")}, nil)
	}()
	select {
	case err := <-parked:
		t.Fatalf("the publication past the bound returned %v instead of waiting: the buffer is not bounded", err)
	case <-time.After(100 * time.Millisecond):
	}
	if got := sink.heldCount(); got != bound {
		t.Fatalf("the driver holds %d publications, want %d: the bound admitted more than it says", got, bound)
	}

	// One completion frees exactly one place.
	sink.complete(1, nil)
	select {
	case err := <-parked:
		if err != nil {
			t.Fatalf("the released publication returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the publication was not woken by the completion that freed its place ")
	}
	if got := b.concurrentMax(); got > bound {
		t.Fatalf("the driver held %d publications at once, want at most %d", got, bound)
	}
	sink.complete(sink.heldCount(), nil)
	if got := p.Stats().BackpressureEvents; got != 0 {
		t.Fatalf("Stats().BackpressureEvents = %d after a blocking publisher served everything, want 0", got)
	}
}

// TestBackpressureBlockManyProducersStayBounded runs the bound under
// concurrency, which is where a counter updated outside its lock lets one
// publication too many through.
func TestBackpressureBlockManyProducersStayBounded(t *testing.T) {
	t.Parallel()
	const (
		bound    = 3
		writers  = 8
		perWrite = 25
	)
	b, p := advClient(t, openAsync, mq.PublisherConfig{
		Backpressure:       mq.BackpressureBlock,
		MaxPendingMessages: bound,
	})
	sink := advSinkOf[*advAsyncSink](t, b)
	stop := make(chan struct{})
	var completer sync.WaitGroup
	completer.Add(1)
	go func() {
		defer completer.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if sink.heldCount() > 0 {
				sink.complete(1, nil)
				continue
			}
			time.Sleep(time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	var ok atomic.Int64
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWrite {
				if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil); err != nil {
					t.Errorf("PublishAsync: %v", err)
					return
				}
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	close(stop)
	completer.Wait()
	sink.complete(sink.heldCount(), nil)

	if got := ok.Load(); got != writers*perWrite {
		t.Fatalf("%d publications were admitted, want %d", got, writers*perWrite)
	}
	if got := b.concurrentMax(); got > bound {
		t.Fatalf("the driver held %d publications at once, want at most %d: the bound leaks under concurrency", got, bound)
	}
}

// TestBackpressureRejectRefusesBeforeItAllocates is the rejecting mode.
//
// "Before it allocates" is measured and not asserted by reading the code: a
// refusal that allocated would be a caller's denial-of-service against
// itself, and the refusal is exactly the path a client under load takes
// hardest.
func TestBackpressureRejectRefusesBeforeItAllocates(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openAsync, mq.PublisherConfig{
		Backpressure:       mq.BackpressureReject,
		MaxPendingMessages: 1,
	})
	sink := advSinkOf[*advAsyncSink](t, b)
	ctx := context.Background()
	if err := p.PublishAsync(ctx, &mq.Message{Body: []byte("first")}, nil); err != nil {
		t.Fatalf("the first PublishAsync: %v", err)
	}
	m := &mq.Message{Body: []byte("refused")}
	var ran atomic.Int64
	cb := func(mq.PublishResult) { ran.Add(1) }
	if err := p.PublishAsync(ctx, m, cb); !errors.Is(err, mq.ErrBackpressure) {
		t.Fatalf("PublishAsync over the bound = %v, want ErrBackpressure", err)
	}
	if got := sink.heldCount(); got != 1 {
		t.Fatalf("the driver holds %d publications, want 1: the refused publication reached it", got)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("the refused publication's callback ran %d times, want 0", got)
	}
	// The "before it allocates" half is measured in TestAllocsPublish, which
	// is not a parallel test: testing.AllocsPerRun refuses to run in one.
	if got := p.Stats().BackpressureEvents; got == 0 {
		t.Fatal("Stats().BackpressureEvents = 0 after a refusal, want the refusals counted ")
	}
	if got := p.Stats().Drops; got != 0 {
		t.Fatalf("Stats().Drops = %d for a rejecting publisher, want 0: a refusal is not a drop", got)
	}
	sink.complete(sink.heldCount(), nil)
}

// TestBackpressureDropNewestDropsAndCounts pins the specification's "opt-in, counted,
// never reliable" for the arriving publication.
func TestBackpressureDropNewestDropsAndCounts(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openAsync, mq.PublisherConfig{
		Backpressure:       mq.BackpressureDropNewest,
		MaxPendingMessages: 1,
	})
	sink := advSinkOf[*advAsyncSink](t, b)
	ctx := context.Background()
	if err := p.PublishAsync(ctx, &mq.Message{Body: []byte("kept")}, nil); err != nil {
		t.Fatalf("the first PublishAsync: %v", err)
	}
	var ran atomic.Int64
	err := p.PublishAsync(ctx, &mq.Message{Body: []byte("dropped")}, func(mq.PublishResult) { ran.Add(1) })
	if !errors.Is(err, mq.ErrBackpressure) {
		t.Fatalf("PublishAsync over the bound = %v, want ErrBackpressure", err)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("the dropped publication's callback ran %d times, want 0: the caller was told by the return value", got)
	}
	if got := p.Stats().Drops; got != 1 {
		t.Fatalf("Stats().Drops = %d, want 1: the specification requires a drop to be counted", got)
	}
	sink.complete(sink.heldCount(), nil)
	if got := b.bodies(); len(got) != 1 || got[0] != "kept" {
		t.Fatalf("the driver saw %v, want [kept]: a dropped publication must not reach the broker", got)
	}
}

// TestBackpressureDropOldestDiscardsTheQueuedPublication pins the other drop
// mode: the oldest publication still in the batching buffer is discarded, its
// callback runs exactly once with ErrBackpressure, and the arriving
// publication is admitted in its place.
func TestBackpressureDropOldestDiscardsTheQueuedPublication(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openBatch, mq.PublisherConfig{
		Backpressure:       mq.BackpressureDropOldest,
		MaxPendingMessages: 2,
		// A batch that only ever leaves on a count of four never leaves in
		// this test, so the two publications stay queued and reachable.
		Batching: mq.BatchingConfig{MaxMessages: 4},
	})
	ctx := context.Background()
	outcome := make(chan string, 4)
	for _, body := range []string{"oldest", "newer"} {
		if err := p.PublishAsync(ctx, &mq.Message{Body: []byte(body)}, func(r mq.PublishResult) {
			outcome <- body + ":" + errText(r.Err)
		}); err != nil {
			t.Fatalf("PublishAsync(%s): %v", body, err)
		}
	}
	if err := p.PublishAsync(ctx, &mq.Message{Body: []byte("newest")}, func(r mq.PublishResult) {
		outcome <- "newest:" + errText(r.Err)
	}); err != nil {
		t.Fatalf("PublishAsync(newest): %v", err)
	}
	select {
	case got := <-outcome:
		if got != "oldest:backpressure" {
			t.Fatalf("the dropped publication reported %q, want %q", got, "oldest:backpressure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the oldest publication was never dropped: BackpressureDropOldest admitted past its bound")
	}
	if got := p.Stats().Drops; got != 1 {
		t.Fatalf("Stats().Drops = %d, want 1", got)
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	got := map[string]bool{}
	for range 2 {
		select {
		case s := <-outcome:
			got[s] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of the two surviving publications reported: %v", len(got), got)
		}
	}
	if !got["newer:ok"] || !got["newest:ok"] {
		t.Fatalf("the surviving publications reported %v, want newer:ok and newest:ok", got)
	}
	select {
	case s := <-outcome:
		t.Fatalf("a fourth callback ran: %q", s)
	default:
	}
	if bodies := b.bodies(); len(bodies) != 2 {
		t.Fatalf("the driver saw %v, want the two survivors: a dropped publication must not reach the broker", bodies)
	} else {
		for _, body := range bodies {
			if body == "oldest" {
				t.Fatalf("the driver saw %v: the dropped publication was published anyway", bodies)
			}
		}
	}
}

// TestBackpressureDropOldestFallsBackToRejecting pins the degradation: a
// publisher with nothing queued has nothing it can recall, so the mode
// refuses rather than parking a caller that did not ask to wait.
func TestBackpressureDropOldestFallsBackToRejecting(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openAsync, mq.PublisherConfig{
		Backpressure:       mq.BackpressureDropOldest,
		MaxPendingMessages: 1,
	})
	sink := advSinkOf[*advAsyncSink](t, b)
	ctx := context.Background()
	if err := p.PublishAsync(ctx, &mq.Message{Body: []byte("at-the-driver")}, nil); err != nil {
		t.Fatalf("the first PublishAsync: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- p.PublishAsync(ctx, &mq.Message{Body: []byte("x")}, nil) }()
	select {
	case err := <-done:
		if !errors.Is(err, mq.ErrBackpressure) {
			t.Fatalf("PublishAsync = %v, want ErrBackpressure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the publication parked: BackpressureDropOldest must not wait when it has nothing to recall")
	}
	sink.complete(sink.heldCount(), nil)
}

// TestBackpressureBlockIsUnblockedByClose is the specification's "Close unblocks blocked
// operations", for the one operation of this increment that really blocks.
//
// It is also the lifecycle's own rule, stated in internal/lifecycle's package
// comment: "the section between Enter and Leave must not block ... an
// operation that parks inside the gate parks the shutdown with it. Park
// outside the gate and take a ticket for the placement itself." A publication
// that waited for buffer room while holding a publication ticket deadlocks
// against its own Close — the close waits for the gate to drain, and the
// parked publication waits for room only the close would free — so the
// failure is a hang and not a wrong answer, and nothing else in this suite
// reaches it: every other blocking test frees the room itself.
func TestBackpressureBlockIsUnblockedByClose(t *testing.T) {
	t.Parallel()
	b := newAdvBroker(openAsync)
	cl := mq.NewClient(b)
	p, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders", MaxPendingMessages: 1})
	if err != nil {
		t.Fatalf("opening the publisher: %v", err)
	}
	sink := advSinkOf[*advAsyncSink](t, b)
	if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("held")}, nil); err != nil {
		t.Fatalf("the first PublishAsync: %v", err)
	}
	parked := make(chan error, 1)
	go func() { parked <- p.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil) }()
	select {
	case err := <-parked:
		t.Fatalf("the publication past the bound returned %v instead of waiting", err)
	case <-time.After(100 * time.Millisecond):
	}
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Close never returned: the publication parked inside the publication gate, so the close waits for the gate to drain and the publication waits for room the close would have freed")
	}
	select {
	case err := <-parked:
		if !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("the parked publication = %v, want ErrClosed", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the parked publication was never released by Close ")
	}
	sink.complete(sink.heldCount(), nil)
	_ = cl.Close()
}

// TestBackpressureBlockHonoursTheCallersContext pins the specification's "cancellation
// unblocks local admission".
func TestBackpressureBlockHonoursTheCallersContext(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openAsync, mq.PublisherConfig{MaxPendingMessages: 1})
	sink := advSinkOf[*advAsyncSink](t, b)
	if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("first")}, nil); err != nil {
		t.Fatalf("the first PublishAsync: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.PublishAsync(ctx, &mq.Message{Body: []byte("x")}, nil) }()
	select {
	case err := <-done:
		t.Fatalf("PublishAsync returned %v instead of waiting for room", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the cancelled publication = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelling the context did not unblock admission ")
	}
	sink.complete(sink.heldCount(), nil)
}

// TestBackpressureBlockHonoursTheTimeout pins PublisherConfig.Timeout: it
// bounds the wait the core itself performs, on the injected clock, and never
// by deriving a context per message.
func TestBackpressureBlockHonoursTheTimeout(t *testing.T) {
	t.Parallel()
	clk := newFixedClock()
	b := newAdvBroker(openAsync)
	p := advPublisher(t, b, mq.PublisherConfig{
		MaxPendingMessages: 1,
		Timeout:            2 * time.Second,
	}, mq.WithClock(clk))
	sink := advSinkOf[*advAsyncSink](t, b)
	if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("first")}, nil); err != nil {
		t.Fatalf("the first PublishAsync: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- p.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil) }()
	// Wait until the publication is really parked on a timer of the injected
	// clock, so that advancing the clock is what ends its wait.
	waitForTimers(t, clk, 1)
	clk.advance(3 * time.Second)
	select {
	case err := <-done:
		if mq.CodeOf(err) != mq.CodeDeadlineExceeded {
			t.Fatalf("the timed-out publication = %v (code %v), want CodeDeadlineExceeded", err, mq.CodeOf(err))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("PublisherConfig.Timeout did not bound the wait for buffer room")
	}
	sink.complete(sink.heldCount(), nil)
}

// waitForTimers blocks until the fake clock has handed out at least n live
// timers, so a test can advance the clock knowing what is waiting on it.
func waitForTimers(tb testing.TB, clk *fixedClock, n int) {
	tb.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if clk.liveTimers() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	tb.Fatalf("only %d timers were armed, want %d", clk.liveTimers(), n)
}

// errText renders an outcome as a word a test can compare.
func errText(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, mq.ErrBackpressure):
		return "backpressure"
	case errors.Is(err, mq.ErrClosed):
		return "closed"
	case mq.CodeOf(err) == mq.CodeSettlementUnknown:
		return "unknown"
	default:
		return err.Error()
	}
}
