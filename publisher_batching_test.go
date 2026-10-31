package iqmq_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// TestBatchingLeavesWhenFull pins the size trigger: a batch of MaxMessages
// leaves on its own, in one native call, and every publication in it gets
// exactly one callback.
func TestBatchingLeavesWhenFull(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openBatch, mq.PublisherConfig{
		Batching: mq.BatchingConfig{MaxMessages: 4},
	})
	outcomes := make(chan error, 8)
	for i := range 4 {
		body := []byte{byte('a' + i)}
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: body}, func(r mq.PublishResult) {
			outcomes <- r.Err
		}); err != nil {
			t.Fatalf("PublishAsync(%d): %v", i, err)
		}
	}
	for i := range 4 {
		select {
		case err := <-outcomes:
			if err != nil {
				t.Fatalf("callback %d reported %v, want nil", i, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of 4 callbacks ran: a full batch must leave without a Flush", i)
		}
	}
	if got := b.bodies(); len(got) != 4 {
		t.Fatalf("the driver saw %v, want four messages", got)
	}
}

// TestBatchingSizeOnlyWaitsForAFlush pins the other half of the same trigger:
// a publisher that named no Linger promised no latency bound, so a partly
// filled batch waits for the caller.
//
// It is the configuration the winner arms nothing for, and it is the
// one whose latency a caller has to understand — which is why it is pinned
// rather than left to the batcher.
func TestBatchingSizeOnlyWaitsForAFlush(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openBatch, mq.PublisherConfig{
		Batching: mq.BatchingConfig{MaxMessages: 100},
	})
	var ran atomic.Int64
	for i := range 3 {
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte{byte('a' + i)}}, func(mq.PublishResult) {
			ran.Add(1)
		}); err != nil {
			t.Fatalf("PublishAsync(%d): %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if got := b.nativeCalls(); got != 0 {
		t.Fatalf("the driver saw %d publications, want 0: a partly filled batch with no Linger waits", got)
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := ran.Load(); got != 3 {
		t.Fatalf("%d callbacks ran after Flush, want 3", got)
	}
	if got := len(b.bodies()); got != 3 {
		t.Fatalf("the driver saw %d publications after Flush, want 3", got)
	}
}

// TestBatchingLingerLeavesOnTheDeadline pins the timed trigger on the
// injected clock: a publisher that named a Linger promised the batch would
// leave within it, and nothing but the clock makes it happen.
func TestBatchingLingerLeavesOnTheDeadline(t *testing.T) {
	t.Parallel()
	clk := newFixedClock()
	b := newAdvBroker(openBatch)
	p := advPublisher(t, b, mq.PublisherConfig{
		Batching: mq.BatchingConfig{MaxMessages: 100, Linger: 20 * time.Millisecond},
	}, mq.WithClock(clk))
	done := make(chan error, 2)
	for i := range 2 {
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte{byte('a' + i)}}, func(r mq.PublishResult) {
			done <- r.Err
		}); err != nil {
			t.Fatalf("PublishAsync(%d): %v", i, err)
		}
	}
	// The batch is armed on the injected clock, so nothing happens until the
	// clock moves.
	waitForTimers(t, clk, 1)
	if got := b.nativeCalls(); got != 0 {
		t.Fatalf("the driver saw %d publications before the linger deadline, want 0", got)
	}
	clk.advance(25 * time.Millisecond)
	for i := range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("callback %d reported %v", i, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of 2 callbacks ran: the linger deadline did not send the batch", i)
		}
	}
	if got := len(b.bodies()); got != 2 {
		t.Fatalf("the driver saw %d publications, want 2", got)
	}
}

// TestBatchingSettlesQueuedPublicationsOnClose is the recurring defect class
// of this project, on the publisher's side: a record placed into a buffer
// while the handle is closing.
//
// A message queued in the batching buffer when Close arrives was never handed
// to the driver, so its callback runs exactly once and says so definitely —
// ErrClosed and not an unknown outcome — and the publisher's own pending
// count returns to zero, because a callback that never ran is a reservation
// nothing ever gives back.
func TestBatchingSettlesQueuedPublicationsOnClose(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openBatch, mq.PublisherConfig{
		Batching: mq.BatchingConfig{MaxMessages: 100},
	})
	outcomes := make(chan string, 8)
	for i := range 3 {
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte{byte('a' + i)}}, func(r mq.PublishResult) {
			outcomes <- errText(r.Err)
		}); err != nil {
			t.Fatalf("PublishAsync(%d): %v", i, err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i := range 3 {
		select {
		case got := <-outcomes:
			if got != "closed" {
				t.Fatalf("callback %d reported %q, want closed: the message never reached the driver", i, got)
			}
		default:
			t.Fatalf("only %d of 3 callbacks had run when Close returned", i)
		}
	}
	if got := b.nativeCalls(); got != 0 {
		t.Fatalf("the driver saw %d publications, want 0: a prompt Close does not flush ", got)
	}
	if got := p.Stats().Pending; got != 0 {
		t.Fatalf("Stats().Pending = %d after Close, want 0", got)
	}
}

// TestBatchingShutdownFlushesQueuedPublications is the graceful half: the specification's
// Flushing phase sends what was admitted rather than abandoning it.
func TestBatchingShutdownFlushesQueuedPublications(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openBatch, mq.PublisherConfig{
		Batching: mq.BatchingConfig{MaxMessages: 100},
	})
	outcomes := make(chan string, 8)
	for i := range 3 {
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte{byte('a' + i)}}, func(r mq.PublishResult) {
			outcomes <- errText(r.Err)
		}); err != nil {
			t.Fatalf("PublishAsync(%d): %v", i, err)
		}
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	for i := range 3 {
		select {
		case got := <-outcomes:
			if got != "ok" {
				t.Fatalf("callback %d reported %q, want ok: a graceful shutdown flushes what it admitted", i, got)
			}
		default:
			t.Fatalf("only %d of 3 callbacks had run when Shutdown returned", i)
		}
	}
	if got := len(b.bodies()); got != 3 {
		t.Fatalf("the driver saw %d publications, want 3", got)
	}
}

// TestBatchingRacesPublishAgainstClose is the same defect class under load.
//
// The known instances of this bug in the engine appeared at five to twenty
// failures per two or three thousand rounds, so the count matters: a handful
// of rounds proves nothing. What it checks is the conservation the publisher
// owes — every admitted publication gets exactly one callback — while Close
// runs concurrently with the placement into the batching buffer.
func TestBatchingRacesPublishAgainstClose(t *testing.T) {
	t.Parallel()
	const rounds = 2000
	for round := range rounds {
		b := newAdvBroker(openBatch)
		cl := mq.NewClient(b)
		p, err := cl.Publisher(context.Background(), mq.PublisherConfig{
			Topic:    "orders",
			Batching: mq.BatchingConfig{MaxMessages: 8},
		})
		if err != nil {
			t.Fatalf("round %d: opening the publisher: %v", round, err)
		}
		var (
			admitted atomic.Int64
			ran      atomic.Int64
			late     atomic.Int64
			closed   atomic.Bool
		)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for range 5 {
					err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, func(mq.PublishResult) {
						if closed.Load() {
							late.Add(1)
						}
						ran.Add(1)
					})
					if err == nil {
						admitted.Add(1)
						continue
					}
					if !errors.Is(err, mq.ErrClosed) && !errors.Is(err, mq.ErrBackpressure) {
						t.Errorf("round %d: PublishAsync: %v", round, err)
						return
					}
				}
			}()
		}
		closer := make(chan struct{})
		go func() {
			<-start
			_ = p.Close()
			closed.Store(true)
			close(closer)
		}()
		close(start)
		wg.Wait()
		<-closer
		if got, want := ran.Load(), admitted.Load(); got != want {
			t.Fatalf("round %d: %d publications were admitted and %d callbacks ran: a publication placed into the batch while the publisher closed was never released",
				round, want, got)
		}
		if got := late.Load(); got != 0 {
			t.Fatalf("round %d: %d callbacks arrived after Close returned", round, got)
		}
		if got := p.Stats().Pending; got != 0 {
			t.Fatalf("round %d: Stats().Pending = %d after Close, want 0", round, got)
		}
		_ = cl.Close()
	}
}

// TestBatchingKeepsPerPublicationOptions pins that batching does not flatten
// the option sets: publications whose sets differ are handed to the driver
// under their own, because a native batch carries one set for every message
// in it.
func TestBatchingKeepsPerPublicationOptions(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openBatch, mq.PublisherConfig{
		Batching: mq.BatchingConfig{MaxMessages: 100},
	})
	keys := []string{"one", "one", "two", "one"}
	for i, key := range keys {
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte{byte('a' + i)}}, nil, mq.DedupKey(key)); err != nil {
			t.Fatalf("PublishAsync(%d): %v", i, err)
		}
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	got := b.records()
	if len(got) != len(keys) {
		t.Fatalf("the driver saw %d publications, want %d", len(got), len(keys))
	}
	for i := range keys {
		if got[i].dedup != keys[i] {
			t.Fatalf("publication %d carried dedup key %q, want %q: batching flattened the option sets",
				i, got[i].dedup, keys[i])
		}
	}
}

// TestBatchingReportsPerPublicationFailures pins the alignment a batch's
// callbacks depend on: one message's failure is not reported as another's.
func TestBatchingReportsPerPublicationFailures(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openBatch, mq.PublisherConfig{
		Batching: mq.BatchingConfig{MaxMessages: 100},
	})
	refused := errors.New("refused")
	b.setFail(func(n int, _ *mq.Message) error {
		if n == 2 {
			return refused
		}
		return nil
	})
	var mu sync.Mutex
	outcome := map[string]string{}
	for i := range 3 {
		body := string(rune('a' + i))
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte(body)}, func(r mq.PublishResult) {
			mu.Lock()
			outcome[body] = errText(r.Err)
			mu.Unlock()
		}); err != nil {
			t.Fatalf("PublishAsync(%d): %v", i, err)
		}
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := map[string]string{"a": "ok", "b": "refused", "c": "ok"}
	for body, wantText := range want {
		if outcome[body] != wantText {
			t.Fatalf("the callbacks reported %v, want %v: a batch's results must stay aligned with its messages", outcome, want)
		}
	}
}

// TestBatchingStopsEveryLaterRunAfterAFatalError is the flusher's half of
// the same rule: a batch flushed as two option runs whose first run meets a
// closed sink settles the second run as not attempted, without offering it
// to the sink.
func TestBatchingStopsEveryLaterRunAfterAFatalError(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openBatch, mq.PublisherConfig{
		Batching: mq.BatchingConfig{MaxMessages: 100},
	})
	b.setFail(func(n int, _ *mq.Message) error {
		if n == 1 {
			return mq.ErrClosed
		}
		return nil
	})
	var mu sync.Mutex
	outcome := map[string]error{}
	for i, key := range []string{"one", "two"} {
		body := string(rune('a' + i))
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte(body)}, func(r mq.PublishResult) {
			mu.Lock()
			outcome[body] = r.Err
			mu.Unlock()
		}, mq.DedupKey(key)); err != nil {
			t.Fatalf("PublishAsync(%d): %v", i, err)
		}
	}
	// Flush reports the batch's failures, which this test reads from the
	// callbacks instead.
	_ = p.Flush(context.Background())
	if got := b.nativeCalls(); got != 1 {
		t.Fatalf("the driver saw %d publications, want 1: the second run went to a closed sink", got)
	}
	mu.Lock()
	defer mu.Unlock()
	got, ok := outcome["b"]
	if !ok || !errors.Is(got, mq.ErrClosed) || mq.IsAmbiguous(got) || !mq.IsRetryable(got) {
		t.Fatalf("the second run's publication settled with %v, want a definite, retryable not-attempted ErrClosed", got)
	}
}

// stopSink is a native batcher that answers the way drivers/activemq/stomp's
// PublishBatch does: it stops at the first message whose body is "a" or "c"
// with ErrClosed, answers every message behind it as never attempted
// (definite, retryable, with ErrClosed's code), and hands the per-message
// answers to driverkit.CollapseBatch, which may collapse that shape to the
// first error.
type stopSink struct{ advSink }

func openStop(b *advBroker, cfg mq.SinkConfig) mq.Sink {
	return &stopSink{advSink{b: b, receipts: cfg.Receipts}}
}

func (s *stopSink) PublishBatch(_ context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	errs := make([]error, len(ms))
	for i, m := range ms {
		if body := string(m.Body); body != "a" && body != "c" {
			errs[i] = s.b.accept(m, o, s.receipts)
			continue
		}
		errs[i] = mq.ErrClosed
		for j := i + 1; j < len(ms); j++ {
			errs[j] = &mq.OpError{
				Driver: "adv", Operation: mq.OpPublish, Topic: "orders",
				Code: mq.CodeClosed, Retryable: true, Err: errcode.ErrNotAttempted,
			}
		}
		break
	}
	return driverkit.CollapseBatch(errs)
}

// closedSink is the same stop through the mandatory sink alone.
type closedSink struct{ advSink }

func openClosedPlain(b *advBroker, cfg mq.SinkConfig) mq.Sink {
	return &closedSink{advSink{b: b, receipts: cfg.Receipts}}
}

func (s *closedSink) Publish(context.Context, *mq.Message, *mq.PublishOptions) error {
	return mq.ErrClosed
}

// TestACollapsedStopKeepsTheNotAttemptedAnswersBehindIt is a native batcher
// that stopped at a message with ErrClosed and collapsed [ErrClosed, not
// attempted, ...] to ErrClosed, which driverkit.CollapseBatch allows. The
// core handed that one error to every message of the piece, so a message the
// driver never attempted read ErrClosed, not retryable, while the same stop
// through a sink without a batch method answered it as not attempted,
// definite and retryable. A caller retrying the retryable entries dropped it
// and republished the message behind it past it. Each message behind the
// stop now gets its own not-attempted answer, under Batching and in a split
// PublishBatch alike.
func TestACollapsedStopKeepsTheNotAttemptedAnswersBehindIt(t *testing.T) {
	t.Parallel()
	notAttempted := func(err error) bool {
		return errors.Is(err, mq.ErrClosed) && !mq.IsAmbiguous(err) && mq.IsRetryable(err)
	}
	for name, open := range map[string]func(*advBroker, mq.SinkConfig) mq.Sink{"plain": openClosedPlain, "native": openStop} {
		t.Run("PublishAsync with Batching/"+name, func(t *testing.T) {
			t.Parallel()
			_, p := advClient(t, open, mq.PublisherConfig{Batching: mq.BatchingConfig{MaxMessages: 3}})
			var mu sync.Mutex
			got := make([]error, 3)
			var wg sync.WaitGroup
			wg.Add(3)
			for i, body := range []string{"a", "b", "c"} {
				if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte(body)}, func(r mq.PublishResult) {
					mu.Lock()
					got[i] = r.Err
					mu.Unlock()
					wg.Done()
				}); err != nil {
					t.Fatalf("PublishAsync(%s): %v", body, err)
				}
			}
			wg.Wait()
			mu.Lock()
			defer mu.Unlock()
			if !errors.Is(got[0], mq.ErrClosed) || mq.IsAmbiguous(got[0]) {
				t.Errorf("message 0 settled with %v, want the definite ErrClosed that stopped the batch", got[0])
			}
			for i, err := range got[1:] {
				if !notAttempted(err) {
					t.Errorf("message %d settled with %v (IsRetryable %v), want its own definite, retryable "+
						"not-attempted answer", i+1, err, mq.IsRetryable(err))
				}
			}
		})
	}
	t.Run("PublishBatch split", func(t *testing.T) {
		t.Parallel()
		// Pieces of two: the first is accepted, the second's driver stops at
		// "c" and collapses its answer, and the core fills the third as not
		// attempted. "d" and "e" were both never offered to the broker.
		b := newAdvBroker(openStop)
		b.caps = advCapsWithBatchLimit(2, mq.FeatureOrderedByKey)
		p := advPublisher(t, b, mq.PublisherConfig{})
		ms := make([]*mq.Message, 0, 6)
		for _, body := range []string{"x", "y", "c", "d", "e", "f"} {
			ms = append(ms, &mq.Message{Body: []byte(body)})
		}
		err := p.PublishBatch(context.Background(), ms)
		var be *mq.BatchError
		if !errors.As(err, &be) || len(be.Errs) != len(ms) {
			t.Fatalf("PublishBatch = %v, want a *BatchError with %d results", err, len(ms))
		}
		if !errors.Is(be.Errs[2], mq.ErrClosed) || mq.IsAmbiguous(be.Errs[2]) {
			t.Errorf("c = %v, want the definite ErrClosed that stopped the batch", be.Errs[2])
		}
		for i := 3; i < len(ms); i++ {
			if e := be.Errs[i]; !notAttempted(e) {
				t.Errorf("%s (never attempted) = %v (IsRetryable %v), want a definite, retryable not-attempted answer",
					ms[i].Body, e, mq.IsRetryable(e))
			}
		}
	})
}

// TestFlushDoesNotWaitForPublicationsAdmittedAfterIt is Flush's promise that
// it completes the publications already admitted. It waited for the
// completion table to be empty instead, so a publication admitted while it
// waited was waited for too. Under Batching with no Linger that one sits in a
// partial batch nothing sends, and Flush waited for its context, or for ever
// with context.Background; under constant publishing it chased new arrivals.
func TestFlushDoesNotWaitForPublicationsAdmittedAfterIt(t *testing.T) {
	t.Parallel()
	t.Run("one admitted while it waits", func(t *testing.T) {
		t.Parallel()
		_, p := advClient(t, openBatch, mq.PublisherConfig{
			Callbacks: mq.CallbackDeferred,
			Batching:  mq.BatchingConfig{MaxMessages: 100},
		})
		started := make(chan struct{})
		release := make(chan struct{})
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m1")}, func(mq.PublishResult) {
			close(started)
			<-release
		}); err != nil {
			t.Fatalf("PublishAsync(m1): %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		flushed := make(chan error, 1)
		go func() { flushed <- p.Flush(ctx) }()
		// m1's callback runs once the Flush has sent it; m2 is admitted
		// after the Flush began, into a batch that nothing fills.
		<-started
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m2")}, nil); err != nil {
			t.Fatalf("PublishAsync(m2): %v", err)
		}
		close(release)
		if err := <-flushed; err != nil {
			t.Fatalf("Flush = %v, want nil: it waited for m2, which was admitted after it began and is in a batch nothing sends", err)
		}
	})
	for _, mode := range []mq.CallbackMode{mq.CallbackInline, mq.CallbackDeferred} {
		t.Run("under constant publishing/"+mode.String(), func(t *testing.T) {
			t.Parallel()
			_, p := advClient(t, openBatch, mq.PublisherConfig{
				Callbacks: mode,
				Batching:  mq.BatchingConfig{MaxMessages: 64},
			})
			stop := make(chan struct{})
			var wg sync.WaitGroup
			for range 4 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						_ = p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, nil)
					}
				}()
			}
			defer func() {
				close(stop)
				wg.Wait()
			}()
			for i := range 20 {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := p.Flush(ctx)
				cancel()
				if err != nil {
					t.Fatalf("Flush %d = %v, want nil: it chased the publications admitted after it", i, err)
				}
			}
		})
	}
}

// countingFlushSink is a sink whose driver flush counts its calls, holds the
// first one until gate is closed, and fails the call numbered failOn.
type countingFlushSink struct {
	advSink
	gate    chan struct{}
	entered chan struct{}
	failOn  int
	refusal error

	fmu   sync.Mutex
	calls int
}

func (s *countingFlushSink) Flush(context.Context) error {
	s.fmu.Lock()
	s.calls++
	n := s.calls
	s.fmu.Unlock()
	if n == 1 {
		close(s.entered)
		<-s.gate
	}
	if n == s.failOn {
		return s.refusal
	}
	return nil
}

func (s *countingFlushSink) flushes() int {
	s.fmu.Lock()
	defer s.fmu.Unlock()
	return s.calls
}

// TestConcurrentFlushCallsShareTheWork is Flush calls made at once, the
// "publish, then Flush, then settle" pattern of concurrent handlers. Since
// one Flush at a time moves the flush generation, they queued and each paid a
// whole driver flush in turn. A Flush waiting its turn now returns once one
// that took its turn after it was called has finished without error, so
// eight Flush calls, seven of which arrive while the first is in the driver,
// cost two driver flushes: the one in flight, which began before the seven
// and covers none of them, and the next, which covers the rest. A Flush that
// failed covers nobody: the others go on and flush themselves, here at a
// third driver flush, and only the one that failed reports the refusal.
func TestConcurrentFlushCallsShareTheWork(t *testing.T) {
	t.Parallel()
	refused := errors.New("the broker refused the flush")
	for _, tc := range []struct {
		name    string
		failOn  int
		flushes int
		failed  int
	}{
		{"all succeed", 0, 2, 0},
		{"the covering flush fails", 2, 3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				gate, entered := make(chan struct{}), make(chan struct{})
				b := newAdvBroker(func(b *advBroker, cfg mq.SinkConfig) mq.Sink {
					return &countingFlushSink{
						advSink: advSink{b: b, receipts: cfg.Receipts},
						gate:    gate, entered: entered, failOn: tc.failOn, refusal: refused,
					}
				})
				p := advPublisher(t, b, mq.PublisherConfig{})
				sink := advSinkOf[*countingFlushSink](t, b)
				publishThenFlush := func() error {
					if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, nil); err != nil {
						return err
					}
					return p.Flush(context.Background())
				}
				results := make(chan error, 8)
				go func() { results <- publishThenFlush() }()
				<-entered
				for range 7 {
					go func() { results <- publishThenFlush() }()
				}
				// Every one of the seven is waiting for its turn.
				synctest.Wait()
				close(gate)
				failed := 0
				for range 8 {
					switch err := <-results; {
					case errors.Is(err, refused):
						failed++
					case err != nil:
						t.Errorf("Flush = %v, want nil or the driver's refusal", err)
					}
				}
				if got := sink.flushes(); got != tc.flushes {
					t.Errorf("8 Flush calls made %d driver flushes, want %d", got, tc.flushes)
				}
				if failed != tc.failed {
					t.Errorf("%d Flush calls reported the driver's refusal, want %d: a Flush that failed must not answer for the others", failed, tc.failed)
				}
			})
		})
	}
}

// TestAFlushWaitingItsTurnStillWaitsForItsPublications is the other half of
// sharing: a Flush waiting its turn is not released by a Flush that covers
// it only in part. Here the Flush in flight began before W published, and W's
// publication is still at the driver when that Flush finishes, so W, and X
// behind it, wait until it completes.
func TestAFlushWaitingItsTurnStillWaitsForItsPublications(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b, p := advClient(t, openAsync, mq.PublisherConfig{})
		sink := advSinkOf[*advAsyncSink](t, b)
		var mu sync.Mutex
		var events []string
		note := func(s string) {
			mu.Lock()
			events = append(events, s)
			mu.Unlock()
		}
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("a0")}, func(mq.PublishResult) { note("a0 done") }); err != nil {
			t.Fatalf("PublishAsync(a0): %v", err)
		}
		first := make(chan error, 1)
		go func() { first <- p.Flush(context.Background()) }()
		synctest.Wait()
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, func(mq.PublishResult) { note("m done") }); err != nil {
			t.Fatalf("PublishAsync(m): %v", err)
		}
		w, x := make(chan error, 1), make(chan error, 1)
		go func() {
			err := p.Flush(context.Background())
			note("W returned")
			w <- err
		}()
		go func() { x <- p.Flush(context.Background()) }()
		synctest.Wait()
		sink.complete(1, nil) // a0
		if err := <-first; err != nil {
			t.Fatalf("the first Flush = %v", err)
		}
		synctest.Wait()
		select {
		case err := <-w:
			t.Fatalf("W returned %v while its publication m was still at the driver", err)
		case err := <-x:
			t.Fatalf("X returned %v while m, published before it was called, was still at the driver", err)
		default:
		}
		sink.complete(1, nil) // m
		if err := <-w; err != nil {
			t.Errorf("W = %v, want nil", err)
		}
		if err := <-x; err != nil {
			t.Errorf("X = %v, want nil", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if !slices.Equal(events, []string{"a0 done", "m done", "W returned"}) {
			t.Errorf("events = %v, want m's callback before W returned", events)
		}
	})
}

// stuckSink is a broker that has stopped answering. Its Publish returns once
// its context ends or the sink is closed or, with ignoreCtx set, only once
// the sink is closed: a driver call that honours its context, and one that
// only closing the native client can end.
type stuckSink struct {
	ignoreCtx bool
	entered   chan struct{}
	closed    chan struct{}
	once      sync.Once
}

func openStuck(ignoreCtx bool) func(*advBroker, mq.SinkConfig) mq.Sink {
	return func(*advBroker, mq.SinkConfig) mq.Sink {
		return &stuckSink{ignoreCtx: ignoreCtx, entered: make(chan struct{}, 64), closed: make(chan struct{})}
	}
}

func (s *stuckSink) Publish(ctx context.Context, _ *mq.Message, _ *mq.PublishOptions) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	done := ctx.Done()
	if s.ignoreCtx {
		done = nil
	}
	select {
	case <-done:
		return ctx.Err()
	case <-s.closed:
		return mq.ErrClosed
	}
}

func (s *stuckSink) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

// TestBatchingFlushAndShutdownAreBoundedByTheirContext is Flush's "ctx is how
// a caller bounds it" under Batching. Flush, Shutdown and the client's
// Shutdown used to hand the batch to the driver themselves, under the
// flusher's context and not the caller's, so a broker that stopped answering
// held them for ever, and a Close behind them too. Each now returns at its
// context's deadline, and the close that follows ends the send.
func TestBatchingFlushAndShutdownAreBoundedByTheirContext(t *testing.T) {
	calls := []struct {
		name string
		call func(ctx context.Context, cl *mq.Client, p *mq.PublisherHandle) error
	}{
		{"Flush", func(ctx context.Context, _ *mq.Client, p *mq.PublisherHandle) error { return p.Flush(ctx) }},
		{"Shutdown", func(ctx context.Context, _ *mq.Client, p *mq.PublisherHandle) error { return p.Shutdown(ctx) }},
		{"client Shutdown", func(ctx context.Context, cl *mq.Client, _ *mq.PublisherHandle) error { return cl.Shutdown(ctx) }},
	}
	for _, c := range calls {
		for _, ignoreCtx := range []bool{false, true} {
			name := c.name + "/driver honours its context"
			if ignoreCtx {
				name = c.name + "/driver ends only on close"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					b := newAdvBroker(openStuck(ignoreCtx))
					cl := mq.NewClient(b)
					p, err := cl.Publisher(context.Background(), mq.PublisherConfig{
						Topic:    "orders",
						Batching: mq.BatchingConfig{MaxMessages: 100},
					})
					if err != nil {
						t.Fatalf("opening the publisher: %v", err)
					}
					sink := advSinkOf[*stuckSink](t, b)
					ran := make(chan error, 2)
					if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("a")}, func(r mq.PublishResult) {
						ran <- r.Err
					}); err != nil {
						t.Fatalf("PublishAsync: %v", err)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					defer cancel()
					start := time.Now()
					ret := make(chan error, 1)
					go func() { ret <- c.call(ctx, cl, p) }()
					select {
					case err := <-ret:
						if elapsed := time.Since(start); elapsed != 100*time.Millisecond || !errors.Is(err, context.DeadlineExceeded) {
							t.Errorf("%s = %v after %v, want context.DeadlineExceeded at 100ms", c.name, err, elapsed)
						}
					case <-time.After(time.Hour):
						t.Errorf("%s(a 100ms context) had not returned an hour later: it waited for the driver under the flusher's context", c.name)
						_ = sink.Close()
						t.Logf("%s returned %v once the sink closed", c.name, <-ret)
					}
					closed := make(chan struct{})
					go func() {
						_ = cl.Close()
						close(closed)
					}()
					select {
					case <-closed:
					case <-time.After(time.Hour):
						t.Errorf("a Close behind %s had not returned an hour later", c.name)
						_ = sink.Close()
						<-closed
					}
					select {
					case err := <-ran:
						if err == nil {
							t.Errorf("the publication reported success from a broker that never answered")
						}
					default:
						t.Errorf("the publication's callback had not run when Close returned")
					}
				})
			})
		}
	}
}

// TestBatchingCloseEndsAFlusherSendOnlyTheSinkCanEnd is a prompt Close behind
// a batch the flusher is sending to a driver that only its own Close can
// stop. The close waited for the flusher before it closed the sink, and the
// flusher waited for the sink: a deadlock. Close now closes the sink first,
// which ends the send, and the publication is told the driver's answer.
func TestBatchingCloseEndsAFlusherSendOnlyTheSinkCanEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newAdvBroker(openStuck(true))
		p := advPublisher(t, b, mq.PublisherConfig{Batching: mq.BatchingConfig{MaxMessages: 1}})
		sink := advSinkOf[*stuckSink](t, b)
		ran := make(chan error, 2)
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("a")}, func(r mq.PublishResult) {
			ran <- r.Err
		}); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
		<-sink.entered
		closed := make(chan error, 1)
		go func() { closed <- p.Close() }()
		select {
		case err := <-closed:
			if err != nil {
				t.Errorf("Close = %v, want nil", err)
			}
		case <-time.After(time.Hour):
			t.Errorf("Close had not returned an hour later: it waited for a send only the sink's close could end")
			_ = sink.Close()
			t.Logf("Close returned %v once the sink closed", <-closed)
		}
		select {
		case err := <-ran:
			if !errors.Is(err, mq.ErrClosed) {
				t.Errorf("the publication reported %v, want the driver's ErrClosed", err)
			}
		default:
			t.Errorf("the publication's callback had not run when Close returned")
		}
	})
}

// bufferingSink is a driver that takes a publication into a buffer of its
// own and sends the buffer only when it is flushed, as Kafka and NATS core do
// under ConfirmNone. Its Publish waits for gate first, which holds a batch on
// its way to the driver.
type bufferingSink struct {
	entered chan struct{}
	gate    chan struct{}

	mu       sync.Mutex
	buffered []string
	flushed  []string
}

func openBuffering(*advBroker, mq.SinkConfig) mq.Sink {
	return &bufferingSink{entered: make(chan struct{}, 64), gate: make(chan struct{})}
}

func (s *bufferingSink) Publish(ctx context.Context, m *mq.Message, _ *mq.PublishOptions) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	s.buffered = append(s.buffered, string(m.Body))
	s.mu.Unlock()
	return nil
}

func (s *bufferingSink) Flush(context.Context) error {
	s.mu.Lock()
	s.flushed = append(s.flushed, s.buffered...)
	s.buffered = nil
	s.mu.Unlock()
	return nil
}

func (s *bufferingSink) Close() error { return nil }

func (s *bufferingSink) state() (buffered, flushed []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.buffered...), append([]string(nil), s.flushed...)
}

// TestBatchingFlushWaitsForTheBatchTheFlusherIsSending is Flush and Shutdown
// while the flusher goroutine is part way through handing a batch to the
// driver. They used to find the batching buffer empty, flush the driver's
// buffer before the batch had reached it, and return nil once the callback
// said the driver had taken it: the message sat in the driver's buffer, and
// the close lost it. They now wait for that send before they flush the
// driver.
func TestBatchingFlushWaitsForTheBatchTheFlusherIsSending(t *testing.T) {
	calls := []struct {
		name string
		call func(p *mq.PublisherHandle) error
	}{
		{"Flush", func(p *mq.PublisherHandle) error { return p.Flush(context.Background()) }},
		{"Shutdown", func(p *mq.PublisherHandle) error { return p.Shutdown(context.Background()) }},
	}
	for _, c := range calls {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newAdvBroker(openBuffering)
				p := advPublisher(t, b, mq.PublisherConfig{Batching: mq.BatchingConfig{MaxMessages: 1}})
				sink := advSinkOf[*bufferingSink](t, b)
				ran := make(chan error, 1)
				if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("a")}, func(r mq.PublishResult) {
					ran <- r.Err
				}); err != nil {
					t.Fatalf("PublishAsync: %v", err)
				}
				<-sink.entered
				ret := make(chan error, 1)
				go func() { ret <- c.call(p) }()
				synctest.Wait()
				close(sink.gate)
				if err := <-ret; err != nil {
					t.Fatalf("%s = %v, want nil", c.name, err)
				}
				if err := <-ran; err != nil {
					t.Fatalf("the callback reported %v, want nil", err)
				}
				if buffered, flushed := sink.state(); len(buffered) != 0 || len(flushed) != 1 {
					t.Fatalf("%s returned nil with %v in the driver's buffer and %v flushed: it flushed the driver before the batch reached it",
						c.name, buffered, flushed)
				}
			})
		})
	}
}
