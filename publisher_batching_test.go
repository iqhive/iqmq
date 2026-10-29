package iqmq_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
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
