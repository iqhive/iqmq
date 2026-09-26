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
