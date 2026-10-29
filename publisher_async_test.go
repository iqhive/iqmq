package iqmq_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestPublishAsyncBorrowsTheOptionSetUntilTheCallbackRuns is the option-set
// lifetime of AsyncSink, and it is the test that would catch a
// cross-message option swap.
//
// PublisherHandle.Publish takes its resolved option set out of a pool and
// gives it back the moment the synchronous call returns, which Sink.Publish's
// contract allows: o is borrowed for the call. PublishAsync's driver does not
// return when the publication completes, so an asynchronous publication given
// a pooled set would be handing the driver a set that another message owns by
// the time the driver reads it. That is not a crash — it is one message
// published under another's dedup key — so the driver here reads o from its
// own goroutine after the call has returned, which is the only place the
// swap is visible.
//
// Two publications on one goroutine are enough: a sync.Pool hands the object
// just returned straight back to the same P.
func TestPublishAsyncBorrowsTheOptionSetUntilTheCallbackRuns(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openAsync, mq.PublisherConfig{})
	sink := advSinkOf[*advAsyncSink](t, b)
	ctx := context.Background()

	const n = 8
	msgs := make([]*mq.Message, n)
	for i := range msgs {
		msgs[i] = &mq.Message{Body: []byte{byte('a' + i)}}
		key := "dedup-" + string(rune('a'+i))
		if err := p.PublishAsync(ctx, msgs[i], nil, mq.DedupKey(key), mq.Delay(time.Duration(i+1)*time.Minute)); err != nil {
			t.Fatalf("PublishAsync(%d): %v", i, err)
		}
	}
	if got := sink.heldCount(); got != n {
		t.Fatalf("the driver holds %d publications, want %d", got, n)
	}
	calls := sink.complete(n, nil)
	for i, c := range calls {
		wantKey := "dedup-" + string(rune('a'+i))
		wantDelay := time.Duration(i+1) * time.Minute
		if c.seenDedup != wantKey || c.seenDelay != wantDelay {
			t.Fatalf("the driver read (%q, %v) for message %d, want (%q, %v): the option set was reused while the publication was still outstanding",
				c.seenDedup, c.seenDelay, i, wantKey, wantDelay)
		}
	}
}

// TestPublishAsyncCallbackRunsExactlyOnce is the central promise: a nil
// return means the callback runs exactly once.
func TestPublishAsyncCallbackRunsExactlyOnce(t *testing.T) {
	t.Parallel()
	for _, mode := range []mq.CallbackMode{mq.CallbackInline, mq.CallbackDeferred} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			b, p := advClient(t, openAsync, mq.PublisherConfig{Callbacks: mode})
			sink := advSinkOf[*advAsyncSink](t, b)
			ctx := context.Background()

			const n = 16
			var mu sync.Mutex
			seen := map[string]int{}
			var errs []error
			for i := range n {
				m := &mq.Message{Body: []byte{byte('a' + i)}}
				body := string(m.Body)
				if err := p.PublishAsync(ctx, m, func(r mq.PublishResult) {
					mu.Lock()
					seen[body]++
					errs = append(errs, r.Err)
					mu.Unlock()
				}); err != nil {
					t.Fatalf("PublishAsync(%d): %v", i, err)
				}
			}
			sink.complete(n, nil)
			// A deferred callback runs on the completer's goroutine, so the
			// outcome is waited for rather than assumed.
			if err := p.Flush(ctx); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(seen) != n {
				t.Fatalf("%d distinct callbacks ran, want %d", len(seen), n)
			}
			for body, count := range seen {
				if count != 1 {
					t.Fatalf("the callback for %q ran %d times, want exactly 1", body, count)
				}
			}
			for i, err := range errs {
				if err != nil {
					t.Fatalf("callback %d reported %v, want nil", i, err)
				}
			}
			if got := p.Stats().Published; got != n {
				t.Fatalf("Stats().Published = %d, want %d", got, n)
			}
			if got := p.Stats().Pending; got != 0 {
				t.Fatalf("Stats().Pending = %d after every callback ran, want 0", got)
			}
		})
	}
}

// TestPublishAsyncImmediateErrorSuppressesTheCallback is the other half: a
// non-nil immediate error means the callback will not run, on every path that
// can produce one.
func TestPublishAsyncImmediateErrorSuppressesTheCallback(t *testing.T) {
	t.Parallel()
	driverErr := errors.New("the broker refused it")
	cases := []struct {
		name  string
		want  error
		setup func(t *testing.T) (*mq.PublisherHandle, *mq.Message, []mq.PublishOption)
	}{
		{"driver", driverErr, func(t *testing.T) (*mq.PublisherHandle, *mq.Message, []mq.PublishOption) {
			b, p := advClient(t, openAsync, mq.PublisherConfig{})
			advSinkOf[*advAsyncSink](t, b).setRefuse(driverErr)
			return p, &mq.Message{Body: []byte("x")}, nil
		}},
		{"unsupported-option", mq.ErrUnsupported, func(t *testing.T) (*mq.PublisherHandle, *mq.Message, []mq.PublishOption) {
			b := newAdvBroker(openAsync, mq.FeatureScheduledPublish)
			p := advPublisher(t, b, mq.PublisherConfig{})
			return p, &mq.Message{Body: []byte("x")}, []mq.PublishOption{mq.Delay(time.Minute)}
		}},
		{"too-large", mq.ErrMessageTooLarge, func(t *testing.T) (*mq.PublisherHandle, *mq.Message, []mq.PublishOption) {
			_, p := advClient(t, openAsync, mq.PublisherConfig{})
			return p, &mq.Message{Body: make([]byte, (1<<20)+1)}, nil
		}},
		{"closed", mq.ErrClosed, func(t *testing.T) (*mq.PublisherHandle, *mq.Message, []mq.PublishOption) {
			_, p := advClient(t, openAsync, mq.PublisherConfig{})
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			return p, &mq.Message{Body: []byte("x")}, nil
		}},
		{"rejected", mq.ErrBackpressure, func(t *testing.T) (*mq.PublisherHandle, *mq.Message, []mq.PublishOption) {
			b, p := advClient(t, openAsync, mq.PublisherConfig{
				Backpressure:       mq.BackpressureReject,
				MaxPendingMessages: 1,
			})
			sink := advSinkOf[*advAsyncSink](t, b)
			if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("first")}, nil); err != nil {
				t.Fatalf("the first PublishAsync: %v", err)
			}
			if sink.heldCount() != 1 {
				t.Fatalf("the driver holds %d, want 1", sink.heldCount())
			}
			return p, &mq.Message{Body: []byte("x")}, nil
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			p, m, opts := c.setup(t)
			var ran atomic.Int64
			err := p.PublishAsync(context.Background(), m, func(mq.PublishResult) { ran.Add(1) }, opts...)
			if err == nil {
				t.Fatal("PublishAsync returned nil, want an error")
			}
			if !errors.Is(err, c.want) {
				t.Fatalf("PublishAsync = %v, want %v", err, c.want)
			}
			// Give an implementation that handed the callback to another
			// goroutine every chance to run it: the promise is that it never
			// runs at all. Flush cannot be used for this — it waits for the
			// publications this publisher is deliberately still holding.
			for range 100 {
				runtime.Gosched()
			}
			if got := ran.Load(); got != 0 {
				t.Fatalf("the callback ran %d times after an immediate error, want 0 ", got)
			}
		})
	}
}

// TestPublishAsyncCloseRunsEveryCallbackExactlyOnce races Close against
// publications in flight.
//
// The specification gives every admitted publication exactly one callback, and makes
// Close prompt; together they mean a publication outstanding at the driver
// when Close arrives is completed by the close rather than lost, and that the
// completion has happened by the time Close returns. Both halves fail
// silently: a lost callback is a caller waiting for ever, and a callback
// after Close is a use of storage the caller has already reclaimed.
//
// The driver here never completes anything of its own accord, so every
// callback in this test comes from the close.
func TestPublishAsyncCloseRunsEveryCallbackExactlyOnce(t *testing.T) {
	t.Parallel()
	const rounds = 400
	for round := range rounds {
		b := newAdvBroker(openAsync)
		cl := mq.NewClient(b)
		p, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders"})
		if err != nil {
			t.Fatalf("round %d: opening the publisher: %v", round, err)
		}
		sink := advSinkOf[*advAsyncSink](t, b)

		var (
			mu        sync.Mutex
			counts    = map[int]int{}
			afterLate atomic.Int64
			closed    atomic.Bool
			admitted  atomic.Int64
		)
		const writers = 4
		var wg sync.WaitGroup
		start := make(chan struct{})
		for w := range writers {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-start
				for i := range 8 {
					id := w*8 + i
					m := &mq.Message{Body: []byte("m")}
					err := p.PublishAsync(context.Background(), m, func(mq.PublishResult) {
						if closed.Load() {
							afterLate.Add(1)
						}
						mu.Lock()
						counts[id]++
						mu.Unlock()
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
			}(w)
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

		// The driver still holds whatever it was given. Completing it now
		// must not produce a second callback: the close already delivered
		// each one.
		sink.complete(sink.heldCount(), nil)

		mu.Lock()
		ran := 0
		for id, c := range counts {
			if c != 1 {
				mu.Unlock()
				t.Fatalf("round %d: publication %d got %d callbacks, want exactly 1", round, id, c)
			}
			ran++
		}
		mu.Unlock()
		if int64(ran) != admitted.Load() {
			t.Fatalf("round %d: %d publications were admitted and %d callbacks ran", round, admitted.Load(), ran)
		}
		if got := afterLate.Load(); got != 0 {
			t.Fatalf("round %d: %d callbacks arrived after Close returned", round, got)
		}
		_ = cl.Close()
	}
}

// TestPublishAsyncAbandonedOutcomeIsUnknown pins what a publication
// outstanding at the driver when the publisher closes is told: not success and
// not a definite failure, because nobody knows whether the broker took it.
func TestPublishAsyncAbandonedOutcomeIsUnknown(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openAsync, mq.PublisherConfig{})
	sink := advSinkOf[*advAsyncSink](t, b)
	got := make(chan mq.PublishResult, 1)
	if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, func(r mq.PublishResult) { got <- r }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case r := <-got:
		if mq.CodeOf(r.Err) != mq.CodeSettlementUnknown {
			t.Fatalf("the abandoned publication reported %v (code %v), want an unknown outcome", r.Err, mq.CodeOf(r.Err))
		}
		if !mq.IsAmbiguous(r.Err) {
			t.Fatalf("IsAmbiguous(%v) = false, want true: the publication may or may not have reached the broker", r.Err)
		}
	default:
		t.Fatal("the callback did not run before Close returned")
	}
	sink.complete(sink.heldCount(), nil)
	select {
	case r := <-got:
		t.Fatalf("a second callback ran, with %v", r.Err)
	default:
	}
}

// closeAnsweringSink is an asynchronous sink that deals with what it still
// holds from inside its own Close. With answer set it completes each held
// publication with that answer, as a driver does that knows its closing
// connection never sent them. Without one it writes each out and calls no
// done, as a connection does whose last write leaves as it closes and whose
// confirmation never comes back: the message is the driver's to read until
// its Close returns.
type closeAnsweringSink struct {
	advAsyncSink
	answer error
}

func openCloseAnswering(answer error) func(*advBroker, mq.SinkConfig) mq.Sink {
	return func(b *advBroker, cfg mq.SinkConfig) mq.Sink {
		return &closeAnsweringSink{advAsyncSink: advAsyncSink{advSink: advSink{b: b, receipts: cfg.Receipts}}, answer: answer}
	}
}

func (s *closeAnsweringSink) Close() error {
	for _, c := range s.takeHeld(s.heldCount()) {
		s.b.leave()
		if s.answer != nil {
			c.done(s.answer)
			continue
		}
		_ = s.b.accept(c.m, c.o, s.receipts)
	}
	return s.advSink.Close()
}

// TestPublishAsyncCloseLetsTheDriverFinishWithAMessageBeforeHandingItBack is
// the borrow a close must not cut short. Close used to abandon a publication
// outstanding at the driver, and run its callback, before it closed the
// sink, so a caller that reused the message from the callback rewrote it
// while the driver was still writing it out, and the broker got the next
// message's body. The sink now closes first.
func TestPublishAsyncCloseLetsTheDriverFinishWithAMessageBeforeHandingItBack(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openCloseAnswering(nil), mq.PublisherConfig{})
	got := make(chan error, 1)
	if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("order-1")}, func(r mq.PublishResult) {
		copy(r.Message.Body, "order-2")
		got <- r.Err
	}); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-got:
		if mq.CodeOf(err) != mq.CodeSettlementUnknown {
			t.Fatalf("the abandoned publication reported %v, want an unknown outcome", err)
		}
	default:
		t.Fatal("the callback did not run before Close returned")
	}
	if bodies := b.bodies(); len(bodies) != 1 || bodies[0] != "order-1" {
		t.Fatalf("the broker got %v, want [order-1]: the callback handed the message back while the driver was still writing it", bodies)
	}
}

// TestPublishAsyncCloseKeepsTheAnswerTheDriverGivesFromItsClose is the other
// half of the same order. A driver that completes what it holds from inside
// its own Close knows more than the close does, and its definite answer used
// to arrive after the close had abandoned the publication: it was lost, and
// the callback reported an unknown outcome instead.
func TestPublishAsyncCloseKeepsTheAnswerTheDriverGivesFromItsClose(t *testing.T) {
	t.Parallel()
	for _, mode := range []mq.CallbackMode{mq.CallbackInline, mq.CallbackDeferred} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			_, p := advClient(t, openCloseAnswering(mq.ErrClosed), mq.PublisherConfig{Callbacks: mode})
			got := make(chan error, 1)
			if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, func(r mq.PublishResult) {
				got <- r.Err
			}); err != nil {
				t.Fatalf("PublishAsync: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			select {
			case err := <-got:
				if !errors.Is(err, mq.ErrClosed) || mq.IsAmbiguous(err) {
					t.Fatalf("the publication reported %v, want the driver's definite ErrClosed", err)
				}
			default:
				t.Fatal("the callback did not run before Close returned")
			}
		})
	}
}

// TestPublishAsyncAdaptsADriverWithoutAnAsyncSink pins the promise that a
// driver implementing only the mandatory surface works: the core publishes
// synchronously and still runs the callback exactly once.
func TestPublishAsyncAdaptsADriverWithoutAnAsyncSink(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openPlain, mq.PublisherConfig{})
	var ran atomic.Int64
	var gotErr error
	err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, func(r mq.PublishResult) {
		ran.Add(1)
		gotErr = r.Err
	})
	if err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	if got := ran.Load(); got != 1 {
		t.Fatalf("the callback ran %d times, want exactly 1", got)
	}
	if gotErr != nil {
		t.Fatalf("the callback reported %v, want nil", gotErr)
	}
	if got := b.bodies(); len(got) != 1 || got[0] != "x" {
		t.Fatalf("the driver saw %v, want [x]", got)
	}
}

// TestPublishAsyncReportsADriverFailureThroughTheCallback pins that a failure
// the driver reports late is the callback's business and not the call's.
func TestPublishAsyncReportsADriverFailureThroughTheCallback(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openAsync, mq.PublisherConfig{})
	sink := advSinkOf[*advAsyncSink](t, b)
	want := errors.New("late failure")
	got := make(chan error, 2)
	if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, func(r mq.PublishResult) { got <- r.Err }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	sink.complete(1, want)
	select {
	case err := <-got:
		if !errors.Is(err, want) {
			t.Fatalf("the callback reported %v, want %v", err, want)
		}
	default:
		t.Fatal("the callback did not run")
	}
	if got := p.Stats().Failed; got != 1 {
		t.Fatalf("Stats().Failed = %d, want 1", got)
	}
}

// TestPublishAsyncKeepsADriversUnknownOutcome pins that an unknown outcome
// the driver reported itself reaches the callback as the driver's error. Only
// a publication the close abandoned is given the core's own answer; matching
// that by code replaced every driver's SettlementUnknown, and its native
// cause, with "abandoned by close".
func TestPublishAsyncKeepsADriversUnknownOutcome(t *testing.T) {
	t.Parallel()
	b, p := advClient(t, openAsync, mq.PublisherConfig{})
	sink := advSinkOf[*advAsyncSink](t, b)
	native := errors.New("confirm timed out")
	want := &mq.OpError{Operation: mq.OpPublish, Code: mq.CodeSettlementUnknown, Ambiguous: true, Err: native}
	got := make(chan error, 2)
	if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, func(r mq.PublishResult) { got <- r.Err }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	sink.complete(1, want)
	select {
	case err := <-got:
		if !errors.Is(err, native) {
			t.Fatalf("the callback reported %v, want the driver's %v", err, want)
		}
		if !mq.IsAmbiguous(err) {
			t.Fatalf("IsAmbiguous(%v) = false, want true", err)
		}
	default:
		t.Fatal("the callback did not run")
	}
}

// TestPublishAsyncCloseRacesTheDriversCompletion is the two-goroutine end of the exactly-once promise, and the deterministic reproducer for the race
// The specification fixes.
//
// The publication's state is pooled and the driver's done identifies it BY
// THE OBJECT , so the object may not go back to the pool
// while the driver is still reading the completion table out of it. When a
// close ABANDONS an outstanding token those two run on different goroutines:
// the driver's done on the driver's I/O goroutine, and the table's callback —
// which is where the state is recycled — on the publisher's closing
// goroutine. Recycling from a bare "has the driver settled?" therefore
// recycled on a flag the driver had only just written and was not yet
// finished acting on, which is a data race on the state and, once the state
// has been handed to the next publication, a completion posted against
// another message's token. `stageSettling` is what closes it.
//
// This test exists because the reproducer both increments found the race with
// — `drivers/kafka/kafkago`'s TestCloseRacesPublication — fails once in seven
// to twenty runs of -count=20, and a stage machine this delicate needs a test
// that answers at once. This one is in the root module, needs no broker, and
// fails on its FIRST run under -race in both callback modes.
//
// [advAsyncSink.takeHeld] is what makes the window reachable: the outcomes are
// handed over from goroutines of their own, concurrently with the Close, so
// neither side is ordered before the other.
func TestPublishAsyncCloseRacesTheDriversCompletion(t *testing.T) {
	t.Parallel()
	for _, mode := range []mq.CallbackMode{mq.CallbackInline, mq.CallbackDeferred} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			const (
				rounds  = 60
				pending = 32
			)
			ctx := context.Background()
			for round := range rounds {
				b, p := advClient(t, openAsync, mq.PublisherConfig{Callbacks: mode})
				sink := advSinkOf[*advAsyncSink](t, b)
				var reported atomic.Int64
				for i := range pending {
					m := &mq.Message{Body: []byte{byte('a' + i%26)}}
					if err := p.PublishAsync(ctx, m, func(mq.PublishResult) {
						reported.Add(1)
					}); err != nil {
						t.Fatalf("round %d: PublishAsync(%d): %v", round, i, err)
					}
				}
				held := sink.takeHeld(pending)
				if len(held) != pending {
					t.Fatalf("round %d: the driver holds %d publications, want %d", round, len(held), pending)
				}
				start := make(chan struct{})
				var wg sync.WaitGroup
				for _, c := range held {
					wg.Add(1)
					go func(c *advAsyncCall) {
						defer wg.Done()
						<-start
						b.leave()
						c.done(nil)
					}(c)
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					_ = p.Close()
				}()
				close(start)
				wg.Wait()
				// The specification: a nil return means the callback runs exactly once,
				// close or no close.
				if got := reported.Load(); got != pending {
					t.Fatalf("round %d: %d of %d publications reported an outcome", round, got, pending)
				}
			}
		})
	}
}
