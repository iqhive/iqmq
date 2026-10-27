package iqmq_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// These tests are the PublishAsync callback that closes, shuts down or flushes
// the publisher it belongs to. Each of those calls waits for callbacks, and
// the one it is called from cannot finish until the call returns, so every
// delivery route a callback can take is a route to a call that waits for
// itself: the close's own goroutine, which runs the callbacks of the
// publications it abandons; the completion goroutine of CallbackDeferred and
// the batching flusher, which a close waits for; PublishAsync itself, which
// holds its admission while a driver without an AsyncSink runs the callback;
// and the driver's goroutine, on which a graceful shutdown waits for the
// callback's own publication.
//
// Before the fix each of them waited for ever, or for the 30-second drain
// deadline, or for the minute-long context the Flush tests pass. reentrantWait
// is well short of all three, so a regression fails instead of hanging the
// package.
const reentrantWait = 10 * time.Second

// within returns what ch delivers, or fails the test once reentrantWait has
// passed.
func within[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(reentrantWait):
		t.Fatalf("%s did not finish within %v: it is waiting for the callback it was called from", what, reentrantWait)
	}
	var zero T
	return zero
}

// reentrantPublisher opens a publisher whose client the test's cleanup closes
// without waiting for ever: under a regression the client's close waits for
// the same deadlocked run the test just failed on.
func reentrantPublisher(t *testing.T, open func(*advBroker, mq.SinkConfig) mq.Sink, cfg mq.PublisherConfig) (*advBroker, *mq.PublisherHandle) {
	t.Helper()
	b := newAdvBroker(open)
	cl := reentrantClient(t, b)
	if cfg.Topic == "" {
		cfg.Topic = "orders"
	}
	p, err := cl.Publisher(context.Background(), cfg)
	if err != nil {
		t.Fatalf("opening the publisher: %v", err)
	}
	return b, p
}

// reentrantClient opens a client whose cleanup gives up on its close after
// reentrantWait rather than waiting for ever.
func reentrantClient(t *testing.T, b *advBroker) *mq.Client {
	t.Helper()
	cl := mq.NewClient(b)
	t.Cleanup(func() {
		closed := make(chan struct{})
		go func() {
			_ = cl.Close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(reentrantWait):
			t.Errorf("the client did not close within %v", reentrantWait)
		}
	})
	return cl
}

// closer is Close or Shutdown, which a callback calls on its own publisher.
type closer struct {
	name string
	call func(*mq.PublisherHandle) error
}

func closers() []closer {
	return []closer{
		{"Close", (*mq.PublisherHandle).Close},
		{"Shutdown", func(p *mq.PublisherHandle) error { return p.Shutdown(context.Background()) }},
	}
}

// callbackLog counts each publication's callbacks and keeps what each nested
// call returned.
type callbackLog struct {
	mu     sync.Mutex
	counts map[int]int
	errs   map[int]error
	nested chan error
	ran    chan int
}

func newCallbackLog(n int) *callbackLog {
	return &callbackLog{
		counts: make(map[int]int, n),
		errs:   make(map[int]error, n),
		nested: make(chan error, n),
		ran:    make(chan int, n),
	}
}

// done is publication i's callback: it records the outcome, calls call on
// its own publisher, and records what that returned.
func (l *callbackLog) done(i int, p *mq.PublisherHandle, call func(*mq.PublisherHandle) error) func(mq.PublishResult) {
	return func(r mq.PublishResult) {
		l.mu.Lock()
		l.counts[i]++
		l.errs[i] = r.Err
		l.mu.Unlock()
		l.nested <- call(p)
		l.ran <- i
	}
}

// waitAll waits for n callbacks and checks that each nested call returned nil
// and that every publication's callback ran exactly once.
func (l *callbackLog) waitAll(t *testing.T, n int, what string) {
	t.Helper()
	for range n {
		if err := within(t, l.nested, what+" from inside a callback"); err != nil {
			t.Fatalf("%s from inside a callback returned %v, want nil while the close is still going", what, err)
		}
		within(t, l.ran, "the callback that called "+what)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.counts) != n {
		t.Fatalf("%d publications reported, want %d", len(l.counts), n)
	}
	for i, c := range l.counts {
		if c != 1 {
			t.Fatalf("publication %d's callback ran %d times, want exactly once", i, c)
		}
	}
}

// outcome is what publication i's callback was told.
func (l *callbackLog) outcome(i int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.errs[i]
}

// closeFromOutside closes p from the test's goroutine, bounded, and checks
// the close's own outcome.
func closeFromOutside(t *testing.T, p *mq.PublisherHandle) {
	t.Helper()
	out := make(chan error, 1)
	go func() { out <- p.Close() }()
	if err := within(t, out, "Close from outside the callbacks"); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestCloseFromInsideACallbackDoesNotWaitForIt covers Close and Shutdown from
// a callback, on every goroutine a callback is delivered on.
func TestCloseFromInsideACallbackDoesNotWaitForIt(t *testing.T) {
	t.Parallel()
	const n = 4
	for _, op := range closers() {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()

			// The close completes the publications the driver still holds
			// and runs their callbacks on the close's own goroutine, which a
			// prompt Close waits for (U2, inline).
			t.Run("inline/abandoned-by-close", func(t *testing.T) {
				t.Parallel()
				_, p := reentrantPublisher(t, openAsync, mq.PublisherConfig{})
				log := newCallbackLog(n)
				for i := range n {
					if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, log.done(i, p, op.call)); err != nil {
						t.Fatalf("PublishAsync(%d): %v", i, err)
					}
				}
				out := make(chan error, 1)
				go func() { out <- p.Close() }()
				log.waitAll(t, n, op.name)
				if err := within(t, out, "Close from outside the callbacks"); err != nil {
					t.Fatalf("Close: %v", err)
				}
				if got := log.outcome(0); mq.CodeOf(got) != mq.CodeSettlementUnknown {
					t.Fatalf("an abandoned publication reported %v, want CodeSettlementUnknown", got)
				}
			})

			// The same, delivered on the completion goroutine, which the close
			// waits for (U2, deferred).
			t.Run("deferred/abandoned-by-close", func(t *testing.T) {
				t.Parallel()
				_, p := reentrantPublisher(t, openAsync, mq.PublisherConfig{Callbacks: mq.CallbackDeferred})
				log := newCallbackLog(n)
				for i := range n {
					if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, log.done(i, p, op.call)); err != nil {
						t.Fatalf("PublishAsync(%d): %v", i, err)
					}
				}
				out := make(chan error, 1)
				go func() { out <- p.Close() }()
				log.waitAll(t, n, op.name)
				if err := within(t, out, "Close from outside the callbacks"); err != nil {
					t.Fatalf("Close: %v", err)
				}
			})

			// A callback the driver's outcome reached, on the completion
			// goroutine, starts the close itself (U3). Every publication is
			// completed by the driver, because a graceful shutdown waits for
			// the ones it still holds until the drain deadline.
			t.Run("deferred/driver-completion", func(t *testing.T) {
				t.Parallel()
				b, p := reentrantPublisher(t, openAsync, mq.PublisherConfig{Callbacks: mq.CallbackDeferred})
				sink := advSinkOf[*advAsyncSink](t, b)
				log := newCallbackLog(n)
				for i := range n {
					if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, log.done(i, p, op.call)); err != nil {
						t.Fatalf("PublishAsync(%d): %v", i, err)
					}
				}
				go sink.complete(n, nil)
				log.waitAll(t, n, op.name)
				closeFromOutside(t, p)
				if got := log.outcome(0); got != nil {
					t.Fatalf("the publication the driver completed reported %v, want nil", got)
				}
			})

			// A callback on the driver's own goroutine starts the close. A
			// prompt one abandons the rest on its own goroutine while the
			// driver is still delivering them, and a graceful one waits for
			// them.
			t.Run("inline/driver-completion", func(t *testing.T) {
				t.Parallel()
				b, p := reentrantPublisher(t, openAsync, mq.PublisherConfig{})
				sink := advSinkOf[*advAsyncSink](t, b)
				log := newCallbackLog(n)
				for i := range n {
					if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, log.done(i, p, op.call)); err != nil {
						t.Fatalf("PublishAsync(%d): %v", i, err)
					}
				}
				go sink.complete(n, nil)
				log.waitAll(t, n, op.name)
				closeFromOutside(t, p)
			})

			// The batching flusher runs the callbacks of the batch it sent,
			// and the close stops the flusher and waits for it.
			t.Run("batching/flusher", func(t *testing.T) {
				t.Parallel()
				_, p := reentrantPublisher(t, openBatch, mq.PublisherConfig{Batching: mq.BatchingConfig{MaxMessages: n}})
				log := newCallbackLog(n)
				for i := range n {
					if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, log.done(i, p, op.call)); err != nil {
						t.Fatalf("PublishAsync(%d): %v", i, err)
					}
				}
				log.waitAll(t, n, op.name)
				closeFromOutside(t, p)
				if got := log.outcome(0); got != nil {
					t.Fatalf("a batched publication reported %v, want nil", got)
				}
			})

			// What never left the batching buffer is settled with ErrClosed
			// on the close's own goroutine.
			t.Run("batching/left-in-the-buffer", func(t *testing.T) {
				t.Parallel()
				_, p := reentrantPublisher(t, openBatch, mq.PublisherConfig{Batching: mq.BatchingConfig{MaxMessages: 1000}})
				log := newCallbackLog(n)
				for i := range n {
					if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, log.done(i, p, op.call)); err != nil {
						t.Fatalf("PublishAsync(%d): %v", i, err)
					}
				}
				out := make(chan error, 1)
				go func() { out <- p.Close() }()
				log.waitAll(t, n, op.name)
				if err := within(t, out, "Close from outside the callbacks"); err != nil {
					t.Fatalf("Close: %v", err)
				}
			})

			// A driver without an AsyncSink: the core publishes synchronously
			// and runs the callback inside PublishAsync, which still holds the
			// admission the close's first barrier waits for.
			t.Run("adapter/inside-PublishAsync", func(t *testing.T) {
				t.Parallel()
				_, p := reentrantPublisher(t, openPlain, mq.PublisherConfig{})
				log := newCallbackLog(1)
				published := make(chan error, 1)
				go func() {
					published <- p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, log.done(0, p, op.call))
				}()
				if err := within(t, published, "PublishAsync whose callback called "+op.name); err != nil {
					t.Fatalf("PublishAsync: %v", err)
				}
				log.waitAll(t, 1, op.name)
				closeFromOutside(t, p)
			})

			// A driver that completes the publication before its own
			// PublishAsync returns, as the in-memory driver does for a topic
			// nobody subscribes to: the same held admission, reached through
			// the AsyncSink.
			t.Run("async/completed-inside-PublishAsync", func(t *testing.T) {
				t.Parallel()
				_, p := reentrantPublisher(t, openInlineAsync, mq.PublisherConfig{})
				log := newCallbackLog(1)
				published := make(chan error, 1)
				go func() {
					published <- p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, log.done(0, p, op.call))
				}()
				if err := within(t, published, "PublishAsync whose callback called "+op.name); err != nil {
					t.Fatalf("PublishAsync: %v", err)
				}
				log.waitAll(t, 1, op.name)
				closeFromOutside(t, p)
				if got := log.outcome(0); got != nil {
					t.Fatalf("the publication reported %v, want the driver's success", got)
				}
			})
		})
	}
}

// inlineAsyncSink is an AsyncSink that completes every publication before its
// PublishAsync returns.
type inlineAsyncSink struct{ advSink }

func openInlineAsync(b *advBroker, cfg mq.SinkConfig) mq.Sink {
	return &inlineAsyncSink{advSink{b: b, receipts: cfg.Receipts}}
}

func (s *inlineAsyncSink) PublishAsync(ctx context.Context, m *mq.Message, o *mq.PublishOptions, done func(error)) error {
	if err := s.Publish(ctx, m, o); err != nil {
		return err
	}
	done(nil)
	return nil
}

// TestFlushFromInsideACallbackDoesNotWaitForIt is Flush from a callback. Every
// wait Flush makes elsewhere includes the callback's own publication, which is
// not finished until the callback returns, so before the fix each of these
// waited out the minute-long context.
func TestFlushFromInsideACallbackDoesNotWaitForIt(t *testing.T) {
	t.Parallel()
	flushFrom := func() (func(*mq.PublisherHandle) error, func()) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		return func(p *mq.PublisherHandle) error { return p.Flush(ctx) }, cancel
	}
	for _, mode := range []mq.CallbackMode{mq.CallbackInline, mq.CallbackDeferred} {
		t.Run(mode.String()+"/driver-completion", func(t *testing.T) {
			t.Parallel()
			flush, cancel := flushFrom()
			defer cancel()
			b, p := reentrantPublisher(t, openAsync, mq.PublisherConfig{Callbacks: mode})
			sink := advSinkOf[*advAsyncSink](t, b)
			log := newCallbackLog(2)
			for i := range 2 {
				if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, log.done(i, p, flush)); err != nil {
					t.Fatalf("PublishAsync(%d): %v", i, err)
				}
			}
			go sink.complete(2, nil)
			log.waitAll(t, 2, "Flush")
			closeFromOutside(t, p)
		})
	}

	// From the batching flusher, Flush still hands the buffer to the driver:
	// a publication the callback made is at the driver when Flush returns.
	t.Run("batching/flusher", func(t *testing.T) {
		t.Parallel()
		flush, cancel := flushFrom()
		defer cancel()
		b, p := reentrantPublisher(t, openBatch, mq.PublisherConfig{Batching: mq.BatchingConfig{MaxMessages: 2}})
		log := newCallbackLog(3)
		again := func(p *mq.PublisherHandle) error {
			if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("again")}, log.done(2, p, func(*mq.PublisherHandle) error { return nil })); err != nil {
				return err
			}
			return flush(p)
		}
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("first")}, log.done(0, p, again)); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("second")}, log.done(1, p, flush)); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
		log.waitAll(t, 3, "Flush")
		if got := b.bodies(); len(got) != 3 || got[2] != "again" {
			t.Fatalf("the driver saw %v, want the publication made from the callback last", got)
		}
		closeFromOutside(t, p)
	})

	t.Run("adapter/inside-PublishAsync", func(t *testing.T) {
		t.Parallel()
		flush, cancel := flushFrom()
		defer cancel()
		_, p := reentrantPublisher(t, openPlain, mq.PublisherConfig{})
		log := newCallbackLog(1)
		published := make(chan error, 1)
		go func() {
			published <- p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, log.done(0, p, flush))
		}()
		if err := within(t, published, "PublishAsync whose callback called Flush"); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
		log.waitAll(t, 1, "Flush")
		closeFromOutside(t, p)
	})
}

// TestACloseFromOutsideStillWaitsForTheCallbackThatClosed is the guarantee
// the fix must not cost anybody else: a Close from outside the callbacks
// returns only once every callback has run — the one that closed the
// publisher from inside, which goes on after its own Close returned,
// included.
func TestACloseFromOutsideStillWaitsForTheCallbackThatClosed(t *testing.T) {
	t.Parallel()
	for _, mode := range []mq.CallbackMode{mq.CallbackInline, mq.CallbackDeferred} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			b, p := reentrantPublisher(t, openAsync, mq.PublisherConfig{Callbacks: mode})
			sink := advSinkOf[*advAsyncSink](t, b)
			var (
				mu       sync.Mutex
				finished bool
				counts   = map[int]int{}
			)
			nested := make(chan error, 1)
			release := make(chan struct{})
			closer := func(r mq.PublishResult) {
				nested <- p.Close()
				// The callback goes on after its own Close has returned. A
				// timer and not a bare receive, so that a regression that
				// never releases it cannot hang the package.
				select {
				case <-release:
				case <-time.After(reentrantWait):
				}
				mu.Lock()
				finished = true
				counts[0]++
				mu.Unlock()
			}
			if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("closer")}, closer); err != nil {
				t.Fatalf("PublishAsync: %v", err)
			}
			if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("other")}, func(mq.PublishResult) {
				mu.Lock()
				counts[1]++
				mu.Unlock()
			}); err != nil {
				t.Fatalf("PublishAsync: %v", err)
			}
			// Inline, the closing callback runs on the close's own goroutine,
			// abandoned; deferred, on the completion goroutine, completed.
			if mode == mq.CallbackDeferred {
				go sink.complete(1, nil)
			}
			type closed struct {
				err      error
				finished bool
			}
			outer := make(chan closed, 1)
			go func() {
				err := p.Close()
				mu.Lock()
				defer mu.Unlock()
				outer <- closed{err: err, finished: finished}
			}()
			if err := within(t, nested, "Close from inside the callback"); err != nil {
				t.Fatalf("Close from inside the callback returned %v, want nil", err)
			}
			select {
			case got := <-outer:
				t.Fatalf("Close from outside returned (%v) while a callback was still running", got.err)
			case <-time.After(50 * time.Millisecond):
			}
			close(release)
			got := within(t, outer, "Close from outside")
			if got.err != nil {
				t.Fatalf("Close from outside: %v", got.err)
			}
			if !got.finished {
				t.Fatal("Close from outside returned before the callback that closed the publisher had finished")
			}
			mu.Lock()
			defer mu.Unlock()
			if counts[0] != 1 || counts[1] != 1 {
				t.Fatalf("the callbacks ran %v times, want exactly once each", counts)
			}
		})
	}
}

// closeFailSink is an asynchronous sink whose Close fails, which is how a
// close comes to have an outcome worth reporting.
type closeFailSink struct {
	*advAsyncSink
	err error
}

func (s *closeFailSink) Close() error {
	_ = s.advAsyncSink.Close()
	return s.err
}

// TestCloseFromACallbackLeavesTheOutcomeToEveryoneElse pins what a Close from
// inside a callback returns and what it promises: nil, because the close it
// began cannot have finished while the callback runs; publications refused
// from the moment it returns, even before the close's goroutine has run;
// and the close's own error for every Close after it, as an idempotent Close
// always reports.
func TestCloseFromACallbackLeavesTheOutcomeToEveryoneElse(t *testing.T) {
	t.Parallel()
	want := errors.New("the native client would not close")
	b := newAdvBroker(func(b *advBroker, cfg mq.SinkConfig) mq.Sink {
		async, _ := openAsync(b, cfg).(*advAsyncSink)
		return &closeFailSink{advAsyncSink: async, err: want}
	})
	cl := reentrantClient(t, b)
	p, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders", Callbacks: mq.CallbackDeferred})
	if err != nil {
		t.Fatalf("opening the publisher: %v", err)
	}
	sink := advSinkOf[*closeFailSink](t, b)
	type afterward struct {
		closeErr, asyncErr, syncErr error
	}
	got := make(chan afterward, 1)
	if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, func(mq.PublishResult) {
		var a afterward
		a.closeErr = p.Close()
		a.asyncErr = p.PublishAsync(context.Background(), &mq.Message{Body: []byte("late")}, func(mq.PublishResult) {})
		a.syncErr = p.Publish(context.Background(), &mq.Message{Body: []byte("late")})
		got <- a
	}); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	go sink.complete(1, nil)
	a := within(t, got, "the callback that closed its publisher")
	if a.closeErr != nil {
		t.Fatalf("Close from inside the callback returned %v, want nil: the close cannot have finished", a.closeErr)
	}
	if !errors.Is(a.asyncErr, mq.ErrClosed) {
		t.Fatalf("PublishAsync after the callback's Close returned %v, want ErrClosed", a.asyncErr)
	}
	if !errors.Is(a.syncErr, mq.ErrClosed) {
		t.Fatalf("Publish after the callback's Close returned %v, want ErrClosed", a.syncErr)
	}
	for i := range 2 {
		out := make(chan error, 1)
		go func() { out <- p.Close() }()
		if err := within(t, out, "Close from outside"); !errors.Is(err, want) {
			t.Fatalf("Close %d from outside returned %v, want the close's own %v", i+1, err, want)
		}
	}
}

// TestShutdownFromAnotherPublishersCallbackOnTheSameGoroutine is why a call
// from inside ANY publisher's callback does not wait, and not only one from
// inside its own publisher's: one connection's driver delivers the outcomes of
// every publisher on it from one goroutine, so a graceful shutdown waiting
// for its own publisher's outcome can be waiting for the goroutine the
// calling callback holds. Before the fix the shutdown below waited out the
// 30-second drain deadline. After it, the shutdown is still graceful: the
// publication it was waiting for is reported as the driver's success, not
// abandoned.
func TestShutdownFromAnotherPublishersCallbackOnTheSameGoroutine(t *testing.T) {
	t.Parallel()
	bp, p := reentrantPublisher(t, openAsync, mq.PublisherConfig{Topic: "p"})
	bq, q := reentrantPublisher(t, openAsync, mq.PublisherConfig{Topic: "q"})
	sinkP, sinkQ := advSinkOf[*advAsyncSink](t, bp), advSinkOf[*advAsyncSink](t, bq)

	pReported := make(chan error, 2)
	if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("p")}, func(r mq.PublishResult) {
		pReported <- r.Err
	}); err != nil {
		t.Fatalf("PublishAsync on p: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	nested := make(chan error, 1)
	if err := q.PublishAsync(context.Background(), &mq.Message{Body: []byte("q")}, func(mq.PublishResult) {
		nested <- p.Shutdown(ctx)
	}); err != nil {
		t.Fatalf("PublishAsync on q: %v", err)
	}
	qc, pc := sinkQ.takeHeld(1), sinkP.takeHeld(1)
	if len(qc) != 1 || len(pc) != 1 {
		t.Fatalf("the drivers hold %d and %d publications, want one each", len(qc), len(pc))
	}
	// One goroutine delivers both outcomes, q's first, as a shared
	// connection's reply goroutine would.
	go func() {
		bq.leave()
		qc[0].done(nil)
		bp.leave()
		pc[0].done(nil)
	}()
	if err := within(t, nested, "p.Shutdown from inside q's callback"); err != nil {
		t.Fatalf("p.Shutdown from inside q's callback returned %v, want nil", err)
	}
	if err := within(t, pReported, "p's callback"); err != nil {
		t.Fatalf("p's publication reported %v, want the driver's success: the shutdown is graceful", err)
	}
	out := make(chan error, 1)
	go func() { out <- p.Shutdown(context.Background()) }()
	if err := within(t, out, "p.Shutdown from outside"); err != nil {
		t.Fatalf("p.Shutdown from outside: %v", err)
	}
	select {
	case err := <-pReported:
		t.Fatalf("p's callback ran a second time, with %v", err)
	default:
	}
}

// TestAClientCloseWaitsForAPublisherClosedFromItsCallback is the client's half
// of the guarantee a Close from inside a callback must not cost anybody else.
// The callback's Close begins the publisher's close and returns, and the
// close goes on once the callback does; a Client.Close from outside must wait
// for it like any child's, so that it returns only once every callback of
// every child has run and the child's sink closed before the backend.
//
// The publisher used to leave the client's list as soon as the callback's
// Close returned, so the client's close found nothing to wait for: it closed
// the backend and returned with the callback still running, the abandoned
// publication's callback still to come and the sink still open.
func TestAClientCloseWaitsForAPublisherClosedFromItsCallback(t *testing.T) {
	t.Parallel()
	b := newAdvBroker(openAsync)
	cl := reentrantClient(t, b)
	p, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders", Callbacks: mq.CallbackDeferred})
	if err != nil {
		t.Fatalf("opening the publisher: %v", err)
	}
	sink := advSinkOf[*advAsyncSink](t, b)
	var (
		mu     sync.Mutex
		counts = map[int]int{}
	)
	nested := make(chan error, 1)
	release := make(chan struct{})
	if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("closer")}, func(mq.PublishResult) {
		nested <- p.Close()
		// A callback that goes on after its own Close returned, which
		// CallbackDeferred allows. Bounded, so that a regression that never
		// releases it cannot hang the package.
		select {
		case <-release:
		case <-time.After(reentrantWait):
		}
		mu.Lock()
		counts[0]++
		mu.Unlock()
	}); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("abandoned")}, func(mq.PublishResult) {
		mu.Lock()
		counts[1]++
		mu.Unlock()
	}); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	go sink.complete(1, nil)
	if err := within(t, nested, "Close from inside the callback"); err != nil {
		t.Fatalf("Close from inside the callback returned %v, want nil", err)
	}
	type closed struct {
		err        error
		counts     map[int]int
		sinkClosed bool
	}
	outer := make(chan closed, 1)
	go func() {
		err := cl.Close()
		mu.Lock()
		defer mu.Unlock()
		snap := make(map[int]int, len(counts))
		for k, v := range counts {
			snap[k] = v
		}
		outer <- closed{err: err, counts: snap, sinkClosed: sink.isClosed()}
	}()
	select {
	case got := <-outer:
		t.Fatalf("Client.Close returned (%v, callbacks %v, sink closed %v) while its publisher's callback was still running", got.err, got.counts, got.sinkClosed)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	got := within(t, outer, "Client.Close from outside")
	if got.err != nil {
		t.Fatalf("Client.Close: %v", got.err)
	}
	if got.counts[0] != 1 || got.counts[1] != 1 {
		t.Fatalf("Client.Close returned with the callbacks run %v times, want exactly once each", got.counts)
	}
	if !got.sinkClosed {
		t.Fatal("Client.Close returned with its publisher's sink still open")
	}
}

// TestClientCloseFromInsideACallbackDoesNotWaitForIt is Close and Shutdown of
// the client from inside a publisher's callback. Closing a client closes its
// publishers, and a publisher's close waits for the goroutine the callback is
// running on, so the client's close cannot wait either: it begins and
// returns, and a close from outside still waits for all of it.
func TestClientCloseFromInsideACallbackDoesNotWaitForIt(t *testing.T) {
	t.Parallel()
	const n = 2
	for _, op := range []struct {
		name string
		call func(*mq.Client) error
	}{
		{"Close", (*mq.Client).Close},
		{"Shutdown", func(cl *mq.Client) error { return cl.Shutdown(context.Background()) }},
	} {
		for _, mode := range []mq.CallbackMode{mq.CallbackInline, mq.CallbackDeferred} {
			t.Run(op.name+"/"+mode.String(), func(t *testing.T) {
				t.Parallel()
				b := newAdvBroker(openAsync)
				cl := reentrantClient(t, b)
				p, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders", Callbacks: mode})
				if err != nil {
					t.Fatalf("opening the publisher: %v", err)
				}
				sink := advSinkOf[*advAsyncSink](t, b)
				log := newCallbackLog(n)
				closeClient := func(*mq.PublisherHandle) error { return op.call(cl) }
				for i := range n {
					if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("m")}, log.done(i, p, closeClient)); err != nil {
						t.Fatalf("PublishAsync(%d): %v", i, err)
					}
				}
				// Inline, the driver's goroutine runs the first callback and
				// the publisher's close runs the second, abandoned; deferred,
				// both run on the completion goroutine. A graceful shutdown
				// waits for every publication the driver holds, so Shutdown
				// has the driver complete them all.
				complete := 1
				if op.name == "Shutdown" {
					complete = n
				}
				go sink.complete(complete, nil)
				log.waitAll(t, n, "Client."+op.name)
				out := make(chan error, 1)
				go func() { out <- cl.Close() }()
				if err := within(t, out, "Client.Close from outside the callbacks"); err != nil {
					t.Fatalf("Client.Close: %v", err)
				}
				if !sink.isClosed() {
					t.Fatal("Client.Close returned with its publisher's sink still open")
				}
				if err := p.Publish(context.Background(), &mq.Message{Body: []byte("late")}); !errors.Is(err, mq.ErrClosed) {
					t.Fatalf("Publish after the client closed returned %v, want ErrClosed", err)
				}
			})
		}
	}
}
