package middleware_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/middleware"
)

var errBoom = errors.New("boom")

// errUnavailable is a transient dependency failure. The root package exports
// no CodeUnavailable sentinel, so the test builds one with the code the
// breaker's default predicate counts.
var errUnavailable error = errcode.New(errcode.Unavailable, "the dependency is down")

// ---------------------------------------------------------------- Recover

func TestRecoverKeepsThePanicsIdentity(t *testing.T) {
	var got *mq.PanicError
	h := middleware.Recover(func(_ context.Context, _ *mq.Delivery, pe *mq.PanicError) { got = pe })(
		mq.HandlerFunc(func(context.Context, *mq.Delivery) error { panic("the handler exploded") }),
	)
	err := h.Handle(context.Background(), delivery("x"))
	if err == nil {
		t.Fatal("Handle = nil for a handler that panicked; the delivery would be acked")
	}
	var pe *mq.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("Handle = %v, want a *mq.PanicError", err)
	}
	if v, _ := pe.Value.(string); v != "the handler exploded" {
		t.Fatalf("PanicError.Value = %#v, want the value the handler panicked with", pe.Value)
	}
	if len(pe.Stack) == 0 {
		t.Fatal("PanicError.Stack is empty: the stack is the only evidence a panic leaves")
	}
	if !bytes.Contains(pe.Stack, []byte("middleware_test")) {
		t.Fatalf("PanicError.Stack does not name the panicking frames:\n%s", pe.Stack)
	}
	if got != pe {
		t.Fatal("the observer was not called with the error that was returned")
	}
}

func TestRecoverUnwrapsAPanickedError(t *testing.T) {
	h := middleware.Recover(nil)(mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		panic(errBoom)
	}))
	err := h.Handle(context.Background(), delivery("x"))
	if !errors.Is(err, errBoom) {
		t.Fatalf("Handle = %v, want errors.Is to reach the error the handler panicked with", err)
	}
}

func TestRecoverLetsAnOrdinaryResultThrough(t *testing.T) {
	h := middleware.Recover(nil)(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return errBoom }))
	if err := h.Handle(context.Background(), delivery("x")); !errors.Is(err, errBoom) {
		t.Fatalf("Handle = %v, want the handler's own error unchanged", err)
	}
}

// ---------------------------------------------------------------- Timeout

func TestTimeoutBoundsTheHandler(t *testing.T) {
	var deadlineSet bool
	h := mustTimeout(t, 50*time.Millisecond)(
		mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
			_, deadlineSet = ctx.Deadline()
			<-ctx.Done()
			return ctx.Err()
		}),
	)
	err := h.Handle(context.Background(), delivery("x"))
	if !deadlineSet {
		t.Fatal("the handler's context had no deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Handle = %v, want context.DeadlineExceeded", err)
	}
}

func TestTimeoutRefusesANonPositiveDuration(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		if _, err := middleware.Timeout(d); !errors.Is(err, middleware.ErrInvalidConfig) {
			t.Fatalf("Timeout(%v) did not fail; every delivery would be refused before the handler ran", d)
		}
	}
}

// ---------------------------------------------------------------- Retry

func TestRetryRepeatsATransientFailure(t *testing.T) {
	c := &calls{fn: func(n int, _ *mq.Delivery) error {
		if n < 3 {
			return mq.ErrBackpressure
		}
		return nil
	}}
	h := mustRetry(t, middleware.RetryConfig{MaxAttempts: 5, Clock: newFakeClock()})(c)
	if err := h.Handle(context.Background(), delivery("x")); err != nil {
		t.Fatalf("Handle = %v, want nil once the transient failure cleared", err)
	}
	if c.count() != 3 {
		t.Fatalf("the handler was called %d times, want 3", c.count())
	}
}

func TestRetryStopsAtMaxAttempts(t *testing.T) {
	c := &calls{fn: func(int, *mq.Delivery) error { return mq.ErrBackpressure }}
	h := mustRetry(t, middleware.RetryConfig{MaxAttempts: 4, Clock: newFakeClock()})(c)
	if err := h.Handle(context.Background(), delivery("x")); !errors.Is(err, mq.ErrBackpressure) {
		t.Fatalf("Handle = %v, want the last failure", err)
	}
	if c.count() != 4 {
		t.Fatalf("the handler was called %d times, want exactly MaxAttempts (4)", c.count())
	}
}

func TestRetryNeverRepeatsAnAmbiguousOutcome(t *testing.T) {
	// The specification special-cases it and says why: an ambiguous publish may
	// already have reached the broker, so retrying it is how one message
	// becomes two.
	c := &calls{fn: func(int, *mq.Delivery) error { return mq.ErrSettlementUnknown }}
	h := mustRetry(t, middleware.RetryConfig{MaxAttempts: 5, Clock: newFakeClock()})(c)
	if err := h.Handle(context.Background(), delivery("x")); !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("Handle = %v, want the ambiguous outcome unchanged", err)
	}
	if c.count() != 1 {
		t.Fatalf("an ambiguous outcome was retried %d times; it must be retried 0 times", c.count()-1)
	}
}

func TestRetryRefusesTheUnretryable(t *testing.T) {
	for _, err := range []error{
		mq.ErrUnsupported, mq.ErrMessageTooLarge, mq.ErrAlreadySettled,
		mq.ErrLeaseLost, mq.ErrNotSettleable, context.Canceled,
	} {
		t.Run(err.Error(), func(t *testing.T) {
			c := &calls{fn: func(int, *mq.Delivery) error { return err }}
			h := mustRetry(t, middleware.RetryConfig{MaxAttempts: 5, Clock: newFakeClock()})(c)
			_ = h.Handle(context.Background(), delivery("x"))
			if c.count() != 1 {
				t.Fatalf("%v was retried %d times, want 0: nothing about the next attempt is different", err, c.count()-1)
			}
		})
	}
}

func TestRetryNeverRepeatsAControlError(t *testing.T) {
	for _, err := range []error{
		mq.DeadLetter(errBoom), mq.Discard(errBoom), mq.Halt(errBoom),
		mq.RetryAfter(errBoom, time.Second), mq.Postpone(errBoom, time.Second),
	} {
		t.Run(err.Error(), func(t *testing.T) {
			c := &calls{fn: func(int, *mq.Delivery) error { return err }}
			h := mustRetry(t, middleware.RetryConfig{MaxAttempts: 5, Clock: newFakeClock()})(c)
			got := h.Handle(context.Background(), delivery("x"))
			if c.count() != 1 {
				t.Fatalf("a handler that named its own disposition was retried %d times", c.count()-1)
			}
			if d1, _ := mq.DirectiveOf(got); d1 == mq.DirectiveNone {
				t.Fatalf("Handle = %v, want the directive preserved", got)
			}
		})
	}
}

func TestRetryStartsEveryAttemptFromTheMessageItWasGiven(t *testing.T) {
	// Encrypt behind Retry opens the envelope for the handler by replacing
	// the body. The second attempt runs Encrypt again, so it has to be
	// handed the sealed body: handed the plaintext, RequireSealed refused it,
	// and a transient failure the retry was there to absorb became a corrupt
	// message the runner could only dead-letter.
	k := testKey(t)
	rec := &publishRecorder{}
	p := mustWrapPublisher(t, rec, middleware.PublishEncrypt(middleware.EncryptConfig{Keys: k}))
	if err := p.Publish(context.Background(), &mq.Message{Topic: "orders", Body: []byte("card=4111111111111111")}); err != nil {
		t.Fatal(err)
	}
	sealed := rec.sent[0]
	var opened []string
	c := &calls{fn: func(n int, d *mq.Delivery) error {
		opened = append(opened, string(d.Body))
		if n == 1 {
			return mq.ErrBackpressure
		}
		return nil
	}}
	h := mq.Chain(
		mustRetry(t, middleware.RetryConfig{MaxAttempts: 3, Backoff: mq.ConstantBackoff(0), Clock: newFakeClock()}),
		middleware.Encrypt(middleware.EncryptConfig{Keys: k, RequireSealed: true}),
	)(c)
	d := delivery("")
	d.Body, d.Headers = sealed.Body, sealed.Headers
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatalf("Handle = %v, want nil once the transient failure cleared", err)
	}
	if want := []string{"card=4111111111111111", "card=4111111111111111"}; !slices.Equal(opened, want) {
		t.Fatalf("the handler was given %q, want the plaintext on both attempts", opened)
	}
}

func TestRetryIsBoundedByTheLeasesContext(t *testing.T) {
	// The retry holds the delivery, so a wait that outlives the lease means
	// two handlers for one message. The wait is therefore cut short by the
	// handler's context and the last failure is returned.
	ctx, cancel := context.WithCancel(context.Background())
	c := &calls{fn: func(int, *mq.Delivery) error {
		cancel() // the lease is gone as of now
		return mq.ErrBackpressure
	}}
	h := mustRetry(t, middleware.RetryConfig{
		MaxAttempts: 8,
		Backoff:     mq.ConstantBackoff(time.Hour),
		Clock:       newFakeClock(),
	})(c)
	err := h.Handle(ctx, delivery("x"))
	if !errors.Is(err, mq.ErrBackpressure) {
		t.Fatalf("Handle = %v, want the handler's own failure and not the wait's", err)
	}
	if c.count() != 1 {
		t.Fatalf("the handler was called %d times after its context ended, want 1", c.count())
	}
}

func TestRetryKeepsItsWaitsInsideTheDeliverysLease(t *testing.T) {
	// The runner narrows the handler's context to the lease only under
	// WithLeaseDeadline, so with the default run options nothing cut a wait
	// short: a 90ms wait on a 100ms lease ran into it, the broker handed the
	// message to a second handler while the first still held it, and the
	// first one's nack was refused. Under WithLeaseDeadline the second wait
	// ended with the lease, and the nack after it found the lease gone.
	for _, tc := range []struct {
		name string
		opts []mq.RunOption
	}{
		{"with the default run options", nil},
		{"under WithLeaseDeadline", []mq.RunOption{mq.WithLeaseDeadline()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, memory.TestRealClock(), memory.TestTopology(memory.TopologyConfig{AckDeadline: 100 * time.Millisecond}))
				var holding, most atomic.Int64
				held := func(next mq.Handler) mq.Handler {
					return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
						n := holding.Add(1)
						defer holding.Add(-1)
						for m := most.Load(); n > m && !most.CompareAndSwap(m, n); m = most.Load() {
						}
						return next.Handle(ctx, d)
					})
				}
				retry := mustRetry(t, middleware.RetryConfig{MaxAttempts: 5, Backoff: mq.ConstantBackoff(90 * time.Millisecond)})
				c := &calls{fn: func(int, *mq.Delivery) error { return errUnavailable }}
				opts := append([]mq.RunOption{mq.WithConcurrency(4), mq.WithMaxAttempts(100)}, tc.opts...)
				r := h.run(held(retry(c)), opts...)
				h.publish(1, nil)
				time.Sleep(2 * time.Second)
				_ = r.stop()
				if got := most.Load(); got != 1 {
					t.Errorf("%d handlers held the message at once, want 1", got)
				}
				if s := h.snapshot(); s.Expired != 0 {
					t.Errorf("snapshot = %+v, want no lease expired under a handler", s)
				}
				if c.count() < 2 {
					t.Errorf("the handler ran %d times, want the runner's retries to have run it again", c.count())
				}
			})
		})
	}
}

func TestRetryStillSleepsAWaitTheLeaseCanAfford(t *testing.T) {
	// The lease guard cuts only a wait the lease cannot afford. A short one is
	// slept, and so is any wait against a broker whose clock is not the one
	// Retry reads: a memory broker on its deterministic test clock grants
	// leases that ended months before the wall clock's now, and reading them
	// as over would have stopped every in-handler retry against it.
	check := func(t *testing.T, opts ...memory.TestOption) {
		h := newHarness(t, opts...)
		retry := mustRetry(t, middleware.RetryConfig{MaxAttempts: 3, Backoff: mq.ConstantBackoff(10 * time.Millisecond)})
		c := &calls{fn: func(n int, _ *mq.Delivery) error {
			if n < 3 {
				return errUnavailable
			}
			return nil
		}}
		r := h.run(retry(c))
		h.publish(1, nil)
		waitFor(t, "the message to be acked", func() bool { return h.snapshot().Acked == 1 })
		_ = r.stop()
		if s := h.snapshot(); s.Requeued != 0 || s.Expired != 0 {
			t.Errorf("snapshot = %+v, want the message acked in the handler that first held it", s)
		}
		if got := c.count(); got != 3 {
			t.Errorf("the handler ran %d times, want 3", got)
		}
	}
	t.Run("on the broker's clock", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check(t, memory.TestRealClock(), memory.TestTopology(memory.TopologyConfig{AckDeadline: 100 * time.Millisecond}))
		})
	})
	t.Run("on another clock", func(t *testing.T) { check(t) })
}

func TestRetryDoesNotCallTheHandlerAgainOnALeaseThatIsOver(t *testing.T) {
	// The first attempt overran its 100ms lease and then failed transiently.
	// Reading a time past the lease, the guard took it for a clock the lease
	// was not granted on and let the 10ms wait through, so the handler ran
	// again on a message the broker had already handed to another handler.
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, memory.TestRealClock(), memory.TestTopology(memory.TopologyConfig{AckDeadline: 100 * time.Millisecond}))
		var late atomic.Int64
		retry := mustRetry(t, middleware.RetryConfig{MaxAttempts: 3, Backoff: mq.ConstantBackoff(10 * time.Millisecond)})
		c := &calls{fn: func(n int, d *mq.Delivery) error {
			if !time.Now().Before(d.Info().LeaseExpires) {
				late.Add(1)
			}
			if n == 1 {
				time.Sleep(150 * time.Millisecond)
			}
			return errUnavailable
		}}
		r := h.run(retry(c), mq.WithConcurrency(4), mq.WithMaxAttempts(100))
		h.publish(1, nil)
		time.Sleep(time.Second)
		_ = r.stop()
		if got := late.Load(); got != 0 {
			t.Errorf("the handler was called %d times on a lease that was over, want 0", got)
		}
		if c.count() < 2 {
			t.Errorf("the handler ran %d times, want the redelivered message handled too", c.count())
		}
	})
}

func TestRetrySleepsTheWaitsOfALeaseTheRunnerKeepsAlive(t *testing.T) {
	// Under WithLeaseExtension the runner extends the lease while the handler
	// holds the delivery, so the lease the driver granted says nothing about
	// how long Retry may wait. Measured against it, each 60ms wait on a 100ms
	// lease went back to the runner as a failure, and the runner requeued the
	// message four times: four redeliveries a broker counts towards its
	// dead-letter bound, for waits the runner was holding safely.
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, memory.TestRealClock(), memory.TestTopology(memory.TopologyConfig{AckDeadline: 100 * time.Millisecond}))
		retry := mustRetry(t, middleware.RetryConfig{MaxAttempts: 5, Backoff: mq.ConstantBackoff(60 * time.Millisecond)})
		c := &calls{fn: func(n int, _ *mq.Delivery) error {
			if n < 5 {
				return errUnavailable
			}
			return nil
		}}
		r := h.run(retry(c), mq.WithLeaseExtension(30*time.Millisecond), mq.WithMaxAttempts(100))
		h.publish(1, nil)
		time.Sleep(2 * time.Second)
		_ = r.stop()
		if s := h.snapshot(); s.Acked != 1 || s.Nacked != 0 || s.Requeued != 0 || s.Expired != 0 {
			t.Errorf("snapshot = %+v, want the message acked by the handler that first held it", s)
		}
		if got := c.count(); got != 5 {
			t.Errorf("the handler ran %d times, want 5", got)
		}
	})
}

func TestRetryWithoutConfigurationCostsTheChainNothing(t *testing.T) {
	inner := mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil })
	got := mustRetry(t, middleware.RetryConfig{})(inner)
	if _, isFunc := got.(mq.HandlerFunc); !isFunc {
		t.Fatalf("Retry with no attempts returned %T, want the handler it was given", got)
	}
	if err := got.Handle(context.Background(), delivery("x")); err != nil {
		t.Fatal(err)
	}
}

func TestRetryRefusesANegativeAttemptCount(t *testing.T) {
	if _, err := middleware.Retry(middleware.RetryConfig{MaxAttempts: -1}); !errors.Is(err, middleware.ErrInvalidConfig) {
		t.Fatal("Retry with a negative MaxAttempts did not fail")
	}
}

// ---------------------------------------------------------------- Logging

func TestLoggingLogsFailuresOnly(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	l := slog.New(slog.NewTextHandler(&lockedWriter{w: &buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h := middleware.Logging(l)(&calls{fn: func(n int, _ *mq.Delivery) error {
		if n == 1 {
			return nil
		}
		return errBoom
	}})
	d := delivery("secret payload")
	d.ID = "msg-1"
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	afterSuccess := buf.String()
	mu.Unlock()
	if afterSuccess != "" {
		t.Fatalf("a successful delivery was logged: %q (the specification forbids per-message logging at Info+)", afterSuccess)
	}
	if err := h.Handle(context.Background(), d); !errors.Is(err, errBoom) {
		t.Fatalf("Handle = %v, want the handler's error", err)
	}
	mu.Lock()
	out := buf.String()
	mu.Unlock()
	if !strings.Contains(out, "handler failed") || !strings.Contains(out, "msg-1") {
		t.Fatalf("the failure was not logged with its message id: %q", out)
	}
	if strings.Contains(out, "secret payload") {
		t.Fatalf("the log line contains the body: %q (never logs bodies)", out)
	}
}

func TestLoggingWithNoLoggerCostsTheChainNothing(t *testing.T) {
	inner := mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return errBoom })
	if got := middleware.Logging(nil)(inner); got == nil {
		t.Fatal("Logging(nil) returned no handler")
	} else if _, isFunc := got.(mq.HandlerFunc); !isFunc {
		t.Fatalf("Logging(nil) returned %T, want the handler it was given", got)
	}
}

// lockedWriter serialises writes so that the test may read the buffer while
// the logger is not writing.
type lockedWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// ---------------------------------------------------------------- Correlation

func TestCorrelationIDReadsThePropertyAndTheHeader(t *testing.T) {
	cases := []struct {
		name string
		set  func(*mq.Delivery)
		want string
	}{
		{"property", func(d *mq.Delivery) { d.Properties.CorrelationID = "corr-1" }, "corr-1"},
		{"header", func(d *mq.Delivery) {
			d.Headers.Set(middleware.HeaderCorrelationID, []byte("corr-2"))
		}, "corr-2"},
		{"property wins", func(d *mq.Delivery) {
			d.Properties.CorrelationID = "corr-3"
			d.Headers.Set(middleware.HeaderCorrelationID, []byte("corr-4"))
		}, "corr-3"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got string
			h := middleware.CorrelationID(middleware.CorrelationConfig{})(
				mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
					got, _ = middleware.CorrelationFromContext(ctx)
					return nil
				}),
			)
			d := delivery("x")
			c.set(d)
			if err := h.Handle(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Fatalf("correlation id = %q, want %q", got, c.want)
			}
		})
	}
}

func TestCorrelationIDGeneratesOnlyWhenAsked(t *testing.T) {
	var got string
	var ok bool
	plain := middleware.CorrelationID(middleware.CorrelationConfig{})(
		mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
			got, ok = middleware.CorrelationFromContext(ctx)
			return nil
		}),
	)
	if err := plain.Handle(context.Background(), delivery("x")); err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("a correlation id (%q) was invented for a delivery that carried none", got)
	}
	gen := middleware.CorrelationID(middleware.CorrelationConfig{Generate: func() string { return "made-up" }})(
		mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
			got, ok = middleware.CorrelationFromContext(ctx)
			return nil
		}),
	)
	if err := gen.Handle(context.Background(), delivery("x")); err != nil {
		t.Fatal(err)
	}
	if !ok || got != "made-up" {
		t.Fatalf("correlation id = %q, %v, want the generated one", got, ok)
	}
}

func TestCorrelationIDDoesNotMutateTheDelivery(t *testing.T) {
	h := middleware.CorrelationID(middleware.CorrelationConfig{Generate: func() string { return "made-up" }})(
		mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }),
	)
	d := delivery("x")
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if d.Properties.CorrelationID != "" {
		t.Fatalf("the delivery's correlation id was overwritten with %q; a retry and a dead-letter copy would carry an id the sender never sent", d.Properties.CorrelationID)
	}
	if _, ok := d.Headers.Get(middleware.HeaderCorrelationID); ok {
		t.Fatal("a correlation-id header was written onto the delivery")
	}
}

func TestCorrelationIDPropagatesTheTraceContext(t *testing.T) {
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var got middleware.TraceContext
	var ok bool
	h := middleware.CorrelationID(middleware.CorrelationConfig{Trace: true})(
		mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
			got, ok = middleware.TraceFromContext(ctx)
			return nil
		}),
	)
	d := delivery("x")
	d.Headers.Set(middleware.HeaderTraceParent, []byte(tp))
	d.Headers.Set(middleware.HeaderTraceState, []byte("acme=1,other=2"))
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("no trace context reached the handler")
	}
	if got.String() != tp {
		t.Fatalf("trace context = %q, want %q", got, tp)
	}
	if got.State != "acme=1,other=2" {
		t.Fatalf("tracestate = %q, want the header's own", got.State)
	}
	if !got.Sampled() {
		t.Fatal("the sampled flag was lost")
	}
}

func TestAMalformedTraceContextIsIgnoredAndNotFatal(t *testing.T) {
	// The specification: broker-delivered metadata is untrusted. A broken trace header is
	// not a broken message, and failing the delivery for one would let a
	// publisher stop a consumer by writing nonsense into a header.
	var ok bool
	h := middleware.CorrelationID(middleware.CorrelationConfig{Trace: true})(
		mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
			_, ok = middleware.TraceFromContext(ctx)
			return nil
		}),
	)
	d := delivery("x")
	d.Headers.Set(middleware.HeaderTraceParent, []byte("not a trace context at all"))
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatalf("Handle = %v, want nil: a malformed trace header is not a failed delivery", err)
	}
	if ok {
		t.Fatal("a trace context was produced from a malformed header")
	}
}

func TestTraceStateIsBounded(t *testing.T) {
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var got middleware.TraceContext
	h := middleware.CorrelationID(middleware.CorrelationConfig{Trace: true})(
		mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
			got, _ = middleware.TraceFromContext(ctx)
			return nil
		}),
	)
	d := delivery("x")
	d.Headers.Set(middleware.HeaderTraceParent, []byte(tp))
	d.Headers.Set(middleware.HeaderTraceState, bytes.Repeat([]byte("a"), 4096))
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if len(got.State) > middleware.MaxTraceStateLen {
		t.Fatalf("tracestate is %d bytes, want at most %d (bounded metadata)", len(got.State), middleware.MaxTraceStateLen)
	}
}

func TestTraceStateWithControlCharactersIsDropped(t *testing.T) {
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var got middleware.TraceContext
	h := middleware.CorrelationID(middleware.CorrelationConfig{Trace: true})(
		mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
			got, _ = middleware.TraceFromContext(ctx)
			return nil
		}),
	)
	d := delivery("x")
	d.Headers.Set(middleware.HeaderTraceParent, []byte(tp))
	d.Headers.Set(middleware.HeaderTraceState, []byte("acme=1\n injected: yes"))
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if got.State != "" {
		t.Fatalf("tracestate = %q, want it dropped: a value with a control character is not a trace state and forwarding it is an injection", got.State)
	}
}

// ---------------------------------------------------------------- RateLimit

func TestRateLimitHandsTheMessageBackInsteadOfHoldingIt(t *testing.T) {
	clk := newFakeClock()
	c := &calls{}
	h := mustRateLimit(t, middleware.RateLimitConfig{Rate: 10, Burst: 2, Clock: clk})(c)
	// The burst passes.
	for i := range 2 {
		if err := h.Handle(context.Background(), delivery("x")); err != nil {
			t.Fatalf("delivery %d within the burst = %v, want nil", i, err)
		}
	}
	// The next is refused and asks for a redelivery, without calling the
	// handler and without sleeping.
	err := h.Handle(context.Background(), delivery("x"))
	if !errors.Is(err, middleware.ErrRateLimited) {
		t.Fatalf("Handle = %v, want ErrRateLimited", err)
	}
	dir, delay := mq.DirectiveOf(err)
	if dir != mq.DirectivePostpone {
		t.Fatalf("directive = %v, want DirectivePostpone: the delivery goes back to the runner rather than sleeping here, and the refusal is not a failed attempt", dir)
	}
	if delay <= 0 || delay > 100*time.Millisecond {
		t.Fatalf("delay = %v, want roughly one token's worth (100ms at 10/s)", delay)
	}
	if c.count() != 2 {
		t.Fatalf("the handler ran %d times, want 2: the refused delivery must not reach it", c.count())
	}
	if mq.CodeOf(err) != mq.CodeThrottled {
		t.Fatalf("CodeOf = %v, want CodeThrottled", mq.CodeOf(err))
	}
	// Time passes and the bucket refills.
	clk.advance(time.Second)
	if err := h.Handle(context.Background(), delivery("x")); err != nil {
		t.Fatalf("Handle after the bucket refilled = %v, want nil", err)
	}
}

func TestRateLimitDoesNotExceedTheBurstAfterAnIdleSpell(t *testing.T) {
	// One per second, a burst of one. An idle spell of 1.9 s refills the
	// bucket; the 0.9 s past the first token was earned while the bucket was
	// full and must not count toward the next one, or a second delivery
	// passes 0.1 s after the first.
	clk := newFakeClock()
	c := &calls{}
	h := mustRateLimit(t, middleware.RateLimitConfig{Rate: 1, Burst: 1, Clock: clk})(c)
	if err := h.Handle(context.Background(), delivery("x")); err != nil {
		t.Fatalf("the first delivery = %v, want nil", err)
	}
	clk.advance(1900 * time.Millisecond)
	if err := h.Handle(context.Background(), delivery("x")); err != nil {
		t.Fatalf("the delivery after the idle spell = %v, want nil", err)
	}
	clk.advance(100 * time.Millisecond)
	if err := h.Handle(context.Background(), delivery("x")); !errors.Is(err, middleware.ErrRateLimited) {
		t.Fatalf("a delivery 0.1 s after a full bucket's only token = %v, want ErrRateLimited: Burst 1 let two through", err)
	}
	if c.count() != 2 {
		t.Fatalf("the handler ran %d times, want 2", c.count())
	}
}

func TestRateLimitRefusesAnUnusableConfiguration(t *testing.T) {
	for _, cfg := range []middleware.RateLimitConfig{
		{Rate: 0, Burst: 1},
		{Rate: -1, Burst: 1},
		{Rate: 1, Burst: 0},
		{Rate: 1, Burst: -1},
		{Rate: 1, Burst: 1, MaxDelay: -time.Second},
		// One token per less than a nanosecond: the interval rounds to zero
		// and the refill would divide by it.
		{Rate: 2e9, Burst: 1},
		// One token per several centuries: the interval overflows a
		// time.Duration, so the bucket would never refill.
		{Rate: 1e-11, Burst: 1},
	} {
		if _, err := middleware.RateLimit(cfg); !errors.Is(err, middleware.ErrInvalidConfig) {
			t.Fatalf("RateLimit(%+v) did not fail", cfg)
		}
	}
}

func TestRateLimitAcceptsTheRepresentableExtremes(t *testing.T) {
	for _, rate := range []float64{1e9, 1e-9} {
		clk := newFakeClock()
		h := mustRateLimit(t, middleware.RateLimitConfig{Rate: rate, Burst: 1, Clock: clk})(&calls{})
		if err := h.Handle(context.Background(), delivery("x")); err != nil {
			t.Fatalf("rate %g: first delivery = %v, want nil", rate, err)
		}
		err := h.Handle(context.Background(), delivery("x"))
		if _, delay := mq.DirectiveOf(err); delay <= 0 {
			t.Fatalf("rate %g: refused delivery asked for a %v delay, want a positive one", rate, delay)
		}
		clk.advance(time.Duration(float64(time.Second) / rate))
		if err := h.Handle(context.Background(), delivery("x")); err != nil {
			t.Fatalf("rate %g: delivery after one interval = %v, want nil", rate, err)
		}
	}
}

// ---------------------------------------------------------------- Breaker

func TestCircuitBreakerOpensAndRecovers(t *testing.T) {
	clk := newFakeClock()
	var states []string
	fail := atomic.Bool{}
	fail.Store(true)
	c := &calls{fn: func(int, *mq.Delivery) error {
		if fail.Load() {
			return errUnavailable
		}
		return nil
	}}
	h := mustCircuitBreaker(t, middleware.BreakerConfig{
		Threshold: 2,
		Cooldown:  time.Second,
		Clock:     clk,
		OnStateChange: func(_, to middleware.BreakerState) {
			states = append(states, to.String())
		},
	})(c)

	for range 2 {
		if err := h.Handle(context.Background(), delivery("x")); !errors.Is(err, errUnavailable) {
			t.Fatalf("Handle = %v, want the dependency's own failure while the circuit is closed", err)
		}
	}
	if c.count() != 2 {
		t.Fatalf("the handler ran %d times before the threshold, want 2", c.count())
	}
	// Open: refused without calling the handler, and handed back.
	err := h.Handle(context.Background(), delivery("x"))
	if !errors.Is(err, middleware.ErrCircuitOpen) {
		t.Fatalf("Handle = %v, want ErrCircuitOpen", err)
	}
	if dir, delay := mq.DirectiveOf(err); dir != mq.DirectivePostpone || delay <= 0 {
		t.Fatalf("directive = %v delay = %v, want Postpone with the remaining cooldown", dir, delay)
	}
	if c.count() != 2 {
		t.Fatalf("the handler ran %d times while the circuit was open, want 2", c.count())
	}
	// After the cooldown one probe is let through; it still fails, so the
	// circuit re-opens.
	clk.advance(time.Second)
	if err := h.Handle(context.Background(), delivery("x")); !errors.Is(err, errUnavailable) {
		t.Fatalf("the probe = %v, want the dependency's own failure", err)
	}
	if c.count() != 3 {
		t.Fatalf("the handler ran %d times, want 3: exactly one probe", c.count())
	}
	if err := h.Handle(context.Background(), delivery("x")); !errors.Is(err, middleware.ErrCircuitOpen) {
		t.Fatalf("Handle = %v, want the circuit open again after a failed probe", err)
	}
	// The dependency recovers.
	fail.Store(false)
	clk.advance(time.Second)
	if err := h.Handle(context.Background(), delivery("x")); err != nil {
		t.Fatalf("the probe after recovery = %v, want nil", err)
	}
	if err := h.Handle(context.Background(), delivery("x")); err != nil {
		t.Fatalf("Handle after recovery = %v, want the circuit closed", err)
	}
	want := []string{"open", "halfopen", "open", "halfopen", "closed"}
	if strings.Join(states, ",") != strings.Join(want, ",") {
		t.Fatalf("states = %v, want %v", states, want)
	}
}

func TestCircuitBreakerLetsOnlyOneProbeThrough(t *testing.T) {
	// The probe is HELD inside the handler until every other goroutine has
	// asked the breaker for permission. Without that the first probe returns
	// before the others arrive, the circuit is open again by the time they
	// ask, and the test passes whether or not the half-open state admits one
	// delivery or a thousand -- which is the whole property.
	clk := newFakeClock()
	release := make(chan struct{})
	var inside atomic.Int64
	c := &calls{fn: func(n int, _ *mq.Delivery) error {
		if n > 1 {
			inside.Add(1)
			<-release
		}
		return errUnavailable
	}}
	h := mustCircuitBreaker(t, middleware.BreakerConfig{Threshold: 1, Cooldown: time.Second, Clock: clk})(c)
	_ = h.Handle(context.Background(), delivery("x")) // trips it
	clk.advance(time.Second)

	const n = 32
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = h.Handle(context.Background(), delivery("x"))
		}()
	}
	// Give every goroutine time to ask, then let the probe finish.
	deadline := time.Now().Add(2 * time.Second)
	for inside.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	probes := inside.Load()
	close(release)
	wg.Wait()
	if probes != 1 {
		t.Fatalf("%d deliveries were inside a half-open circuit at once, want exactly 1", probes)
	}
	// The handler's own call count is the other half of the same statement:
	// errors.Is matches by CODE in this repository, so ErrCircuitOpen
	// and any other CodeUnavailable failure -- including the one this handler
	// returns -- are indistinguishable by it.
	if total := c.count(); total != 2 {
		t.Fatalf("the handler ran %d times in total, want 2 (the trip and one probe)", total)
	}
}

func TestCircuitBreakerIgnoresAnApplicationsOwnRejection(t *testing.T) {
	// A handler reporting invalid input is reporting on the message, not on
	// the dependency: a breaker that counted it would take a consumer down
	// because someone published nonsense.
	clk := newFakeClock()
	c := &calls{fn: func(int, *mq.Delivery) error { return mq.ErrMessageTooLarge }}
	h := mustCircuitBreaker(t, middleware.BreakerConfig{Threshold: 2, Cooldown: time.Second, Clock: clk})(c)
	for range 10 {
		if err := h.Handle(context.Background(), delivery("x")); !errors.Is(err, mq.ErrMessageTooLarge) {
			t.Fatalf("Handle = %v, want the handler's own error; the circuit must stay closed", err)
		}
	}
	if c.count() != 10 {
		t.Fatalf("the handler ran %d times, want 10: the circuit must not have opened", c.count())
	}
}

func TestCircuitBreakerRefusesAnUnusableConfiguration(t *testing.T) {
	for _, cfg := range []middleware.BreakerConfig{
		{Threshold: 0, Cooldown: time.Second},
		{Threshold: -1, Cooldown: time.Second},
		{Threshold: 1, Cooldown: 0},
		{Threshold: 1, Cooldown: -time.Second},
	} {
		if _, err := middleware.CircuitBreaker(cfg); !errors.Is(err, middleware.ErrInvalidConfig) {
			t.Fatalf("CircuitBreaker(%+v) did not fail", cfg)
		}
	}
}

func TestBreakerStateString(t *testing.T) {
	for _, c := range []struct {
		s    middleware.BreakerState
		want string
	}{
		{middleware.BreakerClosed, "closed"},
		{middleware.BreakerOpen, "open"},
		{middleware.BreakerHalfOpen, "halfopen"},
		{middleware.BreakerState(9), "BreakerState(?)"},
	} {
		if got := c.s.String(); got != c.want {
			t.Fatalf("String() = %q, want %q", got, c.want)
		}
		if c.s.Valid() != (c.want != "BreakerState(?)") {
			t.Fatalf("Valid() = %v for %q", c.s.Valid(), c.want)
		}
	}
}

func TestCircuitBreakerCountsAPanickingProbeAsAFailure(t *testing.T) {
	// The runner recovers a panic above the breaker. A probe that panicked
	// and was never recorded left the circuit half-open with its one probe
	// "still running", so it refused every delivery for ever.
	clk := newFakeClock()
	c := &calls{fn: func(n int, _ *mq.Delivery) error {
		if n == 2 {
			panic("in the probe")
		}
		return errUnavailable
	}}
	h := mustCircuitBreaker(t, middleware.BreakerConfig{Threshold: 1, Cooldown: time.Second, Clock: clk})(c)
	_ = h.Handle(context.Background(), delivery("x")) // trips it
	clk.advance(time.Second)
	if _, panicked := handleRecovering(h, delivery("x")); !panicked {
		t.Fatal("the probe's panic did not reach the caller")
	}
	if err := h.Handle(context.Background(), delivery("x")); !errors.Is(err, middleware.ErrCircuitOpen) || c.count() != 2 {
		t.Fatalf("Handle after a panicking probe = %v (handler ran %d times), want the circuit open again", err, c.count())
	}
	clk.advance(time.Second)
	_ = h.Handle(context.Background(), delivery("x"))
	if c.count() != 3 {
		t.Fatalf("the handler ran %d times, want 3: a new probe after the cooldown", c.count())
	}
}

func TestCircuitBreakerIgnoresASlowCallFromBeforeItOpened(t *testing.T) {
	// A call admitted while the circuit was closed and answering after it
	// opened says nothing about the dependency now. Counting its success
	// closed the circuit without a cooldown or a probe.
	clk := newFakeClock()
	inside := make(chan struct{})
	release := make(chan struct{})
	c := &calls{fn: func(n int, _ *mq.Delivery) error {
		if n == 1 {
			close(inside)
			<-release
			return nil
		}
		return errUnavailable
	}}
	h := mustCircuitBreaker(t, middleware.BreakerConfig{Threshold: 1, Cooldown: time.Second, Clock: clk})(c)
	done := make(chan error, 1)
	go func() { done <- h.Handle(context.Background(), delivery("slow")) }()
	<-inside
	_ = h.Handle(context.Background(), delivery("x")) // trips it
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("the slow call = %v, want nil", err)
	}
	if err := h.Handle(context.Background(), delivery("x")); !errors.Is(err, middleware.ErrCircuitOpen) || c.count() != 2 {
		t.Fatalf("Handle after a stale success = %v (handler ran %d times), want the circuit still open", err, c.count())
	}
}

// handleRecovering calls h the way the runner does, recovering a panic.
func handleRecovering(h mq.Handler, d *mq.Delivery) (err error, panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	return h.Handle(context.Background(), d), false
}

// ---------------------------------------------------------------- Dedup

func TestDedupForgetsAKeyWhoseHandlerFailed(t *testing.T) {
	// A store that remembered a key whose handler failed would turn the
	// broker's redelivery into a silent ack -- the message is suppressed as a
	// duplicate of a delivery that never succeeded. It is the worst failure
	// this middleware can have, which is why forgetting is the default and
	// keeping is the named opt-out.
	cases := []struct {
		name string
		cfg  middleware.DedupConfig
		want int64
		why  string
	}{
		{"default forgets", middleware.DedupConfig{}, 2, "the redelivery must reach the handler again"},
		{"KeepOnFailure keeps", middleware.DedupConfig{KeepOnFailure: true}, 1, "the caller asked for the key to survive the failure"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := c.cfg
			cfg.Store = mustNewLRUStore(t, 16)
			counter := &calls{fn: func(n int, _ *mq.Delivery) error {
				if n == 1 {
					return errBoom
				}
				return nil
			}}
			h := middleware.Dedup(cfg)(counter)
			d := delivery("x")
			d.ID = "id-1"
			if err := h.Handle(context.Background(), d); !errors.Is(err, errBoom) {
				t.Fatalf("the first delivery = %v, want the handler's failure", err)
			}
			// The broker redelivers the same message.
			_ = h.Handle(context.Background(), delivery2("x", "id-1"))
			if got := counter.count(); got != c.want {
				t.Fatalf("the handler ran %d times over a failure and its redelivery, want %d (%s)", got, c.want, c.why)
			}
		})
	}
}

func TestDedupForgetsAKeyWhoseHandlerPanicked(t *testing.T) {
	// The runner recovers a panic above Dedup, so a key forgotten only on a
	// returned error stayed seen, and the redelivery was acked unprocessed.
	counter := &calls{fn: func(n int, _ *mq.Delivery) error {
		if n == 1 {
			panic("in the handler")
		}
		return nil
	}}
	h := middleware.Dedup(middleware.DedupConfig{Store: mustNewLRUStore(t, 16)})(counter)
	if _, panicked := handleRecovering(h, delivery2("x", "id-1")); !panicked {
		t.Fatal("the handler's panic did not reach the caller")
	}
	_ = h.Handle(context.Background(), delivery2("x", "id-1"))
	if got := counter.count(); got != 2 {
		t.Fatalf("the handler ran %d times over a panic and its redelivery, want 2", got)
	}
}

func TestDedupHandsBackARedeliveryWhileTheFirstIsInFlight(t *testing.T) {
	// A redelivery that arrives while the first copy is still in the handler
	// is not a duplicate. Acking it lost the message when the first copy then
	// failed.
	inside := make(chan struct{})
	release := make(chan struct{})
	counter := &calls{fn: func(n int, _ *mq.Delivery) error {
		if n == 1 {
			close(inside)
			<-release
			return errBoom
		}
		return nil
	}}
	h := middleware.Dedup(middleware.DedupConfig{Store: mustNewLRUStore(t, 16)})(counter)
	done := make(chan error, 1)
	go func() { done <- h.Handle(context.Background(), delivery2("x", "id-1")) }()
	<-inside
	err := h.Handle(context.Background(), delivery2("x", "id-1"))
	if err == nil || mq.CodeOf(err) != mq.CodeUnavailable {
		t.Fatalf("a redelivery while the first is in flight = %v, want a CodeUnavailable failure so it is redelivered", err)
	}
	if dir, _ := mq.DirectiveOf(err); dir != mq.DirectivePostpone {
		t.Fatalf("directive = %v, want DirectivePostpone: the redelivery was not handled, so it is not a failed attempt", dir)
	}
	close(release)
	if err := <-done; !errors.Is(err, errBoom) {
		t.Fatalf("the first delivery = %v, want the handler's failure", err)
	}
	if err := h.Handle(context.Background(), delivery2("x", "id-1")); err != nil {
		t.Fatalf("the redelivery after the failure = %v, want nil", err)
	}
	if got := counter.count(); got != 2 {
		t.Fatalf("the handler ran %d times, want 2: the failure and the redelivery after it", got)
	}
}

// ---------------------------------------------------------------- Metrics

// recorder collects what Metrics reports.
type recorder struct {
	mu      sync.Mutex
	entries []string
}

func (r *recorder) HandlerDuration(topic, sub, outcome string, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, topic+"/"+sub+"/"+outcome)
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.entries...)
}

func TestMetricsRecordsEveryOutcome(t *testing.T) {
	r := &recorder{}
	n := 0
	h := middleware.Metrics(r, newFakeClock())(middleware.Recover(nil)(
		mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			n++
			switch n {
			case 1:
				return nil
			case 2:
				return errBoom
			default:
				panic("in the handler")
			}
		}),
	))
	for range 3 {
		_ = h.Handle(context.Background(), delivery("x"))
	}
	got := strings.Join(r.all(), " ")
	want := "//success //failure //panic"
	if got != want {
		t.Fatalf("recorded %q, want %q", got, want)
	}
}

func TestMetricsWithNoRecorderCostsTheChainNothing(t *testing.T) {
	inner := mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil })
	got := middleware.Metrics(nil, nil)(inner)
	if _, isFunc := got.(mq.HandlerFunc); !isFunc {
		t.Fatalf("Metrics(nil, nil) returned %T, want the handler it was given", got)
	}
}

// ---------------------------------------------------------------- Composition

func TestTheChainIsComposedOnceAndNotPerDelivery(t *testing.T) {
	var compositions atomic.Int64
	counting := func(next mq.Handler) mq.Handler {
		compositions.Add(1)
		return next
	}
	five := make([]mq.Middleware, 5)
	for i := range five {
		five[i] = counting
	}
	h := mq.Chain(five...)(&calls{})
	for range 1000 {
		if err := h.Handle(context.Background(), delivery("x")); err != nil {
			t.Fatal(err)
		}
	}
	if got := compositions.Load(); got != 5 {
		t.Fatalf("the chain was composed %d times over 1000 deliveries, want 5 (once per middleware)", got)
	}
}

func TestEveryMiddlewareComposes(t *testing.T) {
	// Each of them wraps and calls the next, so a chain of all of them in
	// their pass-through configuration reaches the handler exactly once.
	c := &calls{}
	key, err := middleware.NewStaticKey(bytes.Repeat([]byte("k"), 32), "k1")
	if err != nil {
		t.Fatal(err)
	}
	chain := mq.Chain(
		middleware.Recover(nil),
		mustTimeout(t, time.Minute),
		mustRetry(t, middleware.RetryConfig{MaxAttempts: 2, Clock: newFakeClock()}),
		middleware.Logging(nil),
		middleware.CorrelationID(middleware.CorrelationConfig{Trace: true}),
		mustRateLimit(t, middleware.RateLimitConfig{Rate: 1e6, Burst: 1000, Clock: newFakeClock()}),
		mustCircuitBreaker(t, middleware.BreakerConfig{Threshold: 100, Cooldown: time.Minute, Clock: newFakeClock()}),
		middleware.Filter(func(*mq.Delivery) bool { return true }),
		middleware.Dedup(middleware.DedupConfig{}),
		middleware.Validate(func(*mq.Delivery) error { return nil }),
		mustCompress(t, middleware.CompressConfig{}),
		middleware.Encrypt(middleware.EncryptConfig{Keys: key}),
		middleware.Metrics(&recorder{}, newFakeClock()),
	)
	h := chain(c)
	d := delivery("x")
	d.ID = "id-1"
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatalf("a chain of every middleware = %v, want nil", err)
	}
	if c.count() != 1 {
		t.Fatalf("the handler ran %d times through the full chain, want 1", c.count())
	}
}

func TestConstantBackoffIsUsable(t *testing.T) {
	// Retry's tests lean on it, so a change to the root's constructor is
	// caught here rather than as a mysterious retry-count failure.
	if got := mq.ConstantBackoff(time.Second).Delay(2); got != time.Second {
		t.Fatalf("ConstantBackoff(1s).Delay(2) = %v, want 1s", got)
	}
}

func mustTimeout(tb testing.TB, d time.Duration) mq.Middleware {
	tb.Helper()
	m, err := middleware.Timeout(d)
	if err != nil {
		tb.Fatalf("Timeout: %v", err)
	}
	return m
}

func mustRetry(tb testing.TB, cfg middleware.RetryConfig) mq.Middleware {
	tb.Helper()
	m, err := middleware.Retry(cfg)
	if err != nil {
		tb.Fatalf("Retry: %v", err)
	}
	return m
}

func mustRateLimit(tb testing.TB, cfg middleware.RateLimitConfig) mq.Middleware {
	tb.Helper()
	m, err := middleware.RateLimit(cfg)
	if err != nil {
		tb.Fatalf("RateLimit: %v", err)
	}
	return m
}

func mustCircuitBreaker(tb testing.TB, cfg middleware.BreakerConfig) mq.Middleware {
	tb.Helper()
	m, err := middleware.CircuitBreaker(cfg)
	if err != nil {
		tb.Fatalf("CircuitBreaker: %v", err)
	}
	return m
}

func mustCompress(tb testing.TB, cfg middleware.CompressConfig) mq.Middleware {
	tb.Helper()
	m, err := middleware.Compress(cfg)
	if err != nil {
		tb.Fatalf("Compress: %v", err)
	}
	return m
}

func mustPublishCompress(tb testing.TB, cfg middleware.CompressConfig) mq.PublishMiddleware {
	tb.Helper()
	m, err := middleware.PublishCompress(cfg)
	if err != nil {
		tb.Fatalf("PublishCompress: %v", err)
	}
	return m
}

func mustWrapPublisher(tb testing.TB, p mq.Publisher, mw ...mq.PublishMiddleware) *middleware.Publisher {
	tb.Helper()
	w, err := middleware.WrapPublisher(p, mw...)
	if err != nil {
		tb.Fatalf("WrapPublisher: %v", err)
	}
	return w
}

func mustNewLRUStore(tb testing.TB, capacity int) *middleware.LRUStore {
	tb.Helper()
	s, err := middleware.NewLRUStore(capacity)
	if err != nil {
		tb.Fatalf("NewLRUStore: %v", err)
	}
	return s
}

// ------------------------------------------- reading a recycled delivery

// recycledDelivery hands h a delivery received under BufferReuse from a
// wildcard topic, published with subject "orders.eu". h is told how to settle
// that delivery and receive the next one, published with subject "orders.us":
// with BufferReuse the second delivery takes over the first one's settlement
// record, so a middleware that reads the first delivery's Info after the
// handler settled it reads "orders.us".
func recycledDelivery(t *testing.T, h func(next mq.Handler) mq.Handler) error {
	t.Helper()
	cl, _ := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{Wildcards: true}), memory.TestTopics("orders"))
	ctx := context.Background()
	c, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing", BufferReuse: true})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	for _, subject := range []string{"orders.eu", "orders.us"} {
		if err := cl.Publish(ctx, "orders", &mq.Message{Topic: subject, Body: []byte("b")}); err != nil {
			t.Fatalf("publishing %s: %v", subject, err)
		}
	}
	first := make([]mq.Delivery, 1)
	if n, err := c.Receive(ctx, first); n != 1 || err != nil {
		t.Fatalf("Receive = %d, %v, want 1 delivery", n, err)
	}
	if got := first[0].Info().Topic; got != "orders.eu" {
		t.Fatalf("the first delivery's topic is %q, want orders.eu", got)
	}
	inner := mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
		if err := d.Ack(); err != nil {
			t.Errorf("Ack: %v", err)
		}
		next := make([]mq.Delivery, 1)
		if n, err := c.Receive(ctx, next); n != 1 || err != nil {
			t.Errorf("Receive = %d, %v, want 1 delivery", n, err)
		}
		return errBoom
	})
	return h(inner).Handle(ctx, &first[0])
}

func TestMetricsLabelsTheDeliveryItWasGivenAfterTheHandlerSettledIt(t *testing.T) {
	r := &recorder{}
	if err := recycledDelivery(t, middleware.Metrics(r, newFakeClock())); !errors.Is(err, errBoom) {
		t.Fatalf("Handle = %v, want errBoom", err)
	}
	if got, want := r.all(), []string{"orders.eu/billing/" + middleware.OutcomeFailure}; len(got) != 1 || got[0] != want[0] {
		t.Fatalf("Metrics recorded %v, want %v", got, want)
	}
}

func TestLoggingNamesTheDeliveryItWasGivenAfterTheHandlerSettledIt(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, nil))
	if err := recycledDelivery(t, middleware.Logging(l)); !errors.Is(err, errBoom) {
		t.Fatalf("Handle = %v, want errBoom", err)
	}
	if out := buf.String(); !strings.Contains(out, "topic=orders.eu ") {
		t.Fatalf("the log line is %q, want topic=orders.eu", out)
	}
}
