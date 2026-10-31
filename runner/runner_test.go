package runner_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/runner"
)

// publishN publishes n messages onto topic.
func publishN(t *testing.T, cl *mq.Client, topic string, n int) {
	t.Helper()
	for i := range n {
		m := &mq.Message{ID: strconv.Itoa(i), Key: []byte("k" + strconv.Itoa(i)), Body: []byte("body")}
		if err := cl.Publish(context.Background(), topic, m); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
}

// waitFor polls cond for up to five seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// counter is a handler that counts and records the deliveries it saw.
type counter struct {
	n    atomic.Int64
	mu   sync.Mutex
	ids  []string
	hook func(*mq.Delivery) error
}

func (c *counter) Handle(_ context.Context, d *mq.Delivery) error {
	c.mu.Lock()
	c.ids = append(c.ids, d.ID)
	c.mu.Unlock()
	c.n.Add(1)
	if c.hook != nil {
		return c.hook(d)
	}
	return nil
}

func (c *counter) count() int64 { return c.n.Load() }

// run starts runner.Run in the background and waits until its subscription
// exists.
//
// The wait is not politeness: the specification makes StartLatest the default, so a
// message published before the subscription existed is a message the
// subscription never sees — and this package's Run is what opens the
// subscription, so a test cannot open it first.
//
// stop cancels the run's context and returns what Run returned; wait returns
// it without cancelling, for the halts that stop on their own.
type running struct {
	cancel context.CancelFunc
	done   chan error
	t      *testing.T
}

func start(t *testing.T, cl *mq.Client, b *memory.Broker, h mq.Handler, cfg runner.Config) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{cancel: cancel, done: make(chan error, 1), t: t}
	go func() { r.done <- runner.Run(ctx, cl, h, cfg) }()
	t.Cleanup(cancel)
	waitFor(t, "the subscription to be opened", func() bool {
		_, err := b.Inspect(cfg.Consumer.Topic, cfg.Consumer.Subscription)
		return err == nil
	})
	return r
}

// wait returns what Run returned.
func (r *running) wait() error {
	r.t.Helper()
	select {
	case err := <-r.done:
		return err
	case <-time.After(10 * time.Second):
		r.t.Fatal("Run did not return")
		return nil
	}
}

// stop cancels the run and returns what Run returned.
func (r *running) stop() error {
	r.t.Helper()
	r.cancel()
	return r.wait()
}

func TestRunOpensRunsAndReleasesTheConsumer(t *testing.T) {
	cl, b := memory.NewForTest(t, memory.TestTopics("orders"))
	h := &counter{}
	r := start(t, cl, b, h, runner.Config{
		Consumer: mq.ConsumerConfig{Topic: "orders", Subscription: "billing"},
		Options:  []mq.RunOption{mq.WithConcurrency(4)},
		Drain:    5 * time.Second,
	})
	publishN(t, cl, "orders", 16)
	waitFor(t, "every message to be handled", func() bool { return h.count() == 16 })
	err := r.stop()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	// memory.NewForTest closes the client on cleanup and fails the test if a
	// child handle is still open, so a Run that did not release its consumer
	// is caught there rather than here.
	s, ierr := b.Inspect("orders", "billing")
	if ierr != nil {
		t.Fatalf("inspecting: %v", ierr)
	}
	if s.Acked != 16 || s.InFlight != 0 || s.Ready != 0 {
		t.Fatalf("snapshot = %+v, want 16 acked and nothing left in flight", s)
	}
	if s.ActiveConsumers != 0 {
		t.Fatalf("%d consumer(s) still attached after Run returned; the facade owns the consumer it opened and must release it", s.ActiveConsumers)
	}
	if !s.Conserved() {
		t.Fatalf("The specification conservation broken: %+v", s)
	}
}

func TestRunAppliesTheMiddlewareChainOutermostFirst(t *testing.T) {
	cl, b := memory.NewForTest(t, memory.TestTopics("orders"))
	var mu sync.Mutex
	var order []string
	mark := func(name string) mq.Middleware {
		return func(next mq.Handler) mq.Handler {
			return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
				mu.Lock()
				order = append(order, name)
				mu.Unlock()
				return next.Handle(ctx, d)
			})
		}
	}
	h := &counter{}
	r := start(t, cl, b, h, runner.Config{
		Consumer:   mq.ConsumerConfig{Topic: "orders", Subscription: "billing"},
		Middleware: []mq.Middleware{mark("outer"), nil, mark("inner")},
		Drain:      5 * time.Second,
	})
	publishN(t, cl, "orders", 1)
	waitFor(t, "the message to be handled", func() bool { return h.count() == 1 })
	_ = r.stop()
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "outer" || order[1] != "inner" {
		t.Fatalf("chain order = %v, want [outer inner]: Config.Middleware is applied outermost first, and a nil entry is skipped", order)
	}
}

func TestRunComposesTheChainOnceAndNotPerDelivery(t *testing.T) {
	// The specification rule 6: the chain is composed when the run starts, never per
	// message. A facade that rebuilt it per delivery would be invisible to
	// every other test in this file, because the composed handler behaves
	// identically -- it just costs a composition a message.
	cl, b := memory.NewForTest(t, memory.TestTopics("orders"))
	const n = 32
	var compositions atomic.Int64
	counting := func(next mq.Handler) mq.Handler {
		compositions.Add(1)
		return next
	}
	h := &counter{}
	r := start(t, cl, b, h, runner.Config{
		Consumer:   mq.ConsumerConfig{Topic: "orders", Subscription: "billing"},
		Middleware: []mq.Middleware{counting, counting, counting},
		Drain:      5 * time.Second,
	})
	publishN(t, cl, "orders", n)
	waitFor(t, "every message to be handled", func() bool { return h.count() == n })
	_ = r.stop()
	if got := compositions.Load(); got != 3 {
		t.Fatalf("the chain was composed %d times for %d deliveries, want 3 (once per middleware, at Run)", got, n)
	}
}

func TestRunReportsAHalt(t *testing.T) {
	cl, b := memory.NewForTest(t, memory.TestTopics("orders"))
	boom := errors.New("the handler always fails")
	h := &counter{hook: func(*mq.Delivery) error { return boom }}
	r := start(t, cl, b, h, runner.Config{
		Consumer: mq.ConsumerConfig{Topic: "orders", Subscription: "billing"},
		Options:  []mq.RunOption{mq.WithMaxAttempts(1)},
		Drain:    5 * time.Second,
	})
	publishN(t, cl, "orders", 1)
	err := r.wait()
	if !errors.Is(err, mq.ErrPoisonMessage) {
		t.Fatalf("Run = %v, want the halt to reach the caller as ErrPoisonMessage", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want the handler's own error reachable through it", err)
	}
}

// TestConfigDrainBoundsTheHandlersInFlight is Config.Drain as the bound of
// the drain of the handlers still running when ctx ends. ConsumerHandle.Run
// drains them itself before it returns, bounded by mq.WithDrainTimeout, and
// the runner's Shutdown only came after, with nothing left in flight. So a
// Drain shorter than mq.DefaultDrainTimeout bounded nothing (a process sized
// to it was killed while Run still waited), and a longer one was cut to the
// run's own bound.
func TestConfigDrainBoundsTheHandlersInFlight(t *testing.T) {
	t.Run("shorter than the run's default", func(t *testing.T) {
		cl, b := memory.NewForTest(t, memory.TestTopics("orders"), memory.TestRealClock())
		entered := make(chan struct{}, 1)
		cut := make(chan time.Time, 1)
		release := make(chan struct{})
		defer close(release)
		h := mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-ctx.Done():
				cut <- time.Now()
				return ctx.Err()
			case <-release:
				return nil
			}
		})
		r := start(t, cl, b, h, runner.Config{
			Consumer: mq.ConsumerConfig{Topic: "orders", Subscription: "billing"},
			Drain:    300 * time.Millisecond,
		})
		publishN(t, cl, "orders", 1)
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the handler never ran")
		}
		stopped := time.Now()
		r.cancel()
		select {
		case at := <-cut:
			if took := at.Sub(stopped); took < 200*time.Millisecond {
				t.Fatalf("the handler's context was cancelled %v after the stop, want about Drain (300ms)", took)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("Config.Drain = 300ms, but 3s after ctx ended the handler still runs with a live context: "+
				"the drain was bounded by mq.DefaultDrainTimeout (%v)", mq.DefaultDrainTimeout)
		}
		if err := r.wait(); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	})
	t.Run("longer than a WithDrainTimeout in Options", func(t *testing.T) {
		cl, b := memory.NewForTest(t, memory.TestTopics("orders"), memory.TestRealClock())
		entered := make(chan struct{}, 1)
		result := make(chan error, 1)
		h := mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-ctx.Done():
				result <- ctx.Err()
				return ctx.Err()
			case <-time.After(600 * time.Millisecond):
				result <- nil
				return nil
			}
		})
		r := start(t, cl, b, h, runner.Config{
			Consumer: mq.ConsumerConfig{Topic: "orders", Subscription: "billing"},
			Options:  []mq.RunOption{mq.WithDrainTimeout(100 * time.Millisecond)},
			Drain:    5 * time.Second,
		})
		publishN(t, cl, "orders", 1)
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the handler never ran")
		}
		err := r.stop()
		if herr := <-result; herr != nil {
			t.Fatalf("Config.Drain = 5s, yet the 600ms handler in flight had its context cancelled (%v); Run returned %v", herr, err)
		}
		s, ierr := b.Inspect("orders", "billing")
		if ierr != nil {
			t.Fatalf("inspecting: %v", ierr)
		}
		if s.Acked != 1 {
			t.Fatalf("snapshot = %+v, want the drained delivery acked", s)
		}
	})
}

func TestRunRefusesAnUnusableConfiguration(t *testing.T) {
	cl, _ := memory.NewForTest(t, memory.TestTopics("orders"))
	h := &counter{}
	cases := []struct {
		name string
		cfg  runner.Config
		cl   *mq.Client
		h    mq.Handler
	}{
		{"no topic", runner.Config{Consumer: mq.ConsumerConfig{Subscription: "billing"}}, cl, h},
		{"no subscription", runner.Config{Consumer: mq.ConsumerConfig{Topic: "orders"}}, cl, h},
		{"negative drain", runner.Config{
			Consumer: mq.ConsumerConfig{Topic: "orders", Subscription: "billing"},
			Drain:    -time.Second,
		}, cl, h},
		{"no client", runner.Config{Consumer: mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}}, nil, h},
		{"no handler", runner.Config{Consumer: mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}}, cl, nil},
		// A nil HandlerFunc is a non-nil Handler, and used to open the
		// consumer and halt on its first delivery.
		{"nil handler func", runner.Config{Consumer: mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}}, cl, mq.HandlerFunc(nil)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Bounded, so that a configuration Run accepts by mistake fails
			// here instead of running until the test binary times out.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := runner.Run(ctx, c.cl, c.h, c.cfg)
			if !errors.Is(err, runner.ErrInvalidConfig) {
				t.Fatalf("Run = %v, want ErrInvalidConfig", err)
			}
			if mq.CodeOf(err) != mq.CodeInvalidConfig {
				t.Fatalf("CodeOf(Run) = %v, want CodeInvalidConfig", mq.CodeOf(err))
			}
			if err.Error() != runner.ErrInvalidConfig.Error() {
				t.Fatalf("Run = %q, want this package's own %q", err, runner.ErrInvalidConfig)
			}
		})
	}
}

func TestRunReportsAConsumerThatCannotBeOpened(t *testing.T) {
	// The consumer's own configuration is the root package's to validate,
	// and a Prefetch the core refuses must reach the caller as the core's
	// failure rather than as this package's ErrInvalidConfig: the facade
	// checks what it owns and nothing more. Both carry CodeInvalidConfig, and
	// errors.Is matches by code, so the message is what tells them
	// apart.
	cl, _ := memory.NewForTest(t, memory.TestTopics("orders"))
	h := &counter{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := runner.Run(ctx, cl, h, runner.Config{
		Consumer: mq.ConsumerConfig{Topic: "orders", Subscription: "billing", Prefetch: -1},
		Drain:    time.Second,
	})
	if err == nil {
		t.Fatal("Run = nil for a consumer configuration the core refuses, want the open failure")
	}
	if !strings.Contains(err.Error(), "Prefetch") {
		t.Fatalf("Run = %q, want the core's own open failure and not this package's validation", err)
	}
}

func TestValidateAcceptsWhatRunAccepts(t *testing.T) {
	ok := runner.Config{Consumer: mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("Validate = %v for a usable configuration, want nil", err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		if err := ok.Validate(); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Fatalf("Validate allocated %v times, want 0", got)
	}
}
