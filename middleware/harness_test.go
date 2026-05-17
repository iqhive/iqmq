package middleware_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/memory"
)

// The middlewares that short-circuit the handler — Filter, Validate, Dedup —
// have to SETTLE the delivery, and a delivery a test built itself carries no
// settlement right at all. So those tests run against the real memory
// driver through the real runner, which is the only place a settlement can be
// observed, and they run under mq.SettlementManual: that is the mode in which
// the runner settles nothing, so what the broker's counters show is the
// middleware's own settlement and nobody else's.

// harness is a memory broker, a client and a consumer over one topic.
type harness struct {
	t     *testing.T
	cl    *mq.Client
	b     *memory.Broker
	c     *mq.ConsumerHandle
	topic string
	sub   string
}

func newHarness(t *testing.T, opts ...memory.TestOption) *harness {
	t.Helper()
	opts = append([]memory.TestOption{memory.TestTopics("orders")}, opts...)
	// The runner is what publishes into the subscription in these tests, and the specification makes StartLatest the default, so the consumer is opened
	// before anything is published.
	cl, b := memory.NewForTest(t, opts...)
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &harness{t: t, cl: cl, b: b, c: c, topic: "orders", sub: "billing"}
}

// publish publishes n messages whose ID is id(i).
func (h *harness) publish(n int, id func(i int) string) {
	h.t.Helper()
	for i := range n {
		m := &mq.Message{Body: []byte("body-" + strconv.Itoa(i))}
		if id != nil {
			m.ID = id(i)
		}
		if err := h.cl.Publish(context.Background(), h.topic, m); err != nil {
			h.t.Fatalf("publishing %d: %v", i, err)
		}
	}
}

// snapshot reads the subscription's gauges.
func (h *harness) snapshot() memory.Snapshot {
	h.t.Helper()
	s, err := h.b.Inspect(h.topic, h.sub)
	if err != nil {
		h.t.Fatalf("inspecting: %v", err)
	}
	return s
}

// running is a Run in the background.
type running struct {
	t      *testing.T
	cancel context.CancelFunc
	done   chan error
}

// run starts the runner with the chain composed around handler.
func (h *harness) run(handler mq.Handler, opts ...mq.RunOption) *running {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{t: h.t, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- h.c.Run(ctx, handler, opts...) }()
	h.t.Cleanup(cancel)
	return r
}

// stop cancels the run and returns its error.
func (r *running) stop() error {
	r.t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		return err
	case <-time.After(10 * time.Second):
		r.t.Fatal("Run did not return")
		return nil
	}
}

// wait returns the run's error without cancelling it.
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

// calls counts handler invocations and records the errors it should return.
type calls struct {
	n    atomic.Int64
	mu   sync.Mutex
	ids  []string
	fn   func(n int, d *mq.Delivery) error
	seen chan struct{}
}

func (c *calls) Handle(_ context.Context, d *mq.Delivery) error {
	n := int(c.n.Add(1))
	c.mu.Lock()
	c.ids = append(c.ids, d.ID)
	c.mu.Unlock()
	if c.seen != nil {
		select {
		case c.seen <- struct{}{}:
		default:
		}
	}
	if c.fn != nil {
		return c.fn(n, d)
	}
	return nil
}

func (c *calls) count() int64 { return c.n.Load() }

// delivery builds a delivery a caller could have built: it carries no
// settlement right, which is all the tests that do not exercise settlement
// need.
func delivery(body string) *mq.Delivery {
	return &mq.Delivery{Message: mq.Message{Topic: "orders", Body: []byte(body)}}
}

// delivery2 is delivery with an application identifier.
func delivery2(body, id string) *mq.Delivery {
	d := delivery(body)
	d.ID = id
	return d
}

// fakeClock is a clock a test drives by hand.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 9, 13, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// NewTimer returns a timer that fires immediately: nothing in these tests
// waits on wall time, and a retry's wait is measured by counting calls rather
// than by elapsed time.
func (c *fakeClock) NewTimer(time.Duration) mq.Timer { return firedTimer{} }

// firedTimer is a timer that has already fired.
type firedTimer struct{}

func (firedTimer) C() <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- time.Time{}
	return ch
}

func (firedTimer) Stop() bool               { return false }
func (firedTimer) Reset(time.Duration) bool { return false }

var _ mq.Clock = (*fakeClock)(nil)
