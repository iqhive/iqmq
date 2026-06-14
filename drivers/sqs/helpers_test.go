package sqs_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	sq "bitbucket.org/iqhive/iqmq/drivers/sqs"
	"bitbucket.org/iqhive/iqmq/drivers/sqs/sqsfake"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// The test harness: a driver over the in-process server of
// [bitbucket.org/iqhive/iqmq/drivers/sqs/sqsfake], on the server's own
// clock.
//
// Sharing the clock is what makes the visibility scenarios deterministic. The
// server decides when a window has passed and the driver's local lease check
// decides the same thing, so a test that advanced one and not the other would
// be asserting on a disagreement it created itself.

// harness is one server, one backend and one client.
type harness struct {
	t      *testing.T
	server *sqsfake.Server
	back   *sq.Backend
	client *mq.Client
	cfg    sq.Config
}

// clock is the server's clock as an mq.Clock.
//
// NewTimer is a real timer, and only the time-based and hybrid settlement
// policies ask for one: a fake timer here would need the server's clock to
// drive the settler's ticker, and the two tests that need a ticker use a real
// interval of a few milliseconds instead.
type clock struct{ s *sqsfake.Server }

func (c clock) Now() time.Time { return c.s.Now() }

func (c clock) NewTimer(d time.Duration) mq.Timer { return &realTimer{t: time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// open builds a harness, applying opts to the configuration.
func open(t *testing.T, opts ...func(*sq.Config)) *harness {
	t.Helper()
	return openOn(t, sqsfake.New(sqsfake.Options{StaleHandleDeletes: true}), opts...)
}

// openOn builds a harness over a server the caller already has, which is what
// a test driving the server's faults needs.
func openOn(t *testing.T, server *sqsfake.Server, opts ...func(*sq.Config)) *harness {
	t.Helper()
	cfg := sq.Config{
		Region:          "fake-region-1",
		AccessKeyID:     "AKIAFAKE",
		SecretAccessKey: "not-a-real-secret",
		AutoCreate:      true,
		WaitTime:        time.Second,
		AckDeadline:     30 * time.Second,
		Clock:           clock{s: server},
	}
	for _, fn := range opts {
		fn(&cfg)
	}
	back, err := sq.BackendWithAPI(t.Context(), server, server, driverkit.ClientBorrowed, cfg)
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	h := &harness{t: t, server: server, back: back, cfg: cfg}
	h.client = mq.NewClient(back, mq.WithClock(cfg.Clock))
	t.Cleanup(func() {
		if err := h.client.Close(); err != nil {
			t.Errorf("closing the client: %v", err)
		}
	})
	return h
}

// topic is a queue-name-safe topic for one test.
func topic(t *testing.T, purpose string) string {
	t.Helper()
	return "t-" + purpose + "-" + strconv.FormatUint(topicSeq.Add(1), 36)
}

var topicSeq atomic.Uint64

// publish publishes one message and fails the test if it is refused.
func (h *harness) publish(topic string, m *mq.Message, opts ...mq.PublishOption) {
	h.t.Helper()
	if err := h.client.Publish(h.t.Context(), topic, m, opts...); err != nil {
		h.t.Fatalf("publishing to %s: %v", topic, err)
	}
}

// consumer opens a consumer with the given prefetch.
func (h *harness) consumer(topic string, prefetch int) *mq.ConsumerHandle {
	h.t.Helper()
	c, err := h.client.Consumer(h.t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: "s", Prefetch: prefetch,
	})
	if err != nil {
		h.t.Fatalf("opening a consumer on %s: %v", topic, err)
	}
	h.t.Cleanup(func() {
		if err := c.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
			h.t.Errorf("closing the consumer: %v", err)
		}
	})
	return c
}

// receive takes exactly n deliveries, or fails.
func (h *harness) receive(c *mq.ConsumerHandle, n int, budget time.Duration) []mq.Delivery {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.t.Context(), budget)
	defer cancel()
	out := make([]mq.Delivery, n)
	got := 0
	for got < n {
		m, err := c.Receive(ctx, out[got:])
		got += m
		if err != nil {
			h.t.Fatalf("Receive: took %d of %d: %v", got, n, err)
		}
	}
	return out
}

// receiveOne takes one delivery, or fails.
func (h *harness) receiveOne(c *mq.ConsumerHandle, budget time.Duration) *mq.Delivery {
	h.t.Helper()
	ds := h.receive(c, 1, budget)
	return &ds[0]
}

// expectNothing asserts that nothing arrives within the budget.
func (h *harness) expectNothing(c *mq.ConsumerHandle, budget time.Duration, what string) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.t.Context(), budget)
	defer cancel()
	var dst [1]mq.Delivery
	n, err := c.Receive(ctx, dst[:])
	if n > 0 {
		h.t.Fatalf("received %q where there should be no %s", dst[0].Body, what)
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		h.t.Fatalf("Receive while expecting no %s: %v", what, err)
	}
}

// queue is the queue name a topic maps to.
func (h *harness) queue(topic string) string {
	h.t.Helper()
	name, err := h.back.QueueName(topic)
	if err != nil {
		h.t.Fatalf("QueueName(%q): %v", topic, err)
	}
	return name
}

// bodies is every message the topic's queue still holds.
func (h *harness) bodies(topic string) []string { return h.server.Bodies(h.queue(topic)) }

// depth is how many messages the topic's queue still holds.
func (h *harness) depth(topic string) int { return h.server.Depth(h.queue(topic)) }

// fault is an installed failure that fires a few times, which is what a
// transient service failure looks like: enough rounds to be observed and not
// so many that the test has to remove it.
func fault(op, queue string) sqsfake.Fault {
	return sqsfake.Fault{
		Op:    op,
		Queue: queue,
		Err:   errFaulted,
		Count: 3,
	}
}

// errFaulted is the failure an installed fault reports. It is not one of the
// service's typed errors, so it exercises the mapper's fallback rather than a
// rule.
var errFaulted = errors.New("sqsfake: the operation was faulted")

// contextWithTimeout is a bounded context derived from a test's own, for the
// op-script fuzz targets, whose steps must not block for long.
func contextWithTimeout(t *testing.T, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(t.Context(), d)
}

// dsnSecret is dsn.Secret, spelled here so the env-gated helpers do not each
// import the package for one conversion.
func dsnSecret(s string) dsn.Secret { return dsn.Secret(s) }

// waitFor polls until cond holds, or fails.
func waitFor(t *testing.T, budget time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", budget, what)
}

// invisible is how many of the topic's queue's messages a poller currently
// holds.
func (h *harness) invisible(topic string) int { return h.server.Invisible(h.queue(topic)) }

// stale is how many superseded deliveries this harness's sources have dropped.
func (h *harness) stale() uint64 {
	var total uint64
	for _, src := range sq.SourcesForTest(h.back) {
		if n, ok := sq.StaleForTest(src); ok {
			total += n
		}
	}
	return total
}
