package snssqs_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	sns "github.com/iqhive/iqmq/drivers/snssqs"
	"github.com/iqhive/iqmq/drivers/snssqs/internal/snsfake"
	"github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// The test harness: the fan-out driver over an in-process SNS server that
// fans out into the in-process SQS server `drivers/sqs` was tested against,
// on the SQS server's own clock.
//
// Sharing the clock is what makes the visibility scenarios deterministic: the
// SQS server decides when a window has passed and the driver's local lease
// check decides the same thing, so a test that advanced one and not the other
// would be asserting on a disagreement it created itself.
//
// The SNS server has no clock of its own, and it does not need one: nothing
// SNS does in this driver is timed. The delay a caller might ask for is
// refused, the retries are the service's own and are not modelled, and the
// only window in the whole path is the queue's.

// harness is two servers, one backend and one client.
type harness struct {
	t      *testing.T
	queues *sqsfake.Server
	sns    *snsfake.Server
	back   *sns.Backend
	client *mq.Client
	cfg    sns.Config
}

// clock is the SQS server's clock as an mq.Clock.
type clock struct{ s *sqsfake.Server }

func (c clock) Now() time.Time { return c.s.Now() }

func (c clock) NewTimer(d time.Duration) mq.Timer { return &realTimer{t: time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// open builds a harness whose subscriptions have raw message delivery, which
// is every correctly configured deployment.
func open(t *testing.T, opts ...func(*sns.Config)) *harness {
	t.Helper()
	queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	server := snsfake.New(snsfake.Options{
		Region: "fake-region-1", AccountID: "000000000000", Queues: queues,
	})
	return openOn(t, server, queues, opts...)
}

// openOn builds a harness over servers the caller already has, which is what a
// test driving a fault or a deliberately misconfigured subscription needs.
func openOn(t *testing.T, server *snsfake.Server, queues *sqsfake.Server, opts ...func(*sns.Config)) *harness {
	t.Helper()
	cfg := sns.Config{
		Region:          "fake-region-1",
		AccountID:       "000000000000",
		AccessKeyID:     "AKIAFAKE",
		SecretAccessKey: "not-a-real-secret",
		AutoCreate:      true,
		NoQueuePolicy:   false,
		WaitTime:        time.Second,
		AckDeadline:     30 * time.Second,
		Clock:           clock{s: queues},
	}
	for _, fn := range opts {
		fn(&cfg)
	}
	back, err := sns.BackendWithAPI(t.Context(), server, queues, queues, server, driverkit.ClientBorrowed, cfg)
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	h := &harness{t: t, queues: queues, sns: server, back: back, cfg: cfg}
	h.client = mq.NewClient(back, mq.WithClock(cfg.Clock))
	t.Cleanup(func() {
		if err := h.client.Close(); err != nil {
			t.Errorf("closing the client: %v", err)
		}
	})
	return h
}

// topic is a topic name safe for both name grammars, for one test.
func topic(t *testing.T, purpose string) string {
	t.Helper()
	return "t-" + purpose + "-" + strconv.FormatUint(topicSeq.Add(1), 36)
}

var topicSeq atomic.Uint64

// provision creates the topic and each subscription's queue and subscription,
// which is what a fan-out has to do before anything is published: SNS
// delivers to the subscriptions that exist when a message is published, so a
// publication to an unsubscribed topic is accepted and delivered nowhere.
func (h *harness) provision(topic string, subs ...string) {
	h.t.Helper()
	admin, err := h.client.Admin()
	if err != nil {
		h.t.Fatalf("Admin: %v", err)
	}
	if err := admin.EnsureTopic(h.t.Context(), mq.TopicSpec{Name: topic}); err != nil {
		h.t.Fatalf("EnsureTopic(%q): %v", topic, err)
	}
	for _, sub := range subs {
		if err := admin.EnsureSubscription(h.t.Context(), mq.SubscriptionSpec{Topic: topic, Name: sub}); err != nil {
			h.t.Fatalf("EnsureSubscription(%q, %q): %v", topic, sub, err)
		}
	}
}

// publish publishes one message and fails the test if it is refused.
func (h *harness) publish(topic string, m *mq.Message, opts ...mq.PublishOption) {
	h.t.Helper()
	if err := h.client.Publish(h.t.Context(), topic, m, opts...); err != nil {
		h.t.Fatalf("publishing to %s: %v", topic, err)
	}
}

// consumer opens a consumer on one subscription.
func (h *harness) consumer(topic, sub string, prefetch int) *mq.ConsumerHandle {
	h.t.Helper()
	c, err := h.client.Consumer(h.t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: sub, Prefetch: prefetch,
	})
	if err != nil {
		h.t.Fatalf("opening a consumer on %s/%s: %v", topic, sub, err)
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

// queue is the queue name a (topic, subscription) pair maps to.
func (h *harness) queue(topic, sub string) string {
	h.t.Helper()
	name, err := sns.QueueName(topic, sub, h.cfg.TopicPrefix, h.cfg.QueuePrefix)
	if err != nil {
		h.t.Fatalf("QueueName(%q, %q): %v", topic, sub, err)
	}
	return name
}

// depth is how many messages one subscription's queue still holds.
func (h *harness) depth(topic, sub string) int { return h.queues.Depth(h.queue(topic, sub)) }

// bodies is every message one subscription's queue still holds.
func (h *harness) bodies(topic, sub string) []string { return h.queues.Bodies(h.queue(topic, sub)) }

// errFaulted is the failure an installed fault reports. It is not one of the
// services' typed errors, so it exercises the mapper's fallback rather than a
// rule.
var errFaulted = errors.New("snsfake: the operation was faulted")

// waitFor polls until cond holds, or fails.
func waitFor(t *testing.T, budget time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", budget, what)
}

// ownBorrowed is driverkit.ClientBorrowed, spelled here so that the test files
// do not each import driverkit for one constant.
func ownBorrowed() driverkit.Ownership { return driverkit.ClientBorrowed }

// topicName is the SNS topic name a portable topic maps to, which is what the
// SNS server knows a topic by.
func (h *harness) topicName(topic string) string {
	h.t.Helper()
	name, err := sns.TopicName(topic, h.cfg.TopicPrefix)
	if err != nil {
		h.t.Fatalf("TopicName(%q): %v", topic, err)
	}
	return name
}

// queueCapabilities is the SQS driver's standard-profile table, for the test
// that holds the two tables to each other.
//
// It is built through that driver's own public constructor rather than copied,
// so a change to either table is a failure rather than a stale expectation.
func queueCapabilities(t *testing.T) mq.Capabilities {
	t.Helper()
	queues := sqsfake.New(sqsfake.Options{})
	back, err := sqs.BackendWithAPI(t.Context(), queues, queues, driverkit.ClientBorrowed, sqs.Config{
		Region: "fake-region-1", AutoCreate: true, WaitTime: time.Second,
	})
	if err != nil {
		t.Fatalf("building the queue driver's backend: %v", err)
	}
	t.Cleanup(func() {
		if err := back.Close(); err != nil {
			t.Errorf("closing the queue driver's backend: %v", err)
		}
	})
	return back.Capabilities()
}
