package kafkago_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	mq "bitbucket.org/iqhive/iqmq"
	kq "bitbucket.org/iqhive/iqmq/drivers/kafka/kafkago"
)

// cluster starts a kfake cluster with the given topics, each with the given
// number of partitions, and stops it on cleanup.
func cluster(tb testing.TB, partitions int32, topics []string, opts ...kfake.Opt) *kfake.Cluster {
	tb.Helper()
	all := make([]kfake.Opt, 0, len(opts)+3)
	all = append(all,
		kfake.NumBrokers(1),
		// Six seconds is a real broker's own minimum, and a group that takes
		// six seconds to notice a dead member makes every rebalance test
		// slow. kfake accepts less.
		kfake.GroupMinSessionTimeout(time.Second),
	)
	if len(topics) > 0 {
		all = append(all, kfake.SeedTopics(partitions, topics...))
	}
	all = append(all, opts...)
	c, err := kfake.NewCluster(all...)
	if err != nil {
		tb.Fatalf("kfake cluster: %v", err)
	}
	tb.Cleanup(c.Close)
	return c
}

// config is the driver configuration the tests use against a kfake cluster.
func config(c *kfake.Cluster, mutate ...func(*kq.Config)) kq.Config {
	cfg := kq.Config{
		Brokers:  c.ListenAddrs(),
		ClientID: "iqmq-test",
		// A short commit interval keeps the at-least-once window small enough
		// that a test which reopens a subscription sees what the previous one
		// acknowledged, without any test having to sleep.
		CommitInterval:    50 * time.Millisecond,
		SessionTimeout:    2 * time.Second,
		RebalanceTimeout:  4 * time.Second,
		HeartbeatInterval: 300 * time.Millisecond,
		JoinBackoff:       200 * time.Millisecond,
		RevokeWait:        2 * time.Second,
		FetchMaxWait:      50 * time.Millisecond,
		RequestTimeout:    3 * time.Second,
		WriteTimeout:      2 * time.Second,
	}
	for _, m := range mutate {
		m(&cfg)
	}
	return cfg
}

// openOn opens a client against a running cluster, closed on cleanup.
func openOn(tb testing.TB, c *kfake.Cluster, mutate ...func(*kq.Config)) *mq.Client {
	tb.Helper()
	cl, err := kq.Open(tb.Context(), config(c, mutate...))
	if err != nil {
		tb.Fatalf("opening a client: %v", err)
	}
	tb.Cleanup(func() { _ = cl.Close() })
	return cl
}

// backendOn is openOn for a test that needs the driver's own surfaces.
func backendOn(tb testing.TB, c *kfake.Cluster, mutate ...func(*kq.Config)) *kq.Backend {
	tb.Helper()
	b, err := kq.OpenBackend(tb.Context(), config(c, mutate...))
	if err != nil {
		tb.Fatalf("opening a backend: %v", err)
	}
	tb.Cleanup(func() { _ = b.Close() })
	return b
}

// openPair opens the driver's backend and wraps it in a client, so a test can
// use the portable API and still read the driver's own counters and call
// [bitbucket.org/iqhive/iqmq/drivers/kafka/kafkago.Backend.Commit].
//
// mq.Client.Native() returns the NATIVE writer and not the backend, so there
// is no route from a client back to the driver's type. That is deliberate in
// the SPI and it means a driver test that needs both opens both, which is
// what kq.OpenBackend is for.
func openPair(tb testing.TB, c *kfake.Cluster, mutate ...func(*kq.Config)) (*mq.Client, *kq.Backend) {
	tb.Helper()
	b, err := kq.OpenBackend(tb.Context(), config(c, mutate...))
	if err != nil {
		tb.Fatalf("opening a backend: %v", err)
	}
	cl := mq.NewClient(b)
	tb.Cleanup(func() { _ = cl.Close() })
	return cl, b
}

// openClient starts a one-partition cluster with one topic and opens a client
// on it, returning both.
func openClient(tb testing.TB, mutate ...func(*kq.Config)) (*mq.Client, string) {
	tb.Helper()
	topic := newTopic("t")
	c := cluster(tb, 1, []string{topic})
	return openOn(tb, c, mutate...), topic
}

// topicSeq names a fresh topic per call, so nothing in one test reaches
// another even when they share a cluster.
var topicSeq atomic.Uint64

func newTopic(prefix string) string {
	return prefix + "-" + strconv.FormatUint(topicSeq.Add(1), 36)
}

// consumerOn opens a consumer, closed on cleanup.
func consumerOn(tb testing.TB, c *mq.Client, topic, sub string) *mq.ConsumerHandle {
	tb.Helper()
	h, err := c.Consumer(tb.Context(), mq.ConsumerConfig{Topic: topic, Subscription: sub})
	if err != nil {
		tb.Fatalf("opening a consumer on %s/%s: %v", topic, sub, err)
	}
	tb.Cleanup(func() { _ = h.Close() })
	return h
}

// consumerFrom opens a consumer at a start position, closed on cleanup.
func consumerFrom(tb testing.TB, c *mq.Client, topic, sub string, start mq.StartPosition) *mq.ConsumerHandle {
	tb.Helper()
	h, err := c.Consumer(tb.Context(), mq.ConsumerConfig{Topic: topic, Subscription: sub, Start: start})
	if err != nil {
		tb.Fatalf("opening a consumer on %s/%s: %v", topic, sub, err)
	}
	tb.Cleanup(func() { _ = h.Close() })
	return h
}

// receiveOne waits for one delivery, or fails.
func receiveOne(tb testing.TB, c *mq.ConsumerHandle, budget time.Duration) *mq.Delivery {
	tb.Helper()
	d, err := tryReceiveOne(tb, c, budget)
	if err != nil {
		tb.Fatalf("Receive: %v", err)
	}
	return d
}

// receiveBuf holds the delivery slice a receive fills, so that the Delivery
// the caller settles is the one in the buffer: the specification makes a COPY of a Delivery
// able to settle the original and unable to tell that something else already
// has, so a test that copied one out would be testing a usage the
// specification warns against.
type receiveBuf struct{ dst []mq.Delivery }

// tryReceiveOne waits for one delivery and reports what happened.
func tryReceiveOne(tb testing.TB, c *mq.ConsumerHandle, budget time.Duration) (*mq.Delivery, error) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), budget)
	defer cancel()
	buf := &receiveBuf{dst: make([]mq.Delivery, 1)}
	for {
		n, err := c.Receive(ctx, buf.dst)
		if n == 1 {
			return &buf.dst[0], nil
		}
		if err != nil {
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
}

// receiveN waits for n deliveries and calls fn on each where it lies in the
// receive buffer.
func receiveN(tb testing.TB, c *mq.ConsumerHandle, n int, budget time.Duration, fn func(d *mq.Delivery) error) int {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), budget)
	defer cancel()
	dst := make([]mq.Delivery, n)
	got := 0
	for got < n {
		m, err := c.Receive(ctx, dst[:n-got])
		for i := range m {
			if ferr := fn(&dst[i]); ferr != nil {
				tb.Fatalf("delivery %d: %v", got+i, ferr)
			}
		}
		got += m
		if err != nil {
			return got
		}
	}
	return got
}

// expectNothing fails when a delivery arrives within budget.
func expectNothing(tb testing.TB, c *mq.ConsumerHandle, budget time.Duration, what string) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), budget)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, _ := c.Receive(ctx, dst)
	if n > 0 {
		tb.Fatalf("%s: a delivery arrived when none should have (body %q)", what, dst[0].Body)
	}
}

// publish publishes one message through the client's own per-topic publisher.
func publish(tb testing.TB, c *mq.Client, topic string, m *mq.Message) {
	tb.Helper()
	if err := c.Publish(tb.Context(), topic, m); err != nil {
		tb.Fatalf("publishing to %s: %v", topic, err)
	}
}

// waitFor polls until cond holds, or fails.
func waitFor(tb testing.TB, budget time.Duration, what string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out after %v waiting for %s", budget, what)
}
