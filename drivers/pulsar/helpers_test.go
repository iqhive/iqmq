package pulsar_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// rigConfig is the configuration every rig starts from: the in-process model's
// clock, a lease short enough for a test to step past, and no administrative
// endpoint, because the model has none.
func rigConfig(server *pulsarfake.Server) pd.Config {
	return pd.Config{
		ServiceURL:          "pulsar://pulsarfake:6650",
		AckTimeout:          10 * time.Second,
		LeaseGuard:          time.Second,
		NackRedeliveryDelay: 500 * time.Millisecond,
		Clock:               server,
	}
}

// newRig builds a backend over the in-process model.
func newRig(t *testing.T, mutate func(*pd.Config), opts ...pulsarfake.Option) (*pd.Backend, *pulsarfake.Server) {
	t.Helper()
	server := pulsarfake.New(opts...)
	cfg := rigConfig(server)
	if mutate != nil {
		mutate(&cfg)
	}
	b, err := pd.BackendWithClient(context.Background(), server.Client(), driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, server
}

// sinkOf opens a sink on a topic.
func sinkOf(t *testing.T, b *pd.Backend, topic string, meta mq.MetadataMode) mq.Sink {
	t.Helper()
	s, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: topic, Metadata: meta})
	if err != nil {
		t.Fatalf("OpenSink(%q): %v", topic, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// sourceOf opens a source on a topic and subscription.
func sourceOf(t *testing.T, b *pd.Backend, topic, sub string, meta mq.MetadataMode, prefetch int) mq.Source {
	t.Helper()
	s, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: topic, Subscription: sub, Prefetch: prefetch, Metadata: meta,
		Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource(%q/%q): %v", topic, sub, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// publish sends one message through a sink.
func publish(t *testing.T, snk mq.Sink, m *mq.Message) {
	t.Helper()
	if err := snk.Publish(context.Background(), m, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// fetchN fetches n deliveries, failing the test if they do not arrive.
func fetchN(t *testing.T, src mq.Source, n int) []mq.DriverDelivery {
	t.Helper()
	out := make([]mq.DriverDelivery, 0, n)
	dst := make([]mq.DriverDelivery, n)
	deadline := time.Now().Add(5 * time.Second)
	for len(out) < n {
		if time.Now().After(deadline) {
			t.Fatalf("fetched %d of %d deliveries before the deadline", len(out), n)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		got, err := src.Fetch(ctx, dst[:n-len(out)])
		cancel()
		if got == 0 && err != nil && !isTimeout(err) {
			t.Fatalf("Fetch: %v", err)
		}
		out = append(out, dst[:got]...)
	}
	return out
}

// fetchOne fetches exactly one delivery.
func fetchOne(t *testing.T, src mq.Source) mq.DriverDelivery {
	t.Helper()
	return fetchN(t, src, 1)[0]
}

// fetchNothing fails when a delivery arrives within the budget.
func fetchNothing(t *testing.T, src mq.Source, what string) {
	t.Helper()
	dst := make([]mq.DriverDelivery, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if n, _ := src.Fetch(ctx, dst); n > 0 {
		t.Fatalf("%s: a delivery arrived carrying %q when none should have", what, dst[0].Message.Body)
	}
}

// waitFor polls until a condition holds, which is what a test needs between
// moving the model's clock and reading what the DRIVER did with it.
//
// The driver's lease sweeper is a goroutine: Server.Advance fires its timer and
// returns, and the sweeper then wakes up and writes to the broker. A test that
// advanced twice in a row without waiting would race its own second step past
// the sweeper's first, which is what three of these tests did before this
// existed.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// keyThatMoves is a key whose partition changes when a topic's partition count
// goes from one number to another.
//
// It is SEARCHED for rather than hard-coded, because a hard-coded key would
// make the partition-expansion test an assertion about Java's String.hashCode
// rather than about the driver.
func keyThatMoves(t *testing.T, from, to int) string {
	t.Helper()
	for i := range 2048 {
		key := "k-" + strconv.Itoa(i)
		if pulsarfake.PartitionOf(key, from) != pulsarfake.PartitionOf(key, to) {
			return key
		}
	}
	t.Fatalf("no key of 2048 changes partition between %d and %d partitions, which cannot be", from, to)
	return ""
}

// keysInDifferentRanges is two keys that a Key_Shared subscription with n
// consumers gives to two DIFFERENT consumers.
func keysInDifferentRanges(t *testing.T, consumers int) (string, string) {
	t.Helper()
	first := ""
	for i := range 2048 {
		key := "key-" + strconv.Itoa(i)
		owner := pulsarfake.RangeOf(key) % consumers
		if first == "" {
			if owner == 0 {
				first = key
			}
			continue
		}
		if owner != 0 {
			return first, key
		}
	}
	t.Fatalf("no two keys of 2048 land in hash ranges owned by different consumers of %d", consumers)
	return "", ""
}

// isTimeout reports whether err is a deadline this helper set itself.
func isTimeout(err error) bool {
	return mq.CodeOf(err) == mq.CodeDeadlineExceeded || mq.CodeOf(err) == mq.CodeCanceled ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// fetchAdvancing fetches one delivery, moving the fake clock by step between
// attempts.
//
// A test that holds a fake clock cannot "wait" for something the server will
// only do once time passes: waiting IS advancing. A single Advance before a
// fetch assumes the message is already queued, and when it is not — because
// the driver released an in-process hold a moment ago and the frame has not
// landed — the fetch waits out its budget at a fake time that never moves
// again.
func fetchAdvancing(t *testing.T, src mq.Source, server *pulsarfake.Server, step time.Duration) (mq.DriverDelivery, bool) {
	t.Helper()
	dst := make([]mq.DriverDelivery, 1)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		server.Advance(step)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		got, err := src.Fetch(ctx, dst)
		cancel()
		if got == 0 && err != nil && !isTimeout(err) {
			t.Fatalf("Fetch: %v", err)
		}
		if got > 0 {
			return dst[0], true
		}
	}
	return mq.DriverDelivery{}, false
}
