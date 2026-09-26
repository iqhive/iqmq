package nsq_test

import (
	"context"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	nq "github.com/iqhive/iqmq/drivers/nsq"
	"github.com/iqhive/iqmq/drivers/nsq/nsqfake"
)

// rig is a driver wired to the in-process model.
//
// It exists because every interesting question on this broker is one the
// broker answers: an expired per-message timeout, a redelivery to a second
// consumer, a delayed requeue, a settlement for a message somebody else now
// holds. The model's clock is the driver's clock, so none of them costs a
// second of wall time.
type rig struct {
	t      *testing.T
	server *nsqfake.Server
	client *mq.Client
	cfg    nq.Config
}

// newRig opens the driver against a fresh model.
func newRig(t *testing.T, opts ...func(*nq.Config)) *rig {
	t.Helper()
	server := nsqfake.New()
	cfg := nq.Config{
		Addrs:       []string{"nsqfake:4150"},
		AckDeadline: 30 * time.Second,
		LeaseGuard:  time.Second,
		Clock:       server,
	}
	for _, o := range opts {
		o(&cfg)
	}
	client, err := nq.OpenWithClient(context.Background(), server.Producer(), driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return &rig{t: t, server: server, client: client, cfg: cfg}
}

// backend opens the driver against a fresh model and returns the backend, for
// the tests that drive the SPI rather than the client.
func newBackendRig(t *testing.T, opts ...func(*nq.Config)) (*nq.Backend, *nsqfake.Server) {
	t.Helper()
	server := nsqfake.New()
	cfg := nq.Config{
		Addrs:       []string{"nsqfake:4150"},
		AckDeadline: 30 * time.Second,
		LeaseGuard:  time.Second,
		Clock:       server,
	}
	for _, o := range opts {
		o(&cfg)
	}
	b, err := nq.BackendWithClient(context.Background(), server.Producer(), driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return b, server
}

// sourceOf opens one subscription on a backend wired to the model.
func sourceOf(t *testing.T, b *nq.Backend, topic, sub string, mode mq.MetadataMode, prefetch int) mq.Source {
	t.Helper()
	src, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic:        topic,
		Subscription: sub,
		Prefetch:     prefetch,
		Metadata:     mode,
		AckDeadline:  30 * time.Second,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	return src
}

// sinkOf opens one publication channel on a backend wired to the model.
func sinkOf(t *testing.T, b *nq.Backend, topic string, mode mq.MetadataMode) mq.Sink {
	t.Helper()
	snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: topic, Metadata: mode})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = snk.Close() })
	return snk
}

// fetchOne fetches exactly one delivery, or fails.
func fetchOne(t *testing.T, src mq.Source) mq.DriverDelivery {
	t.Helper()
	ds := fetchN(t, src, 1)
	return ds[0]
}

// fetchN fetches n deliveries, retrying the short returns a Fetch is allowed.
func fetchN(t *testing.T, src mq.Source, n int) []mq.DriverDelivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out := make([]mq.DriverDelivery, 0, n)
	buf := make([]mq.DriverDelivery, n)
	for len(out) < n {
		got, err := src.Fetch(ctx, buf[:n-len(out)])
		if err != nil {
			t.Fatalf("Fetch after %d of %d: %v", len(out), n, err)
		}
		for i := range got {
			// The core copies the struct out of the fetch buffer before
			// reusing it, and a test that did not would settle whatever
			// the next Fetch wrote there.
			d := buf[i]
			m := *d.Message
			d.Message = &m
			out = append(out, d)
		}
	}
	return out
}
