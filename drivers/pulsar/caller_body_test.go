package pulsar_test

import (
	"context"
	"sync"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// deferringClient is the in-process model with pulsar-client-go's SendAsync
// timing. The library queues the request and returns, and the producer's event
// loop reads the message's payload later, when it adds the message to a batch
// (internalSendAsync, dataChan, internalSend, batchBuilder.Add). The model
// copies the payload inside the call, which is what hid a sink that handed the
// client the caller's own Body.
type deferringClient struct {
	pulsarapi.Client
	mu    sync.Mutex
	prods []*deferringProducer
}

func (c *deferringClient) CreateProducer(o pulsar.ProducerOptions) (pulsar.Producer, error) {
	p, err := c.Client.CreateProducer(o)
	if err != nil {
		return nil, err
	}
	dp := &deferringProducer{Producer: p}
	c.mu.Lock()
	c.prods = append(c.prods, dp)
	c.mu.Unlock()
	return dp, nil
}

// producer is the one producer the test's single topic opened.
func (c *deferringClient) producer(t *testing.T) *deferringProducer {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.prods) != 1 {
		t.Fatalf("%d producers were opened, want 1", len(c.prods))
	}
	return c.prods[0]
}

// deferringProducer holds every SendAsync until drain, the way the library's
// send queue holds a request until its event loop takes it.
type deferringProducer struct {
	pulsar.Producer
	mu     sync.Mutex
	queued []func()
}

func (p *deferringProducer) SendAsync(ctx context.Context, m *pulsar.ProducerMessage,
	cb func(pulsar.MessageID, *pulsar.ProducerMessage, error),
) {
	p.mu.Lock()
	p.queued = append(p.queued, func() { p.Producer.SendAsync(ctx, m, cb) })
	p.mu.Unlock()
}

// drain is the event loop getting round to what was queued.
func (p *deferringProducer) drain() {
	p.mu.Lock()
	queued := p.queued
	p.queued = nil
	p.mu.Unlock()
	for _, send := range queued {
		send()
	}
}

// newDeferringRig is newRig over a deferringClient.
func newDeferringRig(t *testing.T) (*pd.Backend, *deferringClient) {
	t.Helper()
	server := pulsarfake.New()
	cl := &deferringClient{Client: server.Client()}
	b, err := pd.BackendWithClient(context.Background(), cl, driverkit.ClientOwned, rigConfig(server))
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, cl
}

// TestAConfirmNonePublishDoesNotKeepTheCallersBody is a caller that reuses its
// buffer once Publish has returned, which the Sink contract allows. Under
// ConfirmNone the sink returns as soon as SendAsync has, and it used to hand
// the client the caller's Body as the payload, so the broker got whatever the
// caller wrote into the buffer next.
func TestAConfirmNonePublishDoesNotKeepTheCallersBody(t *testing.T) {
	b, cl := newDeferringRig(t)
	src := sourceOf(t, b, "reused", "workers", mq.MetadataNativeStrict, 4)
	snk, err := b.OpenSink(context.Background(), mq.SinkConfig{
		Topic: "reused", Metadata: mq.MetadataNativeStrict, Confirmation: mq.ConfirmNone,
	})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = snk.Close() })
	prod := cl.producer(t)
	// Registered after the sink's own cleanup so that it runs first: Close
	// waits for the callbacks of what it admitted, and a queued send has
	// none until it is drained.
	t.Cleanup(prod.drain)

	body := []byte("first")
	if err := snk.Publish(context.Background(), &mq.Message{Body: body}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	copy(body, "XXXXX")
	prod.drain()
	if got := string(fetchOne(t, src).Message.Body); got != "first" {
		t.Fatalf("the published body is %q, want %q: the sink left the client reading the caller's Body "+
			"after Publish had returned", got, "first")
	}
}
