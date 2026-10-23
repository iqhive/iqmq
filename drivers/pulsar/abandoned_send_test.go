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

// abandoningClient is the in-process model with one change to its producers:
// a Send can be told to give up the way pulsar-client-go's does when the
// caller's context ends while the request is still queued. The library returns
// ctx.Err() at once and keeps the *ProducerMessage in its send queue, where the
// event loop reads it later and may well write it to the broker.
type abandoningClient struct {
	pulsarapi.Client
	mu    sync.Mutex
	prods []*abandoningProducer
}

func (c *abandoningClient) CreateProducer(o pulsar.ProducerOptions) (pulsar.Producer, error) {
	p, err := c.Client.CreateProducer(o)
	if err != nil {
		return nil, err
	}
	ap := &abandoningProducer{Producer: p}
	c.mu.Lock()
	c.prods = append(c.prods, ap)
	c.mu.Unlock()
	return ap, nil
}

// producer is the one producer the test's single topic opened.
func (c *abandoningClient) producer(t *testing.T) *abandoningProducer {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.prods) != 1 {
		t.Fatalf("%d producers were opened, want 1", len(c.prods))
	}
	return c.prods[0]
}

// abandoningProducer gives up on the next Send when abandon is set, and keeps
// the message it was handed, as the library's queue does.
type abandoningProducer struct {
	pulsar.Producer
	mu      sync.Mutex
	abandon error
	held    *pulsar.ProducerMessage
}

// abandonNext makes the next Send return err while keeping its message.
func (p *abandoningProducer) abandonNext(err error) {
	p.mu.Lock()
	p.abandon = err
	p.mu.Unlock()
}

func (p *abandoningProducer) Send(ctx context.Context, m *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	p.mu.Lock()
	err := p.abandon
	p.abandon = nil
	if err != nil {
		p.held = m
	}
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return p.Producer.Send(ctx, m)
}

// newAbandoningRig is newRig over an abandoningClient.
func newAbandoningRig(t *testing.T) (*pd.Backend, *abandoningClient) {
	t.Helper()
	server := pulsarfake.New()
	cl := &abandoningClient{Client: server.Client()}
	b, err := pd.BackendWithClient(context.Background(), cl, driverkit.ClientOwned, rigConfig(server))
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, cl
}

// TestASendAbandonedOnItsContextIsAmbiguous is the synchronous publication
// whose context ended while the request was queued in the client. Send returns
// the bare context error, which carries no Pulsar result code, and the sink
// used to classify that as a refusal from before the send: a definite,
// retryable deadline, although the client may still write the message.
func TestASendAbandonedOnItsContextIsAmbiguous(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled} {
		b, cl := newAbandoningRig(t)
		snk := sinkOf(t, b, "abandoned", mq.MetadataNativeStrict)
		cl.producer(t).abandonNext(cause)
		err := snk.Publish(context.Background(), &mq.Message{Body: []byte("maybe")}, nil)
		if err == nil {
			t.Fatalf("%v: the publication succeeded although its Send gave up", cause)
		}
		if !mq.IsAmbiguous(err) || mq.IsRetryable(err) {
			t.Errorf("a Send abandoned on %v reported %v (ambiguous %v, retryable %v), want ambiguous and not retryable",
				cause, err, mq.IsAmbiguous(err), mq.IsRetryable(err))
		}
	}
}

// TestAPublishOnADoneContextIsDefinite is the other side: a context that had
// ended before Send was called means nothing reached the client.
func TestAPublishOnADoneContextIsDefinite(t *testing.T) {
	b, _ := newAbandoningRig(t)
	snk := sinkOf(t, b, "done", mq.MetadataNativeStrict)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := snk.Publish(ctx, &mq.Message{Body: []byte("never")}, nil)
	if err == nil {
		t.Fatal("a publication on a cancelled context succeeded")
	}
	if mq.IsAmbiguous(err) {
		t.Fatalf("a publication whose context ended before the send reported %v, which is ambiguous", err)
	}
}
