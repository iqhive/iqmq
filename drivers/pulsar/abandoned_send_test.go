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

// TestAnAbandonedSendKeepsItsMessage is the pooled scratch message a Send
// that gave up on its context still leaves in the client's queue. The sink
// used to return it to the pool at once, so it was zeroed and then reused by
// the next publication while the client's event loop could still read it: the
// first message went out with the second one's key and properties, and the
// two goroutines raced on it.
func TestAnAbandonedSendKeepsItsMessage(t *testing.T) {
	b, cl := newAbandoningRig(t)
	snk := sinkOf(t, b, "retained", mq.MetadataNativeStrict)
	prod := cl.producer(t)

	first := &mq.Message{Key: []byte("first-key"), Body: []byte("first")}
	first.Headers.Set("which", []byte("first"))
	prod.abandonNext(context.DeadlineExceeded)
	if err := snk.Publish(context.Background(), first, nil); err == nil {
		t.Fatal("the publication succeeded although its Send gave up")
	}
	prod.mu.Lock()
	held := prod.held
	prod.mu.Unlock()
	if held == nil {
		t.Fatal("the abandoned Send kept no message")
	}

	for range 8 {
		second := &mq.Message{Key: []byte("second-key"), Body: []byte("second")}
		second.Headers.Set("which", []byte("second"))
		if err := snk.Publish(context.Background(), second, nil); err != nil {
			t.Fatalf("publishing the next message: %v", err)
		}
	}
	if held.Key != "first-key" || string(held.Payload) != "first" || held.Properties["which"] != "first" {
		t.Fatalf("the message the client still holds now reads key %q, payload %q, which=%q; want the first "+
			"publication's: the sink recycled it while the client could still send it",
			held.Key, held.Payload, held.Properties["which"])
	}
}

// TestAnAbandonedSendDoesNotKeepTheCallersBody is the caller's side of the
// abandoned Send. Publish has returned, so the caller may reuse its Body, but
// the client still holds the request and reads its payload when the event loop
// gets to it. The sink used to hand the client the caller's Body itself, so the
// message the client could still write carried whatever the caller put in the
// buffer next.
func TestAnAbandonedSendDoesNotKeepTheCallersBody(t *testing.T) {
	b, cl := newAbandoningRig(t)
	snk := sinkOf(t, b, "reused", mq.MetadataNativeStrict)
	prod := cl.producer(t)
	// A context that can end, because a real Send gives up on nothing else.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	body := []byte("first")
	prod.abandonNext(context.DeadlineExceeded)
	if err := snk.Publish(ctx, &mq.Message{Body: body}, nil); err == nil {
		t.Fatal("the publication succeeded although its Send gave up")
	}
	copy(body, "XXXXX")
	prod.mu.Lock()
	held := prod.held
	prod.mu.Unlock()
	if held == nil {
		t.Fatal("the abandoned Send kept no message")
	}
	if string(held.Payload) != "first" {
		t.Fatalf("the message the client still holds now reads payload %q, want %q: the sink left the "+
			"client reading the caller's Body after Publish had returned", held.Payload, "first")
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
