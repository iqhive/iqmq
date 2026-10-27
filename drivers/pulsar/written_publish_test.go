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

// writtenClient is the in-process model with pulsar-client-go's answer for a
// message it has already written when the producer goes away: the message
// waits in the producer's pending queue for its receipt, and the producer
// fails it with the reason it closed (failPendingMessages, from doClose on
// Close and when a reconnection is refused with TopicTerminated or
// ProducerFenced, and on its own for ProducerBlockedQuotaExceededException).
type writtenClient struct {
	pulsarapi.Client
	fail  error
	mu    sync.Mutex
	prods []*writtenProducer
}

func (c *writtenClient) CreateProducer(o pulsar.ProducerOptions) (pulsar.Producer, error) {
	p, err := c.Client.CreateProducer(o)
	if err != nil {
		return nil, err
	}
	wp := &writtenProducer{Producer: p, fail: c.fail, written: make(chan struct{}), closed: make(chan struct{})}
	c.mu.Lock()
	c.prods = append(c.prods, wp)
	c.mu.Unlock()
	return wp, nil
}

// producer is the one producer the test's single topic opened.
func (c *writtenClient) producer(t *testing.T) *writtenProducer {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.prods) != 1 {
		t.Fatalf("%d producers were opened, want 1", len(c.prods))
	}
	return c.prods[0]
}

// writtenProducer stores the message, as the broker does once it is on the
// wire, and then waits for a receipt that its Close turns into fail.
type writtenProducer struct {
	pulsar.Producer
	fail    error
	written chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (p *writtenProducer) Send(ctx context.Context, m *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	if _, err := p.Producer.Send(ctx, m); err != nil {
		return nil, err
	}
	close(p.written)
	<-p.closed
	return nil, p.fail
}

func (p *writtenProducer) Close() {
	p.once.Do(func() { close(p.closed) })
	p.Producer.Close()
}

// TestAWrittenPublishFailedByTheProducerIsAmbiguous is a confirmed Publish
// whose message the broker stored and whose receipt never came, because the
// producer closed first. The client fails such a message with the reason it
// closed, and the sink used to read those reasons as refusals made before
// anything was written: the caller was told the message was not published, and
// a retry duplicated it.
func TestAWrittenPublishFailedByTheProducerIsAmbiguous(t *testing.T) {
	for _, fail := range []error{
		pulsar.ErrProducerClosed, pulsar.ErrTopicTerminated, pulsar.ErrProducerFenced,
		pulsar.ErrProducerBlockedQuotaExceeded,
	} {
		server := pulsarfake.New()
		cl := &writtenClient{Client: server.Client(), fail: fail}
		b, err := pd.BackendWithClient(context.Background(), cl, driverkit.ClientOwned, rigConfig(server))
		if err != nil {
			t.Fatalf("BackendWithClient: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		src := sourceOf(t, b, "closing", "workers", mq.MetadataNativeStrict, 4)
		snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "closing", Metadata: mq.MetadataNativeStrict})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		prod := cl.producer(t)
		res := make(chan error, 1)
		go func() { res <- snk.Publish(context.Background(), &mq.Message{Body: []byte("on the wire")}, nil) }()
		<-prod.written
		_ = snk.Close()
		perr := <-res
		if got := string(fetchOne(t, src).Message.Body); got != "on the wire" {
			t.Fatalf("%v: the broker holds %q", fail, got)
		}
		if perr == nil || !mq.IsAmbiguous(perr) {
			t.Errorf("%v: the Publish of a message the broker stored reported %v (code %v, ambiguous %v), "+
				"want an ambiguous failure", fail, perr, mq.CodeOf(perr), mq.IsAmbiguous(perr))
		}
	}
}
