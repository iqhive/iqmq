package pulsar_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// closingClient is the in-process model with producers the client can close
// for good. pulsar-client-go v0.21 does that when a reconnection is refused
// with TopicNotFound, TopicTerminated or ProducerFenced (reconnectToBroker,
// doClose): it fails the message still waiting for its receipt with that
// reason and every later Send and Flush with ProducerClosed, and it never
// reconnects that producer again.
type closingClient struct {
	pulsarapi.Client
	mu     sync.Mutex
	prods  []*closingProducer
	refuse error
	hold   chan struct{}
	calls  int
}

func (c *closingClient) CreateProducer(o pulsar.ProducerOptions) (pulsar.Producer, error) {
	c.mu.Lock()
	c.calls++
	refuse, hold := c.refuse, c.hold
	c.mu.Unlock()
	if hold != nil {
		<-hold
	}
	if refuse != nil {
		return nil, refuse
	}
	p, err := c.Client.CreateProducer(o)
	if err != nil {
		return nil, err
	}
	cp := &closingProducer{Producer: p}
	c.mu.Lock()
	c.prods = append(c.prods, cp)
	c.mu.Unlock()
	return cp, nil
}

// closeNewest closes the newest producer the way the client does when a
// reconnection is refused with reason.
func (c *closingClient) closeNewest(t *testing.T, reason error) *closingProducer {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.prods) == 0 {
		t.Fatal("no producer has been opened")
	}
	p := c.prods[len(c.prods)-1]
	p.closeForGood(reason)
	return p
}

// refuseWith makes CreateProducer answer err, or succeed again when err is nil.
func (c *closingClient) refuseWith(err error) {
	c.mu.Lock()
	c.refuse = err
	c.mu.Unlock()
}

// holdCreation makes CreateProducer wait until the returned function is called.
func (c *closingClient) holdCreation() (release func()) {
	ch := make(chan struct{})
	c.mu.Lock()
	c.hold = ch
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			c.hold = nil
			c.mu.Unlock()
			close(ch)
		})
	}
}

// created is how many times CreateProducer was called.
func (c *closingClient) created() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// closingProducer is a producer the client can close underneath its users.
type closingProducer struct {
	pulsar.Producer
	mu      sync.Mutex
	gone    bool
	pending error
	closed  bool
}

// closeForGood fails the next publication with reason, as the one waiting for
// its receipt, and every one after it with ProducerClosed.
func (p *closingProducer) closeForGood(reason error) {
	p.mu.Lock()
	p.gone, p.pending = true, reason
	p.mu.Unlock()
}

// refusal is what a publication on a closed producer gets, or nil while the
// producer is open.
func (p *closingProducer) refusal() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.gone {
		return nil
	}
	if r := p.pending; r != nil {
		p.pending = nil
		return errors.Join(r, errors.New("the broker refused the reconnection"))
	}
	return pulsar.ErrProducerClosed
}

// isClosed reports whether the driver closed this producer.
func (p *closingProducer) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

func (p *closingProducer) Send(ctx context.Context, m *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	if err := p.refusal(); err != nil {
		return nil, err
	}
	return p.Producer.Send(ctx, m)
}

func (p *closingProducer) SendAsync(ctx context.Context, m *pulsar.ProducerMessage, cb func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	if err := p.refusal(); err != nil {
		cb(nil, m, err)
		return
	}
	p.Producer.SendAsync(ctx, m, cb)
}

func (p *closingProducer) FlushWithCtx(ctx context.Context) error {
	if err := p.refusal(); err != nil {
		return err
	}
	return p.Producer.FlushWithCtx(ctx)
}

func (p *closingProducer) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.Producer.Close()
}

// newClosingRig is newRig over a closingClient.
func newClosingRig(t *testing.T) (*pd.Backend, *closingClient) {
	t.Helper()
	server := pulsarfake.New()
	cl := &closingClient{Client: server.Client()}
	b, err := pd.BackendWithClient(context.Background(), cl, driverkit.ClientOwned, rigConfig(server))
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, cl
}

// TestASinkOpenedAfterTheClientClosedTheProducerPublishes is a publisher
// opened on a topic whose cached producer the client has closed for good,
// while another sink still holds it. The backend used to hand the new sink
// that same producer, so every one of its publications failed with
// ProducerClosed for as long as the first sink stayed open.
func TestASinkOpenedAfterTheClientClosedTheProducerPublishes(t *testing.T) {
	b, cl := newClosingRig(t)
	src := sourceOf(t, b, "deleted", "workers", mq.MetadataNativeStrict, 4)
	first := sinkOf(t, b, "deleted", mq.MetadataNativeStrict)
	cl.closeNewest(t, pulsar.ErrTopicNotfound)
	if err := first.Publish(context.Background(), &mq.Message{Body: []byte("lost")}, nil); err == nil {
		t.Fatal("a publication on a producer the client had closed succeeded")
	}

	second := sinkOf(t, b, "deleted", mq.MetadataNativeStrict)
	if err := second.Publish(context.Background(), &mq.Message{Body: []byte("fresh")}, nil); err != nil {
		t.Fatalf("a sink opened after the client closed the topic's producer reported %v (code %v): "+
			"the backend handed it the producer the client had closed", err, mq.CodeOf(err))
	}
	if got := string(fetchOne(t, src).Message.Body); got != "fresh" {
		t.Fatalf("the consumer received %q, want %q", got, "fresh")
	}
}

// TestASinkWhoseProducerTheClientClosedPublishesThroughAFreshOne is the sink
// that was already open. Its producer is never reconnected once the client has
// closed it, so the sink used to fail every later publication, on every
// publication surface, until somebody reopened it.
//
// The publication the close fails is the client's to report, and it stays
// ambiguous: a message failed with the close's reason was waiting for its
// receipt. Under ConfirmNone and PublishOwned the caller hears nothing of it,
// so what holds there is that only the next message reaches the consumer. The
// next one goes through a fresh producer, and the closed one is released.
func TestASinkWhoseProducerTheClientClosedPublishesThroughAFreshOne(t *testing.T) {
	surfaces := []struct {
		name    string
		confirm mq.ConfirmationMode
		reports bool
		publish func(mq.Sink, *mq.Message) error
	}{
		{"Publish", mq.ConfirmAccepted, true, func(s mq.Sink, m *mq.Message) error {
			return s.Publish(context.Background(), m, nil)
		}},
		{"Publish under ConfirmNone", mq.ConfirmNone, false, func(s mq.Sink, m *mq.Message) error {
			return s.Publish(context.Background(), m, nil)
		}},
		{"PublishAsync", mq.ConfirmAccepted, true, func(s mq.Sink, m *mq.Message) error {
			done := make(chan error, 1)
			if err := s.(mq.AsyncSink).PublishAsync(context.Background(), m, nil, func(err error) { done <- err }); err != nil {
				return err
			}
			return <-done
		}},
		{"PublishOwned", mq.ConfirmAccepted, false, func(s mq.Sink, m *mq.Message) error {
			released := make(chan struct{})
			err := s.(mq.OwnedSink).PublishOwned(context.Background(),
				mq.OwnedMessage{Message: m, Release: func(*mq.Message) { close(released) }}, nil)
			<-released
			return err
		}},
		{"PublishBatch", mq.ConfirmAccepted, true, func(s mq.Sink, m *mq.Message) error {
			return s.(mq.BatchSink).PublishBatch(context.Background(), []*mq.Message{m}, nil)
		}},
		{"Flush", mq.ConfirmAccepted, true, func(s mq.Sink, m *mq.Message) error {
			if err := s.(mq.FlushSink).Flush(context.Background()); err != nil {
				return err
			}
			return s.Publish(context.Background(), m, nil)
		}},
	}
	for _, surface := range surfaces {
		t.Run(surface.name, func(t *testing.T) {
			b, cl := newClosingRig(t)
			src := sourceOf(t, b, "closed", "workers", mq.MetadataNativeStrict, 4)
			snk, err := b.OpenSink(context.Background(), mq.SinkConfig{
				Topic: "closed", Metadata: mq.MetadataNativeStrict, Confirmation: surface.confirm,
			})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			t.Cleanup(func() { _ = snk.Close() })
			closed := cl.closeNewest(t, pulsar.ErrProducerClosed)
			if err := surface.publish(snk, &mq.Message{Body: []byte("lost")}); err == nil && surface.reports {
				t.Fatal("a publication on a producer the client had closed succeeded")
			}
			if err := surface.publish(snk, &mq.Message{Body: []byte("fresh")}); err != nil {
				t.Fatalf("the publication after the client closed the producer reported %v (code %v): "+
					"the sink is still publishing through the closed one", err, mq.CodeOf(err))
			}
			if got := string(fetchOne(t, src).Message.Body); got != "fresh" {
				t.Fatalf("the consumer received %q, want %q", got, "fresh")
			}
			waitFor(t, "the release of the producer the client closed", closed.isClosed)
		})
	}
}

// TestEveryReasonTheClientClosesAProducerForIsSeen is the close's own reason.
// The message failed with it was waiting for its receipt, so it stays
// ambiguous, and it is already the sign that the producer will never
// reconnect: the next publication goes through a fresh one rather than
// failing again with ProducerClosed.
func TestEveryReasonTheClientClosesAProducerForIsSeen(t *testing.T) {
	for _, reason := range []error{
		pulsar.ErrTopicNotfound, pulsar.ErrTopicTerminated, pulsar.ErrProducerFenced, pulsar.ErrProducerClosed,
	} {
		b, cl := newClosingRig(t)
		snk := sinkOf(t, b, "reason", mq.MetadataNativeStrict)
		cl.closeNewest(t, reason)
		err := snk.Publish(context.Background(), &mq.Message{Body: []byte("pending")}, nil)
		if err == nil || !mq.IsAmbiguous(err) {
			t.Errorf("%v: the publication the close failed reported %v (ambiguous %v), want an ambiguous failure",
				reason, err, mq.IsAmbiguous(err))
		}
		if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("next")}, nil); err != nil {
			t.Errorf("%v: the publication after it reported %v (code %v), want it published through a fresh producer",
				reason, err, mq.CodeOf(err))
		}
	}
}

// TestAProducerTheBrokerWillNotRecreateIsRefusedDefinitely is a topic that
// refuses the fresh producer too: a terminated topic, here. The publication
// gets the creation's own answer, which is definite because nothing was sent,
// and so does a sink opened on the topic; once the topic takes producers
// again, the next publication goes through.
func TestAProducerTheBrokerWillNotRecreateIsRefusedDefinitely(t *testing.T) {
	b, cl := newClosingRig(t)
	src := sourceOf(t, b, "terminated", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "terminated", mq.MetadataNativeStrict)
	cl.closeNewest(t, pulsar.ErrTopicTerminated)
	cl.refuseWith(pulsar.ErrTopicTerminated)
	_ = snk.Publish(context.Background(), &mq.Message{Body: []byte("pending")}, nil)

	err := snk.Publish(context.Background(), &mq.Message{Body: []byte("refused")}, nil)
	if mq.CodeOf(err) != mq.CodeConflict || mq.IsAmbiguous(err) {
		t.Fatalf("a publication whose fresh producer the topic refused reported %v (code %v, ambiguous %v), "+
			"want the definite Conflict a terminated topic answers", err, mq.CodeOf(err), mq.IsAmbiguous(err))
	}
	if _, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "terminated", Metadata: mq.MetadataNativeStrict}); mq.CodeOf(err) != mq.CodeConflict {
		t.Fatalf("a sink opened on the terminated topic reported %v (code %v), want Conflict", err, mq.CodeOf(err))
	}

	cl.refuseWith(nil)
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("back")}, nil); err != nil {
		t.Fatalf("the publication once the topic took producers again reported %v", err)
	}
	if got := string(fetchOne(t, src).Message.Body); got != "back" {
		t.Fatalf("the consumer received %q, want %q", got, "back")
	}
}

// TestAFreshProducerIsCreatedOnceAndHonoursEachContext is the wait for the
// fresh producer. pulsar-client-go's CreateProducer takes no context, so a
// publication that waited on it directly would outlive its own deadline by up
// to OperationTimeout; it gives up on its context instead, definitely, since
// nothing was sent. The publications waiting at once share one creation.
func TestAFreshProducerIsCreatedOnceAndHonoursEachContext(t *testing.T) {
	b, cl := newClosingRig(t)
	snk := sinkOf(t, b, "slow", mq.MetadataNativeStrict)
	cl.closeNewest(t, pulsar.ErrProducerClosed)
	_ = snk.Publish(context.Background(), &mq.Message{Body: []byte("lost")}, nil)
	release := cl.holdCreation()
	t.Cleanup(release)
	before := cl.created()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	out := make(chan error, 1)
	go func() { out <- snk.Publish(ctx, &mq.Message{Body: []byte("impatient")}, nil) }()
	select {
	case err := <-out:
		if !errors.Is(err, context.DeadlineExceeded) || mq.IsAmbiguous(err) {
			t.Fatalf("a publication whose context ended while the producer was created reported %v "+
				"(ambiguous %v), want a definite deadline", err, mq.IsAmbiguous(err))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a publication waited out the producer's creation past its own context")
	}

	const n = 4
	results := make(chan error, n)
	for range n {
		go func() { results <- snk.Publish(context.Background(), &mq.Message{Body: []byte("patient")}, nil) }()
	}
	// Long enough for each to reach the wait; one that has not yet is
	// handed the fresh producer and creates nothing, so the count below
	// cannot fail for a slow start.
	time.Sleep(50 * time.Millisecond)
	release()
	for range n {
		if err := <-results; err != nil {
			t.Fatalf("a publication that waited for the fresh producer reported %v", err)
		}
	}
	if got := cl.created() - before; got != 1 {
		t.Fatalf("the publications waiting for a fresh producer created %d, want 1", got)
	}
}
