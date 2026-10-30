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
	// creating is how many CreateProducer calls have not returned, and
	// closedCreating is how many there were when the client was closed.
	creating       int
	closedCreating int
	// gate holds the next CreateProducer only, and entered is closed once
	// that call is waiting at it.
	gate    chan struct{}
	entered chan struct{}
}

func (c *closingClient) CreateProducer(o pulsar.ProducerOptions) (pulsar.Producer, error) {
	c.mu.Lock()
	c.calls++
	c.creating++
	refuse, hold := c.refuse, c.hold
	gate, entered := c.gate, c.entered
	c.gate, c.entered = nil, nil
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.creating--
		c.mu.Unlock()
	}()
	if gate != nil {
		close(entered)
		<-gate
	}
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

// holdNextCreation makes the next CreateProducer, and only that one, wait until
// the returned function is called, which is harmless to call twice. The
// channel is closed once that call is waiting.
func (c *closingClient) holdNextCreation() (<-chan struct{}, func()) {
	gate, entered := make(chan struct{}), make(chan struct{})
	c.mu.Lock()
	c.gate, c.entered = gate, entered
	c.mu.Unlock()
	return entered, sync.OnceFunc(func() { close(gate) })
}

// created is how many times CreateProducer was called.
func (c *closingClient) created() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// Close closes the client, recording how many producer creations were still
// under way on it.
func (c *closingClient) Close() {
	c.mu.Lock()
	c.closedCreating = c.creating
	c.mu.Unlock()
	c.Client.Close()
}

// creatingAtClose is how many producer creations were under way when the
// client was closed.
func (c *closingClient) creatingAtClose() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closedCreating
}

// newest is the producer CreateProducer made last.
func (c *closingClient) newest(t *testing.T) *closingProducer {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.prods) == 0 {
		t.Fatal("no producer has been opened")
	}
	return c.prods[len(c.prods)-1]
}

// closingProducer is a producer the client can close underneath its users.
type closingProducer struct {
	pulsar.Producer
	mu      sync.Mutex
	gone    bool
	pending error
	closed  bool
	// holding arms holdNextSend, written is closed once the send is held,
	// and held is that send: written, and waiting for its receipt.
	holding bool
	written chan struct{}
	held    *heldSend
}

// heldSend is one send that is waiting for its receipt. A synchronous one
// waits for cb too.
type heldSend struct {
	ctx context.Context
	m   *pulsar.ProducerMessage
	cb  func(pulsar.MessageID, *pulsar.ProducerMessage, error)
}

// holdNextSend makes the next send, synchronous or asynchronous, wait for its
// receipt until the returned function is called, which is harmless to call
// twice. The channel is closed once that send is held. Closing the producer
// first fails the send with ProducerClosed, which is what pulsar-client-go v0.21
// does with every message a closing producer still holds (doClose,
// failPendingMessages). A held synchronous send also ends with its context,
// and with SendTimeout disabled nothing else ends it.
func (p *closingProducer) holdNextSend() (<-chan struct{}, func()) {
	written := make(chan struct{})
	p.mu.Lock()
	p.holding, p.written = true, written
	p.mu.Unlock()
	return written, func() {
		p.mu.Lock()
		h := p.held
		p.held = nil
		p.mu.Unlock()
		if h != nil {
			p.Producer.SendAsync(h.ctx, h.m, h.cb)
		}
	}
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
	p.mu.Lock()
	if p.holding {
		type receipt struct {
			id  pulsar.MessageID
			err error
		}
		out := make(chan receipt, 1)
		p.holding = false
		p.held = &heldSend{ctx: ctx, m: m, cb: func(id pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
			out <- receipt{id, err}
		}}
		close(p.written)
		p.mu.Unlock()
		select {
		case r := <-out:
			return r.id, r.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	p.mu.Unlock()
	return p.Producer.Send(ctx, m)
}

func (p *closingProducer) SendAsync(ctx context.Context, m *pulsar.ProducerMessage, cb func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	if err := p.refusal(); err != nil {
		cb(nil, m, err)
		return
	}
	p.mu.Lock()
	if p.holding {
		p.holding = false
		p.held = &heldSend{ctx: ctx, m: m, cb: cb}
		close(p.written)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
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
	h := p.held
	p.held = nil
	p.mu.Unlock()
	if h != nil {
		h.cb(nil, h.m, pulsar.ErrProducerClosed)
	}
	p.Producer.Close()
}

// newClosingRig is newRig over a closingClient.
func newClosingRig(t *testing.T) (*pd.Backend, *closingClient) {
	t.Helper()
	return newClosingRigWith(t, nil)
}

// newClosingRigWith is newClosingRig with a configuration mutate changes.
func newClosingRigWith(t *testing.T, mutate func(*pd.Config)) (*pd.Backend, *closingClient) {
	t.Helper()
	server := pulsarfake.New()
	cl := &closingClient{Client: server.Client()}
	cfg := rigConfig(server)
	if mutate != nil {
		mutate(&cfg)
	}
	b, err := pd.BackendWithClient(context.Background(), cl, driverkit.ClientOwned, cfg)
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

// TestBackendCloseWaitsForAProducerReplacement is a backend closed while the
// replacement of a producer the client closed for good is still being
// created.
//
// The replacement runs on a goroutine of its own, because CreateProducer takes
// no context, and nothing waited for it: Close returned while it was still
// inside CreateProducer, and the client this backend owns was closed under it.
// A creation that then failed counted a degradation against a closed backend,
// and one that succeeded registered a producer with the broker after Close had
// returned.
//
// The first wait is a window in which a Close that does not wait for the
// replacement returns. A slow runner can only make that check miss the
// defect, never report it falsely, and the client's record still catches it.
func TestBackendCloseWaitsForAProducerReplacement(t *testing.T) {
	b, cl := newClosingRig(t)
	snk := sinkOf(t, b, "slow", mq.MetadataNativeStrict)
	cl.closeNewest(t, pulsar.ErrProducerClosed)
	_ = snk.Publish(context.Background(), &mq.Message{Body: []byte("lost")}, nil)
	release := cl.holdCreation()
	t.Cleanup(release)
	before := cl.created()

	// A publication starts the replacement and gives up on its context
	// while the replacement is inside CreateProducer.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan error, 1)
	go func() { out <- snk.Publish(ctx, &mq.Message{Body: []byte("impatient")}, nil) }()
	waitFor(t, "the replacement to start creating its producer", func() bool { return cl.created() > before })
	cancel()
	if err := <-out; !errors.Is(err, context.Canceled) {
		t.Fatalf("the publication that gave up on its context reported %v, want context.Canceled", err)
	}

	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	var (
		err      error
		returned bool
	)
	select {
	case err = <-closed:
		returned = true
		t.Errorf("Backend.Close returned while its producer replacement was still inside CreateProducer")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	if !returned {
		select {
		case err = <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("Close never returned after the replacement's creation ended")
		}
	}
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := cl.creatingAtClose(); n != 0 {
		t.Fatalf("the backend closed the client it owns with %d producer creation(s) still under way on it", n)
	}
	if p := cl.newest(t); !p.isClosed() {
		t.Fatal("the producer the replacement created for a closed backend was left open")
	}
}

// TestAReplacementLeavesAnotherSinksSendInFlightAlone is two sinks on one
// topic. The client has closed their shared producer for good on one
// partition, while the first sink's publication on a healthy partition is
// still waiting for its receipt.
//
// The replacement closed the old producer as soon as the new one existed, and
// pulsar-client-go fails every message a closing producer still holds. So the
// first sink's publication failed ambiguously although its partition was
// healthy, and a caller that retried it published it twice.
//
// The first wait is a window in which a replacement that does not wait for the
// send fails it. A slow runner can only make that check miss the defect, never
// report it falsely.
func TestAReplacementLeavesAnotherSinksSendInFlightAlone(t *testing.T) {
	b, cl := newClosingRig(t)
	src := sourceOf(t, b, "parts", "workers", mq.MetadataNativeStrict, 4)
	first := sinkOf(t, b, "parts", mq.MetadataNativeStrict)
	second := sinkOf(t, b, "parts", mq.MetadataNativeStrict)
	old := cl.newest(t)

	written, receipt := old.holdNextSend()
	t.Cleanup(receipt)
	inFlight := make(chan error, 1)
	err := first.(mq.AsyncSink).PublishAsync(context.Background(), &mq.Message{Body: []byte("in flight")}, nil,
		func(err error) { inFlight <- err })
	if err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	select {
	case <-written:
	case <-time.After(5 * time.Second):
		t.Fatal("the first sink's publication never reached the producer")
	}

	old.closeForGood(pulsar.ErrProducerClosed)
	_ = second.Publish(context.Background(), &mq.Message{Body: []byte("lost")}, nil)
	if err := second.Publish(context.Background(), &mq.Message{Body: []byte("fresh")}, nil); err != nil {
		t.Fatalf("the publication through the fresh producer reported %v", err)
	}
	select {
	case err := <-inFlight:
		t.Fatalf("the first sink's publication, waiting for its receipt on a healthy partition, ended with %v "+
			"(ambiguous %v) before the receipt came: the replacement closed the producer it was in flight on",
			err, mq.IsAmbiguous(err))
	case <-time.After(100 * time.Millisecond):
	}
	receipt()
	if err := <-inFlight; err != nil {
		t.Fatalf("the first sink's publication reported %v once its receipt came, want nil", err)
	}
	waitFor(t, "the release of the replaced producer", old.isClosed)
	got := map[string]bool{}
	for _, d := range fetchN(t, src, 2) {
		got[string(d.Message.Body)] = true
	}
	if !got["in flight"] || !got["fresh"] {
		t.Fatalf("the consumer received %v, want the publication that was in flight and the fresh one", got)
	}
}

// TestBackendCloseEndsASyncSendOnAReplacedProducer is a backend closed while a
// synchronous publication is still waiting for its receipt on a producer that
// a replacement has taken out of use, with SendTimeout disabled.
//
// The replacement closes that producer only once the sends in flight on it
// have finished, and Close waited for the replacement. Nothing ended the send:
// Close closed only the cached producers, and with SendTimeout disabled the
// caller's context is the publication's only bound, so Close never returned.
// Close now closes the replaced producer with the cached ones, which fails the
// send as ambiguous.
func TestBackendCloseEndsASyncSendOnAReplacedProducer(t *testing.T) {
	b, cl := newClosingRigWith(t, func(c *pd.Config) { c.SendTimeout = -1 })
	first := sinkOf(t, b, "parts", mq.MetadataNativeStrict)
	second := sinkOf(t, b, "parts", mq.MetadataNativeStrict)
	old := cl.newest(t)

	written, receipt := old.holdNextSend()
	t.Cleanup(receipt)
	published := make(chan error, 1)
	go func() {
		published <- first.Publish(context.Background(), &mq.Message{Body: []byte("in flight")}, nil)
	}()
	select {
	case <-written:
	case <-time.After(5 * time.Second):
		t.Fatal("the first sink's publication never reached the producer")
	}
	old.closeForGood(pulsar.ErrProducerClosed)
	_ = second.Publish(context.Background(), &mq.Message{Body: []byte("lost")}, nil)
	if err := second.Publish(context.Background(), &mq.Message{Body: []byte("fresh")}, nil); err != nil {
		t.Fatalf("the publication through the fresh producer reported %v", err)
	}

	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		receipt()
		err := <-closed
		t.Fatalf("Backend.Close did not return (%v once the receipt came) while a synchronous publication "+
			"waited for its receipt on the producer a replacement had retired", err)
	}
	if err := <-published; err == nil || !mq.IsAmbiguous(err) {
		t.Fatalf("the publication Close ended reported %v (ambiguous %v), want an ambiguous failure",
			err, mq.IsAmbiguous(err))
	}
	if !old.isClosed() {
		t.Fatal("Close left the replaced producer open")
	}
}

// TestASinkThatLostTheProducerRaceOpensWithoutWaitingForOtherSends is a sink
// whose open lost the race to create a topic's producer, to a sink whose
// producer the client then closed for good while that sink had a publication
// in flight on a healthy partition.
//
// The losing open made its own producer the replacement and retired the
// winner's itself, so it waited for the other sink's send to finish, past its
// own context, and out of Close's sight. It now leaves the replacement to the
// one every publication uses, and returns once the fresh producer exists.
func TestASinkThatLostTheProducerRaceOpensWithoutWaitingForOtherSends(t *testing.T) {
	b, cl := newClosingRig(t)
	src := sourceOf(t, b, "parts", "workers", mq.MetadataNativeStrict, 4)
	entered, letGo := cl.holdNextCreation()
	t.Cleanup(letGo)
	type opened struct {
		s   mq.Sink
		err error
	}
	late := make(chan opened, 1)
	go func() {
		s, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "parts", Metadata: mq.MetadataNativeStrict})
		late <- opened{s, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the losing open never started creating its producer")
	}
	first := sinkOf(t, b, "parts", mq.MetadataNativeStrict)
	old := cl.newest(t)

	written, receipt := old.holdNextSend()
	t.Cleanup(receipt)
	inFlight := make(chan error, 1)
	err := first.(mq.AsyncSink).PublishAsync(context.Background(), &mq.Message{Body: []byte("in flight")}, nil,
		func(err error) { inFlight <- err })
	if err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	select {
	case <-written:
	case <-time.After(5 * time.Second):
		t.Fatal("the first sink's publication never reached the producer")
	}
	old.closeForGood(pulsar.ErrProducerClosed)
	if err := first.Publish(context.Background(), &mq.Message{Body: []byte("lost")}, nil); err == nil {
		t.Fatal("a publication on a producer the client had closed succeeded")
	}
	letGo()

	var o opened
	select {
	case o = <-late:
	case <-time.After(5 * time.Second):
		receipt()
		<-late
		t.Fatal("the losing open waited for the other sink's publication to finish")
	}
	if o.err != nil {
		t.Fatalf("the losing open reported %v", o.err)
	}
	t.Cleanup(func() { _ = o.s.Close() })
	select {
	case err := <-inFlight:
		t.Fatalf("the first sink's publication on a healthy partition ended with %v before its receipt", err)
	default:
	}
	receipt()
	if err := <-inFlight; err != nil {
		t.Fatalf("the first sink's publication reported %v once its receipt came, want nil", err)
	}
	waitFor(t, "the release of the replaced producer", old.isClosed)
	if err := o.s.Publish(context.Background(), &mq.Message{Body: []byte("fresh")}, nil); err != nil {
		t.Fatalf("the losing sink's publication reported %v", err)
	}
	got := map[string]bool{}
	for _, d := range fetchN(t, src, 2) {
		got[string(d.Message.Body)] = true
	}
	if !got["in flight"] || !got["fresh"] {
		t.Fatalf("the consumer received %v, want the publication that was in flight and the fresh one", got)
	}
}

// TestAReplacementThatFailsDuringCloseLeavesHealthAlone is a producer
// replacement whose creation fails, as the broker being unreachable, after the
// backend has begun closing.
//
// The failure counted as a degradation of a backend that was closing, so
// DegradedTransitions went up after Close had started, for a producer nobody
// would use.
func TestAReplacementThatFailsDuringCloseLeavesHealthAlone(t *testing.T) {
	b, cl := newClosingRig(t)
	snk := sinkOf(t, b, "unreachable", mq.MetadataNativeStrict)
	old := cl.closeNewest(t, pulsar.ErrProducerClosed)
	_ = snk.Publish(context.Background(), &mq.Message{Body: []byte("lost")}, nil)
	release := cl.holdCreation()
	t.Cleanup(release)
	cl.refuseWith(unreachable{})
	before := cl.created()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan error, 1)
	go func() { out <- snk.Publish(ctx, &mq.Message{Body: []byte("impatient")}, nil) }()
	waitFor(t, "the replacement to start creating its producer", func() bool { return cl.created() > before })
	cancel()
	if err := <-out; !errors.Is(err, context.Canceled) {
		t.Fatalf("the publication that gave up on its context reported %v, want context.Canceled", err)
	}
	degraded := b.DegradedTransitions()

	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	// Close takes the cached producer out of use only once the backend is
	// closing, so a closed producer is a backend that is closing.
	waitFor(t, "Close to close the cached producer", old.isClosed)
	release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close never returned after the replacement's creation failed")
	}
	if got := b.DegradedTransitions(); got != degraded {
		t.Fatalf("a replacement that failed while the backend was closing took DegradedTransitions from %d to %d",
			degraded, got)
	}
}

// unreachable is the answer of a broker that cannot be reached, with the
// result code pulsar-client-go gives it.
type unreachable struct{}

func (unreachable) Error() string         { return "the broker cannot be reached" }
func (unreachable) Result() pulsar.Result { return pulsar.ConnectError }
