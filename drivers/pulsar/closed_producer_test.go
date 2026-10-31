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
	// holding arms holdNextSend and batching arms batchNextSend; written is
	// closed once the armed send is taken. held is a send that was written
	// and waits for its receipt; batched is one still in the producer's
	// batch, which tick flushes once delay has passed; dropped is a batched
	// send Close dropped with its callback.
	holding  bool
	batching bool
	delay    time.Duration
	written  chan struct{}
	held     *heldSend
	batched  *heldSend
	tick     *time.Timer
	dropped  *heldSend
	// parking arms parkNextSend: the armed send parks inside SendAsync until
	// unpark is closed, and parked is closed once it has. onFlush and
	// onClose are closed by the producer's first flush and its close after
	// that. late counts sends that reached the queue after Close.
	parking bool
	unpark  chan struct{}
	parked  chan struct{}
	onFlush chan struct{}
	onClose chan struct{}
	late    int
}

// parkEvents is what parkNextSend reports: the send has parked, the producer
// has been flushed, the producer has been closed; and the function that lets
// the send go on, which is harmless to call twice.
type parkEvents struct {
	parked, flushed, closed <-chan struct{}
	release                 func()
}

// parkNextSend makes the next SendAsync park on its way into the producer,
// after the producer's closed check and before its queue, until released.
//
// It is a publication pulsar-client-go v0.21 has not queued yet when Close
// runs: descheduled inside SendAsync, or, under BlockIfQueueFull, waiting in
// reserveSemaphore for room in a full queue. Released, it reaches the queue:
// if the producer has closed by then, v0.21 sends on the closed queue and
// panics, which this records in late and models by dropping the callback;
// otherwise it is in the batch, which a flush, or delay, sends.
func (p *closingProducer) parkNextSend(delay time.Duration) parkEvents {
	unpark, parked := make(chan struct{}), make(chan struct{})
	flushed, closed := make(chan struct{}), make(chan struct{})
	p.mu.Lock()
	p.parking, p.delay, p.unpark, p.parked = true, delay, unpark, parked
	p.onFlush, p.onClose = flushed, closed
	p.mu.Unlock()
	return parkEvents{
		parked: parked, flushed: flushed, closed: closed,
		release: sync.OnceFunc(func() { close(unpark) }),
	}
}

// park parks the send parkNextSend armed, reporting whether there was one.
func (p *closingProducer) park() bool {
	p.mu.Lock()
	if !p.parking {
		p.mu.Unlock()
		return false
	}
	p.parking = false
	unpark := p.unpark
	close(p.parked)
	p.mu.Unlock()
	<-unpark
	return true
}

// arrive puts a send that was parked into the producer's queue, or drops it
// with its callback when the producer has closed meanwhile: see parkNextSend.
func (p *closingProducer) arrive(ctx context.Context, m *pulsar.ProducerMessage, cb func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := &heldSend{ctx: ctx, m: m, cb: cb, answered: make(chan struct{})}
	if p.closed {
		p.late++
		p.dropped = h
		return
	}
	p.batched = h
	p.tick = time.AfterFunc(p.delay, p.flushBatch)
}

// lateSends is how many sends reached the queue after the producer closed.
func (p *closingProducer) lateSends() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.late
}

// signal closes *ch once, if it is armed.
func (p *closingProducer) signal(ch *chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if *ch != nil {
		close(*ch)
		*ch = nil
	}
}

// heldSend is one send the test has taken out of the normal path. A
// synchronous one waits for cb too. answered is closed once a held send has
// its answer: its receipt, or the failure Close gives it.
type heldSend struct {
	ctx      context.Context
	m        *pulsar.ProducerMessage
	cb       func(pulsar.MessageID, *pulsar.ProducerMessage, error)
	answered chan struct{}
}

// holdNextSend makes the next send, synchronous or asynchronous, wait for its
// receipt until the returned function is called, which is harmless to call
// twice. The channel is closed once that send is held. Closing the producer
// first fails the send with ProducerClosed, which is what pulsar-client-go v0.21
// does with every message a closing producer still holds (doClose,
// failPendingMessages). A held synchronous send also ends with its context,
// and with SendTimeout disabled nothing else ends it. A flush waits for the
// held send's receipt, or for its own context, as v0.21's FlushWithCtx waits
// for the last pending message.
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
			close(h.answered)
		}
	}
}

// batchNextSend makes the next send, synchronous or asynchronous, sit in the
// producer's batch until the batch is flushed, by a flush or once delay has
// passed, after which the broker answers it at once. The channel is closed
// once that send is in the batch.
//
// It is pulsar-client-go v0.21's batch container: a batched message's
// callback stays there until the batch is flushed (batch_builder.go), and the
// producer's Close neither flushes the batch nor fails what is in it (doClose
// fails the pending queue alone), so a message still batched when the
// producer closes never gets its callback, and a synchronous Send of it waits
// for its context alone.
func (p *closingProducer) batchNextSend(delay time.Duration) <-chan struct{} {
	written := make(chan struct{})
	p.mu.Lock()
	p.batching, p.delay, p.written = true, delay, written
	p.mu.Unlock()
	return written
}

// intercept takes the send holdNextSend or batchNextSend armed, reporting
// whether there was one.
func (p *closingProducer) intercept(ctx context.Context, m *pulsar.ProducerMessage, cb func(pulsar.MessageID, *pulsar.ProducerMessage, error)) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := &heldSend{ctx: ctx, m: m, cb: cb, answered: make(chan struct{})}
	switch {
	case p.holding:
		p.holding, p.held = false, h
	case p.batching:
		p.batching, p.batched = false, h
		p.tick = time.AfterFunc(p.delay, p.flushBatch)
	default:
		return false
	}
	close(p.written)
	return true
}

// flushBatch sends what the batch holds, which the broker answers at once.
func (p *closingProducer) flushBatch() {
	p.mu.Lock()
	h := p.batched
	p.batched = nil
	if p.tick != nil {
		p.tick.Stop()
	}
	p.mu.Unlock()
	if h != nil {
		p.Producer.SendAsync(h.ctx, h.m, h.cb)
	}
}

// rescue runs the callback of a batched send Close dropped, with
// ProducerClosed, which no real producer does: it is how a test that found
// the callback lost ends what was waiting for it.
func (p *closingProducer) rescue() {
	p.mu.Lock()
	h := p.dropped
	p.dropped = nil
	p.mu.Unlock()
	if h != nil {
		h.cb(nil, h.m, pulsar.ErrProducerClosed)
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
	type receipt struct {
		id  pulsar.MessageID
		err error
	}
	out := make(chan receipt, 1)
	if !p.intercept(ctx, m, func(id pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
		out <- receipt{id, err}
	}) {
		return p.Producer.Send(ctx, m)
	}
	select {
	case r := <-out:
		return r.id, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *closingProducer) SendAsync(ctx context.Context, m *pulsar.ProducerMessage, cb func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	if err := p.refusal(); err != nil {
		cb(nil, m, err)
		return
	}
	if p.park() {
		p.arrive(ctx, m, cb)
		return
	}
	if p.intercept(ctx, m, cb) {
		return
	}
	p.Producer.SendAsync(ctx, m, cb)
}

func (p *closingProducer) FlushWithCtx(ctx context.Context) error {
	p.signal(&p.onFlush)
	if err := p.refusal(); err != nil {
		return err
	}
	p.flushBatch()
	p.mu.Lock()
	h := p.held
	p.mu.Unlock()
	if h != nil {
		select {
		case <-h.answered:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return p.Producer.FlushWithCtx(ctx)
}

func (p *closingProducer) Close() {
	p.mu.Lock()
	p.closed = true
	h := p.held
	p.held = nil
	if p.tick != nil {
		p.tick.Stop()
	}
	if p.batched != nil {
		p.dropped, p.batched = p.batched, nil
	}
	p.mu.Unlock()
	p.signal(&p.onClose)
	if h != nil {
		h.cb(nil, h.m, pulsar.ErrProducerClosed)
		close(h.answered)
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

// closeBound is the OperationTimeout the Close tests below give their
// backend, which bounds the flush Close makes before it closes a producer.
const closeBound = 200 * time.Millisecond

// TestBackendCloseEndsAnAsyncSendInFlight is the asynchronous half of
// TestBackendCloseEndsASyncSendOnAReplacedProducer: a PublishAsync waiting for
// its receipt when Close runs, with SendTimeout disabled, on the cached
// producer and on one a replacement retired.
//
// teardown closed the sinks first, and a sink's close waits for the callbacks
// of the asynchronous publications it admitted, which only the producer's
// Close fails, and that came after the wait. So Close waited for the receipt,
// for ever with SendTimeout disabled and up to SendTimeout otherwise, where a
// synchronous send in the same place was failed at once. Close now flushes,
// for at most OperationTimeout, closes the producers and only then waits.
func TestBackendCloseEndsAnAsyncSendInFlight(t *testing.T) {
	for _, retired := range []bool{false, true} {
		name := "cached producer"
		if retired {
			name = "retired producer"
		}
		t.Run(name, func(t *testing.T) {
			b, cl := newClosingRigWith(t, func(c *pd.Config) { c.SendTimeout, c.OperationTimeout = -1, closeBound })
			first := sinkOf(t, b, "parts", mq.MetadataNativeStrict)
			second := sinkOf(t, b, "parts", mq.MetadataNativeStrict)
			old := cl.newest(t)

			written, receipt := old.holdNextSend()
			t.Cleanup(receipt)
			done := make(chan error, 1)
			if err := first.(mq.AsyncSink).PublishAsync(context.Background(),
				&mq.Message{Body: []byte("in flight")}, nil, func(err error) { done <- err }); err != nil {
				t.Fatalf("PublishAsync: %v", err)
			}
			select {
			case <-written:
			case <-time.After(5 * time.Second):
				t.Fatal("the publication never reached the producer")
			}
			if retired {
				old.closeForGood(pulsar.ErrProducerClosed)
				_ = second.Publish(context.Background(), &mq.Message{Body: []byte("lost")}, nil)
				if err := second.Publish(context.Background(), &mq.Message{Body: []byte("fresh")}, nil); err != nil {
					t.Fatalf("the publication through the fresh producer reported %v", err)
				}
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
				t.Fatalf("Backend.Close did not return (%v once the receipt came, and the callback then "+
					"reported %v) while an asynchronous publication waited for its receipt on the %s",
					err, <-done, name)
			}
			select {
			case err := <-done:
				if err == nil || !mq.IsAmbiguous(err) {
					t.Fatalf("the publication Close ended reported %v (ambiguous %v), want an ambiguous failure",
						err, mq.IsAmbiguous(err))
				}
			default:
				t.Fatal("Close returned before the publication's callback ran")
			}
		})
	}
}

// TestClientCloseEndsAnAsyncSendInFlight is the same publication closed the
// way most programs close it: the core's Client.Close, which closes the
// publisher, and so its sink, before the backend.
//
// The sink's Close waited for the callback before it released the producer,
// so with SendTimeout disabled Client.Close never returned while the broker
// stayed silent. The sink that holds a producer's last reference now flushes
// it, closes it and then waits, so the publication fails as ambiguous.
func TestClientCloseEndsAnAsyncSendInFlight(t *testing.T) {
	b, cl := newClosingRigWith(t, func(c *pd.Config) { c.SendTimeout, c.OperationTimeout = -1, closeBound })
	c := mq.NewClient(b)
	pub, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: "parts"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	old := cl.newest(t)
	written, receipt := old.holdNextSend()
	t.Cleanup(receipt)
	done := make(chan mq.PublishResult, 1)
	if err := pub.PublishAsync(context.Background(), &mq.Message{Body: []byte("in flight")},
		func(r mq.PublishResult) { done <- r }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	select {
	case <-written:
	case <-time.After(5 * time.Second):
		t.Fatal("the publication never reached the producer")
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Client.Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		receipt()
		err := <-closed
		t.Fatalf("Client.Close did not return (%v once the receipt came, and the callback then got %+v) while "+
			"an asynchronous publication waited for its receipt (SendTimeout disabled)", err, <-done)
	}
	select {
	case r := <-done:
		if r.Err == nil || !mq.IsAmbiguous(r.Err) {
			t.Fatalf("the publication Close ended reported %v (ambiguous %v), want an ambiguous failure",
				r.Err, mq.IsAmbiguous(r.Err))
		}
	default:
		t.Fatal("Client.Close returned before the publication's callback ran")
	}
}

// TestCloseKeepsAReceiptThatArrivesDuringItsFlush is a healthy broker: the
// receipt of the publication in flight arrives while Close is flushing, and
// the publication succeeds rather than being failed by the producer's close.
func TestCloseKeepsAReceiptThatArrivesDuringItsFlush(t *testing.T) {
	b, cl := newClosingRigWith(t, func(c *pd.Config) { c.SendTimeout, c.OperationTimeout = -1, 5*time.Second })
	snk := sinkOf(t, b, "parts", mq.MetadataNativeStrict)
	old := cl.newest(t)
	written, receipt := old.holdNextSend()
	t.Cleanup(receipt)
	done := make(chan error, 1)
	if err := snk.(mq.AsyncSink).PublishAsync(context.Background(),
		&mq.Message{Body: []byte("in flight")}, nil, func(err error) { done <- err }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	<-written
	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	time.Sleep(50 * time.Millisecond)
	receipt()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Backend.Close did not return once the receipt came")
	}
	if err := <-done; err != nil {
		t.Fatalf("a publication whose receipt arrived while Close was flushing reported %v, want nil", err)
	}
}

// TestCloseDoesNotCountWhatItFailsAsADegradation is a ConfirmNone publication
// waiting for its receipt when Close runs. Such a publication's failure is
// recorded in the health state, because its caller hears nothing of it; a
// failure the Close itself causes, by closing the producer, is not the
// broker's and is not counted.
func TestCloseDoesNotCountWhatItFailsAsADegradation(t *testing.T) {
	b, cl := newClosingRigWith(t, func(c *pd.Config) { c.SendTimeout, c.OperationTimeout = -1, closeBound })
	snk, err := b.OpenSink(context.Background(), mq.SinkConfig{
		Topic: "parts", Metadata: mq.MetadataNativeStrict, Confirmation: mq.ConfirmNone,
	})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	old := cl.newest(t)
	written, receipt := old.holdNextSend()
	t.Cleanup(receipt)
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("in flight")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	<-written
	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		receipt()
		t.Fatalf("Backend.Close did not return (%v once the receipt came) while a ConfirmNone publication "+
			"waited for its receipt", <-closed)
	}
	if n := b.DegradedTransitions(); n != 0 {
		t.Fatalf("closing the backend counted %d degraded transition(s) for the publication it failed itself", n)
	}
}

// TestCloseSendsWhatTheProducerIsStillBatching is a publication still in the
// producer's batch when Close closes the producer.
//
// pulsar-client-go v0.21 keeps a batched message's callback in its batch
// container until the batch is flushed, and the producer's Close neither
// flushes the batch nor fails what is in it. Close closed the producers
// without a flush, so a synchronous publication made within the batching
// delay before Close never got an answer: with SendTimeout disabled it waited
// for its context alone. Closing before waiting, as a prompt Close has to,
// would lose an asynchronous publication's callback the same way. Close now
// flushes first, so the batch reaches the broker and is answered.
func TestCloseSendsWhatTheProducerIsStillBatching(t *testing.T) {
	cases := []struct {
		name  string
		delay time.Duration
		async bool
		// sinkClose closes the sink, which holds the producer's last
		// reference, rather than the backend.
		sinkClose bool
	}{
		// The batching delay is long enough for Close to come inside it.
		{name: "synchronous, Backend.Close", delay: time.Hour},
		{name: "asynchronous, Backend.Close", delay: 20 * time.Millisecond, async: true},
		{name: "asynchronous, the last sink's Close", delay: 20 * time.Millisecond, async: true, sinkClose: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, cl := newClosingRigWith(t, func(c *pd.Config) { c.SendTimeout, c.OperationTimeout = -1, closeBound })
			snk := sinkOf(t, b, "parts", mq.MetadataNativeStrict)
			old := cl.newest(t)
			batched := old.batchNextSend(tc.delay)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			done := make(chan error, 1)
			m := &mq.Message{Body: []byte("batched")}
			if tc.async {
				if err := snk.(mq.AsyncSink).PublishAsync(ctx, m, nil, func(err error) { done <- err }); err != nil {
					t.Fatalf("PublishAsync: %v", err)
				}
			} else {
				go func() { done <- snk.Publish(ctx, m, nil) }()
			}
			<-batched
			closed := make(chan error, 1)
			go func() {
				if tc.sinkClose {
					closed <- snk.Close()
					return
				}
				closed <- b.Close()
			}()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatalf("Close: %v", err)
				}
			case <-time.After(5 * time.Second):
				old.rescue()
				err := <-closed
				t.Fatalf("Close did not return (%v once the lost callback was run by hand) while a publication was "+
					"still in the producer's batch", err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("the batched publication reported %v, want nil: Close flushes the batch before it "+
						"closes the producer", err)
				}
			case <-time.After(5 * time.Second):
				cancel()
				t.Fatalf("the batched publication never got an answer after Close (it ended with its context: %v)",
					<-done)
			}
		})
	}
}

// TestCloseWaitsForAPublicationOnItsWayIn is an asynchronous publication the
// sink admitted that is still inside pulsar-client-go's SendAsync when Close
// runs: descheduled before it reaches the producer's queue, or, under
// Config.BlockIfQueueFull, waiting there for room.
//
// Close stopped admitting, flushed and closed the producer, and then waited for
// the callbacks. A publication that reached the queue after the flush was
// dropped with its callback by the close, so Close never returned; one that
// reached it after the close sent on the producer's closed queue, which in
// pulsar-client-go v0.21 panics and takes the process with it
// (internalSendAsync after doClose). Close now waits for every publication it
// admitted to return from SendAsync before it flushes.
func TestCloseWaitsForAPublicationOnItsWayIn(t *testing.T) {
	afterFlush := func(e parkEvents) <-chan struct{} { return e.flushed }
	afterClose := func(e parkEvents) <-chan struct{} { return e.closed }
	cases := []struct {
		name      string
		at        func(parkEvents) <-chan struct{}
		sinkClose bool
	}{
		{"reaching the queue after the flush", afterFlush, false},
		{"reaching the queue after the close", afterClose, false},
		{"reaching the queue after the flush, the last sink's Close", afterFlush, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, cl := newClosingRigWith(t, func(c *pd.Config) { c.SendTimeout, c.OperationTimeout = -1, closeBound })
			snk := sinkOf(t, b, "parts", mq.MetadataNativeStrict)
			old := cl.newest(t)
			// The queue's delay is long enough that only a flush sends what
			// reaches it.
			ev := old.parkNextSend(time.Hour)
			t.Cleanup(ev.release)
			done := make(chan error, 1)
			returned := make(chan error, 1)
			go func() {
				returned <- snk.(mq.AsyncSink).PublishAsync(context.Background(),
					&mq.Message{Body: []byte("on its way in")}, nil, func(err error) { done <- err })
			}()
			<-ev.parked
			closed := make(chan error, 1)
			go func() {
				if tc.sinkClose {
					closed <- snk.Close()
					return
				}
				closed <- b.Close()
			}()
			// The send goes on at the moment the case names, or after a
			// while if Close never gets there, as it must not while a
			// send is on its way in.
			select {
			case <-tc.at(ev):
			case <-time.After(closeBound):
			}
			ev.release()
			if err := <-returned; err != nil {
				t.Fatalf("PublishAsync: %v", err)
			}
			select {
			case err := <-closed:
				if err != nil {
					t.Fatalf("Close: %v", err)
				}
			case <-time.After(5 * time.Second):
				late := old.lateSends()
				old.rescue()
				err := <-closed
				t.Fatalf("Close did not return (%v once the lost callback was run by hand) while a publication "+
					"was on its way into the producer; %d send(s) reached the queue after the producer closed, "+
					"which pulsar-client-go v0.21 answers with a panic", err, late)
			}
			if n := old.lateSends(); n != 0 {
				t.Fatalf("%d send(s) reached the producer's queue after Close had closed it, which "+
					"pulsar-client-go v0.21 answers with a panic (send on closed channel)", n)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("the publication reported %v, want nil: Close flushes it once it is queued", err)
				}
			default:
				t.Fatal("Close returned before the publication's callback ran")
			}
		})
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
