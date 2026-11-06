package pulsar_test

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// queueClient is the in-process model with pulsar-client-go v0.21's publish
// queue in front of every producer it opens (producer_partition.go): a
// publication takes one of MaxPendingMessages places before it is queued
// (reserveSemaphore) and gives it back once it is answered (sendRequest.done),
// and the queue is answered one publication at a time on a reader goroutine of
// the producer's own, as v0.21's connection reader answers receipts. Without
// DisableBlockIfQueueFull a publication waits for its place in v0.21's own
// semaphore; see v021Semaphore for why that wait is one nobody may give up on.
type queueClient struct {
	pulsarapi.Client
	mu    sync.Mutex
	prods []*queueProducer
	// partitions is how many partitions its producers say they have, one
	// when zero; places, when set, is the size of their queues in place of
	// MaxPendingMessages, as one partition's queue or the memory buffer
	// fills first.
	partitions int
	places     int
	// memory makes a full queue answer ClientMemoryBufferIsFull, as the
	// client's memory buffer does when it fills first.
	memory bool
	// rescuing ends the goroutines rescue starts, and rescuers waits for them.
	rescuing  chan struct{}
	rescuers  sync.WaitGroup
	rescueOne sync.Once
	endOne    sync.Once
}

func (c *queueClient) CreateProducer(o pulsar.ProducerOptions) (pulsar.Producer, error) {
	p, err := c.Client.CreateProducer(o)
	if err != nil {
		return nil, err
	}
	n := o.MaxPendingMessages
	if n == 0 {
		n = 1000 // v0.21's own default
	}
	if c.places > 0 {
		n = c.places
	}
	ready := make(chan struct{})
	close(ready)
	qp := &queueProducer{
		Producer: p, opts: o, sem: &v021Semaphore{max: int32(n), ch: make(chan bool)},
		ready: ready, wake: make(chan struct{}, 1), quit: make(chan struct{}), exited: make(chan struct{}),
	}
	qp.parts.Store(int32(max(c.partitions, 1)))
	qp.memory = c.memory
	go qp.read()
	c.mu.Lock()
	c.prods = append(c.prods, qp)
	c.mu.Unlock()
	return qp, nil
}

// Close closes the client, and with it every producer's reader.
func (c *queueClient) Close() {
	for _, p := range c.producers() {
		p.stop()
	}
	c.Client.Close()
}

// producers is every producer the client has opened.
func (c *queueClient) producers() []*queueProducer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*queueProducer(nil), c.prods...)
}

// newest is the producer the client opened last.
func (c *queueClient) newest(t *testing.T) *queueProducer {
	t.Helper()
	ps := c.producers()
	if len(ps) == 0 {
		t.Fatal("no producer has been opened")
	}
	return ps[len(ps)-1]
}

// rescue takes every place a v0.21 semaphore's Release is stuck offering, for
// the rest of the test, so that a test that found one stuck can still close
// what it opened and fail rather than hang. endRescue ends it.
func (c *queueClient) rescue() {
	c.rescueOne.Do(func() {
		for _, p := range c.producers() {
			c.rescuers.Add(1)
			go func() {
				defer c.rescuers.Done()
				for {
					select {
					case <-p.sem.ch:
					case <-c.rescuing:
						return
					}
				}
			}()
		}
	})
}

// endRescue ends what rescue started.
func (c *queueClient) endRescue() {
	c.endOne.Do(func() { close(c.rescuing) })
	c.rescuers.Wait()
}

// newQueueRig is a backend over a queueClient. A test that fails takes over
// any place a stuck Release offers before the backend closes, and gives that
// up once it has.
func newQueueRig(t *testing.T, mutate func(*pd.Config)) (*pd.Backend, *queueClient) {
	t.Helper()
	server := pulsarfake.New()
	c := &queueClient{Client: server.Client(), rescuing: make(chan struct{})}
	t.Cleanup(c.endRescue)
	cfg := rigConfig(server)
	if mutate != nil {
		mutate(&cfg)
	}
	b, err := pd.BackendWithClient(context.Background(), c, driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	t.Cleanup(func() {
		if t.Failed() {
			c.rescue()
		}
	})
	return b, c
}

// v021Semaphore is pulsar-client-go v0.21's publish semaphore
// (internal/semaphore.go), line for line.
//
// Acquire counts itself in before it waits, and a waiter whose context ends
// counts itself out and leaves. Release counts one out and, when the count it
// leaves is still at the limit or over it, offers the place on an unbuffered
// channel to a waiter it counted. When every waiter it counted has left on its
// context, nothing takes the offer, and the Release waits for ever: on a
// publication's own goroutine, or on the connection's reader, which then
// answers nothing more.
type v021Semaphore struct {
	max     int32
	permits atomic.Int32
	ch      chan bool
}

func (s *v021Semaphore) Acquire(ctx context.Context) bool {
	if s.permits.Add(1) <= s.max {
		return true
	}
	select {
	case <-s.ch:
		return true
	case <-ctx.Done():
		s.permits.Add(-1)
		return false
	}
}

func (s *v021Semaphore) TryAcquire() bool {
	for {
		cur := s.permits.Load()
		if cur >= s.max {
			return false
		}
		if s.permits.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (s *v021Semaphore) Release() {
	if s.permits.Add(-1) >= s.max {
		s.ch <- true
	}
}

// queueProducer is one producer of a queueClient.
type queueProducer struct {
	pulsar.Producer
	opts pulsar.ProducerOptions
	sem  *v021Semaphore
	// parts is how many partitions the producer says it has, and memory
	// whether a full queue is its memory buffer.
	parts  atomic.Int32
	memory bool
	// ready is closed: a select between it and a context that has ended
	// takes either, as v0.21's select between that context and its buffered
	// send queue does (internalSendAsync).
	ready chan struct{}

	mu sync.Mutex
	// queue is what waits for its receipt, and pending counts it with the
	// one the reader is answering; idle is closed when pending drops to
	// nothing while a flush waits. held, when set, holds every receipt
	// until it is closed.
	queue   []queuedSend
	pending int
	idle    chan struct{}
	held    chan struct{}
	closed  bool
	// queued counts the publications that took a place and reached the
	// queue, and late the ones that reached it after the producer closed,
	// which v0.21 answers with a panic (a send on its closed queue).
	queued int
	late   int
	// refusals counts the publications the queue refused for want of room,
	// and refusedBy counts them by payload.
	refusals  int
	refusedBy map[string]int

	wake   chan struct{}
	quit   chan struct{}
	exited chan struct{}
	once   sync.Once
}

type queuedSend struct {
	m  *pulsar.ProducerMessage
	cb func(pulsar.MessageID, *pulsar.ProducerMessage, error)
}

// enqueue is v0.21's internalSendAsync, as far as the queue goes.
func (p *queueProducer) enqueue(ctx context.Context, m *pulsar.ProducerMessage, cb func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		cb(nil, m, pulsar.ErrProducerClosed)
		return
	}
	if err := ctx.Err(); err != nil {
		cb(nil, m, err)
		return
	}
	switch {
	case !p.opts.DisableBlockIfQueueFull:
		if !p.sem.Acquire(ctx) {
			cb(nil, m, pulsar.ErrContextExpired)
			return
		}
	case !p.sem.TryAcquire():
		p.mu.Lock()
		p.refusals++
		if p.refusedBy == nil {
			p.refusedBy = map[string]int{}
		}
		p.refusedBy[string(m.Payload)]++
		p.mu.Unlock()
		if p.memory {
			cb(nil, m, pulsar.ErrMemoryBufferIsFull)
			return
		}
		cb(nil, m, pulsar.ErrSendQueueIsFull)
		return
	}
	select {
	case <-ctx.Done():
		p.sem.Release()
		cb(nil, m, ctx.Err())
		return
	case <-p.ready:
	}
	p.mu.Lock()
	if p.closed {
		p.late++
		p.mu.Unlock()
		return
	}
	p.queue = append(p.queue, queuedSend{m: m, cb: cb})
	p.pending++
	p.queued++
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// read answers the queue, one publication at a time.
func (p *queueProducer) read() {
	defer close(p.exited)
	for {
		select {
		case <-p.quit:
			return
		case <-p.wake:
		}
		for {
			p.mu.Lock()
			held := p.held
			if held == nil && len(p.queue) == 0 {
				p.mu.Unlock()
				break
			}
			if held != nil {
				p.mu.Unlock()
				select {
				case <-held:
					continue
				case <-p.quit:
					return
				}
			}
			s := p.queue[0]
			p.queue = p.queue[1:]
			p.mu.Unlock()
			id, err := p.Producer.Send(context.Background(), s.m)
			p.sem.Release()
			s.cb(id, s.m, err)
			p.mu.Lock()
			p.pending--
			if p.pending == 0 && p.idle != nil {
				close(p.idle)
				p.idle = nil
			}
			p.mu.Unlock()
		}
	}
}

// hold holds every receipt until the returned function is called, which is
// harmless to call twice: a broker that has stopped answering.
func (p *queueProducer) hold() (release func()) {
	held := make(chan struct{})
	p.mu.Lock()
	p.held = held
	p.mu.Unlock()
	return sync.OnceFunc(func() {
		p.mu.Lock()
		p.held = nil
		p.mu.Unlock()
		close(held)
		select {
		case p.wake <- struct{}{}:
		default:
		}
	})
}

// counts is how many publications reached the queue, and how many reached it
// after the producer closed.
// NumPartitions is how many partitions the producer has, as v0.21's
// partitioned producer says.
func (p *queueProducer) NumPartitions() uint32 { return uint32(p.parts.Load()) }

func (p *queueProducer) counts() (queued, late int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.queued, p.late
}

// refused is how many publications the queue has refused for want of room.
func (p *queueProducer) refused() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.refusals
}

// refusedFor is how many times the queue refused the publication of body.
func (p *queueProducer) refusedFor(body string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.refusedBy[body]
}

func (p *queueProducer) SendAsync(ctx context.Context, m *pulsar.ProducerMessage, cb func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	p.enqueue(ctx, m, cb)
}

// Send is v0.21's: the request goes in as SendAsync's does, and the call
// waits for its answer or its context.
func (p *queueProducer) Send(ctx context.Context, m *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	type answer struct {
		id  pulsar.MessageID
		err error
	}
	out := make(chan answer, 1)
	p.enqueue(ctx, m, func(id pulsar.MessageID, _ *pulsar.ProducerMessage, err error) { out <- answer{id, err} })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case a := <-out:
		return a.id, a.err
	}
}

// FlushWithCtx waits for every queued publication to be answered, as v0.21's
// waits for the last one.
func (p *queueProducer) FlushWithCtx(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return pulsar.ErrProducerClosed
	}
	if p.pending == 0 {
		p.mu.Unlock()
		return nil
	}
	if p.idle == nil {
		p.idle = make(chan struct{})
	}
	idle := p.idle
	p.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close fails what is still queued, as v0.21's doClose does
// (failPendingMessages), and ends the reader.
func (p *queueProducer) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	left := p.queue
	p.queue = nil
	p.pending -= len(left)
	idle := p.idle
	if p.pending == 0 {
		p.idle = nil
	}
	p.mu.Unlock()
	for _, s := range left {
		p.sem.Release()
		s.cb(nil, s.m, pulsar.ErrProducerClosed)
	}
	if idle != nil && p.pending == 0 {
		close(idle)
	}
	p.stop()
	p.Producer.Close()
}

// stop ends the reader and waits for it.
func (p *queueProducer) stop() {
	p.once.Do(func() { close(p.quit) })
	<-p.exited
}

// blockConfig is BlockIfQueueFull with a queue of n.
func blockConfig(n int) func(*pd.Config) {
	return func(c *pd.Config) {
		c.BlockIfQueueFull, c.MaxPendingMessages = true, n
		c.OperationTimeout = time.Second
	}
}

// TestBlockIfQueueFullWaitsInTheDriver is Config.BlockIfQueueFull on a full
// queue.
//
// The driver passed BlockIfQueueFull through, so a publication waited for room
// in pulsar-client-go v0.21's own semaphore, whose wait nobody may give up on
// (v021Semaphore): a Close that ended the synchronous publications waiting
// there left one Release offering a place nobody took, and Close waited for
// that publication for ever; publications whose deadlines ended while they
// waited left the connection's reader stuck the same way, and no receipt was
// read on that connection again. The driver now never lets the client wait
// (it always sets DisableBlockIfQueueFull) and waits for room itself, sized to
// MaxPendingMessages: nothing reaches the client until there is a place for
// it, and a wait that ends hands nothing over.
func TestBlockIfQueueFullWaitsInTheDriver(t *testing.T) {
	b, c := newQueueRig(t, blockConfig(2))
	snk := sinkOf(t, b, "full", mq.MetadataNativeStrict)
	prod := c.newest(t)
	if !prod.opts.DisableBlockIfQueueFull {
		t.Error("the driver let pulsar-client-go wait for room in its own queue (DisableBlockIfQueueFull unset), " +
			"which v0.21 cannot do safely: a wait that gives up on its context can leave the semaphore's Release " +
			"waiting for ever")
	}
	release := prod.hold()
	t.Cleanup(release)
	answered := make(chan error, 4)
	for range 2 {
		if err := snk.(mq.AsyncSink).PublishAsync(context.Background(), &mq.Message{Body: []byte("in flight")}, nil,
			func(err error) { answered <- err }); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
	}
	// The queue is full: a publication with a deadline waits for room and
	// gives up on it without handing anything to the client.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := snk.Publish(ctx, &mq.Message{Body: []byte("no room")}, nil)
	if !errors.Is(err, context.DeadlineExceeded) || mq.IsAmbiguous(err) {
		t.Errorf("a Publish whose deadline ended while it waited for room reported %v (ambiguous %v), want the "+
			"definite DeadlineExceeded of a publication the client never saw", err, mq.IsAmbiguous(err))
	}
	err = snk.(mq.AsyncSink).PublishAsync(ctx, &mq.Message{Body: []byte("no room")}, nil,
		func(err error) { t.Errorf("a PublishAsync that was refused ran its callback with %v", err) })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a PublishAsync whose deadline ended while it waited for room reported %v, want DeadlineExceeded", err)
	}
	if queued, _ := prod.counts(); queued != 2 {
		t.Errorf("%d publications reached the client's queue of 2, want 2: the driver handed over publications "+
			"it had no room for", queued)
	}
	// One that waits without a deadline gets the room a receipt frees.
	published := make(chan error, 1)
	go func() { published <- snk.Publish(context.Background(), &mq.Message{Body: []byte("waits")}, nil) }()
	select {
	case err := <-published:
		t.Fatalf("a Publish on a full queue returned (%v) before any room was freed", err)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	select {
	case err := <-published:
		if err != nil {
			t.Fatalf("the Publish that waited for room reported %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the Publish that waited for room never got it")
	}
	for range 2 {
		if err := <-answered; err != nil {
			t.Fatalf("a publication in flight reported %v", err)
		}
	}
}

// TestCloseEndsAPublicationWaitingForRoom is a Close while publications wait
// for room in a full queue under BlockIfQueueFull, with a broker that has
// stopped answering.
//
// Close used to wait for an asynchronous publication waiting there, for as
// long as the broker stayed silent, and to end a synchronous one as ambiguous.
// Neither has reached the client while it waits for room in the driver, so
// Close now ends both at once, as definite refusals of a closed sink.
func TestCloseEndsAPublicationWaitingForRoom(t *testing.T) {
	for _, via := range []string{"Backend.Close", "the last sink's Close", "another sink's Close"} {
		t.Run(via, func(t *testing.T) {
			b, c := newQueueRig(t, func(cfg *pd.Config) {
				blockConfig(1)(cfg)
				cfg.SendTimeout = -1
			})
			snk := sinkOf(t, b, "full", mq.MetadataNativeStrict)
			other := sinkOf(t, b, "full", mq.MetadataNativeStrict)
			if via != "another sink's Close" {
				_ = other.Close()
			}
			prod := c.newest(t)
			release := prod.hold()
			t.Cleanup(release)
			answered := make(chan error, 1)
			if err := snk.(mq.AsyncSink).PublishAsync(context.Background(), &mq.Message{Body: []byte("in flight")},
				nil, func(err error) { answered <- err }); err != nil {
				t.Fatalf("PublishAsync: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			syncDone := make(chan error, 1)
			asyncDone := make(chan error, 1)
			go func() { syncDone <- snk.Publish(ctx, &mq.Message{Body: []byte("waits")}, nil) }()
			go func() {
				asyncDone <- snk.(mq.AsyncSink).PublishAsync(ctx, &mq.Message{Body: []byte("waits")}, nil,
					func(err error) { t.Errorf("a PublishAsync Close refused ran its callback with %v", err) })
			}()
			time.Sleep(50 * time.Millisecond)
			closed := make(chan error, 1)
			go func() {
				if via == "Backend.Close" {
					closed <- b.Close()
					return
				}
				closed <- snk.Close()
			}()
			for _, w := range []struct {
				name string
				ch   chan error
			}{{"synchronous", syncDone}, {"asynchronous", asyncDone}} {
				select {
				case err := <-w.ch:
					if !errors.Is(err, mq.ErrClosed) || mq.IsAmbiguous(err) {
						t.Errorf("the %s publication Close ended while it waited for room reported %v "+
							"(ambiguous %v), want the definite ErrClosed of a publication the client never saw",
							w.name, err, mq.IsAmbiguous(err))
					}
				case <-time.After(5 * time.Second):
					cancel()
					t.Fatalf("Close did not end the %s publication waiting for room (%v once its context was "+
						"cancelled)", w.name, <-w.ch)
				}
			}
			if via == "another sink's Close" {
				// The producer stays open for the other sink, and the
				// publication in flight on it waits for its receipt.
				release()
			}
			select {
			case err := <-closed:
				if err != nil {
					t.Fatalf("Close: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not return")
			}
			if err := <-answered; via != "another sink's Close" && (err == nil || !mq.IsAmbiguous(err)) {
				t.Fatalf("the publication in flight reported %v, want the ambiguous failure of a close", err)
			}
			if queued, late := prod.counts(); queued != 1 || late != 0 {
				t.Fatalf("%d publications reached the client's queue (%d after it closed), want only the one in "+
					"flight", queued, late)
			}
		})
	}
}

// TestCloseUnderBlockIfQueueFullReturns is many synchronous publishers on a
// queue of two under BlockIfQueueFull, on a healthy broker, and then a plain
// close: the last sink's and then the backend's, or the other way round.
//
// Close ends every synchronous Publish inside the producer through its context
// and waits for it. Most of them were waiting for room in v0.21's semaphore,
// and one ended there could be handed a place as it left, and offer it back to
// waiters that had all left too, so its Release, and the Close waiting for it,
// never returned.
func TestCloseUnderBlockIfQueueFullReturns(t *testing.T) {
	for _, sinkFirst := range []bool{true, false} {
		name := "the sink's Close first"
		if !sinkFirst {
			name = "Backend.Close first"
		}
		t.Run(name, func(t *testing.T) {
			for round := range 40 {
				closeUnderLoad(t, round, sinkFirst)
				if t.Failed() {
					return
				}
			}
		})
	}
}

// closeUnderLoad is one round of TestCloseUnderBlockIfQueueFullReturns.
func closeUnderLoad(t *testing.T, round int, sinkFirst bool) {
	b, c := newQueueRig(t, blockConfig(2))
	snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "load", Metadata: mq.MetadataNativeStrict})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	var stop atomic.Bool
	var publishers sync.WaitGroup
	for range 64 {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			for !stop.Load() {
				if snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil) != nil {
					return
				}
			}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		if sinkFirst {
			_ = snk.Close()
			_ = b.Close()
			return
		}
		_ = b.Close()
		_ = snk.Close()
	}()
	stop.Store(true)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		c.rescue()
		<-closed
		publishers.Wait()
		t.Fatalf("round %d: Close did not return on a healthy broker while synchronous publications waited "+
			"for room under BlockIfQueueFull", round)
	}
	waited := make(chan struct{})
	go func() { publishers.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		c.rescue()
		<-waited
		t.Fatalf("round %d: Close returned with synchronous publications still waiting", round)
	}
	for _, p := range c.producers() {
		if _, late := p.counts(); late != 0 {
			t.Fatalf("round %d: %d publications reached the client's queue after the producer closed", round, late)
		}
	}
}

// TestShortDeadlinesUnderBlockIfQueueFullLeaveThePublisherWorking is many
// synchronous publishers with deadlines of a few milliseconds on a queue of two
// under BlockIfQueueFull, on a healthy broker, and then one publication with
// time to spare.
//
// Publications whose deadlines ended while they waited for room in v0.21's
// semaphore could leave the reader that answers receipts offering a freed
// place to waiters that had all left, for ever: no receipt was read again, and
// every later publication on the producer, and on anything else sharing its
// connection, failed with its deadline. Under the driver's own wait the client
// is never asked to wait, and the publication with time to spare goes through.
func TestShortDeadlinesUnderBlockIfQueueFullLeaveThePublisherWorking(t *testing.T) {
	for round := range 5 {
		b, c := newQueueRig(t, blockConfig(2))
		snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "deadlines", Metadata: mq.MetadataNativeStrict})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		var stop atomic.Bool
		var publishers sync.WaitGroup
		for w := range 64 {
			publishers.Add(1)
			go func() {
				defer publishers.Done()
				r := rand.New(rand.NewSource(int64(w)))
				for !stop.Load() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Duration(200+r.Intn(3000))*time.Microsecond)
					_ = snk.Publish(ctx, &mq.Message{Body: []byte("x")}, nil)
					cancel()
				}
			}()
		}
		time.Sleep(200 * time.Millisecond)
		stop.Store(true)
		publishers.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = snk.Publish(ctx, &mq.Message{Body: []byte("after")}, nil)
		cancel()
		if err != nil {
			c.rescue()
			t.Fatalf("round %d: a Publish with two seconds to spare on a healthy broker reported %v after publications "+
				"with short deadlines had waited for room: the client's reader is stuck offering a place to "+
				"waiters that have left", round, err)
		}
		_ = snk.Close()
		_ = b.Close()
	}
}

// TestBlockIfQueueFullWaitsOutTheClientsRefusal is a client that refuses a
// publication for want of room although the driver had room for it: its queue
// still holds publications whose callers gave up on them, or its memory buffer
// is full. Under BlockIfQueueFull the publication waits for room rather than
// being refused, as the configuration says, and it is handed over again once
// there is some.
func TestBlockIfQueueFullWaitsOutTheClientsRefusal(t *testing.T) {
	for _, async := range []bool{false, true} {
		name := "Publish"
		if async {
			name = "PublishAsync"
		}
		t.Run(name, func(t *testing.T) {
			b, server := newRig(t, blockConfig(4))
			snk := sinkOf(t, b, "refused", mq.MetadataNativeStrict)
			server.SetQueueFull("persistent://public/default/refused", true)
			done := make(chan error, 1)
			go func() {
				if !async {
					done <- snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
					return
				}
				if err := snk.(mq.AsyncSink).PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil,
					func(err error) { done <- err }); err != nil {
					done <- err
				}
			}()
			select {
			case err := <-done:
				t.Fatalf("a publication the client refused for want of room under BlockIfQueueFull reported %v "+
					"(code %v), want it to wait for room", err, mq.CodeOf(err))
			case <-time.After(100 * time.Millisecond):
			}
			server.SetQueueFull("persistent://public/default/refused", false)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("the publication that waited out the client's refusal reported %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the publication that waited out the client's refusal was never handed over again")
			}
			if n := server.Messages("persistent://public/default/refused"); n != 1 {
				t.Fatalf("the topic holds %d messages, want 1", n)
			}
		})
	}
}

// TestBlockIfQueueFullHoldsMaxPendingMessagesForEachPartition is a partitioned
// topic under BlockIfQueueFull.
//
// pulsar-client-go v0.21 bounds each partition's queue at MaxPendingMessages.
// The driver's own wait bounded the topic as a whole at it, so a topic of four
// partitions held a quarter of what it used to and published at about half
// the rate. The room now holds MaxPendingMessages for each partition the
// producer has, and grows when partitions are added.
func TestBlockIfQueueFullHoldsMaxPendingMessagesForEachPartition(t *testing.T) {
	b, c := newQueueRig(t, blockConfig(2))
	c.partitions, c.places = 3, 8
	snk := sinkOf(t, b, "parts", mq.MetadataNativeStrict)
	prod := c.newest(t)
	release := prod.hold()
	t.Cleanup(release)
	answered := make(chan error, 9)
	publish := func(ctx context.Context) error {
		return snk.(mq.AsyncSink).PublishAsync(ctx, &mq.Message{Body: []byte("x")}, nil,
			func(err error) { answered <- err })
	}
	for i := range 6 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := publish(ctx)
		cancel()
		if err != nil {
			t.Fatalf("publication %d of six on three partitions of two places each reported %v, want room for it", i+1, err)
		}
	}
	waiting := make(chan error, 1)
	go func() { waiting <- publish(context.Background()) }()
	select {
	case err := <-waiting:
		t.Fatalf("a seventh publication on three partitions of two places each returned (%v) before any room was "+
			"freed", err)
	case <-time.After(50 * time.Millisecond):
	}
	if queued, _ := prod.counts(); queued != 6 {
		t.Fatalf("%d publications reached the client, want 6", queued)
	}
	// A fourth partition brings two places more.
	prod.parts.Store(4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := publish(ctx); err != nil {
		t.Fatalf("a publication after a fourth partition was added reported %v, want room for it", err)
	}
	select {
	case err := <-waiting:
		if err != nil {
			t.Fatalf("the seventh publication reported %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the seventh publication never got the room a fourth partition brought")
	}
	if queued, _ := prod.counts(); queued != 8 {
		t.Fatalf("%d publications reached the client, want 8", queued)
	}
	release()
	for range 8 {
		if err := <-answered; err != nil {
			t.Fatalf("a publication reported %v", err)
		}
	}
}

// TestARefusedPublicationKeepsItsTurn is the client refusing a publication for
// want of room under BlockIfQueueFull, as it does while its memory buffer is
// full, with another publication coming after it.
//
// A refused publication gave its place back and waited, longer each time it
// was refused, and one that came after it, waiting less, took the room the
// client freed first. Under a full memory buffer the slowest publication
// waited seconds. A refused publication now keeps its place and its turn: it
// is woken when room comes back, and the publications after it wait behind it.
func TestARefusedPublicationKeepsItsTurn(t *testing.T) {
	for round := range 3 {
		b, c := newQueueRig(t, blockConfig(4))
		// The client's memory buffer takes one publication where the room
		// has four.
		c.places, c.memory = 1, true
		snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "turns", Metadata: mq.MetadataNativeStrict})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		prod := c.newest(t)
		release := prod.hold()
		t.Cleanup(release)
		inFlight := make(chan error, 1)
		if err := snk.(mq.AsyncSink).PublishAsync(context.Background(), &mq.Message{Body: []byte("in flight")}, nil,
			func(err error) { inFlight <- err }); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
		type outcome struct {
			name string
			err  error
		}
		order := make(chan outcome, 2)
		publish := func(name string) {
			order <- outcome{name, snk.Publish(context.Background(), &mq.Message{Body: []byte(name)}, nil)}
		}
		go publish("first")
		// The first is refused until it waits its longest between tries
		// (seven tries: 1, 2, 4, 8, 16, 32 and then 50 ms), and the second
		// comes, and room with it, just after one of them, well inside the
		// first's next wait, so that neither lands while the first is being
		// handed to the client.
		waitFor(t, "the first publication to be refused eight times", func() bool { return prod.refused() >= 8 })
		last := prod.refused()
		waitFor(t, "the first publication's next try", func() bool { return prod.refused() > last })
		time.Sleep(2 * time.Millisecond)
		go publish("second")
		time.Sleep(10 * time.Millisecond)
		release()
		for _, want := range []string{"first", "second"} {
			select {
			case got := <-order:
				if got.err != nil {
					t.Fatalf("round %d: the %s publication reported %v", round, got.name, got.err)
				}
				if got.name != want {
					t.Fatalf("round %d: the %s publication went before the one the client had refused first, "+
						"which waited longer", round, got.name)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("round %d: the %s publication never went through", round, want)
			}
		}
		if err := <-inFlight; err != nil {
			t.Fatalf("round %d: the publication in flight reported %v", round, err)
		}
		_ = snk.Close()
		_ = b.Close()
	}
}

// TestAFullQueueWhileCallersGiveUpLetsOthersTry is a queue the client fills
// with a synchronous publication whose caller gave up on it, and two
// publications without deadlines waiting for room behind it.
//
// The first of them to be refused kept its place in the line, and nobody else
// was let in until the client took it. Under deadlines of a few milliseconds
// that served the oldest publications first, the ones nearest their
// deadlines, and the hunter's probe got 18 to 31 publications through in 8
// seconds where giving the place back got about 2000. While callers are giving
// up on their deadlines, a publication refused for a full queue now gives its
// place back for its pause, and the next one tries meanwhile.
func TestAFullQueueWhileCallersGiveUpLetsOthersTry(t *testing.T) {
	b, c := newQueueRig(t, blockConfig(1))
	snk := sinkOf(t, b, "abandoned", mq.MetadataNativeStrict)
	prod := c.newest(t)
	release := prod.hold()
	t.Cleanup(release)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := snk.Publish(ctx, &mq.Message{Body: []byte("abandoned")}, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the publication whose caller gave up reported %v, want DeadlineExceeded", err)
	}
	done := make(chan error, 2)
	go func() { done <- snk.Publish(context.Background(), &mq.Message{Body: []byte("first")}, nil) }()
	waitFor(t, "the first publication to be refused", func() bool { return prod.refusedFor("first") > 0 })
	go func() { done <- snk.Publish(context.Background(), &mq.Message{Body: []byte("second")}, nil) }()
	deadline := time.Now().Add(2 * time.Second)
	for prod.refusedFor("second") == 0 {
		if time.Now().After(deadline) {
			release()
			t.Fatal("the second publication never reached the client while the first, refused for a queue a " +
				"publication whose caller gave up was filling, held the only place")
		}
		time.Sleep(time.Millisecond)
	}
	release()
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("a publication reported %v once the client had room", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a publication never went through once the client had room")
		}
	}
}
