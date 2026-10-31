package stomp

import (
	"context"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// A source's subscription carries the backend's connection generation, and a
// settlement is refused once that generation has moved: the backend's
// connection dying invalidates every source. The tests here land that death
// inside the source's own dial, which can take up to DialTimeout, by parking
// the dial's CONNECT at the server.

// reconnecting lets the backend re-dial after the loss a test causes.
func reconnecting(c *Config) {
	c.NoReconnect = false
	c.ReconnectBase = 5 * time.Millisecond
	c.ReconnectMax = 20 * time.Millisecond
}

// loseBackendConnection tears down the backend's connection the way a sink
// does when its publication meets the dead socket, and checks that the
// generation moved.
func loseBackendConnection(t *testing.T, b *Backend) {
	t.Helper()
	before := b.Generation()
	b.mu.Lock()
	bc := b.conn
	b.mu.Unlock()
	b.connectionLostFor(bc, gostomp.ErrClosedUnexpectedly)
	if b.Generation() == before {
		t.Fatal("the backend's generation did not move")
	}
}

// parkedDial waits for the dial that sends the CONNECT after the first
// `connects` of them, which the server's gate parks, and keeps that one
// connection's CONNECT parked while every later dial goes through.
func parkedDial(t *testing.T, srv *stompsrv.Server, connects int) int {
	t.Helper()
	if err := srv.WaitFrames(frame.CONNECT, connects+1, requeueBudget); err != nil {
		srv.GateConnects(false)
		t.Fatalf("the dial never sent its CONNECT: %v", err)
	}
	cs := srv.FramesOf(frame.CONNECT)
	parked := cs[connects].Conn
	srv.GateConnection(parked, true)
	srv.GateConnects(false)
	return parked
}

// awaitBackend waits for the backend to have a connection again.
func awaitBackend(ctx context.Context, t *testing.T, b *Backend) {
	t.Helper()
	for {
		if _, _, err := b.connection(ctx); err == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatal("the backend never reconnected")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestASubscriptionMadeWhileTheClientsConnectionDiesHandsOutSettleableDeliveries
// is a source re-dialling its own connection while the backend's dies and is
// replaced, which a broker failover produces: both sockets die from one event,
// the source's pump notices first and its next fetch re-dials, and the
// backend's death and its reconnection land during that dial.
//
// The subscription made after the dial carried the generation read BEFORE it,
// so every delivery it handed out was refused on settlement while the broker
// kept it on a live subscription; the next fetch retired that subscription and
// still handed out what it had buffered, after the UNSUBSCRIBE that gave it
// back.
func TestASubscriptionMadeWhileTheClientsConnectionDiesHandsOutSettleableDeliveries(t *testing.T) {
	t.Parallel()
	srv, b, s := requeueFixture(t, reconnecting)
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()

	// The source's own socket dies (it is the newest connection), so its next
	// fetch dials a new one.
	s.mu.Lock()
	pumpDone := s.pumpDone
	s.mu.Unlock()
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}
	select {
	case <-pumpDone:
	case <-time.After(requeueBudget):
		t.Fatal("the source's pump never saw its socket die")
	}
	_ = s.takeErr()

	srv.GateConnects(true)
	connects := srv.Count(frame.CONNECT)
	type fetched struct {
		d   mq.DriverDelivery
		n   int
		err error
	}
	out := make(chan fetched, 1)
	go func() {
		dst := make([]mq.DriverDelivery, 1)
		n, err := s.Fetch(ctx, dst)
		out <- fetched{dst[0], n, err}
	}()
	parked := parkedDial(t, srv, connects)
	loseBackendConnection(t, b)
	awaitBackend(ctx, t, b)
	srv.GateConnection(parked, false)
	if err := srv.WaitSubscribed(s.Destination(), 2, requeueBudget); err != nil {
		t.Fatalf("the source never subscribed again: %v", err)
	}
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("one")},
		stompsrv.Message{Body: []byte("two")}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	got := <-out
	if got.err != nil || got.n != 1 {
		t.Fatalf("Fetch: n=%d err=%v", got.n, got.err)
	}
	if err := s.set.Ack(ctx, &got.d); err != nil {
		t.Errorf("Ack of a delivery from the subscription made after the backend's loss: %v (delivery generation "+
			"%d, backend generation %d): it came from a live subscription on a live socket, so the broker "+
			"redelivers it after its handler ran", err, got.d.Gen, b.Generation())
	}
	for deadline := time.Now().Add(requeueBudget); s.buffer().Len() == 0; {
		if time.Now().After(deadline) {
			t.Fatal("the second delivery never reached the buffer")
		}
		time.Sleep(time.Millisecond)
	}
	second := fetchAll(t, s, 1)
	if n := srv.Count(frame.UNSUBSCRIBE); n > 0 {
		t.Errorf("the second fetch handed out a delivery after the source sent UNSUBSCRIBE for the subscription "+
			"it came from (%d UNSUBSCRIBE frames); its Ack reports %v\nframes:%s",
			n, s.set.Ack(ctx, &second[0]), frameLogOf(srv))
		return
	}
	if err := s.set.Ack(ctx, &second[0]); err != nil {
		t.Errorf("Ack of the second delivery: %v", err)
	}
}

// artemisServer is an in-process server answering as Artemis, whose protocol
// violations fail the test.
func artemisServer(t *testing.T) *stompsrv.Server {
	t.Helper()
	srv, err := stompsrv.New(stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.44.0"})
	if err != nil {
		t.Fatalf("stompsrv.New: %v", err)
	}
	t.Cleanup(func() {
		_ = srv.Close()
		for _, v := range srv.Violations() {
			t.Errorf("the driver committed a STOMP protocol violation: %v", v)
		}
	})
	return srv
}

// openedAcrossLoss opens a source whose dial the backend's connection dies
// during, with `between` run while that dial is parked after the loss, and
// returns what OpenSource returned.
func openedAcrossLoss(ctx context.Context, t *testing.T, srv *stompsrv.Server, b *Backend,
	between func(),
) (mq.Source, error) {
	t.Helper()
	srv.GateConnects(true)
	type opened struct {
		src mq.Source
		err error
	}
	out := make(chan opened, 1)
	go func() {
		src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 4})
		out <- opened{src, err}
	}()
	parked := parkedDial(t, srv, srv.Count(frame.CONNECT))
	loseBackendConnection(t, b)
	between()
	srv.GateConnection(parked, false)
	got := <-out
	return got.src, got.err
}

// TestASourceOpenedWhileTheClientsConnectionDiesHandsOutSettleableDeliveries
// is the same death and reconnection landing in OpenSource's own dial, before
// the source is in the backend's list, so the backend's teardown never reached
// it either.
func TestASourceOpenedWhileTheClientsConnectionDiesHandsOutSettleableDeliveries(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := Config{
		Broker:         activemq.Artemis,
		Addrs:          []string{srv.Addr()},
		ReceiptTimeout: requeueBudget,
		DialTimeout:    requeueBudget,
	}
	reconnecting(&cfg)
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	opened, err := openedAcrossLoss(ctx, t, srv, b, func() { awaitBackend(ctx, t, b) })
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	s, ok := opened.(*Source)
	if !ok {
		t.Fatalf("OpenSource returned %T", opened)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := srv.WaitLive(s.Destination(), 1, requeueBudget); err != nil {
		t.Fatalf("WaitLive: %v", err)
	}
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("one")}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	// The pump buffers it first, so that a fetch that retires the
	// subscription would find it there.
	for deadline := time.Now().Add(requeueBudget); s.buffer().Len() == 0; {
		if time.Now().After(deadline) {
			t.Fatal("the delivery never reached the buffer")
		}
		time.Sleep(time.Millisecond)
	}
	d := fetchAll(t, s, 1)
	if err := s.set.Ack(ctx, &d[0]); err != nil {
		t.Errorf("Ack of the first delivery of a source opened across the backend's loss: %v (delivery "+
			"generation %d, backend generation %d, UNSUBSCRIBE frames %d): the broker redelivers a message whose "+
			"handler ran", err, d[0].Gen, b.Generation(), srv.Count(frame.UNSUBSCRIBE))
	}
}

// TestASourceOpenedAsTheClientsConnectionDiesForGoodIsRefused is that death
// on a client that never reconnects (NoReconnect). The generation and the
// liveness check are both asked once the dial is done, so OpenSource reports
// the client's own terminal answer rather than returning a source that
// outlives it, and the connection it dialled is let go.
func TestASourceOpenedAsTheClientsConnectionDiesForGoodIsRefused(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := Config{
		Broker:         activemq.Artemis,
		Addrs:          []string{srv.Addr()},
		ReceiptTimeout: requeueBudget,
		DialTimeout:    requeueBudget,
		NoReconnect:    true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	opened, err := openedAcrossLoss(ctx, t, srv, b, func() {})
	if err == nil {
		_ = opened.Close()
		t.Fatalf("OpenSource returned a source on a client whose connection died for good during its dial")
	}
	if mq.IsRetryable(err) {
		t.Errorf("OpenSource returned %v, which says to retry a client nothing will reconnect", err)
	}
	if n := srv.Count(frame.SUBSCRIBE); n != 0 {
		t.Errorf("the refused OpenSource sent %d SUBSCRIBE frames", n)
	}
	if err := srv.WaitFrames(frame.DISCONNECT, 1, requeueBudget); err != nil {
		t.Errorf("the refused OpenSource kept the connection it dialled: %v", err)
	}
}

// TestARetiredSubscriptionHandsOutNothingItBuffered is the retire a fetch
// makes when the backend's connection has moved under a live subscription the
// source was not told about, which is where a source OpenSource has not yet
// registered is when the backend's connection dies.
//
// The retire unsubscribes, so the broker takes back everything the
// subscription delivered. It neither invalidated the tokens nor swapped the
// buffer, so the same fetch handed out what the retired subscription had
// buffered, after the UNSUBSCRIBE, and its settlement was refused.
func TestARetiredSubscriptionHandsOutNothingItBuffered(t *testing.T) {
	t.Parallel()
	srv, b, s := requeueFixture(t, reconnecting)
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("one")},
		stompsrv.Message{Body: []byte("two")}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	for deadline := time.Now().Add(requeueBudget); s.buffer().Len() < 2; {
		if time.Now().After(deadline) {
			t.Fatal("the deliveries never reached the buffer")
		}
		time.Sleep(time.Millisecond)
	}
	// The backend's connection moves on and the source is not told.
	b.mu.Lock()
	delete(b.srcs, s)
	b.mu.Unlock()
	loseBackendConnection(t, b)
	b.mu.Lock()
	b.srcs[s] = struct{}{}
	b.mu.Unlock()
	awaitBackend(ctx, t, b)

	fctx, fcancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer fcancel()
	dst := make([]mq.DriverDelivery, 2)
	n, _ := s.Fetch(fctx, dst)
	if srv.Count(frame.UNSUBSCRIBE) == 0 {
		t.Fatalf("the fetch did not retire the subscription\nframes:%s", frameLogOf(srv))
	}
	for i := range n {
		t.Errorf("the fetch that retired the subscription handed out %q, which that subscription had buffered "+
			"before its UNSUBSCRIBE gave it back; its Ack reports %v", dst[i].Message.Body, s.set.Ack(ctx, &dst[i]))
	}
	if err := srv.WaitSubscribed(s.Destination(), 2, requeueBudget); err != nil {
		t.Fatalf("the fetch never subscribed again: %v", err)
	}
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("three")}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	d := fetchAll(t, s, 1)
	if err := s.set.Ack(ctx, &d[0]); err != nil {
		t.Errorf("Ack of a delivery from the subscription the retire made: %v", err)
	}
}
