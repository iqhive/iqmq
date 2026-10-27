package jetstream_test

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	sjs "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// TestAsyncPublicationsAReconnectionDroppedAreUnknown holds the asynchronous
// path across a reconnection.
//
// nats.go v1.54.0 forgets every publication outstanding at a disconnection and
// defers their error callbacks inside a loop, so those callbacks ran only when
// the connection closed: the caller's callback, the publication's place in the
// core's completion table and Flush all waited for the publisher's Close, and
// the PubAck timeout could not help because the library had already forgotten
// the publication. A stream created with NoAck keeps every publication
// outstanding, ForceReconnect is the disconnection, and the PubAck timeout is a
// minute, so nothing but the reconnection can be what resolves them.
func TestAsyncPublicationsAReconnectionDroppedAreUnknown(t *testing.T) {
	stream := provisionStream(t, func(cfg *sjs.StreamConfig) { cfg.NoAck = true })
	b := openBackend(t, func(cfg *nq.Config) {
		cfg.Stream, cfg.SubjectPrefix, cfg.ManageStream = stream, strings.ToLower(stream), false
		cfg.PublishTimeout = time.Minute
	})
	c := mq.NewClient(b)
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: newTopic("dropped")})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	const n = 3
	results := make(chan error, n)
	for range n {
		if err := p.PublishAsync(t.Context(), &mq.Message{Body: []byte("dropped")},
			func(res mq.PublishResult) { results <- res.Err }); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
	}
	if err := b.Conn().ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	for i := range n {
		select {
		case err := <-results:
			if mq.CodeOf(err) != mq.CodeSettlementUnknown || !mq.IsAmbiguous(err) {
				t.Errorf("a publication the reconnection dropped reported %v (code %v), want an ambiguous SettlementUnknown",
					err, mq.CodeOf(err))
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%d of %d callbacks ran in 10s after the reconnection: a publication the client library dropped waits for the publisher's Close", i, n)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := p.Flush(ctx); err != nil {
		t.Errorf("Flush after the reconnection: %v; a resolved publication still holds Flush", err)
	}
	if st := p.Stats(); st.Pending != 0 {
		t.Errorf("%d publications still hold a place after their callbacks ran", st.Pending)
	}
}

// TestAsyncPublicationAReconnectionDroppedResolvesOnce holds the callback to
// exactly once across the reports a dropped publication now has: the
// driver's at the reconnection, the client library's own, which it still
// makes when the connection closes, and the sink's at its Close. Whichever
// comes first wins and the others are no-ops; a second call would release a
// caller's pooled message twice.
//
// The connection is closed before the backend, because the backend closes its
// sinks first and a sink's Close would otherwise answer before the library's
// callback could. It is the SPI sink rather than the core publisher, because
// the core absorbs a second call to done and the count here has to see every
// one.
func TestAsyncPublicationAReconnectionDroppedResolvesOnce(t *testing.T) {
	stream := provisionStream(t, func(cfg *sjs.StreamConfig) { cfg.NoAck = true })
	b := openBackend(t, func(cfg *nq.Config) {
		cfg.Stream, cfg.SubjectPrefix, cfg.ManageStream = stream, strings.ToLower(stream), false
		cfg.PublishTimeout = time.Minute
	})
	async := asyncSinkOn(t, b, newTopic("once"))
	const n = 3
	var mu sync.Mutex
	calls := make([]int, n)
	fired := make(chan error, 2*n)
	for i := range n {
		if err := async.PublishAsync(t.Context(), &mq.Message{Body: []byte("once")}, nil, func(err error) {
			mu.Lock()
			calls[i]++
			mu.Unlock()
			fired <- err
		}); err != nil {
			t.Fatalf("PublishAsync %d: %v", i, err)
		}
	}
	if err := b.Conn().ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	for i := range n {
		select {
		case err := <-fired:
			if mq.CodeOf(err) != mq.CodeSettlementUnknown {
				t.Errorf("a publication the reconnection dropped reported %v (code %v), want SettlementUnknown", err, mq.CodeOf(err))
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%d of %d callbacks ran in 10s after the reconnection", i, n)
		}
	}
	// Closing the connection is what makes the library run the callbacks it
	// held back, and its watcher has run them all once it has returned.
	b.Conn().Close()
	waitForPublisherWatchers(t, 10*time.Second)
	onceEach(t, &mu, calls, "the connection closed")
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	onceEach(t, &mu, calls, "the backend and its sink closed")
}

// TestAsyncCallbackResolvedAtAReconnectionMayPublishAgain is the lock half of
// the reconnection's resolution. The core runs a callback inline by default,
// so a callback the reconnection resolves runs on the connection's callback
// goroutine, inside the driver's sweep, and the core lets it publish again. A
// sweep that held the driver's map lock, or ran under the library's publisher
// lock, would wait for ever on that publication.
//
// The second publication is made after the drop, so the library keeps it, and
// it is resolved by its 300ms PubAck timeout rather than by the sweep.
func TestAsyncCallbackResolvedAtAReconnectionMayPublishAgain(t *testing.T) {
	stream := provisionStream(t, func(cfg *sjs.StreamConfig) { cfg.NoAck = true })
	b := openBackend(t, func(cfg *nq.Config) {
		cfg.Stream, cfg.SubjectPrefix, cfg.ManageStream = stream, strings.ToLower(stream), false
		cfg.PublishTimeout = 300 * time.Millisecond
	})
	c := mq.NewClient(b)
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: newTopic("again")})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	firstDone := make(chan error, 1)
	againDone := make(chan error, 1)
	first := func(res mq.PublishResult) {
		firstDone <- res.Err
		if err := p.PublishAsync(t.Context(), &mq.Message{Body: []byte("again")},
			func(res mq.PublishResult) { againDone <- res.Err }); err != nil {
			againDone <- err
		}
	}
	if err := p.PublishAsync(t.Context(), &mq.Message{Body: []byte("dropped")}, first); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	if err := b.Conn().ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	select {
	case err := <-firstDone:
		if mq.CodeOf(err) != mq.CodeSettlementUnknown {
			t.Fatalf("the dropped publication reported %v (code %v), want SettlementUnknown", err, mq.CodeOf(err))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the dropped publication was not resolved at the reconnection")
	}
	select {
	case err := <-againDone:
		if !mq.IsAmbiguous(err) {
			t.Errorf("the publication made from the callback reported %v (code %v), want its ambiguous PubAck timeout",
				err, mq.CodeOf(err))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the publication made from a callback the reconnection resolved never completed: the sweep holds a lock it needs")
	}
}

// onceEach fails for every callback that has not run exactly once.
func onceEach(tb testing.TB, mu *sync.Mutex, calls []int, after string) {
	tb.Helper()
	mu.Lock()
	defer mu.Unlock()
	for i, got := range calls {
		if got != 1 {
			tb.Errorf("after %s the callback for publication %d had run %d times, want exactly once", after, i, got)
		}
	}
}

// TestAsyncPublicationStalledAcrossAReconnectionIsUnknown is the publication
// the reconnection's sweep cannot see: one still inside the client library's
// PublishMsgAsync, stalled on a full window, when the link drops and comes
// back. The library forgets it with the rest, then lets it out when another
// PubAck frees the window, and it never hears of it again.
//
// The window is one publication. The second stalls behind the first; the
// reconnection drops both and the sweep resolves the first; a PubAck from
// another stream on the same JetStream context then frees the window and lets
// the second out.
func TestAsyncPublicationStalledAcrossAReconnectionIsUnknown(t *testing.T) {
	stream := provisionStream(t, func(cfg *sjs.StreamConfig) { cfg.NoAck = true })
	acking := provisionStream(t, nil)
	b := openBackend(t, func(cfg *nq.Config) {
		cfg.Stream, cfg.SubjectPrefix, cfg.ManageStream = stream, strings.ToLower(stream), false
		cfg.PublishTimeout = time.Minute
		cfg.PublishAsyncMaxPending = 1
	})
	async := asyncSinkOn(t, b, newTopic("stalled"))
	first := make(chan error, 1)
	if err := async.PublishAsync(t.Context(), &mq.Message{Body: []byte("first")}, nil,
		func(err error) { first <- err }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	second := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// The library stalls this call for at most 200ms waiting for the
		// window, and refuses it afterwards.
		if err := async.PublishAsync(t.Context(), &mq.Message{Body: []byte("second")}, nil,
			func(err error) { second <- err }); err != nil {
			second <- err
		}
	}()
	defer wg.Wait()
	// The poll is tight because the stall is short: everything from here to
	// the PubAck that frees the window has to fit in it.
	for deadline := time.Now().Add(10 * time.Second); b.AsyncPending() != 2; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the second publication never stalled on the window")
		}
	}
	if err := b.Conn().ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	select {
	case err := <-first:
		if mq.CodeOf(err) != mq.CodeSettlementUnknown {
			t.Fatalf("the first publication reported %v (code %v), want SettlementUnknown", err, mq.CodeOf(err))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the first publication was not resolved at the reconnection")
	}
	fut, err := b.JetStream().PublishMsgAsync(&natsgo.Msg{
		Subject: strings.ToLower(acking) + ".release", Data: []byte("release"),
	})
	if err != nil {
		t.Fatalf("publishing the PubAck that frees the window: %v", err)
	}
	select {
	case <-fut.Ok():
	case err := <-fut.Err():
		t.Fatalf("the publication that frees the window failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the publication that frees the window was never acknowledged")
	}
	select {
	case err := <-second:
		switch {
		case mq.CodeOf(err) == mq.CodeBackpressure:
			t.Skip("the library's 200ms stall ran out before the reconnection was over, so the stalled publication was refused rather than let out")
		case mq.CodeOf(err) != mq.CodeSettlementUnknown:
			t.Fatalf("the stalled publication reported %v (code %v), want SettlementUnknown", err, mq.CodeOf(err))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the publication stalled across the reconnection was never resolved: it waits for the publisher's Close")
	}
}

// TestAsyncPublicationTheLibraryKeptIsLeftToItsPubAck is the other half of
// the reconnection's resolution: it answers only for what the client library
// dropped. A publication made after the drop is one the library still tracks,
// so its outcome is the library's to report, and a sweep that answered it
// with SettlementUnknown would turn a publication the stream acknowledged
// into one the caller retries.
//
// The sweep has to run with such a publication outstanding, which takes two
// reconnections. The callback of the publication the first one dropped holds
// the connection's callback goroutine, so the second reconnection's sweep
// waits behind it. A probe the second disconnection drops shows that the drop
// is over, and the publication made after it, on a NoAck stream so that
// nothing but its own PubAck timeout resolves it, is outstanding when the
// held callback lets the second sweep run.
func TestAsyncPublicationTheLibraryKeptIsLeftToItsPubAck(t *testing.T) {
	stream := provisionStream(t, func(cfg *sjs.StreamConfig) { cfg.NoAck = true })
	b := openBackend(t, func(cfg *nq.Config) {
		cfg.Stream, cfg.SubjectPrefix, cfg.ManageStream = stream, strings.ToLower(stream), false
		cfg.PublishTimeout = 2 * time.Second
	})
	async := asyncSinkOn(t, b, newTopic("kept"))
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()
	dropped := make(chan error, 1)
	if err := async.PublishAsync(t.Context(), &mq.Message{Body: []byte("dropped")}, nil, func(err error) {
		dropped <- err
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
	}); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	if err := b.Conn().ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	select {
	case err := <-dropped:
		if mq.CodeOf(err) != mq.CodeSettlementUnknown {
			t.Fatalf("the dropped publication reported %v (code %v), want SettlementUnknown", err, mq.CodeOf(err))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the dropped publication was not resolved at the reconnection")
	}
	probe, err := b.JetStream().PublishMsgAsync(&natsgo.Msg{
		Subject: strings.ToLower(stream) + ".probe", Data: []byte("probe"),
	})
	if err != nil {
		t.Fatalf("publishing the probe: %v", err)
	}
	if err := b.Conn().ForceReconnect(); err != nil {
		t.Fatalf("the second ForceReconnect: %v", err)
	}
	select {
	case err := <-probe.Err():
		if !errors.Is(err, natsgo.ErrDisconnected) {
			t.Fatalf("the probe reported %v, want the drop's ErrDisconnected", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second disconnection never dropped the probe")
	}
	for deadline := time.Now().Add(10 * time.Second); b.Conn().Status() != natsgo.CONNECTED; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the connection did not come back after the second ForceReconnect")
		}
	}
	kept := make(chan error, 2)
	if err := async.PublishAsync(t.Context(), &mq.Message{Body: []byte("kept")}, nil,
		func(err error) { kept <- err }); err != nil {
		t.Fatalf("PublishAsync after the drop: %v", err)
	}
	releaseOnce()
	select {
	case err := <-kept:
		if mq.CodeOf(err) != mq.CodeDeadlineExceeded {
			t.Fatalf("a publication the library kept across the sweep reported %v (code %v), want its own PubAck timeout (DeadlineExceeded): the sweep answered for a publication the library had not dropped",
				err, mq.CodeOf(err))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the publication made after the drop was never resolved")
	}
}

// asyncSinkOn opens an asynchronous SPI sink on topic, closed on cleanup.
func asyncSinkOn(tb testing.TB, b *nq.Backend, topic string) mq.AsyncSink {
	tb.Helper()
	sink, err := b.OpenSink(tb.Context(), mq.SinkConfig{Topic: topic})
	if err != nil {
		tb.Fatalf("OpenSink: %v", err)
	}
	tb.Cleanup(func() { _ = sink.Close() })
	async, ok := sink.(mq.AsyncSink)
	if !ok {
		tb.Fatal("the sink is not an AsyncSink")
	}
	return async
}

// waitForPublisherWatchers waits until no nats.go asynchronous publisher is
// watching a connection. The watcher is the goroutine that runs the error
// callbacks the library held back, and it runs them as it returns, so once it
// is gone every one of them has run.
func waitForPublisherWatchers(tb testing.TB, budget time.Duration) {
	tb.Helper()
	const watcher = "jetstream.(*jetStream).resetPendingAcksOnReconnect"
	deadline := time.Now().Add(budget)
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n == len(buf) {
			buf = make([]byte, 2*len(buf))
			continue
		}
		if !bytes.Contains(buf[:n], []byte(watcher)) {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("a nats.go publisher watcher was still running %v after its connection closed", budget)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
