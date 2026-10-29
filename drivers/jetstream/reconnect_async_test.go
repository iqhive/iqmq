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
// The client library drops every publication outstanding at a disconnection
// and reports each one with ErrDisconnected, the error its synchronous calls
// return before anything was sent. Read that way it is a definite, retryable
// failure, and a caller that retried it would duplicate any message that had
// reached the stream. A stream created with NoAck keeps every publication
// outstanding, ForceReconnect is the disconnection, and the PubAck timeout is a
// minute, so nothing but the drop can be what resolves them.
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

// TestBatchPublicationsAReconnectionDroppedAreUnknown is the same drop on the
// batch path. PublishBatch publishes asynchronously and waits on the client
// library's own futures, and the drop fails each of those with ErrDisconnected
// too. Read through the error map, that told the caller the batch had failed
// for certain and could be retried while the stream might hold every message,
// so the retry duplicated them. The reconnection is forced once the library
// holds both publications, on a NoAck stream with a minute's PubAck timeout,
// so nothing but the drop can answer them.
func TestBatchPublicationsAReconnectionDroppedAreUnknown(t *testing.T) {
	stream := provisionStream(t, func(cfg *sjs.StreamConfig) { cfg.NoAck = true })
	b := openBackend(t, func(cfg *nq.Config) {
		cfg.Stream, cfg.SubjectPrefix, cfg.ManageStream = stream, strings.ToLower(stream), false
		cfg.PublishTimeout = time.Minute
	})
	sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: newTopic("batchdrop")})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	batch, ok := sink.(mq.BatchSink)
	if !ok {
		t.Fatal("the sink is not a BatchSink")
	}
	const n = 2
	forced := make(chan error, 1)
	go func() {
		for deadline := time.Now().Add(10 * time.Second); b.AsyncPending() < n; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				forced <- errors.New("the batch's publications never became outstanding")
				return
			}
		}
		forced <- b.Conn().ForceReconnect()
	}()
	err = batch.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}}, nil)
	if ferr := <-forced; ferr != nil {
		t.Fatalf("forcing the reconnection: %v", ferr)
	}
	outcomes := []error{err}
	var be *mq.BatchError
	if errors.As(err, &be) {
		outcomes = be.Errs
	}
	for i, got := range outcomes {
		if mq.CodeOf(got) != mq.CodeSettlementUnknown || !mq.IsAmbiguous(got) {
			t.Errorf("outcome %d of a batch the reconnection dropped is %v (code %v, retryable %v), want an ambiguous SettlementUnknown: a retry duplicates whatever the stream already holds",
				i, got, mq.CodeOf(got), mq.IsRetryable(got))
		}
	}
}

// TestAsyncPublicationAReconnectionDroppedResolvesOnce holds the callback to
// exactly once across the reports a dropped publication can have: the client
// library's at the drop and the sink's at its Close. Whichever comes first
// wins and the other is a no-op; a second call would release a caller's pooled
// message twice.
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
	// Once the library's watcher has returned it can report nothing more.
	b.Conn().Close()
	waitForPublisherWatchers(t, 10*time.Second)
	onceEach(t, &mu, calls, "the connection closed")
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	onceEach(t, &mu, calls, "the backend and its sink closed")
}

// TestAsyncCallbackResolvedAtAReconnectionMayPublishAgain is the lock half of
// the drop's resolution. The core runs a callback inline by default, so the
// callback of a dropped publication runs on the client library's goroutine
// that watches the connection, and the core lets it publish again. A callback
// run under the driver's map lock, or under the library's publisher lock,
// would wait for ever on that publication.
//
// The second publication is made after the drop, so the library keeps it, and
// it is resolved by its 300ms PubAck timeout.
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
		t.Fatal("the dropped publication was not resolved at the drop")
	}
	select {
	case err := <-againDone:
		if !mq.IsAmbiguous(err) {
			t.Errorf("the publication made from the callback reported %v (code %v), want its ambiguous PubAck timeout",
				err, mq.CodeOf(err))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the publication made from a dropped publication's callback never completed: the callback runs under a lock it needs")
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

// TestAsyncPublicationStalledAcrossAReconnectionIsLetOut is a publication
// still inside the client library's PublishMsgAsync, stalled on a full window,
// when the link drops. The library takes a publication into the window only
// once it has a place, so the stalled one is not among those the drop
// resolves: the drop empties the window and lets it out, and its own PubAck
// answers it. Answering it SettlementUnknown would have the caller retry a
// message the stream acknowledged.
//
// The window is one publication, filled by a library publication to a NoAck
// stream that never answers. The sink publishes to a stream that does, and
// stalls behind it.
func TestAsyncPublicationStalledAcrossAReconnectionIsLetOut(t *testing.T) {
	filler := provisionStream(t, func(cfg *sjs.StreamConfig) { cfg.NoAck = true })
	acking := provisionStream(t, nil)
	b := openBackend(t, func(cfg *nq.Config) {
		cfg.Stream, cfg.SubjectPrefix, cfg.ManageStream = acking, strings.ToLower(acking), false
		cfg.PublishTimeout = time.Minute
		cfg.PublishAsyncMaxPending = 1
	})
	async := asyncSinkOn(t, b, newTopic("stalled"))
	fill, err := b.JetStream().PublishMsgAsync(&natsgo.Msg{
		Subject: strings.ToLower(filler) + ".fill", Data: []byte("fill"),
	})
	if err != nil {
		t.Fatalf("filling the window: %v", err)
	}
	stalled := make(chan error, 2)
	returned := make(chan error, 1)
	go func() {
		// The library stalls this call for at most 200ms waiting for the
		// window, and refuses it afterwards.
		returned <- async.PublishAsync(t.Context(), &mq.Message{Body: []byte("stalled")}, nil,
			func(err error) { stalled <- err })
	}()
	// Nothing outside the library marks the stall, so the call is given a
	// moment to reach it. One that has returned already was refused.
	select {
	case err := <-returned:
		t.Skipf("the stalled publication returned before the disconnection: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := b.Conn().ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	select {
	case err := <-fill.Err():
		if !errors.Is(err, natsgo.ErrDisconnected) {
			t.Fatalf("the publication filling the window reported %v, want the drop's ErrDisconnected", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the disconnection never dropped the publication filling the window")
	}
	select {
	case err := <-returned:
		switch {
		case mq.CodeOf(err) == mq.CodeBackpressure:
			t.Skip("the library's 200ms stall ran out before the drop emptied the window, so the stalled publication was refused rather than let out")
		case err != nil:
			t.Fatalf("the stalled PublishAsync returned %v after the drop let it out", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the stalled PublishAsync never returned after the drop emptied the window")
	}
	select {
	case err := <-stalled:
		if err != nil {
			t.Fatalf("the publication let out of the stall reported %v (code %v), want its PubAck", err, mq.CodeOf(err))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the publication let out of the stall was never resolved")
	}
}

// TestAsyncPublicationTheLibraryKeptIsLeftToItsPubAck is the other half of
// the drop's resolution: SettlementUnknown answers only what the client
// library dropped. A publication made after the drop is one the library still
// tracks, buffered across the outage while the link is down, so its outcome is
// its own PubAck's. Answering it SettlementUnknown at the reconnection would
// turn a publication the stream acknowledged into one the caller retries. It
// goes to a NoAck stream, so nothing but its own PubAck timeout resolves it.
func TestAsyncPublicationTheLibraryKeptIsLeftToItsPubAck(t *testing.T) {
	stream := provisionStream(t, func(cfg *sjs.StreamConfig) { cfg.NoAck = true })
	b := openBackend(t, func(cfg *nq.Config) {
		cfg.Stream, cfg.SubjectPrefix, cfg.ManageStream = stream, strings.ToLower(stream), false
		cfg.PublishTimeout = 2 * time.Second
	})
	async := asyncSinkOn(t, b, newTopic("kept"))
	dropped := make(chan error, 1)
	if err := async.PublishAsync(t.Context(), &mq.Message{Body: []byte("dropped")}, nil,
		func(err error) { dropped <- err }); err != nil {
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
		t.Fatal("the dropped publication was not resolved at the drop")
	}
	kept := make(chan error, 2)
	if err := async.PublishAsync(t.Context(), &mq.Message{Body: []byte("kept")}, nil,
		func(err error) { kept <- err }); err != nil {
		t.Fatalf("PublishAsync after the drop: %v", err)
	}
	for deadline := time.Now().Add(10 * time.Second); b.Conn().Status() != natsgo.CONNECTED; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the connection did not come back after ForceReconnect")
		}
	}
	select {
	case err := <-kept:
		if mq.CodeOf(err) != mq.CodeDeadlineExceeded {
			t.Fatalf("a publication the library kept across the reconnection reported %v (code %v), want its own PubAck timeout (DeadlineExceeded)",
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
// watching a connection. The watcher is the goroutine that drops a
// disconnection's publications and runs their error callbacks, so once it is
// gone the library reports nothing more.
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
