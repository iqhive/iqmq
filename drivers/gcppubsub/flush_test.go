package gcppubsub_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/pstest"
	"google.golang.org/api/option"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psfake"
)

// The driver's Flush, against the two things the client library's Flush does
// not give it: a context, and safety beside the publisher's Stop.

// fakeSink opens one sink straight over the scripted fake, below the core, so
// that a test can call the driver's own Flush.
func fakeSink(t *testing.T, srv *psfake.Server, cfg gps.Config) mq.Sink {
	t.Helper()
	b, err := gps.BackendWithAPI(context.Background(), srv, driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: newTopic("flush")})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = snk.Close() })
	return snk
}

// TestAFlushWhoseContextEndedBeforeTheCallAnswersOneWay: the library's Flush
// takes no context, so the driver runs it on a goroutine and selects between
// that goroutine and ctx.Done. With a context that ended before the call both
// can be ready, on an idle publisher, and the select picked one at random: the
// same call answered nil or the context's error (hunt 8 cs8-1). It now answers
// the context's error every time, as the other drivers whose flush reads an
// ended context do, and still starts the library's flush, so an ended Flush
// still sends.
func TestAFlushWhoseContextEndedBeforeTheCallAnswersOneWay(t *testing.T) {
	srv := psfake.New("iqmq-test")
	snk := fakeSink(t, srv, gps.Config{ProjectID: "iqmq-test", AutoCreate: true})
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("warm")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	fs := snk.(mq.FlushSink)
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	// The nil answer needs the flush goroutine to finish before the caller
	// reaches its select, which is rare: 1 to 30 in 200,000 calls before the
	// fix.
	const calls = 200000
	var nilN, otherN int
	var other error
	for range calls {
		switch err := fs.Flush(ended); {
		case err == nil:
			nilN++
		case !errors.Is(err, context.Canceled):
			otherN++
			other = err
		}
	}
	if nilN != 0 || otherN != 0 {
		t.Fatalf("%d Flush calls with a context that ended before the call, nothing to send: %d answered nil "+
			"and %d something else (%v); want the context's error every time", calls, nilN, otherN, other)
	}
}

// TestCloseWaitsForAFlushThatOutlivedItsContext: a Flush that returns on its
// context leaves its goroutine in the library's Flush, which reads the
// publisher's stopped flag without the lock Stop writes it under. The sink's
// Close called Stop at once, beside that Flush, which is a data race inside
// the client library, reached by every Client.Shutdown whose bound ends while
// the library is still sending (hunt 8 cs8-2). Close now waits for the flushes
// it owns before it stops the publisher; Stop flushes anyway, so the wait
// costs it nothing.
//
// The fake counts a Stop made while a Flush runs, which is what lets this test
// see the overlap without -race.
func TestCloseWaitsForAFlushThatOutlivedItsContext(t *testing.T) {
	srv := psfake.New("iqmq-test")
	snk := fakeSink(t, srv, gps.Config{ProjectID: "iqmq-test", AutoCreate: true})
	srv.GatePublishResults()
	sent := make(chan error, 1)
	if err := snk.(mq.AsyncSink).PublishAsync(context.Background(), &mq.Message{Body: []byte("held")}, nil,
		func(err error) { sent <- err }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := snk.(mq.FlushSink).Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Flush = %v, want the deadline: the library is holding the publication", err)
	}
	var wg sync.WaitGroup
	var closeErr error
	wg.Go(func() { closeErr = snk.Close() })
	// A Close that stops the publisher at once does it within this wait. One
	// that waits for the flush cannot stop it before the publication is sent.
	waitUntil(200*time.Millisecond, func() bool { return srv.StopsInFlush() > 0 })
	srv.ReleasePublishResults()
	wg.Wait()
	if closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	if err := <-sent; err != nil {
		t.Fatalf("the held publication failed: %v", err)
	}
	if n := srv.StopsInFlush(); n != 0 {
		t.Fatalf("Close stopped the publisher while the Flush that outlived its context was still in the library's "+
			"Flush (%d time(s)): the library reads its stopped flag there without a lock, a data race", n)
	}
}

// slowPublish holds every Publish RPC at the pstest server, so that the
// library's flush is still sending when the test moves on.
type slowPublish struct{ d time.Duration }

// React delays the RPC and lets pstest answer it.
func (s slowPublish) React(any) (bool, any, error) {
	time.Sleep(s.d)
	return false, nil, nil
}

// TestCloseWaitsForAFlushThatOutlivedItsContextOnTheLibrary is the same over
// the real client library, below the core and through Client.Shutdown, whose
// bound ends while the flush step is still sending. It fails under -race only:
// the defect is the data race in the library's Flush and Stop.
func TestCloseWaitsForAFlushThatOutlivedItsContextOnTheLibrary(t *testing.T) {
	held := func(cfg *gps.Config) {
		cfg.PublishCountThreshold = 1000
		cfg.PublishDelayThreshold = time.Hour
	}
	t.Run("sink", func(t *testing.T) {
		l := newLive(t, pstest.ServerReactorOption{FuncName: "Publish", Reactor: slowPublish{d: 100 * time.Millisecond}})
		cfg := gps.Config{ProjectID: "iqmq-test", AutoCreate: true}
		held(&cfg)
		client, err := pubsub.NewClient(context.Background(), cfg.ProjectID, option.WithGRPCConn(l.conn))
		if err != nil {
			t.Fatalf("pubsub.NewClient: %v", err)
		}
		b, err := gps.BackendWithClient(context.Background(), client, driverkit.ClientOwned, cfg)
		if err != nil {
			t.Fatalf("BackendWithClient: %v", err)
		}
		defer func() { _ = b.Close() }()
		snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: newTopic("flushlib")})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		sent := make(chan error, 1)
		if err := snk.(mq.AsyncSink).PublishAsync(context.Background(), &mq.Message{Body: []byte("held")}, nil,
			func(err error) { sent <- err }); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if err := snk.(mq.FlushSink).Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Flush = %v, want the deadline: the server holds the publish RPC for 100 ms", err)
		}
		if err := snk.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := <-sent; err != nil {
			t.Fatalf("the held publication failed: %v", err)
		}
	})
	t.Run("Shutdown", func(t *testing.T) {
		l := newLive(t, pstest.ServerReactorOption{FuncName: "Publish", Reactor: slowPublish{d: 100 * time.Millisecond}})
		c := l.open(t, held)
		topic := newTopic("shutlib")
		provision(t, c, topic, "s")
		p, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: topic})
		if err != nil {
			t.Fatalf("Publisher: %v", err)
		}
		if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("held")}, nil); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if err := c.Shutdown(ctx); err == nil {
			t.Log("Shutdown finished inside its bound; the race needs the flush step to outlive it")
		}
	})
}

// TestAFlushBesideTheFirstPublicationLeavesTheLibraryAlone: the library
// builds its bundler on a publisher's first Publish, under its lock, and its
// Flush reads the bundler without that lock. A Flush made beside the first
// publication of a sink is therefore a data race inside the library as well.
// The driver now leaves the library's Flush alone until a publication has
// been handed to it, which is also the only time there is anything to flush.
// It fails under -race only.
func TestAFlushBesideTheFirstPublicationLeavesTheLibraryAlone(t *testing.T) {
	for range 20 {
		l := newLive(t)
		cfg := gps.Config{ProjectID: "iqmq-test", AutoCreate: true}
		client, err := pubsub.NewClient(context.Background(), cfg.ProjectID, option.WithGRPCConn(l.conn))
		if err != nil {
			t.Fatalf("pubsub.NewClient: %v", err)
		}
		b, err := gps.BackendWithClient(context.Background(), client, driverkit.ClientOwned, cfg)
		if err != nil {
			t.Fatalf("BackendWithClient: %v", err)
		}
		snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: newTopic("firstflush")})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		var wg sync.WaitGroup
		var publishErr, flushErr error
		wg.Go(func() { publishErr = snk.Publish(context.Background(), &mq.Message{Body: []byte("first")}, nil) })
		wg.Go(func() { flushErr = snk.(mq.FlushSink).Flush(context.Background()) })
		wg.Wait()
		if publishErr != nil || flushErr != nil {
			t.Fatalf("Publish = %v, Flush = %v", publishErr, flushErr)
		}
		_ = snk.Close()
		_ = b.Close()
	}
}
