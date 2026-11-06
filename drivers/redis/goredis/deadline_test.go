package goredis_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// TestPublishReturnsAtItsDeadline is a caller's timeout on the publish path.
//
// go-redis applies a context's deadline to the socket only when the client has
// ContextTimeoutEnabled, and the client this driver dialled did not, so a
// Publish whose deadline had passed waited for the reply up to the read
// timeout (Block plus DialTimeout, ten seconds by default) and then reported
// success. The deadline now bounds the read, and a publication whose reply
// did not arrive in time is reported as ambiguous.
func TestPublishReturnsAtItsDeadline(t *testing.T) {
	slow, addr := serveSlowFirstXAdd(t, 1500*time.Millisecond)
	cfg := baseConfig(slow.Server)
	cfg.Block = 0 // the default, so the read timeout is the driver's own
	cfg.Addrs = []string{addr}
	b, err := goredis.OpenBackend(context.Background(), cfg)
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	sink, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: testTopic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	perr := sink.Publish(ctx, &mq.Message{Body: []byte("x")}, nil)
	took := time.Since(start)
	t.Logf("Publish = %v after %v (deadline 100ms)", perr, took)
	if took > 500*time.Millisecond {
		t.Errorf("a Publish with a 100ms deadline returned after %v", took)
	}
	if perr == nil || !mq.IsAmbiguous(perr) {
		t.Errorf("a Publish whose reply missed its deadline returned %v, want an ambiguous failure", perr)
	}
	<-slow.landed
}

// TestFetchReturnsAtItsDeadline is the same bound on the consume path: a Fetch
// parked in XREADGROUP BLOCK returns at its context's deadline rather than
// when the block ends.
func TestFetchReturnsAtItsDeadline(t *testing.T) {
	const block = 2 * time.Second
	r := newRig(t, func(c *rd.Config) { c.Block = block })
	src, err := r.b.OpenSource(r.ctx, mq.SourceConfig{
		Topic: testTopic, Subscription: testSub, Prefetch: 4,
		AckDeadline: testLease, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	n, ferr := src.Fetch(ctx, make([]mq.DriverDelivery, 4))
	took := time.Since(start)
	t.Logf("Fetch = %d, %v after %v (deadline 100ms, block %v)", n, ferr, took, block)
	if took > 500*time.Millisecond {
		t.Errorf("a Fetch with a 100ms deadline returned after %v: it waited out the %v block", took, block)
	}
	if !errors.Is(ferr, context.DeadlineExceeded) {
		t.Errorf("Fetch = %v, want the context's deadline", ferr)
	}
}

// TestShortFetchesKeepTheirConnection is the go-redis arm of the cost a Fetch
// whose deadline ended inside the block used to pay. The deadline is the
// socket's read deadline here, and go-redis drops a connection whose read
// timed out, so every idle pull with a deadline shorter than Config.Block
// dialled again. The read now asks the server to give up before the
// deadline.
func TestShortFetchesKeepTheirConnection(t *testing.T) {
	if v, old := driversRedisBefore(t, "v1.2.0"); old {
		t.Skipf("this build links drivers/redis %s, whose reads use the whole block", v)
	}
	_, srv := newServer(t, fakestreams.Options{AOF: true, Version: "7.4.2"})
	cfg := rd.Config{Addrs: []string{srv.Addr()}, AutoCreate: true}
	opts := goredis.UniversalOptions(cfg)
	var dials atomic.Int32
	opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		dials.Add(1)
		var d net.Dialer
		return d.DialContext(ctx, network, addr)
	}
	b, err := goredis.BackendWithClient(context.Background(), redis.NewUniversalClient(opts), driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	src, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: testTopic, Subscription: testSub, Prefetch: 4, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	fetch := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		n, err := src.Fetch(ctx, make([]mq.DriverDelivery, 4))
		if n != 0 || (!errors.Is(err, context.DeadlineExceeded) && mq.CodeOf(err) != mq.CodeDeadlineExceeded) {
			t.Fatalf("an idle Fetch = %d, %v; want its deadline", n, err)
		}
	}
	fetch()
	before := dials.Load()
	const fetches = 10
	for range fetches {
		fetch()
	}
	if n := dials.Load() - before; n >= fetches/2 {
		t.Errorf("%d idle Fetches with a 200ms deadline (Config.Block %v) dialled %d new connections", fetches, rd.DefaultBlock, n)
	}
}
