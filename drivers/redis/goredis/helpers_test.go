package goredis_test

import (
	"context"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	rd "bitbucket.org/iqhive/iqmq/drivers/redis"
	"bitbucket.org/iqhive/iqmq/drivers/redis/goredis"
	"bitbucket.org/iqhive/iqmq/internal/streams"
	"bitbucket.org/iqhive/iqmq/internal/streams/fakestreams"
	"bitbucket.org/iqhive/iqmq/internal/streams/respsrv"
)

const (
	testTopic = "acme.orders"
	testSub   = "workers"
	testLease = 5 * time.Second
)

// rig is an in-memory Redis, a RESP server in front of it, a go-redis client
// dialled at it, and the driver over that.
//
// It is the rueidis driver's own `rig` with two layers inserted, and the
// insertion is the whole value of this package's tests: the driver, the
// command layer and the server are identical, so anything that behaves
// differently here behaves differently BECAUSE of the client library. The
// fake's clock is still settable and its pending list still inspectable from
// the test, which is what makes the reclaim race a three-line test rather
// than a flaky twenty-second one.
type rig struct {
	fake *fakestreams.Server
	srv  *respsrv.Server
	b    *rd.Backend
	ctx  context.Context
}

func newRig(t testing.TB, opts ...func(*rd.Config)) *rig {
	t.Helper()
	fake, srv := newServer(t, fakestreams.Options{AOF: true, Version: "7.4.2"})
	cfg := baseConfig(fake)
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.Addrs = []string{srv.Addr()}
	ctx := context.Background()
	b, err := goredis.OpenBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return &rig{fake: fake, srv: srv, b: b, ctx: ctx}
}

// newServer starts an in-memory Redis behind a RESP front-end.
func newServer(t testing.TB, opts fakestreams.Options) (*fakestreams.Server, *respsrv.Server) {
	t.Helper()
	fake := fakestreams.New(opts)
	srv, err := respsrv.Serve(fake, respsrv.Options{Version: opts.Version})
	if err != nil {
		t.Fatalf("respsrv.Serve: %v", err)
	}
	t.Cleanup(func() {
		if cerr := srv.Close(); cerr != nil {
			t.Errorf("respsrv.Close: %v", cerr)
		}
		for _, perr := range srv.Errors() {
			t.Errorf("the client broke the protocol: %v", perr)
		}
	})
	return fake, srv
}

// baseConfig is the configuration every test starts from.
func baseConfig(fake *fakestreams.Server) rd.Config {
	return rd.Config{
		AckDeadline:   testLease,
		ClaimInterval: time.Hour, // the tests drive the sweep themselves
		Block:         time.Millisecond,
		AutoCreate:    true,
		Clock:         fakeClock{fake},
	}
}

// fakeClock reads the fake server's clock, so that the driver's notion of now
// and the server's idle times cannot drift apart.
type fakeClock struct{ srv *fakestreams.Server }

func (c fakeClock) Now() time.Time { return c.srv.Now() }

func (c fakeClock) NewTimer(d time.Duration) mq.Timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Stop() bool                 { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// goredisClient dials a go-redis client at addr, closed by the test.
func goredisClient(t testing.TB, addr string) redis.UniversalClient {
	t.Helper()
	c := redis.NewUniversalClient(goredis.UniversalOptions(rd.Config{Addrs: []string{addr}}))
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// goredisCommander is a Commander over a go-redis client at addr.
func goredisCommander(t testing.TB, addr string) streams.Commander {
	t.Helper()
	cmder, err := goredis.NewCommander(goredisClient(t, addr))
	if err != nil {
		t.Fatalf("NewCommander: %v", err)
	}
	return cmder
}

// rueidisCommander is a Commander over a rueidis client at addr, reached
// through `drivers/redis`'s own entry point so that the comparison is between
// the two SHIPPED adapters and not between one shipped adapter and a copy of
// the other written for the test.
func rueidisCommander(t testing.TB, addr string) streams.Commander {
	t.Helper()
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	if err != nil {
		t.Fatalf("rueidis.NewClient: %v", err)
	}
	cmder, err := rd.NewCommander(client)
	if err != nil {
		client.Close()
		t.Fatalf("rd.NewCommander: %v", err)
	}
	t.Cleanup(func() { _ = cmder.Close() })
	return cmder
}

// newRueidisRig is the same driver over rueidis, for the tests that hold the
// two clients to one answer through the DRIVER rather than through the
// command layer.
func newRueidisRig(t testing.TB, opts ...func(*rd.Config)) *rd.Backend {
	t.Helper()
	fake, srv := newServer(t, fakestreams.Options{AOF: true, Version: "7.4.2"})
	cfg := baseConfig(fake)
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.Addrs = []string{srv.Addr()}
	b, err := rd.OpenBackend(context.Background(), cfg)
	if err != nil {
		t.Fatalf("rd.OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// openPair opens a sink and a source on one topic.
func (r *rig) openPair(t testing.TB) (mq.Sink, mq.Source) {
	t.Helper()
	src, err := r.b.OpenSource(r.ctx, mq.SourceConfig{
		Topic:        testTopic,
		Subscription: testSub,
		Prefetch:     16,
		AckDeadline:  testLease,
		Start:        mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	sink, err := r.b.OpenSink(r.ctx, mq.SinkConfig{Topic: testTopic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	return sink, src
}

// fetchOne fetches exactly one delivery, failing if none arrives.
func fetchOne(t testing.TB, ctx context.Context, src mq.Source) mq.DriverDelivery {
	t.Helper()
	dst := make([]mq.DriverDelivery, 1)
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	n, err := src.Fetch(fctx, dst)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if n != 1 {
		t.Fatalf("Fetch returned %d deliveries, want 1", n)
	}
	return dst[0]
}

var _ = driverkit.ClientOwned
