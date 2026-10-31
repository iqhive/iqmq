package goredis_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
	"github.com/iqhive/iqmq/internal/streams/respsrv"
)

// slowFirstXAdd is the in-memory Redis with the FIRST XADD held back: the
// server stores the entry, but only after delay, as a server stalled by an
// fsync, a busy script or a slow network would. landed closes once it has.
type slowFirstXAdd struct {
	*fakestreams.Server
	delay  time.Duration
	once   atomic.Bool
	landed chan struct{}
}

func newSlowFirstXAdd(delay time.Duration) *slowFirstXAdd {
	return &slowFirstXAdd{
		Server: fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"}),
		delay:  delay,
		landed: make(chan struct{}),
	}
}

func (s *slowFirstXAdd) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	if len(c.Tokens) == 0 || !strings.EqualFold(c.Tokens[0], "XADD") || !s.once.CompareAndSwap(false, true) {
		return s.Server.Do(ctx, c)
	}
	time.Sleep(s.delay)
	v, err := s.Server.Do(ctx, c)
	close(s.landed)
	return v, err
}

// serveSlowFirstXAdd starts a RESP server in front of a slowFirstXAdd.
func serveSlowFirstXAdd(t *testing.T, delay time.Duration) (*slowFirstXAdd, string) {
	t.Helper()
	slow := newSlowFirstXAdd(delay)
	srv, err := respsrv.Serve(slow, respsrv.Options{Version: "7.4.2"})
	if err != nil {
		t.Fatalf("respsrv.Serve: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return slow, srv.Addr()
}

// clientKind is the go-redis client a case runs through.
type clientKind int

const (
	// borrowedSingle is a single-server client built with go-redis's defaults.
	borrowedSingle clientKind = iota
	// borrowedRing is a Ring of one shard built with go-redis's defaults.
	borrowedRing
	// dialledCluster is the cluster client the driver dials when Config.Addrs
	// has several addresses.
	dialledCluster
)

// retryClient builds the client of kind over addr with a read timeout short
// enough for a held-back reply to miss it. The borrowed kinds keep go-redis's
// defaults (MaxRetries 0 means three retries). The dialled cluster is
// UniversalOptions' own answer for two addresses (MaxRetries -1, MaxRedirects
// left at go-redis's three), told that one node serves every slot because the
// in-memory server does not answer CLUSTER SLOTS.
func retryClient(addr string, kind clientKind) redis.UniversalClient {
	const read = 300 * time.Millisecond
	switch kind {
	case borrowedRing:
		return redis.NewRing(&redis.RingOptions{Addrs: map[string]string{"a": addr}, ReadTimeout: read})
	case dialledCluster:
		opt := goredis.UniversalOptions(rd.Config{Addrs: []string{addr, addr}}).Cluster()
		opt.ReadTimeout = read
		opt.ClusterSlots = func(context.Context) ([]redis.ClusterSlot, error) {
			return []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{Addr: addr}}}}, nil
		}
		return redis.NewClusterClient(opt)
	case borrowedSingle:
	}
	return redis.NewClient(&redis.Options{Addr: addr, ReadTimeout: read})
}

// TestGoRedisDoesNotRetryAWrittenXAdd is a late reply turned into a second
// copy of the message by go-redis's own retries.
//
// go-redis re-sends a command whose reply did not arrive within the read
// timeout, after the server may already have run it, so an XADD whose reply
// was late was written again inside the one Publish call and the caller was
// told "published once". Three clients did it. A client the caller builds
// keeps go-redis's default of three retries, and a Ring retries the same way.
// The cluster client the driver dials itself has MaxRetries -1, and still
// re-sent the command through its redirect loop, which runs up to
// MaxRedirects times whatever MaxRetries says. A plain pipeline (DoMulti,
// which carries settlements and sweeps) was re-sent whole.
//
// A batch and a durable publication did not duplicate, for reasons that belong
// to go-redis rather than to this driver (a pinned connection is not reused
// after a timeout, and a Ring does not retry a pipeline), so those cases are
// here to keep it that way.
//
// The driver now marks every command it sends, on every client, as one
// go-redis must not retry, and leaves the client's own settings alone. A late
// reply is then reported as what it is, a write that may or may not have
// landed.
func TestGoRedisDoesNotRetryAWrittenXAdd(t *testing.T) {
	for _, tc := range []struct {
		name    string
		confirm mq.ConfirmationMode
		batch   int
		kind    clientKind
		multi   bool
	}{
		{name: "Publish", confirm: mq.ConfirmAccepted, batch: 1},
		{name: "PublishBatch", confirm: mq.ConfirmAccepted, batch: 3},
		{name: "Publish under ConfirmDurable", confirm: mq.ConfirmDurable, batch: 1},
		{name: "DoMulti", confirm: mq.ConfirmAccepted, batch: 1, multi: true},
		{name: "Publish through a Ring", confirm: mq.ConfirmAccepted, batch: 1, kind: borrowedRing},
		{name: "PublishBatch through a Ring", confirm: mq.ConfirmAccepted, batch: 3, kind: borrowedRing},
		{name: "Publish through the cluster client the driver dials", confirm: mq.ConfirmAccepted, batch: 1, kind: dialledCluster},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slow, addr := serveSlowFirstXAdd(t, 600*time.Millisecond)
			client := retryClient(addr, tc.kind)
			t.Cleanup(func() { _ = client.Close() })
			b, err := goredis.BackendWithClient(context.Background(), client, driverkit.ClientBorrowed, baseConfig(slow.Server))
			if err != nil {
				t.Fatalf("BackendWithClient: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			sink, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: testTopic, Confirmation: tc.confirm})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			t.Cleanup(func() { _ = sink.Close() })

			keys, err := streams.NewKeys(testTopic, 1)
			if err != nil {
				t.Fatalf("NewKeys: %v", err)
			}
			var perr error
			switch {
			case tc.multi:
				perr = doMultiOneXAdd(t, client, keys.Stream(0))
			case tc.batch == 1:
				perr = sink.Publish(context.Background(), &mq.Message{Body: []byte("once")}, nil)
			default:
				ms := make([]*mq.Message, tc.batch)
				for i := range ms {
					ms[i] = &mq.Message{Body: []byte("once")}
				}
				perr = sink.(mq.BatchSink).PublishBatch(context.Background(), ms, nil)
			}
			select {
			case <-slow.landed:
			case <-time.After(5 * time.Second):
				t.Fatal("the held-back XADD never ran")
			}
			// The rest of a held-back pipeline runs straight after it.
			time.Sleep(100 * time.Millisecond)

			n := len(slow.Entries(keys.Stream(0)))
			t.Logf("result = %v; %d message(s) sent, %d entries in the stream", perr, tc.batch, n)
			if n > tc.batch {
				t.Errorf("%d message(s) went in as %d entries: go-redis sent the write again", tc.batch, n)
			}
			if perr == nil && n != tc.batch {
				t.Errorf("the write was reported as done and the stream holds %d entries for %d message(s)", n, tc.batch)
			}
			// The commander's own errors are go-redis's, not yet mapped to a
			// code, so only a publication's can be asked whether it is ambiguous.
			if perr != nil && !tc.multi && !mq.IsAmbiguous(perr) && n > 0 {
				t.Errorf("the publication was reported as a definite failure (%v) and the stream holds %d entries", perr, n)
			}
		})
	}
}

// doMultiOneXAdd sends one XADD to stream through the commander's DoMulti,
// the path every settlement, sweep and Close takes, and returns its error.
// The commander is not closed: the client is the test's.
func doMultiOneXAdd(t *testing.T, client redis.UniversalClient, stream string) error {
	t.Helper()
	cmder, err := goredis.NewCommander(client)
	if err != nil {
		t.Fatalf("NewCommander: %v", err)
	}
	xadd := streams.XAdd(streams.XAddArgs{Key: stream, ID: streams.IDAuto, Fields: []string{"b", "once"}})
	res, err := cmder.DoMulti(context.Background(), []streams.Cmd{xadd})
	if err != nil {
		return err
	}
	return res[0].Err
}
