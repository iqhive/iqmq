package goredis_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
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

// TestDoPinnedRunsTheWaitWithItsWrites is the commander half of the durable
// boundary: the driver hands a durable publication's XADD and WAITAOF to
// DoPinned as one call, because WAITAOF counts the writes of the connection
// it runs on, and the call has to answer both, in order.
func TestDoPinnedRunsTheWaitWithItsWrites(t *testing.T) {
	_, srv := newServer(t, fakestreams.Options{AOF: true, Version: "7.4.2"})
	p, ok := goredisCommander(t, srv.Addr()).(interface {
		DoPinned(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error)
	})
	if !ok {
		t.Fatal("the go-redis commander cannot pin a connection, so a durable publication's WAITAOF may run on one that wrote nothing")
	}
	wait, err := streams.WaitAOF(1, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	xadd := streams.XAdd(streams.XAddArgs{Key: "{pinned}", ID: streams.IDAuto, Fields: []string{"f", "v"}})
	res, err := p.DoPinned(context.Background(), []streams.Cmd{xadd, wait})
	if err != nil || len(res) != 2 {
		t.Fatalf("DoPinned = (%d results, %v), want 2 and nil", len(res), err)
	}
	if res[0].Err != nil || streams.CheckXAdd(res[0].Value) != nil {
		t.Fatalf("the XADD answered %v / %v", res[0].Err, streams.CheckXAdd(res[0].Value))
	}
	ack, err := streams.ParseWaitAOF(res[1].Value)
	if res[1].Err != nil || err != nil || !ack.Reached(1, 0) {
		t.Fatalf("the WAITAOF answered %+v / %v / %v, want the local append-only file reached", ack, res[1].Err, err)
	}
}

// verbRecorder is one ring shard: the in-memory Redis, recording the verbs it
// was sent.
type verbRecorder struct {
	*fakestreams.Server
	mu    sync.Mutex
	verbs []string
}

func (r *verbRecorder) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	if len(c.Tokens) > 0 {
		r.mu.Lock()
		r.verbs = append(r.verbs, strings.ToUpper(c.Tokens[0]))
		r.mu.Unlock()
	}
	return r.Server.Do(ctx, c)
}

func (r *verbRecorder) count(verb string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, v := range r.verbs {
		if v == verb {
			n++
		}
	}
	return n
}

// serveRing starts two in-memory Redis servers, each recording what it was
// sent, and a go-redis Ring over them, closed by the test.
func serveRing(t *testing.T) (*redis.Ring, map[string]*verbRecorder) {
	t.Helper()
	shards := map[string]*verbRecorder{}
	addrs := map[string]string{}
	for _, name := range []string{"a", "b"} {
		rec := &verbRecorder{Server: fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})}
		srv, err := respsrv.Serve(rec, respsrv.Options{Version: "7.4.2"})
		if err != nil {
			t.Fatalf("respsrv.Serve: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		shards[name], addrs[name] = rec, srv.Addr()
	}
	ring := redis.NewRing(&redis.RingOptions{Addrs: addrs, MaxRetries: -1})
	t.Cleanup(func() { _ = ring.Close() })
	return ring, shards
}

// TestADurablePublicationOnARingWaitsOnTheShardThatWroteIt is the durable
// boundary on a client that shards by key without a cluster.
//
// WAITAOF names no key, and go-redis's Ring routes a command it cannot find a
// key in by its first argument, here the local count "1", as if that were a
// key. DoPinned used to hand a Ring the plain pipeline it already had, so for
// every topic whose hash tag lives on the other shard, the WAITAOF ran on a
// server that wrote nothing, returned at once, and the publication was
// reported durable without the shard that holds it ever being asked.
func TestADurablePublicationOnARingWaitsOnTheShardThatWroteIt(t *testing.T) {
	ring, shards := serveRing(t)
	b, err := goredis.BackendWithClient(context.Background(), ring, driverkit.ClientBorrowed, baseConfig(shards["a"].Server))
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	// Eight topics, so that both shards hold some and the test does not
	// depend on where any one topic hashes.
	used := map[string]bool{}
	for i := range 8 {
		topic := "ring.topic." + strconv.Itoa(i)
		sink, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: topic, Confirmation: mq.ConfirmDurable})
		if err != nil {
			t.Fatalf("OpenSink(%s, ConfirmDurable): %v", topic, err)
		}
		before := map[string][2]int{}
		for name, rec := range shards {
			before[name] = [2]int{rec.count("XADD"), rec.count("WAITAOF")}
		}
		perr := sink.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
		_ = sink.Close()
		if perr != nil {
			t.Fatalf("%s: Publish(ConfirmDurable): %v", topic, perr)
		}
		var wrote, waited string
		for name, rec := range shards {
			if rec.count("XADD") > before[name][0] {
				wrote = name
			}
			if rec.count("WAITAOF") > before[name][1] {
				waited = name
			}
		}
		used[wrote] = true
		if wrote != waited {
			t.Errorf("%s: the XADD went to shard %q and the WAITAOF that confirmed it to shard %q", topic, wrote, waited)
		}
	}
	if len(used) != 2 {
		t.Fatalf("every topic hashed to one shard (%v), so the test proved nothing", used)
	}
}

// movedMaster is a cluster master that has handed its slots to another node:
// once moved is set it answers every XADD with MOVED, as the old owner of a
// slot does after a resharding or a failover.
type movedMaster struct {
	*verbRecorder
	to    string
	moved atomic.Bool
}

func (m *movedMaster) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	if m.moved.Load() && len(c.Tokens) > 0 && strings.EqualFold(c.Tokens[0], "XADD") {
		return streams.Nil(), errors.New("MOVED 0 " + m.to)
	}
	return m.verbRecorder.Do(ctx, c)
}

// movedCluster is a go-redis cluster client over two in-memory masters, with
// UniversalOptions' own settings, whose every slot is on the first until move
// is called and on the second after it. It returns the backend over that
// client, move, and the second master.
func movedCluster(t *testing.T) (*rd.Backend, func(), *verbRecorder) {
	t.Helper()
	serve := func(cmder streams.Commander) string {
		srv, err := respsrv.Serve(cmder, respsrv.Options{Version: "7.4.2"})
		if err != nil {
			t.Fatalf("respsrv.Serve: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		return srv.Addr()
	}
	next := &verbRecorder{Server: fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})}
	nextAddr := serve(next)
	old := &movedMaster{verbRecorder: &verbRecorder{Server: fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})}, to: nextAddr}
	oldAddr := serve(old)
	var owner atomic.Value
	owner.Store(oldAddr)
	opt := goredis.UniversalOptions(rd.Config{Addrs: []string{oldAddr, nextAddr}}).Cluster()
	opt.ClusterSlots = func(context.Context) ([]redis.ClusterSlot, error) {
		addr, _ := owner.Load().(string)
		return []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{Addr: addr}}}}, nil
	}
	b, err := goredis.BackendWithClient(context.Background(), redis.NewClusterClient(opt), driverkit.ClientOwned, baseConfig(old.Server))
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	move := func() {
		owner.Store(nextAddr)
		old.moved.Store(true)
	}
	return b, move, next
}

// TestAPinnedPublicationFindsTheNewMasterAfterASlotMoves is a durable
// publication and a batch on a cluster whose slot has just moved.
//
// DoPinned looks the master up with ClusterClient.MasterForKey, which reads
// the client's cached slot map, and runs the pipeline on that node's own
// connection, where nothing follows a MOVED reply or tells the client its map
// is stale. Every durable publication and every batch to the moved slot then
// failed with MOVED until the client's periodic reload, 60 seconds by default.
// rueidis refreshes its map on MOVED, so only its first attempt fails, and
// that is the behaviour wanted here: the attempt that met MOVED wrote nothing
// and fails as retryable, and the next one reaches the new master.
func TestAPinnedPublicationFindsTheNewMasterAfterASlotMoves(t *testing.T) {
	for _, tc := range []struct {
		name    string
		confirm mq.ConfirmationMode
		publish func(mq.Sink) error
	}{
		{"durable Publish", mq.ConfirmDurable, func(s mq.Sink) error {
			return s.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
		}},
		{"PublishBatch", mq.ConfirmAccepted, func(s mq.Sink) error {
			return s.(mq.BatchSink).PublishBatch(context.Background(), []*mq.Message{{Body: []byte("b1")}, {Body: []byte("b2")}}, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, move, next := movedCluster(t)
			sink, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: testTopic, Confirmation: tc.confirm})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			t.Cleanup(func() { _ = sink.Close() })
			if err := tc.publish(sink); err != nil {
				t.Fatalf("before the slot moved: %v", err)
			}
			move()
			var failed []error
			deadline := time.Now().Add(2 * time.Second)
			for {
				err := tc.publish(sink)
				if err == nil {
					break
				}
				if !mq.IsRetryable(err) {
					t.Fatalf("an attempt that met MOVED wrote nothing, and it failed as not retryable: %v", err)
				}
				failed = append(failed, err)
				if time.Now().After(deadline) {
					t.Fatalf("after the slot moved, %d attempts over 2s failed (the first with %v): "+
						"the pinned path never told the client its slot map was stale", len(failed), failed[0])
				}
				time.Sleep(20 * time.Millisecond)
			}
			if next.count("XADD") == 0 {
				t.Fatal("the publication succeeded and the new master took no XADD")
			}
			t.Logf("%d attempt(s) failed after the move before one reached the new master", len(failed))
		})
	}
}
