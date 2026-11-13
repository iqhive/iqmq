package goredis_test

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	redis "github.com/redis/go-redis/v9"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
	"github.com/iqhive/iqmq/internal/streams/respsrv"
)

// replicaNode is the in-memory Redis acting as one cluster node. It answers
// READONLY, which go-redis sends to every node of a cluster client with
// ReadOnly set, and records the verbs it was sent.
type replicaNode struct {
	*verbRecorder
}

func (r *replicaNode) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	if strings.EqualFold(c.Name(), "READONLY") {
		return streams.Str("OK"), nil
	}
	return r.verbRecorder.Do(ctx, c)
}

// readOnlyTable answers go-redis's COMMAND as Redis 7.4's command table does
// for the verbs the driver sends (S/hunt8fix/rd/cmdflags-74.txt), because the
// in-memory server lists no commands and go-redis then sends nothing to a
// replica.
type readOnlyTable struct{}

func (readOnlyTable) DialHook(next redis.DialHook) redis.DialHook { return next }

func (readOnlyTable) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if c, ok := cmd.(*redis.CommandsInfoCmd); ok {
			table := map[string]*redis.CommandInfo{}
			for _, name := range []string{"exists", "get", "scan", "xlen", "xpending", "xrange", "zcard", "zrangebyscore"} {
				table[name] = &redis.CommandInfo{Name: name, ReadOnly: true}
			}
			for _, name := range []string{
				"config", "del", "eval", "evalsha", "info", "ping", "script", "waitaof", "xack", "xadd",
				"xautoclaim", "xclaim", "xdel", "xgroup", "xinfo", "xreadgroup", "xtrim", "zadd",
			} {
				table[name] = &redis.CommandInfo{Name: name}
			}
			c.SetVal(table)
			return nil
		}
		return next(ctx, cmd)
	}
}

func (readOnlyTable) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestAReadOnlyClusterClientDoesNotReadTheDriversStateFromAReplica: over a
// cluster client with ReadOnly set (or RouteByLatency or RouteRandomly, which
// set it), go-redis sends a pipeline of read-only commands to a replica. The
// driver's reads are of state it has just written, so they go to the master.
//
// A source's Close checks that its consumer owns nothing pending, with an
// XPENDING-only pipeline, before XGROUP DELCONSUMER, which deletes whatever
// the consumer still owns. The check went to a replica, and a replica behind
// its master saw nothing pending, so DELCONSUMER on the master deleted the
// entry this consumer held, and the message was never delivered again to the
// subscription. Here the replica has the stream and the group and none of
// the master's deliveries, as one that is behind does.
func TestAReadOnlyClusterClientDoesNotReadTheDriversStateFromAReplica(t *testing.T) {
	serve := func(cmder streams.Commander) string {
		srv, err := respsrv.Serve(cmder, respsrv.Options{Version: "7.4.2"})
		if err != nil {
			t.Fatalf("respsrv.Serve: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		return srv.Addr()
	}
	master := &replicaNode{&verbRecorder{Server: fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})}}
	replica := &replicaNode{&verbRecorder{Server: fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})}}
	masterAddr, replicaAddr := serve(master), serve(replica)
	opt := goredis.UniversalOptions(rd.Config{Addrs: []string{masterAddr}}).Cluster()
	opt.ReadOnly = true
	opt.ClusterSlots = func(context.Context) ([]redis.ClusterSlot, error) {
		return []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{Addr: masterAddr}, {Addr: replicaAddr}}}}, nil
	}
	cluster := redis.NewClusterClient(opt)
	cluster.OnNewNode(func(node *redis.Client) { node.AddHook(readOnlyTable{}) })
	t.Cleanup(func() { _ = cluster.Close() })
	cfg := baseConfig(master.Server)
	cfg.Nack = streams.LeavePending
	b, err := goredis.BackendWithClient(t.Context(), cluster, driverkit.ClientBorrowed, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: testTopic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	src, err := b.OpenSource(t.Context(), mq.SourceConfig{Topic: testTopic, Subscription: testSub, Prefetch: 1, Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatal(err)
	}
	group, err := streams.Group(testSub)
	if err != nil {
		t.Fatal(err)
	}
	// The replica is behind: it has the stream and the group, and none of
	// what the master has delivered.
	if _, err := replica.Server.Do(t.Context(), streams.XGroupCreate(keys.Stream(0), group, streams.IDZero, true)); err != nil {
		t.Fatalf("XGROUP CREATE on the replica: %v", err)
	}
	if err := sink.Publish(t.Context(), &mq.Message{Topic: testTopic, Body: []byte("held")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, t.Context(), src)
	// A requeue under LeavePending forgets the delivery here and leaves the
	// entry pending for this consumer, so only the server says it is held.
	if err := src.Settler().Nack(t.Context(), &d, mq.NackOptions{Action: mq.NackActionRequeue}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	cerr := src.Close()
	if n := len(master.Pending(keys.Stream(0), group)); n != 1 || errors.Is(cerr, streams.ErrConsumerHadPending) {
		t.Errorf("Close = %v, and the master holds %d pending entries, want the 1 this consumer held: its "+
			"emptiness check read the replica, which was behind (replica sent %v), and DELCONSUMER on the master "+
			"deleted the entry", cerr, n, replica.sent())
	}
	if n := replica.count("XPENDING") + replica.count("XINFO"); n != 0 {
		t.Errorf("the replica was sent %d of the driver's reads (%v), want none", n, replica.sent())
	}
}

// readOnlyMoved is a movedMaster that answers READONLY, as every node of a
// cluster client with ReadOnly set is sent it.
type readOnlyMoved struct {
	*movedMaster
}

func (m readOnlyMoved) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	if strings.EqualFold(c.Name(), "READONLY") {
		return streams.Str("OK"), nil
	}
	return m.movedMaster.Do(ctx, c)
}

// TestAReadOnlyClusterClientFollowsARedirectForAWrite: over a cluster client
// with ReadOnly set, a publication to a slot that has just moved reaches the
// new master at the first attempt, because the cluster client follows the
// MOVED reply, as it does over a client without ReadOnly. Only the driver's
// reads go to a master's own client (TestAReadOnlyClusterClientDoesNotReadTheDriversStateFromAReplica).
//
// The first version of that fix sent every command of such a client to the
// master's own client, where nothing follows a redirect, so while a cluster
// moved a topic's slot every publication and settlement for it failed with
// ASK, and with MOVED after, until the client reloaded its map (measured on a
// two-master 7.4 cluster).
func TestAReadOnlyClusterClientFollowsARedirectForAWrite(t *testing.T) {
	serve := func(cmder streams.Commander) string {
		srv, err := respsrv.Serve(cmder, respsrv.Options{Version: "7.4.2"})
		if err != nil {
			t.Fatalf("respsrv.Serve: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		return srv.Addr()
	}
	next := &replicaNode{&verbRecorder{Server: fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})}}
	nextAddr := serve(next)
	old := &movedMaster{verbRecorder: &verbRecorder{Server: fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})}, to: nextAddr}
	oldAddr := serve(readOnlyMoved{old})
	var owner atomic.Value
	owner.Store(oldAddr)
	opt := goredis.UniversalOptions(rd.Config{Addrs: []string{oldAddr, nextAddr}}).Cluster()
	opt.ReadOnly = true
	opt.ClusterSlots = func(context.Context) ([]redis.ClusterSlot, error) {
		addr, _ := owner.Load().(string)
		return []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{Addr: addr}}}}, nil
	}
	cluster := redis.NewClusterClient(opt)
	cluster.OnNewNode(func(node *redis.Client) { node.AddHook(readOnlyTable{}) })
	t.Cleanup(func() { _ = cluster.Close() })
	b, err := goredis.BackendWithClient(t.Context(), cluster, driverkit.ClientBorrowed, baseConfig(old.Server))
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: testTopic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	if err := sink.Publish(t.Context(), &mq.Message{Body: []byte("before")}, nil); err != nil {
		t.Fatalf("Publish before the slot moved: %v", err)
	}
	owner.Store(nextAddr)
	old.moved.Store(true)
	if err := sink.Publish(t.Context(), &mq.Message{Body: []byte("after")}, nil); err != nil {
		t.Errorf("the first Publish after the slot moved = %v: the cluster client follows MOVED, and the driver's "+
			"write did not go through it", err)
	}
	if n := next.count("XADD"); n != 1 {
		t.Errorf("the new master took %d XADDs, want the one the redirect sent it", n)
	}
}

// TestTheReplicaReadsAreTheServersReadOnlyCommands holds replicaRead to a
// server's command table, which is what go-redis routes on: of the verbs the
// driver sends, every one the table marks readonly is moved to the master,
// and none it marks write is. A read missing from the list would be read from
// a replica again; a write in it would lose the cluster client's redirects. A
// verb with neither flag (a container such as XINFO from 7.0, or INFO) may be
// either. It needs a server: IQMQ_REDIS_ADDR. On 6.2 it found XINFO, which
// that version marks readonly and the first list left out.
func TestTheReplicaReadsAreTheServersReadOnlyCommands(t *testing.T) {
	addr := os.Getenv("IQMQ_REDIS_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_REDIS_ADDR to hold the list to a server's command table")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	table, err := c.Command(t.Context()).Result()
	if err != nil {
		t.Fatalf("COMMAND: %v", err)
	}
	// Every verb internal/streams and drivers/redis build.
	for _, verb := range []string{
		"CONFIG", "DEL", "EVAL", "EVALSHA", "EXISTS", "GET", "INFO", "PING", "SCAN", "SCRIPT", "WAITAOF", "XACK",
		"XADD", "XAUTOCLAIM", "XCLAIM", "XDEL", "XGROUP", "XINFO", "XLEN", "XPENDING", "XRANGE", "XREADGROUP",
		"XTRIM", "ZADD", "ZCARD", "ZRANGEBYSCORE",
	} {
		info := table[strings.ToLower(verb)]
		if info == nil {
			// WAITAOF before 7.2: the driver does not send it there.
			t.Logf("the server's command table has no %s", verb)
			continue
		}
		got := goredis.ReplicaReadForTest(verb)
		switch {
		case info.ReadOnly && !got:
			t.Errorf("%s is readonly in the server's table, so go-redis sends it to a replica, and replicaRead "+
				"does not move it to the master", verb)
		case got && slices.Contains(info.Flags, "write"):
			t.Errorf("%s is a write in the server's table (%v), and replicaRead moves it off the cluster client, "+
				"which follows MOVED and ASK", verb, info.Flags)
		}
	}
}
