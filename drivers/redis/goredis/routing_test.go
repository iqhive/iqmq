package goredis_test

import (
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// TestKeyRoutingIsDecidedByTheCommandsKeyAndNotAtRandom is the cluster-safety
// property, proved on the one go-redis client that shards by key WITHOUT a
// cluster: a Ring.
//
// go-redis routes a command built through `Do` by looking its key POSITION up
// in the server's command table, which knows nothing about a command sent that
// way, and falls back to `hashtag.RandomSlot()` — a random node. On a
// standalone server that is correct by accident; on a cluster or a ring every
// command this driver sends goes somewhere arbitrary, and a stream written on
// one node is then invisible from another. `build` states the position from
// [streams.Cmd]'s key span, and a Ring is what makes the difference
// observable in a unit test: it hashes the first key to pick a shard, so a
// key that routed at random would spread one stream over two servers.
//
// XREADGROUP is the case that matters most, and the reason the span is a span:
// its keys sit between STREAMS and the identifiers, so nothing about the verb
// or the first argument locates them.
func TestKeyRoutingIsDecidedByTheCommandsKeyAndNotAtRandom(t *testing.T) {
	fakeA, srvA := newServer(t, fakestreams.Options{})
	fakeB, srvB := newServer(t, fakestreams.Options{})
	ring := redis.NewRing(&redis.RingOptions{
		Addrs:      map[string]string{"a": srvA.Addr(), "b": srvB.Addr()},
		MaxRetries: -1,
		// The heartbeat is a background PING per shard. It is left on because
		// turning it off would be testing a client this driver does not
		// build; it stops with Close, which TestMain's goleak checks.
		HeartbeatFrequency: time.Second,
	})
	t.Cleanup(func() { _ = ring.Close() })
	cmder, err := goredis.NewCommander(ring)
	if err != nil {
		t.Fatalf("NewCommander: %v", err)
	}
	keys, err := streams.NewKeys("routing", 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	stream := keys.Stream(0)
	group, err := streams.Group("workers")
	if err != nil {
		t.Fatalf("Group: %v", err)
	}
	ctx := t.Context()
	if _, err := cmder.Do(ctx, streams.XGroupCreate(stream, group, streams.IDZero, true)); err != nil {
		t.Fatalf("XGROUP CREATE: %v", err)
	}
	const rounds = 20
	for i := range rounds {
		cmd := streams.XAdd(streams.XAddArgs{Key: stream, Fields: []string{"b", string(rune('a' + i))}})
		if _, err := cmder.Do(ctx, cmd); err != nil {
			t.Fatalf("XADD %d: %v", i, err)
		}
	}
	a, b := len(fakeA.Entries(stream)), len(fakeB.Entries(stream))
	if a+b != rounds {
		t.Fatalf("the ring holds %d + %d entries, want %d in total", a, b, rounds)
	}
	if a != rounds && b != rounds {
		t.Fatalf("one stream's entries were split across two servers (%d and %d): the commands "+
			"were routed at random rather than by their key", a, b)
	}
	// And the read has to reach the same shard, which is what the key span
	// exists for: XREADGROUP's keys are not at a position any heuristic finds.
	read, err := streams.XReadGroup(streams.XReadGroupArgs{
		Group: group, Consumer: "c1", Keys: []string{stream}, IDs: []string{streams.IDNew}, Count: rounds,
	})
	if err != nil {
		t.Fatalf("XReadGroup: %v", err)
	}
	v, err := cmder.Do(ctx, read)
	if err != nil {
		t.Fatalf("XREADGROUP: %v", err)
	}
	entries, err := streams.ParseXReadGroup(v, keys.Shard, nil)
	if err != nil {
		t.Fatalf("ParseXReadGroup: %v", err)
	}
	if len(entries) != rounds {
		t.Fatalf("the read found %d of %d entries: XREADGROUP was routed to a shard the XADDs did "+
			"not write to", len(entries), rounds)
	}
}
