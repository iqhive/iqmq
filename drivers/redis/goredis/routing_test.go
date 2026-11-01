package goredis_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
	"github.com/iqhive/iqmq/internal/streams/respsrv"
)

// TestARingIsRefused is a go-redis Ring handed to the driver.
//
// A Ring that marks a shard down, after about three failed heartbeats, hashes
// that shard's keys onto the live shards until it comes back. A topic's stream
// and consumer groups do not move with them, so a publication in that window
// went to a shard where the subscription has no consumer group: it was
// reported done and no subscriber ever received it. The driver cannot keep a
// topic on its home shard through a Ring, so every way in refuses one, as
// invalid configuration that names what to use instead, before anything is
// sent and without closing the client.
func TestARingIsRefused(t *testing.T) {
	ring, shards := serveRing(t)
	ctx := context.Background()
	cfg := baseConfig(shards["a"].Server)
	for _, tc := range []struct {
		name string
		open func() error
	}{
		{"OpenWithClient", func() error {
			c, err := goredis.OpenWithClient(ctx, ring, driverkit.ClientBorrowed, cfg)
			if err == nil {
				_ = c.Close()
			}
			return err
		}},
		{"BackendWithClient owning the client", func() error {
			b, err := goredis.BackendWithClient(ctx, ring, driverkit.ClientOwned, cfg)
			if err == nil {
				_ = b.Close()
			}
			return err
		}},
		{"NewCommander", func() error {
			_, err := goredis.NewCommander(ring)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.open()
			if mq.CodeOf(err) != mq.CodeInvalidConfig {
				t.Fatalf("%s(a *redis.Ring) = %v, want an invalid-configuration refusal", tc.name, err)
			}
			if msg := err.Error(); !strings.Contains(msg, "Ring") || !strings.Contains(msg, "Cluster") {
				t.Errorf("the refusal %q does not say why a Ring is refused and what to use instead", msg)
			}
			if err := ring.Ping(ctx).Err(); err != nil {
				t.Errorf("the Ring no longer answers after the refusal (%v): the driver closed a client it refused", err)
			}
		})
	}
	for name, rec := range shards {
		if sent := rec.sent(); len(sent) > 0 {
			t.Errorf("shard %q was sent %v by an open that was refused", name, sent)
		}
	}
}

// verbRecorder is one shard: the in-memory Redis, recording the verbs it was
// sent.
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

// sent is the verbs the shard was sent besides the ones a go-redis client
// sends on its own: the handshake and the heartbeat or a test's PING.
func (r *verbRecorder) sent() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.verbs))
	for _, v := range r.verbs {
		switch v {
		case "HELLO", "CLIENT", "PING":
			continue
		}
		out = append(out, v)
	}
	return out
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

// TestKeyRoutingIsDecidedByTheCommandsKeyAndNotAtRandom is the cluster-safety
// property, proved on the one go-redis client that shards by key WITHOUT a
// cluster: a Ring. The driver refuses a Ring (TestARingIsRefused), so the test
// builds its commander past the refusal.
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
	cmder := goredis.CommanderOverForTest(ring)
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
