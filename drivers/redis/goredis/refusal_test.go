package goredis_test

import (
	"context"
	"errors"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// refusalRulesRelease is the first release of drivers/redis whose error table
// classifies go-redis's own refusals.
const refusalRulesRelease = "v1.1.2"

// driversRedisBefore reports the version of drivers/redis this test binary
// linked, and whether it is a release older than want.
//
// CI also builds this module with GOWORK=off against the drivers/redis
// release its go.mod requires, so a test of behaviour drivers/redis gained
// after that release skips there until the release train moves the
// requirement on. A workspace build links the module in the tree, which the
// build information reports as (devel), and that is older than nothing.
func driversRedisBefore(t *testing.T, want string) (string, bool) {
	t.Helper()
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("this test binary carries no build information")
	}
	for _, dep := range info.Deps {
		if dep.Path != "github.com/iqhive/iqmq/drivers/redis" {
			continue
		}
		if dep.Replace != nil {
			dep = dep.Replace
		}
		have, okHave := releaseNumbers(dep.Version)
		limit, okLimit := releaseNumbers(want)
		return dep.Version, okHave && okLimit && slices.Compare(have, limit) < 0
	}
	t.Fatal("this test binary's build information does not list drivers/redis")
	return "", false
}

// releaseNumbers is the major, minor and patch numbers of a module version
// such as v1.1.1 or v1.1.2-0.20261030010203-0123456789ab, and false for
// anything else, (devel) and the empty version of a directory among them.
func releaseNumbers(version string) ([]int, bool) {
	core, ok := strings.CutPrefix(version, "v")
	if !ok {
		return nil, false
	}
	if i := strings.IndexAny(core, "-+"); i >= 0 {
		core = core[:i]
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return nil, false
	}
	out := make([]int, 3)
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, false
		}
		out[i] = n
	}
	return out, true
}

// TestGoRedisOwnRefusalsAreClassified is what the go-redis arm reports for a
// refusal go-redis makes itself, before anything reaches the server.
//
// The error table knew rueidis's sentinels and none of go-redis's, so both of
// these were CodeUnknown and not retryable:
//   - redis.ErrPoolTimeout, every connection busy (here one parked Fetch on a
//     pool of one). Nothing was written and a retry can succeed.
//   - redis.ErrClosed, a borrowed client closed under the backend, which the
//     rueidis arm reports as CodeClosed.
func TestGoRedisOwnRefusalsAreClassified(t *testing.T) {
	if v, old := driversRedisBefore(t, refusalRulesRelease); old {
		t.Skipf("drivers/redis %s classifies go-redis's refusals from %s on", v, refusalRulesRelease)
	}
	_, srv := newServer(t, fakestreams.Options{AOF: true, Version: "7.4.2"})
	client := redis.NewClient(&redis.Options{
		Addr: srv.Addr(), PoolSize: 1, PoolTimeout: 100 * time.Millisecond,
		ReadTimeout: 5 * time.Second, ContextTimeoutEnabled: true,
	})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	b, err := goredis.BackendWithClient(ctx, client, driverkit.ClientBorrowed, rd.Config{
		AckDeadline: 30 * time.Second, ClaimInterval: time.Hour, Block: 2 * time.Second, AutoCreate: true,
	})
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "pool.topic", Subscription: testSub, Prefetch: 1, Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: testTopic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })

	parked := make(chan struct{})
	go func() {
		defer close(parked)
		fctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, _ = src.Fetch(fctx, make([]mq.DriverDelivery, 1))
	}()
	time.Sleep(200 * time.Millisecond) // the Fetch holds the pool's one connection
	busy := sink.Publish(ctx, &mq.Message{Body: []byte("x")}, nil)
	<-parked
	if !errors.Is(busy, redis.ErrPoolTimeout) || mq.CodeOf(busy) != mq.CodeResourceExhausted ||
		!mq.IsRetryable(busy) || mq.IsAmbiguous(busy) {
		t.Errorf("a publication refused because the pool had no free connection = %v (code %v, retryable %v, ambiguous %v), "+
			"want ResourceExhausted, retryable and definite", busy, mq.CodeOf(busy), mq.IsRetryable(busy), mq.IsAmbiguous(busy))
	}

	_ = client.Close()
	closed := sink.Publish(ctx, &mq.Message{Body: []byte("y")}, nil)
	if !errors.Is(closed, redis.ErrClosed) || !errors.Is(closed, mq.ErrClosed) || mq.IsRetryable(closed) {
		t.Errorf("a publication on a closed go-redis client = %v (code %v, retryable %v), want CodeClosed as rueidis reports it",
			closed, mq.CodeOf(closed), mq.IsRetryable(closed))
	}
}
