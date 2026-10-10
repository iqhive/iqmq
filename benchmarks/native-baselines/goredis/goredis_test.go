// Package goredis measures github.com/redis/go-redis/v9 against the Redis
// container with the same Streams consumer-group shape as the rueidis
// baseline: XADD to publish, XREADGROUP to claim, XACK to settle. The two
// Redis baselines differ only in the client library, so their numbers are
// directly comparable.
//
// Confirmation boundary: the server's reply to XADD (no AOF fsync per write,
// no WAIT).
// Settlement: XACK per delivery.
//
// The batching leaf uses a pipeline, go-redis' equivalent of rueidis'
// DoMulti: one write, one read, len(batch) replies.
package goredis

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

func BenchmarkNative(b *testing.B) {
	nativebench.Run(b, "goredis", opener(harness.Redis(b)))
}

// TestNativeSession is the baseline's correctness gate: it round-trips
// single messages and full batches at every payload size before any
// measurement is taken.
func TestNativeSession(t *testing.T) {
	nativebench.SkipUnlessEnabled(t)
	nativebench.CheckSession(t, opener(harness.Redis(t)))
}

// opener binds the running broker to the session constructor.
func opener(broker *harness.Broker) nativebench.Open {
	return func(tb testing.TB, _ nativebench.Params) nativebench.Session {
		return open(tb, broker.Addr)
	}
}

type session struct {
	client   *redis.Client
	stream   string
	group    string
	consumer string
	pending  []string
}

func open(tb testing.TB, addr string) *session {
	tb.Helper()
	ctx := nativebench.Context(tb)
	client := redis.NewClient(&redis.Options{Addr: addr})
	// The stem is the SHARED one, not a literal of this file's own: the key's
	// bytes are charged on the wire and again by the server, so a stream name
	// of a different length from the driver arm's would put itself into the
	// overhead ratio.
	stream := nativebench.Name(nativebench.ResourceStem)
	group := stream + "-group"
	if err := client.XGroupCreateMkStream(ctx, stream, group, "$").Err(); err != nil {
		_ = client.Close()
		tb.Fatalf("go-redis: xgroup create %s: %v", stream, err)
	}
	return &session{client: client, stream: stream, group: group, consumer: "bench"}
}

func (s *session) Publish(ctx context.Context, payload []byte) error {
	return s.client.XAdd(ctx, &redis.XAddArgs{Stream: s.stream, Values: []any{"d", payload}}).Err()
}

func (s *session) PublishBatch(ctx context.Context, payloads [][]byte) error {
	pipe := s.client.Pipeline()
	for _, p := range payloads {
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: s.stream, Values: []any{"d", p}})
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (s *session) Receive(ctx context.Context, n int) error {
	for len(s.pending) < n {
		streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.group,
			Consumer: s.consumer,
			Streams:  []string{s.stream, ">"},
			Count:    int64(n - len(s.pending)),
			Block:    10 * time.Second,
		}).Result()
		if err != nil {
			return err
		}
		for _, st := range streams {
			for _, msg := range st.Messages {
				s.pending = append(s.pending, msg.ID)
			}
		}
	}
	if err := s.client.XAck(ctx, s.stream, s.group, s.pending[:n]...).Err(); err != nil {
		return err
	}
	s.pending = s.pending[:copy(s.pending, s.pending[n:])]
	return nil
}

func (s *session) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := s.client.Del(ctx, s.stream).Err()
	if cerr := s.client.Close(); err == nil {
		err = cerr
	}
	return err
}
