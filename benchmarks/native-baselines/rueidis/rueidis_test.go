// Package rueidis measures github.com/redis/rueidis against the Redis
// container, using the Streams consumer-group shape the iqmq Redis driver
// uses: XADD to publish, XREADGROUP to claim, XACK to settle.
//
// Confirmation boundary: the server's reply to XADD. The container runs with
// Redis' default persistence (no AOF fsync per write, no WAIT), so this is a
// memory-durability boundary.
// Settlement: XACK per delivery, removing the entry from the group's
// pending-entries list.
//
// rueidis builds commands from string arguments; a []byte payload is passed
// through rueidis.BinaryString, the documented zero-copy conversion, so the
// baseline does not charge rueidis for a copy the API does not require.
// Its auto-pipelining means the batching leaf can hand the whole batch to
// DoMulti on one connection.
package rueidis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

func BenchmarkNative(b *testing.B) {
	nativebench.Run(b, "rueidis", opener(harness.Redis(b)))
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
	client   rueidis.Client
	stream   string
	group    string
	consumer string
	pending  []string
}

func open(tb testing.TB, addr string) *session {
	tb.Helper()
	ctx := nativebench.Context(tb)
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	if err != nil {
		tb.Fatalf("rueidis: connect %s: %v", addr, err)
	}
	// The stem is the SHARED one, not a literal of this file's own: rueidis
	// computes the cluster slot with a CRC16 over the key once per command and
	// the server charges the key's bytes again, so a stream name of a
	// different length from the driver arm's would put itself into the
	// overhead ratio.
	stream := nativebench.Name(nativebench.ResourceStem)
	group := stream + "-group"
	cmd := client.B().XgroupCreate().Key(stream).Group(group).Id("$").Mkstream().Build()
	if err := client.Do(ctx, cmd).Error(); err != nil {
		client.Close()
		tb.Fatalf("rueidis: xgroup create %s: %v", stream, err)
	}
	return &session{client: client, stream: stream, group: group, consumer: "bench"}
}

func (s *session) add(payload []byte) rueidis.Completed {
	return s.client.B().Xadd().Key(s.stream).Id("*").FieldValue().
		FieldValue("d", rueidis.BinaryString(payload)).Build()
}

func (s *session) Publish(ctx context.Context, payload []byte) error {
	return s.client.Do(ctx, s.add(payload)).Error()
}

func (s *session) PublishBatch(ctx context.Context, payloads [][]byte) error {
	cmds := make(rueidis.Commands, 0, len(payloads))
	for _, p := range payloads {
		cmds = append(cmds, s.add(p))
	}
	var errs []error
	for _, res := range s.client.DoMulti(ctx, cmds...) {
		if err := res.Error(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *session) Receive(ctx context.Context, n int) error {
	for len(s.pending) < n {
		cmd := s.client.B().Xreadgroup().Group(s.group, s.consumer).
			Count(int64(n - len(s.pending))).Block(10000).Streams().Key(s.stream).Id(">").Build()
		read, err := s.client.Do(ctx, cmd).AsXRead()
		if err != nil {
			return err
		}
		for _, entry := range read[s.stream] {
			s.pending = append(s.pending, entry.ID)
		}
	}
	ack := s.client.B().Xack().Key(s.stream).Group(s.group).Id(s.pending[:n]...).Build()
	if err := s.client.Do(ctx, ack).Error(); err != nil {
		return err
	}
	s.pending = s.pending[:copy(s.pending, s.pending[n:])]
	return nil
}

func (s *session) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := s.client.Do(ctx, s.client.B().Del().Key(s.stream).Build()).Error()
	s.client.Close()
	return err
}
