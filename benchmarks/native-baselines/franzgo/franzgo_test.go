// Package franzgo measures github.com/twmb/franz-go against the Redpanda
// container, with the settings the iqmq Kafka driver will use.
//
// Confirmation boundary: the client default acks=all on an idempotent
// producer (kgo config.go:663 acks: AllISRAcks(); idempotency is on unless
// DisableIdempotentWrite is passed), waited for with ProduceSync.
// Settlement: consumer-group offsets marked per record and committed by the
// client's periodic auto-commit (AutoCommitMarks), which is what a driver
// with an offset tracker does; it is not a per-message commit RPC.
package franzgo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

func BenchmarkNative(b *testing.B) {
	nativebench.Run(b, "franzgo", opener(harness.Redpanda(b)))
}

// TestNativeSession is the baseline's correctness gate: it round-trips
// single messages and full batches at every payload size before any
// measurement is taken.
func TestNativeSession(t *testing.T) {
	nativebench.SkipUnlessEnabled(t)
	nativebench.CheckSession(t, opener(harness.Redpanda(t)))
}

// opener binds the running broker to the session constructor.
func opener(broker *harness.Broker) nativebench.Open {
	return func(tb testing.TB, _ nativebench.Params) nativebench.Session {
		return open(tb, broker.Addr)
	}
}

type session struct {
	producer *kgo.Client
	consumer *kgo.Client
	admin    *kadm.Client
	topic    string
	buf      []*kgo.Record
}

func open(tb testing.TB, addr string) *session {
	tb.Helper()
	ctx := nativebench.Context(tb)
	topic := nativebench.Name("iqmqbench")
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerLinger(0),
	)
	if err != nil {
		tb.Fatalf("franz-go: producer: %v", err)
	}
	admin := kadm.NewClient(producer)
	if _, err := admin.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		producer.Close()
		tb.Fatalf("franz-go: create topic %s: %v", topic, err)
	}
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(topic+"-group"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(time.Second),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		producer.Close()
		tb.Fatalf("franz-go: consumer: %v", err)
	}
	return &session{producer: producer, consumer: consumer, admin: admin, topic: topic}
}

func (s *session) Publish(ctx context.Context, payload []byte) error {
	return s.producer.ProduceSync(ctx, &kgo.Record{Value: payload}).FirstErr()
}

func (s *session) PublishBatch(ctx context.Context, payloads [][]byte) error {
	recs := make([]*kgo.Record, len(payloads))
	for i, p := range payloads {
		recs[i] = &kgo.Record{Value: p}
	}
	return s.producer.ProduceSync(ctx, recs...).FirstErr()
}

func (s *session) Receive(ctx context.Context, n int) error {
	for len(s.buf) < n {
		fetches := s.consumer.PollRecords(ctx, n-len(s.buf))
		if err := fetches.Err(); err != nil {
			return err
		}
		fetches.EachRecord(func(r *kgo.Record) { s.buf = append(s.buf, r) })
		if fetches.Empty() && ctx.Err() != nil {
			return ctx.Err()
		}
	}
	s.consumer.MarkCommitRecords(s.buf[:n]...)
	s.buf = s.buf[:copy(s.buf, s.buf[n:])]
	return nil
}

func (s *session) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := s.consumer.CommitMarkedOffsets(ctx)
	s.consumer.Close()
	_, derr := s.admin.DeleteTopics(ctx, s.topic)
	s.producer.Close()
	return errors.Join(err, derr)
}
