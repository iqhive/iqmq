// Package kafkago measures github.com/segmentio/kafka-go against the
// Redpanda container.
//
// Confirmation boundary: RequiredAcks is set to kafka.RequireAll
// explicitly. The zero value of kafka.RequiredAcks is RequireNone (fire and
// forget); only the deprecated kafka.NewWriter constructor rewrites 0 to
// RequireAll (writer.go:508). A baseline that left the field at its zero
// value would not be comparable with franz-go or Sarama.
// Settlement: consumer-group offsets committed through Reader.CommitMessages
// with CommitInterval > 0, i.e. marked in memory and flushed periodically.
//
// Writer.BatchSize is 1 for the per-message leaves and nativebench.BatchSize
// for the batching leaves: kafka-go's writer waits for BatchTimeout before
// flushing a partial batch, so a single WriteMessages call on a writer
// configured for batching measures the linger, not the broker.
package kafkago

import (
	"context"
	"errors"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

func BenchmarkNative(b *testing.B) {
	nativebench.Run(b, "kafkago", opener(harness.Redpanda(b)))
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
	return func(tb testing.TB, p nativebench.Params) nativebench.Session {
		return open(tb, broker.Addr, p)
	}
}

type session struct {
	writer *kafka.Writer
	reader *kafka.Reader
	client *kafka.Client
	topic  string
}

func open(tb testing.TB, addr string, p nativebench.Params) *session {
	tb.Helper()
	ctx := nativebench.Context(tb)
	topic := nativebench.Name("iqmqbench")
	client := &kafka.Client{Addr: kafka.TCP(addr), Timeout: 30 * time.Second}
	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	})
	if err != nil {
		tb.Fatalf("kafka-go: create topic %s: %v", topic, err)
	}
	for t, terr := range resp.Errors {
		if terr != nil {
			tb.Fatalf("kafka-go: create topic %s: %v", t, terr)
		}
	}
	batch := 1
	if p.Batched() {
		batch = nativebench.BatchSize
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    batch,
		BatchTimeout: 10 * time.Millisecond,
		Async:        false,
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{addr},
		Topic:          topic,
		GroupID:        topic + "-group",
		StartOffset:    kafka.FirstOffset,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		CommitInterval: time.Second,
	})
	return &session{writer: w, reader: r, client: client, topic: topic}
}

func (s *session) Publish(ctx context.Context, payload []byte) error {
	return s.writer.WriteMessages(ctx, kafka.Message{Value: payload})
}

func (s *session) PublishBatch(ctx context.Context, payloads [][]byte) error {
	msgs := make([]kafka.Message, len(payloads))
	for i, p := range payloads {
		msgs[i] = kafka.Message{Value: p}
	}
	return s.writer.WriteMessages(ctx, msgs...)
}

func (s *session) Receive(ctx context.Context, n int) error {
	for range n {
		m, err := s.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err := s.reader.CommitMessages(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rerr := s.reader.Close()
	werr := s.writer.Close()
	_, derr := s.client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{s.topic}})
	return errors.Join(rerr, werr, derr)
}
