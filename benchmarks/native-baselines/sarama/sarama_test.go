// Package sarama measures github.com/IBM/sarama against the Redpanda
// container.
//
// Confirmation boundary: Producer.RequiredAcks is raised to WaitForAll and
// Producer.Idempotent is enabled, matching franz-go's defaults. Sarama's own
// defaults are weaker — NewConfig sets RequiredAcks = WaitForLocal (acks=1)
// and MaxMessageBytes = 1 MiB (config.go:569-570) — and an idempotent
// producer additionally requires Net.MaxOpenRequests = 1 and Retry.Max >= 1
// (config.go:811-824).
// Settlement: consumer-group offsets marked with MarkMessage and flushed by
// the client's automatic offset commit (Consumer.Offsets.AutoCommit,
// 1 s by default).
package sarama

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

func BenchmarkNative(b *testing.B) {
	nativebench.Run(b, "sarama", opener(harness.Redpanda(b)))
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

// delivery carries a claimed message together with the group session that
// owns its offsets, so Receive can settle it.
type delivery struct {
	msg  *sarama.ConsumerMessage
	sess sarama.ConsumerGroupSession
}

type session struct {
	producer sarama.SyncProducer
	group    sarama.ConsumerGroup
	admin    sarama.ClusterAdmin
	topic    string
	msgs     chan delivery
	cancel   context.CancelFunc
	done     chan struct{}
	consumed int
}

func config() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Idempotent = true
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = 3
	cfg.Net.MaxOpenRequests = 1
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = false
	cfg.ClientID = "iqmq-native-baseline"
	return cfg
}

func open(tb testing.TB, addr string) *session {
	tb.Helper()
	cfg := config()
	admin, err := sarama.NewClusterAdmin([]string{addr}, cfg)
	if err != nil {
		tb.Fatalf("sarama: cluster admin: %v", err)
	}
	topic := nativebench.Name("iqmqbench")
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		_ = admin.Close()
		tb.Fatalf("sarama: create topic %s: %v", topic, err)
	}
	producer, err := sarama.NewSyncProducer([]string{addr}, cfg)
	if err != nil {
		_ = admin.Close()
		tb.Fatalf("sarama: producer: %v", err)
	}
	group, err := sarama.NewConsumerGroup([]string{addr}, topic+"-group", cfg)
	if err != nil {
		_ = producer.Close()
		_ = admin.Close()
		tb.Fatalf("sarama: consumer group: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &session{
		producer: producer, group: group, admin: admin, topic: topic,
		msgs: make(chan delivery), cancel: cancel, done: make(chan struct{}),
	}
	go s.consume(ctx)
	return s
}

// consume runs the group's rebalance loop; sarama requires Consume to be
// called again after every rebalance.
func (s *session) consume(ctx context.Context) {
	defer close(s.done)
	for ctx.Err() == nil {
		if err := s.group.Consume(ctx, []string{s.topic}, s); err != nil {
			return
		}
	}
}

func (s *session) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (s *session) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (s *session) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			select {
			case s.msgs <- delivery{msg: msg, sess: sess}:
			case <-sess.Context().Done():
				return nil
			}
		case <-sess.Context().Done():
			return nil
		}
	}
}

func (s *session) Publish(_ context.Context, payload []byte) error {
	_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{Topic: s.topic, Value: sarama.ByteEncoder(payload)})
	return err
}

func (s *session) PublishBatch(_ context.Context, payloads [][]byte) error {
	msgs := make([]*sarama.ProducerMessage, len(payloads))
	for i, p := range payloads {
		msgs[i] = &sarama.ProducerMessage{Topic: s.topic, Value: sarama.ByteEncoder(p)}
	}
	return s.producer.SendMessages(msgs)
}

func (s *session) Receive(ctx context.Context, n int) error {
	for range n {
		select {
		case d := <-s.msgs:
			d.sess.MarkMessage(d.msg, "")
			s.consumed++
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *session) Close() error {
	s.cancel()
	<-s.done
	gerr := s.group.Close()
	perr := s.producer.Close()
	derr := s.admin.DeleteTopic(s.topic)
	aerr := s.admin.Close()
	return errors.Join(gerr, perr, derr, aerr)
}

var _ sarama.ConsumerGroupHandler = (*session)(nil)
