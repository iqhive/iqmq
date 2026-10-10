// Package jetstream measures the JetStream API of github.com/nats-io/nats.go
// against the NATS container started with --jetstream.
//
// Confirmation boundary: a PubAck from a file-backed stream with one replica
// (jetstream.FileStorage, Replicas: 1). The batching leaves use PublishAsync
// and wait for every future, which is the batched form of the same boundary.
// Settlement: AckExplicitPolicy with a single Ack (no double-ack round
// trip), which is what a pull-consumer driver does per message.
package jetstream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

func BenchmarkNative(b *testing.B) {
	nativebench.Run(b, "jetstream", opener(harness.NATS(b)))
}

// TestNativeSession is the baseline's correctness gate: it round-trips
// single messages and full batches at every payload size before any
// measurement is taken.
func TestNativeSession(t *testing.T) {
	nativebench.SkipUnlessEnabled(t)
	nativebench.CheckSession(t, opener(harness.NATS(t)))
}

// opener binds the running broker to the session constructor.
func opener(broker *harness.Broker) nativebench.Open {
	return func(tb testing.TB, _ nativebench.Params) nativebench.Session {
		return open(tb, broker.Addr)
	}
}

type session struct {
	conn     *nats.Conn
	js       jetstream.JetStream
	consumer jetstream.Consumer
	name     string
	subject  string
	buf      []jetstream.Msg
}

func open(tb testing.TB, addr string) *session {
	tb.Helper()
	ctx := nativebench.Context(tb)
	nc, err := nats.Connect("nats://"+addr, nats.Name("iqmq-native-baseline"))
	if err != nil {
		tb.Fatalf("jetstream: connect %s: %v", addr, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		tb.Fatalf("jetstream: new: %v", err)
	}
	name := nativebench.Name("iqmqbench")
	subject := name + ".bench"
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      name,
		Subjects:  []string{subject},
		Storage:   jetstream.FileStorage,
		Replicas:  1,
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		nc.Close()
		tb.Fatalf("jetstream: create stream %s: %v", name, err)
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       name,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 4096,
	})
	if err != nil {
		nc.Close()
		tb.Fatalf("jetstream: create consumer %s: %v", name, err)
	}
	return &session{conn: nc, js: js, consumer: consumer, name: name, subject: subject}
}

func (s *session) Publish(ctx context.Context, payload []byte) error {
	_, err := s.js.Publish(ctx, s.subject, payload)
	return err
}

func (s *session) PublishBatch(_ context.Context, payloads [][]byte) error {
	futures := make([]jetstream.PubAckFuture, 0, len(payloads))
	for _, p := range payloads {
		f, err := s.js.PublishAsync(s.subject, p)
		if err != nil {
			return err
		}
		futures = append(futures, f)
	}
	select {
	case <-s.js.PublishAsyncComplete():
	case <-time.After(60 * time.Second):
		return errors.New("jetstream: publish batch timed out")
	}
	for _, f := range futures {
		select {
		case err := <-f.Err():
			return err
		case <-f.Ok():
		default:
			return errors.New("jetstream: publish future neither ok nor failed")
		}
	}
	return nil
}

func (s *session) Receive(ctx context.Context, n int) error {
	for len(s.buf) < n {
		batch, err := s.consumer.Fetch(n-len(s.buf), jetstream.FetchMaxWait(10*time.Second))
		if err != nil {
			return err
		}
		for msg := range batch.Messages() {
			s.buf = append(s.buf, msg)
		}
		if err := batch.Error(); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	for _, msg := range s.buf[:n] {
		if err := msg.Ack(); err != nil {
			return err
		}
	}
	s.buf = s.buf[:copy(s.buf, s.buf[n:])]
	return nil
}

func (s *session) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := s.js.DeleteStream(ctx, s.name)
	s.conn.Close()
	return err
}
