// Package pulsar measures github.com/apache/pulsar-client-go against the
// Pulsar standalone container.
//
// Confirmation boundary: the broker's send receipt for a persistent topic
// (Producer.Send). Producer batching is on by default in this client
// (BatchingMaxPublishDelay 10 ms, BatchingMaxMessages 1000), so the
// per-message leaves disable it — otherwise Publish would measure the batch
// delay — and the batching leaves keep it and wait for every SendAsync
// callback.
// Settlement: Consumer.Ack per message on a Shared subscription (individual
// acknowledgement, the shape a driver with per-message settlement needs).
package pulsar

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	plog "github.com/apache/pulsar-client-go/pulsar/log"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

func BenchmarkNative(b *testing.B) {
	nativebench.Run(b, "pulsar", opener(harness.Pulsar(b)))
}

// TestNativeSession is the baseline's correctness gate: it round-trips
// single messages and full batches at every payload size before any
// measurement is taken.
func TestNativeSession(t *testing.T) {
	nativebench.SkipUnlessEnabled(t)
	nativebench.CheckSession(t, opener(harness.Pulsar(t)))
}

// opener binds the running broker to the session constructor.
func opener(broker *harness.Broker) nativebench.Open {
	return func(tb testing.TB, p nativebench.Params) nativebench.Session {
		return open(tb, broker.Addr, p)
	}
}

type session struct {
	client   pulsar.Client
	producer pulsar.Producer
	consumer pulsar.Consumer
	topic    string
}

func open(tb testing.TB, addr string, p nativebench.Params) *session {
	tb.Helper()
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://" + addr,
		ConnectionTimeout: 30 * time.Second,
		OperationTimeout:  60 * time.Second,
		Logger:            plog.NewLoggerWithSlog(slog.New(slog.NewTextHandler(io.Discard, nil))),
	})
	if err != nil {
		tb.Fatalf("pulsar: client for %s: %v", addr, err)
	}
	topic := "persistent://public/default/" + nativebench.Name("iqmqbench")
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   topic,
		DisableBatching:         !p.Batched(),
		BatchingMaxPublishDelay: time.Millisecond,
		SendTimeout:             60 * time.Second,
	})
	if err != nil {
		client.Close()
		tb.Fatalf("pulsar: producer %s: %v", topic, err)
	}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:             topic,
		SubscriptionName:  "bench",
		Type:              pulsar.Shared,
		ReceiverQueueSize: 2048,
	})
	if err != nil {
		producer.Close()
		client.Close()
		tb.Fatalf("pulsar: consumer %s: %v", topic, err)
	}
	return &session{client: client, producer: producer, consumer: consumer, topic: topic}
}

func (s *session) Publish(ctx context.Context, payload []byte) error {
	_, err := s.producer.Send(ctx, &pulsar.ProducerMessage{Payload: payload})
	return err
}

func (s *session) PublishBatch(ctx context.Context, payloads [][]byte) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	wg.Add(len(payloads))
	for _, p := range payloads {
		s.producer.SendAsync(ctx, &pulsar.ProducerMessage{Payload: p},
			func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
				wg.Done()
			})
	}
	if err := s.producer.FlushWithCtx(ctx); err != nil {
		return err
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (s *session) Receive(ctx context.Context, n int) error {
	for range n {
		msg, err := s.consumer.Receive(ctx)
		if err != nil {
			return err
		}
		if err := s.consumer.Ack(msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) Close() error {
	// Unsubscribe drops the subscription and its backlog; the topic itself
	// lives until the container is discarded (the Go client has no admin
	// API, and pulsaradmin is a separate module this baseline does not
	// need).
	if err := s.consumer.UnsubscribeForce(); err != nil {
		s.consumer.Close()
		s.producer.Close()
		s.client.Close()
		return err
	}
	s.consumer.Close()
	s.producer.Close()
	s.client.Close()
	return nil
}
