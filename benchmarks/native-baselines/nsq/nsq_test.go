// Package nsq measures github.com/nsqio/go-nsq against the nsqlookupd +
// nsqd container pair.
//
// Confirmation boundary: nsqd's OK response to PUB and MPUB. nsqd keeps
// messages in memory until --mem-queue-size is exceeded, so this is a
// memory-durability boundary, never comparable with a stored publish.
// Settlement: FIN per message. The handler disables go-nsq's automatic
// response so the benchmark, not the library, decides when a message is
// finished.
//
// A 100-message batch of 64 KiB payloads is 6.4 MiB, above nsqd's default
// --max-body-size of 5 MiB, so MultiPublish is split into chunks below that
// limit; per-message size stays under the 1 MiB --max-msg-size default.
//
// The client settings are go-nsq's defaults apart from MaxInFlight (1 by
// default, raised here so a backlog can be prefetched) and MsgTimeout. In
// particular output_buffer_timeout keeps its 250 ms default, which dominates
// the EndToEnd leaves for small payloads: that is what an untuned NSQ
// consumer costs, and the driver page records it as a fact rather than
// hiding it behind a tuned baseline.
//
// IQMQ_NSQ_LOW_LATENCY=1 measures the tuned consumer instead: an
// output_buffer_size of -1 turns nsqd's output buffering off for the
// consumer's connection, so every message is written as soon as it is ready
// and the 250 ms flush timer never applies. The producer keeps the default,
// because nsqd already answers a publish without waiting on that timer and
// turning buffering off there only adds writes. Compare the two runs side by side; the
// benchmark names are the same in both.
package nsq

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

// maxBody keeps one MPUB below nsqd's default --max-body-size (5 MiB).
const maxBody = 4 << 20

func BenchmarkNative(b *testing.B) {
	nativebench.Run(b, "nsq", opener(harness.NSQ(b)))
}

// TestNativeSession is the baseline's correctness gate: it round-trips
// single messages and full batches at every payload size before any
// measurement is taken.
func TestNativeSession(t *testing.T) {
	nativebench.SkipUnlessEnabled(t)
	nativebench.CheckSession(t, opener(harness.NSQ(t)))
}

// opener binds the running broker to the session constructor.
func opener(broker *harness.Broker) nativebench.Open {
	return func(tb testing.TB, _ nativebench.Params) nativebench.Session {
		return open(tb, broker.Addr, broker.Endpoint("http"))
	}
}

type session struct {
	producer *nsq.Producer
	consumer *nsq.Consumer
	topic    string
	httpAddr string
	msgs     chan *nsq.Message
	tb       testing.TB
	// closing makes the handler settle deliveries itself. Messages parked
	// for the benchmark are in flight as far as nsqd is concerned, and
	// Consumer.Stop waits for every in-flight message to be finished.
	closing atomic.Bool
}

func open(tb testing.TB, addr, httpAddr string) *session {
	tb.Helper()
	topic := nativebench.Name("iqmqbench")
	s := &session{topic: topic, httpAddr: httpAddr, msgs: make(chan *nsq.Message, 4096), tb: tb}
	// Create the topic and the channel over the HTTP API first: nsqd drops
	// messages published to a topic that has no channel, so a publish that
	// races the SUB would be lost.
	s.post(tb, "/topic/create", url.Values{"topic": {topic}})
	s.post(tb, "/channel/create", url.Values{"topic": {topic}, "channel": {"bench"}})

	cfg := nsq.NewConfig()
	cfg.MaxInFlight = 1024
	cfg.MsgTimeout = 5 * time.Minute
	producer, err := nsq.NewProducer(addr, cfg)
	if err != nil {
		tb.Fatalf("nsq: producer: %v", err)
	}
	producer.SetLoggerLevel(nsq.LogLevelError)
	ccfg := nsq.NewConfig()
	ccfg.MaxInFlight = cfg.MaxInFlight
	ccfg.MsgTimeout = cfg.MsgTimeout
	if os.Getenv("IQMQ_NSQ_LOW_LATENCY") == "1" {
		ccfg.OutputBufferSize = -1
	}
	consumer, err := nsq.NewConsumer(topic, "bench", ccfg)
	if err != nil {
		producer.Stop()
		tb.Fatalf("nsq: consumer: %v", err)
	}
	consumer.SetLoggerLevel(nsq.LogLevelError)
	consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		if s.closing.Load() {
			return nil // auto-FIN: the session is shutting down
		}
		m.DisableAutoResponse()
		s.msgs <- m
		return nil
	}))
	if err := consumer.ConnectToNSQD(addr); err != nil {
		producer.Stop()
		tb.Fatalf("nsq: connect consumer to %s: %v", addr, err)
	}
	s.producer, s.consumer = producer, consumer
	return s
}

func (s *session) post(tb testing.TB, path string, q url.Values) {
	tb.Helper()
	ctx := nativebench.Context(tb)
	u := "http://" + s.httpAddr + path + "?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		tb.Fatalf("nsq: request %s: %v", path, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		tb.Fatalf("nsq: %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		tb.Fatalf("nsq: %s: %s: %s", path, resp.Status, body)
	}
}

func (s *session) Publish(_ context.Context, payload []byte) error {
	return s.producer.Publish(s.topic, payload)
}

func (s *session) PublishBatch(_ context.Context, payloads [][]byte) error {
	chunk := make([][]byte, 0, len(payloads))
	size := 0
	for _, p := range payloads {
		if size+len(p) > maxBody && len(chunk) > 0 {
			if err := s.producer.MultiPublish(s.topic, chunk); err != nil {
				return err
			}
			chunk, size = chunk[:0], 0
		}
		chunk = append(chunk, p)
		size += len(p)
	}
	if len(chunk) == 0 {
		return nil
	}
	return s.producer.MultiPublish(s.topic, chunk)
}

func (s *session) Receive(ctx context.Context, n int) error {
	for range n {
		select {
		case m := <-s.msgs:
			m.Finish()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Close finishes everything the benchmark left parked before stopping the
// consumer: a message handed to the benchmark and never finished stays in
// flight, and Consumer.Stop waits for in-flight messages. The topic is
// deleted only after the consumer has gone away, so the delete cannot tear
// down a connection that is still draining.
func (s *session) Close() error {
	s.closing.Store(true)
	s.consumer.Stop()
	deadline := time.After(60 * time.Second)
	var err error
drain:
	for {
		select {
		case m := <-s.msgs:
			m.Finish()
		case <-s.consumer.StopChan:
			break drain
		case <-deadline:
			err = errors.New("nsq: consumer did not stop")
			break drain
		}
	}
	s.post(s.tb, "/topic/delete", url.Values{"topic": {s.topic}})
	s.producer.Stop()
	return err
}
