// Package nats measures core NATS (github.com/nats-io/nats.go) against the
// NATS container.
//
// Confirmation boundary: core NATS is at-most-once with no store and no
// per-message reply. The publish leaves therefore measure a server
// round-trip: Publish followed by Flush, which sends PING and waits for
// PONG, so the message is known to have reached the server. A driver that
// only writes into the client's output buffer is not comparable with this
// number, and a core-NATS publish is never comparable with a broker-stored
// one (native comparison rules).
// Settlement: none. Core NATS deliveries carry no acknowledgement, so
// Receive only takes the message off the synchronous subscription.
package nats

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

func BenchmarkNative(b *testing.B) {
	nativebench.Run(b, "nats", opener(harness.NATS(b)))
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
	conn    *nats.Conn
	sub     *nats.Subscription
	subject string
}

func open(tb testing.TB, addr string) *session {
	tb.Helper()
	nc, err := nats.Connect("nats://"+addr,
		nats.Name("iqmq-native-baseline"),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		tb.Fatalf("nats: connect %s: %v", addr, err)
	}
	subject := nativebench.Name("iqmqbench")
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		nc.Close()
		tb.Fatalf("nats: subscribe %s: %v", subject, err)
	}
	// The refill batches 256 messages of up to 64 KiB before the consumer
	// drains them; the default 64 MiB / 512 k pending limits cover that, but
	// pin them so a limit change upstream cannot silently drop messages.
	if err := sub.SetPendingLimits(1<<20, 256<<20); err != nil {
		nc.Close()
		tb.Fatalf("nats: pending limits: %v", err)
	}
	return &session{conn: nc, sub: sub, subject: subject}
}

func (s *session) Publish(_ context.Context, payload []byte) error {
	if err := s.conn.Publish(s.subject, payload); err != nil {
		return err
	}
	return s.conn.Flush()
}

func (s *session) PublishBatch(_ context.Context, payloads [][]byte) error {
	for _, p := range payloads {
		if err := s.conn.Publish(s.subject, p); err != nil {
			return err
		}
	}
	return s.conn.Flush()
}

func (s *session) Receive(ctx context.Context, n int) error {
	for range n {
		if _, err := s.sub.NextMsgWithContext(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) Close() error {
	err := s.sub.Unsubscribe()
	s.conn.Close()
	return err
}
