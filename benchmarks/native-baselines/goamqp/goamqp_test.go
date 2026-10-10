// Package goamqp measures github.com/Azure/go-amqp (AMQP 1.0) against the
// ActiveMQ Artemis container. It is the client the ActiveMQ driver will use
// for Artemis and for ActiveMQ Classic's AMQP transport.
//
// Confirmation boundary: Sender.Send with the default unsettled sender
// settlement mode returns when the broker has settled the transfer with an
// accepted disposition.
// Settlement: Receiver.AcceptMessage per delivery (accepted disposition).
//
// The sender and receiver declare the "queue" capability so Artemis
// auto-creates an ANYCAST address and queue rather than a multicast (topic)
// address; without it the routing type depends on the broker's
// default-address-routing-type.
package goamqp

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

func BenchmarkNative(b *testing.B) {
	nativebench.Run(b, "goamqp", opener(harness.Artemis(b)))
}

// TestNativeSession is the baseline's correctness gate: it round-trips
// single messages and full batches at every payload size before any
// measurement is taken.
func TestNativeSession(t *testing.T) {
	nativebench.SkipUnlessEnabled(t)
	nativebench.CheckSession(t, opener(harness.Artemis(t)))
}

// opener binds the running broker to the session constructor.
func opener(broker *harness.Broker) nativebench.Open {
	return func(tb testing.TB, _ nativebench.Params) nativebench.Session {
		return open(tb, broker)
	}
}

type session struct {
	conn     *amqp.Conn
	sess     *amqp.Session
	sender   *amqp.Sender
	receiver *amqp.Receiver
	queue    string
}

func open(tb testing.TB, broker *harness.Broker) *session {
	tb.Helper()
	ctx := nativebench.Context(tb)
	conn, err := amqp.Dial(ctx, "amqp://"+broker.Addr, &amqp.ConnOptions{
		SASLType:    amqp.SASLTypePlain(broker.Auth.User, broker.Auth.Password.Reveal()),
		ContainerID: "iqmq-native-baseline",
	})
	if err != nil {
		tb.Fatalf("go-amqp: dial %s: %v", broker.Addr, err)
	}
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		_ = conn.Close()
		tb.Fatalf("go-amqp: session: %v", err)
	}
	queue := nativebench.Name("iqmqbench")
	sender, err := sess.NewSender(ctx, queue, &amqp.SenderOptions{
		TargetCapabilities: []string{"queue"},
	})
	if err != nil {
		_ = conn.Close()
		tb.Fatalf("go-amqp: sender %s: %v", queue, err)
	}
	receiver, err := sess.NewReceiver(ctx, queue, &amqp.ReceiverOptions{
		SourceCapabilities: []string{"queue"},
		Credit:             2048,
	})
	if err != nil {
		_ = conn.Close()
		tb.Fatalf("go-amqp: receiver %s: %v", queue, err)
	}
	return &session{conn: conn, sess: sess, sender: sender, receiver: receiver, queue: queue}
}

func (s *session) Publish(ctx context.Context, payload []byte) error {
	return s.sender.Send(ctx, amqp.NewMessage(payload), nil)
}

func (s *session) PublishBatch(ctx context.Context, payloads [][]byte) error {
	for _, p := range payloads {
		if err := s.sender.Send(ctx, amqp.NewMessage(p), nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) Receive(ctx context.Context, n int) error {
	for range n {
		msg, err := s.receiver.Receive(ctx, nil)
		if err != nil {
			return err
		}
		if err := s.receiver.AcceptMessage(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	serr := s.sender.Close(ctx)
	rerr := s.receiver.Close(ctx)
	return errors.Join(serr, rerr, s.conn.Close())
}
