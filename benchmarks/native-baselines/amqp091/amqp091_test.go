// Package amqp091 measures github.com/rabbitmq/amqp091-go (AMQP 0-9-1,
// maintained by the RabbitMQ core team) against the RabbitMQ container.
//
// Confirmation boundary: publisher confirms (basic.ack) for a persistent
// message on a durable quorum queue, waited for with the deferred-confirm
// API. Quorum queues fsync before confirming, so this is the strongest
// boundary of any baseline in this module and must not be compared with a
// memory-durability publish.
// Settlement: basic.ack per delivery, multiple=false.
package amqp091

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

func BenchmarkNative(b *testing.B) {
	nativebench.Run(b, "amqp091", opener(harness.RabbitMQ(b)))
}

// TestNativeSession is the baseline's correctness gate: it round-trips
// single messages and full batches at every payload size before any
// measurement is taken.
func TestNativeSession(t *testing.T) {
	nativebench.SkipUnlessEnabled(t)
	nativebench.CheckSession(t, opener(harness.RabbitMQ(t)))
}

// opener binds the running broker to the session constructor.
func opener(broker *harness.Broker) nativebench.Open {
	return func(tb testing.TB, _ nativebench.Params) nativebench.Session {
		return open(tb, broker)
	}
}

type session struct {
	conn       *amqp.Connection
	ch         *amqp.Channel
	queue      string
	deliveries <-chan amqp.Delivery
}

func open(tb testing.TB, broker *harness.Broker) *session {
	tb.Helper()
	url := "amqp://" + broker.Auth.User + ":" + broker.Auth.Password.Reveal() + "@" + broker.Addr + "/"
	conn, err := amqp.Dial(url)
	if err != nil {
		tb.Fatalf("amqp091: dial %s: %v", broker.Addr, err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		tb.Fatalf("amqp091: channel: %v", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = conn.Close()
		tb.Fatalf("amqp091: confirm mode: %v", err)
	}
	if err := ch.Qos(2048, 0, false); err != nil {
		_ = conn.Close()
		tb.Fatalf("amqp091: qos: %v", err)
	}
	queue := nativebench.Name("iqmqbench")
	if _, err := ch.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		_ = conn.Close()
		tb.Fatalf("amqp091: declare %s: %v", queue, err)
	}
	deliveries, err := ch.Consume(queue, "bench", false, false, false, false, nil)
	if err != nil {
		_ = conn.Close()
		tb.Fatalf("amqp091: consume %s: %v", queue, err)
	}
	return &session{conn: conn, ch: ch, queue: queue, deliveries: deliveries}
}

func (s *session) publish(ctx context.Context, payload []byte) (*amqp.DeferredConfirmation, error) {
	return s.ch.PublishWithDeferredConfirmWithContext(ctx, "", s.queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         payload,
	})
}

func (s *session) Publish(ctx context.Context, payload []byte) error {
	dc, err := s.publish(ctx, payload)
	if err != nil {
		return err
	}
	acked, err := dc.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return errors.New("amqp091: publish nacked")
	}
	return nil
}

func (s *session) PublishBatch(ctx context.Context, payloads [][]byte) error {
	confirms := make([]*amqp.DeferredConfirmation, 0, len(payloads))
	for _, p := range payloads {
		dc, err := s.publish(ctx, p)
		if err != nil {
			return err
		}
		confirms = append(confirms, dc)
	}
	for _, dc := range confirms {
		acked, err := dc.WaitContext(ctx)
		if err != nil {
			return err
		}
		if !acked {
			return errors.New("amqp091: publish nacked")
		}
	}
	return nil
}

func (s *session) Receive(ctx context.Context, n int) error {
	for range n {
		select {
		case d, ok := <-s.deliveries:
			if !ok {
				return errors.New("amqp091: delivery channel closed")
			}
			if err := d.Ack(false); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *session) Close() error {
	_, derr := s.ch.QueueDelete(s.queue, false, false, false)
	cerr := s.conn.Close()
	return errors.Join(derr, cerr)
}
