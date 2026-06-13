package pulsarfake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// The consumer handle's own surface.
//
// The driver reaches this model through `Chan`, so the rest of the client
// library's consumer interface -- `Receive`, `Name`, `Subscription`, the
// cumulative and transactional acknowledgements -- was implemented and run by
// nothing. Each of them is a method the real library has, so a driver that
// started calling one would find out here whether this model answers it or
// panics.

const (
	fakeTopic = "persistent://public/default/orders"
	fakeSub   = "workers"
)

// rig is a server, a producer and a consumer on one topic.
type rig struct {
	server *Server
	prod   pulsar.Producer
	cons   pulsar.Consumer
	ch     chan pulsar.ConsumerMessage
}

func newRig(t *testing.T, opts ...Option) *rig {
	t.Helper()
	s := New(opts...)
	cl := s.Client()
	prod, err := cl.CreateProducer(pulsar.ProducerOptions{Topic: fakeTopic})
	if err != nil {
		t.Fatalf("CreateProducer: %v", err)
	}
	t.Cleanup(prod.Close)
	ch := make(chan pulsar.ConsumerMessage, 64)
	cons, err := cl.Subscribe(pulsar.ConsumerOptions{
		Topic: fakeTopic, SubscriptionName: fakeSub, Type: pulsar.Shared,
		Name: "consumer-1", MessageChannel: ch,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(cons.Close)
	return &rig{server: s, prod: prod, cons: cons, ch: ch}
}

// send publishes one message and flushes it.
func (r *rig) send(t *testing.T, body string) {
	t.Helper()
	if _, err := r.prod.Send(t.Context(), &pulsar.ProducerMessage{Payload: []byte(body)}); err != nil {
		t.Fatalf("Send(%s): %v", body, err)
	}
	r.server.Flush()
}

// TestTheConsumerKnowsItsOwnNameAndSubscription.
//
// `Subscription` is looked up by identity rather than stored, so a model with
// two subscriptions on one topic has to answer the right one for each -- the
// case a single-subscription test cannot see.
func TestTheConsumerKnowsItsOwnNameAndSubscription(t *testing.T) {
	r := newRig(t)
	if got := r.cons.Subscription(); got != fakeSub {
		t.Errorf("the consumer reports the subscription %q, want %q", got, fakeSub)
	}
	if got := r.cons.Name(); got != "consumer-1" {
		t.Errorf("the consumer reports the name %q, want the one it was subscribed with", got)
	}

	second, err := r.server.Client().Subscribe(pulsar.ConsumerOptions{
		Topic: fakeTopic, SubscriptionName: "audit", Type: pulsar.Shared,
		MessageChannel: make(chan pulsar.ConsumerMessage, 8),
	})
	if err != nil {
		t.Fatalf("Subscribe a second subscription: %v", err)
	}
	t.Cleanup(second.Close)
	if got := second.Subscription(); got != "audit" {
		t.Errorf("the second consumer reports the subscription %q, want \"audit\"", got)
	}
	if got := r.cons.Subscription(); got != fakeSub {
		t.Errorf("the first consumer now reports %q; the lookup is by identity and must not confuse two "+
			"subscriptions on one topic", got)
	}
}

// TestReceiveDeliversAndHonoursItsContext.
//
// `Receive` is the blocking half of the library's consumer, and a driver that
// used it instead of the channel would block for ever on an empty topic if
// the context were ignored.
func TestReceiveDeliversAndHonoursItsContext(t *testing.T) {
	r := newRig(t)
	r.send(t, "one")

	got, err := r.cons.Receive(t.Context())
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if string(got.Payload()) != "one" {
		t.Errorf("Receive delivered %q, want \"one\"", got.Payload())
	}
	if err := r.cons.Ack(got); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.cons.Receive(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Receive on an empty topic with a cancelled context is %v, want context.Canceled", err)
	}
}

// TestSettlingAfterCloseIsRefused.
//
// It is the shutdown race: the consumer is closing while a handler settles.
// The library answers ConsumerClosed, and a model that accepted the
// settlement would let a driver's test conclude the message was acknowledged
// when on a real broker it would be redelivered.
func TestSettlingAfterCloseIsRefused(t *testing.T) {
	r := newRig(t)
	r.send(t, "one")
	got, err := r.cons.Receive(t.Context())
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}

	r.cons.Close()
	if err := r.cons.Ack(got); err == nil {
		t.Error("acknowledging through a closed consumer succeeded; on a real broker the message is " +
			"redelivered and a test would conclude the opposite")
	}
	if err := r.cons.AckCumulative(got); err == nil {
		t.Error("acknowledging cumulatively through a closed consumer succeeded")
	}
}

// TestAMessageIdentifierFromSomewhereElseIsRefused.
//
// Every settlement takes an identifier the caller got from a delivery. One
// built elsewhere is a driver bug, and the model says so rather than
// silently settling nothing -- which is the failure that looks like a lost
// acknowledgement much later.
func TestAMessageIdentifierFromSomewhereElseIsRefused(t *testing.T) {
	r := newRig(t)
	foreign := pulsar.EarliestMessageID()

	if err := r.cons.AckID(foreign); err == nil {
		t.Error("AckID accepted an identifier this model did not issue")
	}
	if err := r.cons.AckIDCumulative(foreign); err == nil {
		t.Error("AckIDCumulative accepted an identifier this model did not issue")
	}
}

// TestCumulativeAcknowledgementSettlesEverythingUpToIt.
//
// It is the whole point of the call, and the difference from AckID is
// invisible on a single message -- so the test uses three and settles the
// second.
func TestCumulativeAcknowledgementSettlesEverythingUpToIt(t *testing.T) {
	r := newRig(t)
	for _, body := range []string{"one", "two", "three"} {
		r.send(t, body)
	}

	var second pulsar.Message
	for i := range 2 {
		m, err := r.cons.Receive(t.Context())
		if err != nil {
			t.Fatalf("Receive %d: %v", i, err)
		}
		second = m
	}
	if err := r.cons.AckCumulative(second); err != nil {
		t.Fatalf("AckCumulative: %v", err)
	}

	// All THREE were pushed to the consumer's channel, so all three are in
	// flight; the cumulative acknowledgement settles the first two and the
	// third is still outstanding. That is the difference from AckID and the
	// reason this test uses three messages rather than one.
	if got := r.server.InFlight(fakeTopic, fakeSub); got != 1 {
		t.Errorf("%d messages are in flight after a cumulative acknowledgement of the second of three, "+
			"want 1: the cumulative call settles everything up to and including its own identifier", got)
	}
}

// TestATransactionalSettlementIsRefusedWithoutACoordinator.
//
// The model has no transaction coordinator, and the refusal is the library's
// own error rather than a nil that would let a driver believe a transaction
// it never had.
func TestATransactionalSettlementIsRefusedWithoutACoordinator(t *testing.T) {
	r := newRig(t)
	r.send(t, "one")
	got, err := r.cons.Receive(t.Context())
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}

	if err := r.cons.AckWithTxn(got, nil); err == nil {
		t.Error("AckWithTxn succeeded with no transaction coordinator; a driver would believe it had a " +
			"transaction it never had")
	}
}

// TestNackRedeliversAfterTheConfiguredDelay.
//
// Time moves only when the test says so, so the two halves are separable: a
// nack does NOT redeliver immediately, and it does after the delay. A model
// that redelivered at once would make every test of a nack backoff pass
// without the backoff.
func TestNackRedeliversAfterTheConfiguredDelay(t *testing.T) {
	r := newRig(t, WithNackRedeliveryDelay(time.Minute))
	r.send(t, "one")
	got, err := r.cons.Receive(t.Context())
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}

	r.cons.Nack(got)
	// A nacked message leaves the in-flight set at once and comes back only
	// when the delay has passed. Both halves matter: a model that redelivered
	// immediately would make every test of a nack backoff pass without the
	// backoff.
	if n := r.server.InFlight(fakeTopic, fakeSub); n != 0 {
		t.Errorf("%d messages are in flight straight after a nack, want 0", n)
	}
	select {
	case m := <-r.ch:
		t.Fatalf("a nacked message was redelivered as %q before its one-minute delay passed", m.Payload())
	default:
	}

	r.server.Advance(2 * time.Minute)
	select {
	case m := <-r.ch:
		if string(m.Payload()) != "one" {
			t.Errorf("the redelivered message is %q, want \"one\"", m.Payload())
		}
	case <-time.After(2 * time.Second):
		t.Error("a nacked message was not redelivered two minutes after a one-minute redelivery delay")
	}
}
