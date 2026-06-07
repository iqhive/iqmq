package gcppubsub_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mq "bitbucket.org/iqhive/iqmq"
	gps "bitbucket.org/iqhive/iqmq/drivers/gcppubsub"
	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// the and the boundary cases: what a caller is told when the answer is
// "maybe", and what it is told when the answer is "no but try again".
//
// The distinction matters more than it sounds on this broker. A PUBLICATION
// that fails after the request left is genuinely ambiguous — the service may
// have stored the message — and a SETTLEMENT that fails is not: the message is
// still unacknowledged, so it comes back. Reporting either as the other is a
// caller retrying something it should not, or giving up on something it
// should.

// TestPublicationFailureIsAmbiguous is the rule for a transient failure on
// a request that may already have been read.
func TestPublicationFailureIsAmbiguous(t *testing.T) {
	c, srv := scripted(t)
	topic := newTopic("ambig")
	provision(t, c, topic, "s")
	srv.PublishErr = func(string, *psapi.Outgoing) error {
		return status.Error(codes.Unavailable, "injected: the service is unreachable")
	}
	err := c.Publish(context.Background(), topic, &mq.Message{Body: []byte("maybe")})
	if err == nil {
		t.Fatal("the injected failure was not reported")
	}
	if !mq.IsAmbiguous(err) {
		t.Fatalf("a publication that failed with Unavailable reported %v (code %v, ambiguous %v): "+
			"the service answers after it has read the request, so the publication may or may not have been accepted",
			err, mq.CodeOf(err), mq.IsAmbiguous(err))
	}
	// The driver still marks it retryable -- Unavailable is transient -- but
	// that judgement is read where it lives, because mq.IsRetryable answers
	// "safe to repeat" and an ambiguous publication never is. This check used to be `!mq.IsRetryable(err)`.
	var e *mq.OpError
	if !errors.As(err, &e) || !e.Retryable {
		t.Errorf("the failure is not marked retryable: %v", err)
	}
	if mq.IsRetryable(err) {
		t.Errorf("IsRetryable reported an ambiguous publication safe to repeat (%v); repeating it is how it is published twice", err)
	}
}

// TestSettlementFailureIsNotAmbiguousAndLeavesTheDeliverySettleable is the
// other half, and it is the one a caller is most likely to get wrong.
//
// An acknowledgement whose request failed leaves the message unacknowledged at
// the service, so it is redelivered: the outcome is known — the settlement did
// not happen — and the delivery is settleable again. A driver that reported
// ErrSettlementUnknown here would tell a caller nothing later can turn into a
// success, for a settlement it can simply retry.
func TestSettlementFailureIsNotAmbiguousAndLeavesTheDeliverySettleable(t *testing.T) {
	c, srv := scripted(t, func(cfg *gps.Config) {
		// The exactly-once path is the only one whose settlement has an
		// outcome to report at all: on an ordinary subscription the client
		// library readies the acknowledgement immediately with success.
		cfg.ExactlyOnce = true
		cfg.AckDeadline = time.Minute
	})
	topic := newTopic("ackfail")
	provision(t, c, topic, "s")
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	d := receiveOne(t, cons, 5*time.Second)

	srv.AckErr = func(string) error {
		return status.Error(codes.Unavailable, "injected: the acknowledgement did not reach the service")
	}
	aerr := d.Ack()
	if aerr == nil {
		t.Fatal("the injected acknowledgement failure was not reported")
	}
	if mq.IsAmbiguous(aerr) {
		t.Fatalf("a failed acknowledgement reported an AMBIGUOUS outcome (%v): the message is still unacknowledged, "+
			"so it will be redelivered and the outcome is known", aerr)
	}
	if errors.Is(aerr, mq.ErrSettlementUnknown) {
		t.Fatalf("a failed acknowledgement reported ErrSettlementUnknown (%v), which the specification makes sticky: "+
			"nothing later could turn it into a success, and this settlement can simply be retried", aerr)
	}
	if code := mq.CodeOf(aerr); code == mq.CodeUnknown {
		t.Fatalf("the failure carries no judgement: %v", aerr)
	}
	// And the delivery is settleable again, which is what makes the refusal a
	// refusal rather than a loss.
	srv.AckErr = nil
	if err := d.Ack(); err != nil {
		t.Fatalf("the delivery is not settleable after a failed acknowledgement: %v", err)
	}
}

// TestSettlementAfterTheLeaseIsRefusedBeforeTheCall is the local lease check
// from the other direction: not a redelivery, just time passing.
//
// It is the case no API can answer — the service accepts an acknowledgement for
// a lease that has gone — so the refusal has to be local and it has to happen
// before the call.
func TestSettlementAfterTheLeaseIsRefusedBeforeTheCall(t *testing.T) {
	var clk testClock
	clk.set(time.Unix(1_700_000_000, 0))
	c, srv := scripted(t, func(cfg *gps.Config) {
		cfg.AckDeadline = time.Second
		cfg.MaxExtension = time.Second
		cfg.Clock = &clk
	})
	topic := newTopic("leasegone")
	provision(t, c, topic, "s")
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	d := receiveOne(t, cons, 5*time.Second)
	// The lease was one second on the driver's own clock, and the clock has
	// moved an hour. The service has not been told anything.
	clk.advance(time.Hour)
	if err := d.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("acknowledging past the lease reported %v (code %v), want ErrLeaseLost: "+
			"the service would accept it and would acknowledge a message another consumer now holds", err, mq.CodeOf(err))
	}
	// Nothing reached the service: the message is still outstanding there,
	// which is what "refused before the call" means.
	sub, serr := gps.Config{ProjectID: "iqmq-test"}.SubscriptionID(topic, "s")
	if serr != nil {
		t.Fatalf("SubscriptionID: %v", serr)
	}
	if got := srv.Outstanding(sub); got != 1 {
		t.Fatalf("the message is no longer outstanding at the service (outstanding %d): "+
			"the refusal was not made before the call", got)
	}
}
