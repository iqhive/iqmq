package gcppubsub_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
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

// TestSettlementFailureIsNotAmbiguousAndTheMessageComesBack is the other half,
// and it is the one a caller is most likely to get wrong.
//
// An acknowledgement whose request failed leaves the message unacknowledged at
// the service, so it is redelivered: the outcome is known (the settlement did
// not happen). A driver that reported ErrSettlementUnknown here would tell a
// caller nothing later can turn into a success, for a message that will come
// back to be settled again. It comes back as a redelivery and not through the
// same delivery: the client library sends one settlement per message, so the
// refused one was this delivery's last, and a second reports ErrLeaseLost.
func TestSettlementFailureIsNotAmbiguousAndTheMessageComesBack(t *testing.T) {
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
			"nothing later could turn it into a success, and the message will be delivered again", aerr)
	}
	if code := mq.CodeOf(aerr); code == mq.CodeUnknown {
		t.Fatalf("the failure carries no judgement: %v", aerr)
	}
	srv.AckErr = nil
	if err := d.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a second Ack of the delivery whose acknowledgement was refused reported %v, want ErrLeaseLost: "+
			"the client library sends nothing for it after the first", err)
	}
	// And the message comes back, which is what makes the refusal a refusal
	// rather than a loss.
	if n := srv.Expire(); n != 1 {
		t.Fatalf("the service holds %d outstanding message(s) after the refusal, want the refused one", n)
	}
	again := receiveOne(t, cons, 5*time.Second)
	if err := again.Ack(); err != nil {
		t.Fatalf("acknowledging the redelivery: %v", err)
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

// TestADeadLetterPublicationWhoseWaitEndedIsAmbiguous is the Reject half of
// TestAPublicationWhoseWaitEndedIsAmbiguous. A Reject publishes the message to
// the dead-letter topic before it acknowledges the original, and a wait for
// that publication that ended -- the caller's context, or the library's own
// publish timeout -- leaves a copy the service may have stored. The driver
// mapped it with Map, so the Reject reported a definite DeadlineExceeded, the
// delivery stayed settleable, and a retried Reject put a second copy in the
// dead-letter topic. It is now ambiguous, a second settlement is refused, and
// the original is still the service's to redeliver.
func TestADeadLetterPublicationWhoseWaitEndedIsAmbiguous(t *testing.T) {
	dlq := newTopic("dlq-wait")
	c, srv := scripted(t, func(cfg *gps.Config) {
		cfg.DeadLetterTopic = dlq
		cfg.MaxDeliveryAttempts = 5
		cfg.AckDeadline = time.Minute
	})
	topic := newTopic("reject-wait")
	provision(t, c, topic, "s")
	provision(t, c, dlq, "d")
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("poison")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	d := receiveOne(t, cons, 5*time.Second)

	var copies atomic.Int64
	srv.PublishErr = func(topic string, _ *psapi.Outgoing) error {
		if topic != dlq {
			return nil
		}
		if copies.Add(1) == 1 {
			return fmt.Errorf("waiting for the publish result: %w", context.DeadlineExceeded)
		}
		return nil
	}
	rerr := d.Reject()
	if rerr == nil {
		t.Fatal("the injected failure was not reported")
	}
	if !mq.IsAmbiguous(rerr) {
		t.Fatalf("Reject = %v (code %v): IsAmbiguous = false; the dead-letter publication had left when the wait ended, "+
			"so the service may hold the copy and a retry would store a second one", rerr, mq.CodeOf(rerr))
	}
	if err := d.Reject(); !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("a second Reject after an ambiguous one = %v, want ErrSettlementUnknown", err)
	}
	if got := copies.Load(); got != 1 {
		t.Fatalf("the dead-letter topic was published to %d times, want once: the retried Reject duplicated the copy", got)
	}
	sub, err := gps.Config{ProjectID: "iqmq-test"}.SubscriptionID(topic, "s")
	if err != nil {
		t.Fatalf("SubscriptionID: %v", err)
	}
	if got := srv.Outstanding(sub); got != 1 {
		t.Fatalf("the original is no longer outstanding after an ambiguous dead-letter publication (outstanding %d): "+
			"nothing may acknowledge it while nobody knows whether the copy exists", got)
	}
	// Close still gives the original back: the client library holds it until
	// it is settled, and its shutdown waits for that.
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := srv.Backlog(sub); got != 1 {
		t.Fatalf("the original is not deliverable again after Close (backlog %d)", got)
	}
}

// TestADeadLetterPublicationIsClassifiedAsAPublication is the service-answer
// half of TestADeadLetterPublicationWhoseWaitEndedIsAmbiguous. The dead-letter
// publication was mapped by the SETTLEMENT rules, which read every service
// answer as definite, so a DEADLINE_EXCEEDED (which can always come after the
// service stored the copy) or an UNAVAILABLE from a stream that broke
// mid-RPC was a Reject that had not happened: the delivery stayed settleable
// and a retried Reject put a second copy in the dead-letter topic. A
// dead-letter publication now answers what an ordinary publication that met the
// same failure answers, which is the oracle here: ambiguous for the transient
// codes, with the second settlement refused and the copy published once, and
// definite for a refusal, with the delivery settleable again.
func TestADeadLetterPublicationIsClassifiedAsAPublication(t *testing.T) {
	for _, code := range []codes.Code{
		codes.Unavailable, codes.DeadlineExceeded,
		codes.ResourceExhausted, codes.Aborted, codes.Internal, codes.Unknown,
		codes.PermissionDenied, codes.NotFound,
	} {
		t.Run(code.String(), func(t *testing.T) {
			dlq := newTopic("dlq-code")
			c, srv := scripted(t, func(cfg *gps.Config) {
				cfg.DeadLetterTopic = dlq
				cfg.MaxDeliveryAttempts = 5
				cfg.AckDeadline = time.Minute
			})
			topic := newTopic("reject-code")
			provision(t, c, topic, "s")
			provision(t, c, dlq, "d")
			ctx := context.Background()
			injected := status.Error(code, "injected: the publish RPC failed")

			srv.PublishErr = func(string, *psapi.Outgoing) error { return injected }
			perr := c.Publish(ctx, topic, &mq.Message{Body: []byte("probe")})
			if perr == nil {
				t.Fatal("the injected publication failure was not reported")
			}
			ambiguous := mq.IsAmbiguous(perr)
			if (code == codes.Unavailable || code == codes.DeadlineExceeded) && !ambiguous {
				t.Fatalf("an ordinary publication that failed with %v reported %v, which is not ambiguous", code, perr)
			}

			srv.PublishErr = nil
			if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("poison")}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			defer func() { _ = cons.Close() }()
			d := receiveOne(t, cons, 5*time.Second)

			var copies atomic.Int64
			srv.PublishErr = func(topic string, _ *psapi.Outgoing) error {
				if topic != dlq {
					return nil
				}
				if copies.Add(1) == 1 {
					return injected
				}
				return nil
			}
			rerr := d.Reject()
			if rerr == nil {
				t.Fatal("the injected dead-letter failure was not reported")
			}
			if got := mq.IsAmbiguous(rerr); got != ambiguous {
				t.Fatalf("Reject after a dead-letter publication that failed with %v = %v (ambiguous %v), "+
					"but an ordinary publication that failed the same way = %v (ambiguous %v): "+
					"the dead-letter copy is a publication and may be stored exactly when that one may",
					code, rerr, got, perr, ambiguous)
			}
			sub, err := gps.Config{ProjectID: "iqmq-test"}.SubscriptionID(topic, "s")
			if err != nil {
				t.Fatalf("SubscriptionID: %v", err)
			}
			if got := srv.Outstanding(sub); got != 1 {
				t.Fatalf("the original is no longer outstanding after a failed dead-letter publication (outstanding %d)", got)
			}

			again := d.Reject()
			if ambiguous {
				if !errors.Is(again, mq.ErrSettlementUnknown) {
					t.Fatalf("a second Reject after an ambiguous one = %v, want ErrSettlementUnknown", again)
				}
				if got := copies.Load(); got != 1 {
					t.Fatalf("the dead-letter topic was published to %d times, want once: the retried Reject duplicated the copy", got)
				}
				return
			}
			if again != nil {
				t.Fatalf("a second Reject after a definite refusal = %v: the delivery is not settleable again", again)
			}
			if got := copies.Load(); got != 2 {
				t.Fatalf("the dead-letter topic was published to %d times, want twice: the refusal and the retry", got)
			}
		})
	}
}
