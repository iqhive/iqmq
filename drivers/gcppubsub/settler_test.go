package gcppubsub_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// The settlement paths the specification makes different, and what they do when the
// service refuses.
//
// A subscription with exactly-once delivery settles through a call that
// RETURNS a result, and one without settles through a call that does not.
// That is the whole difference Config.ExactlyOnce buys, and the arm with the
// result -- the one where a settlement can FAIL and be reported -- ran
// nowhere, because every test here opens an ordinary subscription.

// exactlyOnceRig opens a client whose subscription reports exactly-once
// delivery, which is what selects the settlement calls that return a result.
func exactlyOnceRig(t *testing.T) (*mq.Client, *psfakeServer, string) {
	t.Helper()
	c, srv := scripted(t, func(cfg *gps.Config) { cfg.ExactlyOnce = true })
	topic := newTopic("settle")
	provision(t, c, topic, "workers")
	return c, srv, topic
}

// delivered publishes one message and returns the delivery.
func delivered(t *testing.T, c *mq.Client, topic string) mq.Delivery {
	t.Helper()
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "workers"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { _ = cons.Close() })
	return receiveOne(t, cons, 5*time.Second)
}

// TestAnExactlyOnceAcknowledgementReportsAServiceThatRefusedIt.
//
// This is what exactly-once delivery is FOR. An ordinary acknowledgement is
// fire-and-forget, so a caller cannot know it failed; the exactly-once call
// returns a result, and a driver that dropped it would report success for a
// message the service will redeliver -- which is the one thing the feature
// was turned on to prevent.
func TestAnExactlyOnceAcknowledgementReportsAServiceThatRefusedIt(t *testing.T) {
	t.Parallel()
	c, srv, topic := exactlyOnceRig(t)
	d := delivered(t, c, topic)

	boom := errors.New("the service refused the acknowledgement")
	srv.AckErr = func(string) error { return boom }

	if err := d.Ack(); err == nil {
		t.Error("Ack returned nil while the service was refusing every acknowledgement; on an exactly-once " +
			"subscription the call returns a result, and dropping it reports success for a message that " +
			"will be redelivered")
	}
}

// TestAnExactlyOnceNegativeSettlementReportsARefusalToo.
//
// The same reasoning in the other direction: a nack the service refused
// means the redelivery the caller asked for is not going to happen.
func TestAnExactlyOnceNegativeSettlementReportsARefusalToo(t *testing.T) {
	t.Parallel()
	c, srv, topic := exactlyOnceRig(t)
	d := delivered(t, c, topic)

	boom := errors.New("the service refused the negative settlement")
	// NackErr, not AckErr: the fake keeps them apart because the driver
	// takes a different call for each, and using one for both would have
	// tested the acknowledgement path twice.
	srv.NackErr = func(string) error { return boom }

	if err := d.Nack(); err == nil {
		t.Error("Nack returned nil while the service was refusing; the redelivery the caller asked for is " +
			"not going to happen and they have not been told")
	}
}

// TestAnExactlyOnceAcknowledgementWhoseWaitEndedIsAmbiguous.
//
// The client library sends the acknowledgement before the result is waited
// for, so a context that ends during the wait leaves one that may have landed.
// It used to be judged on the raw context error, which never says ambiguous,
// and the delivery went back to settleable.
func TestAnExactlyOnceAcknowledgementWhoseWaitEndedIsAmbiguous(t *testing.T) {
	t.Parallel()
	c, srv, topic := exactlyOnceRig(t)
	d := delivered(t, c, topic)

	srv.AckErr = func(string) error { return context.Canceled }
	err := d.Ack()
	if !mq.IsAmbiguous(err) && !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("an acknowledgement whose wait was cancelled reported %v, want an ambiguous outcome", err)
	}
	srv.AckErr = nil
	if err := d.Ack(); !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Errorf("a second Ack after an ambiguous one reported %v, want ErrSettlementUnknown", err)
	}
}

// TestARefusedExactlyOnceSettlementIsTheLastOneTheLibrarySends runs the real
// client library over pstest, because the behaviour under test is the
// library's: it sends one settlement per message. The first Ack, Nack,
// AckWithResult or NackWithResult takes the message out of the set the library
// extends, and every later call answers with the first one's result without
// sending anything. A refused exactly-once settlement used to hand the delivery
// back as settleable, with an UNKNOWN status mapped as retryable, so a caller
// who retried was told the same refusal again for ever, nothing reached the
// service, and Extend went on reporting a lease the library had stopped
// keeping. The refusal is now final: not retryable, and the delivery is lost,
// because the message comes back once its deadline passes.
func TestARefusedExactlyOnceSettlementIsTheLastOneTheLibrarySends(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rpc    string
		settle func(*mq.Delivery) error
	}{
		{"ack", "Acknowledge", (*mq.Delivery).Ack},
		{"nack", "ModifyAckDeadline", func(d *mq.Delivery) error { return d.Nack() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			refuse := &refuseFirstSettlement{}
			l := newLive(t, pstest.ServerReactorOption{FuncName: tc.rpc, Reactor: refuse})
			c := l.open(t, func(cfg *gps.Config) {
				cfg.ExactlyOnce = true
				cfg.AckDeadline = time.Minute
			})
			topic := newTopic("eod-refused-" + tc.name)
			provision(t, c, topic, "workers")
			d := delivered(t, c, topic)

			err := tc.settle(&d)
			if err == nil {
				t.Fatal("the refused settlement was not reported")
			}
			if mq.IsAmbiguous(err) {
				t.Fatalf("the refused settlement reported an ambiguous outcome (%v); the service answered", err)
			}
			if mq.IsRetryable(err) {
				t.Errorf("the refused settlement is retryable (%v), but the client library never sends a second "+
					"settlement for a message", err)
			}
			if err := tc.settle(&d); !errors.Is(err, mq.ErrLeaseLost) {
				t.Errorf("a second settlement after the refusal reported %v, want ErrLeaseLost: the library "+
					"stopped extending the message when the first was sent", err)
			}
			if err := d.Extend(time.Second); !errors.Is(err, mq.ErrLeaseLost) {
				t.Errorf("Extend after the refusal reported %v, want ErrLeaseLost: the library no longer keeps "+
					"the lease it would be moving", err)
			}
			if got := refuse.calls.Load(); got != 1 {
				t.Errorf("the service received %d settlement(s) for the message, want 1", got)
			}
		})
	}
}

// refuseFirstSettlement is a pstest reactor that refuses the first settlement
// it sees with UNKNOWN, a code the client library does not retry and this
// driver maps as transient, and passes everything after it to pstest. A
// ModifyAckDeadline only counts as a settlement when it is a nack, a deadline
// of zero: the library also sends one on every receipt and every extension.
type refuseFirstSettlement struct{ calls atomic.Int32 }

// React implements pstest.Reactor.
func (r *refuseFirstSettlement) React(req any) (bool, any, error) {
	if m, ok := req.(*pubsubpb.ModifyAckDeadlineRequest); ok && m.GetAckDeadlineSeconds() != 0 {
		return false, nil, nil
	}
	if r.calls.Add(1) == 1 {
		return true, nil, status.Error(codes.Unknown, "injected: the settlement is refused")
	}
	return false, nil, nil
}

// TestAnExactlyOnceSettlementThatSucceedsIsNotReportedAsAFailure.
//
// The other half, because a settler that returned an error unconditionally
// would pass both tests above and break every consumer.
func TestAnExactlyOnceSettlementThatSucceedsIsNotReportedAsAFailure(t *testing.T) {
	t.Parallel()
	c, _, topic := exactlyOnceRig(t)
	d := delivered(t, c, topic)

	if err := d.Ack(); err != nil {
		t.Errorf("Ack on an exactly-once subscription the service accepted is %v, want nil", err)
	}
}

// TestDropIsRefusedBecauseThereIsNowhereToDropIt.
//
// NackActionDrop asks the driver to discard a message without
// redelivering it, and Pub/Sub has no such settlement: the alternatives are
// acknowledging it -- which is not a drop, it is a success -- or holding it
// until the deadline, which redelivers. Refusing is what the specification requires of a
// thing the broker cannot do.
func TestDropIsRefusedBecauseThereIsNowhereToDropIt(t *testing.T) {
	t.Parallel()
	c, _, topic := exactlyOnceRig(t)
	d := delivered(t, c, topic)

	err := d.Nack(mq.NackDrop())
	if got := mq.CodeOf(err); got != mq.CodeUnsupported {
		t.Errorf("a drop is %v (code %v), want CodeUnsupported: acknowledging is not a drop, it is a "+
			"success, and holding it until the deadline redelivers", err, got)
	}
}

// TestANackOptionNamingNoParameterIsRefused, rather than defaulted.
func TestANackOptionNamingNoParameterIsRefused(t *testing.T) {
	t.Parallel()
	c, _, topic := exactlyOnceRig(t)
	d := delivered(t, c, topic)

	// The zero NackOption names no parameter, which is what makes a stray
	// one an error rather than a silent requeue -- the settlement a caller
	// did not ask for.
	if err := d.Nack(mq.NackOption{}); err == nil {
		t.Error("a NackOption naming no parameter was accepted; the zero has to be an error rather than a " +
			"silent requeue")
	}
}

var _ = psapi.SubscriptionInfo{}
