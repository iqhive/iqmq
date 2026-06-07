package gcppubsub_test

import (
	"errors"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	gps "bitbucket.org/iqhive/iqmq/drivers/gcppubsub"
	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
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
