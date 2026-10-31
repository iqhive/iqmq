package activemq_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	amq "github.com/iqhive/iqmq/drivers/activemq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
)

// An Artemis queue name is unique across the whole broker, not per address,
// and a subscription's queue is `<topic>.<subscription>`. So a fan-out topic
// "orders" with subscription "billing" and a JMS queue "orders.billing" (this
// driver's own AddressQueue topic of that name, or any other application's)
// derive one queue name on two addresses. The tests here need a real Artemis:
// what the broker answers is the whole question.

// artemisAt is the Artemis broker IQMQ_ARTEMIS_ADDR names, with the
// credentials TestConformance reads, or a skip.
func artemisAt(t *testing.T) amq.Config {
	t.Helper()
	addr := os.Getenv("IQMQ_ARTEMIS_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_ARTEMIS_ADDR (and IQMQ_ACTIVEMQ_USER, IQMQ_ACTIVEMQ_PASSWORD) to run this against " +
			"an Artemis broker; what the broker answers is the whole question")
	}
	return amq.Config{
		Broker:            activemq.Artemis,
		Addrs:             []string{addr},
		Username:          os.Getenv("IQMQ_ACTIVEMQ_USER"),
		Password:          dsn.Secret(os.Getenv("IQMQ_ACTIVEMQ_PASSWORD")),
		InsecurePlainSASL: true,
		// A prefix of the run's own, so that two applications in one run
		// collide with each other and with nothing a previous run left.
		Prefix: "iqmq.collision." + strconv.FormatInt(time.Now().UnixNano(), 36) + ".",
	}
}

// adminOf is cfg's Admin, closed with the test.
func adminOf(ctx context.Context, t *testing.T, cfg amq.Config) mq.Admin {
	t.Helper()
	c, err := amq.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	adm, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	return adm
}

// TestArtemisEnsureSubscriptionIsNotSatisfiedByAQueueOnAnotherAddress is
// Admin's half, both ways round. EnsureSubscription met AMQ229019 "Queue
// orders.billing already exists on address orders.billing" and reported the
// subscription as there; under AddressQueue, EnsureTopic("orders.billing")
// did the same over a subscription's queue. Each is the broker's conflict,
// and ensuring the queue on its own address again is still success.
func TestArtemisEnsureSubscriptionIsNotSatisfiedByAQueueOnAnotherAddress(t *testing.T) {
	cfg := artemisAt(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	qcfg := cfg
	qcfg.Addressing = amq.AddressQueue
	fanout, jms := adminOf(ctx, t, cfg), adminOf(ctx, t, qcfg)
	sub := mq.SubscriptionSpec{Topic: "orders", Name: "billing"}

	// The JMS queue first, then the subscription over it.
	if err := jms.EnsureTopic(ctx, mq.TopicSpec{Name: "orders.billing"}); err != nil {
		t.Fatalf("the other application's EnsureTopic: %v", err)
	}
	err := fanout.EnsureSubscription(ctx, sub)
	if code := mq.CodeOf(err); code != mq.CodeConflict {
		t.Errorf("EnsureSubscription(orders, billing) over the JMS queue orders.billing returned %v (code %v), "+
			"want Conflict: the subscription does not exist and its queue name belongs to another application",
			err, code)
	}
	if err := jms.DeleteTopic(ctx, "orders.billing"); err != nil {
		t.Fatalf("DeleteTopic(orders.billing) under AddressQueue: %v", err)
	}

	// The subscription first, then the JMS queue over it.
	if err := fanout.EnsureSubscription(ctx, sub); err != nil {
		t.Fatalf("EnsureSubscription(orders, billing) with no collision: %v", err)
	}
	t.Cleanup(func() { _ = fanout.DeleteTopic(context.WithoutCancel(ctx), "orders") })
	if err := fanout.EnsureSubscription(ctx, sub); err != nil {
		t.Errorf("EnsureSubscription(orders, billing) a second time: %v, want success", err)
	}
	err = jms.EnsureTopic(ctx, mq.TopicSpec{Name: "orders.billing"})
	// The address the refused EnsureTopic created; the queue is the
	// subscription's and DeleteTopic leaves it alone.
	t.Cleanup(func() { _ = jms.DeleteTopic(context.WithoutCancel(ctx), "orders.billing") })
	if code := mq.CodeOf(err); code != mq.CodeConflict {
		t.Errorf("EnsureTopic(orders.billing) under AddressQueue over the subscription queue orders.billing "+
			"returned %v (code %v), want Conflict", err, code)
	}
}
