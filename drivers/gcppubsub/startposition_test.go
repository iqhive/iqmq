package gcppubsub_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestStartEarliestOnANewSubscriptionIsRefused holds the driver to what a
// Pub/Sub subscription can read. Its backlog begins at its own creation, so a
// subscription the open creates reads nothing the topic was sent before.
// OpenSource used to accept StartEarliest, create the subscription and read
// from there, which is StartLatest under another name: a consumer that asked
// for the topic's history received none of it and was told nothing. It is
// refused now with ErrUnsupported, before the subscription is created.
//
// A subscription that exists has a position of its own, its backlog, and
// StartEarliest leaves it there; a new one opened at StartLatest begins at its
// creation, which is what that asks for.
func TestStartEarliestOnANewSubscriptionIsRefused(t *testing.T) {
	c, srv := scripted(t)
	topic := newTopic("earliest")
	ctx := context.Background()
	provision(t, c, topic, "existing")
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("history")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	late, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartEarliest()})
	if err == nil {
		_ = late.Close()
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("opening a new subscription at StartEarliest = %v, want ErrUnsupported: it reads from its own "+
			"creation, so none of the topic's history reaches it", err)
	}
	if subInfo(t, srv, topic, "late").Exists {
		t.Error("the refused open created the subscription anyway, so the next open at StartEarliest finds it and reads from its creation")
	}

	existing, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "existing", Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("opening an existing subscription at StartEarliest: %v", err)
	}
	defer func() { _ = existing.Close() }()
	if d := receiveOne(t, existing, 5*time.Second); string(d.Body) != "history" {
		t.Errorf("the existing subscription delivered %q, want the message in its backlog", d.Body)
	}

	fresh, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "fresh", Start: mq.StartLatest()})
	if err != nil {
		t.Fatalf("opening a new subscription at StartLatest: %v", err)
	}
	_ = fresh.Close()
}
