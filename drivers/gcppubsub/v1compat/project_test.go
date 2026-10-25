package v1compat_test

import (
	"context"
	"testing"
	"time"

	//nolint:staticcheck // as in client.go: the deprecated v1 client library is what this package exists to wrap
	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"

	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	v1 "github.com/iqhive/iqmq/drivers/gcppubsub/v1compat"
)

// TestAQualifiedNameIsResolvedInTheProjectItNames holds the shim to the v2
// arm's reading of a fully qualified resource name: "projects/other/topics/t"
// is topic t in project "other", not a topic t in the client's own project.
// The shim used to keep only the last segment, so a publication to another
// project's topic went to (or failed on) the client's project, and creating
// one quietly created its namesake here.
func TestAQualifiedNameIsResolvedInTheProjectItNames(t *testing.T) {
	conn := service(t)
	ctx := t.Context()
	open := func(project string) *pubsub.Client {
		cl, err := pubsub.NewClient(ctx, project, option.WithGRPCConn(conn), option.WithoutAuthentication())
		if err != nil {
			t.Fatalf("building the v1 client for %s: %v", project, err)
		}
		t.Cleanup(func() { _ = cl.Close() })
		return cl
	}
	other := open("other")
	native := open("iqmq-compat")
	c := v1.Wrap(native)

	if _, err := other.CreateTopic(ctx, "orders"); err != nil {
		t.Fatalf("creating the other project's topic: %v", err)
	}
	if _, err := other.CreateSubscription(ctx, "workers", pubsub.SubscriptionConfig{Topic: other.Topic("orders")}); err != nil {
		t.Fatalf("creating the other project's subscription: %v", err)
	}
	const topic, sub = "projects/other/topics/orders", "projects/other/subscriptions/workers"

	admin := c.Admin()
	ok, err := admin.TopicExists(ctx, topic)
	if err != nil || !ok {
		t.Fatalf("TopicExists(%s) = %v, %v; want true: it was looked up in the client's own project", topic, ok, err)
	}
	info, err := admin.GetSubscription(ctx, sub)
	if err != nil || !info.Exists {
		t.Fatalf("GetSubscription(%s) = %+v, %v; want it found", sub, info, err)
	}

	pub, err := c.Publisher(psapi.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer pub.Stop()
	if _, err := pub.Publish(ctx, &psapi.Outgoing{Data: []byte("hello")}).Get(ctx); err != nil {
		t.Fatalf("publishing to %s: %v", topic, err)
	}

	s, err := c.Subscription(psapi.SubscriptionConfig{Subscription: sub})
	if err != nil {
		t.Fatalf("Subscription: %v", err)
	}
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var got string
	if err := s.Receive(rctx, func(_ context.Context, m *psapi.Incoming) {
		got = string(m.Data)
		m.Settle.Ack()
		cancel()
	}); err != nil {
		t.Fatalf("Receive(%s): %v", sub, err)
	}
	if got != "hello" {
		t.Fatalf("received %q from %s, want hello", got, sub)
	}

	// The v1 API creates only in the client's project, so a name qualified
	// with another one is refused rather than created here under its last
	// segment.
	if err := admin.CreateTopic(ctx, "projects/other/topics/refunds"); err == nil {
		t.Error("CreateTopic in another project succeeded; the v1 API can only create in the client's project")
	}
	if ok, _ := native.Topic("refunds").Exists(ctx); ok {
		t.Error("CreateTopic of another project's topic created its namesake in the client's project")
	}
	err = admin.CreateSubscription(ctx, psapi.SubscriptionSpec{Topic: topic, Subscription: "projects/other/subscriptions/audit"})
	if err == nil {
		t.Error("CreateSubscription in another project succeeded; the v1 API can only create in the client's project")
	}
	// A subscription in the client's project may still follow another
	// project's topic.
	if err := admin.CreateSubscription(ctx, psapi.SubscriptionSpec{Topic: topic, Subscription: "local"}); err != nil {
		t.Fatalf("subscribing locally to %s: %v", topic, err)
	}
	if info, err := admin.GetSubscription(ctx, "local"); err != nil || !info.Exists || info.Topic != "orders" {
		t.Fatalf("GetSubscription(local) = %+v, %v", info, err)
	}
	cfg, err := native.Subscription("local").Config(ctx)
	if err != nil || cfg.Topic.String() != topic {
		t.Fatalf("the local subscription follows %v (%v), want %s", cfg.Topic, err, topic)
	}
}
