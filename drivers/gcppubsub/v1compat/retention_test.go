package v1compat_test

import (
	"context"
	"errors"
	"testing"
	"time"

	//nolint:staticcheck // as in client.go: the deprecated v1 client library is what this package exists to wrap
	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"

	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	v1 "github.com/iqhive/iqmq/drivers/gcppubsub/v1compat"
)

// TestTheV1ArmKeepsATopicsRetention holds this arm to the v2 arm's reading of
// topic message retention, against pstest: a topic created with retention
// reports it back, one created without reports zero, a missing one is
// ErrNotFound, and a topic qualified with another project is refused rather
// than created here, as CreateTopic refuses it.
//
// The two methods are reached through a type assertion rather than through
// psapi.Admin because this module builds outside the workspace against a
// released drivers/gcppubsub whose interface may not have them yet.
func TestTheV1ArmKeepsATopicsRetention(t *testing.T) {
	conn := service(t)
	ctx := t.Context()
	native, err := pubsub.NewClient(ctx, "iqmq-compat", option.WithGRPCConn(conn), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("building the v1 client: %v", err)
	}
	t.Cleanup(func() { _ = native.Close() })
	admin, ok := v1.Wrap(native).Admin().(interface {
		CreateTopic(ctx context.Context, topic string) error
		CreateRetainedTopic(ctx context.Context, topic string, retention time.Duration) error
		TopicRetention(ctx context.Context, topic string) (time.Duration, error)
	})
	if !ok {
		t.Fatal("the v1 arm's administration has no CreateRetainedTopic or TopicRetention")
	}

	if err := admin.CreateRetainedTopic(ctx, "kept", time.Hour); err != nil {
		t.Fatalf("CreateRetainedTopic: %v", err)
	}
	if err := admin.CreateTopic(ctx, "plain"); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for topic, want := range map[string]time.Duration{"kept": time.Hour, "plain": 0} {
		got, err := admin.TopicRetention(ctx, topic)
		if err != nil || got != want {
			t.Errorf("TopicRetention(%s) = %v, %v; want %v", topic, got, err, want)
		}
	}
	if _, err := admin.TopicRetention(ctx, "absent"); !errors.Is(err, psapi.ErrNotFound) {
		t.Errorf("TopicRetention of a missing topic = %v, want ErrNotFound", err)
	}
	if err := admin.CreateRetainedTopic(ctx, "projects/other/topics/kept", time.Hour); err == nil {
		t.Error("CreateRetainedTopic in another project succeeded; the v1 API can only create in the client's project")
	}
}
