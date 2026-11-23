package v1compat_test

import (
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestBothArmsRefuseASubscriptionNotAttachedToItsTopic is the driver's topic
// check through each client generation: a deleted topic's subscription,
// detached, and a subscription another (topic, subscription) pair's
// identifier names, attached to that pair's topic. Both used to be accepted,
// and on this arm reading a detached subscription back panicked inside the
// v1 library.
func TestBothArmsRefuseASubscriptionNotAttachedToItsTopic(t *testing.T) {
	if have, before := gcppubsubBefore(t, "v1.1.4"); before {
		t.Skipf("drivers/gcppubsub %s checks no subscription's topic; the check is in the tree and ships "+
			"with v1.1.4", have)
	}
	conn := service(t)
	for _, arm := range a34Arms() {
		t.Run(arm.slug, func(t *testing.T) {
			c := arm.open(t, conn, armConfig(1))
			admin, err := c.Admin()
			if err != nil {
				t.Fatalf("Admin: %v", err)
			}
			detached := newName("detached")
			provision(t, c, detached, "workers")
			if err := admin.DeleteTopic(t.Context(), detached); err != nil {
				t.Fatalf("DeleteTopic: %v", err)
			}
			if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: detached}); err != nil {
				t.Fatalf("EnsureTopic: %v", err)
			}
			base := newName("collide")
			provision(t, c, base+"/b", "c")
			for _, tc := range []struct{ topic, sub, says string }{
				{detached, "workers", "detached"},
				{base, "b/c", "another topic"},
			} {
				cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: tc.topic, Subscription: tc.sub})
				if err == nil {
					_ = cons.Close()
				}
				if mq.CodeOf(err) != mq.CodeConflict || !strings.Contains(err.Error(), tc.says) {
					t.Errorf("a consumer of (%s, %s) = %v (code %v), want a CodeConflict refusal saying %q",
						tc.topic, tc.sub, err, mq.CodeOf(err), tc.says)
				}
			}
		})
	}
}
