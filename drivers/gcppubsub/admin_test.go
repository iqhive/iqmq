package gcppubsub_test

import (
	"errors"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	gps "bitbucket.org/iqhive/iqmq/drivers/gcppubsub"
	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psfake"
)

// The Admin surface, driven against the in-process fake.
//
// admin.go was 38.3 % covered and the fake it runs on was 62.9 %: the
// creating paths ran in every test that provisioned a topic, and the
// decisions -- what makes a creation idempotent, what is NOT reconfigured,
// what the driver refuses to answer -- ran nowhere.

// adminRig is a client, its fake and the admin surface over them.
func adminRig(t *testing.T, tweak ...func(*gps.Config)) (mq.Admin, *psfakeServer) {
	t.Helper()
	c, srv := scripted(t, tweak...)
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	return admin, srv
}

// TestEnsureTopicIsIdempotentWithoutLookingFirst.
//
// "Already exists" is what makes the creation idempotent, and taking it from
// the CREATE rather than from a GetTopic first is the decision: a check-then-
// create has a window, and two processes that both read "absent" would have
// one of them fail on a topic that is now there either way.
func TestEnsureTopicIsIdempotentWithoutLookingFirst(t *testing.T) {
	t.Parallel()
	admin, srv := adminRig(t)
	topic := newTopic("ensure-idem")

	var ops []string
	srv.AdminErr = func(op, _ string) error {
		ops = append(ops, op)
		return nil
	}

	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: topic}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: topic}); err != nil {
		t.Fatalf("EnsureTopic a second time: %v; an AlreadyExists is what makes it idempotent", err)
	}
	for _, op := range ops {
		if op == "TopicExists" {
			t.Errorf("EnsureTopic looked the topic up before creating it (%v); a check-then-create has a "+
				"window in which two processes both read \"absent\" and one of them then fails", ops)
			break
		}
	}
}

// TestEnsureSubscriptionCarriesTheConfigurationIntoTheSpec.
//
// The specification keeps a driver's own typed administration as the advanced API, and
// for this driver that is Config's subscription fields. A portable
// EnsureSubscription therefore has to carry them: a subscription created
// without the filter or the dead-letter policy the deployment configured is
// not the subscription the deployment asked for, and nothing later notices,
// because Ensure does not reconfigure what already exists.
func TestEnsureSubscriptionCarriesTheConfigurationIntoTheSpec(t *testing.T) {
	t.Parallel()
	topic, dlq := newTopic("cfg"), newTopic("cfg-dead")
	admin, srv := adminRig(t, func(c *gps.Config) {
		c.Filter = `attributes.kind = "order"`
		c.ExactlyOnce = true
		c.Ordering = true
		c.RetainAcked = true
		c.Retention = 3 * time.Hour
		c.DeadLetterTopic = dlq
		c.MaxDeliveryAttempts = 7
		c.AckDeadline = 45 * time.Second
	})

	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: topic, Name: "workers"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}

	info := subInfo(t, srv, topic, "workers")
	if !info.Exists {
		t.Fatal("EnsureSubscription returned nil and created no subscription")
	}
	if info.Filter != `attributes.kind = "order"` {
		t.Errorf("the subscription's filter is %q, want the configured one: a subscription created without it "+
			"receives everything, and Ensure will not reconfigure it later", info.Filter)
	}
	if !info.ExactlyOnce {
		t.Error("the subscription was created without exactly-once delivery, which the configuration asked for")
	}
	if !info.Ordering {
		t.Error("the subscription was created without ordering, which the configuration asked for")
	}
	if !info.RetainAcked {
		t.Error("the subscription was created without acknowledged-message retention; FeatureReplay depends on it")
	}
	if info.Retention != 3*time.Hour {
		t.Errorf("the subscription's retention is %s, want 3h", info.Retention)
	}
	if info.DeadLetterTopic == "" {
		t.Error("the subscription was created with no dead-letter topic, which the configuration named")
	}
	if info.MaxDeliveryAttempts != 7 {
		t.Errorf("the subscription's delivery-attempt limit is %d, want 7", info.MaxDeliveryAttempts)
	}
	if info.AckDeadline != 45*time.Second {
		t.Errorf("the subscription's acknowledgement deadline is %s, want the configured 45s", info.AckDeadline)
	}
}

// TestEnsureSubscriptionLeavesAnExistingOneAlone.
//
// Ensure creates what is MISSING. An UpdateSubscription that changed
// a filter or a dead-letter policy underneath a running consumer would change
// what that consumer receives without anybody asking for it -- and a
// provisioning step that runs on every deployment would do it silently.
func TestEnsureSubscriptionLeavesAnExistingOneAlone(t *testing.T) {
	t.Parallel()
	topic := newTopic("no-reconfigure")

	// Created with one filter...
	admin, srv := adminRig(t, func(c *gps.Config) { c.Filter = `attributes.kind = "first"` })
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: topic, Name: "workers"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	before := subInfo(t, srv, topic, "workers")

	// ...and asked for again through a client configured with another. The
	// second client shares the fake, so it is the same subscription.
	second := clientOver(t, srv, func(c *gps.Config) { c.Filter = `attributes.kind = "second"` })
	admin2, err := second.Admin()
	if err != nil {
		t.Fatalf("Admin() on the second client: %v", err)
	}
	if err := admin2.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: topic, Name: "workers"}); err != nil {
		t.Fatalf("EnsureSubscription a second time: %v", err)
	}

	after := subInfo(t, srv, topic, "workers")
	if after.Filter != before.Filter {
		t.Errorf("EnsureSubscription changed an existing subscription's filter from %q to %q; Ensure "+
			"creates what is missing, and changing it alters what a running consumer receives without "+
			"anybody asking", before.Filter, after.Filter)
	}
}

// TestDeletingSomethingThatIsNotThereIsSuccess.
//
// Both deletes are idempotent: the caller asked for the thing to be gone and
// it is. A NotFound surfaced as a failure makes every teardown path need a
// pre-check that has the same window a create-then-check does.
func TestDeletingSomethingThatIsNotThereIsSuccess(t *testing.T) {
	t.Parallel()
	admin, _ := adminRig(t)
	missing := newTopic("never-created")

	if err := admin.DeleteTopic(t.Context(), missing); err != nil {
		t.Errorf("DeleteTopic for a topic that does not exist is %v, want nil", err)
	}
	if err := admin.DeleteSubscription(t.Context(), missing, "workers"); err != nil {
		t.Errorf("DeleteSubscription for one that does not exist is %v, want nil", err)
	}
}

// TestStatsIsRefusedRatherThanAnsweredWithZeroes.
//
// Pub/Sub does not answer this question through its own API at all: the
// undelivered-message count and the oldest-unacknowledged age are Cloud
// Monitoring metrics -- a different service, a different credential scope and
// a sampling delay of minutes. The rule that an unknown is never an
// overloaded zero makes the answer a refusal, because three zeroes would tell
// a caller their subscription is empty and up to date.
func TestStatsIsRefusedRatherThanAnsweredWithZeroes(t *testing.T) {
	t.Parallel()
	admin, _ := adminRig(t)
	topic := newTopic("stats")
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: topic, Name: "workers"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}

	stats, err := admin.Stats(t.Context(), topic, "workers")
	if err == nil {
		t.Fatalf("Stats answered %+v with no error; Pub/Sub does not report this through its own API and "+
			"three zeroes would read as an empty, caught-up subscription", stats)
	}
	if got := mq.CodeOf(err); got != mq.CodeUnsupported {
		t.Errorf("Stats is code %v, want CodeUnsupported", got)
	}
	if stats.Depth.Known || stats.InFlight.Known || stats.Lag.Known {
		t.Errorf("the refused Stats still claims to know something: %+v", stats)
	}
}

// TestPurgeDiscardsTheBacklog.
//
// On this broker a purge is a Seek FORWARD, which acknowledges the whole
// backlog in one call and needs no retained messages -- which is why it is
// native here while seeking backwards is conditional on RetainAcked.
func TestPurgeDiscardsTheBacklog(t *testing.T) {
	t.Parallel()
	c, srv := scripted(t)
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	topic := newTopic("purge")
	provision(t, c, topic, "workers")

	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	for range 3 {
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("publishing: %v", err)
		}
	}
	if got := srv.Backlog(subID(t, topic, "workers")); got == 0 {
		t.Fatal("nothing reached the backlog, so a purge would prove nothing")
	}

	if err := admin.Purge(t.Context(), topic, "workers"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if got := srv.Backlog(subID(t, topic, "workers")); got != 0 {
		t.Errorf("the subscription still holds %d messages after Purge, want 0", got)
	}
}

// TestListTopicsTellsAStopApartFromAFailure.
//
// The iterator wraps a callback-driven listing, and a caller's `break` makes
// that callback return false. Both ends are pinned here: stopping early
// yields no error, and a listing that genuinely fails yields one rather than
// ending quietly, which would read as "this project has no topics".
//
// What is NOT pinned is the `stopped` flag itself. It exists for a listing
// that returns an error BECAUSE its callback aborted it, and this fake
// returns nil in that case, so removing the flag leaves every test here
// green. Whether the generated client reports an abort as an error is the
// container arm's to answer; the flag is the conservative reading of it.
func TestListTopicsTellsAStopApartFromAFailure(t *testing.T) {
	t.Parallel()
	admin, srv := adminRig(t)
	for _, n := range []string{newTopic("l1"), newTopic("l2"), newTopic("l3")} {
		if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: n}); err != nil {
			t.Fatalf("EnsureTopic: %v", err)
		}
	}

	// A caller who stops early sees no error.
	var got int
	for _, err := range admin.ListTopics(t.Context()) {
		got++
		if err != nil {
			t.Fatalf("a caller who stopped early was handed the error %v", err)
		}
		break
	}
	if got != 1 {
		t.Errorf("a caller that broke out of ListTopics saw %d topics, want 1", got)
	}

	// A caller who does NOT stop early sees a real failure.
	boom := errors.New("the listing failed")
	srv.AdminErr = func(op, _ string) error {
		if op == "ListTopics" {
			return boom
		}
		return nil
	}
	var last error
	var n int
	for _, err := range admin.ListTopics(t.Context()) {
		n++
		last = err
	}
	if last == nil {
		t.Errorf("ListTopics over a failing listing yielded %d topics and no error; an empty iteration reads "+
			"as \"this project has no topics\"", n)
	}
}

// TestAnAdministrativeFailureIsMapped: a failure that is neither
// AlreadyExists nor NotFound reaches the caller as an error, rather than
// being swallowed by the idempotence the other two get.
func TestAnAdministrativeFailureIsMapped(t *testing.T) {
	t.Parallel()
	admin, srv := adminRig(t)
	boom := errors.New("the service refused")
	srv.AdminErr = func(string, string) error { return boom }
	topic := newTopic("mapped")

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"EnsureTopic", func() error { return admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: topic}) }},
		{"DeleteTopic", func() error { return admin.DeleteTopic(t.Context(), topic) }},
		{"EnsureSubscription", func() error {
			return admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: topic, Name: "workers"})
		}},
		{"DeleteSubscription", func() error { return admin.DeleteSubscription(t.Context(), topic, "workers") }},
		{"Purge", func() error { return admin.Purge(t.Context(), topic, "workers") }},
	} {
		if err := tc.call(); err == nil {
			t.Errorf("%s returned nil while every administrative call was failing; only AlreadyExists and "+
				"NotFound are the ones idempotence swallows", tc.name)
		}
	}
}

// TestAdminIsRefusedOnAClosedClient.
//
// Cancellation is NOT tested here, and deliberately: this fake ignores the
// context entirely, so a cancellation test against it would pass whether the
// driver passed the caller's context down or dropped it. The transport
// honouring a dead context is the container arm's to prove.
//
// CheckOpen is this driver's own guard and is testable: a closed client has
// no API to administer through, and the surface has to say so rather than
// hand back one that panics on first use.
func TestAdminIsRefusedOnAClosedClient(t *testing.T) {
	t.Parallel()
	c, _ := scripted(t)
	if err := c.Close(); err != nil {
		t.Fatalf("closing the client: %v", err)
	}
	if _, err := c.Admin(); err == nil {
		t.Error("Admin() on a closed client returned a surface; there is no API left to administer through")
	}
}

// subID is the fake's own name for a subscription, which is what the driver
// derives from the portable topic and subscription names.
func subID(t *testing.T, topic, sub string) string {
	t.Helper()
	id, err := (gps.Config{ProjectID: "iqmq-test"}).SubscriptionID(topic, sub)
	if err != nil {
		t.Fatalf("SubscriptionID(%s/%s): %v", topic, sub, err)
	}
	return id
}

// clientOver opens a SECOND client over an existing fake, for the tests whose
// subject is what one client does to another's topology.
func clientOver(t *testing.T, srv *psfakeServer, tweak ...func(*gps.Config)) *mq.Client {
	t.Helper()
	cfg := gps.Config{ProjectID: srv.Project(), AutoCreate: true}
	for _, f := range tweak {
		f(&cfg)
	}
	b, err := gps.BackendWithAPI(t.Context(), srv, driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	c := gps.ClientFor(b, cfg)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// psfakeServer is the fake `scripted` hands back, named once here so the
// helpers below read as sentences.
type psfakeServer = psfake.Server

// subInfo reads a subscription's configuration back off the fake.
func subInfo(t *testing.T, srv *psfakeServer, topic, sub string) psapi.SubscriptionInfo {
	t.Helper()
	info, err := srv.Admin().GetSubscription(t.Context(), subID(t, topic, sub))
	if err != nil {
		t.Fatalf("GetSubscription(%s/%s): %v", topic, sub, err)
	}
	return info
}
