package gcppubsub_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psfake"
)

// TestStartEarliestOnANewSubscriptionIsRefused holds the driver to what a
// Pub/Sub subscription can read. Its backlog begins at its own creation, so a
// subscription the open creates reads nothing the topic was sent before
// unless the topic retains messages and the subscription seeks into them.
// OpenSource used to accept StartEarliest, create the subscription and read
// from there, which is StartLatest under another name: a consumer that asked
// for the topic's history received none of it and was told nothing. On a
// topic with no message retention it is refused with ErrUnsupported, before
// the subscription is created, and the refusal names the setting that would
// make it work.
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
	if !strings.Contains(err.Error(), "TopicRetention") {
		t.Errorf("the refusal does not name Config.TopicRetention, the setting that makes StartEarliest work: %v", err)
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

// TestStartEarliestOnANewSubscriptionReadsWhatTheTopicRetains is the other
// half: on a topic with message retention a new subscription can read what was
// published before it existed, and StartEarliest is the request for exactly
// that. The topic here is provisioned with retention by somebody other than
// the driver, which is the case the driver has to find out about from the
// service rather than from its own configuration.
//
// The fake starts a new subscription with an empty backlog whatever the topic
// retains, as the managed service does, so the history arriving at all is the
// proof that the open sought back into the retention window. An existing
// subscription is left where it is: it already acknowledged "first", and
// StartEarliest must not hand it back.
func TestStartEarliestOnANewSubscriptionReadsWhatTheTopicRetains(t *testing.T) {
	c, srv := scripted(t)
	topic := newTopic("retained")
	srv.CreateRetainedTopic(topic, time.Hour)
	provision(t, c, topic, "existing")
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("first")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	existing, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "existing"})
	if err != nil {
		t.Fatalf("opening the existing subscription: %v", err)
	}
	first := receiveOne(t, existing, 5*time.Second)
	if err := first.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	_ = existing.Close()

	late, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("opening a new subscription at StartEarliest on a topic that retains messages: %v", err)
	}
	defer func() { _ = late.Close() }()
	if d := receiveOne(t, late, 5*time.Second); string(d.Body) != "first" {
		t.Fatalf("the new subscription delivered %q, want \"first\", published before it existed", d.Body)
	}
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("second")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if d := receiveOne(t, late, 5*time.Second); string(d.Body) != "second" {
		t.Errorf("after its history the new subscription delivered %q, want \"second\"", d.Body)
	}

	again, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "existing", Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("opening the existing subscription at StartEarliest: %v", err)
	}
	defer func() { _ = again.Close() }()
	d := receiveOne(t, again, 5*time.Second)
	if string(d.Body) != "second" {
		t.Errorf("the existing subscription delivered %q, want \"second\": StartEarliest sought it back and handed "+
			"it a message it had already acknowledged", d.Body)
	}
}

// TestStartLatestOnARetainingTopicDoesNotReplay is the conformance kit's
// start/positions check on a topic with message retention, held here because
// the emulator cannot hold it there: it hands a new subscription on a
// retaining topic the retained messages at once, and the managed service,
// which this fake follows, does not. The emulator run therefore checks
// start/positions on a topic without retention. A topic that retains is no
// reason for StartLatest to seek, so a subscription opened there after a
// publication receives only what comes next.
func TestStartLatestOnARetainingTopicDoesNotReplay(t *testing.T) {
	c, srv := scripted(t, func(cfg *gps.Config) { cfg.TopicRetention = time.Hour })
	topic := newTopic("latest")
	srv.CreateRetainedTopic(topic, time.Hour)
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	latest, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "latest", Start: mq.StartLatest()})
	if err != nil {
		t.Fatalf("opening a new subscription at StartLatest: %v", err)
	}
	defer func() { _ = latest.Close() }()
	receiveNone(t, latest, 200*time.Millisecond)
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("after")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if d := receiveOne(t, latest, 5*time.Second); string(d.Body) != "after" {
		t.Errorf("the StartLatest subscription delivered %q, want \"after\"", d.Body)
	}
}

// TestAFailedSeekLeavesNoSubscriptionBehind is the decision about the seek,
// the step that comes straight after the create. A subscription created for
// StartEarliest whose seek failed reads from its own creation, and the next
// open at StartEarliest would find it and accept it as an existing
// subscription, which is the old silent bug again. So the open deletes what it
// created and reports the seek's failure, and a retry starts from nothing and
// gets the history. The create's own failure and what fails after the seek
// have tests of their own below.
func TestAFailedSeekLeavesNoSubscriptionBehind(t *testing.T) {
	c, srv := scripted(t)
	topic := newTopic("seekfail")
	srv.CreateRetainedTopic(topic, time.Hour)
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("history")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	boom := errors.New("the service refused the seek")
	srv.AdminErr = func(op, _ string) error {
		if op == "Seek" {
			return boom
		}
		return nil
	}

	late, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartEarliest()})
	if err == nil {
		_ = late.Close()
		t.Fatal("the open succeeded although the seek into the topic's retention failed: the subscription reads from its own creation")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("the open failed with %v, want the seek's own failure", err)
	}
	if subInfo(t, srv, topic, "late").Exists {
		t.Fatal("the subscription whose seek failed was left in place; the next open at StartEarliest would " +
			"accept it as an existing subscription and read from its creation")
	}

	srv.AdminErr = nil
	late, err = c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("retrying the open: %v", err)
	}
	defer func() { _ = late.Close() }()
	if d := receiveOne(t, late, 5*time.Second); string(d.Body) != "history" {
		t.Errorf("the retried open delivered %q, want the topic's history", d.Body)
	}
}

// TestAFailedSeekWhoseCleanupFailsSaysSo is the path where the subscription
// cannot be taken back either. It then exists and reads from its creation,
// and the error has to say so in terms a caller can act on: it is a conflict
// rather than a retryable failure, because a retry would open that
// subscription as an existing one and read none of the history.
func TestAFailedSeekWhoseCleanupFailsSaysSo(t *testing.T) {
	c, srv := scripted(t)
	topic := newTopic("seekstuck")
	srv.CreateRetainedTopic(topic, time.Hour)
	boom := errors.New("the service is refusing administration")
	srv.AdminErr = func(op, _ string) error {
		if op == "Seek" || op == "DeleteSubscription" {
			return boom
		}
		return nil
	}

	late, err := c.Consumer(context.Background(), mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartEarliest()})
	if err == nil {
		_ = late.Close()
		t.Fatal("the open succeeded although neither the seek nor the cleanup did")
	}
	if !errors.Is(err, boom) {
		t.Errorf("the open failed with %v, want it to carry the service's failure", err)
	}
	if got := mq.CodeOf(err); got != mq.CodeConflict {
		t.Errorf("the open failed with code %v, want %v: the subscription exists in a state StartEarliest cannot use", got, mq.CodeConflict)
	}
	if mq.IsRetryable(err) {
		t.Error("the failure is reported as retryable, and a retry opens the half-made subscription as an existing one")
	}
	if !strings.Contains(err.Error(), "delete") {
		t.Errorf("the error does not tell the caller to delete the subscription: %v", err)
	}
	if !subInfo(t, srv, topic, "late").Exists {
		t.Error("the fake refused the deletion and the subscription is gone anyway; the test proves nothing")
	}
}

// TestACreateThatLandedTwiceIsStillSought covers the client library's own
// retry. CreateSubscription is retried on UNAVAILABLE, so a first attempt that
// created the subscription and lost its answer comes back as AlreadyExists.
// The open saw no subscription a moment before, so the subscription is at most
// moments old and it is sought all the same: not seeking it would leave this
// open reading from the subscription's creation and reporting success. What
// the open does NOT do is delete a subscription it cannot be sure it made, so
// a seek that fails there is the conflict above rather than a cleanup.
func TestACreateThatLandedTwiceIsStillSought(t *testing.T) {
	srv := psfake.New("iqmq-test")
	cfg := gps.Config{ProjectID: "iqmq-test", AutoCreate: true}
	b, err := gps.BackendWithAPI(context.Background(), createdThen{srv, psapi.ErrAlreadyExists}, driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	c := gps.ClientFor(b, cfg)
	t.Cleanup(func() { _ = c.Close() })
	topic := newTopic("landedtwice")
	srv.CreateRetainedTopic(topic, time.Hour)
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("history")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	late, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("opening at StartEarliest when the create answered AlreadyExists: %v", err)
	}
	if d := receiveOne(t, late, 5*time.Second); string(d.Body) != "history" {
		t.Errorf("the subscription delivered %q, want the topic's history", d.Body)
	}
	_ = late.Close()

	boom := errors.New("the service refused the seek")
	srv.AdminErr = func(op, _ string) error {
		if op == "Seek" {
			return boom
		}
		return nil
	}
	other, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "other", Start: mq.StartEarliest()})
	if err == nil {
		_ = other.Close()
		t.Fatal("the open succeeded although the seek failed")
	}
	if got := mq.CodeOf(err); got != mq.CodeConflict || !errors.Is(err, boom) {
		t.Errorf("the open failed with %v (code %v), want a conflict carrying the seek's failure", err, got)
	}
	if !subInfo(t, srv, topic, "other").Exists {
		t.Error("the open deleted a subscription whose create answered AlreadyExists, which may be another consumer's")
	}
}

// TestTopicRetentionIsAppliedToTopicsThisDriverCreates holds Config's half of
// the option. A topic the driver creates, through Admin.EnsureTopic or
// AutoCreate on either side, gets the configured retention, and a new
// subscription opened at StartEarliest then reads what the topic retained.
// A topic that already exists is not reconfigured, the same rule Ensure
// keeps for subscriptions, so one without retention still refuses
// StartEarliest whatever the configuration says.
func TestTopicRetentionIsAppliedToTopicsThisDriverCreates(t *testing.T) {
	c, srv := scripted(t, func(cfg *gps.Config) { cfg.TopicRetention = 2 * time.Hour })
	ctx := context.Background()
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	ensured, published, plain := newTopic("ensured"), newTopic("published"), newTopic("plain")
	if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: ensured}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	if err := c.Publish(ctx, published, &mq.Message{Body: []byte("history")}); err != nil {
		t.Fatalf("Publish with AutoCreate: %v", err)
	}
	srv.CreateTopic(plain)
	if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: plain}); err != nil {
		t.Fatalf("EnsureTopic on an existing topic: %v", err)
	}
	for topic, want := range map[string]time.Duration{ensured: 2 * time.Hour, published: 2 * time.Hour, plain: 0} {
		got, err := srv.Admin().(psapi.TopicRetainer).TopicRetention(ctx, topic)
		if err != nil {
			t.Fatalf("TopicRetention(%s): %v", topic, err)
		}
		if got != want {
			t.Errorf("topic %s has retention %s, want %s", topic, got, want)
		}
	}

	late, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: published, Subscription: "late", Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("opening a new subscription at StartEarliest: %v", err)
	}
	defer func() { _ = late.Close() }()
	if d := receiveOne(t, late, 5*time.Second); string(d.Body) != "history" {
		t.Errorf("the new subscription delivered %q, want the history published before it existed", d.Body)
	}

	refused, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: plain, Subscription: "late", Start: mq.StartEarliest()})
	if err == nil {
		_ = refused.Close()
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("StartEarliest on an existing topic without retention = %v, want ErrUnsupported", err)
	}
}

// TestStartEarliestOnATopicThatDoesNotExistYet is the AutoCreate case with
// nothing provisioned: the open creates the topic with Config.TopicRetention
// and the subscription with it, and the subscription reads everything the
// topic will ever be sent. Without the option the open is refused before it
// creates anything, the topic included.
func TestStartEarliestOnATopicThatDoesNotExistYet(t *testing.T) {
	ctx := context.Background()
	c, _ := scripted(t, func(cfg *gps.Config) { cfg.TopicRetention = time.Hour })
	topic := newTopic("unborn")
	src, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "first", Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("opening at StartEarliest on a topic that does not exist yet: %v", err)
	}
	defer func() { _ = src.Close() }()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("one")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if d := receiveOne(t, src, 5*time.Second); string(d.Body) != "one" {
		t.Errorf("delivered %q, want \"one\"", d.Body)
	}

	bare, bareSrv := scripted(t)
	other := newTopic("unborn-bare")
	refused, err := bare.Consumer(ctx, mq.ConsumerConfig{Topic: other, Subscription: "first", Start: mq.StartEarliest()})
	if err == nil {
		_ = refused.Close()
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("StartEarliest without TopicRetention on a missing topic = %v, want ErrUnsupported", err)
	}
	if _, err := bareSrv.Admin().(psapi.TopicRetainer).TopicRetention(ctx, other); !errors.Is(err, psapi.ErrNotFound) {
		t.Errorf("the refused open created the topic (%v); it should refuse before it creates anything", err)
	}
}

// TestStartAtWithTopicRetentionIsASeek keeps FeatureSeekByTime in step with
// Replay. A topic with message retention lets any attached subscription seek
// back inside it, so on such a topic StartAt is a seek even without
// Config.RetainAcked, and on a new subscription it reaches messages published
// before the subscription existed. What counts is the TOPIC's retention as the
// service reports it: a topic provisioned elsewhere with retention is enough
// with no setting at all, and Config.TopicRetention on its own is not enough
// for a topic that already exists without retention, because it applies only
// to topics this driver creates and the seek would reach nothing.
func TestStartAtWithTopicRetentionIsASeek(t *testing.T) {
	ctx := context.Background()
	c, _ := scripted(t, func(cfg *gps.Config) { cfg.TopicRetention = time.Hour })
	topic := newTopic("startat")
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("history")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	src, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartAt(time.Now().Add(-time.Minute))})
	if err != nil {
		t.Fatalf("StartAt with TopicRetention: %v", err)
	}
	defer func() { _ = src.Close() }()
	if d := receiveOne(t, src, 5*time.Second); string(d.Body) != "history" {
		t.Errorf("delivered %q, want the message published a moment before the subscription existed", d.Body)
	}

	bare, bareSrv := scripted(t)
	refused, err := bare.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartAt(time.Now())})
	if err == nil {
		_ = refused.Close()
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("StartAt with neither RetainAcked nor a retaining topic = %v, want ErrUnsupported", err)
	}

	elsewhere := newTopic("startat-provisioned")
	bareSrv.CreateRetainedTopic(elsewhere, time.Hour)
	if err := bare.Publish(ctx, elsewhere, &mq.Message{Body: []byte("kept")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	kept, err := bare.Consumer(ctx, mq.ConsumerConfig{Topic: elsewhere, Subscription: "late", Start: mq.StartAt(time.Now().Add(-time.Minute))})
	if err != nil {
		t.Fatalf("StartAt on a topic provisioned elsewhere with retention, with no setting: %v", err)
	}
	defer func() { _ = kept.Close() }()
	if d := receiveOne(t, kept, 5*time.Second); string(d.Body) != "kept" {
		t.Errorf("delivered %q, want the message published before the subscription existed", d.Body)
	}

	configured, configuredSrv := scripted(t, func(cfg *gps.Config) { cfg.TopicRetention = time.Hour })
	plain := newTopic("startat-plain")
	configuredSrv.CreateTopic(plain)
	notKept, err := configured.Consumer(ctx, mq.ConsumerConfig{Topic: plain, Subscription: "late", Start: mq.StartAt(time.Now())})
	if err == nil {
		_ = notKept.Close()
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("StartAt with Config.TopicRetention on an existing topic WITHOUT retention = %v, want ErrUnsupported: "+
			"the setting does not reach a topic that already exists, and the seek would reach nothing", err)
	}
}

// TestAFailureAfterTheCreateLeavesNothingTheRetryMisreads is the review
// finding, held as a test. The open used to create the subscription, read it
// back with GetSubscription, and only then seek, cleaning up after the seek
// alone. A GetSubscription that failed in between (UNAVAILABLE here, or the
// caller's own deadline) returned a retryable error and left the subscription
// behind un-sought, and the retry took it for an existing one and read from
// its creation. The seek now comes straight after the create, so whatever
// fails afterwards leaves a subscription whose backlog is the history.
func TestAFailureAfterTheCreateLeavesNothingTheRetryMisreads(t *testing.T) {
	c, srv := scripted(t)
	topic := newTopic("getafter")
	srv.CreateRetainedTopic(topic, time.Hour)
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("history")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	var mu sync.Mutex
	created := false
	srv.AdminErr = func(op, _ string) error {
		mu.Lock()
		defer mu.Unlock()
		switch op {
		case "CreateSubscription":
			created = true
		case "GetSubscription":
			if created {
				return status.Error(codes.Unavailable, "unavailable after the create")
			}
		}
		return nil
	}
	late, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartEarliest()})
	if err == nil {
		_ = late.Close()
		t.Fatal("the open succeeded although its GetSubscription failed")
	}
	srv.AdminErr = nil

	late, err = c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("retrying the open: %v", err)
	}
	defer func() { _ = late.Close() }()
	if d := receiveOne(t, late, 5*time.Second); string(d.Body) != "history" {
		t.Errorf("the retried open delivered %q, want the topic's history", d.Body)
	}
}

// TestACreateThatFailedAfterLandingIsUndone is the create's own ambiguity. A
// create can fail after the service made the subscription, and a subscription
// left that way reads from its creation, so the open deletes what the create
// may have left and a retry starts from nothing.
func TestACreateThatFailedAfterLandingIsUndone(t *testing.T) {
	srv := psfake.New("iqmq-test")
	cfg := gps.Config{ProjectID: "iqmq-test", AutoCreate: true}
	unavailable := status.Error(codes.Unavailable, "the answer was lost")
	b, err := gps.BackendWithAPI(context.Background(), createdThen{srv, unavailable}, driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	c := gps.ClientFor(b, cfg)
	t.Cleanup(func() { _ = c.Close() })
	topic := newTopic("landedlost")
	srv.CreateRetainedTopic(topic, time.Hour)
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("history")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	late, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartEarliest()})
	if err == nil {
		_ = late.Close()
		t.Fatal("the open succeeded although its create failed")
	}
	if !errors.Is(err, unavailable) {
		t.Errorf("the open failed with %v, want the create's own failure", err)
	}
	if subInfo(t, srv, topic, "late").Exists {
		t.Fatal("the subscription the failed create made was left in place; a retry would read it from its creation")
	}

	srvClient := clientOver(t, srv)
	late, err = srvClient.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("retrying the open through a client whose create answers: %v", err)
	}
	defer func() { _ = late.Close() }()
	if d := receiveOne(t, late, 5*time.Second); string(d.Body) != "history" {
		t.Errorf("the retried open delivered %q, want the topic's history", d.Body)
	}
}

// TestARefusedCreateIsReportedAsItself is the other side of the clean-up. A
// create the service refused (a filter it cannot parse, here) made nothing, so
// there is nothing to delete: the open returns the refusal as it is. Deleting
// anyway could only go wrong, either into a misleading conflict when the
// principal may not delete subscriptions, which is the case here, or by
// removing a subscription another consumer's open created a moment before.
func TestARefusedCreateIsReportedAsItself(t *testing.T) {
	c, srv := scripted(t)
	topic := newTopic("refusedcreate")
	srv.CreateRetainedTopic(topic, time.Hour)
	var mu sync.Mutex
	deletes := 0
	srv.AdminErr = func(op, _ string) error {
		mu.Lock()
		defer mu.Unlock()
		switch op {
		case "CreateSubscription":
			return status.Error(codes.InvalidArgument, "the filter does not parse")
		case "DeleteSubscription":
			deletes++
			return status.Error(codes.PermissionDenied, "the principal may not delete subscriptions")
		}
		return nil
	}
	src, err := c.Consumer(context.Background(), mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartEarliest()})
	if err == nil {
		_ = src.Close()
		t.Fatal("the open succeeded although its create was refused")
	}
	if got := mq.CodeOf(err); got != mq.CodeInvalidArgument {
		t.Errorf("the open failed with code %v, want the create's own %v: %v", got, mq.CodeInvalidArgument, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if deletes != 0 {
		t.Errorf("the open tried to delete after a refused create (%d times), which made nothing to delete", deletes)
	}
}

// TestStartEarliestWithoutAutoCreateIsStillNotFound keeps the refusal for a
// missing subscription what it was. With AutoCreate off nothing is created,
// so nothing can be left half-made, and a subscription that is not there is
// CodeNotFound rather than a conflict, even on a topic that retains.
func TestStartEarliestWithoutAutoCreateIsStillNotFound(t *testing.T) {
	c, srv := scripted(t, func(cfg *gps.Config) { cfg.AutoCreate = false })
	topic := newTopic("noautocreate")
	srv.CreateRetainedTopic(topic, time.Hour)
	src, err := c.Consumer(context.Background(), mq.ConsumerConfig{Topic: topic, Subscription: "missing", Start: mq.StartEarliest()})
	if err == nil {
		_ = src.Close()
		t.Fatal("the open of a missing subscription with AutoCreate off succeeded")
	}
	if !mq.IsSubscriptionNotFound(err) {
		t.Errorf("the open failed with %v (code %v), want the subscription reported missing", err, mq.CodeOf(err))
	}
}

// TestAClockAheadOfTheServiceStillReadsTheHistory is the second review
// finding. The seek used to target now less the topic's retention by
// Config.Clock, so a clock two hours ahead with an hour of retention sought
// past every message and the open succeeded reading nothing. The target is now
// a month before the wall clock, before any window the service allows.
func TestAClockAheadOfTheServiceStillReadsTheHistory(t *testing.T) {
	clk := &testClock{}
	clk.set(time.Now().Add(2 * time.Hour))
	c, srv := scripted(t, func(cfg *gps.Config) { cfg.Clock = clk })
	topic := newTopic("clockahead")
	srv.CreateRetainedTopic(topic, time.Hour)
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("history")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	late, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("opening at StartEarliest: %v", err)
	}
	defer func() { _ = late.Close() }()
	if d := receiveOne(t, late, 5*time.Second); string(d.Body) != "history" {
		t.Errorf("delivered %q, want the history", d.Body)
	}
}

// TestAnArmWithoutTopicRetentionRefusesWhatNeedsIt is the compatibility
// half. drivers/gcppubsub/v1compat up to v1.1.1 implements psapi.Admin and
// not psapi.TopicRetainer, and it has to go on compiling and working against
// this driver. What needs a topic's retention is refused with an error that
// names the upgrade; everything else, StartEarliest on an existing
// subscription and StartAt with RetainAcked included, works as it did.
func TestAnArmWithoutTopicRetentionRefusesWhatNeedsIt(t *testing.T) {
	ctx := context.Background()
	srv := psfake.New("iqmq-test")
	t.Cleanup(func() { _ = srv.Close() })
	open := func(cfg gps.Config) (*mq.Client, error) {
		b, err := gps.BackendWithAPI(ctx, withoutRetainer{srv}, driverkit.ClientBorrowed, cfg)
		if err != nil {
			return nil, err
		}
		c := gps.ClientFor(b, cfg)
		t.Cleanup(func() { _ = c.Close() })
		return c, nil
	}
	_, err := open(gps.Config{ProjectID: "iqmq-test", AutoCreate: true, TopicRetention: time.Hour})
	if !errors.Is(err, mq.ErrUnsupported) || !strings.Contains(err.Error(), "v1compat") {
		t.Fatalf("opening with TopicRetention over an arm without it = %v, want ErrUnsupported naming the v1compat upgrade", err)
	}

	c, err := open(gps.Config{ProjectID: "iqmq-test", AutoCreate: true})
	if err != nil {
		t.Fatalf("opening without TopicRetention: %v", err)
	}
	topic := newTopic("oldarm")
	srv.CreateRetainedTopic(topic, time.Hour)
	provision(t, c, topic, "existing")
	for _, start := range []mq.StartPosition{mq.StartEarliest(), mq.StartAt(time.Now())} {
		refused, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "late", Start: start})
		if err == nil {
			_ = refused.Close()
		}
		if !errors.Is(err, mq.ErrUnsupported) || !strings.Contains(err.Error(), "v1compat") {
			t.Errorf("%v on a new subscription over an arm without topic retention = %v, want ErrUnsupported naming the upgrade", start.Kind(), err)
		}
	}
	existing, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "existing", Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("StartEarliest on an existing subscription: %v", err)
	}
	_ = existing.Close()

	retaining, err := open(gps.Config{ProjectID: "iqmq-test", AutoCreate: true, RetainAcked: true, Retention: gps.MinRetention})
	if err != nil {
		t.Fatalf("opening with RetainAcked: %v", err)
	}
	sought, err := retaining.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "existing", Start: mq.StartAt(time.Now())})
	if err != nil {
		t.Fatalf("StartAt with RetainAcked over an arm without topic retention: %v", err)
	}
	_ = sought.Close()
}

// TestTopicRetentionThroughTheV2Client runs the option through the real client
// library against pstest, which keeps a topic's retention and reports it back:
// the scripted fake proves what the driver decides, and this proves that the
// v2 arm's CreateRetainedTopic, TopicRetention and Seek carry it to a
// service. A topic created with Config.TopicRetention lets a new subscription
// open at StartEarliest, and one created without it does not. Nothing is
// published before the seek, because pstest's own Seek does not keep a
// replayed message's body.
func TestTopicRetentionThroughTheV2Client(t *testing.T) {
	l := newLive(t)
	retaining := l.open(t, func(cfg *gps.Config) { cfg.TopicRetention = time.Hour })
	plain := l.open(t)
	ctx := context.Background()
	kept, bare := newTopic("v2-kept"), newTopic("v2-bare")
	for c, topic := range map[*mq.Client]string{retaining: kept, plain: bare} {
		admin, err := c.Admin()
		if err != nil {
			t.Fatalf("Admin: %v", err)
		}
		if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: topic}); err != nil {
			t.Fatalf("EnsureTopic(%s): %v", topic, err)
		}
	}

	src, err := plain.Consumer(ctx, mq.ConsumerConfig{Topic: kept, Subscription: "late", Start: mq.StartEarliest()})
	if err != nil {
		t.Fatalf("StartEarliest on a topic created with an hour's retention, through a client that configures none: %v", err)
	}
	defer func() { _ = src.Close() }()
	if err := plain.Publish(ctx, kept, &mq.Message{Body: []byte("after")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if d := receiveOne(t, src, 10*time.Second); string(d.Body) != "after" {
		t.Errorf("delivered %q, want \"after\"", d.Body)
	}

	refused, err := retaining.Consumer(ctx, mq.ConsumerConfig{Topic: bare, Subscription: "late", Start: mq.StartEarliest()})
	if err == nil {
		_ = refused.Close()
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("StartEarliest on a topic created without retention = %v, want ErrUnsupported", err)
	}
}

// createdThen is the fake with a CreateSubscription that creates the
// subscription and then answers err. AlreadyExists is what the client
// library's retry of a create whose first attempt landed looks like to the
// driver, and UNAVAILABLE is a create that landed and lost its answer.
type createdThen struct {
	*psfake.Server
	err error
}

// Admin is the fake's administration with that create, and with the fake's
// topic retention, which an Admin wrapped alone would hide.
func (c createdThen) Admin() psapi.Admin {
	a := c.Server.Admin()
	r, _ := a.(psapi.TopicRetainer)
	return createdThenAdmin{Admin: a, TopicRetainer: r, err: c.err}
}

// createdThenAdmin is the administration surface of [createdThen].
type createdThenAdmin struct {
	psapi.Admin
	psapi.TopicRetainer
	err error
}

// CreateSubscription creates the subscription and then answers the error.
func (a createdThenAdmin) CreateSubscription(ctx context.Context, spec psapi.SubscriptionSpec) error {
	if err := a.Admin.CreateSubscription(ctx, spec); err != nil {
		return err
	}
	return a.err
}

// withoutRetainer is the fake as drivers/gcppubsub/v1compat up to v1.1.1
// presents itself: an administration surface with no topic retention.
type withoutRetainer struct{ *psfake.Server }

// Admin is the fake's administration with everything but psapi.Admin hidden.
func (w withoutRetainer) Admin() psapi.Admin { return adminOnly{w.Server.Admin()} }

// adminOnly is psapi.Admin and nothing else.
type adminOnly struct{ psapi.Admin }
