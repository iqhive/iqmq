package gcppubsub_test

import (
	"context"
	"strings"
	"sync/atomic"
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

// A Pub/Sub subscription names exactly one topic, and the service reports
// which. These tests are about the driver asking: a subscription identifier
// the driver derived, or a caller named, can belong to a subscription attached
// to a topic other than the one the call is about, or to none at all.

// attachmentArm opens two clients over one service, one with AutoCreate on
// and one with it off, so that both of OpenSource's paths are reached: the
// create that answers AlreadyExists, and the read-back.
type attachmentArm struct {
	name string
	open func(t *testing.T) (on, off *mq.Client)
}

// attachmentArms are the real client library over pstest, which reports a
// detached subscription's topic as the service does, and the scripted fake.
var attachmentArms = []attachmentArm{
	{"pstest", func(t *testing.T) (*mq.Client, *mq.Client) {
		l := newLive(t)
		return l.open(t), l.open(t, func(cfg *gps.Config) { cfg.AutoCreate = false })
	}},
	{"scripted", func(t *testing.T) (*mq.Client, *mq.Client) {
		c, srv := scripted(t)
		return c, clientOver(t, srv, func(cfg *gps.Config) { cfg.AutoCreate = false })
	}},
}

// refusedAsConflict fails the test unless err is a CodeConflict refusal whose
// message holds want. No retry changes the topic a subscription is on, so it
// is neither retryable nor ambiguous.
func refusedAsConflict(t *testing.T, what string, err error, want string) {
	t.Helper()
	switch {
	case err == nil:
		t.Errorf("%s answered nil", what)
	case mq.CodeOf(err) != mq.CodeConflict:
		t.Errorf("%s = %v (code %v), want CodeConflict", what, err, mq.CodeOf(err))
	case !strings.Contains(err.Error(), want):
		t.Errorf("%s = %v, which does not say %q", what, err, want)
	case mq.IsRetryable(err), mq.IsAmbiguous(err):
		t.Errorf("%s = %v is retryable (%v) or ambiguous (%v); no retry moves a subscription to another topic",
			what, err, mq.IsRetryable(err), mq.IsAmbiguous(err))
	}
}

// TestADetachedSubscriptionIsRefusedRatherThanReadForEver: Pub/Sub keeps a
// deleted topic's subscriptions, detached (their topic reads
// "_deleted-topic_"), and does not attach them to a topic created again under
// the same name. Admin().DeleteTopic deletes only the topic, so a deployment
// that deleted a topic and provisioned it again as it did the first time got
// nil from EnsureSubscription, because CreateSubscription's AlreadyExists was
// taken as success without asking which topic the subscription is on. Its
// consumer then opened on the detached subscription and received nothing
// published to the recreated topic, for ever, with nothing reported anywhere.
//
// Every path refuses it now, with CodeConflict, and says it is detached.
// DeleteSubscription still removes it, which is the repair.
func TestADetachedSubscriptionIsRefusedRatherThanReadForEver(t *testing.T) {
	for _, arm := range attachmentArms {
		t.Run(arm.name, func(t *testing.T) {
			on, off := arm.open(t)
			admin, err := on.Admin()
			if err != nil {
				t.Fatalf("Admin: %v", err)
			}
			tp := newTopic("recreated")
			provision(t, on, tp, "workers")
			if err := admin.DeleteTopic(t.Context(), tp); err != nil {
				t.Fatalf("DeleteTopic: %v", err)
			}
			if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: tp}); err != nil {
				t.Fatalf("EnsureTopic after the delete: %v", err)
			}

			err = admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: tp, Name: "workers"})
			refusedAsConflict(t, "EnsureSubscription of the detached subscription", err, "detached")
			for _, open := range []struct {
				what string
				c    *mq.Client
				cfg  mq.ConsumerConfig
			}{
				{"a consumer with AutoCreate on", on, mq.ConsumerConfig{Topic: tp, Subscription: "workers"}},
				{"a consumer with AutoCreate off", off, mq.ConsumerConfig{Topic: tp, Subscription: "workers"}},
				{"a StartEarliest consumer", off, mq.ConsumerConfig{
					Topic: tp, Subscription: "workers", Start: mq.StartEarliest(),
				}},
			} {
				cons, err := open.c.Consumer(t.Context(), open.cfg)
				if err == nil {
					_ = cons.Close()
				}
				refusedAsConflict(t, open.what+" of the detached subscription", err, "detached")
			}

			// The repair the refusal names.
			if err := admin.DeleteSubscription(t.Context(), tp, "workers"); err != nil {
				t.Fatalf("DeleteSubscription of the detached subscription: %v", err)
			}
			provision(t, on, tp, "workers")
			cons, err := on.Consumer(t.Context(), mq.ConsumerConfig{Topic: tp, Subscription: "workers"})
			if err != nil {
				t.Fatalf("Consumer after the repair: %v", err)
			}
			defer func() { _ = cons.Close() }()
			if err := on.Publish(t.Context(), tp, &mq.Message{Body: []byte("after the repair")}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			d := receiveOne(t, cons, 5*time.Second)
			if string(d.Body) != "after the repair" {
				t.Errorf("the repaired subscription delivered %q", d.Body)
			}
			_ = d.Ack()
		})
	}
}

// TestASubscriptionAttachedToAnotherTopicIsRefused: the subscription
// identifier joins the topic and the subscription with "~" and writes each
// "/" as "~" too, so ("<b>/b", "c") and ("<b>", "b/c") are one identifier,
// "<b>~b~c". The identifiers stay as they are, because renaming them would
// leave every deployment's subscriptions behind, and the driver now asks which
// topic a subscription is attached to before it consumes, ensures, purges or
// deletes it.
//
// It used to take AlreadyExists as success and ask nothing. A consumer of
// ("<b>", "b/c") read "<b>/b"'s messages, labelled as "<b>"'s, and competed
// with that pair's own consumers for them, while "<b>"'s publications reached
// no subscription; Purge("<b>", "b/c") discarded the other pair's backlog, and
// DeleteSubscription deleted its subscription.
func TestASubscriptionAttachedToAnotherTopicIsRefused(t *testing.T) {
	for _, arm := range attachmentArms {
		t.Run(arm.name, func(t *testing.T) {
			on, off := arm.open(t)
			admin, err := on.Admin()
			if err != nil {
				t.Fatalf("Admin: %v", err)
			}
			base := newTopic("collide")
			other := base + "/b"
			provision(t, on, other, "c")
			if err := on.Publish(t.Context(), other, &mq.Message{Body: []byte("for " + other)}); err != nil {
				t.Fatalf("Publish(%s): %v", other, err)
			}

			for _, open := range []struct {
				what string
				c    *mq.Client
			}{
				{"a consumer with AutoCreate on", on},
				{"a consumer with AutoCreate off", off},
			} {
				cons, err := open.c.Consumer(t.Context(), mq.ConsumerConfig{Topic: base, Subscription: "b/c"})
				if err == nil {
					_ = cons.Close()
				}
				refusedAsConflict(t, open.what+" of ("+base+", b/c)", err, "another topic")
			}
			err = admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: base, Name: "b/c"})
			refusedAsConflict(t, "EnsureSubscription("+base+", b/c)", err, "another topic")
			err = admin.Purge(t.Context(), base, "b/c")
			refusedAsConflict(t, "Purge("+base+", b/c)", err, "another topic")
			err = admin.DeleteSubscription(t.Context(), base, "b/c")
			refusedAsConflict(t, "DeleteSubscription("+base+", b/c)", err, "another topic")

			// The other pair's subscription is still there, backlog and all.
			cons, err := off.Consumer(t.Context(), mq.ConsumerConfig{Topic: other, Subscription: "c"})
			if err != nil {
				t.Fatalf("Consumer(%s, c): %v (the subscription was deleted through the other pair)", other, err)
			}
			defer func() { _ = cons.Close() }()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			dst := make([]mq.Delivery, 1)
			if n, _ := cons.Receive(ctx, dst); n == 0 {
				t.Errorf("the consumer of (%s, c) received nothing: its backlog went through the other pair", other)
			} else {
				_ = dst[0].Ack()
			}
		})
	}
}

// racedAPI is the scripted fake with one race in it: another process's open
// creates the subscription between this open's check and its create, so this
// open's CreateSubscription answers AlreadyExists, and the read that follows
// fails transiently, or the open's own context ends there (cancel).
type racedAPI struct {
	*psfake.Server
	raced  atomic.Bool
	cancel context.CancelFunc
}

// Admin is the fake's, with the race scripted into it.
func (r *racedAPI) Admin() psapi.Admin {
	inner := r.Server.Admin()
	return racedAdmin{Admin: inner, TopicRetainer: inner.(psapi.TopicRetainer), r: r}
}

// racedAdmin is racedAPI's administration surface.
type racedAdmin struct {
	psapi.Admin
	psapi.TopicRetainer
	r *racedAPI
}

// CreateSubscription lets the other process's create land, and answers this
// one with AlreadyExists.
func (a racedAdmin) CreateSubscription(ctx context.Context, spec psapi.SubscriptionSpec) error {
	if err := a.Admin.CreateSubscription(ctx, spec); err != nil {
		return err
	}
	a.r.raced.Store(true)
	return psapi.ErrAlreadyExists
}

// GetSubscription fails once the race has run.
func (a racedAdmin) GetSubscription(ctx context.Context, sub string) (psapi.SubscriptionInfo, error) {
	if !a.r.raced.Load() {
		return a.Admin.GetSubscription(ctx, sub)
	}
	if a.r.cancel != nil {
		a.r.cancel()
		return psapi.SubscriptionInfo{}, context.Canceled
	}
	return psapi.SubscriptionInfo{}, status.Error(codes.Unavailable, "injected: the read after AlreadyExists")
}

// TestAnOpenNeverDeletesASubscriptionItsCreateFoundThere: at StartEarliest an
// open that may have created a subscription and could not finish deletes it
// again, because one left unsought reads none of the history. A create that
// answered AlreadyExists made nothing: what is there may be another
// consumer's, as when replicas open one new subscription together. The read
// the topic check added after AlreadyExists returned its failure as a create
// that may have landed, so a transient failure there, or the open's context
// ending there, deleted the other process's subscription with the history it
// had been sought to. Such a failure is answered as a failed seek of a
// subscription this open did not create is: a conflict that names it, and no
// deletion.
func TestAnOpenNeverDeletesASubscriptionItsCreateFoundThere(t *testing.T) {
	for _, how := range []string{"unavailable", "cancelled"} {
		t.Run(how, func(t *testing.T) {
			srv := psfake.New("iqmq-test")
			api := &racedAPI{Server: srv}
			cfg := gps.Config{ProjectID: "iqmq-test", AutoCreate: true, TopicRetention: time.Hour}
			b, err := gps.BackendWithAPI(t.Context(), api, driverkit.ClientOwned, cfg)
			if err != nil {
				t.Fatalf("BackendWithAPI: %v", err)
			}
			c := gps.ClientFor(b, cfg)
			t.Cleanup(func() { _ = c.Close() })
			admin, err := c.Admin()
			if err != nil {
				t.Fatalf("Admin: %v", err)
			}
			tp := newTopic("raced")
			if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: tp}); err != nil {
				t.Fatalf("EnsureTopic: %v", err)
			}
			ctx := t.Context()
			if how == "cancelled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(t.Context())
				defer cancel()
				api.cancel = cancel
			}
			cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: tp, Subscription: "s", Start: mq.StartEarliest()})
			if err == nil {
				_ = cons.Close()
				t.Fatalf("the open succeeded though the read after AlreadyExists failed")
			}
			if mq.CodeOf(err) != mq.CodeConflict || mq.IsRetryable(err) {
				t.Errorf("the open = %v (code %v, retryable %v), want a CodeConflict that is not retryable: a retry "+
					"would open the unsought subscription as an existing one", err, mq.CodeOf(err), mq.IsRetryable(err))
			}
			id, _ := cfg.SubscriptionID(tp, "s")
			if info, gerr := srv.Admin().GetSubscription(context.Background(), id); gerr != nil || !info.Exists {
				t.Errorf("the other process's subscription %q is gone (%v) after this open's read after "+
					"AlreadyExists failed (%v)", id, gerr, err)
			}
		})
	}
}

// TestTheTopicCheckIsSkippedWhereTheSubscriptionCannotBeRead: the topic check
// reads the subscription, which takes pubsub.subscriptions.get, and
// roles/pubsub.subscriber does not have it. A call that such a credential
// could make before there was a check (an Ensure of a subscription that is
// there, a Purge, a DeleteSubscription) goes ahead unchecked rather than
// failing on PermissionDenied. A consumer's open read the subscription
// before the check existed, and still fails without the permission.
func TestTheTopicCheckIsSkippedWhereTheSubscriptionCannotBeRead(t *testing.T) {
	c, srv := scripted(t)
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	tp := newTopic("no-get")
	provision(t, c, tp, "workers")
	if err := c.Publish(t.Context(), tp, &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	id := subID(t, tp, "workers")
	denied := status.Error(codes.PermissionDenied, "injected: no pubsub.subscriptions.get")
	srv.AdminErr = func(op, _ string) error {
		if op == "GetSubscription" {
			return denied
		}
		return nil
	}
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: tp, Name: "workers"}); err != nil {
		t.Errorf("EnsureSubscription of an existing subscription without get = %v, want nil as before", err)
	}
	if err := admin.Purge(t.Context(), tp, "workers"); err != nil {
		t.Errorf("Purge without get = %v, want nil as before", err)
	}
	if got := srv.Backlog(id); got != 0 {
		t.Errorf("the subscription holds %d messages after Purge without get, want 0", got)
	}
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: tp, Subscription: "workers"})
	if err == nil {
		_ = cons.Close()
		t.Errorf("a consumer opened without get; its open has read the subscription since before the check")
	} else if mq.CodeOf(err) != mq.CodeForbidden {
		t.Errorf("the open without get = %v (code %v), want CodeForbidden", err, mq.CodeOf(err))
	}
	if err := admin.DeleteSubscription(t.Context(), tp, "workers"); err != nil {
		t.Errorf("DeleteSubscription without get = %v, want nil as before", err)
	}
	srv.AdminErr = nil
	if info := subInfo(t, srv, tp, "workers"); info.Exists {
		t.Errorf("the subscription is still there after DeleteSubscription without get")
	}
}
