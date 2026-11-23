package gcppubsub

import (
	"context"
	"errors"
	"iter"
	"time"

	"google.golang.org/grpc/codes"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// The compile-time proof that the administration surface is the specification's.
var _ mq.Admin = (*admin)(nil)

// Admin returns the portable topology surface.
func (b *Backend) Admin() (mq.Admin, error) {
	if err := b.CheckOpen(context.Background()); err != nil {
		return nil, err
	}
	return &admin{b: b}, nil
}

// admin is the portable administration surface over the client library's
// generated admin clients.
//
// It is the portable INTERSECTION and no more: the specification keeps a driver's own
// typed administration as the advanced API, which for this driver is
// [Config]'s subscription fields — the filter, the dead-letter policy, the
// retention, exactly-once delivery — because a Pub/Sub subscription's
// configuration is a subscription's and not a portable spec's.
type admin struct{ b *Backend }

// EnsureTopic creates the topic if it does not exist. It is
// idempotent.
func (a *admin) EnsureTopic(ctx context.Context, spec mq.TopicSpec) error {
	topic, err := a.b.cfg.TopicID(spec.Name)
	if err != nil {
		return err
	}
	return a.b.ensureTopic(ctx, topic)
}

// ensureTopic creates one topic, treating "it is already there" as success.
//
// AlreadyExists is what makes this idempotent without a GetTopic first, which
// is one round trip rather than two and, more importantly, has no window: two
// processes that both read "absent" and both create would otherwise have one
// of them fail.
func (b *Backend) ensureTopic(ctx context.Context, topic string) error {
	callCtx, cancel := b.call(ctx)
	defer cancel()
	err := b.createTopic(callCtx, topic)
	if err == nil || errors.Is(err, psapi.ErrAlreadyExists) {
		return nil
	}
	return b.errs.Map(mq.OpAdmin, topic, "", err)
}

// createTopic creates one topic with [Config.TopicRetention] when one is
// configured. A topic that already exists keeps the retention it has: Ensure
// creates what is missing and does not reconfigure what is there. The
// retainer is never nil here, because a backend whose arm has none refuses
// Config.TopicRetention at open.
func (b *Backend) createTopic(ctx context.Context, topic string) error {
	if b.cfg.TopicRetention > 0 {
		return b.retainer.CreateRetainedTopic(ctx, topic, b.cfg.TopicRetention)
	}
	return b.api.Admin().CreateTopic(ctx, topic)
}

// DeleteTopic removes a topic and everything in it.
//
// "Everything in it" is not quite what the service does: deleting a topic
// leaves its subscriptions in place, detached, with the backlog they already
// had. That is the service's behaviour and this driver does not paper over it
// by deleting subscriptions it did not create; docs/drivers/gcppubsub.md says
// so.
func (a *admin) DeleteTopic(ctx context.Context, topic string) error {
	id, err := a.b.cfg.TopicID(topic)
	if err != nil {
		return err
	}
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	if derr := a.b.api.Admin().DeleteTopic(callCtx, id); derr != nil {
		if errors.Is(derr, psapi.ErrNotFound) {
			return nil
		}
		return a.b.errs.Map(mq.OpAdmin, topic, "", derr)
	}
	return nil
}

// ListTopics iterates the project's topics, as the portable topics every
// other method here takes. The iteration stops at the first error it yields.
//
// The service reports topic identifiers, and they are read back through
// [Config.TopicID]: the prefix comes off and each "~" becomes "/" again. A
// topic this configuration did not name, under another prefix or none, is
// left out, because handed back to this surface it would be mapped to some
// other topic. The identifiers used to be yielded as they were, so under a
// prefix DeleteTopic of a listed name deleted nothing and answered nil.
func (a *admin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		callCtx, cancel := a.b.call(ctx)
		defer cancel()
		stopped := false
		err := a.b.api.Admin().ListTopics(callCtx, func(id string) bool {
			topic, ok := a.b.cfg.topicOf(id)
			if !ok {
				return true
			}
			if !yield(topic, nil) {
				stopped = true
				return false
			}
			return true
		})
		if err != nil && !stopped {
			yield("", a.b.errs.Map(mq.OpAdmin, "", "", err))
		}
	}
}

// EnsureSubscription creates the subscription if it does not exist. It
// is idempotent.
//
// One that exists is read back and refused, with CodeConflict, when it is
// attached to another topic or detached ([Backend.checkAttached]), unless the
// credential may not read it ([Backend.readBack]).
func (a *admin) EnsureSubscription(ctx context.Context, spec mq.SubscriptionSpec) error {
	topic, err := a.b.cfg.TopicID(spec.Topic)
	if err != nil {
		return err
	}
	sub, err := a.b.cfg.SubscriptionID(spec.Topic, spec.Name)
	if err != nil {
		return err
	}
	got, err := a.b.ensureSubscription(ctx, topic, sub, 0)
	if err != nil {
		return err
	}
	if err := a.b.checkAttached(got.info, topic); err != nil {
		return a.b.errs.Map(mq.OpAdmin, spec.Topic, spec.Name, err)
	}
	return nil
}

// ensuredSub is what [Backend.ensureSubscription] did, which is what
// StartEarliest's clean-up turns on: a subscription the open created may be
// deleted again when its seek fails, and one that was already there may be
// another consumer's and never is.
type ensuredSub struct {
	// created is this call creating the subscription. sent is its
	// CreateSubscription request having been sent at all: false when the
	// failure came before the request, from the topic's creation or the
	// dead-letter topic's name, so that nothing can have been created.
	created, sent bool
	// existed is the create answering AlreadyExists: no request of this call
	// made what is there. It is also what the client library's own retry of
	// a create whose first attempt landed looks like.
	existed bool
	// info is the subscription read back after AlreadyExists, for the topic
	// check ([Backend.checkAttached]). Its Exists is false when nothing was
	// read: the create succeeded, or the credential may not read it
	// ([Backend.readBack]).
	info psapi.SubscriptionInfo
}

// ensureSubscription creates one subscription from the configuration's
// subscription fields, treating "it is already there" as success.
//
// A subscription that already exists is NOT reconfigured. Ensure
// creates what is missing, and an UpdateSubscription that changed a
// subscription's filter or dead-letter policy underneath a running consumer
// would change what that consumer receives without anybody asking for it.
//
// It is read back instead, because AlreadyExists does not say which topic
// the subscription is on, and the caller checks that before it acts on it.
// The read failing is reported with existed set: what is there was made by
// no request of this call, so StartEarliest's clean-up must not take the
// failure for a create that may have landed and delete it.
func (b *Backend) ensureSubscription(ctx context.Context, topic, sub string, deadline time.Duration) (ensuredSub, error) {
	if deadline <= 0 {
		deadline = b.cfg.AckDeadline
	}
	callCtx, cancel := b.call(ctx)
	defer cancel()
	if b.cfg.AutoCreate {
		if err := b.ensureTopic(callCtx, topic); err != nil {
			return ensuredSub{}, err
		}
	}
	spec := psapi.SubscriptionSpec{
		Subscription:        sub,
		Topic:               topic,
		AckDeadline:         deadline,
		Filter:              b.cfg.Filter,
		ExactlyOnce:         b.cfg.ExactlyOnce,
		Ordering:            b.cfg.Ordering,
		RetainAcked:         b.cfg.RetainAcked,
		Retention:           b.cfg.Retention,
		MaxDeliveryAttempts: b.cfg.MaxDeliveryAttempts,
	}
	if b.cfg.DeadLetterTopic != "" {
		dlq, err := b.cfg.TopicID(b.cfg.DeadLetterTopic)
		if err != nil {
			return ensuredSub{}, err
		}
		spec.DeadLetterTopic = dlq
	}
	err := b.api.Admin().CreateSubscription(callCtx, spec)
	switch {
	case err == nil:
		return ensuredSub{created: true, sent: true}, nil
	case errors.Is(err, psapi.ErrAlreadyExists):
		got := ensuredSub{sent: true, existed: true}
		info, _, err := b.readBack(callCtx, sub)
		if err != nil {
			return got, b.errs.Map(mq.OpAdmin, topic, sub, err)
		}
		got.info = info
		return got, nil
	default:
		return ensuredSub{sent: true}, b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
}

// readBack reads a subscription for the topic check, and reports whether it
// could. A credential that may not read it (PermissionDenied) is answered as
// not read, with no error, and the caller acts without the check, as it did
// before there was one: roles/pubsub.subscriber consumes, seeks and attaches
// but has no pubsub.subscriptions.get, and a call such a credential could make
// must not start failing for a read it never needed.
func (b *Backend) readBack(ctx context.Context, sub string) (psapi.SubscriptionInfo, bool, error) {
	info, err := b.api.Admin().GetSubscription(ctx, sub)
	switch {
	case err == nil:
		return info, true, nil
	case matchCodes(codes.PermissionDenied)(err):
		return psapi.SubscriptionInfo{}, false, nil
	default:
		return psapi.SubscriptionInfo{}, false, err
	}
}

// checkOwn reads a subscription and refuses one attached to another topic,
// before a call that acts on it. It reports whether the call should go ahead:
// false for a subscription that does not exist.
//
// A detached subscription is not refused here: which topic it was attached to
// is gone with the topic, and deleting it is the repair the refusal at open
// names. So of two pairs that share an identifier, a Purge or a
// DeleteSubscription of one acts on the other's subscription once that one's
// topic has been deleted. One that cannot be read goes ahead unchecked
// ([Backend.readBack]).
func (a *admin) checkOwn(ctx context.Context, topic, sub, id string) (bool, error) {
	topicID, err := a.b.cfg.TopicID(topic)
	if err != nil {
		return false, err
	}
	info, read, err := a.b.readBack(ctx, id)
	switch {
	case err != nil:
		return false, a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	case !read || info.Topic == psapi.DetachedTopic:
		return true, nil
	}
	if err := a.b.checkAttached(info, topicID); err != nil {
		return false, a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	return info.Exists, nil
}

// DeleteSubscription removes a subscription.
//
// It reads the subscription first and refuses, with CodeConflict, one
// attached to another topic, whose identifier this pair's can be
// ([Config.SubscriptionID]). A detached one is deleted.
func (a *admin) DeleteSubscription(ctx context.Context, topic, sub string) error {
	id, err := a.b.cfg.SubscriptionID(topic, sub)
	if err != nil {
		return err
	}
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	if exists, err := a.checkOwn(callCtx, topic, sub, id); err != nil || !exists {
		return err
	}
	if derr := a.b.api.Admin().DeleteSubscription(callCtx, id); derr != nil {
		if errors.Is(derr, psapi.ErrNotFound) {
			return nil
		}
		return a.b.errs.Map(mq.OpAdmin, topic, sub, derr)
	}
	return nil
}

// Purge discards everything a subscription has not consumed.
//
// On this broker that is a Seek to NOW, which acknowledges the whole backlog
// in one call. It needs no retained messages — seeking FORWARD is always
// available — which is why this is native while seeking backwards is
// conditional on [Config.RetainAcked].
//
// Like DeleteSubscription it refuses a subscription attached to another
// topic, whose backlog this would discard.
func (a *admin) Purge(ctx context.Context, topic, sub string) error {
	id, err := a.b.cfg.SubscriptionID(topic, sub)
	if err != nil {
		return err
	}
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	if _, err := a.checkOwn(callCtx, topic, sub, id); err != nil {
		return err
	}
	if perr := a.b.api.Admin().Purge(callCtx, id); perr != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, sub, perr)
	}
	return nil
}

// Stats reports a subscription's depth, in-flight count and lag.
//
// Pub/Sub does not answer this question through its own API at all: the
// undelivered-message count and the oldest-unacknowledged-message age are
// Cloud Monitoring metrics, which is a different service, a different
// credential scope and a sampling delay of minutes. The rule that an
// unknown is never an overloaded zero makes the answer here ErrUnsupported
// rather than three zeroes, because a caller cannot tell an empty
// subscription from one nobody can measure.
func (a *admin) Stats(ctx context.Context, topic, sub string) (mq.QueueStats, error) {
	_ = ctx
	_, _ = topic, sub
	return mq.QueueStats{}, errNoStats
}
