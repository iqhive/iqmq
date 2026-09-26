package gcppubsub

import (
	"context"
	"errors"
	"iter"
	"time"

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
	err := b.api.Admin().CreateTopic(callCtx, topic)
	if err == nil || errors.Is(err, psapi.ErrAlreadyExists) {
		return nil
	}
	return b.errs.Map(mq.OpAdmin, topic, "", err)
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

// ListTopics iterates the project's topics. The iteration stops at the
// first error it yields.
func (a *admin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		callCtx, cancel := a.b.call(ctx)
		defer cancel()
		stopped := false
		err := a.b.api.Admin().ListTopics(callCtx, func(topic string) bool {
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
func (a *admin) EnsureSubscription(ctx context.Context, spec mq.SubscriptionSpec) error {
	topic, err := a.b.cfg.TopicID(spec.Topic)
	if err != nil {
		return err
	}
	sub, err := a.b.cfg.SubscriptionID(spec.Topic, spec.Name)
	if err != nil {
		return err
	}
	return a.b.ensureSubscription(ctx, topic, sub, 0)
}

// ensureSubscription creates one subscription from the configuration's
// subscription fields, treating "it is already there" as success.
//
// A subscription that already exists is NOT reconfigured. Ensure
// creates what is missing, and an UpdateSubscription that changed a
// subscription's filter or dead-letter policy underneath a running consumer
// would change what that consumer receives without anybody asking for it.
func (b *Backend) ensureSubscription(ctx context.Context, topic, sub string, deadline time.Duration) error {
	if deadline <= 0 {
		deadline = b.cfg.AckDeadline
	}
	callCtx, cancel := b.call(ctx)
	defer cancel()
	if b.cfg.AutoCreate {
		if err := b.ensureTopic(callCtx, topic); err != nil {
			return err
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
			return err
		}
		spec.DeadLetterTopic = dlq
	}
	err := b.api.Admin().CreateSubscription(callCtx, spec)
	if err == nil || errors.Is(err, psapi.ErrAlreadyExists) {
		return nil
	}
	return b.errs.Map(mq.OpAdmin, topic, sub, err)
}

// DeleteSubscription removes a subscription.
func (a *admin) DeleteSubscription(ctx context.Context, topic, sub string) error {
	id, err := a.b.cfg.SubscriptionID(topic, sub)
	if err != nil {
		return err
	}
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
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
func (a *admin) Purge(ctx context.Context, topic, sub string) error {
	id, err := a.b.cfg.SubscriptionID(topic, sub)
	if err != nil {
		return err
	}
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
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
