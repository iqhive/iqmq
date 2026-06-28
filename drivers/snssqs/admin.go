package snssqs

import (
	"context"
	"iter"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Admin returns the topology surface.
func (b *Backend) Admin() (mq.Admin, error) {
	if b.Closed() {
		return nil, mq.ErrClosed
	}
	return &admin{b: b}, nil
}

// TypedAdmin returns this driver's own administration, which is where
// everything SNS's own lives: the subscription's ARN and attributes, the
// filter policy that is the conditional claim, and the one operation that
// changes an existing subscription's raw delivery.
func (b *Backend) TypedAdmin() (*TypedAdmin, error) {
	if b.Closed() {
		return nil, mq.ErrClosed
	}
	return &TypedAdmin{b: b}, nil
}

// admin is the portable topology surface.
type admin struct{ b *Backend }

var _ mq.Admin = (*admin)(nil)

// EnsureTopic creates the SNS topic if it does not exist.
//
// It is idempotent: SNS's CreateTopic returns the existing topic's ARN for a
// name that exists, which is the service's own behaviour and not something
// this driver has to arrange.
//
// It creates the TOPIC and no queue, which is the honest split: a topic with
// no subscriptions is a complete, usable thing that delivers nowhere, and a
// queue belongs to a subscription.
func (a *admin) EnsureTopic(ctx context.Context, spec mq.TopicSpec) error {
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	if _, err := a.b.topicARN(callCtx, spec.Name, true); err != nil {
		return a.b.mapOpen(spec.Name, "", err)
	}
	return nil
}

// DeleteTopic removes the SNS topic and every subscription on it.
//
// What it does NOT remove is the queues. That is the service's behaviour and
// it is also the right one: a queue may hold messages nobody has consumed, and
// deleting a topic is a statement about the topic. The queues are reachable
// through [TypedAdmin.Queue] and are deleted with the SQS driver's own admin,
// which docs/drivers/snssqs.md says.
func (a *admin) DeleteTopic(ctx context.Context, topic string) error {
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	arn, err := a.b.topicARN(callCtx, topic, false)
	if err != nil {
		return a.b.mapOpen(topic, "", err)
	}
	if _, err := a.b.sns.DeleteTopic(callCtx, &sns.DeleteTopicInput{TopicArn: aws.String(arn)}); err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	// The cached ARN names a topic that no longer exists, and handing it out
	// again would produce a NotFound on every subsequent publication.
	a.b.forgetTopic(topic)
	return nil
}

// ListTopics iterates the SNS topics the service reports, as topic names
//
// The names come back as topic names and are reported as such: the inverse of
// the topic mapping is not a function — "orders/eu" and "orders-eu" map to one
// SNS topic — so inventing a portable topic from a name would be inventing the
// one a caller happened to use.
func (a *admin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		var token *string
		for {
			callCtx, cancel := a.b.call(ctx)
			out, err := a.b.sns.ListTopics(callCtx, &sns.ListTopicsInput{NextToken: token})
			cancel()
			if err != nil {
				yield("", a.b.errs.Map(mq.OpAdmin, "", "", err))
				return
			}
			for i := range out.Topics {
				if !yield(TopicNameOfARN(aws.ToString(out.Topics[i].TopicArn)), nil) {
					return
				}
			}
			if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
				return
			}
			token = out.NextToken
		}
	}
}

// EnsureSubscription creates the subscription's queue and the SNS
// subscription that delivers into it, with raw message delivery on.
//
// It is the operation a program calls before it publishes, and on this driver
// that matters more than on most: a publication to a topic with no
// subscriptions is accepted and delivered nowhere, so a subscriber created
// after a publication never sees it. It is idempotent, and it refuses — rather
// than repairing — a subscription that exists with raw delivery off.
func (a *admin) EnsureSubscription(ctx context.Context, spec mq.SubscriptionSpec) error {
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	if _, err := a.b.subscription(callCtx, spec.Topic, spec.Name, true); err != nil {
		return a.b.mapOpen(spec.Topic, spec.Name, err)
	}
	return nil
}

// DeleteSubscription removes the SNS subscription.
//
// The QUEUE survives, and that is deliberate rather than an omission: it may
// hold messages nobody has consumed, and a portable "delete the subscription"
// that destroyed them would be the one operation that loses data
// silently. After this call the queue receives nothing new and whatever it
// holds is still consumable — with `drivers/sqs`, since this driver's
// consumption goes through a subscription that no longer exists.
// [TypedAdmin.Queue] names it.
func (a *admin) DeleteSubscription(ctx context.Context, topic, sub string) error {
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	resolved, err := a.b.locate(callCtx, topic, sub, false)
	if err != nil {
		return a.b.mapOpen(topic, sub, err)
	}
	if _, err := a.b.sns.Unsubscribe(callCtx, &sns.UnsubscribeInput{
		SubscriptionArn: aws.String(resolved.ARN),
	}); err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	a.b.forgetSubscription(topic, sub)
	return nil
}

// Purge discards everything the subscription's queue holds.
//
// It purges ONE subscription's queue and no other, which is what a fan-out
// makes of the operation: the copies are separate messages, so there is no
// such thing as purging "the topic".
func (a *admin) Purge(ctx context.Context, topic, sub string) error {
	name, err := a.queue(ctx, topic, sub)
	if err != nil {
		return err
	}
	inner, err := a.b.inner.Admin()
	if err != nil {
		return err
	}
	return inner.Purge(ctx, name, sub)
}

// Stats reports the subscription's queue depth, in-flight count and lag
//
// Every number the service gives is APPROXIMATE and is reported as such. They
// are one subscription's: a topic has no depth of its own, and adding its
// queues' depths together would report a number no consumer experiences.
func (a *admin) Stats(ctx context.Context, topic, sub string) (mq.QueueStats, error) {
	name, err := a.queue(ctx, topic, sub)
	if err != nil {
		return mq.QueueStats{}, err
	}
	inner, err := a.b.inner.Admin()
	if err != nil {
		return mq.QueueStats{}, err
	}
	return inner.Stats(ctx, name, sub)
}

// queue is the queue name a (topic, subscription) pair maps to, without
// provisioning anything.
func (a *admin) queue(ctx context.Context, topic, sub string) (string, error) {
	_ = ctx
	name, err := a.b.queueName(topic, sub)
	if err != nil {
		return "", a.b.mapOpen(topic, sub, err)
	}
	return name, nil
}

// TypedAdmin is this driver's own administration: the subscription's ARN and
// attributes, the filter policy, and raw delivery.
type TypedAdmin struct{ b *Backend }

// Queue is the queue a (topic, subscription) pair's copies arrive in, as a
// name and a URL, resolving the topology if necessary.
//
// It is the bridge to `drivers/sqs`: with the name in hand a caller reaches
// [bitbucket.org/iqhive/iqmq/drivers/sqs.Backend] through [Backend.SQS] for the
// redrive policy, the message-move task and the queue attributes, all of which
// belong to the queue and not to the topic.
func (a *TypedAdmin) Queue(ctx context.Context, topic, sub string) (name, url, arn string, err error) {
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	resolved, err := a.b.locate(callCtx, topic, sub, a.b.cfg.AutoCreate)
	if err != nil {
		return "", "", "", a.b.mapOpen(topic, sub, err)
	}
	return resolved.QueueName, resolved.QueueURL, resolved.QueueARN, nil
}

// SubscriptionARN is the SNS subscription's ARN, which is what every SNS
// subscription operation takes and what nothing else in the portable API can
// name.
//
// It does NOT apply the raw-delivery refusal, and nor does anything else on
// this type: the typed administration is where an operator finds out what is
// wrong with a subscription and fixes it, so a refusal that stopped it reading
// the subscription would stop the repair too.
func (a *TypedAdmin) SubscriptionARN(ctx context.Context, topic, sub string) (string, error) {
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	resolved, err := a.b.locate(callCtx, topic, sub, a.b.cfg.AutoCreate)
	if err != nil {
		return "", a.b.mapOpen(topic, sub, err)
	}
	return resolved.ARN, nil
}

// TopicARN is the SNS topic ARN a portable topic maps to, resolving it if
// necessary.
func (a *TypedAdmin) TopicARN(ctx context.Context, topic string) (string, error) {
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	arn, err := a.b.topicARN(callCtx, topic, a.b.cfg.AutoCreate)
	if err != nil {
		return "", a.b.mapOpen(topic, "", err)
	}
	return arn, nil
}

// Attributes reads a subscription's attributes, which is the escape hatch for
// everything the specification keeps out of the portable surface — and the call that
// answers whether raw delivery is on, which is what [Backend.OpenSource]
// refuses on.
func (a *TypedAdmin) Attributes(ctx context.Context, topic, sub string) (map[string]string, error) {
	arn, err := a.SubscriptionARN(ctx, topic, sub)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	out, err := a.b.sns.GetSubscriptionAttributes(callCtx, &sns.GetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(arn),
	})
	if err != nil {
		return nil, a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	return out.Attributes, nil
}

// SetFilterPolicy writes a subscription's filter policy, which is the
// condition on FeatureServerFilter claim.
//
// An empty policy REMOVES the filter, which is how a subscription goes back to
// receiving everything. The policy is checked against the service's grammar
// first — a JSON object inside the size limit — so a typo is a refusal here
// rather than a subscription that silently stops delivering.
//
// It is the only way to change an existing subscription's filter:
// [Config.FilterPolicies] applies at Subscribe and is never written over a
// subscription that already exists, because changing which messages a live
// subscriber receives is an operator's decision and not a restart's.
func (a *TypedAdmin) SetFilterPolicy(ctx context.Context, topic, sub, policy string, scope FilterScope) error {
	if !scope.Valid() {
		return errFilterScope
	}
	if policy != "" {
		if err := CheckFilterPolicy(sub, policy); err != nil {
			return err
		}
	}
	arn, err := a.SubscriptionARN(ctx, topic, sub)
	if err != nil {
		return err
	}
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	if _, err := a.b.sns.SetSubscriptionAttributes(callCtx, &sns.SetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(arn),
		AttributeName:   aws.String(attrFilterScope),
		AttributeValue:  aws.String(scope.Attribute()),
	}); err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	// The scope is written FIRST, so that a policy is never in force under
	// the wrong scope: a policy written against header names and evaluated
	// against a JSON body matches nothing, and a subscriber that silently
	// received nothing is the worst of the outcomes here.
	if _, err := a.b.sns.SetSubscriptionAttributes(callCtx, &sns.SetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(arn),
		AttributeName:   aws.String(attrFilterPolicy),
		AttributeValue:  aws.String(policy),
	}); err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	return nil
}

// EnableRawDelivery turns RawMessageDelivery on for an existing subscription.
//
// It exists because [Backend.OpenSource] refuses a subscription that does not
// have it and will not repair one: a subscription somebody else made with raw
// delivery off has consumers that expect SNS's JSON notification, and a driver
// that flipped the attribute on construction would break them without anyone
// asking. This is the operator's decision, spelled out.
func (a *TypedAdmin) EnableRawDelivery(ctx context.Context, topic, sub string) error {
	arn, err := a.SubscriptionARN(ctx, topic, sub)
	if err != nil {
		return err
	}
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	if _, err := a.b.sns.SetSubscriptionAttributes(callCtx, &sns.SetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(arn),
		AttributeName:   aws.String(attrRawDelivery),
		AttributeValue:  aws.String("true"),
	}); err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	a.b.forgetSubscription(topic, sub)
	return nil
}

// Subscriptions lists the SQS subscriptions on a topic, as their endpoint
// queue ARNs, which is how an operator sees how wide a fan-out really is.
func (a *TypedAdmin) Subscriptions(ctx context.Context, topic string) ([]string, error) {
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	arn, err := a.b.topicARN(callCtx, topic, false)
	if err != nil {
		return nil, a.b.mapOpen(topic, "", err)
	}
	var out []string
	var token *string
	for {
		page, err := a.b.sns.ListSubscriptionsByTopic(callCtx, &sns.ListSubscriptionsByTopicInput{
			TopicArn: aws.String(arn), NextToken: token,
		})
		if err != nil {
			return nil, a.b.errs.Map(mq.OpAdmin, topic, "", err)
		}
		for i := range page.Subscriptions {
			s := &page.Subscriptions[i]
			if aws.ToString(s.Protocol) == protocolSQS {
				out = append(out, aws.ToString(s.Endpoint))
			}
		}
		if page.NextToken == nil || aws.ToString(page.NextToken) == "" {
			return out, nil
		}
		token = page.NextToken
	}
}

// The admin refusals.
var _ = errcode.New
