package snssqs

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
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
	if _, err := a.b.topicARN(callCtx, spec.Name, true, false); err != nil {
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
	arn, err := a.b.topicARN(callCtx, topic, false, true)
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

// ListTopics iterates the SNS topics the service reports, as the portable
// topics every other method here takes.
//
// A name is read back through [TopicName]: [Config.TopicPrefix] comes off and
// the escapes are undone, "_-" to "/" and "_u" to "_". A topic this
// configuration did not name, under another deployment's prefix or none, or
// named by an operator in a way the mapping does not produce, is left out,
// because handed back to this surface it would be mapped to another topic.
// The names used to be reported as the service holds them, and DeleteTopic of
// one mapped it a second time: under a prefix it answered NotFound, and with
// none the listed name of "orders_" was the portable name of "orders_u", so a
// cleanup that deleted what it listed deleted the other topic.
//
// With a [Config.Resolver] the mapping is the caller's and has no inverse
// here, so the names are reported as the service holds them.
func (a *admin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		in := &sns.ListTopicsInput{}
		for {
			callCtx, cancel := a.b.call(ctx)
			out, err := a.b.sns.ListTopics(callCtx, in)
			cancel()
			if err != nil {
				yield("", a.b.errs.Map(mq.OpAdmin, "", "", err))
				return
			}
			for i := range out.Topics {
				topic := TopicNameOfARN(aws.ToString(out.Topics[i].TopicArn))
				if a.b.cfg.Resolver == nil {
					var ok bool
					if topic, ok = topicOfName(topic, a.b.cfg.TopicPrefix); !ok {
						continue
					}
				}
				if !yield(topic, nil) {
					return
				}
			}
			if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
				return
			}
			in.NextToken = out.NextToken
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
//
// It creates what is missing whatever [Config.AutoCreate] says, as
// EnsureTopic creates the topic: it is the explicit form of that opt-in. It
// used to resolve the queue through the inner driver's AutoCreate, so with
// AutoCreate off, the default, it refused the queue.
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
//
// It provisions nothing, whatever [Config.AutoCreate] says. A subscription
// that does not exist, or whose queue does not, is a CodeNotFound refusal; it
// used to create the queue under AutoCreate and then refuse saying AutoCreate
// was off. The SNS subscription is found by its queue's ARN, which is read
// from the queue, so after an operator deleted the queue by hand the
// subscription is not found and stays, delivering into nothing; DeleteTopic
// or the SNS API removes it.
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
// [github.com/iqhive/iqmq/drivers/sqs.Backend] through [Backend.SQS] for the
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
	arn, err := a.b.topicARN(callCtx, topic, a.b.cfg.AutoCreate, true)
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
//
// The service takes one attribute per call, so a change of the scope is not
// atomic. While it is under way the subscription receives every message, a
// superset of what either configuration delivers and never less: a filter
// that keeps something out of a subscriber for compliance keeps it out only
// once the call has returned nil. A call that fails after the old policy was
// removed puts the old scope and policy back; if that fails too, the error
// says so, and the subscription has no filter until the call is repeated. A
// change of the policy alone has no such window, as the policy is replaced in
// one call. The call reads the subscription's attributes first, so it needs
// sns:GetSubscriptionAttributes as well as sns:SetSubscriptionAttributes.
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
	set := func(ctx context.Context, name, value string) error {
		_, err := a.b.sns.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{
			SubscriptionArn: aws.String(arn),
			AttributeName:   aws.String(name),
			AttributeValue:  aws.String(value),
		})
		return a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	current, err := a.b.sns.GetSubscriptionAttributes(callCtx, &sns.GetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(arn),
	})
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	oldPolicy, oldScope := current.Attributes[attrFilterPolicy], scopeOf(current.Attributes)
	if oldPolicy == "" || oldScope == scope.Attribute() {
		if err := set(callCtx, attrFilterScope, scope.Attribute()); err != nil {
			return err
		}
		return set(callCtx, attrFilterPolicy, policy)
	}
	// A policy is never in force under the wrong scope: one written against
	// header names and evaluated against a JSON body matches nothing. The
	// scope used to be written first, which put the OLD policy under the NEW
	// scope until the policy write, and dropped a message both
	// configurations deliver. So when the scope changes, the old policy goes
	// first, then the scope, then the new policy.
	if err := set(callCtx, attrFilterPolicy, ""); err != nil {
		return err
	}
	err = set(callCtx, attrFilterScope, scope.Attribute())
	if err == nil {
		err = set(callCtx, attrFilterPolicy, policy)
	}
	if err == nil {
		return nil
	}
	// The old policy is gone, so a failure here would leave the subscriber
	// receiving everything, and a policy the service refuses under the new
	// scope is the ordinary way to fail. The old configuration is put back
	// under a bound of its own, since the call's may be what ran out.
	backCtx, backCancel := a.b.call(context.WithoutCancel(ctx))
	defer backCancel()
	back := set(backCtx, attrFilterScope, oldScope)
	if back == nil {
		back = set(backCtx, attrFilterPolicy, oldPolicy)
	}
	if back != nil {
		return errors.Join(err, fmt.Errorf("%w: %w", errFilterLeftOpen, back))
	}
	return err
}

// scopeOf is the filter scope a subscription's attributes name, and the
// service's default when they name none.
func scopeOf(attrs map[string]string) string {
	if s := attrs[attrFilterScope]; s != "" {
		return s
	}
	return FilterScopeAttributes.Attribute()
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
	arn, err := a.b.topicARN(callCtx, topic, false, true)
	if err != nil {
		return nil, a.b.mapOpen(topic, "", err)
	}
	var out []string
	in := &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(arn)}
	for {
		page, err := a.b.sns.ListSubscriptionsByTopic(callCtx, in)
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
		in.NextToken = page.NextToken
	}
}

// The admin refusals.
var errFilterLeftOpen = errcode.New(errcode.Unknown,
	"iqmq/snssqs: the filter change failed after the old policy was removed, and putting the old scope and policy back failed too, so the subscription may have no filter policy and receive every message until SetFilterPolicy is repeated")
