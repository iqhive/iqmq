package snssqs

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// The subscription attributes this driver reads and writes.
const (
	// attrRawDelivery is the attribute the whole driver turns on: with it
	// true, the SQS body IS the publisher's body and the SNS message
	// attributes ARE the SQS message attributes. With it false or absent,
	// the body is a JSON envelope with the payload inside a "Message" field
	// and the attributes inside a "MessageAttributes" field.
	attrRawDelivery = "RawMessageDelivery"
	// attrFilterPolicy is the subscription filter policy (the conditional
	// FeatureServerFilter).
	attrFilterPolicy = "FilterPolicy"
	// attrFilterScope is what the policy is evaluated against.
	attrFilterScope = "FilterPolicyScope"
	// attrProtocol and attrEndpoint are what a subscription delivers to.
	attrProtocol = "Protocol"
	attrEndpoint = "Endpoint"
)

// protocolSQS is the only delivery protocol this driver subscribes or accepts.
const protocolSQS = "sqs"

// subscription resolves the (topic, subscription) pair's topology, creating
// what it may and refusing what it must.
//
// The sequence, and why it is this way round:
//
//  1. the queue, through the INNER driver, so that a queue name the SQS
//     grammar refuses is refused once and so that AutoCreate opt-in is
//     one flag rather than two;
//  2. the queue's ARN, which is the only thing Subscribe will take as an
//     endpoint;
//  3. the topic ARN;
//  4. the subscription — found among the topic's own, by protocol and
//     endpoint, or created;
//  5. RawMessageDelivery, VERIFIED rather than assumed.
//
// Step five is verified even for a subscription this driver has just created
// with the attribute set, and that is deliberate: "I asked for raw delivery"
// and "raw delivery is on" are different claims, and an emulator or a service
// that ignored the attribute would otherwise turn every message on the
// subscription into an SNS envelope nobody unwraps. Both the LocalStack run
// and the in-process fake exercise the refusal.
//
// Only a complete success is cached: a subscription that does not exist yet
// must be re-resolved, or a consumer opened before its topology was created
// would never find it.
func (b *Backend) subscription(ctx context.Context, topic, sub string, create bool) (resolvedSub, error) {
	key := topic + "\x00" + sub
	b.mu.Lock()
	got, ok := b.subs[key]
	b.mu.Unlock()
	if ok {
		return got, nil
	}
	out, err := b.locate(ctx, topic, sub, create)
	if err != nil {
		return resolvedSub{}, err
	}
	if err := b.checkRawDelivery(ctx, out.ARN, out.QueueARN); err != nil {
		return resolvedSub{}, err
	}
	b.mu.Lock()
	if !b.closed {
		b.subs[key] = out
	}
	b.mu.Unlock()
	return out, nil
}

// locate is [Backend.subscription] WITHOUT the raw-delivery check and without
// the cache: the topology as it is, whatever state it is in.
//
// The split is what makes the refusal repairable. `subscription` is what
// consumption and provisioning go through, and it refuses a subscription
// without raw delivery rather than repairing it — so the typed administration
// that EXISTS to repair one ([TypedAdmin.EnableRawDelivery]) cannot go through
// it, or the operator's fix would be refused by the thing it is fixing. The
// same applies to reading a subscription's attributes, which is how an
// operator finds out what is wrong in the first place.
func (b *Backend) locate(ctx context.Context, topic, sub string, create bool) (resolvedSub, error) {
	name, err := b.queueName(topic, sub)
	if err != nil {
		return resolvedSub{}, err
	}
	url, err := b.queueURL(ctx, name)
	if err != nil {
		return resolvedSub{}, err
	}
	arn, err := b.queueARN(ctx, url)
	if err != nil {
		return resolvedSub{}, err
	}
	topicARN, err := b.topicARN(ctx, topic, create)
	if err != nil {
		return resolvedSub{}, err
	}
	out := resolvedSub{QueueName: name, QueueURL: url, QueueARN: arn, TopicARN: topicARN}
	existing, err := b.findSubscription(ctx, topicARN, arn)
	switch {
	case err != nil:
		return resolvedSub{}, err
	case existing != "":
		out.ARN = existing
	case !create:
		// The specification: the subscription does not exist and this driver may not
		// create it. Naming the setting is what turns a mystery — a
		// consumer that receives nothing for ever — into a configuration
		// error.
		return resolvedSub{}, errNoAutoCreate
	default:
		created, err := b.createSubscription(ctx, topicARN, arn, url, sub)
		if err != nil {
			return resolvedSub{}, err
		}
		out.ARN = created
	}
	return out, nil
}

// queueName is the queue a (topic, subscription) pair names, without any I/O.
func (b *Backend) queueName(topic, sub string) (string, error) {
	if b.cfg.QueueResolver != nil {
		name, err := b.cfg.QueueResolver(topic, sub)
		if err != nil {
			return "", err
		}
		return name, nil
	}
	return QueueName(topic, sub, b.cfg.TopicPrefix, b.cfg.QueuePrefix)
}

// queueURL resolves the queue through the inner driver, which creates it when
// the opt-in allows and refuses when it does not.
//
// There is no `create` parameter and there must not be one: the inner driver's
// AutoCreate IS this driver's ([Config.sqsConfig] forwards it), so the opt-in
// is one flag with one meaning. A second switch here could disagree with it,
// and the disagreement would be a queue created by a client that was told not
// to provision.
func (b *Backend) queueURL(ctx context.Context, name string) (string, error) {
	return b.inner.QueueURL(ctx, name)
}

// queueARN is the queue's ARN, which is what Subscribe takes as an endpoint.
//
// It is read from the queue rather than built from its URL: an ARN carries the
// partition, the region and the account, and an emulator's queue URLs carry
// whatever that emulator decided. Asking is one call and is authoritative.
func (b *Backend) queueARN(ctx context.Context, url string) (string, error) {
	out, err := b.queues.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return "", err
	}
	arn := out.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
	if arn == "" {
		return "", errNoQueueARN
	}
	return arn, nil
}

// findSubscription is the subscription that delivers topicARN into queueARN,
// and "" for none.
//
// The match is on PROTOCOL and ENDPOINT rather than on a name, because an SNS
// subscription has no name: its identity is what it delivers to. That is also
// why the driver composes a queue per subscription — the queue's ARN is the
// only thing that distinguishes one of a topic's subscribers from another.
func (b *Backend) findSubscription(ctx context.Context, topicARN, queueARN string) (string, error) {
	var token *string
	for {
		out, err := b.sns.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{
			TopicArn: aws.String(topicARN), NextToken: token,
		})
		if err != nil {
			return "", err
		}
		for i := range out.Subscriptions {
			s := &out.Subscriptions[i]
			if !strings.EqualFold(aws.ToString(s.Protocol), protocolSQS) {
				continue
			}
			if aws.ToString(s.Endpoint) != queueARN {
				continue
			}
			arn := aws.ToString(s.SubscriptionArn)
			if arn == "" || arn == pendingConfirmation {
				// An SQS subscription is confirmed the moment it is
				// made, so "pending confirmation" here is a subscription
				// nothing can deliver through yet. Treating it as absent
				// would try to create a second one; reporting it is what
				// lets a caller see the state.
				return "", errPendingSubscription
			}
			return arn, nil
		}
		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			return "", nil
		}
		token = out.NextToken
	}
}

// pendingConfirmation is what the service puts in place of a subscription ARN
// for a subscription the endpoint has not confirmed.
const pendingConfirmation = "pending confirmation"

// createSubscription subscribes the queue to the topic with raw message
// delivery on, and the subscription's filter policy when one is configured.
//
// The queue's access policy is written first and only when the queue has none:
// without it a real account's SNS cannot deliver into the queue at all, and
// rewriting one that exists would be a driver silently changing IAM.
// Neither emulator enforces the policy, so this is the one
// step no test in this repository can prove works — which is exactly why it is
// the narrow version.
func (b *Backend) createSubscription(ctx context.Context, topicARN, queueARN, queueURL, sub string) (string, error) {
	if !b.cfg.NoQueuePolicy {
		if err := b.allowTopic(ctx, queueURL, queueARN, topicARN); err != nil {
			return "", err
		}
	}
	attrs := map[string]string{attrRawDelivery: "true"}
	if policy, ok := b.cfg.FilterPolicies[sub]; ok {
		attrs[attrFilterPolicy] = policy
		attrs[attrFilterScope] = b.cfg.FilterPolicyScope.Attribute()
	}
	out, err := b.sns.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn:              aws.String(topicARN),
		Protocol:              aws.String(protocolSQS),
		Endpoint:              aws.String(queueARN),
		Attributes:            attrs,
		ReturnSubscriptionArn: true,
	})
	if err != nil {
		return "", err
	}
	arn := aws.ToString(out.SubscriptionArn)
	if arn == "" || arn == pendingConfirmation {
		return "", errPendingSubscription
	}
	return arn, nil
}

// checkRawDelivery refuses a subscription whose RawMessageDelivery is not on.
//
// This is the driver's headline refusal and the reason it reads a
// subscription's attributes at all. Without raw delivery SNS wraps the
// publisher's body in a JSON notification — `{"Type":"Notification",
// "MessageId":…,"Message":"<the real body>","MessageAttributes":{…}}` — and
// the consumer receives THAT as its body, with every iqmq message attribute
// moved inside it. A driver that accepted such a subscription would hand every
// application a body that is not the one that was published and a message with
// no headers, key or identifier; the specification makes silently changing a payload a
// disqualifier, and there is no way to tell the two shapes apart reliably
// after the fact — a publisher whose payload IS an SNS-shaped JSON object
// would be unwrapped by any driver that tried.
//
// So the answer is to refuse at open, before a single delivery, and to name
// the attribute and the two ways to turn it on.
//
// It is also NEVER flipped on for an existing subscription. A subscription
// somebody else made with raw delivery off has consumers that expect the
// envelope, and a driver that silently changed it would break them;
// [TypedAdmin.EnableRawDelivery] is where that decision lives, and it is a
// person's.
func (b *Backend) checkRawDelivery(ctx context.Context, subARN, queueARN string) error {
	out, err := b.sns.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(subARN),
	})
	if err != nil {
		return err
	}
	if p := out.Attributes[attrProtocol]; p != "" && !strings.EqualFold(p, protocolSQS) {
		return errNotSQSProtocol
	}
	if e := out.Attributes[attrEndpoint]; e != "" && e != queueARN {
		// The ARN this driver resolved is not the queue the subscription
		// delivers to, so consuming that queue would consume somebody
		// else's messages and none of this topic's.
		return errWrongEndpoint
	}
	if !strings.EqualFold(out.Attributes[attrRawDelivery], "true") {
		return errNotRawDelivery
	}
	return nil
}

// allowTopic writes the SQS access policy that lets one SNS topic deliver into
// one queue, and only when the queue has no policy at all.
//
// The policy is built with encoding/json rather than by string concatenation:
// both ARNs come from outside this process, and a quote in one of them would
// otherwise produce a policy document that means something else.
func (b *Backend) allowTopic(ctx context.Context, queueURL, queueARN, topicARN string) error {
	out, err := b.queues.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNamePolicy},
	})
	if err != nil {
		return err
	}
	if strings.TrimSpace(out.Attributes[string(sqstypes.QueueAttributeNamePolicy)]) != "" {
		// A queue that already carries a policy is a queue somebody
		// configured. Leaving it alone is the conservative half the cost is that SNS may not be allowed to
		// deliver, which shows up as a subscription that receives nothing
		// and is what docs/drivers/snssqs.md's gotchas section warns about.
		return nil
	}
	doc, err := json.Marshal(queuePolicy{
		Version: "2012-10-17",
		Statement: []queueStatement{{
			Sid:       "iqmq-snssqs",
			Effect:    "Allow",
			Principal: queuePrincipal{Service: "sns.amazonaws.com"},
			Action:    "sqs:SendMessage",
			Resource:  queueARN,
			Condition: queueCondition{ArnEquals: map[string]string{"aws:SourceArn": topicARN}},
		}},
	})
	if err != nil {
		return err
	}
	_, err = b.queues.SetQueueAttributes(ctx, &awssqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(queueURL),
		Attributes: map[string]string{string(sqstypes.QueueAttributeNamePolicy): string(doc)},
	})
	return err
}

// The shape of the SQS access policy this driver writes.
type (
	queuePolicy struct {
		Version   string
		Statement []queueStatement
	}
	queueStatement struct {
		Sid       string
		Effect    string
		Principal queuePrincipal
		Action    string
		Resource  string
		Condition queueCondition
	}
	queuePrincipal struct {
		Service string
	}
	queueCondition struct {
		ArnEquals map[string]string
	}
)

// topicARN resolves a portable topic to an SNS topic ARN, creating the topic
// when the opt-in allows.
//
// Four things can resolve a topic, in this order, and each exists because a
// deployment asked for it:
//
//   - [Config.TopicARNs] maps a topic straight to an ARN, for topics that are
//     not named after their iqmq topics at all. It costs no call.
//   - a topic that IS an ARN is used verbatim, which is what a program that
//     already has the ARN from its infrastructure does. It is VERIFIED with
//     GetTopicAttributes, because a publication to an ARN that names nothing
//     fails per message rather than at open.
//   - the cache, for a topic already resolved.
//   - otherwise [Config.Resolver] (or [TopicName]) produces the name, and
//     either CreateTopic (which is idempotent and returns the existing topic's
//     ARN) or, for a caller that may not provision, the ARN built from
//     [Config.AccountID] and verified.
func (b *Backend) topicARN(ctx context.Context, topic string, create bool) (string, error) {
	if arn, ok := b.cfg.TopicARNs[topic]; ok {
		return arn, nil
	}
	if IsTopicARN(topic) {
		if err := b.verifyTopic(ctx, topic); err != nil {
			return "", err
		}
		return topic, nil
	}
	b.mu.Lock()
	arn, ok := b.arns[topic]
	b.mu.Unlock()
	if ok {
		return arn, nil
	}
	name, err := b.topicName(topic)
	if err != nil {
		return "", err
	}
	if create {
		out, err := b.sns.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(name)})
		if err != nil {
			return "", err
		}
		return b.rememberTopic(topic, aws.ToString(out.TopicArn)), nil
	}
	if b.cfg.AccountID == "" {
		// Without an account there is no ARN to build and no call that
		// finds one by name: ListTopics pages the whole account, which is
		// a cost and a race. Naming both settings is what turns an
		// unresolvable topic into a configuration error.
		return "", errNoAccountID
	}
	built := TopicARN(b.cfg.Partition, b.cfg.Region, b.cfg.AccountID, name)
	if err := b.verifyTopic(ctx, built); err != nil {
		return "", err
	}
	return b.rememberTopic(topic, built), nil
}

// verifyTopic reports whether an ARN names a topic that exists.
func (b *Backend) verifyTopic(ctx context.Context, arn string) error {
	_, err := b.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(arn)})
	return err
}

// topicName is the SNS topic name a portable topic maps to, without any I/O.
func (b *Backend) topicName(topic string) (string, error) {
	if b.cfg.Resolver != nil {
		name, err := b.cfg.Resolver(topic)
		if err != nil {
			return "", err
		}
		if err := CheckTopicName(name); err != nil {
			return "", err
		}
		return name, nil
	}
	return TopicName(topic, b.cfg.TopicPrefix)
}

// rememberTopic caches a successful resolution and returns it.
func (b *Backend) rememberTopic(topic, arn string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.arns[topic] = arn
	}
	return arn
}

// forgetTopic drops a topic from the caches, which a delete has to do or a
// subsequent call would name a topic that no longer exists.
func (b *Backend) forgetTopic(topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.arns, topic)
	prefix := topic + "\x00"
	for key := range b.subs {
		if strings.HasPrefix(key, prefix) {
			delete(b.subs, key)
		}
	}
}

// forgetSubscription drops one (topic, subscription) pair from the cache.
func (b *Backend) forgetSubscription(topic, sub string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subs, topic+"\x00"+sub)
}

// The topology refusals.
var (
	errNoAutoCreate = errcode.New(errcode.NotFound,
		"iqmq/snssqs: the SNS subscription that delivers this topic into this subscription's queue does not exist and Config.AutoCreate is off, so the specification forbids creating it")
	errNoAccountID = errcode.New(errcode.InvalidConfig,
		"iqmq/snssqs: resolving a topic ARN without creating the topic needs Config.AccountID, or Config.TopicARNs, or Config.AutoCreate")
	errNoQueueARN = errcode.New(errcode.Unknown,
		"iqmq/snssqs: the queue reported no QueueArn, and an SNS subscription's endpoint is a queue ARN")
	errNotRawDelivery = errcode.New(errcode.InvalidConfig,
		"iqmq/snssqs: the SNS subscription does not have RawMessageDelivery=true, so every body a consumer received would be SNS's JSON notification wrapped AROUND the published body and every header would be inside it; set the attribute on the subscription (TypedAdmin.EnableRawDelivery, or the console) or let Config.AutoCreate make the subscription")
	errNotSQSProtocol = errcode.New(errcode.InvalidConfig,
		"iqmq/snssqs: the SNS subscription's protocol is not sqs, so it does not deliver into a queue this driver can consume")
	errWrongEndpoint = errcode.New(errcode.InvalidConfig,
		"iqmq/snssqs: the SNS subscription delivers to a different queue from the one this subscription maps to")
	errPendingSubscription = errcode.New(errcode.Conflict,
		"iqmq/snssqs: the SNS subscription is pending confirmation, so nothing is delivered through it yet")
)
