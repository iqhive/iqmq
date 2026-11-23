package snssqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/internal/errcode"
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
//  1. the queue, through the INNER driver when this call may create it, so
//     that the queue is created with the inner driver's attributes and the
//     AutoCreate opt-in is one flag rather than two ([Backend.queueURL]);
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
// It is asked of the services every time, and never answered from what this
// backend resolved before: its two callers, Admin().EnsureSubscription and
// the open, are the calls whose job is to find out, and something other than
// this backend (a second process, `iqmq topology`, the console) can have
// removed the subscription or turned raw delivery off since. Answered from
// memory, EnsureSubscription said nil and recreated nothing, and a consumer
// opened after raw delivery was turned off was handed SNS's notification as
// every body. The cost is the calls above, once per open.
//
// A call that may not create is the open path with AutoCreate off, so a queue
// or a subscription that is not there is refused naming the setting. That
// turns a mystery, a consumer that receives nothing for ever, into a
// configuration error.
func (b *Backend) subscription(ctx context.Context, topic, sub string, create bool) (resolvedSub, error) {
	out, err := b.locate(ctx, topic, sub, create)
	if !create && (err == errNoQueue || err == errNoSNSSubscription) { //nolint:errorlint // the sentinels themselves: errors.Is matches any NotFound
		return resolvedSub{}, errNoAutoCreate
	}
	if err != nil {
		return resolvedSub{}, err
	}
	if err := b.checkRawDelivery(ctx, out.ARN, out.QueueARN); err != nil {
		return resolvedSub{}, err
	}
	b.mu.Lock()
	if !b.closed {
		b.subs[topic+"\x00"+sub] = out
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
//
// With create false it provisions nothing, whatever AutoCreate says, and
// answers a queue or a subscription that is not there with errNoQueue or
// errNoSNSSubscription. DeleteSubscription relies on that: it used to resolve the
// queue through the inner driver, which under AutoCreate created the queue of
// a subscription that had never existed and then refused, saying AutoCreate
// was off.
func (b *Backend) locate(ctx context.Context, topic, sub string, create bool) (resolvedSub, error) {
	name, err := b.queueName(topic, sub)
	if err != nil {
		return resolvedSub{}, err
	}
	url, err := b.queueURL(ctx, name, create)
	if err != nil {
		return resolvedSub{}, err
	}
	arn, err := b.queueARN(ctx, url)
	if missingQueue(err) {
		// The URL came from the inner driver's cache and the queue has
		// gone since, deleted by hand. It is the same answer as a lookup
		// that finds no queue, and not a CodeUnknown.
		return resolvedSub{}, errNoQueue
	}
	if err != nil {
		return resolvedSub{}, err
	}
	topicARN, err := b.topicARN(ctx, topic, create, !create)
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
		return resolvedSub{}, errNoSNSSubscription
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

// queueURL resolves the subscription's queue, creating it only when create
// says this call may provision, and answering errNoQueue for a queue that is
// not there and may not be created.
//
// The queue goes through the inner driver, whose AutoCreate IS this driver's
// ([Config.sqsConfig] forwards it), so a queue it creates has the inner
// driver's attributes and its failures are the inner driver's to map. That
// driver creates a missing queue whenever AutoCreate is on, though, and a
// call that must not provision cannot go through it then: a
// DeleteSubscription of a subscription that never existed used to create its
// queue. Such a call looks the queue up itself. The other way round,
// Admin().EnsureSubscription is the explicit form of the opt-in and creates a
// missing queue with AutoCreate off, through the inner driver's own
// EnsureTopic, as Admin().EnsureTopic creates the topic.
func (b *Backend) queueURL(ctx context.Context, name string, create bool) (string, error) {
	if b.cfg.AutoCreate && !create {
		return b.lookupQueue(ctx, name)
	}
	url, err := b.inner.QueueURL(ctx, name)
	if b.cfg.AutoCreate || mq.CodeOf(err) != mq.CodeNotFound {
		return url, err
	}
	// AutoCreate is off, so the inner driver created nothing, and NotFound
	// is its answer for a queue that does not exist.
	if !create {
		return "", errNoQueue
	}
	inner, err := b.inner.Admin()
	if err != nil {
		return "", err
	}
	if err := inner.EnsureTopic(ctx, mq.TopicSpec{Name: name}); err != nil {
		return "", err
	}
	return b.inner.QueueURL(ctx, name)
}

// lookupQueue is the queue's URL, and errNoQueue for a queue that does not
// exist. It never creates one, which the inner driver's resolution does under
// AutoCreate. It asks through the client the inner driver is built over.
//
// The name is held to the queue grammar first, as the inner driver's
// resolution holds it, so a name the service would refuse is refused here in
// the same words.
func (b *Backend) lookupQueue(ctx context.Context, name string) (string, error) {
	if err := sqs.CheckQueueName(name, false); err != nil {
		return "", err
	}
	out, err := b.lookup.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		if missingQueue(err) {
			return "", errNoQueue
		}
		return "", err
	}
	return aws.ToString(out.QueueUrl), nil
}

// missingQueue reports whether the queue service said the queue does not
// exist.
func missingQueue(err error) bool {
	if _, ok := errors.AsType[*sqstypes.QueueDoesNotExist](err); ok {
		return true
	}
	_, ok := errors.AsType[*sqstypes.ResourceNotFoundException](err)
	return ok
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
	in := &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(topicARN)}
	for {
		out, err := b.sns.ListSubscriptionsByTopic(ctx, in)
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
			if !subscriptionARN(arn) {
				// An SQS subscription in the same account is confirmed
				// the moment it is made, so a placeholder here is a
				// subscription nothing can deliver through yet. Treating
				// it as absent would try to create a second one;
				// reporting it is what lets a caller see the state.
				return "", errPendingSubscription
			}
			return arn, nil
		}
		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			return "", nil
		}
		in.NextToken = out.NextToken
	}
}

// subscriptionARN reports whether a SubscriptionArn field holds an ARN.
//
// For a subscription the endpoint has not confirmed the service puts a
// placeholder there instead: "pending confirmation" in Subscribe's answer, and
// "PendingConfirmation" where LocalStack's ListSubscriptions reports one.
// Only the first was recognised, so a pending subscription found by listing
// in the second form would have been used as an ARN, and the attribute read
// would have failed rather than report the pending state. Nothing that is not
// an ARN is one to use.
func subscriptionARN(s string) bool { return strings.HasPrefix(s, arnPrefix) }

// createSubscription subscribes the queue to the topic with raw message
// delivery on, and the subscription's filter policy when one is configured.
//
// The queue's access policy is written first and only when the queue has none:
// without it a real account's SNS cannot deliver into the queue at all, and
// rewriting one that exists would be a driver silently changing IAM. One that
// exists and has no grant for the topic is refused instead.
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
	if !subscriptionARN(arn) {
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
	if existing := strings.TrimSpace(out.Attributes[string(sqstypes.QueueAttributeNamePolicy)]); existing != "" {
		// A queue that already carries a policy is a queue somebody
		// configured, so it is never rewritten. It is READ, though: a
		// policy that does not let this topic deliver used to be left
		// alone and the subscription made anyway, which is a subscription
		// that silently receives nothing.
		if !policyAllows(existing, queueARN, topicARN) {
			return errQueuePolicyDenies
		}
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

// policyAllows reports whether an SQS access policy has a statement that lets
// SNS send messages to queueARN from topicARN.
//
// It reads the statement shapes an SNS grant is written in and nothing more: an
// Allow whose principal is SNS or everyone, whose action covers sqs:SendMessage,
// whose resource covers the queue, and whose aws:SourceArn condition, when it
// has one, matches the topic. A Deny is not evaluated, so a policy this
// accepts can still refuse; one this refuses has no grant for the topic at
// all.
func policyAllows(doc, queueARN, topicARN string) bool {
	var p struct {
		Statement statements
	}
	if err := json.Unmarshal([]byte(doc), &p); err != nil {
		return false
	}
	for i := range p.Statement {
		st := &p.Statement[i]
		if !strings.EqualFold(st.Effect, "Allow") || !st.Principal.coversSNS() {
			continue
		}
		if !anyMatch(st.Action, "sqs:SendMessage", true) {
			continue
		}
		if len(st.Resource) > 0 && !anyMatch(st.Resource, queueARN, false) {
			continue
		}
		if sourceAllowed(st.Condition, topicARN) {
			return true
		}
	}
	return false
}

// sourceAllowed reports whether a statement's conditions let topicARN through:
// no aws:SourceArn condition at all, or a positive one naming it.
func sourceAllowed(cond map[string]map[string]stringList, topicARN string) bool {
	seen := false
	for op, keys := range cond {
		for key, vals := range keys {
			if !strings.EqualFold(key, "aws:SourceArn") {
				continue
			}
			seen = true
			if !strings.Contains(op, "Not") && anyMatch(vals, topicARN, false) {
				return true
			}
		}
	}
	return !seen
}

// anyMatch reports whether any pattern in pats matches s, with IAM's * and ?
// wildcards.
func anyMatch(pats stringList, s string, fold bool) bool {
	for _, p := range pats {
		if fold {
			p, s = strings.ToLower(p), strings.ToLower(s)
		}
		if wildMatch(p, s) {
			return true
		}
	}
	return false
}

// wildMatch matches s against p, where * is any run of characters and ? is one.
func wildMatch(p, s string) bool {
	star, mark := -1, 0
	i, j := 0, 0
	for j < len(s) {
		switch {
		case i < len(p) && (p[i] == '?' || p[i] == s[j]):
			i++
			j++
		case i < len(p) && p[i] == '*':
			star, mark = i, j
			i++
		case star >= 0:
			mark++
			i, j = star+1, mark
		default:
			return false
		}
	}
	for i < len(p) && p[i] == '*' {
		i++
	}
	return i == len(p)
}

// The shapes of an SQS access policy as an account may have written it: IAM
// accepts one statement or a list, a string or a list wherever a list is
// allowed, and "*" for a principal.
type (
	statements      []policyStatement
	policyStatement struct {
		Effect    string
		Principal policyPrincipal
		Action    stringList
		Resource  stringList
		Condition map[string]map[string]stringList
	}
	policyPrincipal struct {
		all     bool
		Service stringList
		AWS     stringList
	}
	stringList []string
)

// UnmarshalJSON implements json.Unmarshaler for one statement or a list.
func (s *statements) UnmarshalJSON(b []byte) error {
	var one policyStatement
	if err := json.Unmarshal(b, &one); err == nil {
		*s = statements{one}
		return nil
	}
	var many []policyStatement
	if err := json.Unmarshal(b, &many); err != nil {
		return err
	}
	*s = many
	return nil
}

// UnmarshalJSON implements json.Unmarshaler for "*" or a principal object.
func (p *policyPrincipal) UnmarshalJSON(b []byte) error {
	var all string
	if err := json.Unmarshal(b, &all); err == nil {
		p.all = all == "*"
		return nil
	}
	var obj struct {
		Service stringList
		AWS     stringList
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return err
	}
	p.Service, p.AWS = obj.Service, obj.AWS
	return nil
}

// UnmarshalJSON implements json.Unmarshaler for a string or a list of them.
func (l *stringList) UnmarshalJSON(b []byte) error {
	var one string
	if err := json.Unmarshal(b, &one); err == nil {
		*l = stringList{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return err
	}
	*l = many
	return nil
}

// coversSNS reports whether the principal includes the SNS service.
func (p policyPrincipal) coversSNS() bool {
	return p.all || anyMatch(p.Service, "sns.amazonaws.com", true) || slices.Contains(p.AWS, "*")
}

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
//   - the cache, for a topic already resolved, when the caller takes cached
//     answers.
//   - otherwise [Config.Resolver] (or [TopicName]) produces the name, and
//     either CreateTopic (which is idempotent and returns the existing topic's
//     ARN) or, for a caller that may not provision, the ARN built from
//     [Config.AccountID] and verified, or with no AccountID the topic of that
//     name that ListTopics reports ([Backend.findTopic]).
//
// Admin().EnsureTopic, and a subscription's resolution that may create, do
// not take cached answers: their job is to find out, and CreateTopic is the
// lookup as well as the create, so asking costs the one call the cache saves
// and recreates a topic another process deleted. From the cache, EnsureTopic
// answered nil for a topic that was gone and left it gone. A publisher keeps
// the cached ARN, one CreateTopic per topic per backend.
func (b *Backend) topicARN(ctx context.Context, topic string, create, cached bool) (string, error) {
	if arn, ok := b.cfg.TopicARNs[topic]; ok {
		return arn, nil
	}
	if IsTopicARN(topic) {
		if err := b.verifyTopic(ctx, topic); err != nil {
			return "", err
		}
		return topic, nil
	}
	if cached {
		b.mu.Lock()
		arn, ok := b.arns[topic]
		b.mu.Unlock()
		if ok {
			return arn, nil
		}
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
		found, err := b.findTopic(ctx, name)
		if err != nil {
			return "", err
		}
		return b.rememberTopic(topic, found), nil
	}
	built := TopicARN(b.cfg.Partition, b.cfg.Region, b.cfg.AccountID, name)
	if err := b.verifyTopic(ctx, built); err != nil {
		return "", err
	}
	return b.rememberTopic(topic, built), nil
}

// findTopic is the ARN of the topic called name, found by paging ListTopics,
// and errNoTopic when the account has none of that name.
//
// It is how a backend with no [Config.AccountID] resolves a topic it may not
// create, which is what that field's doc promises. It used to refuse instead,
// naming AutoCreate as a way out, so a delete on a backend with AutoCreate on
// could not find a topic this process had not created, and AutoCreate cannot
// help a delete. The listing costs one call per hundred topics in the account
// and region, all under the one call's APITimeout. A topic that is found is
// cached, so it costs that once per backend; one that is not found is listed
// again on every lookup, so a caller retrying an open on a missing topic
// lists the account each time and can use up ListTopics' low request quota
// for everyone in the account and region. A deployment with many topics, or
// without sns:ListTopics, sets AccountID and pays one call.
func (b *Backend) findTopic(ctx context.Context, name string) (string, error) {
	in := &sns.ListTopicsInput{}
	for {
		out, err := b.sns.ListTopics(ctx, in)
		if _, denied := errors.AsType[*snstypes.AuthorizationErrorException](err); denied {
			return "", fmt.Errorf("iqmq/snssqs: with no Config.AccountID a topic this call may not create is found by ListTopics, "+
				"and the service refused it; set Config.AccountID, or Config.TopicARNs, to resolve the topic without listing: %w", err)
		}
		if err != nil {
			return "", err
		}
		for i := range out.Topics {
			if arn := aws.ToString(out.Topics[i].TopicArn); TopicNameOfARN(arn) == name {
				return arn, nil
			}
		}
		if aws.ToString(out.NextToken) == "" {
			return "", errNoTopic
		}
		in.NextToken = out.NextToken
	}
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
		"iqmq/snssqs: the subscription's queue, or the SNS subscription that delivers this topic into it, does not exist and Config.AutoCreate is off, so the specification forbids creating it here; Admin().EnsureSubscription creates both")
	errNoQueue = errcode.New(errcode.NotFound,
		"iqmq/snssqs: the subscription's SQS queue does not exist")
	errNoSNSSubscription = errcode.New(errcode.NotFound,
		"iqmq/snssqs: no SNS subscription delivers this topic into this subscription's queue")
	errNoTopic = errcode.New(errcode.NotFound,
		"iqmq/snssqs: no SNS topic in this account and region has the name this topic maps to")
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
	errQueuePolicyDenies = errcode.New(errcode.InvalidConfig,
		"iqmq/snssqs: the queue already has an access policy and it has no statement allowing sns.amazonaws.com to sqs:SendMessage from this topic, so the subscription would receive nothing; the driver does not rewrite a policy it did not write, so add a statement conditioned on the topic ARN, or set Config.NoQueuePolicy if the deployment manages the policy itself")
)
